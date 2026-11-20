# Kable

Kable is a small HTTP server written in Kursal. It uses libuv for TCP and
connection lifetimes and llhttp for incremental HTTP request parsing.

## Build and run

Build the adjacent `../kursal` checkout with `scripts/bootstrap.sh` and
install the libuv and llhttp development packages. Rebuild the compiler
after updating Kursal so format strings use the same standard-library
paths as Kable. From this repository, run these commands.

```sh
pkg-config --modversion libuv libllhttp
mkdir -p bin
../kursal/bin/kursal build kable -o bin/kable
bin/kable 8080
```

Kable listens on `127.0.0.1`. The port defaults to 8080. Pass `0` to have the
OS choose a port. Kable prints the listening address at startup.

```sh
curl http://127.0.0.1:8080/
curl -I http://127.0.0.1:8080/health
curl --data-binary 'hello' http://127.0.0.1:8080/echo
curl http://127.0.0.1:8080/delay
```

`GET` and `HEAD` are available at `/`, `/health`, and `/delay`.
`/delay` waits one second on the event loop while other requests can proceed.
`POST /echo` returns the request body as `application/octet-stream`.
Kable accepts HTTP/1.0 and
HTTP/1.1 origin-form requests, including content-length and chunked bodies,
and serves one request per connection. An `Expect` header receives a 417.
The limits are 128 active connections, 16 KiB for the request line and
headers, 1 MiB for the decoded body, and 1088 KiB for the full request on the
wire.

## Serve a directory

```sh
../kursal/bin/kursal build examples/static-files -o bin/static-files
bin/static-files examples/http-file 8080
```

From another terminal, run `curl http://127.0.0.1:8080/example.txt`. Pass the
directory you want to serve as the first argument. The port defaults to 8080.
The server listens on loopback, accepts `GET` and `HEAD`, and serves
`index.html` for paths ending in a slash.

Opening the file and checking its length use the worker pool with a two-second
deadline. `GET` sends `Content-Length` and reads one 64 KiB chunk after
each socket write completes. `file.chunks(offset, length)` creates an
owned `FileChunks!` stream. Its `Stream.next()` plans one read and returns
the stream with a chunk, EOF, cancellation, or an error after the read
joins. Only a nonempty chunk advances the cursor. The server sends its
headers and each chunk with `Sink.write()`, then pulls the next chunk from
the write continuation. The server closes the stream after the final
write joins or when a read or write is interrupted. An early EOF
ends the response with fewer bytes than its declared length. `HEAD`
uses the file length without reading its body.
Each completed write restarts the five-second idle timer, so a transfer can
continue while writes make progress.

`GET` accepts a single byte range such as `Range: bytes=0-31`,
`bytes=32-`, or `bytes=-32`. A satisfiable range returns `206 Partial
Content` with `Content-Range`, and an unsatisfiable range returns `416`
with the file size in `Content-Range`. The server ignores multipart ranges
and requests with `If-Range`. `HEAD` reports the full file length.

The server sets the content type for common web files. Path resolution uses
Linux’s `openat2` to stay below the document root and reject symlinks.
This executable requires Linux 5.6 or later. [Linux `openat2` manual](https://man7.org/linux/man-pages/man2/openat2.2.html)

## A small async server

The hello server uses one handler for immediate replies and replies that
wait one second. `Server!::bind_async` takes a handler that returns either
action.

```kursal
use std::time::Duration;

fn hello(request: Request!) -> Action! {
    let delayed = &request.path_is("/delay");
    request.destroy();

    if delayed {
        Response!::text(200, "done\n").after(Duration::from_secs(1))
    } else {
        Response!::text(200, "Hello from Kable!\n").reply()
    }
}
```

```sh
../kursal/bin/kursal build examples/hello-server -o bin/hello-server
bin/hello-server
```

Request `/delay` in one terminal, then request `/` from another while the
timer is still pending. The hello reply can arrive first. The timer belongs
to the delayed request and is cancelled when that request ends.

## Connection lifetimes

The linear `Server!` in `lib/server/` owns the handler and its TCP listener
scope. The same transport runs HTTP and the TCP echo example. Each HTTP
connection attaches its own parser and request buffers to the transport.
A request has ten seconds to complete across all reads. After queuing a
response, the connection has five seconds to write it, half-close, and
drain peer input.

On SIGINT or SIGTERM the server stops accepting, cancels connections still
reading or waiting on a timer, and lets connections with queued responses
finish within their existing deadlines. Shutdown releases any response held
by a cancelled timer. The loop drains pending callbacks and closes all handles
before `Server!.run()` releases the Kursal handler.

Handlers run on the event loop and should return promptly. A handler bound with
`Server!::bind_async` can import `task::PlanOutput` and return
`plan.defer(|outcome, state| ...)`. The continuation runs once after the
plan joins and receives its owned outcome. With `RequestState::Active`, it
can return `Some(action)` to reply or defer another plan. With
`RequestState::Cancelled`, it releases the values it owns and returns
`None`. Returning `None` closes the connection. A transport error, the
request deadline, or shutdown cancels unfinished plans. A client can
half-close its sending side and still receive the response.

### A file response with a deadline

```sh
../kursal/bin/kursal build examples/http-file -o bin/http-file
(cd examples/http-file && ../../bin/http-file)
```

Request `/file-with-timeout` on the printed address to read the bundled
`example.txt` with a 64 KiB limit and a one-second deadline.

```kursal
use std::time::Duration;

let access = StaticRead::new();
read_file(&access, "example.txt", FILE_LIMIT)
    .within(Duration::from_secs(1))
    .defer(|outcome, state| timed_reply(outcome, state, file_response, discard_file))
```

`.within(...)` adds the local deadline. If the timer wins, the continuation
gets `DeadlineStatus::TimedOut` after the worker joins, releases any buffer,
and replies 504 while the request is active. If the request ends first, the
continuation releases the outcome and closes the connection. `/file` uses
the request deadline alone, and `/health` remains available while a read is
pending.

### Two independent file reads

`/file-join` starts reads of `example.txt` and `footer.txt` together using
`join(first, second).within(Duration::from_secs(1))` and the same `.defer(...)`
handoff. Both reads have a 64 KiB limit and share the one-second deadline.
The response appends the footer after the first file’s bytes regardless of
which read finishes first.
A timeout returns 504 after both workers retire, and the handler destroys
any buffer it received. [Parallel file handler](examples/http-file/file_join.kur)

### Dependent file reads

`/file-sequence` defers the first read of `example.txt`. Its continuation
chooses the footer and returns a deferred action for the second read under
the original request deadline. The second continuation captures the first
buffer and either appends the footer or destroys the buffer if the request
has ended. [Dependent file handler](examples/http-file/file_sequence.kur)

## Scoped task plans

`lib/task/` defines owned task plans, typed outcomes, and scope policy in
Kursal. A driver provides primitive operations and reports logical completion
and resource retirement separately. The scope chooses winners, tracks
cancellation, and exposes results after all operations have retired.
Plans collect results by the node IDs they registered during staging.

`lib/async/` drives plans with libuv and provides bounded file reads.
`lib/virtual_clock/` drives timer plans by advancing directly to the next
event. It builds and links without libuv. The integration suite builds
the same timer program against both drivers and compares its outcomes.

Import plans, combinators, and outcome types from `task`. For standalone
execution, import `run`, `run_with_deadline`, or `run_sequence` from the
runner. TCP and HTTP handlers submit plans through their listener.

Timers, deadlines, and sequence budgets take `std::time::Duration`.
The libuv runner uses a millisecond clock. It rounds positive timer
durations up to whole milliseconds and preserves zero. The virtual clock
advances in nanoseconds.

`PlanOutput[Task -> Output]` determines the outcome type from the plan
alone. The runner uses `Plan[Task, Driver -> Output]` to check whether its
driver can run the plan and return that outcome. A file plan has a known
outcome even when the selected driver cannot execute file reads.

`run(sleep(duration, value))` returns `(value, status)`. The caller regains the
original value even if the timer is cancelled or fails to start.
`run(join(first, second))` returns `(first_output, second_output, status)`
after both children join. The outputs are in input order and each carries
its own result. In both tuples, `status` is a `Completion[()]`.

`select` takes the first child to complete, which may report an error, then
cancels and joins the loser. It returns `(first_output, second_output, status)`
in input order. On selection, `Completion::Ready(side)` identifies the winner
as `Side::First` or `Side::Second`. Cancellation and failure also return both
outputs. Both combinators accept nested plans and operations supported by the driver.

`run_with_deadline(plan, duration)` adds a parent deadline that cancels every
pending child, including children of nested joins. The scope closes all timer handles before returning.
The outcome hands ownership of the values back to the caller on readiness,
cancellation, or startup failure. A cancelled join preserves the individual
outcomes of children that already completed.

`Completion::Failed`, `StreamOutcome!::Failed`, and
`DeadlineStatus::Failed` carry a `task::Failure`. A driver reports
`Startup(code)` if it cannot prepare or start the scope. An operation that
runs and fails reports `Operation(code)`. Both preserve the backend code.
`NotStarted` covers plans collected without starting their scope.
`InvalidPlan` reports rejected scope structure and `InvalidResult` reports
a missing or unexpected driver result.

```sh
../kursal/bin/kursal build examples/timer-race -o bin/timer-race
bin/timer-race
```

The example reclaims an owned `Buffer!` from the losing timer, then joins
a timer race with another wait. It also shows a parent deadline and a
winning branch preserving completed child results while cancelling pending
nested waits.

### Plan adapters

`plan.map(transform)` converts the plan's joined outcome. Its closure can
capture owned state and runs once, including on cancellation or failure.

`plan.within(duration)` races the plan against a local timer and returns
`(output, deadline)` after both join. `DeadlineStatus::Completed` means the
plan finished first, including when the operation reported an error.
`TimedOut` means the local timer fired first. `Cancelled` means the enclosing
scope cancelled the race. `Failed(failure)` reports an error from the scope or
the winning timer. Every case gives the caller the inner output to reclaim.
A local timeout leaves unrelated sibling plans running.

Import `task::PlanOutput` to use both methods. Driver implementations use
`task::driver` for staging, collection, and scope policy.

### Implementing a driver

The virtual clock is a reference driver for timer plans. It uses the same
`ScopeState!` policy as libuv. [Virtual clock implementation](lib/virtual_clock/lib.kur)

1. Implement `PlanDriver[Scope]` to register timers and pairs and report
   joined results. Use the IDs assigned by `ScopeState!`. Register a pair's
   children before the pair. For each engine-specific operation, implement
   `PlanOutput` for its outcome type and `Plan` to stage and collect it.
2. Call `prepare(task, scope)` and use `&prepared.with_scope(...)` to drive the
   registered root. Validate it with `begin`. Report `started` for each
   scheduled resource, `complete(id)` when an operation finishes, and
   `retired` when its resource is safe to release. Drain `take_cancel` to stop
   the leaves the policy cancelled. A cancelled operation can still be in
   flight.
3. Call `prepared.collect()` after the scope joins, or to reclaim a plan
   whose staging or submission failed. Collection returns the plan's owned
   outcome and the scope for teardown.

An engine that supports sequences also implements `Clock` and
`StageDriver`. Each stage uses the sequence's absolute deadline and reports
parent deadline expiry alongside its joined output. [Driver interfaces](lib/task/driver.kur) · [Sequence interfaces](lib/task/sequence.kur)

### Pull-based streams

Import `task::Stream` to call `source.next()`. It moves the source into a
one-shot plan. After the plan joins it returns the source alongside
`StreamOutcome![Item]`, which carries an item, EOF, cancellation, or an
error code. The caller decides when to request another item. File chunks
and TCP connections both use this interface. TCP EOF ends the read side
and the returned connection can still write.

### Sequential writes

Import `task::Sink` to call `writer.write(bytes)` on an output file or a TCP
connection. The plan owns both values until it joins and returns
`(writer, bytes, status)`. A `Completion::Ready` status means the writer
accepted the full buffer. The caller can release or reuse the buffer on
every path.

### Standalone sequencing

Import `sequence` from `task::sequence`. `sequence(first).then(resume)` builds a chain of adapters in the style
of Kursal’s iterator chaining. The previous plan determines the closure’s
input type. The closure consumes the previous outcome
and returns `Some(next)` to start another stage or `None` to stop. Both
the closure captures and the previous outcome must be copyable because
the handoff can be skipped.

Use `.then_owned(state, resume, cancel)` when the captures or outcome need
explicit cleanup. An inline `resume` closure infers its inputs and the next
plan. It takes ownership of `state` and the outcome after the previous stage
joins. Put linear values in `state` so the named `cancel` function can
consume them and any available outcome if the handoff is skipped. The
`resume` closure’s own captures must be copyable.

`run_sequence(chain, budget)` drives a standalone chain on either engine.
The budget is an `Option[Duration]` that applies to the whole chain.
Match the result using `task::sequence::SequenceOutcome`.

- `Finished(output)` returns the final stage's joined output, including any
  cancellation or failure reported by that stage.
- `Stopped` means a continuation returned `None`.
- `Cancelled` means the shared deadline prevented a handoff. The skipped
  stages release their owned state through their cleanup callbacks.

A stage's local `.within()` timeout is part of the output passed to the next
continuation while the shared budget remains.

```sh
../kursal/bin/kursal build examples/task-chain -o bin/task-chain
bin/task-chain
```

### Bounded file reads

`read_file(&access, path, limit)` plans a regular-file read. After the plan
joins, match `Completion::Ready(bytes)`, `Completion::Cancelled`, or
`Completion::Failed(failure)`. The same cases apply to `open_file` and
`create_file`, whose ready values are owned file handles. The work runs
on libuv's worker pool. Cancellation skips queued work when possible and
asks a running worker to stop between reads. The scope waits for the worker
to return and close its descriptor before handing back the result.
When reads are joined, each child reports its own result.

```sh
../kursal/bin/kursal build examples/async-read -o bin/async-read
bin/async-read kur.toml README.md
```

The example joins two reads under a common deadline and writes their contents
in argument order. Each file is limited to 64 KiB.

### Copy a file with bounded reads and writes

```sh
../kursal/bin/kursal build examples/file-copy -o bin/file-copy
bin/file-copy README.md README.copy.md
```

The example uses `async::with_runner` to share one libuv loop across the
open, create, read, and write operations. Inside the callback,
`&runner.run(plan, deadline)` runs and joins one plan at a time. The optional
`Duration` sets a parent deadline for that operation.

The copy pulls one chunk from `FileChunks!` and waits for
`destination.write(bytes)` to join before pulling the next. Each sequential
write starts at the file's current position. The worker continues through
short writes, and `&destination.position()` reports the total accepted even
after cancellation or failure. For random access, `write_chunk(offset, bytes)`
returns `(file, bytes, count, status)`. The count includes any bytes accepted
before cancellation or failure, and the sequential position stays the same. [Linux positioned I/O manual](https://man7.org/linux/man-pages/man2/pwrite.2.html)

The destination must be a new path. Pass an optional third argument to set
a deadline in milliseconds for each write. If a copy fails or is cancelled,
the destination contains the bytes written so far.

## Scoped TCP reads and writes

`lib/tcp/` runs a loopback listener with one in-flight operation per
connection. `Listener!::bind_connections(port, read_next)` calls
`read_next(connection)` once per accepted connection. The echo handler runs
`connection.next().within(Duration::from_secs(1))` with `tcp::defer` on the
listener loop. When it receives bytes, it defers
`connection.write(bytes).within(Duration::from_secs(1))`. Its continuation
releases the buffer and starts the next read only if the write succeeded
within the deadline.

Omitting `.within(...)` gives the write the connection's lifetime. The
continuation can hand the connection back to `read_next` after the write joins.

```kursal
defer(connection.write(bytes), |result| {
    let (connection, bytes, status) = result;
    bytes.destroy();
    match status {
        Completion::Ready => read_next(connection),
        Completion::Cancelled | Completion::Failed => connection.close(),
    }
})
```

The write plan owns the connection and buffer until completion and returns
both on every outcome. Cancelling an in-flight write closes the socket and
waits for the write callback. On failure or cancellation, the buffer is
available for cleanup and delivery progress is unspecified. On SIGINT or
SIGTERM the listener stops accepting and waits for pending scopes to join.

```sh
../kursal/bin/kursal build examples/tcp-echo -o bin/tcp-echo
bin/tcp-echo
```

Connect to the printed address with a TCP client such as `nc 127.0.0.1 PORT`.
The example echoes binary data and gives each read and write one second.
Other connections can continue while either operation is pending.

### Connection-local state

The session example starts each connection with its own linear buffer.
Each read continuation owns the pending bytes and can retry one idle read
with the same buffer. When complete lines arrive, the write continuation
holds any trailing partial line until the write joins. Closing the session
releases the buffer and returns an action to close the connection.

```sh
../kursal/bin/kursal build examples/tcp-session -o bin/tcp-session
bin/tcp-session
```

## Code and checks

- `kable/` — command-line program and built-in routes
- `lib/task/` — portable task plans and scope policy
- `lib/async/` — libuv driver and bounded file reads
- `lib/virtual_clock/` — deterministic timer driver
- `lib/http/` — owned request and response types and response encoding
- `lib/server/` — server lifecycle and the native libuv and llhttp adapter
- `lib/tcp/` — scoped TCP reads, deadlines, and writes
- `examples/hello-server/` — immediate and delayed replies in one handler
- `examples/static-files/` — streams files from a document root
- `examples/http-file/` — bounded file replies, parallel reads, and an owned continuation
- `examples/async-read/` — two bounded reads under a shared deadline
- `examples/timer-race/` — timer races, joins, and nested cancellation with linear payloads
- `examples/task-chain/` — explicit sequential adapters and owned state on both engines
- `examples/tcp-echo/` — TCP echo with an idle read deadline
- `examples/tcp-session/` — owned connection state across a read retry

```sh
python3 tests/integration.py --check-timeout
```

The integration suite builds Kable and the examples with the adjacent Kursal
compiler. It checks timer races, file reads, HTTP framing and request limits,
and TCP state across read deadlines and shutdown. The hosted file check holds
a worker inside a read so it can verify that `/health` stays responsive,
sequential plans join in order, and shutdown waits for the active stage.
It also checks cancellation after a TCP reset and delivery after a half-close.
Omit `--check-timeout` to skip the longer request and streaming deadline checks.

## License

Kable is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Kable by
you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
