# Kable

Kable is a small HTTP server written in Kursal. It uses libuv for TCP and
connection lifetimes and llhttp for incremental HTTP request parsing.

## Build and run

Build the adjacent `../kursal` repository first and install the libuv and
llhttp development packages. From this repository, run these commands.

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
each socket write completes. The Kursal transfer loop hands its owned file
to `file.read_chunk(offset, limit)` and gets it back once the read joins,
even after cancellation. It closes the file after the final read or when a
cancelled read joins. `HEAD` uses the file length without reading its body.
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
fn hello(request: Request!) -> Action! {
    let delayed = &request.path_is("/delay");
    request.destroy();

    if delayed {
        Response!::text(200, "done\n").after(1000)
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
`Server!::bind_async` can import `server::PlanOutput` and return
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
let access = StaticRead::new();
read_file(&access, "example.txt", FILE_LIMIT)
    .within(1000)
    .defer(|outcome, state| timed_reply(outcome, state, file_response, discard_file))
```

`.within(1000)` adds the local deadline. If the timer wins, the continuation
gets `WithinOutcome::TimedOut` after the worker joins, releases any buffer,
and replies 504 while the request is active. If the request ends first, the
continuation releases the outcome and closes the connection. `/file` uses
the request deadline alone, and `/health` remains available while a read is
pending.

### Two independent file reads

`/file-join` starts reads of `example.txt` and `footer.txt` together using
`join(first, second).within(1000)` and the same `.defer(...)` handoff.
Both reads have a 64 KiB
limit and share the one-second deadline. The response appends the footer
after the first file’s bytes regardless of which read finishes first.
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

`PlanOutput[Task -> Output]` determines the outcome type from the plan
alone. The runner uses `Plan[Task, Driver -> Output]` to check whether its
driver can run the plan and return that outcome. A file plan has a known
outcome even when the selected driver cannot execute file reads.

Build a wait with `sleep(ms, value)` and drive it with the chosen engine's
`run`. `join` waits for both child plans. `select` takes the first child to
complete, which may report an error, then cancels and joins the loser.
Both combinators accept nested plans and operations supported by the driver.

`run_with_deadline(plan, ms)` adds a parent deadline that cancels every
pending child, including children of nested joins. The scope closes all timer handles before returning.
The outcome hands ownership of the values back to the caller on readiness,
cancellation, or startup failure. A cancelled join preserves the individual
outcomes of children that already completed.

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

`plan.within(ms)` races the plan against a local timer. `TimedOut` means the
timer won, `Cancelled` means the enclosing scope cancelled the plan, and
`Completed` carries the plan's own outcome, including any operation error.
Every branch returns the inner outcome after cleanup so the caller can
reclaim owned values. A local timeout leaves unrelated sibling plans running.

Import `PlanOutput` to use both methods. Driver implementations use
`task::driver` for staging, collection, and scope policy.

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
Its optional millisecond budget applies to the whole chain. It returns
`None` when the last stage was never constructed and `Some(outcome)` when
that stage produced an outcome, including cancellation during the stage.

```sh
../kursal/bin/kursal build examples/task-chain -o bin/task-chain
bin/task-chain
```

### Bounded file reads

`read_file(&access, path, limit)` plans a regular-file read. `run` or `run_with_deadline`
returns an owned `Buffer!`, cancellation, or a libuv error. The work runs
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

## Scoped TCP reads

`lib/tcp/` runs a loopback listener with one in-flight operation per
connection. The echo handler moves its owned `Connection!` into
`connection.read_plan().within(1000u64)` and returns a `tcp::defer` action.
That action runs the plan on the connection’s listener loop.
`tcp::defer_write` writes a chunk before starting the next plan.

The read and its timer race in the same scope used by file plans. The read
finishes or stops before the scope returns the connection, and the timer
handle has closed by then. The handler can use the returned connection for
another read or close it. On SIGINT or SIGTERM the listener stops accepting
and waits for pending scopes to finish cancelling before returning.

Handlers that work directly with TCP events can use
`connection.read().timeout(1000u64).wait()`, `write(bytes, next_read)`, and
`connection.close()`.

```sh
../kursal/bin/kursal build examples/tcp-echo -o bin/tcp-echo
bin/tcp-echo
```

Connect to the printed address with a TCP client such as `nc 127.0.0.1 PORT`.
The example echoes binary data and closes connections after one second of
read inactivity. Other connections can continue sending data during that wait.

### Connection-local state

`Listener!::bind_stateful(port, start, handle, finish)` lets each connection
own a state value, including a linear `Buffer!`. The listener calls `start`
once for each accepted connection. Each event moves the state into `handle`,
which returns the next state and action. After the connection and any pending
operation have closed, `finish` consumes the last state exactly once.

```sh
../kursal/bin/kursal build examples/tcp-session -o bin/tcp-session
bin/tcp-session
```

The session example echoes complete lines and keeps partial lines in a
connection-local buffer. It retries an idle read once, preserving the partial
line across the timeout. Connecting with a second client gives it an independent
buffer.

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
