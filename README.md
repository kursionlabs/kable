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
`Server!::bind_async` can use `defer(plan, continuation)` to run a timer, file
read, or joined plan on the listener's loop. The continuation can capture
owned request values and runs once after the plan joins, including on
cancellation. A transport error, the request deadline, or shutdown cancels
unfinished plans. When cancellation ends the request, the connection closes
after cleanup. A client can half-close its sending side and still receive
the response.

```sh
../kursal/bin/kursal build examples/http-file -o bin/http-file
(cd examples/http-file && ../../bin/http-file)
```

Request `/file` on the printed address to read the bundled `example.txt`.
`/health` remains available while the read is pending.
`/file-with-timeout` races the same read against a one-second timer. If the
timer wins, the handler returns 504 after the worker closes the file and returns.

`defer_next(plan, state, resume, cancel)` lets a continuation return another
HTTP action. On an active request, `resume` receives the owned state and
the completed plan's outcome. When the request is cancelled, `cancel`
consumes both after the active plan joins. Every stage shares the original
request deadline.

An HTTP handler can also pass a portable `sequence(...).then(...)` chain to
`defer_sequence`. The listener drives each stage and delivers request
cancellation to the chain. The example's `/file-sequence` uses this API to read
`example.txt`, then carry its owned buffer into a second read of `footer.txt`.
It returns their contents together.

## Scoped task plans

`lib/task/` defines owned task plans, typed outcomes, and scope policy in
Kursal. A driver provides primitive operations and reports logical completion
and resource retirement separately. The scope chooses winners, tracks
cancellation, and exposes results after all operations have retired.

`lib/async/` drives plans with libuv and provides bounded file reads.
`lib/virtual_clock/` drives timer plans by advancing directly to the next
event. It builds and links without libuv. The integration suite builds
the same timer program against both drivers and compares its outcomes.

Build a wait with `sleep(ms, value)` and drive it with the chosen engine's
`run`. `race` runs two waits in one scope and cancels the loser when the first
timer fires. `join` waits for both child plans. `select` takes the first child
to complete, which may report an error, then cancels and joins the loser.
Both combinators accept nested plans and operations supported by the driver.

`timeout` adds a parent deadline that cancels every pending child, including
children of nested joins. The scope closes all timer handles before returning.
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

### Explicit sequencing

`sequence(first).then(state, resume, cancel)` builds a chain of adapters in
the style of Kursal's iterator chaining. `resume(state, outcome)` runs after
the previous stage joins and returns `Some(next)` to start another stage or
`None` to stop. When the deadline prevents a handoff, `cancel` consumes
the state and any previous outcome. Later adapters also receive their
state back through `cancel` if an earlier adapter stopped.

`run_sequence(chain, budget)` drives a standalone chain on either engine.
Its optional millisecond budget applies to the whole chain. It returns
`None` when the last stage was never constructed and `Some(outcome)` when
that stage produced an outcome, including cancellation during the stage.
`carry(plan, state)` moves an owned value alongside a stage and returns it
with that stage's outcome.

`map(plan, transform)` converts a plan's result without scheduling an
extra operation. Its closure can capture owned state. It receives the full
typed outcome once cleanup has finished, including outcomes from cancelled
or failed plans. A mapped plan can be used inside `join`, `select`, or a
sequence stage.

```sh
../kursal/bin/kursal build examples/task-chain -o bin/task-chain
bin/task-chain
```

### Bounded file reads

`read_file(&access, path, limit)` plans a regular-file read. `run` or `timeout`
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
connection. A handler receives an owned `Connection!` in each event. It can
return `wait(timeout(1000u64, read(connection)))` to start a timed read, or
`write(bytes, next_read)` to write the bytes before starting another read.

A read that gets bytes first stops reading and cancels its timer. If the
deadline fires first, the timer stops the read. In both cases the handler
gets the connection back after the timer closes and can read again or close
it. EOF, transport failure, and shutdown end the connection. On SIGINT or
SIGTERM the listener stops accepting and waits for pending operations to
finish cancelling before returning.

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
- `examples/hello-server/` — standalone server with a custom handler
- `examples/http-file/` — file plan and an owned continuation in an HTTP request
- `examples/async-read/` — two bounded reads under a shared deadline
- `examples/timer-race/` — timer races, joins, and nested cancellation with linear payloads
- `examples/task-chain/` — explicit sequential adapters and owned state on both engines
- `examples/tcp-echo/` — TCP echo with an idle read deadline
- `examples/tcp-session/` — owned connection state across a read retry

Build the example with `../kursal/bin/kursal build examples/hello-server -o bin/hello-server`.
Run `bin/hello-server` and request the address it prints.

```sh
python3 tests/integration.py --check-timeout
```

The integration suite builds Kable and the examples with the adjacent Kursal
compiler. It checks timer races, file reads, HTTP framing and request limits,
and TCP state across read deadlines and shutdown. The hosted file check holds
a worker inside a read so it can verify that `/health` stays responsive,
sequential plans join in order, and shutdown waits for the active stage.
It also checks cancellation after a TCP reset and delivery after a half-close.
Omit `--check-timeout` to skip the ten-second HTTP request deadline checks.

## License

Kable is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Kable by
you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
