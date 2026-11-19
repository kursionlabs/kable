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
```

`GET` and `HEAD` are available at `/` and `/health`. `POST /echo` returns the
request body as `application/octet-stream`. Kable accepts HTTP/1.0 and
HTTP/1.1 origin-form requests, including content-length and chunked bodies,
and serves one request per connection. An `Expect` header receives a 417.
The limits are 128 active connections, 16 KiB for the request line and
headers, 1 MiB for the decoded body, and 1088 KiB for the full request on the
wire.

## Connection lifetimes

The linear `Server!` in `lib/server/` owns the loop and the handler. The loop
multiplexes connections, each with its own parser, buffers, and deadline.
A request has ten seconds to complete. After queuing a response, the
connection has five seconds to write it, half-close, and drain peer input.

On SIGINT or SIGTERM the server stops accepting, cancels connections still
reading their request, and lets connections with queued responses finish
within their existing deadlines. The loop drains pending callbacks and
closes all handles before `Server!.run()` releases the Kursal handler.

Handlers run synchronously on the event loop and should return promptly.

## Code and checks

- `kable/` — command-line program and built-in routes
- `lib/http/` — owned request and response types and response encoding
- `lib/server/` — server lifecycle and the native libuv and llhttp adapter
- `examples/hello-server/` — standalone server with a custom handler

Build the example with `../kursal/bin/kursal build examples/hello-server -o bin/hello-server`.
Run `bin/hello-server` and request the address it prints.

```sh
python3 tests/integration.py --check-timeout
```

The integration suite builds Kable with the adjacent Kursal compiler and checks
HTTP framing, binary bodies, request limits, concurrent clients, and signal
shutdown over loopback. Omit `--check-timeout` to skip the ten-second request
deadline check.

## License

Kable is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Kable by
you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
