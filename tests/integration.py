"""Exercise Kable's HTTP handling and shutdown over real TCP connections."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import errno
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
DELAY = b"GET /delay HTTP/1.1\r\nHost: localhost\r\n\r\n"
HEALTH = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"


def connect(port):
    return socket.create_connection(("127.0.0.1", port), timeout=15)


def read_headers(stream):
    version, status, _ = stream.readline().split(b" ", 2)
    headers = {}
    for line in stream:
        if line == b"\r\n":
            break
        key, value = line.split(b":", 1)
        headers[key.lower()] = value.strip()
    assert headers[b"connection"] == b"close", headers
    return version, int(status), headers


def read_response(client):
    with client.makefile("rb") as stream:
        version, status, headers = read_headers(stream)
        return version, status, headers, stream.read()


def exchange(port, *parts, eof=False):
    with connect(port) as client:
        for part in parts:
            client.sendall(part)
        if eof:
            client.shutdown(socket.SHUT_WR)
        return read_response(client)


def assert_response(response, status, body, *, head=False, version=b"HTTP/1.1"):
    actual_version, actual_status, headers, actual_body = response
    assert (actual_version, actual_status) == (version, status), response[:3]
    assert int(headers[b"content-length"]) == len(body), headers
    assert actual_body == (b"" if head else body)
    return headers


def check_http(port):
    host = b"Host: localhost\r\n"
    assert_response(exchange(port, b"GET / HTTP/1.1\r\n", host, b"\r\n"), 200, b"Kable\n")
    assert_response(exchange(port, b"HEAD /health?probe=1 HTTP/1.1\r\n", host, b"\r\n"),
                    200, b"ok\n", head=True)
    assert_response(exchange(port, b"GET / HTTP/1.0\r\n\r\n"),
                    200, b"Kable\n", version=b"HTTP/1.0")
    assert_response(exchange(port, b"GET /missing HTTP/1.1\r\n", host, b"\r\n"),
                    404, b"Not Found\n")
    headers = assert_response(exchange(port, b"GET /echo HTTP/1.1\r\n", host, b"\r\n"),
                              405, b"Method Not Allowed\n")
    assert headers[b"allow"] == b"POST"

    headers = assert_response(exchange(port, b"POST /echo HTTP/1.1\r\nHo", b"st: localhost\r\n",
                                       b"Transfer-Encoding: chunked\r\n\r\n",
                                       b"2\r\na", b"\x00\r\n1\r\nb\r\n0\r\n\r\n"), 200, b"a\x00b")
    assert headers[b"content-type"] == b"application/octet-stream"

    with connect(port) as client:
        client.sendall(b"POST /health HTTP/1.1\r\n" + host + b"Content-Length: 6\r\n\r\n")
        assert_response(read_response(client), 405, b"Method Not Allowed\n")

    assert_response(exchange(port, b"GET / HTTP/1.1\r\n\r\n"), 400, b"Bad Request\n")
    assert_response(exchange(port, b"GET / HTTP/1.1\r\n", host, host, b"\r\n"),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"GET / HTTP/1.1\r\nHost: two names\r\n\r\n"),
                    400, b"Bad Request\n")
    for first, rest, status, body in (
        (b"local", b"host \t", 200, b"Kable\n"),
        (b"two ", b"names", 400, b"Bad Request\n"),
    ):
        with connect(port) as client:
            client.sendall(b"GET / HTTP/1.1\r\nHo")
            assert not select.select([client], [], [], 0.05)[0]
            client.sendall(b"st: " + first)
            assert not select.select([client], [], [], 0.05)[0]
            client.sendall(rest + b"\r\n\r\n")
            assert_response(read_response(client), status, body)
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 2\r\n\r\na", eof=True), 400, b"Bad Request\n")
    assert_response(exchange(port, b"GET /health HTTP/1.1\r\nHost: local", eof=True),
                    400, b"Bad Request\n")
    with connect(port) as client:
        client.shutdown(socket.SHUT_WR)
        assert client.recv(1) == b""
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 1048577\r\n\r\n"), 413, b"Content Too Large\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Expect: 100-continue\r\nContent-Length: 1\r\n\r\n"),
                    417, b"Expectation Failed\n")
    oversized_expected = b"Expect: 100-continue\r\nContent-Length: 1048577\r\n\r\n"
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host, oversized_expected),
                    417, b"Expectation Failed\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\nHost: \t\r\n", oversized_expected),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"GET / HTTP/1.1\r\n", host,
                             b"X-Large: " + b"a" * 16384),
                    431, b"Request Header Fields Too Large\n")
    request = b"GET /health HTTP/1.1\r\n" + host + b"\r\n"
    assert_response(exchange(port, request + request), 200, b"ok\n")


def check_concurrency(port, process):
    with connect(port) as partial, socket.socket() as output, socket.socket() as blocked:
        partial.sendall(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\na")
        body = bytes(range(256)) * 4096
        for client in (output, blocked):
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
            client.settimeout(15)
            client.connect(("127.0.0.1", port))
            client.sendall(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048576\r\n\r\n")
            client.sendall(body)
            assert client.recv(1, socket.MSG_PEEK) == b"H"

        def health(_):
            response = exchange(port, HEALTH)
            assert_response(response, 200, b"ok\n")

        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(health, range(24)))

        with connect(port) as delayed:
            delayed.sendall(DELAY)
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert not select.select([delayed], [], [], 0.05)[0], "delayed reply already readable"
            process.terminate()
            assert delayed.recv(1) == b"", "shutdown sent a cancelled reply"
            assert partial.recv(1) == b""
            assert_response(read_response(output), 200, body)
            # The peer that never consumes its response must not hold up shutdown.
            assert process.wait(timeout=8) == 0


def check_delayed_handler(port, immediate=HEALTH, body=b"ok\n", delayed_request=DELAY):
    with connect(port) as delayed:
        delayed.sendall(delayed_request)
        assert not select.select([delayed], [], [], 0.05)[0], "delay replied immediately"
        assert_response(exchange(port, immediate), 200, body)
        assert not select.select([delayed], [], [], 0)[0], "the immediate reply was blocked by the timer"
        assert_response(read_response(delayed), 200, b"done\n")


def check_hello_server(binary):
    with subprocess.Popen([str(binary)], stdout=subprocess.PIPE) as process:
        try:
            port = listening_port(process, b"Listening on http://127.0.0.1:")
            check_delayed_handler(
                port,
                immediate=b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
                body=b"Hello from Kable!\n",
                delayed_request=DELAY.replace(b"/delay ", b"/delay?test=1 "),
            )
            process.terminate()
            assert process.wait(timeout=5) == 0
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable hello server checks passed")


def check_static_files(command, directory, check_write_timeout=False):
    root = directory / "public"
    root.mkdir()
    pages = root / "pages"
    pages.mkdir()
    outside = directory / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_bytes(b"outside the document root")
    (root / "external.txt").symlink_to(outside / "secret.txt")
    (root / "external").symlink_to(outside, target_is_directory=True)
    (root / "index.html").write_bytes(b"<h1>Kable</h1>\n")
    (pages / "index.html").write_bytes(b"<p>Nested index</p>\n")
    (pages / "a b.txt").write_bytes(b"A file with spaces\n")
    (root / "payload.bin").write_bytes(b"\x00\x01\xff")
    large = bytes(range(256)) * 5000 + b"last chunk"
    (root / "large.bin").write_bytes(large)
    (root / "empty").touch()
    stalled_path = root / "stalled.bin"
    with stalled_path.open("wb") as file:
        file.truncate(16 * 1024 * 1024)
    os.mkfifo(root / "pipe")

    with subprocess.Popen([*command, str(root), "0"], stdout=subprocess.PIPE) as process:
        try:
            port = listening_port(process, b"Listening on http://127.0.0.1:")

            def request(path, method=b"GET", headers=b""):
                return exchange(port, method + b" " + path + b" HTTP/1.1\r\nHost: localhost\r\n" + headers + b"\r\n")

            headers = assert_response(request(b"/"), 200, b"<h1>Kable</h1>\n")
            assert headers[b"content-type"] == b"text/html; charset=utf-8"
            assert_response(request(b"/pages/"), 200, b"<p>Nested index</p>\n")
            headers = assert_response(request(b"/pages/a%20b.txt?download"), 200, b"A file with spaces\n")
            assert headers[b"content-type"] == b"text/plain; charset=utf-8"
            headers = assert_response(request(b"/payload.bin"), 200, b"\x00\x01\xff")
            assert headers[b"content-type"] == b"application/octet-stream"
            assert_response(request(b"/payload.bin", b"HEAD"), 200, b"\x00\x01\xff", head=True)
            assert_response(request(b"/empty"), 200, b"")
            headers = assert_response(request(b"/large.bin"), 200, large)
            assert headers[b"accept-ranges"] == b"bytes" and b"content-range" not in headers
            half_closed = exchange(port, b"GET /large.bin HTTP/1.1\r\nHost: localhost\r\n\r\n", eof=True)
            assert_response(half_closed, 200, large)
            assert_response(request(b"/large.bin", b"HEAD"), 200, large, head=True)
            for value, start, end in (
                (b"bytes=65000-140000", 65000, 140000),
                (b"\tByTeS=\t65000-65019 \t", 65000, 65019),
                (b"bytes=" + b"0" * 247 + b"0-0", 0, 0),
                (f"bytes={len(large) - 19}-".encode(), len(large) - 19, len(large) - 1),
                (f"bytes={len(large) - 19}-999999999999999999999".encode(), len(large) - 19, len(large) - 1),
                (b"bytes=-19", len(large) - 19, len(large) - 1),
                (b"bytes=-999999999999999999999", 0, len(large) - 1),
            ):
                headers = assert_response(request(b"/large.bin", headers=b"rAnGe: " + value + b"\r\n"),
                                          206, large[start:end + 1])
                assert headers[b"content-range"] == f"bytes {start}-{end}/{len(large)}".encode(), headers
                assert headers[b"accept-ranges"] == b"bytes", headers
                assert headers[b"content-type"] == b"application/octet-stream", headers
            with connect(port) as client:
                client.sendall(b"GET /payload.bin HTTP/1.1\r\nHost: localhost\r\nRa")
                assert not select.select([client], [], [], 0.05)[0]
                client.sendall(b"nGe: bytes=1-")
                assert not select.select([client], [], [], 0.05)[0]
                client.sendall(b"1\r\n\r\n")
                headers = assert_response(read_response(client), 206, b"\x01")
                assert headers[b"content-range"] == b"bytes 1-1/3", headers
            for path, size, value in (
                (b"/large.bin", len(large), f"bytes={len(large)}-".encode()),
                (b"/large.bin", len(large), b"bytes=999999999999999999999-"),
                (b"/large.bin", len(large), b"bytes=-0"),
                (b"/empty", 0, b"bytes=0-"),
            ):
                headers = assert_response(request(path, headers=b"Range: " + value + b"\r\n"), 416, b"")
                assert headers[b"content-range"] == f"bytes */{size}".encode(), headers
            assert_response(request(b"/empty", headers=b"Range: bytes=-1\r\n"), 200, b"")
            for extra in (
                b"Range: records=0-1\r\n",
                b"Range: bytes=2-1\r\n",
                b"Range: bytes=1-2junk\r\n",
                b"Range: bytes=0-0,2-2\r\n",
                b"Range: bytes=0-0\r\nRange: bytes=2-2\r\n",
                b"Range: bytes=0-0\r\nIf-Range: \"unavailable\"\r\n",
                b"Range: bytes=" + b"0" * 248 + b"0-0\r\n",
            ):
                headers = assert_response(request(b"/payload.bin", headers=extra), 200, b"\x00\x01\xff")
                assert b"content-range" not in headers, headers
            headers = assert_response(request(b"/payload.bin", b"HEAD", b"Range: bytes=1-1\r\n"),
                                      200, b"\x00\x01\xff", head=True)
            assert b"content-range" not in headers, headers
            headers = assert_response(request(b"/", b"POST"), 405, b"Method Not Allowed\n")
            assert headers[b"allow"] == b"GET, HEAD"
            for path in (b"/missing", b"/pages", b"/pipe", b"/external.txt",
                         b"/external/secret.txt", b"/%2e%2e/outside/secret.txt"):
                assert_response(request(path), 404, b"Not Found\n")
            for path in (b"/%xz", b"/%00", b"/short%"):
                assert_response(request(path), 400, b"Bad Request\n")
            with slow_download(port, stalled_path) as (stalled, _):
                assert holds_file(process, stalled_path), "slow transfer finished before backpressure"
                assert_response(request(b"/"), 200, b"<h1>Kable</h1>\n")
                assert holds_file(process, stalled_path), "slow transfer finished while serving another request"
                stalled.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            wait_for_file_release(process, stalled_path, 2)
            if check_write_timeout:
                check_stream_idle_timeout(port, process, stalled_path)
            assert_response(request(b"/"), 200, b"<h1>Kable</h1>\n")
            process.terminate()
            assert process.wait(timeout=5) == 0
        finally:
            if process.poll() is None:
                process.kill()
    library = build_file_gate(directory)
    check_stream_write_order(command, directory, root, large)
    check_stream_shutdown(command, root, library)
    check_stream_truncation(command, root, library)
    print("Kable static file checks passed")


@contextmanager
def slow_download(port, path, offset=None):
    with socket.socket() as client:
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65_536)
        client.settimeout(8)
        client.connect(("127.0.0.1", port))
        range_header = b"" if offset is None else f"Range: bytes={offset}-\r\n".encode()
        client.sendall(b"GET /" + os.fsencode(path.name) + b" HTTP/1.1\r\nHost: localhost\r\n" + range_header + b"\r\n")
        with client.makefile("rb") as stream:
            version, status, headers = read_headers(stream)
            size = path.stat().st_size
            assert version == b"HTTP/1.1" and status == (200 if offset is None else 206), (version, status)
            if offset is not None:
                assert headers[b"content-range"] == f"bytes {offset}-{size - 1}/{size}".encode(), headers
            assert int(headers[b"content-length"]) == size - (offset or 0), headers
            yield client, stream


def check_stream_idle_timeout(port, process, path):
    chunk = bytes(512 * 1024)
    offset = 64 * 1024
    with slow_download(port, path, offset) as (_, stream):
        for _ in range(6):
            time.sleep(1)
            assert stream.read(len(chunk)) == chunk, "paced download ended early"
        assert holds_file(process, path), "stream closed despite continued write progress"
        remaining = path.stat().st_size - offset - 6 * len(chunk)
        assert stream.read() == bytes(remaining), "paced download lost bytes past the original deadline"

    start = time.monotonic()
    with slow_download(port, path):
        assert holds_file(process, path), "stalled stream never held the file"
        wait_for_file_release(process, path, 8)
        assert time.monotonic() - start >= 4.75, "stalled stream expired too early"


def check_stream_write_order(command, directory, root, body):
    library = directory / "stream-write-order.so"
    subprocess.run(["cc", "-shared", "-fPIC", str(ROOT / "tests/stream_write_order.c"),
                    "-ldl", "-o", str(library)], check=True)
    env = os.environ | {"LD_PRELOAD": str(library)}
    with subprocess.Popen([*command, str(root), "0"], env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        try:
            port = listening_port(process, b"Listening on http://127.0.0.1:")
            request = b"GET /large.bin HTTP/1.1\r\nHost: localhost\r\n\r\n"
            version, status, headers, sent = exchange(port, request)
            assert version == b"HTTP/1.1" and status == 200, (version, status)
            assert int(headers[b"content-length"]) == len(body) and sent == b"", (headers, sent)
            wait_for_file_release(process, root / "large.bin", 2)
            assert_response(exchange(port, request), 200, body)
            process.terminate()
            _, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
        finally:
            if process.poll() is None:
                process.kill()

    prefix = b"stream-test: "
    trace = [line.removeprefix(prefix) for line in stderr.splitlines() if line.startswith(prefix)]
    queued, wrote = b"queued file work", b"wrote"
    # Each request first opens the file and writes headers. The first body
    # write fails to start. Each read in the next request follows a write.
    rejected = [queued, wrote, queued, b"write rejected"]
    chunks = (len(body) + 65_535) // 65_536
    assert trace == rejected + [queued, wrote] * (chunks + 1), stderr


def check_stream_shutdown(command, root, library):
    path = root / "large.bin"
    with gated_file_server([*command, str(root), "0"], root, library, gates=8) as (process, port, started, permit):
        with connect(port) as client:
            client.sendall(b"GET /large.bin HTTP/1.1\r\nHost: localhost\r\n\r\n")
            wait_for_held_read(started, b"D")
            with client.makefile("rb") as stream:
                _, status, headers = read_headers(stream)
                assert status == 200 and int(headers[b"content-length"]) == path.stat().st_size
                assert_response(exchange(port, b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                                200, b"<h1>Kable</h1>\n")
                process.terminate()
                wait_for_listener_shutdown(port)
                assert process.poll() is None, "shutdown returned before the chunk read joined"
                assert holds_file(process, path), "shutdown closed the file during the read"
                assert os.write(permit, b"R") == 1
                assert stream.read() == b"", "the cancelled read sent a chunk"
                assert process.wait(timeout=5) == 0, process.stderr.read()


def check_stream_truncation(command, root, library):
    path = root / "large.bin"
    size = path.stat().st_size
    with gated_file_server([*command, str(root), "0"], root, library, gates=8) as (process, port, started, permit):
        with connect(port) as client:
            client.sendall(b"GET /large.bin HTTP/1.1\r\nHost: localhost\r\n\r\n")
            wait_for_held_read(started, b"D")
            path.write_bytes(b"")
            assert os.write(permit, b"R") == 1
            _, status, headers, body = read_response(client)
            assert status == 200 and int(headers[b"content-length"]) == size and body == b""
        wait_for_file_release(process, path, 2)
        assert_response(exchange(port, b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                        200, b"<h1>Kable</h1>\n")
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()


def holds_file(process, path):
    for descriptor in Path(f"/proc/{process.pid}/fd").iterdir():
        try:
            if descriptor.readlink() == path:
                return True
        except FileNotFoundError:
            pass
    return False


def wait_for_file_release(process, path, timeout):
    deadline = time.monotonic() + timeout
    while holds_file(process, path):
        assert time.monotonic() < deadline, "stream kept the file open"
        time.sleep(0.01)


def check_timeout(port):
    with connect(port) as stalled, connect(port) as body:
        stalled.sendall(b"GET /health HTTP/1.1\r\nHost: local")
        body.sendall(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\na")
        assert not select.select([stalled, body], [], [], 5)[0], "request expired too early"
        stalled.sendall(b"host\r\n")
        body.sendall(b"b")
        assert select.select([stalled], [], [], 7)[0], "another read reset the request deadline"
        assert_response(read_response(stalled), 408, b"Request Timeout\n")
        assert select.select([body], [], [], 2)[0], "a body fragment reset the request deadline"
        assert_response(read_response(body), 408, b"Request Timeout\n")


def check_thunk_message(binary):
    message = b"hello\n\nworld\n.\n"
    cases = (
        ([], message, 0, b"Message (13 bytes)\nhello\n\nworld\n", b""),
        (["--check"], message, 0, b"", b""),
        ([], b".", 0, b"Message (0 bytes)\n", b""),
        ([], b"unfinished\n", 1, b"", b"thunk-message: expected a line containing only '.'\n"),
    )
    for args, data, status, stdout, stderr in cases:
        result = subprocess.run([binary, *args], input=data, capture_output=True, timeout=5)
        assert (result.returncode, result.stdout, result.stderr) == (status, stdout, stderr), result
    print("Kable thunk message checks passed")


def check_async_read(command, directory):
    first = directory / "first.bin"
    second = directory / "a second file.bin"
    empty = directory / "empty"
    oversized = directory / "oversized"
    fifo = directory / "fifo"
    payload = bytes(range(256)) * 256
    first.write_bytes(payload)
    second.write_bytes(b"Kable\x00file read\n")
    empty.touch()
    oversized.write_bytes(payload + b"!")
    os.mkfifo(fifo)

    def run(*args):
        result = subprocess.run([*command, *args], capture_output=True, timeout=10)
        assert result.returncode in (0, 1), result
        return result

    result = run(first, second)
    assert result.returncode == 0 and result.stdout == payload + second.read_bytes(), result.stderr
    result = run(empty, empty)
    assert result.returncode == 0 and result.stdout == b"", result.stderr
    failures = [((oversized, second), b"first file failed"),
                ((first, directory / "missing"), b"second file failed"),
                ((fifo, second), b"first file failed"),
                ((first, second, "0"), b"deadline expired")]
    for args, expected in failures:
        result = run(*args)
        assert result.returncode == 1 and result.stdout == b"" and expected in result.stderr, result

    library = build_file_gate(directory)
    growing = directory / "example.txt"
    too_large = f"async-read: first file failed (operation error {-errno.EFBIG})\n".encode()
    growth_cases = (
        (payload[:6], payload[6:16 * 1024 + 1], 0),
        (payload[:6], payload[6:], 0),
        (payload, b"!", 1),
    )
    for prefix, tail, expected in growth_cases:
        growing.write_bytes(prefix)
        with gated_file_process([*command, str(growing), str(empty)], directory, library) as (
            process, started, permit
        ):
            wait_for_held_read(started)
            with growing.open("ab") as output:
                output.write(tail)
            assert os.write(permit, b"R") == 1
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == expected, stderr
            if expected == 0:
                assert stdout == prefix + tail, stderr
            else:
                assert stdout == b"" and stderr == too_large, stderr
    print("Kable async file checks passed")


def check_file_copy(command, directory):
    library = directory / "short-file-write.so"
    subprocess.run(["cc", "-shared", "-fPIC", str(ROOT / "tests/short_file_write.c"),
                    "-ldl", "-pthread", "-o", str(library)], check=True)
    env = os.environ | {"LD_PRELOAD": str(library)}
    source = directory / "copy source"
    destination = directory / "copy destination"
    payload = bytes(range(253)) * 570 + b"last chunk"
    source.write_bytes(payload)

    def copy(source, destination, *args, env=env):
        return subprocess.run([*command, str(source), str(destination), *args],
                              env=env, capture_output=True, timeout=10)

    result = copy(source, destination)
    assert result.returncode == 0 and destination.read_bytes() == payload, result.stderr
    result = copy(source, source)
    assert result.returncode == 1 and source.read_bytes() == payload, result.stderr
    empty = directory / "empty source"
    empty.touch()
    empty_copy = directory / "empty copy"
    result = copy(empty, empty_copy)
    assert result.returncode == 0 and empty_copy.read_bytes() == b"", result.stderr

    failed = directory / "failed copy"
    result = copy(source, failed, env=env | {"KABLE_TEST_WRITE_FAIL": "1"})
    assert result.returncode == 1 and b"write failed after 4093 bytes" in result.stderr, result.stderr
    assert failed.read_bytes() == payload[:4093]

    cancelled = directory / "cancelled copy"
    result = copy(source, cancelled, "0")
    assert result.returncode == 1 and b"write timed out after 0 bytes" in result.stderr, result.stderr
    assert cancelled.read_bytes() == b""

    started, notify = os.pipe()
    release, permit = os.pipe()
    gated_env = env | {"KABLE_TEST_WRITE_STARTED": str(notify), "KABLE_TEST_WRITE_RELEASE": str(release)}
    partial = directory / "partial copy"
    try:
        with subprocess.Popen([*command, str(source), str(partial), "500"], env=gated_env,
                              pass_fds=(notify, release), stderr=subprocess.PIPE) as process:
            try:
                wait_for_held_read(started, b"W")
                time.sleep(0.75)
                assert process.poll() is None, "copy exited before the write worker joined"
                assert partial.read_bytes() == payload[:4093]
                assert os.write(permit, b"R") == 1
                assert process.wait(timeout=5) == 1
                assert b"write timed out after 4093 bytes" in process.stderr.read()
                assert partial.read_bytes() == payload[:4093]
            finally:
                if process.poll() is None:
                    process.kill()
    finally:
        for fd in (started, notify, release, permit):
            os.close(fd)

    truncated = directory / "large.bin"
    truncated.write_bytes(payload)
    truncated_copy = directory / "truncated copy"
    read_gate = build_file_gate(directory)
    with gated_file_process([*command, str(truncated), str(truncated_copy)], directory, read_gate,
                            gates=8) as (process, started, permit):
        wait_for_held_read(started, b"D")
        prefix = payload[:4096]
        truncated.write_bytes(prefix)
        assert os.write(permit, b"R") == 1
        assert process.wait(timeout=5) == 1
        assert b"file-copy: source ended early" in process.stderr.read()
        assert truncated_copy.read_bytes() == prefix
    print("Kable scoped file copy checks passed")


@contextmanager
def gated_file_process(command, directory, library, gates=1, reset_notify=None):
    started, notify = os.pipe()
    release, permit = os.pipe()
    env = os.environ | {
        "LD_PRELOAD": str(library),
        "KABLE_TEST_FILE_STARTED": str(notify),
        "KABLE_TEST_FILE_RELEASE": str(release),
        "KABLE_TEST_FILE_GATES": str(gates),
        "UV_THREADPOOL_SIZE": "2",
    }
    fds = (notify, release)
    if reset_notify is not None:
        env["KABLE_TEST_TCP_RESET"] = str(reset_notify)
        fds += (reset_notify,)
    try:
        with subprocess.Popen(command, cwd=directory, env=env, pass_fds=fds,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0) as process:
            try:
                yield process, started, permit
            finally:
                if process.poll() is None:
                    process.kill()
    finally:
        for fd in (started, notify, release, permit):
            os.close(fd)


@contextmanager
def gated_file_server(command, directory, library, gates=1, reset_notify=None):
    with gated_file_process(command, directory, library, gates, reset_notify) as (process, started, permit):
        port = listening_port(process, b"Listening on http://127.0.0.1:")
        yield process, port, started, permit


def check_timed_file_read(command, directory):
    root = directory / "timed read"
    root.mkdir()
    payload = b"file position survives the deadline\x00\n"
    (root / "large.bin").write_bytes(payload)
    library = build_file_gate(directory)
    with gated_file_process([*command, "--timed-read"], root, library, gates=8) as (process, started, permit):
        wait_for_held_read(started, b"D")
        assert not select.select([process.stdout], [], [], 0.75)[0], "timed read returned before the worker joined"
        assert os.write(permit, b"R") == 1
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr
        assert stdout == b"timed out\n" + payload, (stdout, stderr)
    print("Kable timed file read and retry checks passed")


def wait_for_held_read(started, expected=b"A"):
    assert select.select([started], [], [], 5)[0], "file worker did not start"
    token = os.read(started, 1)
    if expected is not None:
        assert token == expected
    return token


def start_held_read(client, request, started):
    client.sendall(request)
    wait_for_held_read(started)


def wait_for_listener_shutdown(port):
    deadline = time.monotonic() + 5
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                pass
        except ConnectionRefusedError:
            return
        except ConnectionResetError:
            # A connection queued just before close can be reset first.
            pass
        assert time.monotonic() < deadline, "shutdown kept accepting connections"
        time.sleep(0.01)


def build_file_gate(directory):
    library = directory / "hold-file-read.so"
    subprocess.run(["cc", "-shared", "-fPIC", str(ROOT / "tests/hold_file_read.c"),
                    "-ldl", "-pthread", "-o", str(library)], check=True)
    return library


def check_http_file(command, directory, check_request_timeout=False):
    library = build_file_gate(directory)
    payload = b"Kable\x00hosted file read\n"
    document = directory / "example.txt"
    document.write_bytes(payload)
    request = b"GET /file HTTP/1.1\r\nHost: localhost\r\n\r\n"
    timed = b"GET /file-with-timeout HTTP/1.1\r\nHost: localhost\r\n\r\n"

    with gated_file_server(command, directory, library) as (process, port, started, permit):
        with connect(port) as client:
            start_held_read(client, request, started)
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert not select.select([client], [], [], 0)[0], "file reply arrived before the read"
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 200, payload)
        head = b"HEAD /file HTTP/1.1\r\nHost: localhost\r\n\r\n"
        assert_response(exchange(port, head), 200, payload, head=True)
        timed_with_query = timed.replace(b"/file-with-timeout ", b"/file-with-timeout?download=1 ")
        assert_response(exchange(port, timed_with_query), 200, payload)
        assert_response(exchange(port, timed.replace(b"GET ", b"POST ", 1)),
                        405, b"Method Not Allowed\n")
        document.unlink()
        assert_response(exchange(port, timed), 500, b"Internal Server Error\n")
        document.write_bytes(payload)
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()

    with gated_file_server(command, directory, library) as (process, port, started, permit):
        with connect(port) as client:
            start_held_read(client, timed, started)
            assert not select.select([client], [], [], 1.25)[0], "timeout replied before the worker joined"
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 504, b"Gateway Timeout\n")
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()

    for pending, cancellation_logs in ((request, 1), (timed, 0)):
        with gated_file_server(command, directory, library) as (process, port, started, permit):
            with connect(port) as client:
                start_held_read(client, pending, started)
                process.terminate()
                wait_for_listener_shutdown(port)
                assert process.poll() is None, "shutdown returned before the worker joined"
                assert not select.select([client], [], [], 0)[0], "request finished before the worker"
                assert os.write(permit, b"R") == 1
                assert client.recv(1) == b"", "shutdown sent a cancelled file response"
                assert process.wait(timeout=5) == 0, process.stderr.read()
                assert process.stderr.read().count(b"File request cancelled.") == cancellation_logs

    if check_request_timeout:
        with gated_file_server(command, directory, library) as (process, port, started, permit):
            with connect(port) as client:
                start_held_read(client, request, started)
                assert not select.select([client], [], [], 10.25)[0], "deadline returned before the worker joined"
                assert_response(exchange(port, HEALTH), 200, b"ok\n")
                assert os.write(permit, b"R") == 1
                assert client.recv(1) == b"", "deadline sent a cancelled file response"
                read_log_until(process.stderr, b"File request cancelled.")
            process.terminate()
            assert process.wait(timeout=5) == 0, process.stderr.read()
    check_http_close(command, directory, library)
    check_http_join(command, directory, library, payload)
    check_http_sequence(command, directory, library, payload, check_request_timeout)
    check_http_reset(command, directory, library)
    print("Kable HTTP file checks passed")


def check_http_close(command, directory, library):
    with gated_file_server(command, directory, library) as (process, port, started, permit):
        with connect(port) as client:
            client.sendall(b"GET /close HTTP/1.1\r\nHost: localhost\r\n\r\n")
            assert client.recv(1) == b"", "immediate close sent an HTTP response"
        with connect(port) as client:
            request = b"GET /close-after-read HTTP/1.1\r\nHost: localhost\r\n\r\n"
            start_held_read(client, request, started)
            assert not select.select([client], [], [], 0.05)[0], "deferred close ran before the read joined"
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert os.write(permit, b"R") == 1
            assert client.recv(1) == b"", "deferred close sent an HTTP response"
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()


def check_http_join(command, directory, library, payload):
    suffix = b"\x00joined footer\n"
    files = (directory / "example.txt", directory / "footer.txt")
    files[1].write_bytes(suffix)
    request = b"GET /file-join HTTP/1.1\r\nHost: localhost\r\n\r\n"

    def start_both_reads(client, started):
        client.sendall(request)
        workers = {wait_for_held_read(started, None) for _ in range(2)}
        assert workers == {b"A", b"B"}, workers

    with gated_file_server(command, directory, library, gates=3) as (process, port, started, permit):
        with connect(port) as client:
            start_both_reads(client, started)
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert os.write(permit, b"R") == 1
            assert not select.select([client], [], [], 0.05)[0], "join replied after only one read"
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 200, payload + suffix)
        for path in files:
            data = path.read_bytes()
            path.unlink()
            assert_response(exchange(port, request), 500, b"Internal Server Error\n")
            path.write_bytes(data)
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()

    with gated_file_server(command, directory, library, gates=3) as (process, port, started, permit):
        with connect(port) as client:
            start_both_reads(client, started)
            assert os.write(permit, b"R") == 1
            assert not select.select([client], [], [], 1.25)[0], "deadline replied before both reads joined"
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 504, b"Gateway Timeout\n")
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()


def check_http_sequence(command, directory, library, payload, check_request_timeout):
    suffix = b"\x00second read\n"
    document = directory / "example.txt"
    footer = directory / "footer.txt"
    footer.write_bytes(suffix)
    detailed = b"\x00detailed footer\n"
    detailed_payload = b"footer: detailed\n" + payload
    (directory / "footer-detailed.txt").write_bytes(detailed)
    request = b"GET /file-sequence HTTP/1.1\r\nHost: localhost\r\n\r\n"
    first_cancelled = b"Sequence cancelled before the second read."
    second_cancelled = f"Sequence cancelled with {len(payload)} bytes from the first read.".encode()

    with gated_file_server(command, directory, library, gates=7) as (process, port, started, permit):
        document.unlink()
        with connect(port) as client:
            client.sendall(request)
            ready, _, _ = select.select([client, started], [], [], 5)
            assert ready == [client], "failed first read started a footer"
            assert_response(read_response(client), 500, b"Internal Server Error\n")
            assert not select.select([started], [], [], 0)[0], "failed first read started a footer"
        document.write_bytes(payload)

        with connect(port) as client:
            start_held_read(client, request, started)
            client.shutdown(socket.SHUT_WR)
            assert not select.select([started], [], [], 0.05)[0], "second read started before the first joined"
            assert os.write(permit, b"R") == 1
            wait_for_held_read(started, b"B")
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 200, payload + suffix)
        head = b"HEAD /file-sequence HTTP/1.1\r\nHost: localhost\r\n\r\n"
        assert_response(exchange(port, head), 200, payload + suffix, head=True)
        legacy_head = head.replace(b"HTTP/1.1", b"HTTP/1.0")
        assert_response(exchange(port, legacy_head), 200, payload + suffix, head=True, version=b"HTTP/1.0")
        document.write_bytes(detailed_payload)
        with connect(port) as client:
            client.sendall(request)
            wait_for_held_read(started, b"C")
            assert os.write(permit, b"R") == 1
            assert_response(read_response(client), 200, detailed_payload + detailed)
        document.write_bytes(payload)
        footer.unlink()
        assert_response(exchange(port, request), 500, b"Internal Server Error\n")
        footer.write_bytes(suffix)
        process.terminate()
        assert process.wait(timeout=5) == 0, process.stderr.read()

    for second, expected in ((False, first_cancelled), (True, second_cancelled)):
        document.write_bytes(payload if second else detailed_payload)
        with gated_file_server(command, directory, library, gates=7) as (process, port, started, permit):
            with connect(port) as client:
                start_held_read(client, request, started)
                if second:
                    assert os.write(permit, b"R") == 1
                    wait_for_held_read(started, b"B")
                process.terminate()
                wait_for_listener_shutdown(port)
                assert process.poll() is None, "shutdown returned before the active stage joined"
                assert not select.select([client], [], [], 0)[0], "request finished before the active stage"
                assert os.write(permit, b"R") == 1
                assert client.recv(1) == b"", "shutdown sent a cancelled sequence response"
                assert process.wait(timeout=5) == 0, process.stderr.read()
                assert process.stderr.read().count(expected) == 1
                assert not select.select([started], [], [], 0)[0], "shutdown started another stage"

    if check_request_timeout:
        with gated_file_server(command, directory, library, gates=3) as (process, port, started, permit):
            with connect(port) as client:
                start_held_read(client, request, started)
                assert not select.select([client], [], [], 5.25)[0]
                assert os.write(permit, b"R") == 1
                wait_for_held_read(started, b"B")
                assert not select.select([client], [], [], 5.25)[0], "deadline returned before the second read joined"
                assert os.write(permit, b"R") == 1
                assert client.recv(1) == b"", "the second read reset the request deadline"
                read_log_until(process.stderr, second_cancelled)
            process.terminate()
            assert process.wait(timeout=5) == 0, process.stderr.read()


def check_http_reset(command, directory, library):
    observed, notify = os.pipe()
    try:
        with gated_file_server(command, directory, library, gates=3,
                               reset_notify=notify) as (process, port, started, permit):
            request = b"GET /file-sequence HTTP/1.1\r\nHost: localhost\r\n\r\n"
            with connect(port) as client:
                start_held_read(client, request, started)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            assert select.select([observed], [], [], 5)[0], "listener did not observe the TCP reset"
            assert os.read(observed, 1) == b"R"
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            cancelled = b"Sequence cancelled before the second read."
            logs = bytearray()
            if select.select([process.stderr], [], [], 0)[0]:
                logs.extend(os.read(process.stderr.fileno(), 65536))
            assert cancelled not in logs, "cancellation cleanup ran before the worker returned"
            assert os.write(permit, b"R") == 1
            logs.extend(read_log_until(process.stderr, cancelled))
            assert not select.select([started], [], [], 0)[0], "reset started another stage"
            assert_response(exchange(port, HEALTH), 200, b"ok\n")
            process.terminate()
            assert process.wait(timeout=5) == 0, process.stderr.read()
            logs.extend(process.stderr.read())
            assert logs.count(cancelled) == 1, logs
    finally:
        os.close(observed)
        os.close(notify)


def assert_received(client, data):
    received = bytearray()
    while len(received) < len(data):
        chunk = client.recv(len(data) - len(received))
        assert chunk, "connection closed before the full echo"
        received.extend(chunk)
    assert received == data


def assert_echo(client, data):
    client.sendall(data)
    assert_received(client, data)


def listening_port(process, prefix):
    ready, _, _ = select.select([process.stdout], [], [], 15)
    assert ready, "server did not announce its port"
    address = process.stdout.readline()
    assert address.startswith(prefix), address
    return int(address.rsplit(b":", 1)[1])


def check_tcp_echo(command):
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        try:
            port = listening_port(process, b"Listening on 127.0.0.1:")

            with connect(port) as stalled, connect(port) as active:
                assert not select.select([stalled], [], [], 0.05)[0], "idle read closed immediately"
                assert_echo(active, bytes(range(256)) * 257)
                assert not select.select([stalled], [], [], 0)[0], "stalled read blocked the echo"
                assert_echo(active, b"another read on the same connection")
                active.shutdown(socket.SHUT_WR)
                assert active.recv(1) == b""
                assert stalled.recv(1) == b"", "idle deadline did not close the echo connection"

            with connect(port) as first, connect(port) as second:
                assert_echo(first, b"first")
                assert_echo(second, b"second")
                assert not select.select([first, second], [], [], 0.05)[0]
                process.terminate()
                assert first.recv(1) == second.recv(1) == b""
                assert process.wait(timeout=5) == 0
                output = process.stderr.read()
                assert output.count(b"Read deadline expired.") == 1, output
                assert output.count(b"Cancelled connection on shutdown.") == 2, output
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable TCP echo checks passed")


def read_log_until(stream, message):
    logs = bytearray()
    while message not in logs:
        assert select.select([stream], [], [], 5)[0], logs
        line = stream.readline()
        assert line, logs
        logs.extend(line)
    return logs


def check_tcp_write(command):
    @contextmanager
    def pending_write(port):
        with socket.socket() as client:
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            client.settimeout(5)
            client.connect(("127.0.0.1", port))
            client.shutdown(socket.SHUT_WR)
            assert client.recv(1) == b"\0", "scoped write did not start after read EOF"
            yield client

    cases = (
        ("write", [], b"write timeout: cancelled\n", b"write completed: failed\n", b"write shutdown: cancelled\n"),
        ("send", ["--send"], b"send timed out\n", b"send failed\n", b"send cancelled\n"),
    )
    for mode, args, timed_out, failed, cancelled in cases:
        with subprocess.Popen([*command, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              bufsize=0) as process:
            try:
                port = listening_port(process, b"Listening on 127.0.0.1:")
                with pending_write(port):
                    read_log_until(process.stderr, timed_out)
                with pending_write(port) as client:
                    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                read_log_until(process.stderr, failed)
                with pending_write(port):
                    process.terminate()
                    read_log_until(process.stderr, cancelled)
                    assert process.wait(timeout=5) == 0, process.stderr.read()
            finally:
                if process.poll() is None:
                    process.kill()
        print(f"Kable scoped TCP {mode} checks passed")


def check_tcp_session(command):
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          bufsize=0) as process:
        try:
            port = listening_port(process, b"Listening on 127.0.0.1:")
            with connect(port) as batched:
                batched.sendall(b"one\ntwo\nleft")
                assert_received(batched, b"one\ntwo\n")
                batched.sendall(b"over\nunfinished")
                assert_received(batched, b"leftover\n")
                batched.shutdown(socket.SHUT_WR)
                assert batched.recv(1) == b""
                read_log_until(process.stderr, b"Closed session with 10 buffered bytes.")

            with connect(port) as overlong:
                overlong.sendall(b"x" * 16385)
                assert overlong.recv(1) == b""
                read_log_until(process.stderr, b"Closed session with 16385 buffered bytes.")

            with connect(port) as idle:
                idle.sendall(b"retry limit")
                assert idle.recv(1) == b""
                logs = read_log_until(process.stderr, b"Closed session with 11 buffered bytes.")
                assert logs.count(b"Retrying idle read.") == 1, logs

            with connect(port) as first, connect(port) as second:
                first.sendall(b"first fragment ")
                assert not select.select([first], [], [], 0.05)[0]
                assert_echo(second, b"independent\n")
                second.shutdown(socket.SHUT_WR)
                assert second.recv(1) == b""

                logs = read_log_until(process.stderr, b"Retrying idle read.")
                first.sendall(b"continued\nleft")
                assert_received(first, b"first fragment continued\n")
                logs.extend(read_log_until(process.stderr, b"Retrying idle read."))
                process.terminate()
                assert first.recv(1) == b""
                assert process.wait(timeout=5) == 0
                logs.extend(process.stderr.read())
                assert logs.count(b"Discarded a read for a closing session.") == 1, logs
                assert logs.count(b"Closed session with 0 buffered bytes.") == 1, logs
                assert logs.count(b"Closed session with 4 buffered bytes.") == 1, logs
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable stateful TCP checks passed")


def run_checks(command, check_request_timeout):
    with subprocess.Popen([*command, "0"], stdout=subprocess.PIPE) as process:
        try:
            port = listening_port(process, b"Listening on http://127.0.0.1:")
            conflict = subprocess.run([*command, str(port)], capture_output=True, timeout=15)
            assert conflict.returncode == 1 and b"cannot listen" in conflict.stderr, conflict
            check_http(port)
            check_delayed_handler(port)
            if check_request_timeout:
                check_timeout(port)
            check_concurrency(port, process)
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable loopback checks passed")


def check_http_streams(binary):
    with subprocess.Popen([binary], stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0) as process:
        try:
            port = listening_port(process, b"Listening on http://127.0.0.1:")

            def request(path, method=b"GET"):
                return exchange(port, method + b" " + path + b" HTTP/1.1\r\nHost: localhost\r\n\r\n")

            headers = assert_response(request(b"/memory"), 200, b"abcdef")
            assert headers[b"content-type"] == b"application/octet-stream"
            assert_response(request(b"/head", b"HEAD"), 200, b"abcdef", head=True)
            assert_response(request(b"/empty"), 200, b"")
            for path, length, body in ((b"/short", 7, b"abcdef"), (b"/over", 5, b"abc")):
                version, status, headers, sent = request(path)
                assert (version, status, int(headers[b"content-length"]), sent) == (
                    b"HTTP/1.1", 200, length, body
                )

            headers = b"POST /pull HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
            with connect(port) as client:
                client.sendall(headers + b"2\r")
                assert not select.select([client, process.stderr], [], [], 0.05)[0]
                client.sendall(b"\na\x00\r\n")
                read_log_until(process.stderr, b"Received body chunk.")
                client.sendall(b"1\r\nb\r\n0\r\n")
                read_log_until(process.stderr, b"Received body chunk.")
                assert not select.select([client], [], [], 0.05)[0]
                client.sendall(b"\r\n")
                assert_response(read_response(client), 200, b"2:a\x00b")

            with connect(port) as client:
                client.sendall(headers + b"1\r\na\r\n")
                read_log_until(process.stderr, b"Received body chunk.")
                process.terminate()
                assert client.recv(1) == b"", "shutdown resumed a pending body pull"
                assert process.wait(timeout=5) == 0
                assert process.stderr.read() == b"Discarded pending body pull.\n"
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable HTTP stream checks passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-timeout", action="store_true", help="also run the longer deadline checks")
    example_checks = parser.add_mutually_exclusive_group()
    example_checks.add_argument("--tcp-echo", action="store_true", help="run echo checks against the given command")
    example_checks.add_argument("--tcp-session", action="store_true", help="run stateful TCP checks against the given command")
    example_checks.add_argument("--async-read", action="store_true", help="run file checks against the given command")
    example_checks.add_argument("--http-file", action="store_true", help="run hosted file checks against the given command")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="use a prebuilt server with an optional runner")
    args = parser.parse_args()
    if args.async_read or args.http_file:
        if not args.command:
            parser.error("file checks require an example command")
        command = [str(Path(arg).resolve()) if Path(arg).is_file() else arg for arg in args.command]
        with tempfile.TemporaryDirectory() as directory:
            if args.http_file:
                check_http_file(command, Path(directory), args.check_timeout)
            else:
                check_async_read(command, Path(directory))
    elif args.tcp_echo or args.tcp_session:
        if not args.command:
            parser.error("TCP checks require a server command")
        check = check_tcp_echo if args.tcp_echo else check_tcp_session
        check(args.command)
    elif args.command:
        run_checks(args.command, args.check_timeout)
    else:
        with tempfile.TemporaryDirectory() as directory:
            check_http_parser(Path(directory))
            binary = build("kable", Path(directory) / "kable")
            hello = build("examples/hello-server", Path(directory) / "hello-server")
            static_files = build("examples/static-files", Path(directory) / "static-files")
            thunk = build("tests/thunk", Path(directory) / "thunk")
            subprocess.run([thunk], check=True, timeout=5)
            check_thunk_message(build("examples/thunk-message", Path(directory) / "thunk-message"))
            check_timer_engines(Path(directory))
            async_read = build("examples/async-read", Path(directory) / "async-read")
            file_copy = build("examples/file-copy", Path(directory) / "file-copy")
            file_position = build("tests/file-position", Path(directory) / "file-position")
            http_file = build("examples/http-file", Path(directory) / "http-file")
            tcp_echo = build("examples/tcp-echo", Path(directory) / "tcp-echo")
            check_tcp_api_boundary(Path(directory))
            tcp_write = build("tests/tcp-write", Path(directory) / "tcp-write")
            tcp_session = build("examples/tcp-session", Path(directory) / "tcp-session")
            check_hello_server(hello)
            check_http_streams(build("tests/http-streams", Path(directory) / "http-streams"))
            check_static_files([str(static_files)], Path(directory), args.check_timeout)
            check_async_read([str(async_read)], Path(directory))
            check_file_copy([str(file_copy)], Path(directory))
            subprocess.run([file_position], cwd=directory, check=True, timeout=10)
            assert (Path(directory) / "mixed-writes").read_bytes() == b"aXcd"
            check_timed_file_read([str(file_position)], Path(directory))
            check_http_file([str(http_file)], Path(directory), args.check_timeout)
            check_tcp_echo([str(tcp_echo)])
            check_tcp_write([str(tcp_write)])
            check_tcp_session([str(tcp_session)])
            run_checks([str(binary)], args.check_timeout)


def build(package, binary):
    subprocess.run(
        [str(ROOT / "../kursal/bin/kursal"), "build", package, "-o", str(binary)],
        cwd=ROOT,
        check=True,
    )
    return binary


def check_tcp_api_boundary(directory):
    package = directory / "tcp-api-source"
    package.mkdir()
    (package / "kur.toml").write_text(f"""
[package]
name="tcp_api_test"
source_root="."
kind="executable"
entry="main"

[dependencies.tcp]
path="{ROOT / 'lib/tcp'}"

[dependencies.std]
path="{(ROOT / '../kursal/lib/std').resolve()}"

[dependencies.task]
path="{ROOT / 'lib/task'}"
""")
    (package / "lib.kur").write_text("""
use std::buffer::Buffer!;
use task::{Discard, PlanOutput, Stream, StreamOutcome};
use tcp::{Action!, Connection!, OperationPlan!};

fn receive(connection: Connection!) -> Action! {
    let read: OperationPlan![Connection!, (Connection!, StreamOutcome[Buffer!])] = connection.next();
    read.defer_tcp(|result| {
        let (connection, outcome) = result;
        outcome.discard();
        connection.close()
    })
}

fn main() -> i32 {
    let handler = receive;
    0
}
""")
    build(str(package), directory / "tcp-api")

    probes = [
        ("""
use tcp::Connection!;

fn from_raw(raw: *u8) -> Connection! { Connection!.{ raw } }

fn main() -> i32 {
    let constructor = from_raw;
    0
}
""", "the shape of tcp::Connection! is not exposed here"),
    ]
    for path, member in (("tcp::prepare_read", "prepare_read"), ("tcp::operation::prepare_read", "operation")):
        probes.append((f"""
use {path};

fn main() -> i32 {{
    let prepare = prepare_read;
    0
}}
""", f"unknown or private member {member} of tcp"))

    for source, diagnostic in probes:
        (package / "lib.kur").write_text(source)
        result = subprocess.run(
            [str(ROOT / "../kursal/bin/kursal"), "build", str(package), "-o", str(directory / "tcp-api")],
            cwd=ROOT, capture_output=True, text=True,
        )
        assert result.returncode != 0 and diagnostic in result.stderr, result
    print("Kable TCP API visibility checks passed")


def build_virtual(package, directory):
    name = Path(package).name
    virtual_package = directory / f"virtual-{name}-source"
    virtual_package.mkdir()
    (virtual_package / "lib.kur").write_bytes((ROOT / package / "lib.kur").read_bytes())
    (virtual_package / "kur.toml").write_text(f"""
[package]
name="virtual_task_test"
source_root="."
kind="executable"
entry="main"

[dependencies.std]
path="{(ROOT / '../kursal/lib/std').resolve()}"

[dependencies.async]
path="{ROOT / 'lib/virtual_clock'}"

[dependencies.task]
path="{ROOT / 'lib/task'}"
""")
    return build(str(virtual_package), directory / f"virtual-{name}")


def check_virtual_file_rejected(directory):
    package = directory / "virtual-file-source"
    package.mkdir()
    (package / "kur.toml").write_text(f"""
[package]
name="virtual_file_test"
source_root="."
kind="executable"
entry="main"

[dependencies.std]
path="{(ROOT / '../kursal/lib/std').resolve()}"

[dependencies.async]
path="{ROOT / 'lib/async'}"

[dependencies.task]
path="{ROOT / 'lib/task'}"

[dependencies.virtual_clock]
path="{ROOT / 'lib/virtual_clock'}"
""")
    (package / "lib.kur").write_text("""
use async::read_file;
use std::slice::StaticRead;
use task::PlanOutput;

fn main() {
    let access = StaticRead::new();
    let file = read_file(&access, "README.md", 65_536usize).map(|outcome| outcome);
    let result = virtual_clock::run(file);
}
""")
    result = subprocess.run(
        [str(ROOT / "../kursal/bin/kursal"), "build", str(package), "-o", str(directory / "virtual-file")],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, result
    assert "no instance of" in result.stderr and "Plan[FilePathPlan![Buffer!], Virtual!, Completion[Buffer!]]" in result.stderr, result


def check_http_parser(directory):
    binary = directory / "http-parser"
    subprocess.run([
        "cc", str(ROOT / "tests/http_parser.c"),
        str(ROOT / "lib/server/native/request.c"), "-lllhttp", "-o", str(binary),
    ], check=True)
    subprocess.run([str(binary)], check=True, timeout=5)
    print("Kable HTTP parser memory checks passed")


def check_native_registration(directory):
    native = ROOT / "lib/async/native"
    binary = directory / "native-registration"
    subprocess.run([
        "cc", str(ROOT / "tests/native_registration.c"),
        str(native / "timer.c"), "-Wl,--wrap=realloc",
        "-luv", "-o", str(binary),
    ], check=True)
    subprocess.run([str(binary)], check=True, timeout=5)
    print("Kable native registration fault checks passed")


def check_timer_engines(directory):
    check_native_registration(directory)

    def check(binary):
        result = subprocess.run([str(binary)], capture_output=True, timeout=5)
        assert result.returncode == 0, (binary, result.returncode, result.stdout, result.stderr)

    check(build("tests/task-scope", directory / "task-scope"))
    check(build("tests/task-collection", directory / "task-collection"))
    check(build_virtual("tests/task-collection", directory))
    check(build("tests/hosted-plan", directory / "hosted-plan"))
    check(build("tests/virtual-clock", directory / "virtual-clock"))
    check_virtual_file_rejected(directory)
    print("Kable task driver contract checks passed")

    for name in ("timer-race", "task-chain"):
        package = f"examples/{name}"
        check(build(package, directory / name))
        check(build_virtual(package, directory))
    print("Kable timer and sequence examples passed on both drivers")


if __name__ == "__main__":
    main()
