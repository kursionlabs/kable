"""Exercise Kable's HTTP handling and shutdown over real TCP connections."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import select
import socket
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def connect(port):
    return socket.create_connection(("127.0.0.1", port), timeout=15)


def read_response(client):
    with client.makefile("rb") as stream:
        version, status, _ = stream.readline().split(b" ", 2)
        headers = {}
        for line in stream:
            if line == b"\r\n":
                break
            key, value = line.split(b":", 1)
            headers[key.lower()] = value.strip()
        body = stream.read()
    assert headers[b"connection"] == b"close", headers
    return version, int(status), headers, body


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

    assert_response(exchange(port, b"GET / HTTP/1.1\r\n\r\n"), 400, b"Bad Request\n")
    assert_response(exchange(port, b"GET / HTTP/1.1\r\n", host, host, b"\r\n"),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"GET / HTTP/1.1\r\nHost: two names\r\n\r\n"),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"),
                    400, b"Bad Request\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 2\r\n\r\na", eof=True), 400, b"Bad Request\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Content-Length: 1048577\r\n\r\n"), 413, b"Content Too Large\n")
    assert_response(exchange(port, b"POST /echo HTTP/1.1\r\n", host,
                             b"Expect: 100-continue\r\nContent-Length: 1\r\n\r\n"),
                    417, b"Expectation Failed\n")
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
            response = exchange(port, b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            assert_response(response, 200, b"ok\n")

        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(health, range(24)))

        process.terminate()
        assert partial.recv(1) == b""
        assert_response(read_response(output), 200, body)
        # The peer that never consumes its response must not hold up shutdown.
        assert process.wait(timeout=8) == 0


def check_timeout(port):
    with connect(port) as stalled:
        stalled.sendall(b"GET /health HTTP/1.1\r\nHost: local")
        assert_response(read_response(stalled), 408, b"Request Timeout\n")


def run_checks(command, check_request_timeout):
    with subprocess.Popen([*command, "0"], stdout=subprocess.PIPE) as process:
        try:
            ready, _, _ = select.select([process.stdout], [], [], 15)
            assert ready, "server did not announce its port"
            address = process.stdout.readline()
            assert address.startswith(b"Listening on http://127.0.0.1:"), address
            port = int(address.rsplit(b":", 1)[1])
            conflict = subprocess.run([*command, str(port)], capture_output=True, timeout=15)
            assert conflict.returncode == 1 and b"cannot listen" in conflict.stderr, conflict
            check_http(port)
            if check_request_timeout:
                check_timeout(port)
            check_concurrency(port, process)
        finally:
            if process.poll() is None:
                process.kill()
    print("Kable loopback checks passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-timeout", action="store_true", help="also wait for the request deadline")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="use a prebuilt server with an optional runner")
    args = parser.parse_args()
    if args.command:
        run_checks(args.command, args.check_timeout)
    else:
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "kable"
            subprocess.run(
                [str(ROOT / "../kursal/bin/kursal"), "build", "kable", "-o", str(binary)],
                cwd=ROOT,
                check=True,
            )
            run_checks([str(binary)], args.check_timeout)


if __name__ == "__main__":
    main()
