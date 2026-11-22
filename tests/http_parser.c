#include "../lib/server/native/request.h"

#include <assert.h>
#include <string.h>

static uint16_t validate_head(uint8_t major, uint8_t minor, uint32_t hosts, uint8_t upgrade,
    uint8_t expect, uint64_t content_length, uint64_t body_limit, uint8_t *head,
    size_t head_len, size_t target_len) {
    assert(major == 1 && minor == 1 && hosts == 1 && !upgrade && !expect);
    assert(content_length == 0 && body_limit == 1024 * 1024);
    assert(target_len == 5 && head_len == sizeof "/echolocalhost" - 1);
    assert(memcmp(head, "/echolocalhost", head_len) == 0);
    return 0;
}

int main(void) {
    static const uint8_t wire[] =
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
        "2\r\na\0\r\n1\r\nb\r\n0\r\n\r\n";
    KableRequest request;
    kable_request_init(&request, validate_head);
    assert(kable_request_eof(&request) == REQUEST_CLOSED);
    assert(kable_request_feed(&request, wire, 8) == REQUEST_MORE);
    assert(kable_request_eof(&request) == REQUEST_TRUNCATED);
    kable_release_request(&request);

    kable_request_init(&request, validate_head);
    size_t length = sizeof wire - 1;
    int saw_headers = 0;
    for (size_t index = 0; index < length; index++) {
        uint8_t fragment = wire[index];
        uint16_t result = kable_request_feed(&request, &fragment, 1);
        if (result == REQUEST_HEADERS) {
            assert(!saw_headers && request.body.len == 0);
            saw_headers = 1;
            result = kable_request_resume(&request);
        }
        assert(result == (index + 1 < length ? REQUEST_MORE : REQUEST_COMPLETE));
    }
    assert(saw_headers);

    uint8_t http10;
    uint8_t *target;
    size_t target_len;
    assert(kable_request_view(&request, &http10, &target, &target_len) == METHOD_POST);
    assert(!http10 && target_len == 5 && memcmp(target, "/echo", target_len) == 0);
    assert(request.body.len == 3 && memcmp(request.body.data, "a\0b", 3) == 0);
    kable_release_request(&request);

    kable_request_init(&request, validate_head);
    assert(kable_request_feed(&request, wire, length) == REQUEST_HEADERS);
    assert(request.body.len == 0 && request.pending.len > 0);
    assert(kable_request_resume(&request) == REQUEST_COMPLETE);
    assert(request.body.len == 3 && memcmp(request.body.data, "a\0b", 3) == 0);
    kable_release_request(&request);
}
