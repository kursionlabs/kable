#include "request.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

enum {
    MAX_HEAD = 16 * 1024,
    MAX_BODY = 1024 * 1024,
    MAX_WIRE = MAX_BODY + 4 * MAX_HEAD
};

static int append(KableRequestBytes *bytes, const char *src, size_t len, size_t limit) {
    if (len > limit - bytes->len) return ENOBUFS;
    size_t needed = bytes->len + len;
    if (needed > bytes->capacity) {
        size_t capacity = bytes->capacity ? 2 * bytes->capacity : 128;
        if (capacity < needed) capacity = needed;
        if (capacity > limit) capacity = limit;
        uint8_t *data = realloc(bytes->data, capacity);
        if (!data) return ENOMEM;
        bytes->data = data;
        bytes->capacity = capacity;
    }
    if (len) memcpy(bytes->data + bytes->len, src, len);
    bytes->len = needed;
    return 0;
}

static int append_head(KableRequest *request, const char *at, size_t len) {
    int status = append(&request->head, at, len, MAX_HEAD);
    if (status == 0) return 0;
    request->rejection = status == ENOBUFS ? 431 : 500;
    return HPE_USER;
}

static int on_url(llhttp_t *parser, const char *at, size_t len) {
    KableRequest *request = parser->data;
    int status = append_head(request, at, len);
    request->target_len = request->head.len;
    return status;
}

static int on_field(llhttp_t *parser, const char *at, size_t len) {
    KableRequest *request = parser->data;
    if (request->headers_done || request->field_len > sizeof request->field) return 0;
    if (len > sizeof request->field - request->field_len) {
        request->field_len = sizeof request->field + 1;
    } else {
        memcpy(request->field + request->field_len, at, len);
        request->field_len += len;
    }
    return 0;
}

static int field_is(const KableRequest *request, const char *name, size_t len) {
    return request->field_len == len && strncasecmp(request->field, name, len) == 0;
}

static int on_field_complete(llhttp_t *parser) {
    KableRequest *request = parser->data;
    if (request->headers_done) return 0;
    if (field_is(request, "host", 4)) request->header = HEADER_HOST;
    else if (field_is(request, "range", 5)) request->header = HEADER_RANGE;
    else if (field_is(request, "if-range", 8)) request->header = HEADER_IF_RANGE;
    else if (field_is(request, "expect", 6)) request->header = HEADER_EXPECT;
    else request->header = HEADER_OTHER;
    request->field_len = 0;
    return 0;
}

static int on_value(llhttp_t *parser, const char *at, size_t len) {
    KableRequest *request = parser->data;
    if (request->headers_done) return 0;
    if (request->header == HEADER_HOST && !request->hosts) {
        return append_head(request, at, len);
    }
    if (request->header == HEADER_RANGE && !request->ranges && request->range_len <= MAX_RANGE) {
        if (len > MAX_RANGE - request->range_len) {
            request->range_len = MAX_RANGE + 1;
        } else if (len) {
            memcpy(request->range + request->range_len, at, len);
            request->range_len += len;
        }
    }
    return 0;
}

static int on_value_complete(llhttp_t *parser) {
    KableRequest *request = parser->data;
    if (request->headers_done) return 0;
    switch (request->header) {
    case HEADER_HOST: request->hosts++; break;
    case HEADER_RANGE: request->ranges++; break;
    case HEADER_IF_RANGE: request->if_range = 1; break;
    case HEADER_EXPECT: request->expect = 1; break;
    case HEADER_OTHER: break;
    }
    return 0;
}

static int on_headers(llhttp_t *parser) {
    KableRequest *request = parser->data;
    uint64_t length = parser->flags & F_CONTENT_LENGTH ? parser->content_length : 0;
    request->rejection = request->validate_head(llhttp_get_http_major(parser), llhttp_get_http_minor(parser),
        request->hosts, llhttp_get_upgrade(parser) || llhttp_get_method(parser) == HTTP_CONNECT,
        request->expect, length, MAX_BODY, request->head.data, request->head.len, request->target_len);
    if (request->rejection) return -1;
    request->content_length = parser->flags & F_CONTENT_LENGTH ? length : UINT64_MAX;
    request->headers_done = 1;
    if (length || (parser->flags & F_CHUNKED)) {
        request->headers_paused = 1;
        return HPE_PAUSED;
    }
    return 0;
}

static int on_body(llhttp_t *parser, const char *at, size_t len) {
    KableRequest *request = parser->data;
    size_t limit = request->body_limit < MAX_BODY ? request->body_limit : MAX_BODY;
    if (len > MAX_BODY - request->body_bytes || request->body.len > limit) {
        request->rejection = 413;
        return HPE_USER;
    }
    int status = append(&request->body, at, len, limit);
    if (status == 0) {
        request->body_bytes += len;
        return 0;
    }
    request->rejection = status == ENOBUFS ? 413 : 500;
    return HPE_USER;
}

static int on_complete(llhttp_t *parser) {
    KableRequest *request = parser->data;
    request->complete = 1;
    return HPE_PAUSED;
}

static const llhttp_settings_t settings = {
    .on_url = on_url,
    .on_header_field = on_field,
    .on_header_field_complete = on_field_complete,
    .on_header_value = on_value,
    .on_header_value_complete = on_value_complete,
    .on_headers_complete = on_headers,
    .on_body = on_body,
    .on_message_complete = on_complete,
};

void kable_request_init(KableRequest *request, KableValidateHead validate_head) {
    *request = (KableRequest){ .validate_head = validate_head, .body_limit = MAX_BODY, .content_length = UINT64_MAX };
    llhttp_init(&request->parser, HTTP_REQUEST, &settings);
    request->parser.data = request;
}

void kable_release_request_head(KableRequest *request) {
    free(request->head.data);
    request->head = (KableRequestBytes){0};
    request->target_len = 0;
}

void kable_release_request(KableRequest *request) {
    kable_release_request_head(request);
    free(request->body.data);
    free(request->pending.data);
    request->body = (KableRequestBytes){0};
    request->pending = (KableRequestBytes){0};
}

/* Borrows the bounded Range value from the live request, or exposes zero bytes. */
const uint8_t *kable_range_header(KableRequest *request, size_t *length) {
    *length = 0;
    if (llhttp_get_method(&request->parser) == HTTP_GET && !request->if_range &&
        request->ranges == 1 && request->range_len <= MAX_RANGE) {
        *length = request->range_len;
    }
    return request->range;
}

/* Lends the target until release. Range metadata stays on the request. */
uint8_t kable_request_view(KableRequest *request, uint8_t *http10,
    uint8_t **target, size_t *target_len) {
    *http10 = llhttp_get_http_major(&request->parser) == 1
        && llhttp_get_http_minor(&request->parser) == 0;
    *target = request->head.data;
    *target_len = request->target_len;
    switch (llhttp_get_method(&request->parser)) {
    case HTTP_GET: return METHOD_GET;
    case HTTP_HEAD: return METHOD_HEAD;
    case HTTP_POST: return METHOD_POST;
    default: return METHOD_OTHER;
    }
}

uint16_t kable_request_feed(KableRequest *request, const uint8_t *bytes, size_t remaining) {
    const char *data = (const char *)bytes;
    while (remaining) {
        size_t limit = request->headers_done ? MAX_WIRE : MAX_HEAD;
        size_t budget = limit - request->wire_bytes;
        size_t count = remaining < budget ? remaining : budget;
        llhttp_errno_t result = llhttp_execute(&request->parser, data, count);
        if (result == HPE_PAUSED) {
            if (request->headers_paused) {
                size_t consumed = (size_t)(llhttp_get_error_pos(&request->parser) - data);
                request->wire_bytes += consumed;
                int status = append(&request->pending, data + consumed, remaining - consumed, MAX_WIRE);
                return status == 0 ? REQUEST_HEADERS : status == ENOBUFS ? 413 : 500;
            }
            return REQUEST_COMPLETE;
        }
        request->wire_bytes += count;
        if (result != HPE_OK) {
            if (result == HPE_INVALID_VERSION) return 505;
            return request->rejection ? request->rejection : 400;
        }
        if (!request->headers_done && request->wire_bytes == MAX_HEAD) {
            return 431;
        }
        if (request->wire_bytes == MAX_WIRE) {
            return 413;
        }
        remaining -= count;
        data += count;
    }
    return REQUEST_MORE;
}

uint16_t kable_request_resume(KableRequest *request) {
    if (!request->headers_paused) return request->complete ? REQUEST_COMPLETE : REQUEST_MORE;
    request->headers_paused = 0;
    llhttp_resume(&request->parser);
    KableRequestBytes pending = request->pending;
    request->pending = (KableRequestBytes){0};
    uint16_t result = kable_request_feed(request, pending.data, pending.len);
    free(pending.data);
    return result;
}

uint16_t kable_request_eof(const KableRequest *request) {
    return request->wire_bytes ? REQUEST_TRUNCATED : REQUEST_CLOSED;
}
