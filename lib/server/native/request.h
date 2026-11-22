#ifndef KABLE_HTTP_REQUEST_H
#define KABLE_HTTP_REQUEST_H

#include <llhttp.h>
#include <stddef.h>
#include <stdint.h>

/* Read outcomes below the HTTP rejection codes. */
enum { REQUEST_COMPLETE, REQUEST_MORE, REQUEST_TRUNCATED, REQUEST_CLOSED };
enum { METHOD_OTHER, METHOD_GET, METHOD_HEAD, METHOD_POST };
enum { MAX_RANGE = 256 };

typedef uint16_t (*KableValidateHead)(uint8_t, uint8_t, uint32_t, uint8_t, uint8_t,
    uint64_t, uint64_t, uint8_t *, size_t, size_t);

typedef enum { HEADER_OTHER, HEADER_HOST, HEADER_RANGE, HEADER_IF_RANGE, HEADER_EXPECT } HeaderKind;

typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} KableRequestBytes;

typedef struct {
    llhttp_t parser;
    KableValidateHead validate_head;
    KableRequestBytes head; /* The request target followed by the first Host value. */
    size_t target_len;
    KableRequestBytes body;
    size_t wire_bytes;
    uint16_t rejection;
    int headers_done;
    char field[8];
    size_t field_len;
    HeaderKind header;
    uint32_t hosts;
    uint8_t range[MAX_RANGE];
    size_t range_len;
    unsigned ranges;
    int if_range;
    int expect;
} KableRequest;

void kable_request_init(KableRequest *request, KableValidateHead validate_head);
/* The fragment is borrowed for this call. Stop feeding at the first terminal result. */
uint16_t kable_request_feed(KableRequest *request, const uint8_t *bytes, size_t length);
uint16_t kable_request_eof(const KableRequest *request);
uint8_t kable_request_view(KableRequest *request, uint8_t *http10,
    uint8_t **target, size_t *target_len, uint8_t **body, size_t *body_len);
const uint8_t *kable_range_header(KableRequest *request, size_t *length);
void kable_release_request(KableRequest *request);

#endif
