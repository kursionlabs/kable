#include <llhttp.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

#include "../../tcp/native/transport.h"

enum {
    MAX_HEAD = 16 * 1024,
    MAX_RANGE = 256,
    MAX_BODY = 1024 * 1024,
    MAX_WIRE = MAX_BODY + 4 * MAX_HEAD,
    READ_TIMEOUT = 10000
};

typedef int32_t (*receive_fn)(uint8_t *context, uint8_t *client);
typedef uint16_t (*validate_head_fn)(uint8_t, uint8_t, uint32_t, uint8_t, uint8_t,
    uint64_t, uint64_t, uint8_t *, size_t, size_t);

/* Method values returned by the request view. */
enum { METHOD_OTHER, METHOD_GET, METHOD_HEAD, METHOD_POST };

/* Read outcomes below the HTTP rejection codes returned to receive. */
enum { REQUEST_COMPLETE, REQUEST_MORE, REQUEST_TRUNCATED, REQUEST_CLOSED };

typedef enum { HEADER_OTHER, HEADER_HOST, HEADER_RANGE, HEADER_IF_RANGE, HEADER_EXPECT } HeaderKind;

typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} Bytes;

typedef struct {
    uint8_t *listener;
    validate_head_fn validate_head;
    receive_fn receive;
    uint8_t *context;
} Server;

typedef struct {
    uint8_t *connection;
    Server *server;
    llhttp_t parser;
    Bytes head; /* The request target followed by the first Host value. */
    size_t target_len;
    Bytes body;
    size_t wire_bytes;
    uint64_t deadline;
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
} Client;

static int append(Bytes *bytes, const char *src, size_t len, size_t limit) {
    if (len > limit - bytes->len) return UV_ENOBUFS;
    size_t needed = bytes->len + len;
    if (needed > bytes->capacity) {
        size_t capacity = bytes->capacity ? 2 * bytes->capacity : 128;
        if (capacity < needed) capacity = needed;
        if (capacity > limit) capacity = limit;
        uint8_t *data = realloc(bytes->data, capacity);
        if (!data) return UV_ENOMEM;
        bytes->data = data;
        bytes->capacity = capacity;
    }
    if (len) memcpy(bytes->data + bytes->len, src, len);
    bytes->len = needed;
    return 0;
}

static int append_head(Client *client, const char *at, size_t len) {
    int status = append(&client->head, at, len, MAX_HEAD);
    if (status == 0) return 0;
    client->rejection = status == UV_ENOBUFS ? 431 : 500;
    return HPE_USER;
}

static int on_url(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    int status = append_head(client, at, len);
    client->target_len = client->head.len;
    return status;
}

static int on_field(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    if (client->headers_done || client->field_len > sizeof client->field) return 0;
    if (len > sizeof client->field - client->field_len) {
        client->field_len = sizeof client->field + 1;
    } else {
        memcpy(client->field + client->field_len, at, len);
        client->field_len += len;
    }
    return 0;
}

static int field_is(const Client *client, const char *name, size_t len) {
    return client->field_len == len && strncasecmp(client->field, name, len) == 0;
}

static int on_field_complete(llhttp_t *parser) {
    Client *client = parser->data;
    if (client->headers_done) return 0;
    if (field_is(client, "host", 4)) client->header = HEADER_HOST;
    else if (field_is(client, "range", 5)) client->header = HEADER_RANGE;
    else if (field_is(client, "if-range", 8)) client->header = HEADER_IF_RANGE;
    else if (field_is(client, "expect", 6)) client->header = HEADER_EXPECT;
    else client->header = HEADER_OTHER;
    client->field_len = 0;
    return 0;
}

static int on_value(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    if (client->headers_done) return 0;
    if (client->header == HEADER_HOST && !client->hosts) {
        return append_head(client, at, len);
    }
    if (client->header == HEADER_RANGE && !client->ranges && client->range_len <= MAX_RANGE) {
        if (len > MAX_RANGE - client->range_len) {
            client->range_len = MAX_RANGE + 1;
        } else if (len) {
            memcpy(client->range + client->range_len, at, len);
            client->range_len += len;
        }
    }
    return 0;
}

static int on_value_complete(llhttp_t *parser) {
    Client *client = parser->data;
    if (client->headers_done) return 0;
    switch (client->header) {
    case HEADER_HOST: client->hosts++; break;
    case HEADER_RANGE: client->ranges++; break;
    case HEADER_IF_RANGE: client->if_range = 1; break;
    case HEADER_EXPECT: client->expect = 1; break;
    case HEADER_OTHER: break;
    }
    return 0;
}

static int on_headers(llhttp_t *parser) {
    Client *client = parser->data;
    uint64_t length = parser->flags & F_CONTENT_LENGTH ? parser->content_length : 0;
    client->rejection = client->server->validate_head(llhttp_get_http_major(parser), llhttp_get_http_minor(parser),
        client->hosts, llhttp_get_upgrade(parser) || llhttp_get_method(parser) == HTTP_CONNECT,
        client->expect, length, MAX_BODY, client->head.data, client->head.len, client->target_len);
    if (client->rejection) return -1;
    client->headers_done = 1;
    return 0;
}

static int on_body(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    int status = append(&client->body, at, len, MAX_BODY);
    if (status == 0) return 0;
    client->rejection = status == UV_ENOBUFS ? 413 : 500;
    return HPE_USER;
}

static int on_complete(llhttp_t *parser) {
    (void)parser;
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

void kable_release_request(uint8_t *raw) {
    Client *client = (Client *)raw;
    free(client->head.data);
    free(client->body.data);
    client->head = (Bytes){0};
    client->body = (Bytes){0};
    client->target_len = 0;
}

static void destroy_client(uint8_t *attachment) {
    kable_release_request(attachment);
    free(attachment);
}

/* Borrows the bounded Range value from the live client, or exposes zero bytes. */
const uint8_t *kable_range_header(uint8_t *raw, size_t *length) {
    Client *client = (Client *)raw;
    *length = 0;
    if (llhttp_get_method(&client->parser) == HTTP_GET && !client->if_range &&
        client->ranges == 1 && client->range_len <= MAX_RANGE) {
        *length = client->range_len;
    }
    return client->range;
}

uint8_t *kable_client_connection(uint8_t *raw) {
    return ((Client *)raw)->connection;
}

int32_t kable_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    Client *client = (Client *)raw;
    return kable_tcp_schedule_timed(client->connection, KABLE_TCP_DEFER_REPLY,
        scope, root, plan, finish, flags, raw, client->deadline);
}

int32_t kable_receive_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    (void)flags;
    Client *client = (Client *)raw;
    return kable_tcp_schedule_timed(client->connection, KABLE_TCP_RECEIVE,
        scope, root, plan, finish, 0, raw, client->deadline);
}

/* Lends the target and body until release. Range metadata stays on the client. */
uint8_t kable_request_view(uint8_t *raw, uint8_t *http10,
    uint8_t **target, size_t *target_len, uint8_t **body, size_t *body_len) {
    Client *client = (Client *)raw;
    *http10 = llhttp_get_http_major(&client->parser) == 1
        && llhttp_get_http_minor(&client->parser) == 0;
    *target = client->head.data;
    *target_len = client->target_len;
    *body = client->body.data;
    *body_len = client->body.len;
    switch (llhttp_get_method(&client->parser)) {
    case HTTP_GET: return METHOD_GET;
    case HTTP_HEAD: return METHOD_HEAD;
    case HTTP_POST: return METHOD_POST;
    default: return METHOD_OTHER;
    }
}

static uint16_t consume_request(Client *client, const char *data, size_t remaining) {
    while (remaining) {
        size_t limit = client->headers_done ? MAX_WIRE : MAX_HEAD;
        size_t budget = limit - client->wire_bytes;
        size_t count = remaining < budget ? remaining : budget;
        llhttp_errno_t result = llhttp_execute(&client->parser, data, count);
        client->wire_bytes += count;
        if (result == HPE_PAUSED) {
            return REQUEST_COMPLETE;
        }
        if (result != HPE_OK) {
            if (result == HPE_INVALID_VERSION) return 505;
            return client->rejection ? client->rejection : 400;
        }
        if (!client->headers_done && client->wire_bytes == MAX_HEAD) {
            return 431;
        }
        if (client->wire_bytes == MAX_WIRE) {
            return 413;
        }
        remaining -= count;
        data += count;
    }
    return REQUEST_MORE;
}

static int32_t accept_request(uint8_t *context, uint8_t *connection) {
    Client *client = calloc(1, sizeof *client);
    if (!client) return UV_ENOMEM;

    client->server = (Server *)context;
    client->connection = connection;
    llhttp_init(&client->parser, HTTP_REQUEST, &settings);
    client->parser.data = client;
    client->deadline = kable_now_millis() + READ_TIMEOUT;
    kable_tcp_attach(connection, (uint8_t *)client, destroy_client);
    return client->server->receive(client->server->context, (uint8_t *)client);
}

uint16_t kable_parse_input(uint8_t *raw) {
    Client *client = (Client *)raw;
    uint8_t *bytes;
    int32_t count = kable_tcp_read_result(client->connection, &bytes);
    if (count > 0) return consume_request(client, (const char *)bytes, (size_t)count);
    if (count == 0 && client->wire_bytes) return REQUEST_TRUNCATED;
    return REQUEST_CLOSED;
}

uint8_t *kable_bind(uint16_t port, validate_head_fn validate_head, receive_fn receive,
    uint8_t *context, int32_t *error) {
    Server *server = calloc(1, sizeof *server);
    if (!server) {
        *error = UV_ENOMEM;
        return NULL;
    }
    server->validate_head = validate_head;
    server->receive = receive;
    server->context = context;
    server->listener = kable_tcp_bind(port, (uint8_t *)server, accept_request, error);
    if (!server->listener) {
        free(server);
        return NULL;
    }
    return (uint8_t *)server;
}

uint16_t kable_port(uint8_t *raw) {
    return kable_tcp_port(((Server *)raw)->listener);
}

int32_t kable_run(uint8_t *raw) {
    Server *server = (Server *)raw;
    int status = kable_tcp_run(server->listener);
    free(server);
    return status;
}

void kable_stop(uint8_t *raw) {
    Server *server = (Server *)raw;
    kable_tcp_stop(server->listener);
    free(server);
}
