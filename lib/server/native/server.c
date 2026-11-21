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

typedef int32_t (*respond_fn)(uint8_t *, uint8_t *, uint16_t, uint8_t,
    uint8_t, uint8_t *, size_t, uint8_t *, size_t);
typedef uint16_t (*validate_head_fn)(uint8_t, uint8_t, uint32_t, uint8_t, uint8_t,
    uint64_t, uint64_t, uint8_t *, size_t, size_t);

/* Values shared with respond in server.kur. */
enum { METHOD_OTHER, METHOD_GET, METHOD_HEAD, METHOD_POST };
/* Matches the flags in the Kursal HTTP host. */
enum { RESPONSE_ACTIVE = 1 };

typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} Bytes;

typedef struct {
    uint8_t *listener;
    validate_head_fn validate_head;
    respond_fn respond;
    uint8_t *context;
} Server;

typedef struct {
    uint8_t *connection;
    uint8_t *scope;
    uint8_t *plan;
    KablePlanFinish finish;
    uint8_t response_flags;
    int plan_ready;
    int completing_plan;
    Server *server;
    llhttp_t parser;
    Bytes head; /* The request target followed by the first Host value. */
    size_t target_len;
    Bytes body;
    size_t wire_bytes;
    uint64_t deadline;
    uint16_t rejection;
    int headers_done;
    int complete;
    char field[8];
    size_t field_len;
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

static int on_value(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    if (client->headers_done) return 0;
    if (!client->hosts && field_is(client, "host", 4)) {
        return append_head(client, at, len);
    }
    if (field_is(client, "range", 5) && !client->ranges && client->range_len <= MAX_RANGE) {
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
    if (field_is(client, "host", 4)) client->hosts++;
    if (field_is(client, "expect", 6)) client->expect = 1;
    if (field_is(client, "range", 5)) client->ranges++;
    if (field_is(client, "if-range", 8)) client->if_range = 1;
    client->field_len = 0;
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
    Client *client = parser->data;
    client->complete = 1;
    return HPE_PAUSED;
}

static const llhttp_settings_t settings = {
    .on_url = on_url,
    .on_header_field = on_field,
    .on_header_value = on_value,
    .on_header_value_complete = on_value_complete,
    .on_headers_complete = on_headers,
    .on_body = on_body,
    .on_message_complete = on_complete,
};

static void release_request(Client *client) {
    free(client->head.data);
    free(client->body.data);
    client->head = (Bytes){0};
    client->body = (Bytes){0};
    client->target_len = 0;
}

static void destroy_client(uint8_t *attachment) {
    Client *client = (Client *)attachment;
    if (client->plan) abort();
    release_request(client);
    free(client);
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

static uint64_t time_left(const Client *client) {
    uint64_t now = kable_now_millis();
    return now < client->deadline ? client->deadline - now : 0;
}

static void cancel_plan(void *context) {
    Client *client = context;
    kable_scope_cancel(client->scope);
}

static void finish_plan(void *context) {
    Client *client = context;
    client->plan_ready = 1;
    if (client->completing_plan) return;

    /* A continuation may schedule a plan that fails to start immediately.
       Finish it in this loop so chained failures do not recurse. */
    client->completing_plan = 1;
    while (client->plan_ready) {
        client->plan_ready = 0;
        uint8_t ready = kable_tcp_unpark(client->connection) && time_left(client) != 0;
        uint8_t *plan = client->plan;
        client->plan = NULL;
        client->scope = NULL;
        uint8_t flags = client->response_flags | (ready ? RESPONSE_ACTIVE : 0);
        int status = client->finish(plan, (uint8_t *)client, flags);
        if (!ready || status < 0) kable_tcp_close(client->connection);
    }
    client->completing_plan = 0;
}

/* The request owns the plan and its continuation once the socket is parked. */
int32_t kable_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    Client *client = (Client *)raw;
    int status = kable_tcp_park(client->connection, cancel_plan, client);
    if (status < 0) return status;
    client->scope = scope;
    client->plan = plan;
    client->finish = finish;
    client->response_flags = flags;
    if (!kable_scope_start(scope, root, kable_tcp_loop(client->connection), client->deadline,
        1, finish_plan, client)) {
        finish_plan(client);
    }
    return 0;
}

static int reply(Client *client, uint16_t status) {
    uint8_t method = METHOD_OTHER;
    switch (llhttp_get_method(&client->parser)) {
    case HTTP_GET: method = METHOD_GET; break;
    case HTTP_HEAD: method = METHOD_HEAD; break;
    case HTTP_POST: method = METHOD_POST; break;
    }
    uint8_t http10 = llhttp_get_http_major(&client->parser) == 1
        && llhttp_get_http_minor(&client->parser) == 0;
    int result = client->server->respond(client->server->context, (uint8_t *)client,
        status, method, http10, client->head.data, client->target_len,
        client->body.data, client->body.len);
    release_request(client);
    return result;
}

static int read_request(Client *client) {
    uint64_t remaining = time_left(client);
    return remaining ? kable_tcp_read(client->connection, remaining) : reply(client, 408);
}

static int consume_request(Client *client, const char *data, size_t remaining) {
    while (remaining) {
        size_t limit = client->headers_done ? MAX_WIRE : MAX_HEAD;
        size_t budget = limit - client->wire_bytes;
        size_t count = remaining < budget ? remaining : budget;
        llhttp_errno_t result = llhttp_execute(&client->parser, data, count);
        client->wire_bytes += count;
        if (client->complete) {
            return reply(client, 0);
        }
        if (result != HPE_OK) {
            uint16_t status = client->rejection ? client->rejection : 400;
            if (result == HPE_INVALID_VERSION) status = 505;
            return reply(client, status);
        }
        if (!client->headers_done && client->wire_bytes == MAX_HEAD) {
            return reply(client, 431);
        }
        if (client->wire_bytes == MAX_WIRE) {
            return reply(client, 413);
        }
        remaining -= count;
        data += count;
    }
    return read_request(client);
}

static int start_request(Server *server, uint8_t *connection) {
    Client *client = calloc(1, sizeof *client);
    if (!client) return UV_ENOMEM;

    client->server = server;
    client->connection = connection;
    llhttp_init(&client->parser, HTTP_REQUEST, &settings);
    client->parser.data = client;
    client->deadline = kable_now_millis() + READ_TIMEOUT;
    kable_tcp_attach(connection, (uint8_t *)client, destroy_client);
    return read_request(client);
}

static int32_t on_event(uint8_t *context, uint8_t *connection, uint8_t event,
    uint8_t *bytes, size_t len, int32_t error) {
    (void)error;
    if (event == KABLE_TCP_CONNECTED) return start_request((Server *)context, connection);

    Client *client = (Client *)kable_tcp_attachment(connection);
    switch (event) {
    case KABLE_TCP_BYTES:
        return consume_request(client, (const char *)bytes, len);
    case KABLE_TCP_TIMED_OUT:
        return reply(client, 408);
    case KABLE_TCP_END:
        return client->wire_bytes ? reply(client, 400) : 0;
    default:
        return 0;
    }
}

uint8_t *kable_bind(uint16_t port, validate_head_fn validate_head, respond_fn respond, uint8_t *context, int32_t *error) {
    Server *server = calloc(1, sizeof *server);
    if (!server) {
        *error = UV_ENOMEM;
        return NULL;
    }
    server->validate_head = validate_head;
    server->respond = respond;
    server->context = context;
    server->listener = kable_tcp_bind(port, (uint8_t *)server, on_event, error);
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
