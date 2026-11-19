#include <arpa/inet.h>
#include <llhttp.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

enum {
    MAX_CLIENTS = 128,
    MAX_HEAD = 16 * 1024,
    MAX_BODY = 1024 * 1024,
    MAX_WIRE = MAX_BODY + 4 * MAX_HEAD,
    READ_TIMEOUT = 10000,
    WRITE_TIMEOUT = 5000,
    TIMER_INTERVAL = 100
};

typedef int32_t (*respond_fn)(uint8_t *, uint8_t *, uint16_t, uint8_t,
    uint8_t, uint8_t *, size_t, uint8_t *, size_t);

/* Values shared with respond in server.kur. */
enum { METHOD_OTHER, METHOD_GET, METHOD_HEAD, METHOD_POST };

typedef struct {
    uint8_t *data;
    size_t len;
    size_t capacity;
} Bytes;

typedef struct {
    uv_loop_t loop;
    uv_tcp_t listener;
    uv_signal_t interrupt;
    uv_signal_t terminate;
    uv_timer_t timer;
    respond_fn respond;
    uint8_t *context;
    unsigned clients;
    int stopping;
    int status;
    uint16_t port;
    char input[MAX_HEAD];
} Server;

typedef struct {
    uv_tcp_t socket;
    uv_write_t write;
    uv_shutdown_t shutdown;
    Server *server;
    llhttp_t parser;
    Bytes target;
    Bytes body;
    uv_buf_t output;
    size_t wire_bytes;
    uint64_t deadline;
    uint16_t rejection;
    int headers_done;
    int complete;
    int finishing;
    char field[6];
    size_t field_len;
    unsigned hosts;
    enum { HOST_EMPTY, HOST_VALUE, HOST_TRAILING_SPACE, HOST_INVALID } host;
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

static int on_url(llhttp_t *parser, const char *at, size_t len) {
    Client *client = parser->data;
    int status = append(&client->target, at, len, MAX_HEAD);
    if (status == 0) return 0;
    client->rejection = status == UV_ENOBUFS ? 431 : 500;
    return HPE_USER;
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
    if (!client->headers_done && field_is(client, "host", 4)) {
        for (size_t i = 0; i < len; i++) {
            unsigned char byte = (unsigned char)at[i];
            if (byte == ' ' || byte == '\t') {
                if (client->host == HOST_VALUE) client->host = HOST_TRAILING_SPACE;
            } else if (byte < '!' || byte > '~' || byte == ',' ||
                client->host == HOST_TRAILING_SPACE || client->host == HOST_INVALID) {
                client->host = HOST_INVALID;
            } else {
                client->host = HOST_VALUE;
            }
        }
    }
    return 0;
}

static int on_value_complete(llhttp_t *parser) {
    Client *client = parser->data;
    if (client->headers_done) return 0;
    if (field_is(client, "host", 4)) client->hosts++;
    if (field_is(client, "expect", 6)) client->expect = 1;
    client->field_len = 0;
    return 0;
}

static int on_headers(llhttp_t *parser) {
    Client *client = parser->data;
    uint8_t minor = llhttp_get_http_minor(parser);
    if (llhttp_get_http_major(parser) != 1 || minor > 1) {
        client->rejection = 505;
    } else if (client->hosts > 1 || (minor == 1 && !client->hosts)
        || (client->hosts && (client->host == HOST_EMPTY || client->host == HOST_INVALID))) {
        client->rejection = 400;
    } else if (llhttp_get_upgrade(parser) || llhttp_get_method(parser) == HTTP_CONNECT) {
        client->rejection = 501;
    } else if (!client->target.len || client->target.data[0] != '/'
        || memchr(client->target.data, '#', client->target.len)) {
        client->rejection = 400;
    } else if (client->expect) {
        client->rejection = 417;
    } else if ((parser->flags & F_CONTENT_LENGTH) && parser->content_length > MAX_BODY) {
        client->rejection = 413;
    }
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

static void close_handle(uv_handle_t *handle, void *unused) {
    (void)unused;
    if (!uv_is_closing(handle)) uv_close(handle, NULL);
}

static void finish_shutdown(Server *server) {
    if (server->stopping && server->clients == 0) {
        uv_walk(&server->loop, close_handle, NULL);
    }
}

static void release_request(Client *client) {
    free(client->target.data);
    free(client->body.data);
    client->target = (Bytes){0};
    client->body = (Bytes){0};
}

static void client_closed(uv_handle_t *handle) {
    Client *client = handle->data;
    Server *server = client->server;
    release_request(client);
    free(client->output.base);
    free(client);
    server->clients--;
    finish_shutdown(server);
}

static void close_client(Client *client) {
    uv_handle_t *handle = (uv_handle_t *)&client->socket;
    if (!uv_is_closing(handle)) uv_close(handle, client_closed);
}

static void allocate_read(uv_handle_t *handle, size_t suggested, uv_buf_t *buffer) {
    (void)suggested;
    Client *client = handle->data;
    /* The loop invokes alloc/read pairs serially. The parser copies any data
       it needs after the read callback, so connections share this scratch. */
    *buffer = uv_buf_init(client->server->input, sizeof client->server->input);
}

static void discard_input(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buffer) {
    (void)buffer;
    if (nread < 0) close_client(stream->data);
}

static void shutdown_done(uv_shutdown_t *shutdown, int status) {
    Client *client = shutdown->data;
    if (status < 0 || uv_read_start((uv_stream_t *)&client->socket,
        allocate_read, discard_input) < 0) {
        close_client(client);
    }
}

static void write_done(uv_write_t *write, int status) {
    Client *client = write->data;
    free(client->output.base);
    client->output.base = NULL;
    /* Send FIN before draining unread input. Closing a socket with unread
       bytes can reset it and hide a response the client has yet to read. */
    client->shutdown.data = client;
    if (status < 0 || uv_shutdown(&client->shutdown, (uv_stream_t *)&client->socket,
        shutdown_done) < 0) {
        close_client(client);
    }
}

/* libuv owns access to the copy through write_done, even on cancellation. */
int32_t kable_write(uint8_t *raw, uint8_t *bytes, size_t len) {
    Client *client = (Client *)raw;
    if (len == 0 || len > UINT32_MAX) return UV_EINVAL;
    char *copy = malloc(len);
    if (!copy) return UV_ENOMEM;
    memcpy(copy, bytes, len);
    client->output = (uv_buf_t){ .base = copy, .len = (unsigned int)len };
    client->write.data = client;
    int status = uv_write(&client->write, (uv_stream_t *)&client->socket,
        &client->output, 1, write_done);
    if (status < 0) {
        free(copy);
        client->output.base = NULL;
        return status;
    }
    client->finishing = 1;
    client->deadline = uv_now(&client->server->loop) + WRITE_TIMEOUT;
    return 0;
}

static void reply(Client *client, uint16_t status) {
    uv_read_stop((uv_stream_t *)&client->socket);
    uint8_t method = METHOD_OTHER;
    switch (llhttp_get_method(&client->parser)) {
    case HTTP_GET: method = METHOD_GET; break;
    case HTTP_HEAD: method = METHOD_HEAD; break;
    case HTTP_POST: method = METHOD_POST; break;
    }
    uint8_t http10 = llhttp_get_http_major(&client->parser) == 1
        && llhttp_get_http_minor(&client->parser) == 0;
    int result = client->server->respond(client->server->context, (uint8_t *)client,
        status, method, http10, client->target.data, client->target.len,
        client->body.data, client->body.len);
    release_request(client);
    if (result != 0) close_client(client);
}

static void read_done(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buffer) {
    Client *client = stream->data;
    if (nread < 0) {
        if (nread == UV_EOF && client->wire_bytes) reply(client, 400);
        else close_client(client);
        return;
    }

    size_t remaining = (size_t)nread;
    const char *data = buffer->base;
    while (remaining) {
        size_t limit = client->headers_done ? MAX_WIRE : MAX_HEAD;
        size_t budget = limit - client->wire_bytes;
        size_t count = remaining < budget ? remaining : budget;
        llhttp_errno_t result = llhttp_execute(&client->parser, data, count);
        client->wire_bytes += count;
        if (client->complete) {
            reply(client, 0);
            return;
        }
        if (result != HPE_OK) {
            uint16_t status = client->rejection ? client->rejection : 400;
            if (result == HPE_INVALID_VERSION) status = 505;
            reply(client, status);
            return;
        }
        if (!client->headers_done && client->wire_bytes == MAX_HEAD) {
            reply(client, 431);
            return;
        }
        if (client->wire_bytes == MAX_WIRE) {
            reply(client, 413);
            return;
        }
        remaining -= count;
        data += count;
    }
}

static void visit_client(uv_handle_t *handle, void *context) {
    Server *server = context;
    if (handle->type != UV_TCP || handle == (uv_handle_t *)&server->listener
        || uv_is_closing(handle)) return;

    Client *client = handle->data;
    if (server->stopping && !client->finishing) {
        close_client(client);
    } else if (client->deadline <= uv_now(&server->loop)) {
        if (client->finishing) close_client(client);
        else reply(client, 408);
    }
}

static void tick(uv_timer_t *timer) {
    Server *server = timer->data;
    uv_walk(&server->loop, visit_client, server);
}

static void stop_server(Server *server, int status) {
    if (server->stopping) return;
    server->stopping = 1;
    server->status = status;
    close_handle((uv_handle_t *)&server->listener, NULL);
    uv_walk(&server->loop, visit_client, server);
    finish_shutdown(server);
}

static void on_signal(uv_signal_t *signal, int signum) {
    (void)signum;
    stop_server(signal->data, 0);
}

static void connection_ready(uv_stream_t *listener, int status) {
    Server *server = listener->data;
    if (status < 0) {
        stop_server(server, status);
        return;
    }

    Client *client = calloc(1, sizeof *client);
    if (!client) {
        stop_server(server, UV_ENOMEM);
        return;
    }
    status = uv_tcp_init(&server->loop, &client->socket);
    if (status < 0) {
        free(client);
        stop_server(server, status);
        return;
    }

    client->server = server;
    client->socket.data = client;
    server->clients++;
    status = uv_accept(listener, (uv_stream_t *)&client->socket);
    if (status < 0 || server->clients > MAX_CLIENTS) {
        close_client(client);
        return;
    }

    llhttp_init(&client->parser, HTTP_REQUEST, &settings);
    client->parser.data = client;
    client->deadline = uv_now(&server->loop) + READ_TIMEOUT;
    status = uv_read_start((uv_stream_t *)&client->socket, allocate_read, read_done);
    if (status < 0) close_client(client);
}

static int listen_local(Server *server, uint16_t port) {
    struct sockaddr_in address;
    int status = uv_ip4_addr("127.0.0.1", port, &address);
    if (status < 0) return status;
    status = uv_tcp_init(&server->loop, &server->listener);
    if (status < 0) return status;
    server->listener.data = server;
    status = uv_tcp_bind(&server->listener, (const struct sockaddr *)&address, 0);
    if (status < 0) return status;
    status = uv_listen((uv_stream_t *)&server->listener, MAX_CLIENTS, connection_ready);
    if (status < 0) return status;
    int len = sizeof address;
    status = uv_tcp_getsockname(&server->listener, (struct sockaddr *)&address, &len);
    if (status == 0) server->port = ntohs(address.sin_port);
    return status;
}

static int watch_signal(Server *server, uv_signal_t *signal, int signum) {
    int status = uv_signal_init(&server->loop, signal);
    if (status < 0) return status;
    signal->data = server;
    return uv_signal_start(signal, on_signal, signum);
}

static int watch_deadlines(Server *server) {
    int status = uv_timer_init(&server->loop, &server->timer);
    if (status < 0) return status;
    server->timer.data = server;
    return uv_timer_start(&server->timer, tick, TIMER_INTERVAL, TIMER_INTERVAL);
}

static int run_and_free(Server *server) {
    uv_run(&server->loop, UV_RUN_DEFAULT);
    /* All child handles and pending requests must have closed before return. */
    if (uv_loop_close(&server->loop) != 0) abort();
    int status = server->status;
    free(server);
    return status;
}

uint8_t *kable_bind(uint16_t port, respond_fn respond, uint8_t *context, int32_t *error) {
    Server *server = calloc(1, sizeof *server);
    if (!server) {
        *error = UV_ENOMEM;
        return NULL;
    }
    int status = uv_loop_init(&server->loop);
    if (status < 0) {
        *error = status;
        free(server);
        return NULL;
    }

    server->respond = respond;
    server->context = context;
    status = listen_local(server, port);
    if (status == 0) status = watch_signal(server, &server->interrupt, SIGINT);
    if (status == 0) status = watch_signal(server, &server->terminate, SIGTERM);
    if (status == 0) status = watch_deadlines(server);
    *error = status;
    if (status < 0) {
        uv_walk(&server->loop, close_handle, NULL);
        run_and_free(server);
        return NULL;
    }
    return (uint8_t *)server;
}

uint16_t kable_port(uint8_t *raw) {
    Server *server = (Server *)raw;
    return server->port;
}

int32_t kable_run(uint8_t *raw) {
    return run_and_free((Server *)raw);
}

void kable_stop(uint8_t *raw) {
    Server *server = (Server *)raw;
    stop_server(server, 0);
    run_and_free(server);
}
