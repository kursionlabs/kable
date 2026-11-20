#include <arpa/inet.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../../async/native/timer.h"
#include "transport.h"

enum { IDLE, READING, JOINING, SCOPED, PARKED, STREAMING, FINISHING, TERMINAL };
enum { MAX_CONNECTIONS = 128, READ_SIZE = 16384 };

typedef struct {
    uv_loop_t loop;
    uv_tcp_t socket;
    uv_signal_t interrupt;
    uv_signal_t terminate;
    uv_signal_t broken_pipe;
    KableTcpHandler handler;
    uint8_t *context;
    unsigned connections;
    int stopping;
    int status;
    uint16_t port;
} Listener;

typedef struct {
    uv_tcp_t socket;
    uv_write_t write;
    uv_shutdown_t shutdown;
    uv_buf_t output;
    Listener *listener;
    KableTimer *timer;
    uint8_t *attachment;
    void (*destroy_attachment)(uint8_t *);
    void (*cancel_parked)(void *);
    void *parked_context;
    uint64_t stream_idle_ms;
    uint8_t *scope;
    uint8_t *plan;
    KablePlanFinish finish_plan;
    int scope_ready;
    int completing_scope;
    void (*read_ready)(uint8_t *);
    uint8_t *read_context;
    int cancelled;
    int phase;
    int socket_closed;
    int read_eof;
    uint8_t event;
    int error;
    size_t length;
    char input[READ_SIZE];
} Connection;

typedef struct {
    Connection *connection;
    uv_write_t request;
    uv_buf_t buffer;
    void (*ready)(uint8_t *);
    uint8_t *context;
    int status;
} ScopedWrite;

static void close_handle(uv_handle_t *handle, void *unused) {
    (void)unused;
    if (!uv_is_closing(handle)) uv_close(handle, NULL);
}

static void finish_shutdown(Listener *listener) {
    if (listener->stopping && listener->connections == 0) {
        uv_walk(&listener->loop, close_handle, NULL);
    }
}

static void release_connection(Connection *connection) {
    Listener *listener = connection->listener;
    if (connection->destroy_attachment) connection->destroy_attachment(connection->attachment);
    free(connection);
    listener->connections--;
    finish_shutdown(listener);
}

/* The socket, timer, and scope can retire in any order. */
static void release_closed_connection(Connection *connection) {
    if (connection->socket_closed && !connection->timer && !connection->scope) {
        release_connection(connection);
    }
}

static void connection_closed(uv_handle_t *handle) {
    Connection *connection = handle->data;
    connection->socket_closed = 1;
    release_closed_connection(connection);
}

static int socket_closing(Connection *connection) {
    return connection->socket_closed || uv_is_closing((uv_handle_t *)&connection->socket);
}

static void close_socket(Connection *connection) {
    if (!socket_closing(connection)) uv_close((uv_handle_t *)&connection->socket, connection_closed);
}

static void close_connection(Connection *connection) {
    if (connection->phase == SCOPED) {
        kable_scope_cancel(connection->scope);
        return;
    }
    if (connection->phase == PARKED || connection->phase == STREAMING) {
        if (!connection->cancelled) {
            connection->cancelled = 1;
            uv_read_stop((uv_stream_t *)&connection->socket);
            kable_timer_cancel(connection->timer);
            connection->cancel_parked(connection->parked_context);
        }
        return;
    }
    kable_timer_cancel(connection->timer);
    close_socket(connection);
}

void kable_tcp_close(uint8_t *raw) {
    close_connection((Connection *)raw);
}

void kable_tcp_attach(uint8_t *raw, uint8_t *attachment, void (*destroy)(uint8_t *)) {
    Connection *connection = (Connection *)raw;
    connection->attachment = attachment;
    connection->destroy_attachment = destroy;
}

uint8_t *kable_tcp_attachment(uint8_t *raw) {
    return ((Connection *)raw)->attachment;
}

uv_loop_t *kable_tcp_loop(uint8_t *raw) {
    return &((Connection *)raw)->listener->loop;
}

static void notify(Connection *connection, uint8_t event, size_t length, int error) {
    Listener *listener = connection->listener;
    int terminal = event == KABLE_TCP_END || event == KABLE_TCP_CANCELLED || event == KABLE_TCP_FAILED;
    connection->phase = terminal ? TERMINAL : IDLE;
    connection->event = event;
    int status = listener->handler(listener->context, (uint8_t *)connection, event,
        (uint8_t *)connection->input, length, error);
    if (connection->phase == TERMINAL) {
        close_connection(connection);
    } else if (status < 0) {
        notify(connection, KABLE_TCP_FAILED, 0, status);
    }
}

static void report_read(Connection *connection) {
    if (connection->listener->stopping) {
        notify(connection, KABLE_TCP_CANCELLED, 0, 0);
    } else {
        notify(connection, connection->event, connection->length, connection->error);
    }
}

/* Only the first completion can move a read into JOINING. The handler gets
   its connection back after the competing timer's close callback. */
static void complete_read(Connection *connection, uint8_t event, size_t length, int error) {
    if (connection->phase == SCOPED && connection->read_ready) {
        void (*ready)(uint8_t *) = connection->read_ready;
        uint8_t *context = connection->read_context;
        connection->event = event;
        connection->length = length;
        connection->error = error;
        kable_tcp_stop_scoped_read((uint8_t *)connection);
        ready(context);
        return;
    }
    if (connection->phase != READING) return;
    connection->phase = JOINING;
    connection->event = event;
    connection->length = length;
    connection->error = error;
    uv_read_stop((uv_stream_t *)&connection->socket);
    kable_timer_cancel(connection->timer);
}

static void timer_ready(void *context) {
    complete_read(context, KABLE_TCP_TIMED_OUT, 0, 0);
}

static void timer_closed(void *context, int fired) {
    (void)fired;
    Connection *connection = context;
    connection->timer = NULL;
    if (connection->socket_closed) release_closed_connection(connection);
    else report_read(connection);
}

static void allocate_read(uv_handle_t *handle, size_t suggested, uv_buf_t *buffer) {
    (void)suggested;
    Connection *connection = handle->data;
    *buffer = uv_buf_init(connection->input, sizeof connection->input);
}

static void read_done(uv_stream_t *stream, ssize_t count, const uv_buf_t *buffer) {
    (void)buffer;
    Connection *connection = stream->data;
    if (count > 0) complete_read(connection, KABLE_TCP_BYTES, (size_t)count, 0);
    else if (count == UV_EOF) {
        connection->read_eof = 1;
        complete_read(connection, KABLE_TCP_END, 0, 0);
    }
    else if (count < 0) complete_read(connection, KABLE_TCP_FAILED, 0, (int)count);
}

int32_t kable_tcp_read(uint8_t *raw, uint64_t ms) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping || socket_closing(connection)) {
        return UV_ECANCELED;
    }
    int status = uv_read_start((uv_stream_t *)&connection->socket, allocate_read, read_done);
    if (status < 0) return status;

    uv_update_time(&connection->listener->loop);
    status = kable_timer_start(&connection->listener->loop, ms, timer_ready,
        timer_closed, connection, &connection->timer);
    if (status < 0) {
        uv_read_stop((uv_stream_t *)&connection->socket);
        return status;
    }

    connection->phase = READING;
    return 0;
}

int32_t kable_tcp_start_scoped_read(uint8_t *raw, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != SCOPED || connection->read_ready ||
        loop != (uint8_t *)&connection->listener->loop) return UV_EINVAL;
    int status = uv_read_start((uv_stream_t *)&connection->socket, allocate_read, read_done);
    if (status == 0) {
        connection->read_ready = ready;
        connection->read_context = context;
    }
    return status;
}

uint8_t kable_tcp_stop_scoped_read(uint8_t *raw) {
    Connection *connection = (Connection *)raw;
    uv_read_stop((uv_stream_t *)&connection->socket);
    connection->read_ready = NULL;
    connection->read_context = NULL;
    return 0;
}

uint8_t kable_tcp_read_result(uint8_t *raw, uint8_t **bytes, size_t *len, int32_t *error) {
    Connection *connection = (Connection *)raw;
    *bytes = (uint8_t *)connection->input;
    *len = connection->length;
    *error = connection->error;
    return connection->event;
}

uint8_t *kable_tcp_new_scoped_write(uint8_t *raw, uint8_t *bytes, size_t len) {
    static char empty;
    ScopedWrite *write = calloc(1, sizeof *write);
    if (!write) return NULL;
    write->connection = (Connection *)raw;
    if (len > UINT32_MAX) write->status = UV_EINVAL;
    else write->buffer = uv_buf_init(len ? (char *)bytes : &empty, (unsigned int)len);
    return (uint8_t *)write;
}

static void scoped_write_done(uv_write_t *request, int status) {
    ScopedWrite *write = request->data;
    Connection *connection = write->connection;
    if (status == 0 && connection->phase == STREAMING && !connection->cancelled) {
        status = kable_timer_restart(connection->timer, connection->stream_idle_ms);
    }
    write->status = status;
    write->ready(write->context);
}

int32_t kable_tcp_start_scoped_write(uint8_t *raw, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context) {
    ScopedWrite *write = (ScopedWrite *)raw;
    if (!write) return UV_ENOMEM;
    if (write->status < 0) return write->status;
    Connection *connection = write->connection;
    if ((connection->phase != SCOPED && connection->phase != STREAMING) ||
        loop != (uint8_t *)&connection->listener->loop) return UV_EINVAL;
    if (socket_closing(connection) || connection->cancelled) return UV_ECANCELED;
    write->ready = ready;
    write->context = context;
    write->request.data = write;
    return uv_write(&write->request, (uv_stream_t *)&connection->socket,
        &write->buffer, 1, scoped_write_done);
}

uint8_t kable_tcp_stop_scoped_write(uint8_t *raw) {
    ScopedWrite *write = (ScopedWrite *)raw;
    // Closing the socket interrupts the write. Its callback still owns retirement.
    close_socket(write->connection);
    return 1;
}

int32_t kable_tcp_collect_scoped_write(uint8_t *raw) {
    ScopedWrite *write = (ScopedWrite *)raw;
    if (!write) return UV_ENOMEM;
    int status = write->status;
    free(write);
    return status;
}

static void end_stream(Connection *connection, int complete);

static void scope_joined(void *context) {
    Connection *connection = context;
    connection->scope_ready = 1;
    if (connection->completing_scope) return;

    connection->completing_scope = 1;
    while (connection->scope_ready) {
        connection->scope_ready = 0;
        connection->scope = NULL;
        uint8_t *plan = connection->plan;
        connection->plan = NULL;
        int streaming = connection->phase == STREAMING;
        if (!streaming) connection->phase = IDLE;
        int status = connection->finish_plan(plan, (uint8_t *)connection, 0);
        if (streaming) {
            if (status < 0 || connection->cancelled) end_stream(connection, 0);
        } else if (status < 0 || connection->listener->stopping) {
            close_connection(connection);
        }
    }
    connection->completing_scope = 0;
    release_closed_connection(connection);
}

static void discard_input(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buffer) {
    (void)buffer;
    if (nread < 0) close_connection(stream->data);
}

static void shutdown_done(uv_shutdown_t *shutdown, int status) {
    Connection *connection = shutdown->data;
    if (status < 0 || connection->read_eof || uv_read_start((uv_stream_t *)&connection->socket,
        allocate_read, discard_input) < 0) {
        close_connection(connection);
    }
}

static void shutdown_write(Connection *connection) {
    /* Send FIN before discarding unread input so the peer can read the reply. */
    connection->shutdown.data = connection;
    if (uv_shutdown(&connection->shutdown, (uv_stream_t *)&connection->socket, shutdown_done) < 0) {
        close_connection(connection);
    }
}

static void end_stream(Connection *connection, int complete) {
    uv_read_stop((uv_stream_t *)&connection->socket);
    connection->cancel_parked = NULL;
    connection->parked_context = NULL;
    connection->phase = complete ? FINISHING : TERMINAL;
    if (complete) shutdown_write(connection);
    else close_connection(connection);
}

static void cancel_stream(void *context) {
    Connection *connection = context;
    if (connection->scope) kable_scope_cancel(connection->scope);
    else end_stream(connection, 0);
}

static void write_done(uv_write_t *write, int status) {
    Connection *connection = write->data;
    free(connection->output.base);
    connection->output.base = NULL;
    if (connection->phase == FINISHING) {
        if (status < 0) close_connection(connection);
        else shutdown_write(connection);
    }
}

static int queue_write(Connection *connection, uint8_t *bytes, size_t len) {
    if (!len || len > UINT32_MAX) return UV_EINVAL;

    char *copy = malloc(len);
    if (!copy) return UV_ENOMEM;
    memcpy(copy, bytes, len);
    connection->output = (uv_buf_t){ .base = copy, .len = (unsigned int)len };
    connection->write.data = connection;
    int status = uv_write(&connection->write, (uv_stream_t *)&connection->socket,
        &connection->output, 1, write_done);
    if (status < 0) {
        free(copy);
        connection->output.base = NULL;
    }
    return status;
}

int32_t kable_tcp_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    (void)flags;
    Connection *connection = (Connection *)raw;
    if (socket_closing(connection)) return UV_ECANCELED;
    switch (connection->phase) {
    case IDLE:
        if (connection->listener->stopping) return UV_ECANCELED;
        connection->phase = SCOPED;
        break;
    case STREAMING:
        if (connection->cancelled) return UV_ECANCELED;
        if (connection->plan) return UV_EBUSY;
        break;
    default:
        return UV_ECANCELED;
    }
    connection->scope = scope;
    connection->plan = plan;
    connection->finish_plan = finish;
    if (!kable_scope_start(scope, root, &connection->listener->loop, 0, 0, scope_joined, connection)) {
        scope_joined(connection);
    }
    return 0;
}

static void parked_read(uv_stream_t *stream, ssize_t count, const uv_buf_t *buffer) {
    (void)buffer;
    Connection *connection = stream->data;
    if (count == UV_EOF) {
        /* A peer that has finished sending can still read its response. */
        connection->read_eof = 1;
        uv_read_stop(stream);
    } else if (count < 0) {
        close_connection(connection);
    }
}

int32_t kable_tcp_park(uint8_t *raw, void (*cancel)(void *), void *context) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping) return UV_ECANCELED;
    if (!connection->read_eof) {
        int status = uv_read_start((uv_stream_t *)&connection->socket, allocate_read, parked_read);
        if (status < 0) return status;
    }
    connection->phase = PARKED;
    connection->cancel_parked = cancel;
    connection->parked_context = context;
    connection->cancelled = 0;
    return 0;
}

int kable_tcp_unpark(uint8_t *raw) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != PARKED) return 0;
    uv_read_stop((uv_stream_t *)&connection->socket);
    connection->phase = IDLE;
    connection->cancel_parked = NULL;
    connection->parked_context = NULL;
    return !connection->cancelled && !connection->listener->stopping;
}

static void finish_expired(void *context) {
    close_connection(context);
}

static void finish_closed(void *context, int fired) {
    (void)fired;
    Connection *connection = context;
    connection->timer = NULL;
    release_closed_connection(connection);
}

int32_t kable_tcp_finish(uint8_t *raw, uint8_t *bytes, size_t len, uint64_t ms) {
    Connection *connection = (Connection *)raw;
    int peer_eof = connection->phase == TERMINAL && connection->event == KABLE_TCP_END;
    if ((!peer_eof && connection->phase != IDLE) || connection->listener->stopping) {
        return UV_ECANCELED;
    }
    uv_update_time(&connection->listener->loop);
    int status = kable_timer_start(&connection->listener->loop, ms, finish_expired,
        finish_closed, connection, &connection->timer);
    if (status < 0) return status;
    status = queue_write(connection, bytes, len);
    if (status < 0) {
        kable_timer_cancel(connection->timer);
        return status;
    }
    connection->phase = FINISHING;
    return 0;
}

int32_t kable_tcp_stream_begin(uint8_t *raw, uint64_t ms) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping) return UV_ECANCELED;
    uv_update_time(&connection->listener->loop);
    int status = kable_timer_start(&connection->listener->loop, ms, finish_expired,
        finish_closed, connection, &connection->timer);
    if (status < 0) return status;
    status = kable_tcp_park(raw, cancel_stream, connection);
    if (status < 0) {
        kable_timer_cancel(connection->timer);
        return status;
    }
    connection->phase = STREAMING;
    connection->stream_idle_ms = ms;
    return 0;
}

int32_t kable_tcp_stream_end(uint8_t *raw) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != STREAMING || connection->cancelled) return UV_ECANCELED;
    if (connection->plan) return UV_EBUSY;
    end_stream(connection, 1);
    return 0;
}

static void cancel_connection(uv_handle_t *handle, void *context) {
    Listener *listener = context;
    if (handle->type != UV_TCP || handle == (uv_handle_t *)&listener->socket ||
        uv_is_closing(handle)) return;

    Connection *connection = handle->data;
    if (connection->phase == READING) complete_read(connection, KABLE_TCP_CANCELLED, 0, 0);
    else if (connection->phase != JOINING && connection->phase != FINISHING) {
        close_connection(connection);
    }
}

static void stop_listener(Listener *listener, int status) {
    if (listener->stopping) return;
    listener->stopping = 1;
    listener->status = status;
    close_handle((uv_handle_t *)&listener->socket, NULL);
    uv_walk(&listener->loop, cancel_connection, listener);
    finish_shutdown(listener);
}

static void on_signal(uv_signal_t *signal, int signum) {
    // A disconnected peer is reported through the write callback.
    if (signum != SIGPIPE) stop_listener(signal->data, 0);
}

static void accept_connection(uv_stream_t *stream, int status) {
    Listener *listener = stream->data;
    if (status < 0) {
        stop_listener(listener, status);
        return;
    }

    Connection *connection = calloc(1, sizeof *connection);
    if (!connection) {
        stop_listener(listener, UV_ENOMEM);
        return;
    }
    status = uv_tcp_init(&listener->loop, &connection->socket);
    if (status < 0) {
        free(connection);
        stop_listener(listener, status);
        return;
    }
    connection->listener = listener;
    connection->socket.data = connection;
    listener->connections++;
    status = uv_accept(stream, (uv_stream_t *)&connection->socket);
    if (status < 0 || listener->connections > MAX_CONNECTIONS) close_connection(connection);
    else notify(connection, KABLE_TCP_CONNECTED, 0, 0);
}

static int watch_signal(Listener *listener, uv_signal_t *signal, int signum) {
    int status = uv_signal_init(&listener->loop, signal);
    if (status < 0) return status;
    signal->data = listener;
    return uv_signal_start(signal, on_signal, signum);
}

static int listen_local(Listener *listener, uint16_t port) {
    struct sockaddr_in address;
    int status = uv_ip4_addr("127.0.0.1", port, &address);
    if (status < 0) return status;
    status = uv_tcp_init(&listener->loop, &listener->socket);
    if (status < 0) return status;
    listener->socket.data = listener;
    status = uv_tcp_bind(&listener->socket, (const struct sockaddr *)&address, 0);
    if (status < 0) return status;
    status = uv_listen((uv_stream_t *)&listener->socket, MAX_CONNECTIONS, accept_connection);
    if (status < 0) return status;
    int len = sizeof address;
    status = uv_tcp_getsockname(&listener->socket, (struct sockaddr *)&address, &len);
    if (status == 0) listener->port = ntohs(address.sin_port);
    return status;
}

static int run_and_free(Listener *listener) {
    uv_run(&listener->loop, UV_RUN_DEFAULT);
    if (uv_loop_close(&listener->loop) != 0) abort();
    int status = listener->status;
    free(listener);
    return status;
}

uint8_t *kable_tcp_bind(uint16_t port, uint8_t *context, KableTcpHandler handler, int32_t *error) {
    Listener *listener = calloc(1, sizeof *listener);
    if (!listener) {
        *error = UV_ENOMEM;
        return NULL;
    }
    int status = uv_loop_init(&listener->loop);
    if (status < 0) {
        *error = status;
        free(listener);
        return NULL;
    }
    listener->handler = handler;
    listener->context = context;
    status = listen_local(listener, port);
    if (status == 0) status = watch_signal(listener, &listener->interrupt, SIGINT);
    if (status == 0) status = watch_signal(listener, &listener->terminate, SIGTERM);
    if (status == 0) status = watch_signal(listener, &listener->broken_pipe, SIGPIPE);
    *error = status;
    if (status < 0) {
        uv_walk(&listener->loop, close_handle, NULL);
        run_and_free(listener);
        return NULL;
    }
    return (uint8_t *)listener;
}

uint16_t kable_tcp_port(uint8_t *raw) { return ((Listener *)raw)->port; }

int32_t kable_tcp_run(uint8_t *raw) { return run_and_free((Listener *)raw); }

void kable_tcp_stop(uint8_t *raw) {
    Listener *listener = (Listener *)raw;
    stop_listener(listener, 0);
    run_and_free(listener);
}
