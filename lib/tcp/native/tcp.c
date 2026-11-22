#include <arpa/inet.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <uv.h>

#include "../../async/native/timer.h"
#include "transport.h"

enum { IDLE, RECEIVING, SCOPED, PARKED, STREAMING, FINISHING, TERMINAL };
enum { MAX_CONNECTIONS = 128, READ_SIZE = 16384 };

typedef struct {
    uv_loop_t loop;
    uv_tcp_t socket;
    uv_signal_t interrupt;
    uv_signal_t terminate;
    uv_signal_t broken_pipe;
    KableTcpAccept accept;
    uint8_t *context;
    unsigned connections;
    int stopping;
    int status;
    uint16_t port;
} Listener;

typedef struct {
    uint8_t *scope;
    uint8_t *plan;
    KablePlanFinish finish;
    uint8_t *host;
    uint64_t deadline;
    uint8_t flags;
} PendingPlan;

typedef struct {
    uv_tcp_t socket;
    uv_shutdown_t shutdown;
    Listener *listener;
    KableTimer *timer;
    uint8_t *attachment;
    void (*destroy_attachment)(uint8_t *);
    uint64_t stream_idle_ms;
    PendingPlan pending;
    int scope_ready;
    int completing_scope;
    void (*read_ready)(uint8_t *);
    uint8_t *read_context;
    int cancelled;
    int phase;
    int socket_closed;
    int read_eof;
    int32_t read_count;
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

/* The socket, timer, and plan can retire in any order. */
static void release_closed_connection(Connection *connection) {
    if (connection->socket_closed && !connection->timer &&
        !connection->pending.plan && !connection->completing_scope) {
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

static void end_stream(Connection *connection, int complete);

static void close_connection(Connection *connection) {
    if (connection->phase == SCOPED || connection->phase == RECEIVING) connection->cancelled = 1;
    if (connection->phase == SCOPED || connection->phase == RECEIVING ||
        (connection->phase == FINISHING && connection->pending.plan)) {
        kable_scope_cancel(connection->pending.scope);
        return;
    }
    if (connection->phase == PARKED || connection->phase == STREAMING) {
        if (!connection->cancelled) {
            connection->cancelled = 1;
            uv_read_stop((uv_stream_t *)&connection->socket);
            kable_timer_cancel(connection->timer);
            if (connection->pending.plan) kable_scope_cancel(connection->pending.scope);
            else end_stream(connection, 0);
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

static void allocate_read(uv_handle_t *handle, size_t suggested, uv_buf_t *buffer) {
    (void)suggested;
    Connection *connection = handle->data;
    *buffer = uv_buf_init(connection->input, sizeof connection->input);
}

static void read_done(uv_stream_t *stream, ssize_t count, const uv_buf_t *buffer) {
    (void)buffer;
    Connection *connection = stream->data;
    if (count == 0 || !connection->read_ready) return;
    if (count == UV_EOF) {
        connection->read_eof = 1;
        count = 0;
    }
    connection->read_count = (int32_t)count;
    void (*ready)(uint8_t *) = connection->read_ready;
    uint8_t *context = connection->read_context;
    kable_tcp_stop_scoped_read((uint8_t *)connection);
    ready(context);
}

int32_t kable_tcp_start_scoped_read(uint8_t *raw, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context) {
    Connection *connection = (Connection *)raw;
    if ((connection->phase != SCOPED && connection->phase != RECEIVING) || connection->read_ready ||
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

int32_t kable_tcp_read_result(uint8_t *raw, uint8_t **bytes) {
    Connection *connection = (Connection *)raw;
    *bytes = (uint8_t *)connection->input;
    return connection->read_count;
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
    if ((connection->phase != SCOPED && connection->phase != STREAMING && connection->phase != FINISHING) ||
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

static void scope_joined(void *context) {
    Connection *connection = context;
    connection->scope_ready = 1;
    if (connection->completing_scope) return;

    /* A continuation can submit another plan that joins before submission returns. */
    connection->completing_scope = 1;
    while (connection->scope_ready) {
        connection->scope_ready = 0;
        PendingPlan pending = connection->pending;
        connection->pending = (PendingPlan){0};
        int phase = connection->phase;
        int active = 1;
        if (phase == SCOPED || phase == PARKED || phase == RECEIVING) {
            connection->phase = IDLE;
            if (phase == PARKED) uv_read_stop((uv_stream_t *)&connection->socket);
            active = !connection->cancelled && !connection->listener->stopping && !socket_closing(connection) &&
                (phase != PARKED || kable_now_millis() < pending.deadline);
            if (active) pending.flags |= KABLE_TCP_PLAN_ACTIVE;
        }
        int status = pending.finish(pending.plan, pending.host, pending.flags);
        if (phase == STREAMING) {
            if (status < 0 || connection->cancelled) end_stream(connection, 0);
        } else if (!active || status < 0) {
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
    connection->phase = complete ? FINISHING : TERMINAL;
    if (complete) shutdown_write(connection);
    else close_connection(connection);
}

static int32_t start_plan(Connection *connection, PendingPlan pending, size_t root) {
    connection->pending = pending;
    if (!kable_scope_start(pending.scope, root, &connection->listener->loop,
        pending.deadline, connection->phase == PARKED || connection->phase == RECEIVING, scope_joined, connection)) {
        scope_joined(connection);
    }
    return 0;
}

int32_t kable_tcp_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    (void)flags;
    Connection *connection = (Connection *)raw;
    if (socket_closing(connection)) return UV_ECANCELED;
    switch (connection->phase) {
    case IDLE:
        if (connection->listener->stopping) return UV_ECANCELED;
        connection->cancelled = 0;
        connection->phase = SCOPED;
        break;
    case STREAMING:
    case FINISHING:
        if (connection->cancelled) return UV_ECANCELED;
        if (connection->pending.plan) return UV_EBUSY;
        break;
    default:
        return UV_ECANCELED;
    }
    return start_plan(connection, (PendingPlan){ .scope = scope,
        .plan = plan, .finish = finish, .host = raw }, root);
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

static int watch_peer(Connection *connection) {
    return connection->read_eof ? 0 :
        uv_read_start((uv_stream_t *)&connection->socket, allocate_read, parked_read);
}

int32_t kable_tcp_schedule_timed(uint8_t *raw, KableTcpTimedPlan mode, uint8_t *scope,
    size_t root, uint8_t *plan, KablePlanFinish finish, uint8_t flags, uint8_t *host, uint64_t deadline) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping || socket_closing(connection)) {
        return UV_ECANCELED;
    }
    if (mode == KABLE_TCP_DEFER_REPLY) {
        int status = watch_peer(connection);
        if (status < 0) return status;
    }
    connection->cancelled = 0;
    connection->phase = mode == KABLE_TCP_RECEIVE ? RECEIVING : PARKED;
    return start_plan(connection, (PendingPlan){ .scope = scope,
        .plan = plan, .finish = finish, .host = host, .deadline = deadline, .flags = flags }, root);
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

static int start_finish_timer(Connection *connection, uint64_t ms) {
    uv_loop_t *loop = &connection->listener->loop;
    uv_update_time(loop);
    return kable_timer_start(loop, ms, finish_expired, finish_closed, connection, &connection->timer);
}

int32_t kable_tcp_finish_begin(uint8_t *raw, uint64_t ms) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping || socket_closing(connection)) {
        return UV_ECANCELED;
    }
    int status = start_finish_timer(connection, ms);
    if (status < 0) return status;
    connection->phase = FINISHING;
    return 0;
}

int32_t kable_tcp_stream_begin(uint8_t *raw, uint64_t ms) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping) return UV_ECANCELED;
    int status = start_finish_timer(connection, ms);
    if (status < 0) return status;
    status = watch_peer(connection);
    if (status < 0) {
        kable_timer_cancel(connection->timer);
        return status;
    }
    connection->cancelled = 0;
    connection->phase = STREAMING;
    connection->stream_idle_ms = ms;
    return 0;
}

int32_t kable_tcp_stream_end(uint8_t *raw) {
    Connection *connection = (Connection *)raw;
    if ((connection->phase != STREAMING && connection->phase != FINISHING) || connection->cancelled) {
        return UV_ECANCELED;
    }
    if (connection->pending.plan) return UV_EBUSY;
    end_stream(connection, 1);
    return 0;
}

static void cancel_connection(uv_handle_t *handle, void *context) {
    Listener *listener = context;
    if (handle->type != UV_TCP || handle == (uv_handle_t *)&listener->socket ||
        uv_is_closing(handle)) return;

    Connection *connection = handle->data;
    if (connection->phase != FINISHING) close_connection(connection);
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
    if (status == 0 && listener->connections <= MAX_CONNECTIONS) {
        status = listener->accept(listener->context, (uint8_t *)connection);
        if (status >= 0) return;
    }
    close_connection(connection);
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

uint8_t *kable_tcp_bind(uint16_t port, uint8_t *context, KableTcpAccept accept, int32_t *error) {
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
    listener->accept = accept;
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
