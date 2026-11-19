#include <arpa/inet.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../../async/native/timer.h"

enum { CONNECTED, BYTES, TIMED_OUT, END, CANCELLED, FAILED };
enum { IDLE, READING, JOINING, WRITING, TERMINAL };
enum { MAX_CONNECTIONS = 128, READ_SIZE = 16384 };

typedef int32_t (*on_event)(uint8_t *, uint8_t *, uint8_t, uint8_t *, size_t, int32_t);

typedef struct {
    uv_loop_t loop;
    uv_tcp_t socket;
    uv_signal_t interrupt;
    uv_signal_t terminate;
    on_event handler;
    uint8_t *context;
    unsigned connections;
    int stopping;
    int status;
    uint16_t port;
} Listener;

typedef struct {
    uv_tcp_t socket;
    uv_write_t write;
    uv_buf_t output;
    Listener *listener;
    KableTimer *timer;
    uint64_t next_ms;
    uint8_t next_timed;
    int phase;
    uint8_t event;
    int error;
    size_t length;
    char input[READ_SIZE];
} Connection;

static void close_handle(uv_handle_t *handle, void *unused) {
    (void)unused;
    if (!uv_is_closing(handle)) uv_close(handle, NULL);
}

static void finish_shutdown(Listener *listener) {
    if (listener->stopping && listener->connections == 0) {
        uv_walk(&listener->loop, close_handle, NULL);
    }
}

static void connection_closed(uv_handle_t *handle) {
    Connection *connection = handle->data;
    Listener *listener = connection->listener;
    free(connection);
    listener->connections--;
    finish_shutdown(listener);
}

static void close_connection(Connection *connection) {
    uv_handle_t *handle = (uv_handle_t *)&connection->socket;
    if (!uv_is_closing(handle)) uv_close(handle, connection_closed);
}

void kable_tcp_close(uint8_t *raw) {
    close_connection((Connection *)raw);
}

static void notify(Connection *connection, uint8_t event, size_t length, int error) {
    Listener *listener = connection->listener;
    int terminal = event == END || event == CANCELLED || event == FAILED;
    connection->phase = terminal ? TERMINAL : IDLE;
    int status = listener->handler(listener->context, (uint8_t *)connection, event,
        (uint8_t *)connection->input, length, error);
    if (terminal) {
        close_connection(connection);
    } else if (status < 0) {
        notify(connection, FAILED, 0, status);
    }
}

static void report_read(Connection *connection) {
    if (connection->listener->stopping) {
        notify(connection, CANCELLED, 0, 0);
    } else {
        notify(connection, connection->event, connection->length, connection->error);
    }
}

/* Only the first completion can move a read into JOINING. The handler gets
   its connection back after the competing timer's close callback. */
static void complete_read(Connection *connection, uint8_t event, size_t length, int error) {
    if (connection->phase != READING) return;
    connection->phase = JOINING;
    connection->event = event;
    connection->length = length;
    connection->error = error;
    uv_read_stop((uv_stream_t *)&connection->socket);
    if (connection->timer) kable_timer_cancel(connection->timer);
    else report_read(connection);
}

static void timer_ready(void *context) {
    complete_read(context, TIMED_OUT, 0, 0);
}

static void timer_closed(void *context, int fired) {
    (void)fired;
    Connection *connection = context;
    connection->timer = NULL;
    report_read(connection);
}

static void allocate_read(uv_handle_t *handle, size_t suggested, uv_buf_t *buffer) {
    (void)suggested;
    Connection *connection = handle->data;
    *buffer = uv_buf_init(connection->input, sizeof connection->input);
}

static void read_done(uv_stream_t *stream, ssize_t count, const uv_buf_t *buffer) {
    (void)buffer;
    Connection *connection = stream->data;
    if (count > 0) complete_read(connection, BYTES, (size_t)count, 0);
    else if (count == UV_EOF) complete_read(connection, END, 0, 0);
    else if (count < 0) complete_read(connection, FAILED, 0, (int)count);
}

static int start_read(Connection *connection, uint64_t ms, uint8_t timed) {
    int status = uv_read_start((uv_stream_t *)&connection->socket, allocate_read, read_done);
    if (status < 0) return status;

    if (timed) {
        uv_update_time(&connection->listener->loop);
        status = kable_timer_start(&connection->listener->loop, ms, timer_ready,
            timer_closed, connection, &connection->timer);
        if (status < 0) {
            uv_read_stop((uv_stream_t *)&connection->socket);
            return status;
        }
    }

    connection->phase = READING;
    return 0;
}

int32_t kable_tcp_read(uint8_t *raw, uint64_t ms, uint8_t timed) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping) return UV_ECANCELED;
    return start_read(connection, ms, timed);
}

static void write_done(uv_write_t *write, int status) {
    Connection *connection = write->data;
    free(connection->output.base);
    connection->output.base = NULL;
    if (connection->listener->stopping) {
        notify(connection, CANCELLED, 0, 0);
    } else {
        if (status == 0) status = start_read(connection, connection->next_ms, connection->next_timed);
        if (status < 0) notify(connection, FAILED, 0, status);
    }
}

int32_t kable_tcp_write(uint8_t *raw, uint8_t *bytes, size_t len, uint64_t ms, uint8_t timed) {
    Connection *connection = (Connection *)raw;
    if (connection->phase != IDLE || connection->listener->stopping) return UV_ECANCELED;
    if (!len) return start_read(connection, ms, timed);
    if (len > UINT32_MAX) return UV_EINVAL;

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
        return status;
    }
    connection->next_ms = ms;
    connection->next_timed = timed;
    connection->phase = WRITING;
    return 0;
}

static void cancel_connection(uv_handle_t *handle, void *context) {
    Listener *listener = context;
    if (handle->type != UV_TCP || handle == (uv_handle_t *)&listener->socket ||
        uv_is_closing(handle)) return;

    Connection *connection = handle->data;
    if (connection->phase == READING) complete_read(connection, CANCELLED, 0, 0);
    else if (connection->phase != JOINING) close_connection(connection);
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
    (void)signum;
    stop_listener(signal->data, 0);
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
    else notify(connection, CONNECTED, 0, 0);
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

uint8_t *kable_tcp_bind(uint16_t port, uint8_t *context, on_event handler, int32_t *error) {
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
