#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <uv.h>

#include "../../async/native/timer.h"
#include "../../tcp/native/transport.h"
#include "request.h"

enum { READ_TIMEOUT = 10000 };

typedef int32_t (*receive_fn)(uint8_t *context, uint8_t *client);

typedef struct {
    uint8_t *listener;
    KableValidateHead validate_head;
    receive_fn receive;
    uint8_t *context;
} Server;

typedef struct {
    uint8_t *connection;
    KableRequest request;
    uint64_t deadline;
    uint64_t key;
} Client;

static _Atomic uint64_t next_key = 1;

static uint64_t request_key(void) {
    uint64_t key = atomic_load_explicit(&next_key, memory_order_relaxed);
    while (key && !atomic_compare_exchange_weak_explicit(&next_key, &key, key + 1,
        memory_order_relaxed, memory_order_relaxed)) {}
    return key;
}

static void destroy_client(uint8_t *attachment) {
    Client *client = (Client *)attachment;
    kable_release_request(&client->request);
    free(client);
}

KableRequest *kable_client_request(uint8_t *raw) {
    return &((Client *)raw)->request;
}

uint8_t *kable_client_connection(uint8_t *raw) {
    return ((Client *)raw)->connection;
}

uint64_t kable_client_key(uint8_t *raw) {
    return ((Client *)raw)->key;
}

int32_t kable_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    Client *client = (Client *)raw;
    uint64_t key = kable_scope_host_key(scope);
    KableTcpTimedPlan mode = client->request.complete ? KABLE_TCP_DEFER_REPLY : KABLE_TCP_DEFER_INPUT;
    if (key) {
        if (key != client->key) return UV_EINVAL;
        kable_scope_set_host(scope, raw);
        mode = KABLE_TCP_RECEIVE;
    }
    return kable_tcp_schedule_timed(client->connection, mode,
        scope, root, plan, finish, flags, raw, client->deadline);
}

int32_t kable_receive_schedule(uint8_t *raw, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags) {
    (void)flags;
    Client *client = (Client *)raw;
    return kable_tcp_schedule_timed(client->connection, KABLE_TCP_RECEIVE,
        scope, root, plan, finish, 0, raw, client->deadline);
}

static int32_t accept_request(uint8_t *context, uint8_t *connection) {
    Client *client = malloc(sizeof *client);
    if (!client) return UV_ENOMEM;

    Server *server = (Server *)context;
    client->connection = connection;
    client->key = request_key();
    if (!client->key) {
        free(client);
        return UV_EOVERFLOW;
    }
    kable_request_init(&client->request, server->validate_head);
    client->deadline = kable_now_millis() + READ_TIMEOUT;
    kable_tcp_attach(connection, (uint8_t *)client, destroy_client);
    return server->receive(server->context, (uint8_t *)client);
}

typedef struct {
    uint8_t *scope;
    Client *client;
    uint8_t *loop;
    void (*done)(uint8_t *);
    uint8_t *context;
    KableTimer *timer;
    KableRequestBytes output;
    size_t limit;
    int collect;
    int cancelled;
    int status;
} BodyPull;

uint8_t *kable_body_new(uint8_t *scope, size_t limit, uint8_t collect) {
    if (!scope) return NULL;
    BodyPull *pull = calloc(1, sizeof *pull);
    if (!pull) {
        kable_scope_fail_allocation(scope);
        return NULL;
    }
    pull->scope = scope;
    pull->limit = limit;
    pull->collect = collect;
    return (uint8_t *)pull;
}

static void body_read(uint8_t *context);

static void continue_body(BodyPull *pull, uint16_t result) {
    KableRequest *request = &pull->client->request;
    if (result == REQUEST_MORE && (pull->collect || !request->body.len)) {
        int status = kable_tcp_start_scoped_read(pull->client->connection, pull->loop,
            body_read, (uint8_t *)pull);
        if (status == 0) return;
        pull->status = status;
    } else if (result >= 400) {
        request->rejection = result;
        pull->status = result;
    } else {
        pull->output = request->body;
        request->body = (KableRequestBytes){0};
    }
    pull->done(pull->context);
}

static void body_read(uint8_t *context) {
    BodyPull *pull = (BodyPull *)context;
    uint8_t *bytes;
    int32_t count = kable_tcp_read_result(pull->client->connection, &bytes);
    if (count < 0) {
        pull->status = count;
        pull->done(pull->context);
        return;
    }
    KableRequest *request = &pull->client->request;
    uint16_t result = count ? kable_request_feed(request, bytes, (size_t)count) : 400;
    continue_body(pull, result);
}

static void body_started(void *context, int fired) {
    BodyPull *pull = context;
    pull->timer = NULL;
    if (!fired || pull->cancelled) {
        pull->done(pull->context);
        return;
    }
    KableRequest *request = &pull->client->request;
    request->body_limit = pull->limit;
    continue_body(pull, request->rejection ? request->rejection : kable_request_resume(request));
}

int32_t kable_body_start(uint8_t *raw, uint8_t *loop, void (*done)(uint8_t *), uint8_t *context) {
    BodyPull *pull = (BodyPull *)raw;
    Client *client = (Client *)kable_scope_host(pull->scope);
    if (!client) return UV_EINVAL;
    pull->client = client;
    pull->loop = loop;
    pull->done = done;
    pull->context = context;
    return kable_timer_start((uv_loop_t *)loop, 0, NULL, body_started, pull, &pull->timer);
}

uint8_t kable_body_stop(uint8_t *raw) {
    BodyPull *pull = (BodyPull *)raw;
    if (pull->timer) {
        pull->cancelled = 1;
        kable_timer_cancel(pull->timer);
        return 1;
    }
    return kable_tcp_stop_scoped_read(pull->client->connection);
}

int32_t kable_body_result(uint8_t *raw, uint8_t **bytes, size_t *length) {
    BodyPull *pull = (BodyPull *)raw;
    *bytes = pull->output.data;
    *length = pull->output.len;
    return pull->status;
}

void kable_body_free(uint8_t *raw) {
    BodyPull *pull = (BodyPull *)raw;
    if (!pull) return;
    free(pull->output.data);
    free(pull);
}

uint8_t *kable_bind(uint16_t port, KableValidateHead validate_head, receive_fn receive,
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
