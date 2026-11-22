#include <stdint.h>
#include <stdlib.h>
#include <uv.h>

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
} Client;

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

static int32_t accept_request(uint8_t *context, uint8_t *connection) {
    Client *client = malloc(sizeof *client);
    if (!client) return UV_ENOMEM;

    Server *server = (Server *)context;
    client->connection = connection;
    kable_request_init(&client->request, server->validate_head);
    client->deadline = kable_now_millis() + READ_TIMEOUT;
    kable_tcp_attach(connection, (uint8_t *)client, destroy_client);
    return server->receive(server->context, (uint8_t *)client);
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
