#ifndef KABLE_TCP_TRANSPORT_H
#define KABLE_TCP_TRANSPORT_H

#include <stddef.h>
#include <stdint.h>
#include <uv.h>

#include "../../async/native/scope.h"

enum KableTcpEvent {
    KABLE_TCP_CONNECTED, KABLE_TCP_BYTES, KABLE_TCP_TIMED_OUT, KABLE_TCP_END,
    KABLE_TCP_CANCELLED, KABLE_TCP_FAILED
};

/* The callback borrows the connection and bytes until it returns. A negative
   result fails the connection. A successful callback starts one operation
   or closes the connection. */
typedef int32_t (*KableTcpHandler)(uint8_t *, uint8_t *, uint8_t, uint8_t *, size_t, int32_t);

uint8_t *kable_tcp_bind(uint16_t port, uint8_t *context, KableTcpHandler handler, int32_t *error);
uint16_t kable_tcp_port(uint8_t *listener);
int32_t kable_tcp_run(uint8_t *listener);
void kable_tcp_stop(uint8_t *listener);

/* The connection owns one attachment through socket, timer, and plan cleanup. */
void kable_tcp_attach(uint8_t *connection, uint8_t *attachment, void (*destroy)(uint8_t *));
int32_t kable_tcp_start_scoped_read(uint8_t *connection, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context);
uint8_t kable_tcp_stop_scoped_read(uint8_t *connection);
uint8_t kable_tcp_read_result(uint8_t *connection, uint8_t **bytes, size_t *len, int32_t *error);
/* The plan keeps the bytes alive until the request has joined. */
uint8_t *kable_tcp_new_scoped_write(uint8_t *connection, uint8_t *bytes, size_t len);
int32_t kable_tcp_start_scoped_write(uint8_t *request, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context);
uint8_t kable_tcp_stop_scoped_write(uint8_t *request);
int32_t kable_tcp_collect_scoped_write(uint8_t *request);
/* Submits a plan on an idle connection or during a response.
   A successful submission owns the plan until finish runs after the scope joins. */
int32_t kable_tcp_schedule(uint8_t *connection, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags);
/* Watches the peer while the plan runs under an absolute deadline. The join
   callback borrows host and receives the supplied flags, plus ACTIVE when
   the socket can still reply. */
enum { KABLE_TCP_PLAN_ACTIVE = 1 };
/* Schedules one ingress read under the original request deadline. ACTIVE
   allows a reply when the socket is open, including after a deadline fires. */
int32_t kable_tcp_schedule_receive(uint8_t *connection, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t *host, uint64_t deadline);
int32_t kable_tcp_schedule_until(uint8_t *connection, uint8_t *scope, size_t root, uint8_t *plan,
    KablePlanFinish finish, uint8_t flags, uint8_t *host, uint64_t deadline);
/* Starts the fixed deadline for a final write, half-close, and drain.
   A read-side EOF still permits the write, and listener shutdown lets it finish. */
int32_t kable_tcp_finish_begin(uint8_t *connection, uint64_t ms);
/* Keeps the idle timer and peer read active across the response's scopes.
   A successful scoped write refreshes the idle deadline. */
int32_t kable_tcp_stream_begin(uint8_t *connection, uint64_t idle_ms);
/* Half-closes and drains after the final scope has joined. */
int32_t kable_tcp_stream_end(uint8_t *connection);
void kable_tcp_close(uint8_t *connection);

#endif
