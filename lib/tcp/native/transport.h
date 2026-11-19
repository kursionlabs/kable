#ifndef KABLE_TCP_TRANSPORT_H
#define KABLE_TCP_TRANSPORT_H

#include <stddef.h>
#include <stdint.h>

enum KableTcpEvent {
    KABLE_TCP_CONNECTED, KABLE_TCP_BYTES, KABLE_TCP_TIMED_OUT, KABLE_TCP_END,
    KABLE_TCP_CANCELLED, KABLE_TCP_FAILED, KABLE_TCP_WAKE
};

/* The callback borrows the connection and bytes until it returns. A negative
   result fails the connection. A successful callback starts one operation
   or closes the connection. */
typedef int32_t (*KableTcpHandler)(uint8_t *, uint8_t *, uint8_t, uint8_t *, size_t, int32_t);

uint8_t *kable_tcp_bind(uint16_t port, uint8_t *context, KableTcpHandler handler, int32_t *error);
uint16_t kable_tcp_port(uint8_t *listener);
int32_t kable_tcp_run(uint8_t *listener);
void kable_tcp_stop(uint8_t *listener);

/* The connection owns one attachment through its socket and timer cleanup. */
void kable_tcp_attach(uint8_t *connection, void *attachment, void (*destroy)(void *));
void *kable_tcp_attachment(uint8_t *connection);
uint64_t kable_tcp_now(uint8_t *connection);
int32_t kable_tcp_read(uint8_t *connection, uint64_t ms, uint8_t timed);
int32_t kable_tcp_write(uint8_t *connection, uint8_t *bytes, size_t len, uint64_t ms, uint8_t timed);
int32_t kable_tcp_delay(uint8_t *connection, uint64_t ms);
/* Copies the final response, then writes, half-closes, and drains until the
   deadline. A read-side EOF still permits this final write. */
int32_t kable_tcp_finish(uint8_t *connection, uint8_t *bytes, size_t len, uint64_t ms);
void kable_tcp_close(uint8_t *connection);

#endif
