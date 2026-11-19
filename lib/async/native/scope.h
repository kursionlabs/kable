#ifndef KABLE_ASYNC_SCOPE_H
#define KABLE_ASYNC_SCOPE_H

#include <stdint.h>
#include <uv.h>

/* Returns 1 for pending work and calls done after the last resource joins.
   Returns 0 if the scope can be collected immediately, without a callback. */
int kable_scope_start(uint8_t *raw, size_t root, uv_loop_t *loop, uint64_t ms,
    uint8_t timed, void (*done)(void *), void *context);
void kable_scope_cancel(uint8_t *raw);

/* A successful start completes asynchronously on loop. Stop retires it
   synchronously and suppresses the completion callback. */
typedef int32_t (*KableOperationStart)(uint8_t *, uint8_t *, void (*)(uint8_t *), uint8_t *);
typedef void (*KableOperationStop)(uint8_t *);
size_t kable_scope_add_operation(uint8_t *scope, uint8_t *context,
    KableOperationStart start, KableOperationStop stop);

#endif
