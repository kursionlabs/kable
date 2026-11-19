#ifndef KABLE_ASYNC_SCOPE_H
#define KABLE_ASYNC_SCOPE_H

#include <stdint.h>
#include <uv.h>

/* Returns 1 for pending work and calls done after the last resource joins.
   Returns 0 if the scope can be collected immediately, without a callback. */
int kable_scope_start(uint8_t *raw, size_t root, uv_loop_t *loop, uint64_t ms,
    uint8_t timed, void (*done)(void *), void *context);
void kable_scope_cancel(uint8_t *raw);

#endif
