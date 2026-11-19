#ifndef KABLE_TIMER_H
#define KABLE_TIMER_H

#include <stdint.h>
#include <uv.h>

typedef struct KableTimer KableTimer;

/* On success, ready runs at expiry and closed runs when the handle is released.
   Cancellation skips ready. Either callback may be NULL. On error neither runs. */
int kable_timer_start(uv_loop_t *loop, uint64_t ms, void (*ready)(void *),
    void (*closed)(void *, int), void *context, KableTimer **out);
/* Rearms a live timer using the current loop time. */
int kable_timer_restart(KableTimer *timer, uint64_t ms);
void kable_timer_cancel(KableTimer *timer);

#endif
