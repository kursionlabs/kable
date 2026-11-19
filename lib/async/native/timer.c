#include "timer.h"

#include <stdlib.h>

struct KableTimer {
    uv_timer_t handle;
    void (*ready)(void *);
    void (*closed)(void *, int);
    void *context;
    int fired;
};

static void timer_closed(uv_handle_t *handle) {
    KableTimer *timer = handle->data;
    if (timer->closed) timer->closed(timer->context, timer->fired);
    free(timer);
}

void kable_timer_cancel(KableTimer *timer) {
    if (timer && !uv_is_closing((uv_handle_t *)&timer->handle)) {
        uv_close((uv_handle_t *)&timer->handle, timer_closed);
    }
}

static void timer_ready(uv_timer_t *handle) {
    KableTimer *timer = handle->data;
    if (uv_is_closing((uv_handle_t *)handle)) return;
    timer->fired = 1;
    kable_timer_cancel(timer);
    if (timer->ready) timer->ready(timer->context);
}

int kable_timer_start(uv_loop_t *loop, uint64_t ms, void (*ready)(void *),
    void (*closed)(void *, int), void *context, KableTimer **out) {
    KableTimer *timer = calloc(1, sizeof *timer);
    if (!timer) return UV_ENOMEM;
    int status = uv_timer_init(loop, &timer->handle);
    if (status < 0) {
        free(timer);
        return status;
    }
    timer->handle.data = timer;
    status = uv_timer_start(&timer->handle, timer_ready, ms, 0);
    if (status < 0) {
        kable_timer_cancel(timer);
        return status;
    }
    timer->ready = ready;
    timer->closed = closed;
    timer->context = context;
    *out = timer;
    return 0;
}
