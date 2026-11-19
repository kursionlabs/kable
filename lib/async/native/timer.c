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

/* Standalone scopes drive up to two children and an optional parent deadline
   on a private loop. The HTTP server uses the same timer on its own loop. */
enum { DEADLINE, FIRST, SECOND, TIMER_COUNT };

typedef struct TimerScope TimerScope;

typedef struct {
    TimerScope *scope;
    int index;
    KableTimer *timer;
} Entry;

struct TimerScope {
    uv_loop_t loop;
    Entry entries[TIMER_COUNT];
    int result;
};

static void cancel_scope(TimerScope *scope) {
    for (int i = 0; i < TIMER_COUNT; i++) {
        kable_timer_cancel(scope->entries[i].timer);
    }
}

static void select_timer(void *context) {
    Entry *entry = context;
    entry->scope->result = entry->index;
    cancel_scope(entry->scope);
}

static void entry_closed(void *context, int fired) {
    (void)fired;
    Entry *entry = context;
    entry->timer = NULL;
}

static int start_entry(TimerScope *scope, int index, uint64_t ms) {
    Entry *entry = &scope->entries[index];
    entry->scope = scope;
    entry->index = index;
    return kable_timer_start(&scope->loop, ms, select_timer, entry_closed, entry, &entry->timer);
}

int32_t kable_async_drive(uint64_t first, uint64_t second, uint8_t has_second,
    uint64_t deadline, uint8_t has_deadline) {
    TimerScope scope = {0};
    int status = uv_loop_init(&scope.loop);
    if (status < 0) return status;

    if (has_deadline) status = start_entry(&scope, DEADLINE, deadline);
    if (status == 0) status = start_entry(&scope, FIRST, first);
    if (status == 0 && has_second) status = start_entry(&scope, SECOND, second);
    if (status < 0) cancel_scope(&scope);

    uv_run(&scope.loop, UV_RUN_DEFAULT);
    if (uv_loop_close(&scope.loop) != 0) abort();
    return status < 0 ? status : scope.result;
}
