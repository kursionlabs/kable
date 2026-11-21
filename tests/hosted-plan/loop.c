#include <stdlib.h>

#include "../../lib/async/native/scope.h"

static int32_t init_error;
static unsigned init_count;
static unsigned close_count;
static uint32_t scope_closes;
static uint8_t track_next_scope;
static void *tracked_scope;
static uint8_t fake_clock;
static uint64_t now;
static uint64_t advance_on_allocation;
static uint64_t advance_on_init;

static uv_idle_t deferred_close;
static void (*deferred_done)(uint8_t *);
static uint8_t *deferred_context;
static uint8_t deferred_retired;

enum { FILE_ALLOCATION = 1, FILE_REGISTRATION, FILE_START };
static uint8_t file_fault;
static uint8_t *tracked_file;
static uint8_t reject_file_slot;
static unsigned file_callocs;
static unsigned file_starts;
static unsigned file_releases;

int __real_uv_loop_init(uv_loop_t *loop);
int __real_uv_loop_close(uv_loop_t *loop);
uint64_t __real_uv_hrtime(void);
void *__real_calloc(size_t count, size_t size);
void __real_free(void *ptr);
void *__real_realloc(void *ptr, size_t size);
int __real_uv_queue_work(uv_loop_t *loop, uv_work_t *request, uv_work_cb work, uv_after_work_cb after_work);

int32_t kable_test_file_fault_begin(uint8_t fault) {
    file_fault = fault;
    file_callocs = file_starts = file_releases = 0;
    return fault == FILE_START ? UV_EIO : UV_ENOMEM;
}

uint8_t kable_test_file_fault_end(void) {
    int valid = file_callocs == 2 && file_starts == (file_fault == FILE_START) &&
        file_releases == (file_fault != FILE_ALLOCATION) && !tracked_file && !reject_file_slot;
    file_fault = 0;
    return valid;
}

void __wrap_free(void *ptr) {
    if (ptr && ptr == tracked_file) {
        tracked_file = NULL;
        file_releases++;
    }
    if (ptr && ptr == tracked_scope) {
        tracked_scope = NULL;
        scope_closes++;
    }
    __real_free(ptr);
}

void *__wrap_realloc(void *ptr, size_t size) {
    if (reject_file_slot) {
        reject_file_slot = 0;
        return NULL;
    }
    return __real_realloc(ptr, size);
}

int __wrap_uv_queue_work(uv_loop_t *loop, uv_work_t *request, uv_work_cb work, uv_after_work_cb after_work) {
    if (file_fault) file_starts++;
    if (file_fault == FILE_START) return UV_EIO;
    return __real_uv_queue_work(loop, request, work, after_work);
}

void kable_test_track_scope(void) {
    if (track_next_scope || tracked_scope) abort();
    track_next_scope = 1;
}

uint32_t kable_test_scope_closes(void) {
    return scope_closes;
}

void *__wrap_calloc(size_t count, size_t size) {
    now += advance_on_allocation;
    advance_on_allocation = 0;
    /* Running the read allocates the scope first and the worker request second. */
    if (file_fault && ++file_callocs == 2) {
        if (file_fault == FILE_ALLOCATION) return NULL;
        tracked_file = __real_calloc(count, size);
        if (file_fault == FILE_REGISTRATION) reject_file_slot = 1;
        return tracked_file;
    }
    void *ptr = __real_calloc(count, size);
    if (track_next_scope) {
        track_next_scope = 0;
        tracked_scope = ptr;
    }
    return ptr;
}

int __wrap_uv_loop_init(uv_loop_t *loop) {
    init_count++;
    int status = init_error ? init_error : __real_uv_loop_init(loop);
    if (status == 0) now += advance_on_init;
    return status;
}

int __wrap_uv_loop_close(uv_loop_t *loop) {
    close_count++;
    return __real_uv_loop_close(loop);
}

uint64_t __wrap_uv_hrtime(void) {
    return fake_clock ? now : __real_uv_hrtime();
}

void kable_test_loop_begin(int32_t error) {
    init_error = error;
    init_count = 0;
    close_count = 0;
}

void kable_test_advance_clock_on_init(uint64_t nanoseconds) {
    fake_clock = 1;
    now = 1000000000;
    advance_on_init = nanoseconds;
}

void kable_test_advance_clock(uint64_t nanoseconds) {
    now += nanoseconds;
}

void kable_test_advance_clock_on_allocation(uint64_t nanoseconds) {
    advance_on_allocation = nanoseconds;
}

uint8_t kable_test_loop_end(uint32_t expected_closes) {
    init_error = 0;
    fake_clock = 0;
    advance_on_allocation = 0;
    advance_on_init = 0;
    return init_count == 1 && close_count == expected_closes;
}

int32_t kable_test_schedule_immediate(uint8_t *host, uint8_t *scope, size_t root,
    uint8_t *plan, KablePlanFinish finish, uint8_t flags) {
    uv_loop_t loop;
    int status = uv_loop_init(&loop);
    if (status < 0) return status;

    if (kable_scope_start(scope, root, &loop, 0, 0, NULL, NULL)) abort();
    if (finish(plan, host, flags) != -17) abort();
    if (uv_loop_close(&loop) != 0) abort();
    return 0;
}

int32_t kable_test_deferred_start(uint8_t *operation, uint8_t *loop,
    void (*ready)(uint8_t *), uint8_t *context) {
    (void)operation;
    deferred_retired = 0;
    deferred_done = ready;
    deferred_context = context;
    return uv_idle_init((uv_loop_t *)loop, &deferred_close);
}

static void finish_deferred_close(uv_handle_t *handle) {
    (void)handle;
    deferred_retired = 1;
    deferred_done(deferred_context);
}

uint8_t kable_test_deferred_stop(uint8_t *operation) {
    (void)operation;
    uv_close((uv_handle_t *)&deferred_close, finish_deferred_close);
    return 1;
}

uint8_t kable_test_deferred_retired(void) {
    return deferred_retired;
}
