#include <stdint.h>
#include <uv.h>

static int32_t init_error;
static unsigned init_count;
static unsigned close_count;
static uint64_t now;
static uint64_t advance_on_init;

static uv_idle_t deferred_close;
static void (*deferred_done)(uint8_t *);
static uint8_t *deferred_context;
static uint8_t deferred_retired;

int __real_uv_loop_init(uv_loop_t *loop);
int __real_uv_loop_close(uv_loop_t *loop);
uint64_t __real_uv_hrtime(void);

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
    return advance_on_init ? now : __real_uv_hrtime();
}

void kable_test_loop_begin(int32_t error) {
    init_error = error;
    init_count = 0;
    close_count = 0;
}

void kable_test_advance_clock_on_init(uint64_t nanoseconds) {
    now = 1000000000;
    advance_on_init = nanoseconds;
}

uint8_t kable_test_loop_end(uint32_t expected_closes) {
    init_error = 0;
    advance_on_init = 0;
    return init_count == 1 && close_count == expected_closes;
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
