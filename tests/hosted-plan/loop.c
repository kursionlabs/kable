#include <stdint.h>
#include <uv.h>

static int32_t init_error;
static unsigned init_count;
static unsigned close_count;

int __real_uv_loop_init(uv_loop_t *loop);
int __real_uv_loop_close(uv_loop_t *loop);

int __wrap_uv_loop_init(uv_loop_t *loop) {
    init_count++;
    return init_error ? init_error : __real_uv_loop_init(loop);
}

int __wrap_uv_loop_close(uv_loop_t *loop) {
    close_count++;
    return __real_uv_loop_close(loop);
}

void kable_test_loop_begin(int32_t error) {
    init_error = error;
    init_count = 0;
    close_count = 0;
}

uint8_t kable_test_loop_end(uint32_t expected_closes) {
    init_error = 0;
    return init_count == 1 && close_count == expected_closes;
}
