// Records writes and queued file work for serial requests in the streaming test.
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

static uv_write_cb pending_write;
static unsigned writes;

static void write_done(uv_write_t *request, int status) {
    uv_write_cb callback = pending_write;
    pending_write = NULL;
    fputs(status == 0 ? "stream-test: wrote\n" : "stream-test: write failed\n", stderr);
    callback(request, status);
}

int uv_write(uv_write_t *request, uv_stream_t *stream, const uv_buf_t buffers[],
    unsigned int count, uv_write_cb callback) {
    int (*write_fn)(uv_write_t *, uv_stream_t *, const uv_buf_t *, unsigned int, uv_write_cb)
        = dlsym(RTLD_NEXT, "uv_write");
    if (!write_fn || pending_write) abort();
    if (getenv("KABLE_TEST_RELAY_BACKPRESSURE")) {
        int size = 4096;
        if (uv_send_buffer_size((uv_handle_t *)stream, &size) < 0) abort();
        return write_fn(request, stream, buffers, count, callback);
    }
    if (++writes == 2) {
        fputs("stream-test: write rejected\n", stderr);
        return UV_EIO;
    }
    pending_write = callback;
    int status = write_fn(request, stream, buffers, count, write_done);
    if (status < 0) pending_write = NULL;
    return status;
}

int uv_queue_work(uv_loop_t *loop, uv_work_t *request, uv_work_cb work, uv_after_work_cb done) {
    int (*queue_work)(uv_loop_t *, uv_work_t *, uv_work_cb, uv_after_work_cb)
        = dlsym(RTLD_NEXT, "uv_queue_work");
    if (!queue_work) abort();
    fputs("stream-test: queued file work\n", stderr);
    return queue_work(loop, request, work, done);
}
