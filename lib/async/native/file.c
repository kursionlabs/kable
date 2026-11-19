#include "file.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct KableFile {
    uv_work_t request;
    void (*done)(void *);
    void *context;
    char *path;
    uint8_t *data;
    size_t len;
    size_t capacity;
    size_t limit;
    atomic_bool cancelled;
    int queued;
    int status;
};

KableFile *kable_file_new(const uint8_t *path, size_t len, size_t limit) {
    KableFile *file = calloc(1, sizeof *file);
    if (!file) return NULL;
    atomic_init(&file->cancelled, 0);
    file->limit = limit;
    if (len == SIZE_MAX || (len && memchr(path, 0, len))) {
        file->status = UV_EINVAL;
        return file;
    }

    file->path = malloc(len + 1);
    if (!file->path) {
        free(file);
        return NULL;
    }
    if (len) memcpy(file->path, path, len);
    file->path[len] = '\0';
    return file;
}

static int append(KableFile *file, uint8_t *bytes, size_t count) {
    if (count > file->limit - file->len) return UV_EFBIG;
    size_t need = file->len + count;
    if (need > file->capacity) {
        size_t capacity = file->limit - file->capacity < file->capacity
            ? file->limit : file->capacity * 2;
        if (capacity < need) capacity = need;
        uint8_t *data = realloc(file->data, capacity);
        if (!data) return UV_ENOMEM;
        file->data = data;
        file->capacity = capacity;
    }
    memcpy(file->data + file->len, bytes, count);
    file->len = need;
    return 0;
}

static int read_contents(KableFile *file, int fd) {
    struct stat info;
    if (fstat(fd, &info) != 0) return uv_translate_sys_error(errno);
    if (!S_ISREG(info.st_mode)) return UV_EINVAL;
    if (info.st_size > 0 && (uintmax_t)info.st_size > file->limit) return UV_EFBIG;

    uint8_t bytes[16384];
    while (!atomic_load_explicit(&file->cancelled, memory_order_relaxed)) {
        size_t remaining = file->limit - file->len;
        size_t wanted = remaining < sizeof bytes ? remaining + 1 : sizeof bytes;
        ssize_t count = read(fd, bytes, wanted);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return uv_translate_sys_error(errno);
        if (count == 0) return 0;
        int status = append(file, bytes, (size_t)count);
        if (status < 0) return status;
    }
    return UV_ECANCELED;
}

static void read_file(uv_work_t *request) {
    KableFile *file = request->data;
    if (file->status < 0 || atomic_load_explicit(&file->cancelled, memory_order_relaxed)) {
        return;
    }

    /* Avoid blocking on a pipe before fstat can reject it. */
    int fd = open(file->path, O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (fd < 0) {
        file->status = uv_translate_sys_error(errno);
        return;
    }
    file->status = read_contents(file, fd);
    if (close(fd) != 0 && file->status == 0) file->status = uv_translate_sys_error(errno);
}

static void read_done(uv_work_t *request, int status) {
    KableFile *file = request->data;
    file->queued = 0;
    if (status < 0) file->status = status;
    file->done(file->context);
}

int kable_file_start(KableFile *file, uv_loop_t *loop, void (*done)(void *), void *context) {
    file->done = done;
    file->context = context;
    file->request.data = file;
    int status = uv_queue_work(loop, &file->request, read_file, read_done);
    if (status == 0) file->queued = 1;
    return status;
}

void kable_file_cancel(KableFile *file) {
    atomic_store_explicit(&file->cancelled, 1, memory_order_relaxed);
    if (file->queued) uv_cancel((uv_req_t *)&file->request);
}

int kable_file_result(KableFile *file, uint8_t **data, size_t *len) {
    if (file->status == 0) {
        *data = file->data;
        *len = file->len;
    }
    return file->status;
}

void kable_file_free(KableFile *file) {
    if (!file) return;
    free(file->path);
    free(file->data);
    free(file);
}
