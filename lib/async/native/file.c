#include "file.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__) && __has_include(<linux/openat2.h>)
#include <linux/openat2.h>
#include <sys/syscall.h>
#endif

enum { READ_CHUNK = KABLE_OPEN_BENEATH + 1 };

struct KableFile {
    uv_work_t request;
    void (*done)(void *);
    void *context;
    char *path;
    uint8_t *data;
    size_t len;
    size_t capacity;
    size_t limit;
    uint8_t mode;
    int fd;
    uint64_t offset;
    uint64_t size;
    atomic_bool cancelled;
    int queued;
    int status;
};

static KableFile *new_file(size_t limit, uint8_t mode) {
    KableFile *file = calloc(1, sizeof *file);
    if (!file) return NULL;
    atomic_init(&file->cancelled, 0);
    file->limit = limit;
    file->mode = mode;
    file->fd = -1;
    return file;
}

KableFile *kable_file_new(const uint8_t *path, size_t len, size_t limit, uint8_t mode) {
    KableFile *file = new_file(limit, mode);
    if (!file) return NULL;
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

KableFile *kable_file_chunk(int fd, uint64_t offset, size_t limit) {
    KableFile *file = new_file(limit, READ_CHUNK);
    if (file) {
        file->fd = fd;
        file->offset = offset;
    }
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
    if (file->size > file->limit) return UV_EFBIG;

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

static int open_file(KableFile *file) {
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    if (file->mode == KABLE_READ_FILE) return open(file->path, flags);

#ifdef SYS_openat2
    struct open_how how = {
        .flags = flags,
        .resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
    };
    return (int)syscall(SYS_openat2, AT_FDCWD, file->path, &how, sizeof how);
#else
    errno = ENOSYS;
    return -1;
#endif
}

/* The plan owns the descriptor until the worker retires. */
static int read_chunk(KableFile *file) {
    if (!file->limit) return 0;
    if (file->limit > SSIZE_MAX) return UV_EINVAL;
    file->data = malloc(file->limit);
    if (!file->data) return UV_ENOMEM;
    do {
        ssize_t count = pread(file->fd, file->data, file->limit, (off_t)file->offset);
        if (count >= 0) {
            file->len = (size_t)count;
            return 0;
        }
        if (errno != EINTR) return uv_translate_sys_error(errno);
    } while (!atomic_load_explicit(&file->cancelled, memory_order_relaxed));
    return UV_ECANCELED;
}

static void read_file(uv_work_t *request) {
    KableFile *file = request->data;
    if (file->status < 0 || atomic_load_explicit(&file->cancelled, memory_order_relaxed)) {
        return;
    }

    if (file->mode == READ_CHUNK) {
        file->status = read_chunk(file);
        return;
    }

    /* Avoid blocking on a pipe before fstat can reject it. */
    int fd = open_file(file);
    if (fd < 0) {
        file->status = uv_translate_sys_error(errno);
        return;
    }
    struct stat info;
    if (fstat(fd, &info) != 0) {
        file->status = uv_translate_sys_error(errno);
    } else if (!S_ISREG(info.st_mode) || info.st_size < 0) {
        file->status = UV_EINVAL;
    } else {
        file->size = (uint64_t)info.st_size;
        if (file->mode == KABLE_OPEN_BENEATH) {
            file->fd = fd;
            return;
        }
        file->status = read_contents(file, fd);
    }
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

int kable_file_take_descriptor(KableFile *file, uint64_t *len) {
    if (file->status < 0) return file->status;
    if (file->mode != KABLE_OPEN_BENEATH || file->fd < 0) return UV_EINVAL;
    int fd = file->fd;
    file->fd = -1;
    *len = file->size;
    return fd;
}

void kable_file_close_descriptor(int32_t fd) { close(fd); }

void kable_file_free(KableFile *file) {
    if (!file) return;
    if (file->mode == KABLE_OPEN_BENEATH && file->fd >= 0) close(file->fd);
    free(file->path);
    free(file->data);
    free(file);
}
