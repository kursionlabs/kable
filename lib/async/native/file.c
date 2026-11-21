#include "file.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <uv.h>

#if defined(__linux__) && __has_include(<linux/openat2.h>)
#include <linux/openat2.h>
#include <sys/syscall.h>
#endif

enum { READ_CHUNK = KABLE_CREATE_FILE + 1, WRITE_CHUNK };
enum { READ_BATCH = 16384 };

typedef struct {
    uv_work_t request;
    void (*done)(uint8_t *);
    uint8_t *context;
    const char *path;
    uint8_t *data;
    union {
        uint8_t *output;
        const uint8_t *input;
    };
    size_t len;
    size_t capacity;
    size_t limit;
    uint8_t mode;
    int fd;
    uint64_t offset;
    uint64_t size;
    atomic_bool cancelled;
    int status;
} KableFile;

static KableFile *new_file(size_t limit, uint8_t mode) {
    KableFile *file = calloc(1, sizeof *file);
    if (!file) return NULL;
    atomic_init(&file->cancelled, 0);
    file->limit = limit;
    file->mode = mode;
    file->fd = -1;
    return file;
}

uint8_t *kable_file_new(const uint8_t *path, size_t len, size_t limit, uint8_t mode) {
    KableFile *file = new_file(limit, mode);
    if (!file) return NULL;
    if (mode > KABLE_CREATE_FILE || !len || path[len - 1] != 0 || memchr(path, 0, len - 1)) {
        file->status = UV_EINVAL;
    } else {
        file->path = (const char *)path;
    }
    return (uint8_t *)file;
}

uint8_t *kable_file_chunk(int32_t fd, uint64_t offset, uint8_t *bytes, size_t limit) {
    KableFile *file = new_file(limit, READ_CHUNK);
    if (file) {
        file->fd = fd;
        file->offset = offset;
        file->output = bytes;
    }
    return (uint8_t *)file;
}

uint8_t *kable_file_write_chunk(int32_t fd, uint64_t offset, const uint8_t *bytes, size_t len) {
    KableFile *file = new_file(len, WRITE_CHUNK);
    if (file) {
        file->fd = fd;
        file->offset = offset;
        file->input = bytes;
    }
    return (uint8_t *)file;
}

static int grow_read_buffer(KableFile *file) {
    size_t capacity = file->limit - file->capacity < file->capacity
        ? file->limit : file->capacity * 2;
    if (!capacity) {
        capacity = file->size && file->size < READ_BATCH ? (size_t)file->size : READ_BATCH;
        if (capacity > file->limit) capacity = file->limit;
    }
    uint8_t *data = realloc(file->data, capacity);
    if (!data) return UV_ENOMEM;
    file->data = data;
    file->capacity = capacity;
    return 0;
}

static int read_contents(KableFile *file, int fd) {
    if (file->size > file->limit) return UV_EFBIG;

    while (!atomic_load_explicit(&file->cancelled, memory_order_relaxed)) {
        /* If the size hint cannot be reserved, probe before requiring memory. */
        if (!file->capacity && file->size) (void)grow_read_buffer(file);
        size_t room = file->capacity - file->len;
        size_t wanted = room < READ_BATCH ? room : READ_BATCH;
        uint8_t probe;
        uint8_t *target = room ? file->data + file->len : &probe;
        ssize_t count = read(fd, target, room ? wanted : 1);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return uv_translate_sys_error(errno);
        if (count == 0) return 0;
        if (!room) {
            /* A byte beyond the current capacity distinguishes growth from EOF. */
            if (file->len == file->limit) return UV_EFBIG;
            int status = grow_read_buffer(file);
            if (status < 0) return status;
            file->data[file->len] = probe;
        }
        file->len += (size_t)count;
    }
    return UV_ECANCELED;
}

static int open_file(KableFile *file) {
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    if (file->mode == KABLE_READ_FILE || file->mode == KABLE_OPEN_FILE) return open(file->path, flags);
    if (file->mode == KABLE_CREATE_FILE) {
        return open(file->path, O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    }

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

/* The plan owns the descriptor and destination until the worker retires. */
static int read_chunk(KableFile *file) {
    if (file->limit > SSIZE_MAX) return UV_EINVAL;
    if (!file->output) return UV_ENOMEM;
    if (!file->limit) return 0;
    do {
        ssize_t count = pread(file->fd, file->output, file->limit, (off_t)file->offset);
        if (count >= 0) {
            file->len = (size_t)count;
            return 0;
        }
        if (errno != EINTR) return uv_translate_sys_error(errno);
    } while (!atomic_load_explicit(&file->cancelled, memory_order_relaxed));
    return UV_ECANCELED;
}

static int write_chunk(KableFile *file) {
    if (file->offset > INT64_MAX || file->limit > INT64_MAX - file->offset) return UV_EFBIG;
    uint64_t end = file->offset + file->limit;
    if ((off_t)end < 0 || (uint64_t)(off_t)end != end) return UV_EFBIG;

    while (file->len < file->limit) {
        if (atomic_load_explicit(&file->cancelled, memory_order_relaxed)) return UV_ECANCELED;
        size_t remaining = file->limit - file->len;
        size_t count = remaining < SSIZE_MAX ? remaining : SSIZE_MAX;
        ssize_t written = pwrite(file->fd, file->input + file->len, count, (off_t)(file->offset + file->len));
        if (written < 0 && errno == EINTR) continue;
        if (written < 0) return uv_translate_sys_error(errno);
        if (!written) return UV_EIO;
        file->len += (size_t)written;
    }
    return 0;
}

static int retains_descriptor(KableFile *file) {
    return file->mode == KABLE_OPEN_BENEATH || file->mode == KABLE_OPEN_FILE || file->mode == KABLE_CREATE_FILE;
}

static void file_work(uv_work_t *request) {
    KableFile *file = request->data;
    if (file->status < 0 || atomic_load_explicit(&file->cancelled, memory_order_relaxed)) {
        return;
    }

    if (file->mode == READ_CHUNK) {
        file->status = read_chunk(file);
        return;
    }
    if (file->mode == WRITE_CHUNK) {
        file->status = write_chunk(file);
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
        if (retains_descriptor(file)) {
            file->fd = fd;
            return;
        }
        file->status = read_contents(file, fd);
    }
    if (close(fd) != 0 && file->status == 0) file->status = uv_translate_sys_error(errno);
}

static void file_done(uv_work_t *request, int status) {
    KableFile *file = request->data;
    request->data = NULL;
    if (status < 0) file->status = status;
    file->done(file->context);
}

int32_t kable_file_start(uint8_t *raw, uint8_t *loop, void (*done)(uint8_t *), uint8_t *context) {
    KableFile *file = (KableFile *)raw;
    file->done = done;
    file->context = context;
    /* Marks the request as in flight until after-work runs. */
    file->request.data = file;
    int status = uv_queue_work((uv_loop_t *)loop, &file->request, file_work, file_done);
    if (status < 0) file->request.data = NULL;
    return status;
}

uint8_t kable_file_stop(uint8_t *raw) {
    KableFile *file = (KableFile *)raw;
    atomic_store_explicit(&file->cancelled, 1, memory_order_relaxed);
    uv_cancel((uv_req_t *)&file->request);
    /* The after-work callback retires both queued and running requests. */
    return 1;
}

int32_t kable_file_result(uint8_t *raw, uint8_t **data, size_t *len) {
    KableFile *file = (KableFile *)raw;
    if (!file || file->request.data) return UV_EINVAL;
    if (file->status == 0) {
        if (data) *data = file->data;
        *len = file->len;
    }
    return file->status;
}

int32_t kable_file_write_result(uint8_t *raw, size_t *written) {
    KableFile *file = (KableFile *)raw;
    *written = 0;
    if (!file || file->request.data || file->mode != WRITE_CHUNK) return UV_EINVAL;
    *written = file->len;
    return file->status;
}

int32_t kable_file_take_descriptor(uint8_t *raw, uint64_t *len) {
    KableFile *file = (KableFile *)raw;
    if (!file || file->request.data) return UV_EINVAL;
    if (file->status < 0) return file->status;
    if (!retains_descriptor(file) || file->fd < 0) return UV_EINVAL;
    int fd = file->fd;
    file->fd = -1;
    *len = file->size;
    return fd;
}

int32_t kable_file_close_descriptor(int32_t fd) {
    return close(fd) == 0 ? 0 : uv_translate_sys_error(errno);
}

/* Matches FileErrorKind's native tags in lib.kur. */
uint8_t kable_file_error_kind(int32_t code) {
    switch (code) {
    case UV_ENOENT: return 1;
    case UV_ENOTDIR: return 2;
    case UV_ELOOP: return 3;
    case UV_EXDEV: return 4;
    case UV_EINVAL: return 5;
    case UV_EACCES: case UV_EPERM: return 6;
    case UV_EFBIG: return 7;
    default: return 0;
    }
}

void kable_file_free(uint8_t *raw) {
    KableFile *file = (KableFile *)raw;
    if (!file) return;
    if (file->request.data) abort();
    if (retains_descriptor(file) && file->fd >= 0) close(file->fd);
    free(file->data);
    free(file);
}
