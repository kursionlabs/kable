// Forces short writes, with optional failure or a gate after the first write.
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <unistd.h>

static pthread_once_t lookup = PTHREAD_ONCE_INIT;
static ssize_t (*system_pwrite)(int, const void *, size_t, off_t);
static atomic_uint calls;

static void resolve_pwrite(void) {
    system_pwrite = (ssize_t (*)(int, const void *, size_t, off_t))dlsym(RTLD_NEXT, "pwrite");
    if (!system_pwrite) abort();
}

ssize_t pwrite(int fd, const void *bytes, size_t len, off_t offset) {
    pthread_once(&lookup, resolve_pwrite);
    unsigned call = atomic_fetch_add(&calls, 1);
    if (call && getenv("KABLE_TEST_WRITE_FAIL")) {
        errno = ENOSPC;
        return -1;
    }
    size_t count = len < 4093 ? len : 4093;
    ssize_t written = system_pwrite(fd, bytes, count, offset);
    const char *started = getenv("KABLE_TEST_WRITE_STARTED");
    const char *release = getenv("KABLE_TEST_WRITE_RELEASE");
    if (!call && started && release) {
        char token;
        if (write(atoi(started), "W", 1) != 1 || read(atoi(release), &token, 1) != 1) abort();
    }
    return written;
}
