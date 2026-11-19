// Holds one read per selected file until the integration test releases it.
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static pthread_once_t lookup = PTHREAD_ONCE_INIT;
static ssize_t (*system_read)(int, void *, size_t);
static atomic_uint held;

static void resolve_read(void) {
    system_read = (ssize_t (*)(int, void *, size_t))dlsym(RTLD_NEXT, "read");
    if (!system_read) abort();
}

ssize_t read(int fd, void *buffer, size_t size) {
    pthread_once(&lookup, resolve_read);
    const char *started = getenv("KABLE_TEST_FILE_STARTED");
    const char *release = getenv("KABLE_TEST_FILE_RELEASE");
    const char *enabled = getenv("KABLE_TEST_FILE_GATES");
    struct stat input;
    if (started && release && enabled && fstat(fd, &input) == 0 && S_ISREG(input.st_mode)) {
        const char *paths[] = {"example.txt", "footer.txt", "footer-detailed.txt"};
        unsigned gates = (unsigned)atoi(enabled);
        for (unsigned slot = 0; slot < sizeof paths / sizeof paths[0]; slot++) {
            unsigned bit = 1u << slot;
            struct stat target;
            if (!(gates & bit) || (atomic_load(&held) & bit) || stat(paths[slot], &target) != 0
                || input.st_dev != target.st_dev || input.st_ino != target.st_ino
                || (atomic_fetch_or(&held, bit) & bit)) continue;
            char token = (char)('A' + slot);
            if (write(atoi(started), &token, 1) != 1 || system_read(atoi(release), &token, 1) != 1) {
                abort();
            }
            break;
        }
    }
    ssize_t count = system_read(fd, buffer, size);
    if (count < 0 && errno == ECONNRESET) {
        const char *reset = getenv("KABLE_TEST_TCP_RESET");
        if (reset && write(atoi(reset), "R", 1) != 1) abort();
        errno = ECONNRESET;
    }
    return count;
}
