// Holds one read of example.txt until the integration test releases it.
#include <dlfcn.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static pthread_once_t lookup = PTHREAD_ONCE_INIT;
static ssize_t (*system_read)(int, void *, size_t);
static atomic_bool held;

static void resolve_read(void) {
    system_read = (ssize_t (*)(int, void *, size_t))dlsym(RTLD_NEXT, "read");
    if (!system_read) abort();
}

ssize_t read(int fd, void *buffer, size_t size) {
    pthread_once(&lookup, resolve_read);
    const char *started = getenv("KABLE_TEST_FILE_STARTED");
    const char *release = getenv("KABLE_TEST_FILE_RELEASE");
    struct stat input;
    struct stat target;
    if (started && release && !atomic_load(&held) && fstat(fd, &input) == 0
        && S_ISREG(input.st_mode) && stat("example.txt", &target) == 0
        && input.st_dev == target.st_dev && input.st_ino == target.st_ino
        && !atomic_exchange(&held, 1)) {
        char permission;
        if (write(atoi(started), "R", 1) != 1 || system_read(atoi(release), &permission, 1) != 1) {
            abort();
        }
    }
    return system_read(fd, buffer, size);
}
