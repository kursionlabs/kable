#include <assert.h>

/* Check slot counts and reuse the adapter's private status codes. */
#include "../lib/async/native/scope.c"

static int fail_next_realloc;
static int fail_next_file;
static int track_next_file;
static KableFile *tracked_file;
static unsigned released_files;

void *__real_realloc(void *ptr, size_t size);
KableFile *__real_kable_file_chunk(int fd, uint64_t offset, size_t limit);
void __real_kable_file_free(KableFile *file);

void *__wrap_realloc(void *ptr, size_t size) {
    if (fail_next_realloc) {
        fail_next_realloc = 0;
        return NULL;
    }
    return __real_realloc(ptr, size);
}

KableFile *__wrap_kable_file_chunk(int fd, uint64_t offset, size_t limit) {
    if (fail_next_file) {
        fail_next_file = 0;
        return NULL;
    }
    KableFile *file = __real_kable_file_chunk(fd, offset, limit);
    if (track_next_file) {
        track_next_file = 0;
        assert(file);
        tracked_file = file;
    }
    return file;
}

void __wrap_kable_file_free(KableFile *file) {
    if (tracked_file && file == tracked_file) {
        tracked_file = NULL;
        released_files++;
    }
    __real_kable_file_free(file);
}

typedef struct {
    size_t count;
    size_t leaf_attempts;
    int reject_leaf;
    unsigned destroyed;
} Policy;

static size_t add_leaf(uint8_t *raw) {
    Policy *policy = (Policy *)raw;
    policy->leaf_attempts++;
    if (policy->reject_leaf) return SIZE_MAX;
    return policy->count++;
}

static size_t add_pair(uint8_t *raw, uint8_t kind, size_t first, size_t second) {
    Policy *policy = (Policy *)raw;
    assert(kind == 1 && first == 0 && second == 1);
    return policy->count++;
}

static size_t scope_event(uint8_t *raw, uint8_t event, size_t id) {
    Policy *policy = (Policy *)raw;
    (void)id;
    assert(event == PENDING || event == DESTROY);
    if (event == DESTROY) policy->destroyed++;
    return 0;
}

static uint8_t *new_scope(Policy *policy) {
    uint8_t *scope = kable_scope_new((uint8_t *)policy, add_leaf, add_pair, scope_event);
    assert(scope);
    return scope;
}

static void check_failed_scope(uint8_t *raw, Policy *policy, int32_t expected) {
    assert(((Scope *)raw)->count == policy->count);
    kable_scope_run(raw, 0, 0, 0);
    assert(kable_scope_result(raw, 0) == expected);
    assert(kable_scope_result(raw, SIZE_MAX) == expected);
    kable_scope_free(raw);
    assert(policy->destroyed == 1);
}

static void check_slot_growth_failure(void) {
    Policy policy = {0};
    uint8_t *scope = new_scope(&policy);
    assert(kable_scope_add_timer(scope, 0) == 0);

    fail_next_realloc = 1;
    size_t next = 1;
    size_t id;
    while ((id = kable_scope_add_timer(scope, 0)) != SIZE_MAX) {
        assert(id == next);
        next++;
    }
    assert(!fail_next_realloc);
    assert(policy.leaf_attempts == next && policy.count == next);
    check_failed_scope(scope, &policy, UV_ENOMEM);
}

static void check_file_allocation_failure(void) {
    Policy policy = {0};
    uint8_t *scope = new_scope(&policy);
    assert(kable_scope_add_timer(scope, 0) == 0);
    assert(kable_scope_add_timer(scope, 0) == 1);
    assert(kable_scope_add_pair(scope, 1, 0, 1) == 2);

    fail_next_file = 1;
    assert(kable_scope_add_chunk(scope, -1, 0, 10) == SIZE_MAX);
    assert(!fail_next_file);
    assert(policy.leaf_attempts == 2 && policy.count == 3);
    check_failed_scope(scope, &policy, UV_ENOMEM);
}

static void check_rejected_file(void) {
    Policy policy = { .reject_leaf = 1 };
    uint8_t *scope = new_scope(&policy);

    track_next_file = 1;
    assert(kable_scope_add_chunk(scope, -1, 0, 10) == SIZE_MAX);
    assert(!track_next_file && !tracked_file && released_files == 1);
    assert(policy.leaf_attempts == 1 && policy.count == 0);
    check_failed_scope(scope, &policy, SCOPE_INVALID_PLAN);
    assert(released_files == 1);
}

int main(void) {
    check_slot_growth_failure();
    check_file_allocation_failure();
    check_rejected_file();
}
