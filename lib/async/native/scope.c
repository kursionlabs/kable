#include "file.h"
#include "scope.h"
#include "timer.h"

#include <stdlib.h>

enum { SLEEP, RACE, JOIN, READ_FILE, OPERATION };
/* Matches the event bridge in async/lib.kur. */
enum { BEGIN, STARTED, RETIRED, PENDING, COMPLETE, CANCEL, TAKE_CANCEL, RESULT, DESTROY };
enum { SCOPE_COMPLETED = 1 };

typedef size_t (*policy_add)(uint8_t *, uint8_t, size_t, size_t);
typedef size_t (*policy_event)(uint8_t *, uint8_t, size_t);

typedef struct Scope Scope;

typedef struct {
    Scope *scope;
    KableTimer *timer;
    KableFile *file;
    uint8_t *operation;
    KableOperationStart start;
    KableOperationStop stop;
    int active;
    size_t id;
    uint64_t ms;
    int kind;
} Node;

struct Scope {
    KableTimer *deadline;
    void (*done)(void *);
    void *context;
    Node *nodes;
    size_t count;
    size_t capacity;
    uint8_t *policy;
    policy_add add;
    policy_event event;
    int status;
    int driven;
};

uint8_t *kable_scope_new(uint8_t *policy, policy_add add, policy_event event) {
    Scope *scope = calloc(1, sizeof *scope);
    if (scope) {
        scope->policy = policy;
        scope->add = add;
        scope->event = event;
    }
    return (uint8_t *)scope;
}

static size_t report(Scope *scope, uint8_t event, size_t id) {
    return scope->event(scope->policy, event, id);
}

/* Plans register children before parents. The array stays fixed once driven. */
size_t kable_scope_add(uint8_t *raw, uint8_t kind, uint64_t ms, size_t first, size_t second) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven || scope->status < 0) return SIZE_MAX;

    if (scope->count == scope->capacity) {
        if (scope->capacity > SIZE_MAX / sizeof(Node) / 2) {
            scope->status = UV_ENOMEM;
            return SIZE_MAX;
        }
        size_t capacity = scope->capacity ? scope->capacity * 2 : 8;
        Node *nodes = realloc(scope->nodes, capacity * sizeof *nodes);
        if (!nodes) {
            scope->status = UV_ENOMEM;
            return SIZE_MAX;
        }
        scope->nodes = nodes;
        scope->capacity = capacity;
    }

    size_t index = scope->add(scope->policy, kind, first, second);
    if (index != scope->count) {
        scope->status = UV_EINVAL;
        return SIZE_MAX;
    }
    scope->count++;
    scope->nodes[index] = (Node){ .scope = scope, .kind = kind, .ms = ms, .id = index };
    return index;
}

size_t kable_scope_add_file(uint8_t *raw, uint8_t *path, size_t len, size_t limit, uint8_t beneath) {
    size_t index = kable_scope_add(raw, SLEEP, 0, 0, 0);
    if (index == SIZE_MAX) return index;
    Scope *scope = (Scope *)raw;
    KableFile *file = kable_file_new(path, len, limit, beneath);
    if (!file) {
        scope->status = UV_ENOMEM;
        return SIZE_MAX;
    }
    scope->nodes[index].kind = READ_FILE;
    scope->nodes[index].file = file;
    return index;
}

size_t kable_scope_add_operation(uint8_t *raw, uint8_t *context,
    KableOperationStart start, KableOperationStop stop) {
    size_t id = kable_scope_add(raw, SLEEP, 0, 0, 0);
    if (id != SIZE_MAX) {
        Node *node = &((Scope *)raw)->nodes[id];
        node->kind = OPERATION;
        node->operation = context;
        node->start = start;
        node->stop = stop;
    }
    return id;
}

static void cancel_operations(Scope *scope) {
    size_t id;
    while ((id = report(scope, TAKE_CANCEL, 0)) != SIZE_MAX) {
        Node *node = &scope->nodes[id];
        if (node->kind == SLEEP) kable_timer_cancel(node->timer);
        else if (node->kind == READ_FILE) kable_file_cancel(node->file);
        else if (node->kind == OPERATION && node->active) {
            node->active = 0;
            node->stop(node->operation);
            report(scope, RETIRED, 0);
        }
    }
}

static void deadline_ready(void *context) {
    Scope *scope = context;
    report(scope, CANCEL, 0);
    cancel_operations(scope);
}

static void joined(Scope *scope) {
    if (report(scope, RETIRED, 0) && scope->done) scope->done(scope->context);
}

static void deadline_closed(void *context, int fired) {
    (void)fired;
    Scope *scope = context;
    scope->deadline = NULL;
    joined(scope);
}

static void task_ready(void *context) {
    Node *node = context;
    Scope *scope = node->scope;
    if (report(scope, COMPLETE, node->id)) kable_timer_cancel(scope->deadline);
    cancel_operations(scope);
}

static void clear_timer(void *context, int fired) {
    (void)fired;
    Node *node = context;
    node->timer = NULL;
    joined(node->scope);
}

static void leaf_done(void *context) {
    Node *node = context;
    task_ready(node);
    joined(node->scope);
}

static void operation_done(uint8_t *context) {
    Node *node = (Node *)context;
    node->active = 0;
    leaf_done(node);
}

static void cancel_scope(Scope *scope) {
    report(scope, CANCEL, 0);
    cancel_operations(scope);
    kable_timer_cancel(scope->deadline);
}

void kable_scope_cancel(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope || !report(scope, PENDING, 0)) return;
    cancel_scope(scope);
    if (!report(scope, PENDING, 0) && scope->done) scope->done(scope->context);
}

int kable_scope_start(uint8_t *raw, size_t root, uv_loop_t *loop, uint64_t ms,
    uint8_t timed, void (*done)(void *), void *context) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven) return 0;
    scope->driven = 1;
    if (scope->status < 0) return 0;
    if (!report(scope, BEGIN, root)) {
        scope->status = UV_EINVAL;
        return 0;
    }
    if (timed && ms == 0) {
        report(scope, CANCEL, 0);
        return 0;
    }

    scope->done = done;
    scope->context = context;

    if (timed) {
        uv_update_time(loop);
        scope->status = kable_timer_start(loop, ms, deadline_ready, deadline_closed,
            scope, &scope->deadline);
        if (scope->status == 0) report(scope, STARTED, 0);
    }
    for (size_t i = 0; i < scope->count && scope->status == 0; i++) {
        Node *node = &scope->nodes[i];
        if (node->kind == SLEEP) {
            scope->status = kable_timer_start(loop, node->ms, task_ready,
                clear_timer, node, &node->timer);
            if (scope->status == 0) report(scope, STARTED, 0);
        } else if (node->kind == READ_FILE) {
            scope->status = kable_file_start(node->file, loop, leaf_done, node);
            if (scope->status == 0) report(scope, STARTED, 0);
        } else if (node->kind == OPERATION) {
            scope->status = node->start(node->operation, (uint8_t *)loop, operation_done, (uint8_t *)node);
            if (scope->status == 0) {
                node->active = 1;
                report(scope, STARTED, 0);
            }
        }
    }
    if (scope->status < 0) cancel_scope(scope);
    return report(scope, PENDING, 0) != 0;
}

void kable_scope_run(uint8_t *raw, size_t root, uint64_t ms, uint8_t timed) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven || scope->status < 0) return;
    uv_loop_t loop;
    scope->status = uv_loop_init(&loop);
    if (scope->status < 0) return;
    kable_scope_start(raw, root, &loop, ms, timed, NULL, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    if (uv_loop_close(&loop) != 0) abort();
}

int32_t kable_scope_result(uint8_t *raw, size_t id) {
    Scope *scope = (Scope *)raw;
    if (!scope) return UV_ENOMEM;
    if (scope->status < 0) return scope->status;
    if (!scope->driven || report(scope, PENDING, 0) || id >= scope->count) return UV_EINVAL;
    size_t result = report(scope, RESULT, id);
    return result == SIZE_MAX ? UV_EINVAL : (int32_t)result;
}

int32_t kable_scope_file_result(uint8_t *raw, size_t id, uint8_t **data, size_t *len) {
    if (kable_scope_result(raw, id) != SCOPE_COMPLETED) return UV_EINVAL;
    Scope *scope = (Scope *)raw;
    Node *node = &scope->nodes[id];
    if (node->kind != READ_FILE) return UV_EINVAL;
    return kable_file_result(node->file, data, len);
}

void kable_scope_free(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return;
    if (report(scope, PENDING, 0)) abort();
    for (size_t i = 0; i < scope->count; i++) kable_file_free(scope->nodes[i].file);
    report(scope, DESTROY, 0);
    free(scope->nodes);
    free(scope);
}
