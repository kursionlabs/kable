#include "scope.h"
#include "timer.h"

#include <stdlib.h>

enum { TIMER, PAIR, OPERATION };
/* Matches the event bridge in async/lib.kur. */
enum { BEGIN, STARTED, RETIRED, PENDING, COMPLETE, CANCEL, TAKE_CANCEL, RESULT, DESTROY, JOINED };
/* STARTED and RETIRED use a leaf ID or this separate parent timer ID. */
static const size_t PARENT_TIMER_ID = SIZE_MAX;
/* Matches the status decoding in async/lib.kur. Negative codes are backend startup errors. */
enum { SCOPE_NOT_STARTED = 4, SCOPE_INVALID_PLAN, SCOPE_INVALID_RESULT };

typedef size_t (*policy_add_leaf)(uint8_t *);
typedef size_t (*policy_add_pair)(uint8_t *, uint8_t, size_t, size_t);
typedef size_t (*policy_event)(uint8_t *, uint8_t, size_t);

typedef struct Scope Scope;

typedef struct {
    Scope *scope;
    KableTimer *timer;
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
    policy_add_leaf add_leaf;
    policy_add_pair add_pair;
    policy_event event;
    uint64_t host_key;
    uint8_t *host;
    int start_error;
    int invalid_plan;
    int driven;
    uint8_t deadline_expired;
};

typedef struct {
    uv_loop_t loop;
    int start_error;
    int attempted;
} Runner;

uint8_t *kable_scope_new(uint8_t *policy, policy_add_leaf add_leaf,
    policy_add_pair add_pair, policy_event event) {
    Scope *scope = calloc(1, sizeof *scope);
    if (scope) {
        scope->policy = policy;
        scope->add_leaf = add_leaf;
        scope->add_pair = add_pair;
        scope->event = event;
    }
    return (uint8_t *)scope;
}

static size_t report(Scope *scope, uint8_t event, size_t id) {
    return scope->event(scope->policy, event, id);
}

/* Plans register children before parents. The array stays fixed once driven. */
static int reserve_node(Scope *scope) {
    if (!scope || scope->driven || scope->invalid_plan || scope->start_error < 0) return 0;

    if (scope->count == scope->capacity) {
        if (scope->capacity > SIZE_MAX / sizeof(Node) / 2) {
            scope->start_error = UV_ENOMEM;
            return 0;
        }
        size_t capacity = scope->capacity ? scope->capacity * 2 : 8;
        Node *nodes = realloc(scope->nodes, capacity * sizeof *nodes);
        if (!nodes) {
            scope->start_error = UV_ENOMEM;
            return 0;
        }
        scope->nodes = nodes;
        scope->capacity = capacity;
    }
    return 1;
}

static size_t append_node(Scope *scope, size_t index, Node node) {
    if (index != scope->count) {
        scope->invalid_plan = 1;
        return SIZE_MAX;
    }
    node.scope = scope;
    node.id = index;
    scope->nodes[scope->count++] = node;
    return index;
}

static size_t append_leaf(Scope *scope, Node node) {
    return append_node(scope, scope->add_leaf(scope->policy), node);
}

size_t kable_scope_add_timer(uint8_t *raw, uint64_t ms) {
    Scope *scope = (Scope *)raw;
    if (!reserve_node(scope)) return SIZE_MAX;
    return append_leaf(scope, (Node){ .kind = TIMER, .ms = ms });
}

size_t kable_scope_add_pair(uint8_t *raw, uint8_t kind, size_t first, size_t second) {
    Scope *scope = (Scope *)raw;
    if (!reserve_node(scope)) return SIZE_MAX;
    size_t index = scope->add_pair(scope->policy, kind, first, second);
    return append_node(scope, index, (Node){ .kind = PAIR });
}

void kable_scope_fail_allocation(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (scope && !scope->driven && !scope->invalid_plan && scope->start_error == 0) {
        scope->start_error = UV_ENOMEM;
    }
}

uint8_t *kable_scope_require_host(uint8_t *raw, uint64_t key) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven) return NULL;
    if (scope->host_key || !key) {
        scope->invalid_plan = 1;
        return NULL;
    }
    scope->host_key = key;
    return raw;
}

uint64_t kable_scope_host_key(uint8_t *raw) {
    return raw ? ((Scope *)raw)->host_key : 0;
}

void kable_scope_set_host(uint8_t *raw, uint8_t *host) {
    ((Scope *)raw)->host = host;
}

uint8_t *kable_scope_host(uint8_t *raw) {
    return ((Scope *)raw)->host;
}

size_t kable_scope_add_operation(uint8_t *raw, uint8_t *context,
    KableOperationStart start, KableOperationStop stop) {
    Scope *scope = (Scope *)raw;
    if (!reserve_node(scope)) return SIZE_MAX;
    return append_leaf(scope, (Node){
        .kind = OPERATION, .operation = context, .start = start, .stop = stop,
    });
}

static void cancel_operations(Scope *scope) {
    size_t id;
    while ((id = report(scope, TAKE_CANCEL, 0)) != SIZE_MAX) {
        Node *node = &scope->nodes[id];
        if (node->kind == TIMER) kable_timer_cancel(node->timer);
        else if (node->kind == OPERATION && node->active) {
            node->active = 0;
            if (!node->stop(node->operation)) report(scope, RETIRED, id);
        }
    }
}

static void notify_joined(Scope *scope) {
    if (!scope->done || !report(scope, JOINED, 0)) return;
    void (*done)(void *) = scope->done;
    scope->done = NULL;
    done(scope->context);
}

static void deadline_ready(void *context) {
    Scope *scope = context;
    scope->deadline_expired = 1;
    report(scope, CANCEL, 0);
    cancel_operations(scope);
    notify_joined(scope);
}

static void retire_resource(Scope *scope, size_t id) {
    report(scope, RETIRED, id);
    notify_joined(scope);
}

static void deadline_closed(void *context, int fired) {
    (void)fired;
    Scope *scope = context;
    scope->deadline = NULL;
    retire_resource(scope, PARENT_TIMER_ID);
}

static void settle_leaf(Node *node) {
    Scope *scope = node->scope;
    if (report(scope, COMPLETE, node->id)) kable_timer_cancel(scope->deadline);
    cancel_operations(scope);
}

static void timer_ready(void *context) {
    Node *node = context;
    settle_leaf(node);
    notify_joined(node->scope);
}

static void clear_timer(void *context, int fired) {
    (void)fired;
    Node *node = context;
    node->timer = NULL;
    retire_resource(node->scope, node->id);
}

static void operation_done(uint8_t *context) {
    Node *node = (Node *)context;
    node->active = 0;
    settle_leaf(node);
    retire_resource(node->scope, node->id);
}

static void cancel_scope(Scope *scope) {
    report(scope, CANCEL, 0);
    cancel_operations(scope);
    kable_timer_cancel(scope->deadline);
}

void kable_scope_cancel(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope || !scope->driven || scope->invalid_plan || report(scope, JOINED, 0)) return;
    cancel_scope(scope);
    notify_joined(scope);
}

uint64_t kable_now_millis(void) {
    return uv_hrtime() / 1000000;
}

int kable_scope_start(uint8_t *raw, size_t root, uv_loop_t *loop, uint64_t end_ms,
    uint8_t timed, void (*done)(void *), void *context) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven) return 0;
    scope->driven = 1;
    if (scope->start_error < 0 || scope->invalid_plan) return 0;
    if (!report(scope, BEGIN, root)) {
        scope->invalid_plan = 1;
        return 0;
    }
    uv_update_time(loop);
    uint64_t now = timed ? kable_now_millis() : 0;
    if (timed && end_ms <= now) {
        scope->deadline_expired = 1;
        report(scope, CANCEL, 0);
        return 0;
    }

    scope->done = done;
    scope->context = context;

    if (timed) {
        scope->start_error = kable_timer_start(loop, end_ms - now, deadline_ready, deadline_closed,
            scope, &scope->deadline);
        if (scope->start_error == 0) report(scope, STARTED, PARENT_TIMER_ID);
    }
    for (size_t i = 0; i < scope->count && scope->start_error == 0; i++) {
        Node *node = &scope->nodes[i];
        if (node->kind == TIMER) {
            scope->start_error = kable_timer_start(loop, node->ms, timer_ready,
                clear_timer, node, &node->timer);
            if (scope->start_error == 0) report(scope, STARTED, node->id);
        } else if (node->kind == OPERATION) {
            scope->start_error = node->start(node->operation, (uint8_t *)loop, operation_done, (uint8_t *)node);
            if (scope->start_error == 0) {
                node->active = 1;
                report(scope, STARTED, node->id);
            }
        }
    }
    if (scope->start_error < 0) cancel_scope(scope);
    return !report(scope, JOINED, 0);
}

static void require_closed_handle(uv_handle_t *handle, void *context) {
    (void)handle;
    (void)context;
    abort();
}

static uint8_t runner_run(Runner *runner, Scope *scope, size_t root, uint64_t end_ms, uint8_t timed) {
    if (!scope || scope->driven || scope->invalid_plan || scope->start_error < 0) return 0;

    if (!runner) {
        scope->start_error = UV_ENOMEM;
        return 0;
    }
    if (!runner->attempted) {
        runner->attempted = 1;
        runner->start_error = uv_loop_init(&runner->loop);
    }
    if (runner->start_error < 0) {
        scope->start_error = runner->start_error;
        return 0;
    }

    uv_loop_t *loop = &runner->loop;
    kable_scope_start((uint8_t *)scope, root, loop, end_ms, timed, NULL, NULL);
    if (uv_run(loop, UV_RUN_DEFAULT) != 0 || report(scope, PENDING, 0)) abort();
    /* Inactive handles also have to close before the scope's storage is freed. */
    uv_walk(loop, require_closed_handle, NULL);
    return scope->deadline_expired;
}

static void runner_close(Runner *runner) {
    if (runner->attempted && runner->start_error == 0 && uv_loop_close(&runner->loop) != 0) abort();
}

uint8_t kable_scope_run(uint8_t *raw, size_t root, uint64_t end_ms, uint8_t timed) {
    Runner runner = {0};
    uint8_t expired = runner_run(&runner, (Scope *)raw, root, end_ms, timed);
    runner_close(&runner);
    return expired;
}

uint8_t *kable_runner_new(void) {
    return (uint8_t *)calloc(1, sizeof(Runner));
}

uint8_t kable_runner_run(uint8_t *raw_runner, uint8_t *raw_scope, size_t root, uint64_t end_ms, uint8_t timed) {
    return runner_run((Runner *)raw_runner, (Scope *)raw_scope, root, end_ms, timed);
}

void kable_runner_free(uint8_t *raw) {
    Runner *runner = (Runner *)raw;
    if (!runner) return;
    runner_close(runner);
    free(runner);
}

int32_t kable_scope_result(uint8_t *raw, size_t id) {
    Scope *scope = (Scope *)raw;
    if (!scope) return UV_ENOMEM;
    if (scope->start_error < 0) return scope->start_error;
    if (scope->invalid_plan) return SCOPE_INVALID_PLAN;
    if (!scope->driven) return SCOPE_NOT_STARTED;
    if (id >= scope->count) return SCOPE_INVALID_RESULT;
    size_t result = report(scope, RESULT, id);
    return result == SIZE_MAX ? SCOPE_INVALID_RESULT : (int32_t)result;
}

void kable_scope_free(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return;
    if (report(scope, PENDING, 0)) abort();
    report(scope, DESTROY, 0);
    free(scope->nodes);
    free(scope);
}
