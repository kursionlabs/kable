#include "timer.h"

#include <limits.h>
#include <stdlib.h>

enum { SLEEP, RACE, JOIN };
enum { PENDING = INT_MAX, CANCELLED = 0, READY = 1 };

typedef struct Scope Scope;

typedef struct {
    Scope *scope;
    KableTimer *timer;
    size_t parent;
    size_t first;
    size_t second;
    uint64_t ms;
    int kind;
    int result;
} Node;

struct Scope {
    uv_loop_t loop;
    KableTimer *deadline;
    Node *nodes;
    size_t count;
    size_t capacity;
    size_t root;
    size_t cursor;
    int status;
    int driven;
};

uint8_t *kable_scope_new(void) {
    Scope *scope = calloc(1, sizeof *scope);
    return (uint8_t *)scope;
}

/* Plans register children before parents. The array stays fixed once driven. */
size_t kable_scope_add(uint8_t *raw, uint8_t kind, uint64_t ms, size_t first, size_t second) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven || scope->status < 0) return SIZE_MAX;

    if (kind > JOIN || (kind != SLEEP && (first >= scope->count ||
        second >= scope->count || first == second || scope->nodes[first].parent != SIZE_MAX ||
        scope->nodes[second].parent != SIZE_MAX))) {
        scope->status = UV_EINVAL;
        return SIZE_MAX;
    }

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

    size_t index = scope->count++;
    scope->nodes[index] = (Node){
        .scope = scope, .kind = kind, .ms = ms, .first = first, .second = second,
        .parent = SIZE_MAX, .result = PENDING,
    };
    if (kind != SLEEP) {
        scope->nodes[first].parent = index;
        scope->nodes[second].parent = index;
    }
    return index;
}

/* Cancellation preserves any results already earned by the subtree. */
static void cancel(Node *node) {
    if (node->result != PENDING) return;

    node->result = CANCELLED;
    if (node->kind == SLEEP) {
        kable_timer_cancel(node->timer);
    } else {
        cancel(&node->scope->nodes[node->first]);
        cancel(&node->scope->nodes[node->second]);
    }
}

static void deadline_ready(void *context) {
    Scope *scope = context;
    cancel(&scope->nodes[scope->root]);
}

static void deadline_closed(void *context, int fired) {
    (void)fired;
    Scope *scope = context;
    scope->deadline = NULL;
}

static void task_ready(void *context) {
    Node *node = context;
    Scope *scope = node->scope;
    int result = READY;
    while (node->result == PENDING) {
        node->result = result;
        if (node->parent == SIZE_MAX) {
            kable_timer_cancel(scope->deadline);
            return;
        }

        Node *parent = &scope->nodes[node->parent];
        int first = node == &scope->nodes[parent->first];
        Node *sibling = &scope->nodes[first ? parent->second : parent->first];
        if (parent->kind == RACE) {
            cancel(sibling);
            result = first ? 1 : 2;
        } else {
            if (sibling->result == PENDING) return;
            result = READY;
        }
        node = parent;
    }
}

static void clear_timer(void *context, int fired) {
    (void)fired;
    Node *node = context;
    node->timer = NULL;
}

void kable_scope_run(uint8_t *raw, size_t root, uint64_t ms, uint8_t timed) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven) return;
    scope->driven = 1;
    if (scope->status < 0) return;
    if (root >= scope->count || scope->nodes[root].parent != SIZE_MAX) {
        scope->status = UV_EINVAL;
        return;
    }
    /* A well-formed scope has exactly one root. */
    for (size_t i = 0; i < scope->count; i++) {
        if (i != root && scope->nodes[i].parent == SIZE_MAX) {
            scope->status = UV_EINVAL;
            return;
        }
    }

    scope->root = root;
    scope->status = uv_loop_init(&scope->loop);
    if (scope->status < 0) return;

    if (timed) {
        scope->status = kable_timer_start(&scope->loop, ms, deadline_ready, deadline_closed,
            scope, &scope->deadline);
    }
    for (size_t i = 0; i < scope->count && scope->status == 0; i++) {
        Node *node = &scope->nodes[i];
        if (node->kind == SLEEP) {
            scope->status = kable_timer_start(&scope->loop, node->ms, task_ready,
                clear_timer, node, &node->timer);
        }
    }
    if (scope->status < 0) {
        kable_timer_cancel(scope->deadline);
        cancel(&scope->nodes[root]);
    }

    uv_run(&scope->loop, UV_RUN_DEFAULT);
    if (uv_loop_close(&scope->loop) != 0) abort();
}

int32_t kable_scope_next(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return UV_ENOMEM;
    if (scope->status < 0) return scope->status;
    if (!scope->driven || scope->cursor >= scope->count) return UV_EINVAL;
    return scope->nodes[scope->cursor++].result;
}

void kable_scope_free(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return;
    free(scope->nodes);
    free(scope);
}
