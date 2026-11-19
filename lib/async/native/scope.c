#include "file.h"
#include "scope.h"
#include "timer.h"

#include <limits.h>
#include <stdlib.h>

enum { SLEEP, RACE, JOIN, READ_FILE };
enum { PENDING = INT_MAX, CANCELLED = 0, READY = 1 };

typedef struct Scope Scope;

typedef struct {
    Scope *scope;
    KableTimer *timer;
    KableFile *file;
    size_t parent;
    size_t first;
    size_t second;
    uint64_t ms;
    int kind;
    int result;
} Node;

struct Scope {
    KableTimer *deadline;
    void (*done)(void *);
    void *context;
    Node *nodes;
    size_t count;
    size_t capacity;
    size_t root;
    size_t cursor;
    size_t pending;
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

size_t kable_scope_add_file(uint8_t *raw, uint8_t *path, size_t len, size_t limit) {
    size_t index = kable_scope_add(raw, SLEEP, 0, 0, 0);
    if (index == SIZE_MAX) return index;
    Scope *scope = (Scope *)raw;
    KableFile *file = kable_file_new(path, len, limit);
    if (!file) {
        scope->status = UV_ENOMEM;
        return SIZE_MAX;
    }
    scope->nodes[index].kind = READ_FILE;
    scope->nodes[index].file = file;
    return index;
}

/* Cancellation preserves any results already earned by the subtree. */
static void cancel(Node *node) {
    if (node->result != PENDING) return;

    node->result = CANCELLED;
    if (node->kind == SLEEP) {
        kable_timer_cancel(node->timer);
    } else if (node->kind == READ_FILE) {
        kable_file_cancel(node->file);
    } else {
        cancel(&node->scope->nodes[node->first]);
        cancel(&node->scope->nodes[node->second]);
    }
}

static void deadline_ready(void *context) {
    Scope *scope = context;
    cancel(&scope->nodes[scope->root]);
}

static void joined(Scope *scope) {
    scope->pending--;
    if (!scope->pending && scope->done) scope->done(scope->context);
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
    joined(node->scope);
}

static void file_done(void *context) {
    Node *node = context;
    task_ready(node);
    joined(node->scope);
}

void kable_scope_cancel(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope || !scope->pending) return;
    cancel(&scope->nodes[scope->root]);
    kable_timer_cancel(scope->deadline);
}

int kable_scope_start(uint8_t *raw, size_t root, uv_loop_t *loop, uint64_t ms,
    uint8_t timed, void (*done)(void *), void *context) {
    Scope *scope = (Scope *)raw;
    if (!scope || scope->driven) return 0;
    scope->driven = 1;
    if (scope->status < 0) return 0;
    if (root >= scope->count || scope->nodes[root].parent != SIZE_MAX) {
        scope->status = UV_EINVAL;
        return 0;
    }
    /* A well-formed scope has exactly one root. */
    for (size_t i = 0; i < scope->count; i++) {
        if (i != root && scope->nodes[i].parent == SIZE_MAX) {
            scope->status = UV_EINVAL;
            return 0;
        }
    }

    scope->root = root;
    scope->done = done;
    scope->context = context;

    if (timed) {
        uv_update_time(loop);
        scope->status = kable_timer_start(loop, ms, deadline_ready, deadline_closed,
            scope, &scope->deadline);
        if (scope->status == 0) scope->pending++;
    }
    for (size_t i = 0; i < scope->count && scope->status == 0; i++) {
        Node *node = &scope->nodes[i];
        if (node->kind == SLEEP) {
            scope->status = kable_timer_start(loop, node->ms, task_ready,
                clear_timer, node, &node->timer);
            if (scope->status == 0) scope->pending++;
        } else if (node->kind == READ_FILE) {
            scope->status = kable_file_start(node->file, loop, file_done, node);
            if (scope->status == 0) scope->pending++;
        }
    }
    if (scope->status < 0) kable_scope_cancel(raw);
    return scope->pending != 0;
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

int32_t kable_scope_next(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return UV_ENOMEM;
    if (scope->status < 0) return scope->status;
    if (!scope->driven || scope->pending || scope->cursor >= scope->count) return UV_EINVAL;
    return scope->nodes[scope->cursor++].result;
}

int32_t kable_scope_file_result(uint8_t *raw, uint8_t **data, size_t *len) {
    Scope *scope = (Scope *)raw;
    if (!scope || !scope->driven || scope->pending || scope->cursor == 0) return UV_EINVAL;
    Node *node = &scope->nodes[scope->cursor - 1];
    if (node->kind != READ_FILE || node->result != READY) return UV_EINVAL;
    return kable_file_result(node->file, data, len);
}

void kable_scope_free(uint8_t *raw) {
    Scope *scope = (Scope *)raw;
    if (!scope) return;
    if (scope->pending) abort();
    for (size_t i = 0; i < scope->count; i++) kable_file_free(scope->nodes[i].file);
    free(scope->nodes);
    free(scope);
}
