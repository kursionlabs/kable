#include <jansson.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Return a validated, JSON-quoted title that can be embedded in a list response.
char *todo_title(const uint8_t *bytes, size_t length, size_t *size) {
    json_t *object = json_loadb((const char *)bytes, length, JSON_REJECT_DUPLICATES, NULL);
    if (!object) return NULL;
    json_t *title = json_object_get(object, "title");
    char *quoted = NULL;
    if (json_is_string(title) && json_string_length(title) > 0)
        quoted = json_dumps(title, JSON_COMPACT | JSON_ENCODE_ANY);
    json_decref(object);
    if (quoted) *size = strlen(quoted);
    return quoted;
}

void todo_title_free(char *title) {
    json_free_t release;
    json_get_alloc_funcs(NULL, &release);
    release(title);
}
