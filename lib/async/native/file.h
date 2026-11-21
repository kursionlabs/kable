#ifndef KABLE_ASYNC_FILE_H
#define KABLE_ASYNC_FILE_H

#include <stdint.h>
#include <stddef.h>

enum KableFileMode { KABLE_READ_FILE, KABLE_READ_BENEATH, KABLE_OPEN_BENEATH, KABLE_OPEN_FILE, KABLE_CREATE_FILE };
/* The path remains borrowed until the request is freed. Its length includes the final NUL. */
uint8_t *kable_file_new(const uint8_t *path, size_t len, size_t limit, uint8_t mode);
/* The read destination remains borrowed until the worker retires. */
uint8_t *kable_file_chunk(int32_t fd, uint64_t offset, uint8_t *bytes, size_t limit);
uint8_t *kable_file_write_chunk(int32_t fd, uint64_t offset, const uint8_t *bytes, size_t len);
int32_t kable_file_start(uint8_t *raw, uint8_t *loop, void (*done)(uint8_t *), uint8_t *context);
uint8_t kable_file_stop(uint8_t *raw);
/* Pass a null data argument when only the byte count is needed. */
int32_t kable_file_result(uint8_t *raw, uint8_t **data, size_t *len);
int32_t kable_file_write_result(uint8_t *raw, size_t *written);
int32_t kable_file_take_descriptor(uint8_t *raw, uint64_t *len);
void kable_file_free(uint8_t *raw);

#endif
