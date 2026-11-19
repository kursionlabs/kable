#ifndef KABLE_ASYNC_FILE_H
#define KABLE_ASYNC_FILE_H

#include <stdint.h>
#include <uv.h>

typedef struct KableFile KableFile;

enum KableFileMode { KABLE_READ_FILE, KABLE_READ_BENEATH, KABLE_OPEN_BENEATH };
KableFile *kable_file_new(const uint8_t *path, size_t len, size_t limit, uint8_t mode);
KableFile *kable_file_chunk(int fd, uint64_t offset, size_t limit);
int kable_file_start(KableFile *file, uv_loop_t *loop, void (*done)(void *), void *context);
void kable_file_cancel(KableFile *file);
int kable_file_result(KableFile *file, uint8_t **data, size_t *len);
int kable_file_take_descriptor(KableFile *file, uint64_t *len);
void kable_file_free(KableFile *file);

#endif
