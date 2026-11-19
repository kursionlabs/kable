#include <stdint.h>
#include <uv.h>

uint16_t kable_static_file_status(int32_t error) {
    switch (error) {
    case UV_ENOENT:
    case UV_ENOTDIR:
    case UV_ELOOP:
    case UV_EXDEV:
    case UV_EINVAL:
        return 404;
    case UV_EACCES:
    case UV_EPERM:
    case UV_EFBIG:
        return 403;
    default:
        return 500;
    }
}
