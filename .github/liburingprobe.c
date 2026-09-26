// Same probe via liburing (distro package), to rule out my struct layout.
#include <liburing.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main(void) {
    struct io_uring ring;
    int r = io_uring_queue_init(2, &ring, 0);
    if (r < 0) { printf("liburing: queue_init failed %d (%s)\n", r, strerror(-r)); return 1; }
    printf("liburing: features=0x%x\n", ring.features);
    int ret = 0;
    struct io_uring_buf_ring* br = io_uring_setup_buf_ring(&ring, 2, 1, 0, &ret);
    if (br == NULL) { printf("liburing: setup_buf_ring(2,bgid=1,flags=0) failed ret=%d (%s)\n", ret, strerror(-ret)); }
    else { printf("liburing: setup_buf_ring ok\n"); io_uring_free_buf_ring(&ring, br, 2, 1); }
    struct io_uring_buf_reg reg; memset(&reg, 0, sizeof(reg));
    void* mem = NULL;
    if (posix_memalign(&mem, 4096, 4096) == 0) {
        reg.ring_addr = (unsigned long) mem; reg.ring_entries = 2; reg.bgid = 2;
        ret = io_uring_register_buf_ring(&ring, &reg, 0);
        printf("liburing: register_buf_ring(user mem, entries=2, bgid=2) ret=%d (%s)\n", ret, ret < 0 ? strerror(-ret) : "ok");
        if (ret >= 0) io_uring_unregister_buf_ring(&ring, 2);
    }
    io_uring_queue_exit(&ring);
    return 0;
}
