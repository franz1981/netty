// Minimal probe: io_uring_setup + IORING_REGISTER_PBUF_RING via raw syscalls; prints errno.
#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/utsname.h>

struct io_sqring_offsets { uint32_t head, tail, ring_mask, ring_entries, flags, dropped, array, resv1; uint64_t user_addr; };
struct io_cqring_offsets { uint32_t head, tail, ring_mask, ring_entries, overflow, cqes, flags, resv1; uint64_t user_addr; };
struct io_uring_params {
    uint32_t sq_entries, cq_entries, flags, sq_thread_cpu, sq_thread_idle, features, wq_fd, resv[3];
    struct io_sqring_offsets sq_off; struct io_cqring_offsets cq_off;
};
struct io_uring_buf_reg { uint64_t ring_addr; uint32_t ring_entries; uint16_t bgid; uint16_t flags; uint64_t resv[3]; };
#define IORING_REGISTER_PBUF_RING 22
#define IORING_UNREGISTER_PBUF_RING 23
#define IOU_PBUF_RING_MMAP 1

static int try_register(int fd, int entries, uint16_t flags, const char* label) {
    struct io_uring_buf_reg reg; memset(&reg, 0, sizeof(reg));
    size_t ring_size = entries * 16;
    void* br = NULL;
    if (!(flags & IOU_PBUF_RING_MMAP)) {
        br = mmap(NULL, ring_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        if (br == MAP_FAILED) { printf("%s: mmap failed errno=%d (%s)\n", label, errno, strerror(errno)); return -1; }
        reg.ring_addr = (uint64_t)(uintptr_t)br;
    }
    reg.ring_entries = entries; reg.bgid = 1; reg.flags = flags;
    int r = syscall(__NR_io_uring_register, fd, IORING_REGISTER_PBUF_RING, &reg, 1);
    int e = errno;
    if (r < 0) {
        printf("%s: REGISTER_PBUF_RING failed r=%d errno=%d (%s)\n", label, r, e, strerror(e));
    } else {
        printf("%s: REGISTER_PBUF_RING ok\n", label);
        struct io_uring_buf_reg un; memset(&un, 0, sizeof(un)); un.bgid = 1;
        syscall(__NR_io_uring_register, fd, IORING_UNREGISTER_PBUF_RING, &un, 1);
    }
    if (br) munmap(br, ring_size);
    return r;
}

int main(void) {
    struct utsname u; uname(&u); printf("kernel: %s\n", u.release);
    struct rlimit rl; getrlimit(RLIMIT_MEMLOCK, &rl);
    printf("RLIMIT_MEMLOCK: cur=%lld max=%lld\n", (long long)rl.rlim_cur, (long long)rl.rlim_max);
    FILE* f = fopen("/proc/sys/kernel/io_uring_disabled", "r");
    if (f) { int v = -1; if (fscanf(f, "%d", &v) == 1) printf("io_uring_disabled: %d\n", v); fclose(f); }
    else printf("io_uring_disabled: (no sysctl)\n");
    struct io_uring_params p; memset(&p, 0, sizeof(p));
    int fd = syscall(__NR_io_uring_setup, 1, &p);
    if (fd < 0) { printf("io_uring_setup failed errno=%d (%s)\n", errno, strerror(errno)); return 1; }
    printf("io_uring_setup ok fd=%d features=0x%x\n", fd, p.features);
    try_register(fd, 2, 0, "user-mem entries=2");
    try_register(fd, 2, IOU_PBUF_RING_MMAP, "kernel-mmap entries=2");
    try_register(fd, 4096, 0, "user-mem entries=4096");
    close(fd);
    return 0;
}
