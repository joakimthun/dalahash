/* server.cpp — Spawns worker threads, handles shutdown signals. */

#include "server.h"
#include "io_uring_backend.h"
#include "worker.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <unistd.h>

static std::atomic<bool> g_running{true};

/* Signal handler for SIGINT/SIGTERM. Must only use async-signal-safe functions
 * (see signal-safety(7)). write(2) is async-signal-safe; fprintf is NOT.
 * The atomic store to g_running is safe because std::atomic<bool> with
 * relaxed ordering is a single store instruction on x86/ARM. */
static void signal_handler(int sig) {
    (void)sig;
    g_running.store(false, std::memory_order_relaxed);
    const char msg[] = "\ndalahash: shutting down...\n";
    (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
}

static void *worker_thread_fn(void *arg) {
    worker_run(static_cast<WorkerConfig *>(arg));
    return nullptr;
}

static constexpr uint32_t RING_SIZE = 4096;
static constexpr uint32_t BUF_COUNT = 1024;  /* provided buffers per worker */
static constexpr uint32_t BUF_SIZE  = 4096;

int server_start(const ServerConfig *config) {
    int num_workers = config->num_workers;
    if (num_workers <= 0) {
        /* sysconf(3) _SC_NPROCESSORS_ONLN — number of CPUs currently online.
         * We default to one worker per core for the thread-per-core model. */
        num_workers = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
        if (num_workers <= 0) num_workers = 1;
    }

    std::fprintf(stderr, "dalahash: starting %d workers on port %d\n",
                 num_workers, config->port);

    /* sigaction(2) — install signal handlers for clean shutdown.
     * sa_flags = 0: deliberately no SA_RESTART. Without SA_RESTART, blocked
     * syscalls return -EINTR when a signal arrives. This is critical for
     * io_uring: when io_uring_submit_and_wait_timeout is blocked waiting for
     * CQEs and SIGINT arrives, the -EINTR return lets the worker thread check
     * the g_running flag and exit cleanly. With SA_RESTART, the wait would
     * silently restart and the worker would never see the shutdown signal. */
    struct sigaction sa = {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    g_running.store(true, std::memory_order_relaxed);

    auto *configs = static_cast<WorkerConfig *>(std::calloc(static_cast<size_t>(num_workers), sizeof(WorkerConfig)));
    auto *threads = static_cast<pthread_t *>(std::calloc(static_cast<size_t>(num_workers), sizeof(pthread_t)));
    if (!configs || !threads) {
        std::fprintf(stderr, "dalahash: alloc failed\n");
        std::free(configs); std::free(threads);
        return 1;
    }

    for (int i = 0; i < num_workers; i++) {
        IoBackend *backend = io_uring_backend_create(RING_SIZE, BUF_COUNT, BUF_SIZE);
        if (!backend) {
            std::fprintf(stderr, "dalahash: backend create failed for worker %d\n", i);
            g_running.store(false, std::memory_order_relaxed);
            break;
        }
        configs[i] = {.cpu_id = i, .port = config->port, .ops = io_uring_ops(),
                      .backend = backend, .running = &g_running};

        int ret = pthread_create(&threads[i], nullptr, worker_thread_fn, &configs[i]);
        if (ret != 0) {
            std::fprintf(stderr, "dalahash: thread create failed for worker %d: %s\n",
                         i, std::strerror(ret));
            g_running.store(false, std::memory_order_relaxed);
            break;
        }
    }

    for (int i = 0; i < num_workers; i++) {
        if (threads[i]) pthread_join(threads[i], nullptr);
    }

    std::free(configs);
    std::free(threads);
    std::fprintf(stderr, "dalahash: stopped\n");
    return 0;
}
