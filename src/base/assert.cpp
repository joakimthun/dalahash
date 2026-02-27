// assert.cpp — Linux assert failure reporting.
//
// Implementation notes:
//   - Uses execinfo backtrace APIs for callstack dump.
//   - Guards against recursive assert failures with thread-local flag.

#include "base/assert.h"

#if defined(DALAHASH_DEBUG_ASSERTS)

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <execinfo.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

static thread_local bool g_assert_in_progress = false;

static long current_tid() { return static_cast<long>(::syscall(SYS_gettid)); }

static void print_assert_header(const char* expr, const char* file, int line, const char* func) {
    std::fprintf(stderr, "\n=== dalahash ASSERT failed ===\n");
    std::fprintf(stderr, "condition: %s\n", expr ? expr : "<null>");
    std::fprintf(stderr, "location : %s:%d (%s)\n", file ? file : "<unknown>", line,
                 func ? func : "<unknown>");
    std::fprintf(stderr, "pid/tid  : %d/%ld\n", static_cast<int>(::getpid()), current_tid());
}

static void print_stacktrace() {
    // backtrace_symbols_fd avoids heap-allocation management in failure path.
    void* frames[64] = {};
    int count = ::backtrace(frames, static_cast<int>(sizeof(frames) / sizeof(frames[0])));
    if (count <= 0) {
        std::fprintf(stderr, "callstack: <unavailable>\n");
        return;
    }
    std::fprintf(stderr, "callstack:\n");
    (void)::backtrace_symbols_fd(frames, count, STDERR_FILENO);
}

[[noreturn]] static void fail_and_abort() {
    std::fflush(stderr);
    std::abort();
}

} // namespace

[[noreturn]] void dalahash_assert_fail(const char* expr, const char* file, int line, const char* func,
                                       const char* msg) {
    if (g_assert_in_progress)
        _exit(134);
    g_assert_in_progress = true;
    print_assert_header(expr, file, line, func);
    if (msg && msg[0] != '\0')
        std::fprintf(stderr, "message  : %s\n", msg);
    print_stacktrace();
    fail_and_abort();
}

[[noreturn]] void dalahash_assert_fail_fmt(const char* expr, const char* file, int line, const char* func,
                                           const char* fmt, ...) {
    if (g_assert_in_progress)
        _exit(134);
    g_assert_in_progress = true;
    print_assert_header(expr, file, line, func);
    if (fmt) {
        char buffer[1024];
        va_list ap;
        va_start(ap, fmt);
        int n = std::vsnprintf(buffer, sizeof(buffer), fmt, ap);
        va_end(ap);
        if (n > 0)
            std::fprintf(stderr, "message  : %s\n", buffer);
        else
            std::fprintf(stderr, "message  : <formatting failed>\n");
    }
    print_stacktrace();
    fail_and_abort();
}

#else

[[noreturn]] void dalahash_assert_fail(const char*, const char*, int, const char*, const char*) {
    std::abort();
}

[[noreturn]] void dalahash_assert_fail_fmt(const char*, const char*, int, const char*, const char*, ...) {
    std::abort();
}

#endif
