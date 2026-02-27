// assert.h — Debug-only invariant checks.
//
// Usage:
//   ASSERT(cond, "message");
//   ASSERT_FMT(cond, "msg: %d", x);
//
// Behavior:
//   - Debug builds: logs condition/location/message + callstack, then aborts.
//   - Non-debug builds: compiled out.

#pragma once

#include <cstdlib>

#if defined(__GNUC__) || defined(__clang__)
#define DALAHASH_PRINTF_LIKE(fmt_idx, first_arg_idx) __attribute__((format(printf, fmt_idx, first_arg_idx)))
#else
#define DALAHASH_PRINTF_LIKE(fmt_idx, first_arg_idx)
#endif

[[noreturn]] void dalahash_assert_fail(const char* expr, const char* file, int line, const char* func,
                                       const char* msg);

[[noreturn]] void dalahash_assert_fail_fmt(const char* expr, const char* file, int line, const char* func,
                                           const char* fmt, ...) DALAHASH_PRINTF_LIKE(5, 6);

#if defined(DALAHASH_DEBUG_ASSERTS)
// Check invariant and crash on failure with static message.
#define ASSERT(condition, message)                                                                           \
    do {                                                                                                     \
        if (!(condition))                                                                                    \
            dalahash_assert_fail(#condition, __FILE__, __LINE__, __func__, (message));                       \
    } while (0)

// Check invariant and crash on failure with printf-style message.
#define ASSERT_FMT(condition, fmt, ...)                                                                      \
    do {                                                                                                     \
        if (!(condition))                                                                                    \
            dalahash_assert_fail_fmt(#condition, __FILE__, __LINE__, __func__,                               \
                                     (fmt)__VA_OPT__(, ) __VA_ARGS__);                                       \
    } while (0)
#else
// Compiled out in non-debug builds.
#define ASSERT(condition, message)                                                                           \
    do {                                                                                                     \
        (void)sizeof(condition);                                                                             \
    } while (0)
#define ASSERT_FMT(condition, fmt, ...)                                                                      \
    do {                                                                                                     \
        (void)sizeof(condition);                                                                             \
    } while (0)
#endif

#if defined(DALAHASH_DEBUG_ASSERTS)
// Mark impossible control-flow in debug and crash with context if reached.
#define DALAHASH_UNREACHABLE(message)                                                                        \
    do {                                                                                                     \
        dalahash_assert_fail("unreachable", __FILE__, __LINE__, __func__, (message));                        \
    } while (0)
#elif defined(__GNUC__) || defined(__clang__)
#define DALAHASH_UNREACHABLE(message)                                                                        \
    do {                                                                                                     \
        __builtin_unreachable();                                                                             \
    } while (0)
#else
#define DALAHASH_UNREACHABLE(message)                                                                        \
    do {                                                                                                     \
        std::abort();                                                                                        \
    } while (0)
#endif
