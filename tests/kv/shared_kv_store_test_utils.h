#pragma once

#include "kv/shared_kv_store.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace kvtest {

inline std::string view_to_string(const KvValueView& v) {
    return std::string(reinterpret_cast<const char*>(v.data), v.len);
}

// Deterministic xorshift RNG for reproducible concurrency tests.
struct XorShift64 {
    uint64_t state;

    explicit XorShift64(uint64_t seed) : state(seed ? seed : 0x9e3779b97f4a7c15ull) {}

    uint64_t next() {
        uint64_t x = state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state = x;
        return x;
    }
};

inline std::string make_value(uint32_t tid, uint64_t seq, uint64_t salt) {
    char buf[96];
    int n = std::snprintf(buf, sizeof(buf), "t%u:%llu:%llu", tid, static_cast<unsigned long long>(seq),
                          static_cast<unsigned long long>(salt));
    if (n <= 0)
        return {};
    if (static_cast<size_t>(n) > sizeof(buf))
        n = sizeof(buf);
    return std::string(buf, static_cast<size_t>(n));
}

inline bool parse_value(std::string_view s, uint32_t* tid, uint64_t* seq, uint64_t* salt) {
    if (!tid || !seq || !salt)
        return false;
    if (s.empty() || s.front() != 't')
        return false;

    size_t p1 = s.find(':');
    if (p1 == std::string_view::npos || p1 <= 1)
        return false;
    size_t p2 = s.find(':', p1 + 1);
    if (p2 == std::string_view::npos || p2 <= p1 + 1 || p2 + 1 >= s.size())
        return false;

    uint32_t out_tid = 0;
    uint64_t out_seq = 0;
    uint64_t out_salt = 0;

    std::string_view tid_part = s.substr(1, p1 - 1);
    std::string_view seq_part = s.substr(p1 + 1, p2 - p1 - 1);
    std::string_view salt_part = s.substr(p2 + 1);

    auto [tptr, tec] = std::from_chars(tid_part.data(), tid_part.data() + tid_part.size(), out_tid);
    if (tec != std::errc{} || tptr != tid_part.data() + tid_part.size())
        return false;
    auto [sptr, sec] = std::from_chars(seq_part.data(), seq_part.data() + seq_part.size(), out_seq);
    if (sec != std::errc{} || sptr != seq_part.data() + seq_part.size())
        return false;
    auto [xptr, xec] = std::from_chars(salt_part.data(), salt_part.data() + salt_part.size(), out_salt);
    if (xec != std::errc{} || xptr != salt_part.data() + salt_part.size())
        return false;

    *tid = out_tid;
    *seq = out_seq;
    *salt = out_salt;
    return true;
}

inline void wait_for_start(const std::atomic<bool>& start) {
    while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

inline uint64_t monotonic_now_ms() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

inline bool converge_to_capacity(KvStore* store, uint32_t worker_id, uint64_t now_ms, uint32_t rounds) {
    if (!store)
        return false;
    uint64_t cap = kv_store_capacity_bytes(store);
    for (uint32_t i = 0; i < rounds; i++) {
        uint64_t live = kv_store_live_bytes(store);
        if (live <= cap)
            return true;
        std::string key = "__trim__" + std::to_string(i & 15u);
        std::string value = make_value(worker_id, i, 0xdeadbeefull);
        (void)kv_store_set(store, worker_id, key, value, now_ms + i, nullptr);
        kv_store_quiescent(store, worker_id);
    }
    return kv_store_live_bytes(store) <= cap;
}

inline std::vector<std::string> make_key_space(uint32_t count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        keys.push_back("k:" + std::to_string(i));
    }
    return keys;
}

inline uint32_t clamp_threads(uint32_t requested) {
    uint32_t hw = static_cast<uint32_t>(std::thread::hardware_concurrency());
    if (hw == 0)
        hw = 4;
    if (requested < 1)
        requested = 1;
    if (requested > hw * 2u)
        requested = hw * 2u;
    return requested;
}

} // namespace kvtest
