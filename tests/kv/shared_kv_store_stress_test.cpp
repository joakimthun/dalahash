#include "kv/shared_kv_store.h"
#include "shared_kv_store_test_utils.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

namespace {

struct StressCase {
    const char* name;
    uint32_t threads;
    uint32_t workers;
    uint32_t duration_ms;
    uint64_t capacity_bytes;
    uint32_t key_count;
    uint64_t seed;
};

class SharedKvStress : public ::testing::TestWithParam<StressCase> {};

TEST_P(SharedKvStress, SeededConcurrentMatrix) {
    const StressCase c = GetParam();
    uint32_t threads = kvtest::clamp_threads(c.threads);
    uint32_t workers = c.workers == 0 ? 1 : c.workers;
    if (workers > threads)
        workers = threads;

    KvStoreConfig cfg = {
        .capacity_bytes = c.capacity_bytes,
        .shard_count = 0,
        .buckets_per_shard = 0,
        .worker_count = workers,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < workers; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    std::vector<std::string> keys = kvtest::make_key_space(c.key_count);
    std::atomic<bool> start{false};
    std::atomic<uint64_t> invalid_status{0};
    std::atomic<uint64_t> bad_value{0};
    std::atomic<uint64_t> logical_now{5000};
    std::vector<std::thread> pool;
    pool.reserve(threads);

    auto t0 = std::chrono::steady_clock::now();
    auto deadline = t0 + std::chrono::milliseconds(c.duration_ms);

    for (uint32_t tid = 0; tid < threads; tid++) {
        pool.emplace_back([&, tid]() {
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(c.seed + tid * 0x9e3779b97f4a7c15ull + 17ull);
            uint64_t seq = 1;
            uint32_t worker = tid % workers;

            while (std::chrono::steady_clock::now() < deadline) {
                uint64_t r = rng.next();
                uint32_t key_idx = static_cast<uint32_t>(r % keys.size());
                uint64_t now = logical_now.fetch_add(1, std::memory_order_acq_rel);
                uint32_t op = static_cast<uint32_t>(r % 100u);

                if (op < 42) {
                    std::string value = kvtest::make_value(tid, seq++, rng.next());
                    KvSetStatus st = kv_store_set(s, worker, keys[key_idx], value, now, nullptr);
                    if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                        invalid_status.fetch_add(1, std::memory_order_relaxed);
                } else if (op < 64) {
                    KvSetOptions ttl = {
                        .mode = KvExpireMode::AFTER_MS,
                        .value_ms = 1u + static_cast<uint64_t>(rng.next() % 250u),
                    };
                    std::string value = kvtest::make_value(tid, seq++, rng.next());
                    KvSetStatus st = kv_store_set(s, worker, keys[key_idx], value, now, &ttl);
                    if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                        invalid_status.fetch_add(1, std::memory_order_relaxed);
                } else if (op < 76) {
                    KvSetOptions ttl = {
                        .mode = KvExpireMode::AT_MS,
                        .value_ms = now + 1u + static_cast<uint64_t>(rng.next() % 250u),
                    };
                    std::string value = kvtest::make_value(tid, seq++, rng.next());
                    KvSetStatus st = kv_store_set(s, worker, keys[key_idx], value, now, &ttl);
                    if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                        invalid_status.fetch_add(1, std::memory_order_relaxed);
                } else if (op < 96) {
                    KvValueView out = {};
                    KvGetStatus gs = kv_store_get(s, worker, keys[key_idx],
                                                  logical_now.load(std::memory_order_acquire), &out);
                    if (gs == KvGetStatus::HIT) {
                        uint32_t out_tid = 0;
                        uint64_t out_seq = 0, out_salt = 0;
                        if (!kvtest::parse_value(kvtest::view_to_string(out), &out_tid, &out_seq, &out_salt))
                            bad_value.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    kv_store_quiescent(s, worker);
                }

                if ((seq & 255u) == 0)
                    kv_store_quiescent(s, worker);
            }

            kv_store_quiescent(s, worker);
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : pool)
        t.join();

    EXPECT_EQ(invalid_status.load(std::memory_order_acquire), 0u)
        << "invalid status in stress case: " << c.name;
    EXPECT_EQ(bad_value.load(std::memory_order_acquire), 0u) << "bad value shape in stress case: " << c.name;
    EXPECT_LE(kv_store_live_bytes(s), kv_store_capacity_bytes(s) * 2u)
        << "unexpected live bytes growth in stress case: " << c.name;
    EXPECT_TRUE(kvtest::converge_to_capacity(s, 0, logical_now.load(std::memory_order_acquire), 3000))
        << "failed to converge capacity in stress case: " << c.name;

    kv_store_destroy(s);
}

std::string stress_name(const ::testing::TestParamInfo<StressCase>& info) { return info.param.name; }

INSTANTIATE_TEST_SUITE_P(
    Matrix, SharedKvStress,
    ::testing::Values(StressCase{"W4_4s", 4, 4, 4000, 1ull << 20, 400, 0x11111111ull},
                      StressCase{"W8_5s", 8, 8, 5000, 2ull << 20, 800, 0x22222222ull},
                      StressCase{"W12_4s", 12, 12, 4000, 2ull << 20, 1200, 0x33333333ull},
                      StressCase{"W16_3s_SmallCap", 16, 16, 3000, 256ull << 10, 1600, 0x44444444ull}),
    stress_name);

} // namespace
