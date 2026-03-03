#include "kv/shared_kv_store.h"
#include "kv/shared_kv_store_internal_stats.h"
#include "shared_kv_store_test_utils.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

TEST(SharedKvEviction, ConcurrentPressureProducesEvictionsAndStaysStable) {
    KvStoreConfig cfg = {
        .capacity_bytes = 12ull << 10, // intentionally tight to force eviction
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 8,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    for (uint32_t i = 0; i < 8; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    static constexpr uint32_t kWriters = 6;
    static constexpr uint32_t kReaders = 2;
    std::vector<std::string> keys = kvtest::make_key_space(1500);

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> invalid_status{0};
    std::atomic<uint64_t> bad_value_format{0};
    std::atomic<uint64_t> logical_now{1000};
    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < kWriters; w++) {
        threads.emplace_back([&, w]() {
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0xabc000ull + w * 17ull);
            uint64_t seq = 1;
            while (!stop.load(std::memory_order_acquire)) {
                uint32_t idx = static_cast<uint32_t>(rng.next() % keys.size());
                std::string value = kvtest::make_value(w, seq++, rng.next());
                uint64_t now = logical_now.fetch_add(1, std::memory_order_acq_rel);
                KvSetStatus st = kv_store_set(s, w, keys[idx], value, now, nullptr);
                if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                    invalid_status.fetch_add(1, std::memory_order_relaxed);
                if ((seq & 127u) == 0)
                    kv_store_quiescent(s, w);
            }
            kv_store_quiescent(s, w);
        });
    }

    for (uint32_t r = 0; r < kReaders; r++) {
        threads.emplace_back([&, r]() {
            uint32_t worker = kWriters + r;
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0xfeed000ull + r * 31ull);
            while (!stop.load(std::memory_order_acquire)) {
                uint32_t idx = static_cast<uint32_t>(rng.next() % keys.size());
                KvValueView out = {};
                KvGetStatus gs =
                    kv_store_get(s, worker, keys[idx], logical_now.load(std::memory_order_acquire), &out);
                if (gs == KvGetStatus::HIT) {
                    uint32_t tid = 0;
                    uint64_t seq = 0, salt = 0;
                    if (!kvtest::parse_value(kvtest::view_to_string(out), &tid, &seq, &salt))
                        bad_value_format.fetch_add(1, std::memory_order_relaxed);
                }
                kv_store_quiescent(s, worker);
            }
            kv_store_quiescent(s, worker);
        });
    }

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(invalid_status.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(bad_value_format.load(std::memory_order_acquire), 0u);
    EXPECT_LE(kv_store_live_bytes(s), kv_store_capacity_bytes(s) * 4u);
    EXPECT_TRUE(kvtest::converge_to_capacity(s, 0, logical_now.load(std::memory_order_acquire), 2000));

    // Confirm that under pressure at least some keys are missing (eviction happened).
    uint32_t hits = 0;
    uint64_t final_now = logical_now.load(std::memory_order_acquire);
    for (const std::string& k : keys) {
        KvValueView out = {};
        if (kv_store_get(s, 0, k, final_now, &out) == KvGetStatus::HIT)
            hits++;
    }
    EXPECT_LT(hits, static_cast<uint32_t>(keys.size()));

    kv_store_destroy(s);
}

TEST(SharedKvConcurrency, DisjointWritersMonotonicReaders) {
    static constexpr uint32_t kWriterCount = 6;
    static constexpr uint32_t kReaderCount = 6;
    static constexpr uint32_t kIters = 35000;

    KvStoreConfig cfg = {
        .capacity_bytes = 8ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 256,
        .worker_count = kWriterCount + kReaderCount,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < cfg.worker_count; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    std::vector<std::string> keys;
    keys.reserve(kWriterCount);
    for (uint32_t i = 0; i < kWriterCount; i++)
        keys.push_back("owner:" + std::to_string(i));

    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < kWriterCount; w++) {
        threads.emplace_back([&, w]() {
            kvtest::wait_for_start(start);
            for (uint32_t i = 1; i <= kIters; i++) {
                std::string value = kvtest::make_value(w, i, 0);
                KvSetStatus st = kv_store_set(s, w, keys[w], value, 1000 + i, nullptr);
                if (st != KvSetStatus::OK) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                if ((i & 255u) == 0)
                    kv_store_quiescent(s, w);
            }
            kv_store_quiescent(s, w);
        });
    }

    for (uint32_t r = 0; r < kReaderCount; r++) {
        threads.emplace_back([&, r]() {
            uint32_t worker = kWriterCount + r;
            std::vector<uint64_t> last_seen(kWriterCount, 0);
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0x5544332211ull + r);
            while (!failed.load(std::memory_order_acquire)) {
                uint32_t owner = static_cast<uint32_t>(rng.next() % kWriterCount);
                KvValueView out = {};
                KvGetStatus gs = kv_store_get(s, worker, keys[owner], 9999999, &out);
                if (gs == KvGetStatus::HIT) {
                    uint32_t tid = 0;
                    uint64_t seq = 0, salt = 0;
                    if (!kvtest::parse_value(kvtest::view_to_string(out), &tid, &seq, &salt)) {
                        failed.store(true, std::memory_order_release);
                        break;
                    }
                    if (tid != owner || seq < last_seen[owner]) {
                        failed.store(true, std::memory_order_release);
                        break;
                    }
                    last_seen[owner] = seq;
                }
                kv_store_quiescent(s, worker);
                bool all_done = true;
                for (uint64_t v : last_seen) {
                    if (v < kIters / 4u) {
                        all_done = false;
                        break;
                    }
                }
                if (all_done)
                    break;
            }
            kv_store_quiescent(s, worker);
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_FALSE(failed.load(std::memory_order_acquire));
    kv_store_destroy(s);
}

TEST(SharedKvTTLConcurrency, WritersWithShortTTLEventuallyExpireAfterStop) {
    static constexpr uint32_t kWorkers = 10;
    static constexpr uint32_t kKeys = 400;

    KvStoreConfig cfg = {
        .capacity_bytes = 8ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 256,
        .worker_count = kWorkers,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < kWorkers; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    std::vector<std::string> keys = kvtest::make_key_space(kKeys);
    KvSetOptions ttl = {.mode = KvExpireMode::AFTER_MS, .value_ms = 30};

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> logical_now{10000};
    std::atomic<uint64_t> invalid_status{0};
    std::vector<std::thread> threads;

    // Writers continually refresh keys with short TTL.
    for (uint32_t w = 0; w < 6; w++) {
        threads.emplace_back([&, w]() {
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0xbeef100ull + w * 3ull);
            uint64_t seq = 1;
            while (!stop.load(std::memory_order_acquire)) {
                uint32_t idx = static_cast<uint32_t>(rng.next() % keys.size());
                uint64_t now = logical_now.fetch_add(1, std::memory_order_acq_rel);
                std::string value = kvtest::make_value(w, seq++, 0x55);
                KvSetStatus st = kv_store_set(s, w, keys[idx], value, now, &ttl);
                if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                    invalid_status.fetch_add(1, std::memory_order_relaxed);
                if ((seq & 127u) == 0)
                    kv_store_quiescent(s, w);
            }
            kv_store_quiescent(s, w);
        });
    }

    // Readers run concurrently; we only validate structural correctness for HIT values.
    for (uint32_t r = 0; r < 4; r++) {
        threads.emplace_back([&, r]() {
            uint32_t worker = 6 + r;
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0xface000ull + r * 11ull);
            while (!stop.load(std::memory_order_acquire)) {
                uint32_t idx = static_cast<uint32_t>(rng.next() % keys.size());
                KvValueView out = {};
                KvGetStatus gs =
                    kv_store_get(s, worker, keys[idx], logical_now.load(std::memory_order_acquire), &out);
                if (gs == KvGetStatus::HIT) {
                    uint32_t tid = 0;
                    uint64_t seq = 0, salt = 0;
                    if (!kvtest::parse_value(kvtest::view_to_string(out), &tid, &seq, &salt))
                        invalid_status.fetch_add(1, std::memory_order_relaxed);
                }
                kv_store_quiescent(s, worker);
            }
            kv_store_quiescent(s, worker);
        });
    }

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(invalid_status.load(std::memory_order_acquire), 0u);

    // Advance logical time beyond TTL horizon and verify all keys expire.
    uint64_t now = logical_now.fetch_add(10000, std::memory_order_acq_rel) + 10000;
    for (uint32_t i = 0; i < kWorkers; i++)
        kv_store_quiescent(s, i);

    for (const std::string& k : keys) {
        KvValueView out = {};
        EXPECT_EQ(kv_store_get(s, 0, k, now, &out), KvGetStatus::MISS);
    }

    kv_store_destroy(s);
}

TEST(SharedKvConcurrency, SameKeyMultiWriterReadersKeepValueShapeValid) {
    static constexpr uint32_t kWriters = 6;
    static constexpr uint32_t kReaders = 6;
    static constexpr uint32_t kWorkers = kWriters + kReaders;

    KvStoreConfig cfg = {
        .capacity_bytes = 4ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 256,
        .worker_count = kWorkers,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < kWorkers; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> bad_status{0};
    std::atomic<uint64_t> bad_value{0};
    std::atomic<uint64_t> logical_now{5000};
    std::vector<std::thread> threads;

    for (uint32_t w = 0; w < kWriters; w++) {
        threads.emplace_back([&, w]() {
            kvtest::wait_for_start(start);
            kvtest::XorShift64 rng(0xaaa100ull + w * 13ull);
            uint64_t seq = 1;
            while (!stop.load(std::memory_order_acquire)) {
                uint64_t now = logical_now.fetch_add(1, std::memory_order_acq_rel);
                std::string value = kvtest::make_value(w, seq++, rng.next());
                KvSetStatus st = kv_store_set(s, w, "shared:key", value, now, nullptr);
                if (st != KvSetStatus::OK && st != KvSetStatus::OOM)
                    bad_status.fetch_add(1, std::memory_order_relaxed);
                if ((seq & 127u) == 0)
                    kv_store_quiescent(s, w);
            }
            kv_store_quiescent(s, w);
        });
    }

    for (uint32_t r = 0; r < kReaders; r++) {
        threads.emplace_back([&, r]() {
            uint32_t worker = kWriters + r;
            kvtest::wait_for_start(start);
            while (!stop.load(std::memory_order_acquire)) {
                KvValueView out = {};
                KvGetStatus gs =
                    kv_store_get(s, worker, "shared:key", logical_now.load(std::memory_order_acquire), &out);
                if (gs == KvGetStatus::HIT) {
                    uint32_t tid = 0;
                    uint64_t seq = 0, salt = 0;
                    if (!kvtest::parse_value(kvtest::view_to_string(out), &tid, &seq, &salt))
                        bad_value.fetch_add(1, std::memory_order_relaxed);
                }
                kv_store_quiescent(s, worker);
            }
            kv_store_quiescent(s, worker);
        });
    }

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(bad_status.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(bad_value.load(std::memory_order_acquire), 0u);

    kv_store_destroy(s);
}

TEST(SharedKvConcurrency, ConcurrentRegisterAndQuiescentRemainStable) {
    static constexpr uint32_t kWorkers = 24;

    KvStoreConfig cfg = {
        .capacity_bytes = 2ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 128,
        .worker_count = kWorkers,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(kWorkers);

    for (uint32_t i = 0; i < kWorkers; i++) {
        threads.emplace_back([&, i]() {
            kvtest::wait_for_start(start);

            if (kv_store_register_worker(s, i) != 0) {
                failed.store(true, std::memory_order_release);
                return;
            }
            if (kv_store_register_worker(s, i) != 0) {
                failed.store(true, std::memory_order_release);
                return;
            }

            kvtest::XorShift64 rng(0x424242ull + i * 19ull);
            for (uint32_t iter = 0; iter < 25000; iter++) {
                std::string key = "rw:" + std::to_string(rng.next() % 128u);
                std::string value = kvtest::make_value(i, iter + 1u, rng.next());
                KvSetStatus st = kv_store_set(s, i, key, value, 1000 + iter, nullptr);
                if (st != KvSetStatus::OK && st != KvSetStatus::OOM) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                KvValueView out = {};
                KvGetStatus gs = kv_store_get(s, i, key, 2000 + iter, &out);
                if (gs == KvGetStatus::HIT) {
                    uint32_t tid = 0;
                    uint64_t seq = 0, salt = 0;
                    if (!kvtest::parse_value(kvtest::view_to_string(out), &tid, &seq, &salt)) {
                        failed.store(true, std::memory_order_release);
                        break;
                    }
                }
                if ((iter & 63u) == 0)
                    kv_store_quiescent(s, i);
            }

            kv_store_quiescent(s, i);
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_FALSE(failed.load(std::memory_order_acquire));
    EXPECT_TRUE(kvtest::converge_to_capacity(s, 0, 50000, 3000));

    kv_store_destroy(s);
}

TEST(SharedKvConcurrency, ManyWritersBurstRefillDoesNotFalseOOM) {
    static constexpr uint32_t kWorkers = 64;
    static constexpr uint32_t kIters = 1024;

    KvStoreConfig cfg = {
        .capacity_bytes = 8ull << 20,
        .shard_count = 64,
        .buckets_per_shard = 256,
        .worker_count = kWorkers,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < kWorkers; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    std::atomic<bool> start{false};
    std::atomic<uint64_t> bad_status{0};
    std::vector<std::thread> threads;
    threads.reserve(kWorkers);

    for (uint32_t i = 0; i < kWorkers; i++) {
        threads.emplace_back([&, i]() {
            const std::string key = "burst:" + std::to_string(i);
            kvtest::wait_for_start(start);

            for (uint32_t iter = 0; iter < kIters; iter++) {
                std::string value = kvtest::make_value(i, iter + 1u, 0xabc000ull + iter);
                KvSetStatus st = kv_store_set(s, i, key, value, 1000u + iter, nullptr);
                if (st != KvSetStatus::OK) {
                    bad_status.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                if ((iter & 63u) == 0u)
                    kv_store_quiescent(s, i);
            }

            kv_store_quiescent(s, i);
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(bad_status.load(std::memory_order_acquire), 0u);
    kv_store_destroy(s);
}

TEST(SharedKvV2, QuiescentFastPathPublishesStats) {
    KvStoreConfig cfg = {
        .capacity_bytes = 4ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 256,
        .worker_count = 4,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    for (uint32_t i = 0; i < cfg.worker_count; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    for (uint32_t i = 0; i < 2000; i++) {
        std::string key = "k:" + std::to_string(i & 127u);
        std::string value = kvtest::make_value(i & 3u, i + 1u, 0x55u + i);
        ASSERT_EQ(kv_store_set(s, i & 3u, key, value, 1000u + i, nullptr), KvSetStatus::OK);
        if ((i & 31u) == 0u)
            kv_store_quiescent(s, i & 3u);
    }

    for (uint32_t i = 0; i < 512; i++) {
        kv_store_quiescent(s, i & 3u);
    }

    KvStoreInternalStats stats = {};
    ASSERT_TRUE(kv_store_internal_stats_snapshot(s, &stats));
    EXPECT_GT(stats.quiescent_calls, 0u);
    EXPECT_GT(stats.quiescent_fast_returns, 0u);
    EXPECT_GT(stats.retire_batches_enqueued, 0u);
    EXPECT_GT(stats.retired_nodes_enqueued, 0u);
    EXPECT_GT(stats.maintenance_runs, 0u);

    kv_store_destroy(s);
}
