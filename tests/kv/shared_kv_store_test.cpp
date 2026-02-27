#include "kv/shared_kv_store.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

static std::string as_string(const KvValueView& v) {
    return std::string(reinterpret_cast<const char*>(v.data), v.len);
}

TEST(SharedKv, CreateAndDestroy) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 64,
        .worker_count = 2,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(kv_store_capacity_bytes(s), cfg.capacity_bytes);
    kv_store_destroy(s);
}

TEST(SharedKv, BasicSetGetHitAndMiss) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "missing", 100, &out), KvGetStatus::MISS);
    EXPECT_EQ(kv_store_set(s, 0, "k", "v", 100, nullptr), KvSetStatus::OK);
    EXPECT_EQ(kv_store_get(s, 0, "k", 100, &out), KvGetStatus::HIT);
    EXPECT_EQ(as_string(out), "v");

    kv_store_destroy(s);
}

TEST(SharedKv, OverwriteKeepsLatestValue) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    ASSERT_EQ(kv_store_set(s, 0, "k", "first", 100, nullptr), KvSetStatus::OK);
    ASSERT_EQ(kv_store_set(s, 0, "k", "second", 101, nullptr), KvSetStatus::OK);

    KvValueView out = {};
    ASSERT_EQ(kv_store_get(s, 0, "k", 102, &out), KvGetStatus::HIT);
    EXPECT_EQ(as_string(out), "second");

    kv_store_destroy(s);
}

TEST(SharedKv, ExpireAfterMs) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions ttl = {.mode = KvExpireMode::AFTER_MS, .value_ms = 10};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 1000, &ttl), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 1009, &out), KvGetStatus::HIT);
    EXPECT_EQ(kv_store_get(s, 0, "k", 1010, &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKv, ExpireAtMs) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions ttl = {.mode = KvExpireMode::AT_MS, .value_ms = 2500};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 1000, &ttl), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 2499, &out), KvGetStatus::HIT);
    EXPECT_EQ(kv_store_get(s, 0, "k", 2500, &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKv, BoundedSizeEvictsOlderKeys) {
    KvStoreConfig cfg = {
        .capacity_bytes = 640,
        .shard_count = 1,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    for (int i = 0; i < 40; i++) {
        std::string key = "k" + std::to_string(i);
        std::string val(24, static_cast<char>('a' + (i % 26)));
        EXPECT_EQ(kv_store_set(s, 0, key, val, 1000 + static_cast<uint64_t>(i), nullptr), KvSetStatus::OK);
    }

    EXPECT_LE(kv_store_live_bytes(s), kv_store_capacity_bytes(s));

    int hits = 0;
    for (int i = 0; i < 40; i++) {
        std::string key = "k" + std::to_string(i);
        KvValueView out = {};
        if (kv_store_get(s, 0, key, 5000, &out) == KvGetStatus::HIT)
            hits++;
    }
    EXPECT_LT(hits, 40);

    kv_store_destroy(s);
}

TEST(SharedKvConcurrency, SameKeyHeavyContention) {
    KvStoreConfig cfg = {
        .capacity_bytes = 4ull << 20,
        .shard_count = 16,
        .buckets_per_shard = 256,
        .worker_count = 8,
    };
    KvStore* s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    for (uint32_t i = 0; i < 8; i++)
        ASSERT_EQ(kv_store_register_worker(s, i), 0);

    static constexpr uint32_t kWriterId = 0;
    static constexpr uint32_t kReaderCount = 6;
    static constexpr uint32_t kIters = 20000;

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<bool> reader_failed{false};

    std::thread writer([&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint32_t i = 1; i <= kIters; i++) {
            std::string val = std::to_string(i);
            KvSetStatus st = kv_store_set(s, kWriterId, "seq", val, 1000 + i, nullptr);
            if (st != KvSetStatus::OK) {
                reader_failed.store(true, std::memory_order_release);
                break;
            }
            if ((i & 63u) == 0)
                kv_store_quiescent(s, kWriterId);
        }
        stop.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);
    for (uint32_t r = 0; r < kReaderCount; r++) {
        readers.emplace_back([&, r]() {
            uint32_t worker = r + 1;
            uint32_t last = 0;
            while (!start.load(std::memory_order_acquire)) {
            }
            while (!stop.load(std::memory_order_acquire)) {
                KvValueView out = {};
                KvGetStatus gs = kv_store_get(s, worker, "seq", 999999, &out);
                if (gs == KvGetStatus::HIT) {
                    std::string v = as_string(out);
                    uint32_t cur = static_cast<uint32_t>(std::strtoul(v.c_str(), nullptr, 10));
                    if (cur < last) {
                        reader_failed.store(true, std::memory_order_release);
                        break;
                    }
                    last = cur;
                }
                kv_store_quiescent(s, worker);
            }
        });
    }

    start.store(true, std::memory_order_release);
    writer.join();
    for (auto& t : readers)
        t.join();

    EXPECT_FALSE(reader_failed.load(std::memory_order_acquire));
    kv_store_destroy(s);
}
