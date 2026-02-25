#include "kv/shared_kv_store.h"
#include "shared_kv_store_test_utils.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <thread>
#include <vector>

TEST(SharedKvApi, CreateNullConfigFails) {
    EXPECT_EQ(kv_store_create(nullptr), nullptr);
}

TEST(SharedKvApi, ZeroConfigDefaultsAreUsable) {
    KvStoreConfig cfg = {};
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    EXPECT_GT(kv_store_capacity_bytes(s), 0u);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    EXPECT_EQ(kv_store_set(s, 0, "k", "v", 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 10, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "v");

    kv_store_destroy(s);
}

TEST(SharedKvApi, NonPowerOfTwoConfigIsAccepted) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 3,
        .buckets_per_shard = 11,
        .worker_count = 2,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);
    ASSERT_EQ(kv_store_register_worker(s, 1), 0);

    EXPECT_EQ(kv_store_set(s, 0, "a", "b", 1, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 1, "a", 1, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "b");

    kv_store_destroy(s);
}

TEST(SharedKvApi, RegisterWorkerRangeValidation) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 2,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    EXPECT_EQ(kv_store_register_worker(s, 0), 0);
    EXPECT_EQ(kv_store_register_worker(s, 1), 0);
    EXPECT_NE(kv_store_register_worker(s, 2), 0);
    EXPECT_NE(kv_store_register_worker(s, 99), 0);

    kv_store_destroy(s);
}

TEST(SharedKvApi, QuiescentInvalidWorkerIsNoop) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 100, nullptr), KvSetStatus::OK);

    kv_store_quiescent(s, 42);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 100, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "v");

    kv_store_destroy(s);
}

TEST(SharedKvApi, NullStoreAndNullOutHandling) {
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(nullptr, 0, "k", 1, &out), KvGetStatus::MISS);
    EXPECT_EQ(kv_store_get(nullptr, 0, "k", 1, nullptr), KvGetStatus::MISS);
    EXPECT_EQ(kv_store_set(nullptr, 0, "k", "v", 1, nullptr), KvSetStatus::INVALID);
}

TEST(SharedKvApi, NullStoreAuxApisAreSafe) {
    EXPECT_NE(kv_store_register_worker(nullptr, 0), 0);
    kv_store_quiescent(nullptr, 0);
    EXPECT_EQ(kv_store_live_bytes(nullptr), 0u);
    EXPECT_EQ(kv_store_capacity_bytes(nullptr), 0u);
}

TEST(SharedKvApi, EmptyKeyAndValueRoundTrip) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    EXPECT_EQ(kv_store_set(s, 0, "", "", 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "", 10, &out), KvGetStatus::HIT);
    EXPECT_EQ(out.len, 0u);

    kv_store_destroy(s);
}

TEST(SharedKvApi, BinaryKeyAndValueRoundTrip) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    std::string key = std::string("k", 1) + std::string("\0", 1) + std::string("y", 1);
    std::string value = std::string("v", 1) + std::string("\0", 1) + std::string("\n", 1) + std::string("x", 1);

    EXPECT_EQ(kv_store_set(s, 0, key, value, 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    ASSERT_EQ(kv_store_get(s, 0, key, 10, &out), KvGetStatus::HIT);
    ASSERT_EQ(out.len, value.size());
    EXPECT_EQ(std::memcmp(out.data, value.data(), value.size()), 0);

    kv_store_destroy(s);
}

TEST(SharedKvApi, RegisterWorkerIsIdempotent) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);

    EXPECT_EQ(kv_store_register_worker(s, 0), 0);
    EXPECT_EQ(kv_store_register_worker(s, 0), 0);

    EXPECT_EQ(kv_store_set(s, 0, "k", "v", 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 10, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "v");

    kv_store_destroy(s);
}

TEST(SharedKvApi, GetMissResetsOutputView) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvValueView out = {
        .data = reinterpret_cast<const uint8_t *>("abc"),
        .len = 99,
    };
    EXPECT_EQ(kv_store_get(s, 0, "missing", 1, &out), KvGetStatus::MISS);
    EXPECT_EQ(out.data, nullptr);
    EXPECT_EQ(out.len, 0u);

    kv_store_destroy(s);
}

TEST(SharedKvApi, InRangeUnregisteredWorkerCanReadAndWrite) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 2,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    // worker 1 has not been registered yet, but API still permits access.
    EXPECT_EQ(kv_store_set(s, 1, "k", "v1", 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 1, "k", 10, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "v1");

    kv_store_destroy(s);
}

TEST(SharedKvApi, OutOfRangeWorkerIdsFallbackAndRemainUsable) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    EXPECT_EQ(kv_store_set(s, 99, "k", "v", 10, nullptr), KvSetStatus::OK);
    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 777, "k", 10, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "v");

    kv_store_destroy(s);
}

TEST(SharedKvApi, TtlAfterMsZeroExpiresImmediately) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions opts = {.mode = KvExpireMode::AFTER_MS, .value_ms = 0};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 1000, &opts), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 1000, &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKvApi, TtlAtBoundary) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions opts = {.mode = KvExpireMode::AT_MS, .value_ms = 2000};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 1000, &opts), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 1999, &out), KvGetStatus::HIT);
    EXPECT_EQ(kv_store_get(s, 0, "k", 2000, &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKvApi, TtlAfterOverflowSaturatesToMaxTimestamp) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    uint64_t now = std::numeric_limits<uint64_t>::max() - 5u;
    KvSetOptions opts = {.mode = KvExpireMode::AFTER_MS, .value_ms = 100u};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", now, &opts), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", std::numeric_limits<uint64_t>::max() - 1u, &out), KvGetStatus::HIT);
    EXPECT_EQ(kv_store_get(s, 0, "k", std::numeric_limits<uint64_t>::max(), &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKvApi, TtlAtPastTimestampIsImmediatelyExpired) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions opts = {.mode = KvExpireMode::AT_MS, .value_ms = 100u};
    ASSERT_EQ(kv_store_set(s, 0, "k", "v", 1000u, &opts), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 1000u, &out), KvGetStatus::MISS);

    kv_store_destroy(s);
}

TEST(SharedKvApi, OversizedValueReturnsOOM) {
    KvStoreConfig cfg = {
        .capacity_bytes = 128u,
        .shard_count = 1,
        .buckets_per_shard = 16,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    std::string big(8192, 'X');
    EXPECT_EQ(kv_store_set(s, 0, "k", big, 1, nullptr), KvSetStatus::OOM);

    kv_store_destroy(s);
}

TEST(SharedKvApi, ExpiredKeyCanBeReinserted) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 4,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions ttl = {.mode = KvExpireMode::AFTER_MS, .value_ms = 5};
    ASSERT_EQ(kv_store_set(s, 0, "k", "old", 100, &ttl), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k", 105, &out), KvGetStatus::MISS);
    EXPECT_EQ(kv_store_set(s, 0, "k", "new", 106, nullptr), KvSetStatus::OK);
    EXPECT_EQ(kv_store_get(s, 0, "k", 106, &out), KvGetStatus::HIT);
    EXPECT_EQ(kvtest::view_to_string(out), "new");

    kv_store_destroy(s);
}

TEST(SharedKvApi, LiveBytesRespondToOverwriteSizeChanges) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    ASSERT_EQ(kv_store_set(s, 0, "k", "a", 1, nullptr), KvSetStatus::OK);
    uint64_t live_small = kv_store_live_bytes(s);
    ASSERT_GT(live_small, 0u);

    std::string big(600, 'x');
    ASSERT_EQ(kv_store_set(s, 0, "k", big, 2, nullptr), KvSetStatus::OK);
    uint64_t live_big = kv_store_live_bytes(s);
    EXPECT_GE(live_big, live_small);

    ASSERT_EQ(kv_store_set(s, 0, "k", "b", 3, nullptr), KvSetStatus::OK);
    uint64_t live_small_again = kv_store_live_bytes(s);
    EXPECT_LE(live_small_again, live_big);

    kv_store_destroy(s);
}

TEST(SharedKvApi, ExplicitNoneOptionMatchesNullOptions) {
    KvStoreConfig cfg = {
        .capacity_bytes = 1ull << 20,
        .shard_count = 8,
        .buckets_per_shard = 64,
        .worker_count = 1,
    };
    KvStore *s = kv_store_create(&cfg);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(kv_store_register_worker(s, 0), 0);

    KvSetOptions opts_none = {.mode = KvExpireMode::NONE, .value_ms = 12345u};
    ASSERT_EQ(kv_store_set(s, 0, "k1", "v1", 100, nullptr), KvSetStatus::OK);
    ASSERT_EQ(kv_store_set(s, 0, "k2", "v2", 100, &opts_none), KvSetStatus::OK);

    KvValueView out = {};
    EXPECT_EQ(kv_store_get(s, 0, "k1", 999999, &out), KvGetStatus::HIT);
    EXPECT_EQ(kv_store_get(s, 0, "k2", 999999, &out), KvGetStatus::HIT);

    kv_store_destroy(s);
}

TEST(SharedKvApi, TimeHelperReturnsReasonableValue) {
    uint64_t t1 = kv_time_now_ms();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    uint64_t t2 = kv_time_now_ms();
    EXPECT_GT(t1, 0u);
    EXPECT_GE(t2, t1);
}
