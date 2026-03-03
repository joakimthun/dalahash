#include "shared_kv_bench_common.h"

#include "kv/shared_kv_store.h"
#include "kv/shared_kv_store_internal_stats.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

namespace {

struct MultiBenchContext {
    KvStore* store = nullptr;
    std::vector<std::string> keys;
    std::vector<std::string> miss_keys;
    std::vector<std::string> values;
    uint32_t dataset_size = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    uint32_t worker_count = 0;
};

static MultiBenchContext* g_ctx = nullptr;
static std::mutex g_ctx_mu;
static std::atomic<uint32_t> g_teardown_count{0};

static uint32_t next_pow2_u32(uint32_t v) {
    if (v <= 1)
        return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1u;
}

static uint32_t derive_shard_count(uint32_t worker_count) {
    uint32_t target = worker_count == 0 ? 1u : worker_count;
    if (target > UINT32_MAX / 4u)
        target = UINT32_MAX / 4u;
    target *= 4u;
    if (target < 64u)
        target = 64u;
    return next_pow2_u32(target);
}

static uint32_t derive_buckets_per_shard(uint64_t capacity_bytes, uint32_t shard_count) {
    if (shard_count == 0)
        return 16u;

    uint64_t total_buckets = capacity_bytes / 256u;
    if (total_buckets < 64u)
        total_buckets = 64u;
    uint64_t per_shard = total_buckets / shard_count;
    if (per_shard < 16u)
        per_shard = 16u;
    if (per_shard > static_cast<uint64_t>(UINT32_MAX / 2u)) {
        per_shard = static_cast<uint64_t>(UINT32_MAX / 2u);
    }

    uint32_t buckets = next_pow2_u32(static_cast<uint32_t>(per_shard));
    if (buckets <= UINT32_MAX / 2u)
        buckets *= 2u;
    return buckets;
}

static void destroy_context(MultiBenchContext* ctx) {
    if (!ctx)
        return;
    if (ctx->store)
        kv_store_destroy(ctx->store);
    delete ctx;
}

static MultiBenchContext* create_context(uint32_t dataset_size, uint32_t key_size, uint32_t value_size,
                                         uint32_t worker_count) {
    MultiBenchContext* ctx = new (std::nothrow) MultiBenchContext();
    if (!ctx)
        return nullptr;

    ctx->dataset_size = dataset_size;
    ctx->key_size = key_size;
    ctx->value_size = value_size;
    ctx->worker_count = worker_count;
    ctx->keys = kvbench::make_corpus(dataset_size, key_size, 0x6d756c74695f31ull, true);
    ctx->miss_keys = kvbench::make_corpus(dataset_size, key_size, 0x6d756c74695f33ull, true);
    ctx->values = kvbench::make_corpus(dataset_size, value_size, 0x6d756c74695f32ull, false);

    const uint64_t capacity_bytes = kvbench::derive_capacity_bytes(dataset_size, key_size, value_size);
    const uint32_t shard_count = derive_shard_count(worker_count);
    const uint32_t buckets_per_shard = derive_buckets_per_shard(capacity_bytes, shard_count);

    KvStoreConfig cfg = {
        .capacity_bytes = capacity_bytes,
        .shard_count = shard_count,
        .buckets_per_shard = buckets_per_shard,
        .worker_count = worker_count,
    };
    ctx->store = kv_store_create(&cfg);
    if (!ctx->store) {
        delete ctx;
        return nullptr;
    }

    for (uint32_t i = 0; i < worker_count; i++) {
        if (kv_store_register_worker(ctx->store, i) != 0) {
            destroy_context(ctx);
            return nullptr;
        }
    }

    uint64_t now_ms = 1;
    for (uint32_t i = 0; i < dataset_size; i++) {
        KvSetStatus st = kv_store_set(ctx->store, 0, ctx->keys[i], ctx->values[i], now_ms, nullptr);
        if (st != KvSetStatus::OK) {
            destroy_context(ctx);
            return nullptr;
        }
        now_ms++;
    }
    kv_store_quiescent(ctx->store, 0);
    (void)kv_store_internal_stats_reset(ctx->store);
    return ctx;
}

static bool parse_sizes(const benchmark::State& state, uint32_t* dataset_size, uint32_t* key_size,
                        uint32_t* value_size) {
    if (!dataset_size || !key_size || !value_size)
        return false;
    if (state.range(0) <= 0 || state.range(1) <= 0 || state.range(2) <= 0)
        return false;
    *dataset_size = static_cast<uint32_t>(state.range(0));
    *key_size = static_cast<uint32_t>(state.range(1));
    *value_size = static_cast<uint32_t>(state.range(2));
    return true;
}

static void add_baseline_args(benchmark::internal::Benchmark* b) {
    b->Args({4096, 16, 64});
    b->Args({4096, 16, 256});
    b->Args({16384, 24, 128});
    b->Args({65536, 32, 256});
}

static void add_large_dataset_args(benchmark::internal::Benchmark* b) {
    b->Args({2000000, 16, 64});
    b->Args({4000000, 16, 64});
    b->Args({8000000, 16, 64});
    b->Args({2000000, 24, 128});
    b->Args({4000000, 24, 128});
    b->Args({8000000, 24, 128});
}

static void add_larger_key_value_dataset_args(benchmark::internal::Benchmark* b) {
    b->Args({4000000, 128, 256});
}

static void add_all_args(benchmark::internal::Benchmark* b) {
    add_baseline_args(b);
    add_large_dataset_args(b);
    add_larger_key_value_dataset_args(b);
}

static void add_v2_stats(benchmark::State& state, KvStore* store) {
    KvStoreInternalStats stats = {};
    if (!kv_store_internal_stats_snapshot(store, &stats))
        return;
    static constexpr auto kAvg = benchmark::Counter::kAvgThreads;
    state.counters["v2_set_calls"] = benchmark::Counter(static_cast<double>(stats.set_calls), kAvg);
    state.counters["v2_set_same_size"] =
        benchmark::Counter(static_cast<double>(stats.set_overwrite_same_size), kAvg);
    state.counters["v2_set_size_change"] =
        benchmark::Counter(static_cast<double>(stats.set_overwrite_size_change), kAvg);
    state.counters["v2_set_inserts"] = benchmark::Counter(static_cast<double>(stats.set_inserts), kAvg);
    state.counters["v2_set_evictions"] = benchmark::Counter(static_cast<double>(stats.set_evictions), kAvg);
    state.counters["v2_set_cas_failures"] =
        benchmark::Counter(static_cast<double>(stats.set_cas_failures), kAvg);
    state.counters["v2_set_allocations"] =
        benchmark::Counter(static_cast<double>(stats.set_allocations), kAvg);
    state.counters["v2_quiescent_calls"] =
        benchmark::Counter(static_cast<double>(stats.quiescent_calls), kAvg);
    state.counters["v2_quiescent_fast"] =
        benchmark::Counter(static_cast<double>(stats.quiescent_fast_returns), kAvg);
    state.counters["v2_retire_batches"] =
        benchmark::Counter(static_cast<double>(stats.retire_batches_enqueued), kAvg);
    state.counters["v2_retired_nodes"] =
        benchmark::Counter(static_cast<double>(stats.retired_nodes_enqueued), kAvg);
    state.counters["v2_maintenance_runs"] =
        benchmark::Counter(static_cast<double>(stats.maintenance_runs), kAvg);
}

class SharedKvMultiFixture : public benchmark::Fixture {
  public:
    void SetUp(const benchmark::State& state) override {
        uint32_t dataset_size = 0;
        uint32_t key_size = 0;
        uint32_t value_size = 0;
        if (!parse_sizes(state, &dataset_size, &key_size, &value_size))
            return;

        std::lock_guard<std::mutex> lock(g_ctx_mu);
        if (!g_ctx) {
            g_ctx =
                create_context(dataset_size, key_size, value_size, static_cast<uint32_t>(state.threads()));
            g_teardown_count.store(0, std::memory_order_release);
        }
    }

    void TearDown(const benchmark::State& state) override {
        const uint32_t finished = g_teardown_count.fetch_add(1, std::memory_order_acq_rel) + 1u;
        if (finished == static_cast<uint32_t>(state.threads())) {
            std::lock_guard<std::mutex> lock(g_ctx_mu);
            destroy_context(g_ctx);
            g_ctx = nullptr;
            g_teardown_count.store(0, std::memory_order_release);
        }
    }
};

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Mixed80_20)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->values.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f7277ull, static_cast<uint64_t>(g_ctx->dataset_size) ^
                                                     (static_cast<uint64_t>(g_ctx->key_size) << 21u) ^
                                                     (static_cast<uint64_t>(g_ctx->value_size) << 42u));
    kvbench::XorShift64 rng(kvbench::mix_seed(base_seed, worker_id + 1u));

    uint64_t read_ops = 0;
    uint64_t write_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t now_ms = 1000u + worker_id;

    for (auto _ : state) {
        (void)_;
        const uint64_t r = rng.next();
        const uint32_t key_idx = static_cast<uint32_t>(r % g_ctx->keys.size());
        const std::string& key = g_ctx->keys[key_idx];

        if ((r % 10u) < 8u) {
            KvValueView out = {};
            KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, now_ms, &out);
            if (gs != KvGetStatus::HIT) {
                state.SkipWithError("kv_store_get miss in mixed benchmark");
                break;
            }
            benchmark::DoNotOptimize(out.data);
            benchmark::DoNotOptimize(out.len);
            read_ops++;
            bytes += static_cast<uint64_t>(key.size()) + out.len;
        } else {
            const uint32_t value_idx = static_cast<uint32_t>(rng.next() % g_ctx->values.size());
            const std::string& value = g_ctx->values[value_idx];
            KvSetStatus st = kv_store_set(g_ctx->store, worker_id, key, value, now_ms, nullptr);
            if (st != KvSetStatus::OK) {
                state.SkipWithError("kv_store_set failed in mixed benchmark");
                break;
            }
            write_ops++;
            bytes += static_cast<uint64_t>(key.size() + value.size());
        }

        now_ms++;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(read_ops + write_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["read_ops"] =
        benchmark::Counter(static_cast<double>(read_ops), benchmark::Counter::kIsRate);
    state.counters["write_ops"] =
        benchmark::Counter(static_cast<double>(write_ops), benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Get100)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f726full, static_cast<uint64_t>(g_ctx->dataset_size) ^
                                                     (static_cast<uint64_t>(g_ctx->key_size) << 21u) ^
                                                     (static_cast<uint64_t>(g_ctx->value_size) << 42u));
    kvbench::XorShift64 rng(kvbench::mix_seed(base_seed, worker_id + 1u));

    uint64_t read_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t now_ms = 1000u + worker_id;

    for (auto _ : state) {
        (void)_;
        const uint64_t r = rng.next();
        const uint32_t key_idx = static_cast<uint32_t>(r % g_ctx->keys.size());
        const std::string& key = g_ctx->keys[key_idx];

        KvValueView out = {};
        KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, now_ms, &out);
        if (gs != KvGetStatus::HIT) {
            state.SkipWithError("kv_store_get miss in get100 benchmark");
            break;
        }

        benchmark::DoNotOptimize(out.data);
        benchmark::DoNotOptimize(out.len);
        read_ops++;
        bytes += static_cast<uint64_t>(key.size()) + out.len;

        now_ms++;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(read_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["read_ops"] =
        benchmark::Counter(static_cast<double>(read_ops), benchmark::Counter::kIsRate);
    state.counters["write_ops"] = benchmark::Counter(0.0, benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Get80Miss20Hit)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->miss_keys.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f6d73ull, static_cast<uint64_t>(g_ctx->dataset_size) ^
                                                     (static_cast<uint64_t>(g_ctx->key_size) << 21u) ^
                                                     (static_cast<uint64_t>(g_ctx->value_size) << 42u));
    kvbench::XorShift64 rng(kvbench::mix_seed(base_seed, worker_id + 1u));

    uint64_t hit_ops = 0;
    uint64_t miss_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t now_ms = 1000u + worker_id;

    for (auto _ : state) {
        (void)_;
        const uint64_t r = rng.next();
        const uint32_t key_idx = static_cast<uint32_t>(r % g_ctx->keys.size());

        if ((r % 10u) < 8u) {
            KvValueView out = {};
            const std::string& key = g_ctx->miss_keys[key_idx];
            KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, now_ms, &out);
            if (gs != KvGetStatus::MISS) {
                state.SkipWithError("kv_store_get hit in miss-heavy benchmark");
                break;
            }
            benchmark::DoNotOptimize(out.data);
            benchmark::DoNotOptimize(out.len);
            miss_ops++;
            bytes += static_cast<uint64_t>(key.size());
        } else {
            KvValueView out = {};
            const std::string& key = g_ctx->keys[key_idx];
            KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, now_ms, &out);
            if (gs != KvGetStatus::HIT) {
                state.SkipWithError("kv_store_get miss in hit branch");
                break;
            }
            benchmark::DoNotOptimize(out.data);
            benchmark::DoNotOptimize(out.len);
            hit_ops++;
            bytes += static_cast<uint64_t>(key.size()) + out.len;
        }

        now_ms++;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(hit_ops + miss_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["hit_ops"] = benchmark::Counter(static_cast<double>(hit_ops), benchmark::Counter::kIsRate);
    state.counters["miss_ops"] =
        benchmark::Counter(static_cast<double>(miss_ops), benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Set100Overwrite)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->values.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f7365ull, static_cast<uint64_t>(g_ctx->dataset_size) ^
                                                     (static_cast<uint64_t>(g_ctx->key_size) << 21u) ^
                                                     (static_cast<uint64_t>(g_ctx->value_size) << 42u));
    kvbench::XorShift64 rng(kvbench::mix_seed(base_seed, worker_id + 1u));

    uint64_t write_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t now_ms = 1000u + worker_id;

    for (auto _ : state) {
        (void)_;
        const uint32_t key_idx = static_cast<uint32_t>(rng.next() % g_ctx->keys.size());
        const uint32_t value_idx = static_cast<uint32_t>(rng.next() % g_ctx->values.size());
        const std::string& key = g_ctx->keys[key_idx];
        const std::string& value = g_ctx->values[value_idx];

        KvSetStatus st = kv_store_set(g_ctx->store, worker_id, key, value, now_ms, nullptr);
        if (st != KvSetStatus::OK) {
            state.SkipWithError("kv_store_set failed in set100overwrite benchmark");
            break;
        }

        write_ops++;
        bytes += static_cast<uint64_t>(key.size() + value.size());
        now_ms++;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(write_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["read_ops"] = benchmark::Counter(0.0, benchmark::Counter::kIsRate);
    state.counters["write_ops"] =
        benchmark::Counter(static_cast<double>(write_ops), benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Delete50Set50)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->values.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    uint64_t delete_ops = 0;
    uint64_t delete_miss = 0;
    uint64_t set_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t now_ms = 1000u + worker_id;
    uint64_t cursor = 0;
    bool do_delete = true;

    for (auto _ : state) {
        (void)_;
        const uint32_t key_idx = static_cast<uint32_t>(
            ((cursor * static_cast<uint64_t>(g_ctx->worker_count)) + worker_id) % g_ctx->keys.size());
        const std::string& key = g_ctx->keys[key_idx];
        const std::string& value = g_ctx->values[key_idx % g_ctx->values.size()];

        if (do_delete) {
            KvDeleteStatus ds = kv_store_delete(g_ctx->store, worker_id, key, now_ms);
            if (ds != KvDeleteStatus::OK && ds != KvDeleteStatus::NOT_FOUND) {
                state.SkipWithError("kv_store_delete failed in delete/set benchmark");
                break;
            }
            if (ds == KvDeleteStatus::OK)
                delete_ops++;
            else
                delete_miss++;
            bytes += static_cast<uint64_t>(key.size());
        } else {
            KvSetStatus st = kv_store_set(g_ctx->store, worker_id, key, value, now_ms, nullptr);
            if (st != KvSetStatus::OK) {
                state.SkipWithError("kv_store_set failed in delete/set benchmark");
                break;
            }
            set_ops++;
            bytes += static_cast<uint64_t>(key.size() + value.size());
            cursor++;
        }

        do_delete = !do_delete;
        now_ms++;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(delete_ops + delete_miss + set_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["delete_ops"] =
        benchmark::Counter(static_cast<double>(delete_ops), benchmark::Counter::kIsRate);
    state.counters["delete_miss"] =
        benchmark::Counter(static_cast<double>(delete_miss), benchmark::Counter::kIsRate);
    state.counters["set_ops"] = benchmark::Counter(static_cast<double>(set_ops), benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, TtlChurn)(benchmark::State& state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->values.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const KvSetOptions ttl_opts = {.mode = KvExpireMode::AFTER_MS, .value_ms = 2};

    uint64_t set_ops = 0;
    uint64_t hit_ops = 0;
    uint64_t miss_ops = 0;
    uint64_t bytes = 0;
    uint32_t q_count = 0;
    uint64_t cycle_ms = 1000u + worker_id;
    uint64_t cursor = 0;
    uint32_t phase = 0;

    for (auto _ : state) {
        (void)_;
        const uint32_t key_idx = static_cast<uint32_t>(
            ((cursor * static_cast<uint64_t>(g_ctx->worker_count)) + worker_id) % g_ctx->keys.size());
        const std::string& key = g_ctx->keys[key_idx];
        const std::string& value = g_ctx->values[key_idx % g_ctx->values.size()];

        if (phase == 0) {
            KvSetStatus st = kv_store_set(g_ctx->store, worker_id, key, value, cycle_ms, &ttl_opts);
            if (st != KvSetStatus::OK) {
                state.SkipWithError("kv_store_set failed in ttl benchmark");
                break;
            }
            set_ops++;
            bytes += static_cast<uint64_t>(key.size() + value.size());
        } else if (phase == 1) {
            KvValueView out = {};
            KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, cycle_ms + 1u, &out);
            if (gs != KvGetStatus::HIT) {
                state.SkipWithError("kv_store_get miss on ttl hit check");
                break;
            }
            benchmark::DoNotOptimize(out.data);
            benchmark::DoNotOptimize(out.len);
            hit_ops++;
            bytes += static_cast<uint64_t>(key.size()) + out.len;
        } else {
            KvValueView out = {};
            KvGetStatus gs = kv_store_get(g_ctx->store, worker_id, key, cycle_ms + 3u, &out);
            if (gs != KvGetStatus::MISS) {
                state.SkipWithError("kv_store_get hit on ttl miss check");
                break;
            }
            benchmark::DoNotOptimize(out.data);
            benchmark::DoNotOptimize(out.len);
            miss_ops++;
            bytes += static_cast<uint64_t>(key.size());
            cycle_ms += 4u;
            cursor++;
        }

        phase = (phase + 1u) % 3u;
        q_count++;
        if ((q_count & 255u) == 0u)
            kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(set_ops + hit_ops + miss_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["set_ops"] = benchmark::Counter(static_cast<double>(set_ops), benchmark::Counter::kIsRate);
    state.counters["hit_ops"] = benchmark::Counter(static_cast<double>(hit_ops), benchmark::Counter::kIsRate);
    state.counters["miss_ops"] =
        benchmark::Counter(static_cast<double>(miss_ops), benchmark::Counter::kIsRate);
    add_v2_stats(state, g_ctx->store);
}

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Mixed80_20)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Get100)->Apply(add_all_args)->ThreadRange(1, 16)->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Get80Miss20Hit)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Set100Overwrite)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Delete50Set50)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, TtlChurn)->Apply(add_all_args)->ThreadRange(1, 16)->UseRealTime();

} // namespace

BENCHMARK_MAIN();
