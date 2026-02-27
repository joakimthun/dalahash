#include "shared_kv_bench_common.h"

#include "kv/shared_kv_store.h"

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
    KvStore *store = nullptr;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    uint32_t dataset_size = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    uint32_t worker_count = 0;
};

static MultiBenchContext *g_ctx = nullptr;
static std::mutex g_ctx_mu;
static std::atomic<uint32_t> g_teardown_count{0};

static void destroy_context(MultiBenchContext *ctx) {
    if (!ctx) return;
    if (ctx->store) kv_store_destroy(ctx->store);
    delete ctx;
}

static MultiBenchContext *create_context(uint32_t dataset_size, uint32_t key_size,
                                         uint32_t value_size, uint32_t worker_count) {
    MultiBenchContext *ctx = new (std::nothrow) MultiBenchContext();
    if (!ctx) return nullptr;

    ctx->dataset_size = dataset_size;
    ctx->key_size = key_size;
    ctx->value_size = value_size;
    ctx->worker_count = worker_count;
    ctx->keys = kvbench::make_corpus(dataset_size, key_size, 0x6d756c74695f31ull, true);
    ctx->values = kvbench::make_corpus(dataset_size, value_size, 0x6d756c74695f32ull, false);

    KvStoreConfig cfg = {
        .capacity_bytes = kvbench::derive_capacity_bytes(dataset_size, key_size, value_size),
        .shard_count = 0,
        .buckets_per_shard = 0,
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
    return ctx;
}

static bool parse_sizes(const benchmark::State &state, uint32_t *dataset_size,
                        uint32_t *key_size, uint32_t *value_size) {
    if (!dataset_size || !key_size || !value_size) return false;
    if (state.range(0) <= 0 || state.range(1) <= 0 || state.range(2) <= 0) return false;
    *dataset_size = static_cast<uint32_t>(state.range(0));
    *key_size = static_cast<uint32_t>(state.range(1));
    *value_size = static_cast<uint32_t>(state.range(2));
    return true;
}

static void add_baseline_args(benchmark::internal::Benchmark *b) {
    b->Args({4096, 16, 64});
    b->Args({4096, 16, 256});
    b->Args({16384, 24, 128});
    b->Args({65536, 32, 256});
}

static void add_large_dataset_args(benchmark::internal::Benchmark *b) {
    b->Args({2000000, 16, 64});
    b->Args({4000000, 16, 64});
    b->Args({8000000, 16, 64});
    b->Args({2000000, 24, 128});
    b->Args({4000000, 24, 128});
    b->Args({8000000, 24, 128});
}

static void add_all_args(benchmark::internal::Benchmark *b) {
    add_baseline_args(b);
    add_large_dataset_args(b);
}

class SharedKvMultiFixture : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State &state) override {
        uint32_t dataset_size = 0;
        uint32_t key_size = 0;
        uint32_t value_size = 0;
        if (!parse_sizes(state, &dataset_size, &key_size, &value_size)) return;

        std::lock_guard<std::mutex> lock(g_ctx_mu);
        if (!g_ctx) {
            g_ctx = create_context(
                dataset_size, key_size, value_size, static_cast<uint32_t>(state.threads()));
            g_teardown_count.store(0, std::memory_order_release);
        }
    }

    void TearDown(const benchmark::State &state) override {
        const uint32_t finished = g_teardown_count.fetch_add(1, std::memory_order_acq_rel) + 1u;
        if (finished == static_cast<uint32_t>(state.threads())) {
            std::lock_guard<std::mutex> lock(g_ctx_mu);
            destroy_context(g_ctx);
            g_ctx = nullptr;
            g_teardown_count.store(0, std::memory_order_release);
        }
    }
};

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Mixed80_20)(benchmark::State &state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty() || g_ctx->values.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f7277ull,
                          static_cast<uint64_t>(g_ctx->dataset_size) ^
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
        const std::string &key = g_ctx->keys[key_idx];

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
            const std::string &value = g_ctx->values[value_idx];
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
        if ((q_count & 255u) == 0u) kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(read_ops + write_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["read_ops"] =
        benchmark::Counter(static_cast<double>(read_ops), benchmark::Counter::kIsRate);
    state.counters["write_ops"] =
        benchmark::Counter(static_cast<double>(write_ops), benchmark::Counter::kIsRate);
}

BENCHMARK_DEFINE_F(SharedKvMultiFixture, Get100)(benchmark::State &state) {
    if (!g_ctx || !g_ctx->store || g_ctx->keys.empty()) {
        state.SkipWithError("multi benchmark context setup failed");
        return;
    }

    const uint32_t worker_id = static_cast<uint32_t>(state.thread_index());
    const uint64_t base_seed =
        kvbench::mix_seed(0x6d756c74695f726full,
                          static_cast<uint64_t>(g_ctx->dataset_size) ^
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
        const std::string &key = g_ctx->keys[key_idx];

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
        if ((q_count & 255u) == 0u) kv_store_quiescent(g_ctx->store, worker_id);
    }

    kv_store_quiescent(g_ctx->store, worker_id);
    state.SetItemsProcessed(static_cast<int64_t>(read_ops));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.counters["read_ops"] =
        benchmark::Counter(static_cast<double>(read_ops), benchmark::Counter::kIsRate);
    state.counters["write_ops"] = benchmark::Counter(0.0, benchmark::Counter::kIsRate);
}

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Mixed80_20)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_REGISTER_F(SharedKvMultiFixture, Get100)
    ->Apply(add_all_args)
    ->ThreadRange(1, 16)
    ->UseRealTime();

} // namespace

BENCHMARK_MAIN();
