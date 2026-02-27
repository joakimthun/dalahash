#include "shared_kv_bench_common.h"

#include "kv/shared_kv_store.h"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

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

static bool parse_sizes(const benchmark::State &state, uint32_t *dataset_size,
                        uint32_t *key_size, uint32_t *value_size) {
    if (!dataset_size || !key_size || !value_size) return false;
    if (state.range(0) <= 0 || state.range(1) <= 0 || state.range(2) <= 0) return false;
    *dataset_size = static_cast<uint32_t>(state.range(0));
    *key_size = static_cast<uint32_t>(state.range(1));
    *value_size = static_cast<uint32_t>(state.range(2));
    return true;
}

static void BM_SharedKvSingleSet(benchmark::State &state) {
    uint32_t dataset_size = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    if (!parse_sizes(state, &dataset_size, &key_size, &value_size)) {
        state.SkipWithError("invalid benchmark args");
        return;
    }

    KvStoreConfig cfg = {
        .capacity_bytes = kvbench::derive_capacity_bytes(dataset_size, key_size, value_size),
        .shard_count = 0,
        .buckets_per_shard = 0,
        .worker_count = 1,
    };
    KvStore *store = kv_store_create(&cfg);
    if (!store) {
        state.SkipWithError("kv_store_create failed");
        return;
    }
    if (kv_store_register_worker(store, 0) != 0) {
        kv_store_destroy(store);
        state.SkipWithError("kv_store_register_worker failed");
        return;
    }

    const std::vector<std::string> keys =
        kvbench::make_corpus(dataset_size, key_size, 0x73696e676c655f31ull, true);
    const std::vector<std::string> values =
        kvbench::make_corpus(dataset_size, value_size, 0x73696e676c655f32ull, false);

    uint32_t idx = 0;
    uint64_t now_ms = 1;
    uint64_t bytes = 0;
    for (auto _ : state) {
        (void)_;
        const std::string &k = keys[idx];
        const std::string &v = values[idx];
        KvSetStatus st = kv_store_set(store, 0, k, v, now_ms, nullptr);
        if (st != KvSetStatus::OK) {
            state.SkipWithError("kv_store_set failed");
            break;
        }

        benchmark::DoNotOptimize(st);
        bytes += static_cast<uint64_t>(k.size() + v.size());
        now_ms++;
        idx++;
        if (idx == dataset_size) idx = 0;
    }

    kv_store_quiescent(store, 0);
    kv_store_destroy(store);

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
}

static void BM_SharedKvSingleGetHit(benchmark::State &state) {
    uint32_t dataset_size = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    if (!parse_sizes(state, &dataset_size, &key_size, &value_size)) {
        state.SkipWithError("invalid benchmark args");
        return;
    }

    KvStoreConfig cfg = {
        .capacity_bytes = kvbench::derive_capacity_bytes(dataset_size, key_size, value_size),
        .shard_count = 0,
        .buckets_per_shard = 0,
        .worker_count = 1,
    };
    KvStore *store = kv_store_create(&cfg);
    if (!store) {
        state.SkipWithError("kv_store_create failed");
        return;
    }
    if (kv_store_register_worker(store, 0) != 0) {
        kv_store_destroy(store);
        state.SkipWithError("kv_store_register_worker failed");
        return;
    }

    const std::vector<std::string> keys =
        kvbench::make_corpus(dataset_size, key_size, 0x73696e676c655f33ull, true);
    const std::vector<std::string> values =
        kvbench::make_corpus(dataset_size, value_size, 0x73696e676c655f34ull, false);

    uint64_t preload_now = 1;
    for (uint32_t i = 0; i < dataset_size; i++) {
        KvSetStatus st = kv_store_set(store, 0, keys[i], values[i], preload_now++, nullptr);
        if (st != KvSetStatus::OK) {
            kv_store_destroy(store);
            state.SkipWithError("preload kv_store_set failed");
            return;
        }
    }
    kv_store_quiescent(store, 0);

    uint32_t idx = 0;
    uint64_t now_ms = preload_now;
    uint64_t bytes = 0;
    for (auto _ : state) {
        (void)_;
        KvValueView out = {};
        const std::string &k = keys[idx];
        KvGetStatus gs = kv_store_get(store, 0, k, now_ms, &out);
        if (gs != KvGetStatus::HIT) {
            state.SkipWithError("kv_store_get miss in hit benchmark");
            break;
        }

        benchmark::DoNotOptimize(out.data);
        benchmark::DoNotOptimize(out.len);
        bytes += static_cast<uint64_t>(k.size()) + out.len;
        now_ms++;
        idx++;
        if (idx == dataset_size) idx = 0;
    }

    kv_store_quiescent(store, 0);
    kv_store_destroy(store);

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
}

BENCHMARK(BM_SharedKvSingleSet)->Apply(add_all_args)->UseRealTime();
BENCHMARK(BM_SharedKvSingleGetHit)->Apply(add_all_args)->UseRealTime();

} // namespace

BENCHMARK_MAIN();
