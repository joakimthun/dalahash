#pragma once

#include <cstdint>

struct KvStore;

struct KvStoreInternalStats {
    uint64_t set_calls;
    uint64_t set_overwrite_same_size;
    uint64_t set_overwrite_size_change;
    uint64_t set_inserts;
    uint64_t set_evictions;
    uint64_t set_cas_failures;
    uint64_t set_allocations;
    uint64_t quiescent_calls;
    uint64_t quiescent_fast_returns;
    uint64_t retire_batches_enqueued;
    uint64_t retired_nodes_enqueued;
    uint64_t maintenance_runs;
    uint64_t maintenance_nodes_freed;
    uint64_t maintenance_batches_freed;
};

// Returns true when the active implementation exposes internal counters.
bool kv_store_internal_stats_snapshot(const KvStore* store, KvStoreInternalStats* out);

// Resets internal counters when supported by the active implementation.
bool kv_store_internal_stats_reset(KvStore* store);
