#pragma once

#include <cstdint>
#include <string_view>

// Opaque shared key/value store instance.
//
// Threading model:
//   - One KvStore is intended to be shared by many worker threads.
//   - Concurrent GET/SET are supported without external locks.
//   - Each calling thread must use a stable, registered worker_id.
//   - Deferred memory reclamation requires periodic kv_store_quiescent() calls
//     from participating workers.
struct KvStore;

enum class KvGetStatus : uint8_t {
    // Key not found, expired, or invalid input (e.g. null out pointer).
    MISS = 0,
    // Key found and KvValueView was populated.
    HIT = 1,
};

enum class KvDeleteStatus : uint8_t {
    // Key found and removed.
    OK = 0,
    // Key not found or already expired.
    NOT_FOUND = 1,
    // Invalid API usage (e.g. null store pointer, unregistered worker).
    INVALID = 2,
};

enum class KvSetStatus : uint8_t {
    // Insert/overwrite completed.
    OK = 0,
    // Allocation or placement/eviction could not satisfy the write.
    OOM = 1,
    // Invalid API usage (e.g. null store pointer).
    INVALID = 2,
};

enum class KvExpireMode : uint8_t {
    // Key does not expire.
    NONE = 0,
    // Expiration timestamp is computed as now_ms + value_ms.
    AFTER_MS = 1,
    // value_ms is interpreted as absolute unix epoch milliseconds.
    AT_MS = 2,
};

struct KvSetOptions {
    // Expiration policy applied to this write.
    KvExpireMode mode;
    // TTL or absolute timestamp in milliseconds, depending on mode.
    uint64_t value_ms;
};

struct KvValueView {
    // Pointer into internal store memory; caller must treat this as read-only.
    // Lifetime is short-lived and intended for immediate response formatting.
    const uint8_t* data;
    // Value size in bytes.
    uint32_t len;
};

struct KvStoreConfig {
    // Target capacity for active key/value node memory.
    // 0 => implementation default.
    uint64_t capacity_bytes;
    // Number of independent shards (rounded to power-of-two).
    // 0 => implementation chooses based on worker_count.
    uint32_t shard_count;
    // Buckets per shard (rounded to power-of-two).
    // 0 => implementation chooses based on capacity.
    uint32_t buckets_per_shard;
    // Target upper bound for slot-resident item count.
    // 0 => disabled.
    //
    // Applied only when buckets_per_shard == 0.
    // The implementation keeps shard_count selection unchanged and derives
    // buckets_per_shard to satisfy this target as closely as possible.
    uint64_t max_items = 0;
    // Expected number of worker threads calling get/set/quiescent.
    // 0 => treated as 1.
    uint32_t worker_count;
};

// Create a shared store instance.
//
// Thread-safety:
//   - Must not race with kv_store_destroy() on the same pointer.
//   - Returned object supports concurrent get/set from multiple threads.
//
// Returns nullptr on allocation/configuration failure.
KvStore* kv_store_create(const KvStoreConfig* config);

// Destroy a store and free all associated memory.
//
// Thread-safety:
//   - Caller must guarantee no concurrent get/set/quiescent/register calls.
//   - This is a terminal operation for the instance.
void kv_store_destroy(KvStore* store);

// Register a worker id for quiescent reclamation participation.
//
// Thread-safety:
//   - Safe to call concurrently for different worker ids.
//   - Re-registering the same worker id is allowed.
//
// Contract:
//   - worker_id must be registered before using kv_store_get/kv_store_set/
//     kv_store_quiescent from that worker.
//
// Returns 0 on success, -EINVAL on invalid input/range.
int kv_store_register_worker(KvStore* store, uint32_t worker_id);

// Publish a worker quiescent point and perform deferred reclamation work.
//
// Intended usage:
//   - Call periodically from each worker event loop.
//   - Higher call frequency generally reduces retained retired memory.
//
// Thread-safety:
//   - Safe concurrently with get/set/quiescent from other workers.
void kv_store_quiescent(KvStore* store, uint32_t worker_id);

// Lookup a key.
//
// Parameters:
//   - worker_id: worker index used for touch sampling and retirement ownership.
//                Must be registered via kv_store_register_worker.
//   - now_ms: caller-provided current time in milliseconds (used for expiration).
//   - out: output view populated on HIT and zeroed on MISS.
//
// Thread-safety:
//   - Safe for concurrent calls from many threads when each thread uses a
//     stable registered worker_id.
//
// Expiration behavior:
//   - Lazy: expired keys are removed on access path when encountered.
//
// Invalid worker_id behavior:
//   - Out-of-range or unregistered worker ids return MISS.
KvGetStatus kv_store_get(KvStore* store, uint32_t worker_id, std::string_view key, uint64_t now_ms,
                         KvValueView* out);

// Insert or overwrite a key/value pair.
//
// Parameters:
//   - worker_id: worker index used for retirement ownership and trim cursor.
//                Must be registered via kv_store_register_worker.
//   - now_ms: caller-provided current time in milliseconds (used for expiration).
//   - opts: optional expiration options; nullptr means no expiration.
//
// Thread-safety:
//   - Safe for concurrent calls from many threads when each thread uses a
//     stable registered worker_id.
//
// Capacity behavior:
//   - Store attempts to trim via approximate LRU-style eviction when over target.
//   - OOM means write could not be admitted under current conditions.
//   - INVALID is returned for out-of-range or unregistered worker ids.
KvSetStatus kv_store_set(KvStore* store, uint32_t worker_id, std::string_view key, std::string_view value,
                         uint64_t now_ms, const KvSetOptions* opts);

// Delete a key.
//
// Parameters:
//   - worker_id: worker index used for retirement ownership.
//                Must be registered via kv_store_register_worker.
//   - now_ms: caller-provided current time in milliseconds (used for expiration check).
//
// Thread-safety:
//   - Safe for concurrent calls from many threads when each thread uses a
//     stable registered worker_id.
KvDeleteStatus kv_store_delete(KvStore* store, uint32_t worker_id, std::string_view key, uint64_t now_ms);

// Return current accounted bytes for active slot-resident nodes.
//
// Note: this is payload-node accounting, not full process RSS.
uint64_t kv_store_live_bytes(const KvStore* store);

// Return configured capacity target in bytes.
uint64_t kv_store_capacity_bytes(const KvStore* store);

// Helper for current wall-clock time in milliseconds (CLOCK_REALTIME).
//
// Callers may provide their own clock to kv_store_get/set instead.
uint64_t kv_time_now_ms();
