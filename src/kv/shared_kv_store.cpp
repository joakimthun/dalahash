// shared_kv_store.cpp
//
// Design summary:
//   - Sharded hash table with 2-bucket lookup and 4 slots per bucket.
//   - Slot ownership is managed via atomic Node* publication/replacement.
//   - Nodes are immutable after publication; writes replace pointers via CAS.
//   - Deferred reclamation is worker-cooperative via quiescent epochs.
//   - Memory uses size-class pools for common node sizes + large fallback path.
//
// Concurrency summary:
//   - No mutexes on hot path (GET/SET).
//   - correctness relies on acquire/release ordering on slot atomics and epoch
//     progress for safe node reclamation.

#include "shared_kv_store.h"
#include "base/assert.h"

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <new>

// Number of candidate lanes in one hash bucket.
static constexpr uint32_t KV_BUCKET_SLOTS = 4;
// Touch recency bit every 8th hit to reduce cacheline write traffic on hot keys.
static constexpr uint32_t KV_TOUCH_SAMPLE_MASK = 0x7;
// Pool refill size per class growth.
static constexpr uint32_t KV_GROW_BATCH = 64;
static constexpr uint8_t KV_CLASS_LARGE = 0xFF;
static constexpr uint32_t KV_CLASS_COUNT = 10;
// Size classes for Node allocations (bytes). Larger nodes use malloc/free.
static constexpr uint32_t KV_CLASS_SIZES[KV_CLASS_COUNT] = {
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768
};

// Immutable published object referenced by slot atomics.
//
// After a Node* is installed into a slot, key/value fields are never mutated.
// Updates replace the slot pointer with a newly allocated Node.
struct Node {
    uint64_t hash;
    uint64_t expire_at_ms;
    uint64_t retire_seq;
    uint32_t key_len;
    uint32_t value_len;
    uint32_t alloc_size;
    uint8_t class_idx;
    uint8_t reserved0;
    uint8_t reserved1;
    uint8_t reserved2;
    Node *gc_next;
    std::atomic<uint8_t> refbit;
    // Packed payload: [key bytes][value bytes].
    uint8_t bytes[1];
};

// Intrusive free-list entry reused from object memory.
struct FreeChunk {
    FreeChunk *next;
};

// Tracks one bulk allocation backing a class pool.
struct PoolBlock {
    PoolBlock *next;
    void *mem;
};

// One lock-free size class allocator.
//
// alignas(64) keeps hot heads from sharing cache lines with neighbors.
struct alignas(64) ClassPool {
    uint32_t obj_size;
    uint32_t reserved0;
    std::atomic<FreeChunk *> free_head;
    std::atomic<PoolBlock *> blocks;
};

// Per-worker local allocation cache for one size class.
static constexpr uint32_t LOCAL_CACHE_MAX = 32;
static constexpr uint32_t LOCAL_CACHE_BATCH = 16;

struct LocalClassCache {
    FreeChunk *head;
    uint32_t count;
};

// Per-worker cooperative reclamation state.
//
// Each worker owns its retired list to avoid shared-list contention.
struct alignas(64) WorkerState {
    std::atomic<uint64_t> quiescent_seq;
    std::atomic<uint8_t> registered;
    uint8_t reserved0;
    uint8_t reserved1;
    uint8_t reserved2;
    uint32_t touch_counter;
    uint32_t shard_cursor;
    uint32_t quiescent_counter;
    // Accumulated live_bytes delta since last flush to global.
    // Positive means net growth, negative means net shrinkage.
    int64_t local_live_delta;
    Node *retired_head;
    LocalClassCache local_caches[KV_CLASS_COUNT];
};

// Shard-local hash table metadata and slots.
struct alignas(64) Shard {
    uint32_t bucket_count;
    uint32_t bucket_mask;
    std::atomic<uint32_t> clock_hand;
    // Flat slot array sized bucket_count * KV_BUCKET_SLOTS.
    std::atomic<Node *> *slots;
};

// Top-level shared store instance.
struct KvStore {
    uint64_t capacity_bytes;
    std::atomic<uint64_t> live_bytes;
    uint32_t shard_count;
    uint32_t shard_mask;
    Shard *shards;
    uint32_t worker_count;
    WorkerState *workers;
    std::atomic<uint64_t> global_epoch;
    // Cached min epoch to avoid scanning all workers every quiescent call.
    std::atomic<uint64_t> cached_min_epoch;
    ClassPool pools[KV_CLASS_COUNT];
};

// Round up to next power-of-two (min 1).
static uint32_t next_pow2_u32(uint32_t v) {
    if (v <= 1) return 1;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    uint32_t out = v + 1;
    ASSERT(out != 0, "next_pow2_u32 overflowed");
    ASSERT((out & (out - 1u)) == 0, "next_pow2_u32 result is not power of two");
    return out;
}

static uint64_t hash_bytes(std::string_view v) {
    // Bulk multiply-xorshift hash: processes 8 bytes per iteration.
    const uint8_t *p = reinterpret_cast<const uint8_t *>(v.data());
    size_t len = v.size();
    uint64_t h = 0x9e3779b97f4a7c15ull ^ (static_cast<uint64_t>(len) * 0xff51afd7ed558ccdull);

    // 8-byte chunks
    while (len >= 8) {
        uint64_t k;
        std::memcpy(&k, p, 8);
        k *= 0xbf58476d1ce4e5b9ull;
        k ^= k >> 31;
        h ^= k;
        h *= 0x94d049bb133111ebull;
        p += 8;
        len -= 8;
    }

    // Tail bytes
    if (len > 0) {
        uint64_t k = 0;
        std::memcpy(&k, p, len);
        k *= 0xbf58476d1ce4e5b9ull;
        k ^= k >> 31;
        h ^= k;
        h *= 0x94d049bb133111ebull;
    }

    // Final avalanche
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdull;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ull;
    h ^= h >> 33;
    return h;
}

// Shard selection uses high hash bits to decorrelate from low-bit bucket mask.
static uint32_t select_shard(uint64_t hash, uint32_t shard_mask) {
    ASSERT((shard_mask & (shard_mask + 1u)) == 0, "shard_mask must be 2^n-1");
    return static_cast<uint32_t>((hash >> 32u) & static_cast<uint64_t>(shard_mask));
}

// Second candidate bucket for two-choice placement/lookup.
static uint32_t secondary_bucket(uint64_t hash, uint32_t bucket_mask, uint32_t b1) {
    ASSERT((bucket_mask & (bucket_mask + 1u)) == 0, "bucket_mask must be 2^n-1");
    ASSERT((b1 & ~bucket_mask) == 0, "primary bucket index out of range");
    uint64_t mixed = hash ^ (hash >> 19u) ^ (hash << 11u) ^ 0x9e3779b97f4a7c15ull;
    uint32_t b2 = static_cast<uint32_t>(mixed) & bucket_mask;
    if (b2 == b1) b2 = (b1 + 1u) & bucket_mask;
    return b2;
}

// Return pointer to one lane in flat slot array.
static inline std::atomic<Node *> *slot_ptr(Shard *shard, uint32_t bucket_idx, uint32_t lane) {
    ASSERT(shard != nullptr, "slot_ptr requires shard");
    ASSERT(bucket_idx < shard->bucket_count, "bucket index out of range");
    ASSERT(lane < KV_BUCKET_SLOTS, "bucket lane out of range");
    return &shard->slots[static_cast<size_t>(bucket_idx) * KV_BUCKET_SLOTS + lane];
}

// Payload slicing helpers.
static inline const uint8_t *node_key_ptr(const Node *n) {
    ASSERT(n != nullptr, "node_key_ptr requires node");
    return n->bytes;
}

static inline const uint8_t *node_value_ptr(const Node *n) {
    ASSERT(n != nullptr, "node_value_ptr requires node");
    return n->bytes + n->key_len;
}

// Binary-safe key equality against packed node key.
static bool node_key_equals(const Node *n, std::string_view key) {
    ASSERT(n != nullptr, "node_key_equals requires node");
    if (n->key_len != key.size()) return false;
    if (n->key_len == 0) return true;
    return std::memcmp(node_key_ptr(n), key.data(), key.size()) == 0;
}

// Expiration predicate; 0 means immortal.
static bool node_is_expired(const Node *n, uint64_t now_ms) {
    ASSERT(n != nullptr, "node_is_expired requires node");
    return n->expire_at_ms != 0 && now_ms >= n->expire_at_ms;
}

// Normalize expiration options to absolute epoch-ms.
static uint64_t compute_expire_at(uint64_t now_ms, const KvSetOptions *opts) {
    if (!opts) return 0;
    if (opts->mode == KvExpireMode::NONE) return 0;
    if (opts->mode == KvExpireMode::AT_MS) return opts->value_ms;
    if (opts->mode == KvExpireMode::AFTER_MS) {
        uint64_t ttl = opts->value_ms;
        if (ttl > UINT64_MAX - now_ms) return UINT64_MAX;
        return now_ms + ttl;
    }
    return 0;
}

// Resolve worker state for worker-id range checks.
static WorkerState *worker_state_in_range(KvStore *store, uint32_t worker_id) {
    if (!store || store->worker_count == 0) return nullptr;
    ASSERT(store->workers != nullptr, "store workers array must be initialized");
    if (worker_id >= store->worker_count) return nullptr;
    return &store->workers[worker_id];
}

// Resolve worker state only for registered worker ids.
static WorkerState *worker_state_registered(KvStore *store, uint32_t worker_id) {
    WorkerState *ws = worker_state_in_range(store, worker_id);
    if (!ws) return nullptr;
    if (!ws->registered.load(std::memory_order_acquire)) return nullptr;
    return ws;
}

// Select smallest class that can hold size; -1 => large path.
static int select_class(uint32_t size) {
    ASSERT(size > 0, "select_class requires non-zero size");
    for (uint32_t i = 0; i < KV_CLASS_COUNT; i++) {
        if (size <= KV_CLASS_SIZES[i]) return static_cast<int>(i);
    }
    return -1;
}

static bool pool_grow(ClassPool *pool) {
    ASSERT(pool != nullptr, "pool_grow requires pool");
    ASSERT(pool->obj_size > 0, "pool obj_size must be non-zero");
    // Allocate one contiguous backing block for this class.
    size_t bytes = static_cast<size_t>(pool->obj_size) * KV_GROW_BATCH;
    void *mem = std::malloc(bytes);
    if (!mem) return false;

    // Metadata node used to free this backing block at store destroy.
    auto *block = static_cast<PoolBlock *>(std::malloc(sizeof(PoolBlock)));
    if (!block) {
        std::free(mem);
        return false;
    }
    block->mem = mem;

    auto *first = static_cast<FreeChunk *>(mem);
    FreeChunk *cur = first;
    for (uint32_t i = 1; i < KV_GROW_BATCH; i++) {
        auto *next = reinterpret_cast<FreeChunk *>(static_cast<uint8_t *>(mem) + static_cast<size_t>(i) * pool->obj_size);
        cur->next = next;
        cur = next;
    }
    cur->next = nullptr;

    // Publish block ownership entry.
    PoolBlock *old_blocks = pool->blocks.load(std::memory_order_relaxed);
    do {
        block->next = old_blocks;
    } while (!pool->blocks.compare_exchange_weak(old_blocks, block,
                                                 std::memory_order_release,
                                                 std::memory_order_relaxed));

    // Publish all new free objects as one chain onto free_head.
    FreeChunk *head = pool->free_head.load(std::memory_order_relaxed);
    do {
        cur->next = head;
    } while (!pool->free_head.compare_exchange_weak(head, first,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));
    return true;
}

// Batch-refill local cache from global Treiber stack.
static bool local_cache_refill(ClassPool *pool, LocalClassCache *cache) {
    for (uint32_t filled = 0; filled < LOCAL_CACHE_BATCH;) {
        FreeChunk *head = pool->free_head.load(std::memory_order_acquire);
        if (!head) {
            if (!pool_grow(pool)) return filled > 0;
            continue;
        }
        // Try to pop one from global.
        FreeChunk *next = head->next;
        if (pool->free_head.compare_exchange_weak(head, next,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            head->next = cache->head;
            cache->head = head;
            cache->count++;
            filled++;
        }
    }
    return true;
}

// Batch-drain local cache back to global Treiber stack.
static void local_cache_drain(ClassPool *pool, LocalClassCache *cache) {
    uint32_t to_drain = LOCAL_CACHE_BATCH;
    if (to_drain > cache->count) to_drain = cache->count;

    FreeChunk *drain_head = cache->head;
    FreeChunk *drain_tail = drain_head;
    for (uint32_t i = 1; i < to_drain; i++) {
        drain_tail = drain_tail->next;
    }

    // Detach drained portion from local list.
    cache->head = drain_tail->next;
    cache->count -= to_drain;

    // Push drained chain onto global free list.
    FreeChunk *global_head = pool->free_head.load(std::memory_order_relaxed);
    do {
        drain_tail->next = global_head;
    } while (!pool->free_head.compare_exchange_weak(global_head, drain_head,
                                                     std::memory_order_release,
                                                     std::memory_order_relaxed));
}

static Node *node_alloc(KvStore *store, uint32_t worker_id, uint64_t hash,
                        std::string_view key, std::string_view value,
                        uint64_t expire_at_ms) {
    if (!store) return nullptr;

    // Node payload is key bytes + value bytes.
    size_t payload = key.size() + value.size();
    if (payload > UINT32_MAX) return nullptr;

    size_t raw_size = offsetof(Node, bytes) + payload;
    if (raw_size > UINT32_MAX) return nullptr;

    int class_idx = select_class(static_cast<uint32_t>(raw_size));
    uint32_t alloc_size = 0;
    void *mem = nullptr;
    uint8_t stored_class = KV_CLASS_LARGE;

    if (class_idx >= 0) {
        ASSERT(class_idx < static_cast<int>(KV_CLASS_COUNT), "class index out of range");
        ClassPool *pool = &store->pools[class_idx];
        alloc_size = pool->obj_size;

        // Fast path: pop from per-worker local cache.
        WorkerState *ws = worker_state_in_range(store, worker_id);
        if (ws && ws->registered.load(std::memory_order_relaxed)) {
            LocalClassCache *cache = &ws->local_caches[class_idx];
            if (!cache->head) {
                if (!local_cache_refill(pool, cache)) return nullptr;
            }
            FreeChunk *chunk = cache->head;
            cache->head = chunk->next;
            cache->count--;
            mem = chunk;
        } else {
            // Fallback: pop directly from global (e.g. during destroy).
            for (;;) {
                FreeChunk *head = pool->free_head.load(std::memory_order_acquire);
                if (!head) {
                    if (!pool_grow(pool)) return nullptr;
                    continue;
                }
                FreeChunk *next = head->next;
                if (pool->free_head.compare_exchange_weak(head, next,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_acquire)) {
                    mem = head;
                    break;
                }
            }
        }
        stored_class = static_cast<uint8_t>(class_idx);
        ASSERT(alloc_size >= raw_size, "class alloc_size smaller than requested raw size");
    } else {
        // Rare large object path bypasses pools.
        alloc_size = static_cast<uint32_t>(raw_size);
        mem = std::malloc(raw_size);
        if (!mem) return nullptr;
        stored_class = KV_CLASS_LARGE;
    }

    // Fully initialize node before any slot publication.
    auto *node = reinterpret_cast<Node *>(mem);
    node->hash = hash;
    node->expire_at_ms = expire_at_ms;
    node->retire_seq = 0;
    node->key_len = static_cast<uint32_t>(key.size());
    node->value_len = static_cast<uint32_t>(value.size());
    node->alloc_size = alloc_size;
    node->class_idx = stored_class;
    node->reserved0 = 0;
    node->reserved1 = 0;
    node->reserved2 = 0;
    node->gc_next = nullptr;
    node->refbit.store(1, std::memory_order_relaxed);
    ASSERT(node->class_idx == KV_CLASS_LARGE || node->class_idx < KV_CLASS_COUNT,
           "stored node class index out of range");
    ASSERT(node->alloc_size >= raw_size, "node alloc_size smaller than raw size");

    if (!key.empty())
        std::memcpy(node->bytes, key.data(), key.size());
    if (!value.empty())
        std::memcpy(node->bytes + key.size(), value.data(), value.size());
    return node;
}

static void node_free(KvStore *store, uint32_t worker_id, Node *node) {
    if (!store || !node) return;
    if (node->class_idx == KV_CLASS_LARGE) {
        std::free(node);
        return;
    }
    uint8_t class_idx = node->class_idx;
    ASSERT(class_idx == KV_CLASS_LARGE || class_idx < KV_CLASS_COUNT,
           "node class index out of range");
    if (class_idx >= KV_CLASS_COUNT) {
        std::free(node);
        return;
    }
    ClassPool *pool = &store->pools[class_idx];

    // Fast path: push to per-worker local cache.
    WorkerState *ws = worker_state_in_range(store, worker_id);
    if (ws && ws->registered.load(std::memory_order_relaxed)) {
        LocalClassCache *cache = &ws->local_caches[class_idx];
        auto *chunk = reinterpret_cast<FreeChunk *>(node);
        chunk->next = cache->head;
        cache->head = chunk;
        cache->count++;
        if (cache->count > LOCAL_CACHE_MAX) {
            local_cache_drain(pool, cache);
        }
        return;
    }

    // Fallback: push directly to global (e.g. during destroy).
    auto *chunk = reinterpret_cast<FreeChunk *>(node);
    FreeChunk *head = pool->free_head.load(std::memory_order_relaxed);
    do {
        chunk->next = head;
    } while (!pool->free_head.compare_exchange_weak(head, chunk,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));
}

static void retire_node(KvStore *store, uint32_t worker_id, Node *node) {
    if (!store || !node) return;
    WorkerState *ws = worker_state_registered(store, worker_id);
    ASSERT(ws != nullptr, "retire_node requires registered worker");
    if (!ws) return;
    // Stamp with current epoch (read-only); epoch is bumped in kv_store_quiescent.
    node->retire_seq = store->global_epoch.load(std::memory_order_acquire);
    // Worker-local retired list avoids global lock contention.
    node->gc_next = ws->retired_head;
    ws->retired_head = node;
}

static bool evict_one_from_shard(KvStore *store, uint32_t worker_id,
                                 WorkerState *ws, Shard *shard, uint64_t now_ms) {
    ASSERT(store != nullptr, "evict_one_from_shard requires store");
    ASSERT(ws != nullptr, "evict_one_from_shard requires worker state");
    ASSERT(shard != nullptr, "evict_one_from_shard requires shard");
    ASSERT(shard->slots != nullptr, "shard slots must be initialized");
    ASSERT(shard->bucket_count > 0, "bucket_count must be non-zero");
    ASSERT((shard->bucket_count & (shard->bucket_count - 1u)) == 0,
           "bucket_count must be power of two");
    ASSERT(shard->bucket_mask == shard->bucket_count - 1u,
           "bucket_mask must equal bucket_count-1");
    // Start near previous position to spread eviction cost over table.
    uint32_t start = shard->clock_hand.fetch_add(1, std::memory_order_relaxed);
    uint32_t max_scan = shard->bucket_count;
    if (max_scan > 64) max_scan = 64;

    for (uint32_t step = 0; step < max_scan; step++) {
        uint32_t bucket = (start + step) & shard->bucket_mask;
        for (uint32_t lane = 0; lane < KV_BUCKET_SLOTS; lane++) {
            std::atomic<Node *> *slot = slot_ptr(shard, bucket, lane);
            Node *node = slot->load(std::memory_order_acquire);
            if (!node) continue;

            bool expired = node_is_expired(node, now_ms);
            bool cold = node->refbit.load(std::memory_order_relaxed) == 0;
            if (!expired && !cold) {
                // Second-chance: clear refbit and skip this round.
                node->refbit.store(0, std::memory_order_relaxed);
                continue;
            }

            Node *expected = node;
            // CAS-to-null unpublishes victim. If we lose the CAS race, retry scan.
            if (!slot->compare_exchange_strong(expected, nullptr,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
                continue;
            }

            ws->local_live_delta -= static_cast<int64_t>(node->alloc_size);
            retire_node(store, worker_id, node);
            return true;
        }
    }
    return false;
}

// Flush per-worker live_bytes delta to the global atomic.
static void flush_live_delta(KvStore *store, WorkerState *ws) {
    int64_t delta = ws->local_live_delta;
    if (delta == 0) return;
    if (delta > 0) {
        store->live_bytes.fetch_add(static_cast<uint64_t>(delta), std::memory_order_relaxed);
    } else {
        store->live_bytes.fetch_sub(static_cast<uint64_t>(-delta), std::memory_order_relaxed);
    }
    ws->local_live_delta = 0;
}

static bool trim_to_capacity(KvStore *store, uint32_t worker_id,
                              WorkerState *ws, uint64_t now_ms) {
    ASSERT(store != nullptr, "trim_to_capacity requires store");
    ASSERT(ws != nullptr, "trim_to_capacity requires worker state");
    ASSERT(store->shard_count > 0, "shard_count must be non-zero");
    ASSERT((store->shard_count & (store->shard_count - 1u)) == 0,
           "shard_count must be power of two");
    ASSERT(store->shard_mask == store->shard_count - 1u,
           "shard_mask must equal shard_count-1");
    // Flush local delta so global view is current for this worker.
    flush_live_delta(store, ws);
    uint64_t live = store->live_bytes.load(std::memory_order_acquire);
    if (live <= store->capacity_bytes) return true;

    uint32_t cursor = ws->shard_cursor;

    // Best-effort convergence loop; bounded to avoid pathological long stalls.
    uint32_t rounds = 0;
    while (live > store->capacity_bytes) {
        bool evicted = false;
        for (uint32_t off = 0; off < store->shard_count; off++) {
            uint32_t shard_idx = (cursor + off) & store->shard_mask;
            if (evict_one_from_shard(store, worker_id, ws, &store->shards[shard_idx], now_ms)) {
                evicted = true;
                cursor = (shard_idx + 1) & store->shard_mask;
                break;
            }
        }
        if (!evicted) return false;
        // Eviction delta accumulated locally; flush for accurate check.
        flush_live_delta(store, ws);
        live = store->live_bytes.load(std::memory_order_acquire);
        rounds++;
        if (rounds > store->shard_count * 8u) break;
    }

    ws->shard_cursor = cursor;
    return live <= store->capacity_bytes;
}

KvStore *kv_store_create(const KvStoreConfig *config) {
    if (!config) return nullptr;

    // Normalize config with safe defaults and power-of-two geometry.
    uint32_t worker_count = config->worker_count == 0 ? 1 : config->worker_count;
    uint64_t capacity = config->capacity_bytes == 0 ? (256ull << 20) : config->capacity_bytes;
    ASSERT(worker_count > 0, "worker_count must be non-zero");
    ASSERT(capacity > 0, "capacity must be non-zero");

    uint32_t shard_count = config->shard_count;
    if (shard_count == 0) {
        uint32_t target = worker_count * 4u;
        if (target < 64) target = 64;
        shard_count = next_pow2_u32(target);
    } else {
        shard_count = next_pow2_u32(shard_count);
    }
    ASSERT(shard_count > 0, "shard_count must be non-zero");
    ASSERT((shard_count & (shard_count - 1u)) == 0, "shard_count must be power of two");

    uint32_t buckets_per_shard = config->buckets_per_shard;
    if (buckets_per_shard == 0) {
        uint64_t total_buckets = capacity / 256u;
        if (total_buckets < 64) total_buckets = 64;
        uint64_t per_shard = total_buckets / shard_count;
        if (per_shard < 16) per_shard = 16;
        if (per_shard > UINT32_MAX) per_shard = UINT32_MAX;
        buckets_per_shard = next_pow2_u32(static_cast<uint32_t>(per_shard));
    } else {
        buckets_per_shard = next_pow2_u32(buckets_per_shard);
    }
    ASSERT(buckets_per_shard > 0, "buckets_per_shard must be non-zero");
    ASSERT((buckets_per_shard & (buckets_per_shard - 1u)) == 0,
           "buckets_per_shard must be power of two");

    auto *store = new (std::nothrow) KvStore();
    if (!store) return nullptr;

    // Initialize global fields.
    store->capacity_bytes = capacity;
    store->live_bytes.store(0, std::memory_order_relaxed);
    store->shard_count = shard_count;
    store->shard_mask = shard_count - 1;
    store->shards = nullptr;
    store->worker_count = worker_count;
    store->workers = nullptr;
    store->global_epoch.store(1, std::memory_order_relaxed);
    store->cached_min_epoch.store(1, std::memory_order_relaxed);

    for (uint32_t i = 0; i < KV_CLASS_COUNT; i++) {
        store->pools[i].obj_size = KV_CLASS_SIZES[i];
        store->pools[i].reserved0 = 0;
        store->pools[i].free_head.store(nullptr, std::memory_order_relaxed);
        store->pools[i].blocks.store(nullptr, std::memory_order_relaxed);
    }

    store->workers = new (std::nothrow) WorkerState[worker_count];
    if (!store->workers) {
        delete store;
        return nullptr;
    }
    for (uint32_t i = 0; i < worker_count; i++) {
        // Start at sequence 1 (global_epoch starts at 1 as well).
        store->workers[i].quiescent_seq.store(1, std::memory_order_relaxed);
        store->workers[i].registered.store(0, std::memory_order_relaxed);
        store->workers[i].reserved0 = 0;
        store->workers[i].reserved1 = 0;
        store->workers[i].reserved2 = 0;
        store->workers[i].touch_counter = 0;
        store->workers[i].shard_cursor = i & store->shard_mask;
        store->workers[i].quiescent_counter = 0;
        store->workers[i].local_live_delta = 0;
        store->workers[i].retired_head = nullptr;
        for (uint32_t c = 0; c < KV_CLASS_COUNT; c++) {
            store->workers[i].local_caches[c].head = nullptr;
            store->workers[i].local_caches[c].count = 0;
        }
    }

    store->shards = new (std::nothrow) Shard[shard_count];
    if (!store->shards) {
        delete[] store->workers;
        delete store;
        return nullptr;
    }

    bool ok = true;
    for (uint32_t s = 0; s < shard_count; s++) {
        Shard *shard = &store->shards[s];
        shard->bucket_count = buckets_per_shard;
        shard->bucket_mask = buckets_per_shard - 1;
        shard->clock_hand.store(0, std::memory_order_relaxed);
        ASSERT(shard->bucket_count > 0, "bucket_count must be non-zero");
        ASSERT((shard->bucket_count & (shard->bucket_count - 1u)) == 0,
               "bucket_count must be power of two");
        ASSERT(shard->bucket_mask == shard->bucket_count - 1u,
               "bucket_mask must equal bucket_count-1");
        size_t slot_count = static_cast<size_t>(buckets_per_shard) * KV_BUCKET_SLOTS;
        shard->slots = static_cast<std::atomic<Node *> *>(
            std::malloc(sizeof(std::atomic<Node *>) * slot_count));
        if (!shard->slots) {
            ok = false;
            break;
        }
        // Placement-new each atomic slot to nullptr.
        for (size_t i = 0; i < slot_count; i++) {
            new (&shard->slots[i]) std::atomic<Node *>(nullptr);
        }
    }

    if (!ok) {
        // Roll back any partially initialized shard slots.
        for (uint32_t s = 0; s < shard_count; s++) {
            if (!store->shards[s].slots) continue;
            std::free(store->shards[s].slots);
            store->shards[s].slots = nullptr;
        }
        delete[] store->shards;
        delete[] store->workers;
        delete store;
        return nullptr;
    }

    return store;
}

void kv_store_destroy(KvStore *store) {
    if (!store) return;
    ASSERT(store->workers != nullptr, "store workers must be initialized");
    ASSERT(store->shards != nullptr, "store shards must be initialized");

    // Destroy requires external synchronization; no concurrent readers/writers.

    // Flush any remaining live_bytes deltas.
    for (uint32_t w = 0; w < store->worker_count; w++) {
        flush_live_delta(store, &store->workers[w]);
    }

    // Drain all per-worker local caches back to global pools first.
    for (uint32_t w = 0; w < store->worker_count; w++) {
        for (uint32_t c = 0; c < KV_CLASS_COUNT; c++) {
            LocalClassCache *cache = &store->workers[w].local_caches[c];
            while (cache->head) {
                FreeChunk *chunk = cache->head;
                cache->head = chunk->next;
                cache->count--;
                // Push directly to global free list.
                ClassPool *pool = &store->pools[c];
                FreeChunk *head = pool->free_head.load(std::memory_order_relaxed);
                do {
                    chunk->next = head;
                } while (!pool->free_head.compare_exchange_weak(head, chunk,
                                                                 std::memory_order_release,
                                                                 std::memory_order_relaxed));
            }
        }
    }

    // Use UINT32_MAX as worker_id to hit global fallback path in node_free.
    static constexpr uint32_t DESTROY_WORKER = UINT32_MAX;

    for (uint32_t s = 0; s < store->shard_count; s++) {
        Shard *shard = &store->shards[s];
        size_t slot_count = static_cast<size_t>(shard->bucket_count) * KV_BUCKET_SLOTS;
        for (size_t i = 0; i < slot_count; i++) {
            Node *node = shard->slots[i].load(std::memory_order_relaxed);
            if (node) node_free(store, DESTROY_WORKER, node);
        }
        std::free(shard->slots);
        shard->slots = nullptr;
    }

    // Flush retired lists that may still hold deferred nodes.
    for (uint32_t i = 0; i < store->worker_count; i++) {
        Node *n = store->workers[i].retired_head;
        while (n) {
            Node *next = n->gc_next;
            node_free(store, DESTROY_WORKER, n);
            n = next;
        }
        store->workers[i].retired_head = nullptr;
    }

    for (uint32_t i = 0; i < KV_CLASS_COUNT; i++) {
        // Free all backing blocks owned by each pool class.
        PoolBlock *block = store->pools[i].blocks.exchange(nullptr, std::memory_order_acq_rel);
        while (block) {
            PoolBlock *next = block->next;
            std::free(block->mem);
            std::free(block);
            block = next;
        }
        store->pools[i].free_head.store(nullptr, std::memory_order_relaxed);
    }

    delete[] store->shards;
    delete[] store->workers;
    delete store;
}

int kv_store_register_worker(KvStore *store, uint32_t worker_id) {
    if (!store) return -EINVAL;
    if (worker_id >= store->worker_count) return -EINVAL;
    WorkerState *ws = worker_state_in_range(store, worker_id);
    if (!ws) return -EINVAL;
    // Publish registration and current observed sequence frontier.
    ws->registered.store(1, std::memory_order_release);
    ws->quiescent_seq.store(store->global_epoch.load(std::memory_order_acquire),
                            std::memory_order_release);
    return 0;
}

void kv_store_quiescent(KvStore *store, uint32_t worker_id) {
    if (!store) return;
    WorkerState *ws = worker_state_registered(store, worker_id);
    if (!ws) return;

    // Flush accumulated live_bytes delta to global.
    flush_live_delta(store, ws);

    // Advance global epoch; this is the only write site.
    uint64_t cur = store->global_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
    ws->quiescent_seq.store(cur, std::memory_order_release);

    // Full worker scan every 16th call; use cached value otherwise.
    // Stale min_epoch is safe: only delays reclamation, never causes premature free.
    ws->quiescent_counter++;
    uint64_t min_seq;
    if ((ws->quiescent_counter & 0xF) == 0) {
        min_seq = cur;
        for (uint32_t i = 0; i < store->worker_count; i++) {
            if (!store->workers[i].registered.load(std::memory_order_acquire)) continue;
            uint64_t q = store->workers[i].quiescent_seq.load(std::memory_order_acquire);
            if (q < min_seq) min_seq = q;
        }
        store->cached_min_epoch.store(min_seq, std::memory_order_release);
    } else {
        min_seq = store->cached_min_epoch.load(std::memory_order_acquire);
    }

    Node *kept = nullptr;
    Node *node = ws->retired_head;
    while (node) {
        Node *next = node->gc_next;
        if (node->retire_seq < min_seq) {
            // Safe to reclaim: every registered worker has passed this epoch.
            node_free(store, worker_id, node);
        } else {
            // Keep for a future quiescent pass.
            node->gc_next = kept;
            kept = node;
        }
        node = next;
    }
    ws->retired_head = kept;
}

KvGetStatus kv_store_get(KvStore *store, uint32_t worker_id, std::string_view key,
                         uint64_t now_ms, KvValueView *out) {
    if (!store || !out) return KvGetStatus::MISS;
    ASSERT(store->shards != nullptr, "store shards must be initialized");
    ASSERT(store->shard_count > 0, "shard_count must be non-zero");
    ASSERT(store->shard_mask == store->shard_count - 1u,
           "shard_mask must equal shard_count-1");

    out->data = nullptr;
    out->len = 0;
    WorkerState *ws = worker_state_registered(store, worker_id);
    if (!ws) return KvGetStatus::MISS;

    uint64_t hash = hash_bytes(key);
    uint32_t shard_idx = select_shard(hash, store->shard_mask);
    Shard *shard = &store->shards[shard_idx];
    uint32_t b1 = static_cast<uint32_t>(hash) & shard->bucket_mask;
    uint32_t b2 = secondary_bucket(hash, shard->bucket_mask, b1);

    const uint32_t buckets[2] = {b1, b2};
    for (uint32_t bi = 0; bi < 2; bi++) {
        uint32_t bucket = buckets[bi];
        for (uint32_t lane = 0; lane < KV_BUCKET_SLOTS; lane++) {
            std::atomic<Node *> *slot = slot_ptr(shard, bucket, lane);
            // Acquire load pairs with writer CAS publish.
            Node *node = slot->load(std::memory_order_acquire);
            if (!node) continue;
            if (node->hash != hash) continue;
            if (!node_key_equals(node, key)) continue;

            if (node_is_expired(node, now_ms)) {
                Node *expected = node;
                // Lazy deletion: only the CAS winner unpublishes and retires.
                if (slot->compare_exchange_strong(expected, nullptr,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                    ws->local_live_delta -= static_cast<int64_t>(node->alloc_size);
                    retire_node(store, worker_id, node);
                }
                return KvGetStatus::MISS;
            }

            ws->touch_counter++;
            // Sampled refbit write lowers write-amplification on hot reads.
            if ((ws->touch_counter & KV_TOUCH_SAMPLE_MASK) == 0) {
                // Avoid cross-core cacheline ping-pong when bit is already hot.
                if (node->refbit.load(std::memory_order_relaxed) == 0)
                    node->refbit.store(1, std::memory_order_relaxed);
            }

            out->data = node_value_ptr(node);
            out->len = node->value_len;
            return KvGetStatus::HIT;
        }
    }

    return KvGetStatus::MISS;
}

KvSetStatus kv_store_set(KvStore *store, uint32_t worker_id, std::string_view key,
                         std::string_view value, uint64_t now_ms,
                         const KvSetOptions *opts) {
    if (!store) return KvSetStatus::INVALID;
    ASSERT(store->shards != nullptr, "store shards must be initialized");
    ASSERT(store->shard_count > 0, "shard_count must be non-zero");
    ASSERT(store->shard_mask == store->shard_count - 1u,
           "shard_mask must equal shard_count-1");
    WorkerState *ws = worker_state_registered(store, worker_id);
    if (!ws) return KvSetStatus::INVALID;

    uint64_t hash = hash_bytes(key);
    uint64_t expire_at = compute_expire_at(now_ms, opts);
    // Allocate upfront so successful CAS can publish immediately.
    Node *fresh = node_alloc(store, worker_id, hash, key, value, expire_at);
    if (!fresh) return KvSetStatus::OOM;

    // Fast reject values that can never fit the configured capacity.
    if (fresh->alloc_size > store->capacity_bytes) {
        node_free(store, worker_id, fresh);
        return KvSetStatus::OOM;
    }

    uint32_t shard_idx = select_shard(hash, store->shard_mask);
    Shard *shard = &store->shards[shard_idx];
    uint32_t b1 = static_cast<uint32_t>(hash) & shard->bucket_mask;
    uint32_t b2 = secondary_bucket(hash, shard->bucket_mask, b1);

    // Bounded retry loop under contention.
    for (uint32_t attempt = 0; attempt < 128; attempt++) {
        std::atomic<Node *> *found_slot = nullptr;
        Node *found_node = nullptr;
        std::atomic<Node *> *empty_slot = nullptr;
        std::atomic<Node *> *expired_slot = nullptr;
        Node *expired_node = nullptr;
        std::atomic<Node *> *cold_slot = nullptr;
        Node *cold_node = nullptr;
        std::atomic<Node *> *fallback_slot = nullptr;
        Node *fallback_node = nullptr;

        const uint32_t buckets[2] = {b1, b2};
        bool done = false;
        for (uint32_t bi = 0; bi < 2 && !done; bi++) {
            uint32_t bucket = buckets[bi];
            for (uint32_t lane = 0; lane < KV_BUCKET_SLOTS; lane++) {
                std::atomic<Node *> *slot = slot_ptr(shard, bucket, lane);
                Node *node = slot->load(std::memory_order_acquire);
                if (!fallback_slot) {
                    fallback_slot = slot;
                    fallback_node = node;
                }

                if (!node) {
                    // First empty slot candidate for insertion.
                    if (!empty_slot) empty_slot = slot;
                    continue;
                }

                if (node->hash == hash && node_key_equals(node, key)) {
                    // Existing key found: attempt overwrite by pointer swap.
                    found_slot = slot;
                    found_node = node;
                    done = true;
                    break;
                }

                if (node_is_expired(node, now_ms)) {
                    // Prefer expired victim for replacement.
                    if (!expired_slot) {
                        expired_slot = slot;
                        expired_node = node;
                    }
                    continue;
                }

                // Cold candidate for second-chance eviction.
                if (!cold_slot && node->refbit.load(std::memory_order_relaxed) == 0) {
                    cold_slot = slot;
                    cold_node = node;
                }
            }
        }

        if (found_slot && found_node) {
            Node *expected = found_node;
            // Replace existing key atomically; loser retries with fresh scan.
            if (found_slot->compare_exchange_strong(expected, fresh,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                ws->local_live_delta += static_cast<int64_t>(fresh->alloc_size)
                                      - static_cast<int64_t>(found_node->alloc_size);
                retire_node(store, worker_id, found_node);
                // Trim is best-effort; success path remains OK either way.
                if (!trim_to_capacity(store, worker_id, ws, now_ms)) {
                    return KvSetStatus::OK;
                }
                return KvSetStatus::OK;
            }
            continue;
        }

        if (empty_slot) {
            Node *expected = nullptr;
            // Insert into empty slot.
            if (empty_slot->compare_exchange_strong(expected, fresh,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                ws->local_live_delta += static_cast<int64_t>(fresh->alloc_size);
                if (!trim_to_capacity(store, worker_id, ws, now_ms)) {
                    return KvSetStatus::OK;
                }
                return KvSetStatus::OK;
            }
            continue;
        }

        std::atomic<Node *> *victim_slot = nullptr;
        Node *victim_node = nullptr;
        if (expired_slot && expired_node) {
            victim_slot = expired_slot;
            victim_node = expired_node;
        } else if (cold_slot && cold_node) {
            victim_slot = cold_slot;
            victim_node = cold_node;
        } else {
            // No immediate victim: clear refbits in candidate buckets and retry.
            for (uint32_t bi = 0; bi < 2; bi++) {
                uint32_t bucket = buckets[bi];
                for (uint32_t lane = 0; lane < KV_BUCKET_SLOTS; lane++) {
                    Node *node = slot_ptr(shard, bucket, lane)->load(std::memory_order_relaxed);
                    if (!node) continue;
                    node->refbit.store(0, std::memory_order_relaxed);
                }
            }
            if (fallback_slot && fallback_node) {
                // Deterministic last-resort victim to guarantee progress attempt.
                victim_slot = fallback_slot;
                victim_node = fallback_node;
            }
        }

        if (victim_slot && victim_node) {
            Node *expected = victim_node;
            // Replace selected victim atomically.
            if (victim_slot->compare_exchange_strong(expected, fresh,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
                ws->local_live_delta += static_cast<int64_t>(fresh->alloc_size)
                                      - static_cast<int64_t>(victim_node->alloc_size);
                retire_node(store, worker_id, victim_node);
                if (!trim_to_capacity(store, worker_id, ws, now_ms)) {
                    return KvSetStatus::OK;
                }
                return KvSetStatus::OK;
            }
            continue;
        }
    }

    // Could not place within retry budget.
    (void)trim_to_capacity(store, worker_id, ws, now_ms);
    node_free(store, worker_id, fresh);
    return KvSetStatus::OOM;
}

uint64_t kv_store_live_bytes(const KvStore *store) {
    if (!store) return 0;
    return store->live_bytes.load(std::memory_order_acquire);
}

uint64_t kv_store_capacity_bytes(const KvStore *store) {
    if (!store) return 0;
    return store->capacity_bytes;
}

uint64_t kv_time_now_ms() {
    // Real-time clock helper for callers that do not provide their own time.
    struct timespec ts = {};
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return 0;
    uint64_t sec = static_cast<uint64_t>(ts.tv_sec);
    uint64_t nsec = static_cast<uint64_t>(ts.tv_nsec);
    return sec * 1000ull + nsec / 1000000ull;
}
