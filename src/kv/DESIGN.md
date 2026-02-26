# Shared KV Store Design (`src/kv`)

## 1. Purpose and Scope

This document describes the current shared in-memory key/value store implemented in:

- `src/kv/shared_kv_store.h`
- `src/kv/shared_kv_store.cpp`

The store is designed to be protocol-agnostic and shared by many worker threads.

Primary goals in the current implementation:

- Concurrent multi-threaded `GET` and `SET` without mutexes.
- Very low hot-path overhead (atomic pointer operations, short probe windows).
- Bounded memory target with eviction.
- Key expiration support (relative ms and absolute ms).
- Reduced long-run heap fragmentation via pooled allocation classes.

Non-goals (for current version):

- Full Redis semantics beyond simple key/value operations.
- Fully strict wait-free progress for all operations.
- Exact LRU ordering.
- Background sweeper thread for expiration.

---

## 2. API Surface

Declared in `shared_kv_store.h`:

```c++
KvStore *kv_store_create(const KvStoreConfig *config);
void kv_store_destroy(KvStore *store);

int kv_store_register_worker(KvStore *store, uint32_t worker_id);
void kv_store_quiescent(KvStore *store, uint32_t worker_id);

KvGetStatus kv_store_get(KvStore *store, uint32_t worker_id,
                         std::string_view key,
                         uint64_t now_ms,
                         KvValueView *out);

KvSetStatus kv_store_set(KvStore *store, uint32_t worker_id,
                         std::string_view key,
                         std::string_view value,
                         uint64_t now_ms,
                         const KvSetOptions *opts);

uint64_t kv_store_live_bytes(const KvStore *store);
uint64_t kv_store_capacity_bytes(const KvStore *store);

uint64_t kv_time_now_ms();
```

### Status enums

- `KvGetStatus`: `MISS`, `HIT`
- `KvSetStatus`: `OK`, `OOM`, `INVALID`

### Expiration modes

- `NONE`
- `AFTER_MS` (relative TTL)
- `AT_MS` (absolute epoch-ms)

### Important call contract

- `now_ms` is supplied by caller, not fetched internally in `get/set`.
- Worker threads are expected to call `kv_store_quiescent` periodically.

Passing `now_ms` from caller has two benefits:

- Deterministic tests.
- Avoiding a syscall/clock read on every request in very hot loops.

---

## 3. High-Level Architecture

The store is a sharded hash table with lock-free slot updates.

```
+-------------------------------------------------------------+
|                         KvStore                             |
|-------------------------------------------------------------|
| capacity_bytes                                              |
| live_bytes (atomic)                                         |
| shard_count, shard_mask                                     |
| shards[]                                                    |
| worker_count                                                |
| workers[]                                                   |
| global_seq (atomic, for deferred reclamation)              |
| pools[10] (size-class allocators)                           |
+-------------------------------+-----------------------------+
                                |
                                v
                  +-------------------------------+
                  |            Shard              |
                  |-------------------------------|
                  | bucket_count, bucket_mask     |
                  | clock_hand (atomic)           |
                  | slots[] (atomic Node* array)  |
                  +-------------------------------+
```

Each shard stores fixed-size buckets with 4 lanes (`KV_BUCKET_SLOTS = 4`).

```
bucket i
+---------+---------+---------+---------+
| slot 0  | slot 1  | slot 2  | slot 3  |   each slot = atomic<Node*>
+---------+---------+---------+---------+
```

For each key, two bucket indices are computed (`b1`, `b2`), and lookup/insertion scans up to 8 slots total.

---

## 4. Data Structures

## 4.1 `Node`

`Node` is immutable for key/value payload after allocation.

Fields:

- `hash`: cached 64-bit key hash.
- `expire_at_ms`: `0` means no expiration.
- `retire_seq`: sequence number assigned when removed/replaced.
- `key_len`, `value_len`.
- `alloc_size`: actual allocator size used for accounting/freeing.
- `class_idx`: pool class or `KV_CLASS_LARGE`.
- `gc_next`: linked-list pointer for deferred free.
- `refbit` (atomic byte): second-chance eviction hint.
- `bytes[]`: packed `[key bytes][value bytes]`.

Layout advantage:

- Single contiguous allocation for metadata + key + value.
- No extra pointer chasing for key/value storage.

Trade-off:

- Overwrite allocates a new node (copy-on-write style) even for small changes.

## 4.2 Shards

`Shard` (cache-line aligned):

- `bucket_count` and mask.
- `clock_hand` for approximate LRU walk.
- slot array (`atomic<Node*>`).

`alignas(64)` is used to reduce false sharing of hot shard metadata.

## 4.3 Worker state

Per worker (`alignas(64)`):

- `quiescent_seq`: last observed global sequence.
- `registered`: registration flag.
- `touch_counter`: sampled read-touch counter.
- `shard_cursor`: per-worker start point for trim scans.
- `retired_head`: private linked list of retired nodes waiting for safe free.

Purpose:

- Avoid global retired list lock/contention.
- Distribute reclamation work across workers.

## 4.4 Pool classes

Fixed classes (`KV_CLASS_COUNT = 10`):

- 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768 bytes.

Each class has:

- Lock-free free list (`free_head` atomic).
- Block list (`blocks` atomic) for cleanup at destroy.

Large objects (`>32768`) use direct `malloc/free` and class marker `KV_CLASS_LARGE`.

---

## 5. Hashing and Placement

### 5.1 Hash function

Current code uses FNV-1a + final avalanche mixing:

1. FNV-1a over key bytes.
2. Murmur-like avalanche rounds.

Design intent:

- Good distribution for arbitrary binary keys.
- Low compute overhead.

### 5.2 Shard selection

`shard = (hash >> 32) & shard_mask`

Using high bits for shard selection helps decouple from low-bit bucket indexing.

### 5.3 Bucket selection

`b1 = low_bits(hash) & bucket_mask`

`b2 = mixed(hash) & bucket_mask` with forced `b2 != b1`.

This is a two-choice table strategy:

- Better occupancy than single-bucket open addressing.
- Keeps probe window bounded and branch-predictable.

---

## 6. Core Operations

## 6.1 GET path

Pseudo flow:

```
hash(key)
-> select shard
-> compute b1,b2
-> scan bucket b1 (4 lanes), then b2 (4 lanes)
   - empty => continue
   - hash/key mismatch => continue
   - match:
       if expired(now_ms): CAS slot->nullptr and retire node, return MISS
       else: sampled refbit touch, return HIT with value view
-> MISS
```

### Read semantics

- Uses `atomic<Node*>::load(memory_order_acquire)`.
- Returns raw pointer view (`KvValueView`) into node payload.
- No allocation/copy in store on hit.

### Expiration on read

Expiration is lazy:

- No background sweeper.
- Expired entries are removed when encountered by reads/writes/eviction scans.

Trade-off:

- Lower background CPU overhead.
- Expired cold keys may retain memory until touched/evicted.

## 6.2 SET path

Pseudo flow:

```
allocate fresh Node upfront (includes key+value copy)
if node_size > capacity: OOM

hash(key), pick shard, b1,b2
repeat up to 128 attempts:
  scan 8 candidate slots and record:
    - existing key slot (found_slot)
    - first empty slot
    - first expired victim
    - first cold victim (refbit==0)
    - fallback first observed slot

  if found_slot:
    CAS replace old->fresh
    adjust live_bytes by size delta
    retire old
    trim_to_capacity()
    return OK

  else if empty_slot:
    CAS nullptr->fresh
    live_bytes += fresh_size
    trim_to_capacity()
    return OK

  else choose victim:
    prefer expired, then cold, else clear refbits and use fallback
    CAS victim->fresh
    adjust live_bytes by size delta
    retire victim
    trim_to_capacity()
    return OK

if all attempts fail:
  trim_to_capacity() best effort
  free fresh
  return OOM
```

Key properties:

- No in-place mutation of existing node payload.
- Successful writes linearize at a slot CAS.
- Bounded retry loop (`128`) prevents unbounded spinning in high contention.

Trade-offs:

- Allocation occurs before lock-free placement succeeds; repeated contention wastes allocation work.
- Fixed 2-bucket candidate set can reject inserts under high local occupancy even if table has capacity elsewhere.

## 6.3 Quiescent/reclamation path

`kv_store_quiescent`:

1. Worker publishes its current `quiescent_seq = global_seq`.
2. Computes `min_seq` across registered workers.
3. Scans its own retired list.
4. Frees nodes with `retire_seq <= min_seq`.
5. Keeps newer retired nodes for future passes.

This is epoch-style deferred reclamation, distributed per worker.

Why it exists:

- Readers can hold raw node pointers briefly.
- Removed/replaced nodes cannot be freed immediately without risking use-after-free.

Trade-off:

- Reclamation depends on workers calling quiescent regularly.
- A stalled worker delays memory return of retired nodes across the system.

---

## 7. Expiration Model

TTL data is stored directly in `Node::expire_at_ms`.

Computation rules:

- `NONE` => `expire_at_ms = 0`
- `AFTER_MS` => `now_ms + ttl` (saturates to `UINT64_MAX` on overflow)
- `AT_MS` => exact timestamp

Expired check:

- `expire_at_ms != 0 && now_ms >= expire_at_ms`

Deletion is lazy:

- On `GET` match, expired item is CAS-removed.
- On `SET`, expired entries are preferred as victims.
- On trim/eviction, expired entries are preferred.

No periodic scan thread is implemented.

---

## 8. Eviction and Capacity Enforcement

The store tracks `live_bytes` of active slot-resident nodes.

## 8.1 Capacity accounting

- Increment/decrement by `Node::alloc_size` (class size for pooled nodes, raw size for large nodes).
- Capacity target is `capacity_bytes`.

Important nuance:

- This accounting covers active key/value node allocations.
- It does not include all metadata overhead (slots array, pool block structs, etc).

## 8.2 Trimming algorithm (`trim_to_capacity`)

When `live_bytes > capacity_bytes`, trim scans shards in round-robin order from per-worker cursor.

For each shard, `evict_one_from_shard`:

- Starts from shard `clock_hand`.
- Scans up to `min(bucket_count, 64)` buckets.
- For each slot:
  - Prefer expired node.
  - Else if `refbit == 0` (cold), evict.
  - Else clear `refbit` and continue.

This is approximate-LRU via second-chance behavior.

### Why approximate

Exact LRU would require global recency structures and much higher write amplification on read paths.

Approximate approach keeps hot-path cheaper and better parallelizable.

### Hard vs soft bound

The implementation targets bounded memory but is not a strict per-operation hard cap in all races:

- Multiple writers can temporarily overshoot before trims converge.
- If trim cannot find victims quickly, `SET` paths that already inserted/replaced currently still return `OK`.

So the bound is enforced as a best-effort convergence target, not a strict instant limit.

---

## 9. Memory Management and Fragmentation

## 9.1 Allocation strategy

For object size `raw_size = offsetof(Node, bytes) + key_len + value_len`:

- If `raw_size <= 32768`: allocate from nearest size class.
- Else: direct `malloc`.

Class pools grow by `KV_GROW_BATCH=64` objects per refill.

```
ClassPool.free_head (lock-free stack)
   |
   +--> [chunk] -> [chunk] -> ...

ClassPool.blocks (for destroy-time ownership)
   |
   +--> [block(mem ptr)] -> [block] -> ...
```

## 9.2 Fragmentation impact

Positive:

- Reuse within classes significantly reduces allocator churn for common sizes.
- Stable class sizes reduce heap fragmentation over long runs for small/medium values.

Costs:

- Internal fragmentation from size-class rounding (`alloc_size >= raw_size`).
- Very large values still go through libc allocator (`KV_CLASS_LARGE`).

## 9.3 Lifetime and free timing

- Active nodes in slots are freed on replacement/eviction only after quiescent safe-point.
- Retired nodes can accumulate if quiescent cadence is too low.

Operational implication:

- `live_bytes` can be low while retained-but-not-yet-freed memory is still physically resident.

---

## 10. Concurrency Model and Progress Guarantees

The design is lock-free in the sense of no mutexes/condvars; it is atomic/CAS-based.

### 10.1 Atomic points

- Slots: `atomic<Node*>` with acquire load and acq_rel CAS.
- Global/worker seq and registration flags: atomics.
- Pool free lists: lock-free Treiber-like stacks.

### 10.2 Progress characteristics

- `GET`: bounded scan (8 slots), no retry loop except optional expired-CAS attempt.
- `SET`: bounded retry loop (`<=128`) plus optional trim work.

Not formally wait-free for all operations:

- CAS contention can cause retries.
- Trim work can add variable latency.

But hot-path per-attempt work is intentionally bounded and branch-light.

### 10.3 Consistency behavior

For a single key mapped to a slot:

- Successful `SET` publishes a fully initialized node pointer atomically.
- `GET` reads pointer atomically and validates hash/key.

This gives per-slot atomic replacement semantics.

Caveat:

- The implementation is designed for strong practical recency visibility, but it does not implement a full transactional multi-key consistency model.

---

## 11. Cache, False Sharing, and Coherence Analysis

## 11.1 Sharding to reduce contention

Sharding partitions write traffic:

- Different keys likely map to different shards.
- Eviction clock hand is shard-local.

This reduces cross-core cacheline bouncing compared with one global table lock.

## 11.2 Cache-line alignment

`alignas(64)` on `Shard`, `WorkerState`, and `ClassPool` reduces false sharing among frequently updated control fields.

## 11.3 Read-touch sampling

`refbit` updates are sampled (`1/8` hits), not every hit.

Why:

- A write on every `GET` would generate coherence traffic on hot keys.
- Sampling keeps enough recency signal for second-chance eviction while lowering write amplification.

## 11.4 Data locality

Node payload is contiguous (`[metadata][key][value]`) which helps sequential cache use once slot hits.

Trade-off:

- Comparing key requires reading node metadata and possibly key bytes from heap object, not inline in slot.

---

## 12. Complexity and Cost Model

`GET` expected:

- O(1), up to 8 slot probes.
- No allocation.
- One key compare on probable matches.

`SET` expected:

- O(1) expected under healthy occupancy.
- One allocation + key/value copy.
- Up to 8 slot probes per attempt, bounded 128 attempts.
- May include eviction scan if over capacity.

`trim_to_capacity` worst-case per call:

- Multiple shard passes.
- Each `evict_one_from_shard` scans up to 64 buckets * 4 slots.

---

## 13. Configuration Heuristics

When `KvStoreConfig` fields are zero:

- `capacity_bytes`: default `256 MiB`.
- `shard_count`: next pow2 of `max(worker_count*4, 64)`.
- `buckets_per_shard`:
  - starts from `capacity / 256` total buckets
  - divide across shards
  - minimum 16 per shard
  - round up to power of two

Heuristic intent:

- Enough shards to spread worker contention.
- Bucket density roughly scaled with configured capacity.

---

## 14. Failure Modes and Edge Cases

## 14.1 OOM behaviors

`SET` returns `OOM` when:

- Fresh node allocation fails.
- Single node is larger than total capacity.
- Placement repeatedly fails under heavy contention/full candidate set.

## 14.2 Worker registration/quiescent

- `kv_store_register_worker` validates `worker_id`.
- `kv_store_get/set` with invalid worker id still operate, but internal worker-specific behavior degrades:
  - touch sampling fallback behaves differently.
  - retirement falls back to worker 0 list.

## 14.3 Clock source

`kv_time_now_ms()` uses `CLOCK_REALTIME`.

Trade-off:

- Real wall clock semantics.
- Vulnerable to clock jumps (NTP/manual adjustments) if caller uses this directly for TTL.

(Caller may pass monotonic-derived values if desired, as API accepts external `now_ms`.)

## 14.4 Table occupancy limitation

The 2-bucket x 4-slot model can reject inserts despite available space elsewhere in the shard/table.

This is accepted for low-latency bounded probing and simple lock-free behavior.

---

## 15. Integration Notes (Current Dalahash)

Redis path wraps this store via `src/redis/store.h`.

- Server creates one shared `KvStore` and passes it to workers.
- Worker calls protocol quiescent hook each loop iteration.
- Redis `GET`/`SET` use store wrapper and currently no TTL command syntax is exposed at wire level.

Relevant integration points:

- `src/net/server.cpp`
- `src/net/worker.cpp`
- `src/protocol/redis/redis_protocol.h`
- `src/redis/store.h`

---

## 16. Design Trade-Off Summary

### Chosen

- **Shard-local lock-free slots** over global lock:
  - Better multicore scalability, lower lock convoying.
  - More complex memory reclamation.

- **Approximate recency (second-chance/CLOCK)** over exact LRU:
  - Lower metadata/coherence overhead.
  - Non-deterministic victim quality.

- **Lazy expiration** over background sweeper:
  - Lower constant CPU overhead.
  - Cold expired keys can linger.

- **Size-class pools** over pure malloc/free:
  - Lower fragmentation/churn for common sizes.
  - Internal fragmentation and larger implementation surface.

- **Bounded probe windows** over full-table probing/rehash:
  - Predictable latency.
  - Reduced maximum load factor before failures.

### Not yet solved perfectly

- Strict hard memory cap under all races.
- Fully wait-free updates under contention.
- Global/precise fairness of eviction decisions.
- Automatic cleanup when workers do not quiesce.

---

## 17. Operational Guidance

For best behavior in production-like loads:

- Ensure each worker calls `kv_store_quiescent` frequently (already done in worker loop).
- Size `capacity_bytes` with headroom for allocator/pool metadata and deferred reclamation lag.
- Keep shard count reasonably above active writers (default heuristic is a good starting point).
- Avoid very large values if low-latency tail is critical (`KV_CLASS_LARGE` path uses malloc/free).

---

## 18. Potential Future Improvements

1. Stronger hard-cap admission control before insertion under high contention.
2. Better occupancy strategy (bounded cuckoo relocation or stash) to reduce false OOM at high load.
3. Optional background sweeper for expiry and deferred reclaim assist.
4. Optional monotonic-time TTL mode at API level.
5. Per-shard live-bytes to reduce global accounting contention.
6. More cacheline partitioning for slot metadata in very high core-count machines.
7. Read-side epoch/hazard API contract for safer generic external consumers of `KvValueView`.

---

## 19. ASCII Walkthrough Example

`SET foo bar` then `GET foo` with 2 threads:

```
Writer thread                             Reader thread
-------------                             -------------
alloc Node(foo,bar)
scan b1,b2
CAS slot: old -> new   -----------------> load slot (acquire)
retire old node                             hash/key match
live_bytes adjust                            not expired
optional trim                                sampled refbit touch
return OK                                    return value view

quiescent() may later free retired old node only when all registered workers
have advanced their quiescent_seq past old.retire_seq.
```

This is the core publish/read/reclaim lifecycle used throughout the implementation.

---

## 20. Code-Level Walkthrough of `src/kv/shared_kv_store.h`

This section documents the header as a developer-facing contract.

## 20.1 Forward type and ownership model

`struct KvStore;`

- Opaque type. Consumers cannot directly inspect internal fields.
- All lifecycle control is through `kv_store_create/destroy`.
- Internal layout is intentionally hidden so implementation can evolve without
  protocol-layer churn.

## 20.2 Public enums and their meaning

### `KvGetStatus`

- `MISS`: Key not present, expired, invalid input, or output buffer pointer null.
- `HIT`: Key matched and `KvValueView` populated.

### `KvSetStatus`

- `OK`: Insert/overwrite succeeded (possibly after eviction or retries).
- `OOM`: Could not admit value (allocator failure, value bigger than capacity,
  or contention/placement/eviction could not make progress within current policy).
- `INVALID`: Invalid API usage (e.g. null store pointer).

### `KvExpireMode`

- `NONE`: no expiration.
- `AFTER_MS`: compute expiration as `now_ms + ttl`.
- `AT_MS`: use absolute timestamp directly.

## 20.3 Public value/config structs

### `KvSetOptions`

- `mode`: expiration mode.
- `value_ms`: either ttl or absolute timestamp based on mode.

No extra flags currently exist (e.g. NX/XX, compare-and-swap tokens, etc.).

### `KvValueView`

- `data`: pointer into internal node storage.
- `len`: value length.

Pointer stability rules are intentionally short-lived:

- valid for immediate response formatting/copy path.
- not safe for long-term caching by caller.
- may become obsolete after concurrent overwrite/eviction + eventual reclamation.

### `KvStoreConfig`

- `capacity_bytes`: target capacity for active key/value payload nodes.
- `shard_count`: optional override; rounded to power-of-two.
- `buckets_per_shard`: optional override; rounded to power-of-two.
- `worker_count`: expected number of worker threads using quiescent API.

If values are `0`, implementation chooses heuristics.

## 20.4 Lifecycle API

### `kv_store_create`

- Allocates and initializes global object, workers array, shards, slot atomics.
- Initializes size-class pools but does not pre-grow all pool memory.

### `kv_store_destroy`

- Traverses all slots and frees active nodes.
- Frees all retired lists.
- Frees all pool blocks and metadata.
- Not safe to call concurrently with active users.

## 20.5 Worker coordination API

### `kv_store_register_worker`

- Marks worker as participating in quiescent protocol.
- Seeds worker quiescent sequence from current global sequence.

### `kv_store_quiescent`

- Publishes worker quiescent progress.
- Runs deferred reclamation pass for that worker’s retired list.

## 20.6 Data path API

### `kv_store_get`

- Performs dual-bucket lookup.
- Removes expired entries lazily via slot CAS.
- Returns non-owning `KvValueView` on hit.

### `kv_store_set`

- Allocates fresh immutable node.
- Inserts/replaces via CAS in bounded retries.
- Performs trim (eviction) if capacity exceeded.

## 20.7 Introspection helper API

### `kv_store_live_bytes`

- Returns active node accounting.

### `kv_store_capacity_bytes`

- Returns configured capacity target.

### `kv_time_now_ms`

- Convenience real-time clock helper.
- Caller may bypass and provide external time source.

---

## 21. Code-Level Walkthrough of `src/kv/shared_kv_store.cpp`

This section documents every major constant, type, helper, and exported function.

## 21.1 Constants

### `KV_BUCKET_SLOTS = 4`

- Each bucket holds 4 independent `atomic<Node*>` lanes.
- Balances occupancy and probe cost.

### `KV_TOUCH_SAMPLE_MASK = 0x7`

- Refbit touch every 8th hit (sampled write-on-read).
- Reduces coherence traffic from hot read keys.

### `KV_GROW_BATCH = 64`

- Pool refills allocate 64 objects at a time.
- Better amortization than single-object refill.

### `KV_CLASS_COUNT = 10` / `KV_CLASS_SIZES[]`

- Geometric-ish classes up to 32768 bytes.
- Objects larger than max class take large-object path.

## 21.2 Internal structs

### `Node`

Detailed field intent:

- `hash`: fast prefilter before key byte compare.
- `expire_at_ms`: expiration; `0` means immortal.
- `retire_seq`: reclamation ordering stamp.
- `key_len`, `value_len`: lengths for slicing packed payload.
- `alloc_size`: exact accounting/free class behavior.
- `class_idx`: pool class id or large sentinel.
- `gc_next`: singly-linked chain node for deferred free lists.
- `refbit`: second-chance metadata for eviction.
- `bytes[]`: packed key+value to minimize pointer fanout.

### `FreeChunk`

- Minimal node for lock-free free list.
- Reuses object memory itself as freelist link.

### `PoolBlock`

- Tracks each bulk allocation backing a pool class.
- Required to free pooled memory at store destroy.

### `ClassPool` (`alignas(64)`)

- `obj_size`: allocation size for this class.
- `free_head`: lock-free stack of currently free objects.
- `blocks`: ownership list for destroy-time cleanup.

### `WorkerState` (`alignas(64)`)

- `quiescent_seq`: worker’s last published epoch.
- `registered`: whether included in min-seq scan.
- `touch_counter`: local read sample counter.
- `shard_cursor`: local starting point for trim shard sweep.
- `retired_head`: retired nodes pending safe free.

### `Shard` (`alignas(64)`)

- `bucket_count`, `bucket_mask`: table geometry.
- `clock_hand`: per-shard eviction scan cursor.
- `slots`: contiguous `atomic<Node*>` array.

### `KvStore`

- Global runtime object tying all pieces together.

Fields summary:

- capacity and live accounting.
- shard topology and pointer.
- worker topology and pointer.
- global retirement sequence.
- size-class pools.

## 21.3 Helper functions (non-exported)

### `next_pow2_u32`

- Rounds config values to power-of-two.
- Used for shard and bucket masks.

### `hash_bytes`

- FNV-1a followed by avalanche.
- Accepts binary keys (no null-termination assumptions).

### `select_shard`

- Uses high hash bits.
- Works with `shard_mask`.

### `secondary_bucket`

- Derives second bucket from mixed hash.
- Guarantees not equal to primary bucket.

### `slot_ptr`

- Computes pointer to slot lane in flat slot array.

### `node_key_ptr`, `node_value_ptr`

- Cheap accessors into packed `bytes[]`.

### `node_key_equals`

- Length check + `memcmp`.
- Handles binary keys.

### `node_is_expired`

- Expiration predicate with `expire_at_ms == 0` fast immortal path.

### `compute_expire_at`

- Encodes API option semantics.
- Handles overflow in `AFTER_MS` by saturation.

### `worker_state`

- Validates worker id and returns state pointer.
- Invalid id returns null.

### `select_class`

- Chooses smallest class that fits raw node size.

### `pool_grow`

Detailed behavior:

1. `malloc(obj_size * KV_GROW_BATCH)`.
2. Build intrablock singly linked free objects.
3. Allocate `PoolBlock` metadata.
4. Push block onto ownership list (`blocks` CAS loop).
5. Push object chain onto `free_head` CAS loop.

Failure behavior:

- If block metadata allocation fails, raw memory is freed immediately.
- Returns `false` to caller so data path can propagate `OOM`.

### `node_alloc`

Detailed behavior:

1. Validate combined payload lengths fit 32-bit and object size bounds.
2. Pick class:
   - class path: pop from pool free list, grow class on empty.
   - large path: direct `malloc`.
3. Fill all node metadata fields.
4. Copy key/value into packed payload.

Implementation detail:

- Node is fully initialized before publication into any slot.

### `node_free`

Behavior:

- Classed node: push back to class free list.
- Large node: direct `free`.
- Invalid class id fallback: direct `free`.

### `retire_node`

Behavior:

1. Assign increasing `retire_seq = ++global_seq`.
2. Push node to worker-local retired list.
3. Invalid worker id falls back to worker `0` retired list.

### `evict_one_from_shard`

Behavior:

1. Start from shard `clock_hand` (fetch_add).
2. Scan up to 64 buckets max.
3. For each node:
   - expired -> candidate
   - or `refbit == 0` -> candidate
   - else clear refbit and continue
4. CAS victim slot to null.
5. Decrement live bytes and retire node.

Return value:

- `true` if one node evicted.
- `false` if no evictable candidate succeeded.

### `trim_to_capacity`

Behavior:

1. Check if already within capacity.
2. Sweep shards from worker cursor.
3. Repeatedly evict one node until under target or no progress.
4. Update worker cursor.

Bounded work:

- Stops after `shard_count * 8` rounds.

## 21.4 Exported functions

### `kv_store_create`

Step-by-step:

1. Validate config pointer.
2. Resolve default/rounded worker_count, capacity, shards, buckets.
3. Allocate `KvStore`.
4. Initialize pool metadata.
5. Allocate and init `workers[]`.
6. Allocate and init `shards[]`.
7. Allocate shard slot arrays and placement-new each `atomic<Node*>`.
8. Roll back cleanly on partial failures.

Notable design points:

- Buckets-per-shard heuristic scales with capacity (`capacity / 256` baseline).
- No prefill of pool object memory; classes grow on demand.

### `kv_store_destroy`

Step-by-step:

1. For each shard slot, free active nodes.
2. Free all retired nodes from each worker list.
3. Free all class pool blocks (bulk memory).
4. Free shards/workers arrays.
5. Delete store.

### `kv_store_register_worker`

- Validates worker range.
- Sets `registered=1`.
- Seeds `quiescent_seq` from current global seq.

### `kv_store_quiescent`

Step-by-step:

1. Resolve worker state.
2. Publish worker quiescent seq.
3. Compute minimum published seq among registered workers.
4. Walk worker retired list:
   - free nodes at/below min
   - keep newer ones

This is the deferred reclamation gate.

### `kv_store_get`

Detailed path:

1. Validate inputs, clear output view.
2. Compute hash/shard/b1/b2.
3. Probe 8 candidate slots.
4. On match:
   - if expired: try CAS-delete and retire, return miss.
   - else sampled touch of refbit and return view hit.
5. Miss if no match.

### `kv_store_set`

Detailed path:

1. Validate store.
2. Compute hash and expiration.
3. Allocate fresh node first.
4. Reject if single node exceeds capacity.
5. Probe candidate slots with bounded retries.
6. Try replacement, insertion, or victim replacement in priority order.
7. Update live accounting deltas and retire old victims.
8. Call trim to converge toward capacity.
9. Return `OOM` if retries exhausted.

Victim priority:

1. expired
2. cold (`refbit==0`)
3. fallback after clearing refbits in candidate buckets

### `kv_store_live_bytes` / `kv_store_capacity_bytes`

- Simple read accessors.

### `kv_time_now_ms`

- Uses `clock_gettime(CLOCK_REALTIME)`.

---

## 22. Atomic Memory Ordering Rationale (Code-Specific)

This section explains why specific memory orders were chosen.

## 22.1 Slot publication and read

- Writer publishes node via CAS/store using `memory_order_acq_rel`.
- Reader loads slot with `memory_order_acquire`.

Effect:

- Reader that observes pointer also observes fully initialized node fields.

## 22.2 Pool free-list operations

- Free list pushes/pops use release/acquire or acq_rel CAS loops.
- Guarantees object memory contents are visible correctly when re-popped.

## 22.3 Sequence and registration fields

- `global_seq` increments with acq_rel.
- Worker `quiescent_seq` publishes with release and reads with acquire.
- `registered` uses release/acquire.

Effect:

- Reclamation sees a coherent published frontier for safe frees.

## 22.4 Relaxed uses

Some fields intentionally use relaxed ordering:

- `live_bytes` increments/decrements for approximate accounting visibility.
- `refbit` writes and reads (eviction hint only).
- `clock_hand` fetch_add (cursor coordination only).

Rationale:

- These values do not protect object initialization/ownership correctness by
  themselves; stronger ordering would add cost without correctness gain.

---

## 23. Function Interaction Matrix

```
kv_store_set
  -> compute_expire_at
  -> node_alloc
  -> (slot scans + CAS)
  -> retire_node (on replace/evict)
  -> trim_to_capacity
        -> evict_one_from_shard
              -> retire_node

kv_store_get
  -> slot scans
  -> node_is_expired
  -> CAS delete expired
  -> retire_node (if delete won)

kv_store_quiescent
  -> publish seq
  -> min seq scan over workers
  -> node_free on eligible retired nodes
```

This graph helps reason about where memory can move between states.

---

## 24. Node Lifecycle States (Code-Accurate)

```
 [ALLOCATED, UNPUBLISHED]
          |
          | CAS publish to slot
          v
 [ACTIVE IN SLOT]
          |
          | replaced / evicted / expired-CAS-delete
          v
 [RETIRED LIST (worker-local)]
          |
          | kv_store_quiescent + retire_seq <= global min
          v
 [FREED / RETURNED TO POOL]
```

Critical rule:

- Node memory never returns to allocator directly from active slot removal.
- It always passes through retired list and quiescent gate.

---

## 25. Current Code Limitations (Specific and Actionable)

1. Candidate placement is strictly two buckets; no relocation/stash.
   - Can increase `OOM` probability before table truly full.
2. `trim_to_capacity` is best-effort convergence, not strict immediate cap.
3. Reclamation depends on cooperative quiescent calls.
4. `CLOCK_REALTIME` helper may be affected by wall-clock jumps.
5. No per-shard byte accounting, so `live_bytes` is a shared atomic hotspot.
6. No explicit instrumentation counters exposed yet (evictions, retries, CAS fail rates).
7. `kv_store_set` allocates fresh node before confirming placement opportunity,
   increasing wasted allocation work under heavy contention.

---

## 26. Suggested Reading Order for Maintainers

To understand code quickly and correctly:

1. `shared_kv_store.h` public API and statuses.
2. `Node`, `Shard`, `WorkerState`, `ClassPool` struct definitions.
3. `kv_store_get` and `kv_store_set` main data paths.
4. `retire_node` + `kv_store_quiescent` reclamation.
5. `trim_to_capacity` + `evict_one_from_shard` eviction logic.
6. allocator helpers (`select_class`, `pool_grow`, `node_alloc`, `node_free`).

This order mirrors runtime criticality and minimizes misinterpretation.
