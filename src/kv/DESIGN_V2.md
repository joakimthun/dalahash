# Shared KV Store V2 Design (`src/kv/shared_kv_store.cpp`)

## 1. Purpose and Scope

This document describes the current `v2` shared key/value store implementation in:

- `src/kv/shared_kv_store.h`
- `src/kv/shared_kv_store.cpp`
- `src/kv/shared_kv_store_internal_stats.h`

`v2` keeps the same public API as `v1`, but changes the internal implementation to reduce the amount of reclamation work paid on the request path, especially under mixed read/write workloads.

The main motivation for `v2` is the scaling gap observed between:

- read-only workloads (`GET` hit path), which already scale well
- mixed workloads (`GET` + `SET`), where `v1` spent too much time repeatedly scanning deferred-reclamation lists in `kv_store_quiescent()`

`v2` preserves the broad shape of the store:

- sharded hash table
- 2 candidate buckets per key
- 4 slots per bucket
- immutable published values
- lock-free slot publication via atomic `Node *` CAS
- approximate second-chance eviction
- pooled allocation for common object sizes

What changes in `v2` is how deferred memory reclamation is organized and when maintenance work runs.

This document is intentionally code-specific. It describes what `v2` actually does today, not an abstract future design.

---

## 2. Build Wiring and API Compatibility

### 2.1 Build-time wiring

The build now always compiles the current implementation in:

- `src/kv/shared_kv_store.h`
- `src/kv/shared_kv_store.cpp`
- `src/kv/shared_kv_store_internal_stats.h`

Current wiring is in `CMakeLists.txt`.

This implementation preserves the public API that was shared with the legacy store.

### 2.2 Public API surface

The public API remains the one declared in `shared_kv_store.h`:

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

### 2.3 Internal-only stats API

`v2` adds an internal diagnostics interface in `shared_kv_store_internal_stats.h`:

```c++
bool kv_store_internal_stats_snapshot(const KvStore *store, KvStoreInternalStats *out);
bool kv_store_internal_stats_reset(KvStore *store);
```

This is not part of the external wire-level behavior. It exists for benchmarks and tests.

- `v2` returns real counters
- `v1` returns `false` and zeroes or ignores the request

---

## 3. High-Level Design Summary

`v2` is a sharded, mostly lock-free, copy-on-write key/value store with cooperative reclamation.

The most important design choices are:

1. **Immutable published nodes**
   - Once a `Node *` is visible in a slot, its key/value payload does not change.
   - Updates allocate a new node and atomically replace the pointer.

2. **Bounded slot probing**
   - Each key looks at exactly 2 buckets, 4 lanes each, for a maximum of 8 slot probes per attempt.

3. **Deferred reclamation**
   - Removed/replaced nodes are not freed immediately.
   - Readers may still hold short-lived raw pointers via `KvValueView`.

4. **Batched retirement**
   - `v1` used per-worker retired linked lists scanned repeatedly.
   - `v2` accumulates retired nodes into batches and queues those batches for later reclamation.

5. **Cheap `quiescent()` fast path**
   - `kv_store_quiescent()` usually only publishes progress and returns.
   - Maintenance only runs when there is enough pending retirement pressure or on periodic cadence.

6. **Best-effort capacity trimming**
   - Memory accounting is approximate.
   - Capacity is a convergence target, not a strict instantaneous hard cap under all races.

7. **Fast allocation path**
   - Common node sizes use size-class pools.
   - Each worker has local caches to avoid global allocator contention.

---

## 4. Constants and Tunables

The implementation hardcodes several important constants:

| Constant | Value | Role |
|---|---:|---|
| `KV_BUCKET_SLOTS` | `4` | Slots per bucket |
| `KV_TOUCH_SAMPLE_MASK` | `0x7` | Touch `refbit` on every 8th hit |
| `KV_GROW_BATCH` | `64` | Objects allocated when a class pool grows |
| `KV_CLASS_COUNT` | `10` | Number of pooled size classes |
| `KV_CLASS_LARGE` | `0xFF` | Marker for non-pooled large allocations |
| `LOCAL_CACHE_MAX` | `32` | Max local cached objects per worker/class before drain |
| `LOCAL_CACHE_BATCH` | `16` | Refill/drain batch size between local and global class pool |
| `RETIRE_BATCH_MAX_NODES` | `64` | Max nodes in one retire batch |
| `RETIRE_BATCH_MAX_BYTES` | `32 KiB` | Max aggregate bytes in one retire batch |
| `MAINTENANCE_TRIGGER_BATCHES` | `8` | Run maintenance when pending batch count reaches this |
| `MAINTENANCE_TRIGGER_BYTES` | `128 KiB` | Run maintenance when queued retired bytes reach this |
| `MAINTENANCE_MAX_BATCHES` | `8` | Max batches reclaimed in one maintenance run |
| `MAINTENANCE_MAX_NODES` | `256` | Max nodes reclaimed in one maintenance run |
| `MAINTENANCE_MAX_BYTES` | `128 KiB` | Max retired bytes reclaimed in one maintenance run |

Size classes:

- `64`
- `128`
- `256`
- `512`
- `1024`
- `2048`
- `4096`
- `8192`
- `16384`
- `32768`

These values intentionally bias toward:

- small, bounded probe windows
- amortized batch operations
- limited per-call maintenance cost
- stable reuse of common object sizes

---

## 5. Data Structures

## 5.1 `Node`

`Node` is the immutable published value object.

Fields:

- `hash`
  - 64-bit hash of the key, cached in the node
- `expire_at_ms`
  - `0` means immortal
  - non-zero is an absolute expiration timestamp in milliseconds
- `retire_seq`
  - retained from the older layout
  - currently not used by the `v2` reclamation algorithm
  - `v2` sets it to `0` when retiring
- `key_len`
- `value_len`
- `alloc_size`
  - actual allocation size used
  - class size for pooled objects
  - raw size for large objects
- `class_idx`
  - pool class index or `KV_CLASS_LARGE`
- reserved bytes
- `gc_next`
  - intrusive link used to chain retired nodes inside a `RetireBatch`
- `refbit`
  - eviction hint for second-chance behavior
- `bytes[1]`
  - packed payload: `[key bytes][value bytes]`

### Why the payload is packed

Packing key and value into one allocation:

- avoids extra heap objects
- improves locality once a node is found
- keeps the publish unit simple: one pointer

### Main consequence

Overwriting a key still allocates and copies the full new value. `v2` reduces reclaim overhead, but it does not eliminate copy-on-write cost.

## 5.2 `FreeChunk`

`FreeChunk` is an intrusive free-list node overlaid on freed object memory.

When a pooled node is free, its memory is treated as:

```text
[next pointer][rest of object memory...]
```

This avoids separate metadata per free object.

## 5.3 `PoolBlock`

`PoolBlock` tracks one bulk allocation for a size class:

- `mem`: raw contiguous block returned by `malloc`
- `next`: ownership link

`PoolBlock` exists only so destroy-time cleanup can free all backing blocks deterministically.

## 5.4 `RetireBatch`

This is the core `v2` reclamation unit.

Fields:

- `next`
  - link in the global retire queue
- `head`
  - first retired node in the batch
- `tail`
  - last retired node in the batch
- `retire_epoch`
  - epoch assigned when the batch is enqueued, not per node
- `node_count`
- `byte_count`

Important property:

- all nodes in a batch share one retire epoch
- the epoch is stamped when the batch is flushed to the global queue
- not when each node is first retired

This deliberately coarsens reclamation precision to reduce atomic traffic and metadata work.

## 5.5 `ClassPool`

One `ClassPool` exists per size class.

Fields:

- `obj_size`
- `free_head`
  - global Treiber-style free stack for that class
- `blocks`
  - ownership list of backing allocations

`ClassPool` is `alignas(64)` so hot pool heads do not share cache lines with neighboring classes.

## 5.6 `LocalClassCache`

Each worker has a local cache per size class:

- `head`
- `count`

This is the primary fast allocation and free path for pooled node sizes.

Benefits:

- avoids global free-list CAS on every alloc/free
- keeps reused objects hot on the worker that most recently touched them

## 5.7 `KvStoreStats`

This is a set of atomic counters used only for diagnostics.

Counters include:

- `set_calls`
- `set_overwrite_same_size`
- `set_overwrite_size_change`
- `set_inserts`
- `set_evictions`
- `set_cas_failures`
- `set_allocations`
- `quiescent_calls`
- `quiescent_fast_returns`
- `retire_batches_enqueued`
- `retired_nodes_enqueued`
- `maintenance_runs`
- `maintenance_nodes_freed`
- `maintenance_batches_freed`

These counters use relaxed increments and are intended for observability, not strict synchronization.

## 5.8 `WorkerState`

One `WorkerState` exists per registered worker ID.

Fields:

- `quiescent_epoch`
  - worker's last published quiescent epoch
- `registered`
  - registration flag
- `touch_counter`
  - local read-hit sampler for `refbit`
- `shard_cursor`
  - starting shard for trim scans
- `quiescent_counter`
  - local cadence counter used for periodic flushing and maintenance decisions
- `local_live_delta`
  - per-worker accumulated byte delta before flushing into global `live_bytes`
- `open_batch`
  - worker's current in-progress retire batch
- `local_caches[]`
  - one local free cache per class

`WorkerState` is `alignas(64)` to reduce false sharing.

### Important behavioral point

`open_batch` is worker-private and unsynchronized except through the stable worker-thread usage contract:

- each worker uses a stable `worker_id`
- one thread owns one `worker_id`

`v2` depends on that contract.

## 5.9 `Shard`

Each shard contains:

- `bucket_count`
- `bucket_mask`
- `clock_hand`
  - used by eviction scans
- `slots`
  - flat array of `atomic<Node *>`

The slot layout is:

```text
bucket 0: slot 0, slot 1, slot 2, slot 3
bucket 1: slot 0, slot 1, slot 2, slot 3
...
```

## 5.10 `KvStore`

Top-level store fields:

- `capacity_bytes`
- `live_bytes`
- `shard_count`
- `shard_mask`
- `shards`
- `worker_count`
- `workers`
- `global_epoch`
- `pending_retired_bytes`
- `pending_retire_batches`
- `over_capacity`
- `maintenance_token`
- `retire_queue_lock`
- `retire_queue_head`
- `retire_queue_tail`
- `pools[]`
- `stats`

### Key global control fields

- `global_epoch`
  - advanced only by maintenance
- `pending_retired_bytes`
  - global retirement pressure signal
- `pending_retire_batches`
  - another pressure signal
- `over_capacity`
  - sticky hint that trim work is still needed
- `maintenance_token`
  - only one worker may run maintenance at a time
- `retire_queue_lock`
  - small spinlock guarding the global retire queue

---

## 6. Hashing, Sharding, and Placement

## 6.1 Hash function

`v2` uses the same internal hash helper as the current code:

- processes 8-byte chunks
- multiply/xorshift mixing
- final avalanche

Goals:

- stable distribution for arbitrary binary keys
- cheap enough for hot path use

## 6.2 Shard selection

Shard selection uses the high hash bits:

```text
shard = (hash >> 32) & shard_mask
```

Using high bits helps decorrelate shard choice from low-bit bucket indexing.

## 6.3 Two-choice bucket selection

The store uses two buckets per key:

- `b1 = low_bits(hash) & bucket_mask`
- `b2 = secondary_mix(hash) & bucket_mask`
- `b2` is forced to differ from `b1`

This gives:

- bounded lookup cost
- better occupancy than one-bucket-only placement
- predictable hot path

## 6.4 Probe window

Each operation scans at most:

- 2 buckets
- 4 lanes each
- 8 slots total per attempt

That bounded window is one of the core latency constraints in the design.

---

## 7. Read Path (`kv_store_get`)

## 7.1 High-level flow

`kv_store_get()`:

1. Validates pointers and worker registration
2. Hashes the key
3. Selects shard and 2 candidate buckets
4. Scans up to 8 slots
5. On key match:
   - if expired: try lazy remove, then return `MISS`
   - else: return `KvValueView`

## 7.2 Slot access model

Slots are loaded with:

- `load(memory_order_acquire)`

This pairs with writer publication CAS and ensures a reader never observes a partially initialized node.

## 7.3 Match sequence

For each non-null slot:

1. compare cached `hash`
2. compare key length and key bytes
3. if match, decide expiration / hit

Hash check avoids unnecessary key comparisons on most misses.

## 7.4 Lazy expiration on read

On a key match:

- if `expire_at_ms != 0` and `now_ms >= expire_at_ms`
- reader attempts `CAS(slot, node, nullptr)`
- only the CAS winner actually removes the entry
- the winner:
  - subtracts `alloc_size` into `local_live_delta`
  - retires the node

If the CAS loses, another thread already changed the slot. The operation still returns `MISS`.

This keeps expiration lazy:

- no background sweeper
- no periodic full-table walk

## 7.5 `refbit` touch sampling

On a hit:

- `touch_counter` increments
- every 8th hit per worker (`KV_TOUCH_SAMPLE_MASK = 0x7`), the reader may set `node->refbit = 1`

Two additional details matter:

1. It first checks whether `refbit` is already `1`
2. The store only writes when it is currently `0`

This avoids unnecessary cacheline write traffic on hot keys.

## 7.6 Returned value lifetime

`KvValueView` points directly into internal node memory.

This is why the store cannot free or mutate the old node immediately after a replace/remove:

- readers may still be formatting a response from that pointer
- deferred reclamation is mandatory for safety

---

## 8. Write Path (`kv_store_set`)

## 8.1 Main `v2` change versus `v1`

`v1` allocated the replacement node before it knew which placement path would win.

`v2` first classifies the candidate slots, then allocates lazily only if it has a concrete action:

- overwrite
- insert into empty slot
- replace a victim

This reduces wasted allocation and copy work under contention.

## 8.2 High-level flow

For up to 128 attempts:

1. Scan the 8 candidate slots
2. Record:
   - matching key slot
   - first empty slot
   - first expired slot
   - first cold slot (`refbit == 0`)
   - first observed slot as deterministic fallback
3. Choose one action:
   - overwrite existing key
   - insert into empty
   - replace expired/cold/fallback victim
4. Allocate `fresh` if not already allocated
5. Attempt CAS publish
6. On CAS failure, retry

If all attempts fail:

- optionally perform trim
- free `fresh` if allocated
- return `OOM`

## 8.3 Overwrite path

When the matching key is found:

1. Allocate `fresh` if needed
2. Reject immediately if `fresh->alloc_size > capacity_bytes`
3. Attempt `CAS(found_slot, found_node, fresh)`
4. On success:
   - `delta = fresh->alloc_size - found_node->alloc_size`
   - add `delta` into `local_live_delta`
   - retire `found_node`
   - if `delta == 0`, count as same-size overwrite
   - if `delta != 0`, count as size-changing overwrite
   - call `trim_to_capacity()` only if:
     - `delta > 0`, or
     - `over_capacity != 0`

### Why same-size overwrites skip trim

This is a key `v2` optimization.

For same-size overwrites:

- active slot count does not increase
- `live_bytes` does not increase
- immediate trim work is usually pointless

So `v2` avoids calling trim unless there is already known over-capacity pressure.

## 8.4 Insert into empty slot

When an empty slot exists:

1. Allocate `fresh` if needed
2. CAS `nullptr -> fresh`
3. On success:
   - add `fresh->alloc_size` into `local_live_delta`
   - count as insert
   - always call `trim_to_capacity()`

This can temporarily overshoot capacity, but trim runs immediately afterward on the writer thread.

## 8.5 Victim replacement path

If no match and no empty slot:

Priority order:

1. expired slot
2. cold slot (`refbit == 0`)
3. fallback first-seen slot after clearing `refbit` in both candidate buckets

On successful CAS:

- replace victim with `fresh`
- apply size delta into `local_live_delta`
- retire victim
- count as eviction
- call `trim_to_capacity()` if:
  - delta is positive, or
  - `over_capacity` is set

## 8.6 CAS failure handling

On any failed CAS:

- the operation does not free `fresh`
- it retries the scan/classification
- the already-allocated `fresh` is reused across attempts

This avoids repeated allocate/free churn within one `SET`.

`set_cas_failures` counts these failed publication attempts.

## 8.7 Retry bound

The retry loop is capped at 128 attempts.

This prevents unbounded spinning in a pathological hot bucket.

If the loop exhausts:

- best-effort trim may still run
- the operation returns `OOM`

This means `OOM` can represent:

- true allocation failure
- value larger than configured capacity
- inability to place within the current bounded policy

It is not purely "physical memory exhausted."

---

## 9. Capacity Accounting and Eviction

## 9.1 `live_bytes`

`live_bytes` tracks active, slot-resident node memory only.

It does **not** include:

- slot array memory
- `RetireBatch` metadata
- `PoolBlock` metadata
- retained-but-not-yet-freed retired nodes in a separate accounting bucket

So it is a targeted active-data metric, not a full RSS estimate.

## 9.2 Per-worker delta accumulation

Writers do not update `live_bytes` directly on every mutation.

Instead they update:

- `ws->local_live_delta`

That delta is periodically flushed into the global `live_bytes` via `flush_live_delta()`.

This reduces contention on the global accounting atomic.

## 9.3 `over_capacity`

`over_capacity` is a sticky global hint:

- set when trim observes `live_bytes > capacity_bytes`
- cleared when trim converges below the cap

It exists so same-size overwrite fast paths can still perform trim if the store is already known to be above target.

## 9.4 Trimming algorithm

`trim_to_capacity()`:

1. flushes local byte delta
2. loads global `live_bytes`
3. if under cap:
   - clears `over_capacity`
   - returns success
4. if over cap:
   - sets `over_capacity`
   - scans shards starting at `ws->shard_cursor`
   - calls `evict_one_from_shard()` until under cap or no progress

The loop is bounded by:

- at most `store->shard_count * 8` rounds

This keeps trim best-effort and bounded.

## 9.5 `evict_one_from_shard()`

Eviction starts from shard-local `clock_hand`:

- `fetch_add(1)` spreads the starting point

Within a shard:

- scan at most 64 buckets
- each bucket scans 4 lanes

For each candidate node:

1. if expired, evict
2. else if `refbit == 0`, evict
3. else clear `refbit` and continue

This is a second-chance approximation, not exact LRU.

---

## 10. Allocation Strategy

## 10.1 Node size calculation

Requested raw size is:

```text
offsetof(Node, bytes) + key.size() + value.size()
```

That raw size is rounded to the nearest supported class if possible.

## 10.2 Small and medium objects

For sizes up to `32768` bytes:

- choose the first fitting class
- allocate from the class pool

Fast path:

1. use `worker_id` to find the worker's `LocalClassCache`
2. pop from the local cache if available
3. refill from the global class free list in batches of 16 if empty
4. grow the class pool in blocks of 64 objects if the global list is empty

This yields:

- predictable object sizes
- reduced allocator churn
- low synchronization frequency

## 10.3 Large objects

If the raw size exceeds the largest class:

- allocate exact size with `malloc`
- free with `free`

This avoids excessive internal fragmentation for rare large values.

## 10.4 Pool growth

`pool_grow()`:

1. allocates one contiguous block of `obj_size * 64`
2. allocates one `PoolBlock` metadata object
3. links all objects into a free chain
4. pushes the block into `blocks`
5. pushes the chain into `free_head`

This is the main amortization mechanism for pooled sizes.

## 10.5 Local free path

When `node_free()` is called for a pooled node:

- if the freeing worker is a valid registered worker:
  - push into that worker's local cache
  - if local cache exceeds `LOCAL_CACHE_MAX = 32`, drain 16 objects back to global

### Important consequence

Freed nodes are recycled to the *worker that frees them*, not necessarily the worker that originally allocated or retired them.

This matters because maintenance can run on any worker:

- a worker that runs maintenance may absorb many reclaimed nodes into its local caches

This is a deliberate simplification. It avoids per-origin ownership tracking.

## 10.6 Destroy-time free path

Destroy uses `worker_id = UINT32_MAX` as a sentinel.

That forces `node_free()` into the global fallback path so worker-local caches are bypassed.

---

## 11. Reclamation and Cleanup in Detail

This is the most important `v2` subsystem.

## 11.1 Why reclamation is deferred

Readers receive `KvValueView` pointing directly into node memory.

Therefore:

- removing a node from a slot does not mean the node can be freed immediately
- some other reader may still be reading the value bytes from that node

`v2` uses a quiescent-state based reclamation model:

- each worker periodically publishes "I have passed a safe point"
- retired memory becomes reclaimable only after all registered workers have advanced far enough

## 11.2 What changed from `v1`

`v1`:

- appended retired nodes to per-worker lists
- rescanned those lists in `kv_store_quiescent()`

That caused the same old nodes to be revisited many times before finally becoming freeable.

`v2`:

- groups retired nodes into batches
- enqueues whole batches
- reclaims batches only in bounded maintenance passes

The core optimization is:

- **stop paying list traversal on every `quiescent()` call**

## 11.3 Worker-private open batch

Each worker has:

- `open_batch`

This batch is where newly retired nodes are appended.

Retirement flow:

1. ensure an open batch exists
2. append node to `batch->tail`
3. increment `node_count` and `byte_count`
4. if full, flush batch to the global queue

### Batch size thresholds

A batch is considered full when either condition holds:

- `node_count >= 64`
- `byte_count >= 32 KiB`

This lets the batch represent either:

- many tiny objects
- fewer larger objects

## 11.4 Batch enqueue

When a batch is flushed:

1. `batch->retire_epoch = global_epoch.load()`
2. push onto the global retire FIFO queue
3. increment:
   - `pending_retire_batches`
   - `pending_retired_bytes`
4. update stats

The enqueue itself is protected by `retire_queue_lock`, which is an `atomic_flag` spinlock.

### Important nuance

This means `v2` is no longer "purely lock-free" in the strict global sense:

- slot publication is still lock-free
- allocation pools still use lock-free stacks
- but global retire-queue linkage is serialized by a tiny spinlock

That trade-off is intentional:

- reclamation queue operations are relatively infrequent
- they are much cheaper than the repeated list rescans they replace

## 11.5 Epoch assignment granularity

Epoch is assigned at **batch enqueue time**, not per node retirement time.

Implications:

- a node may sit in an open batch briefly before receiving a reclaim epoch
- all nodes in that batch become eligible together
- reclamation can be delayed slightly compared to per-node stamping

This is a latency-for-throughput trade-off:

- less atomic traffic
- less metadata work
- slightly coarser reclamation timing

## 11.6 `kv_store_quiescent()` fast path

`kv_store_quiescent()` now does:

1. validate worker
2. increment `quiescent_calls`
3. flush `local_live_delta`
4. increment `quiescent_counter`
5. publish `quiescent_epoch = global_epoch`
6. if open batch has data and:
   - 64 quiescent calls have passed, or
   - the batch is full
   then flush the batch
7. decide whether maintenance should run
8. if not, count fast return and exit
9. otherwise try to acquire `maintenance_token`
10. if token busy, count fast return and exit
11. if token acquired, run maintenance and release token

### Why periodic batch flush exists

Low write-rate workers might not fill a batch quickly.

Without periodic flushing:

- those nodes could stay hidden in `open_batch`
- maintenance would have nothing to reclaim

So every 64 quiescent calls, a non-empty batch is forced out to the queue.

## 11.7 Maintenance trigger policy

Maintenance is considered only if there is at least one queued batch.

Then it runs when any of these are true:

1. `pending_retire_batches >= 8`
2. `pending_retired_bytes >= 128 KiB`
3. periodic fallback: every 64 quiescent calls for that worker

This gives a mixed trigger model:

- pressure-based under heavy write load
- cadence-based under light write load

## 11.8 Single-runner maintenance

Only one maintenance run is allowed at a time.

`maintenance_token` is an atomic `uint8_t`:

- `0` = no runner
- `1` = maintenance in progress

Workers compete via `compare_exchange_strong`.

If a worker loses the CAS:

- it does not block
- it records a fast return
- it leaves the queued work for the current maintenance runner

This avoids a maintenance stampede.

## 11.9 What one maintenance run does

`maintenance_run()` performs:

1. increment `maintenance_runs`
2. advance `global_epoch` by 1
3. compute `min_epoch` across all registered workers
4. pop reclaimable batches from the queue head while under budget
5. free all nodes in those batches
6. free batch metadata objects
7. update free counters

### Why `global_epoch` advances here

In `v1`, epoch advanced on every `quiescent()`.

In `v2`, epoch advances only during maintenance. This is crucial:

- far fewer writes to the shared epoch counter
- reduced cross-core coherence traffic
- reclamation precision is coarser but much cheaper

### Why reclaim condition is `< min_epoch`

A batch is reclaimed only if:

```text
batch->retire_epoch < min_epoch
```

Not `<=`.

That means:

- if a batch is enqueued at epoch `E`
- all workers later publish `quiescent_epoch >= E`
- one more maintenance epoch advance makes `min_epoch > E`
- only then is the batch eligible

This gives a one-epoch safety margin and simplifies reasoning.

## 11.10 FIFO reclaim queue behavior

The retire queue is a FIFO linked list.

Maintenance only examines the head batch and moves forward in order.

Effects:

- batches are reclaimed in enqueue order
- a newer eligible batch behind an older ineligible head batch will wait

This is intentionally conservative and simple.

Because epochs only advance in maintenance and batch enqueue stamps the current epoch:

- batches naturally tend to cluster by epoch
- strict FIFO ordering is acceptable

## 11.11 Bounded maintenance budget

One maintenance run stops when any budget is hit:

- 8 batches
- 256 nodes
- 128 KiB

This prevents one maintenance pass from becoming an unbounded tail-latency spike.

The trade-off is that under very heavy write pressure:

- some retired memory remains queued
- later quiescent calls will schedule more maintenance

## 11.12 Where reclaimed nodes go

Maintenance frees nodes by calling:

```c++
node_free(store, worker_id, node)
```

with the `worker_id` of the maintenance runner.

Therefore reclaimed pooled nodes typically go to the maintenance runner's local caches first.

This is safe and fast, but it means reclamation can shift allocator locality toward the workers that happen to run maintenance.

## 11.13 Destroy-time cleanup

`kv_store_destroy()` is terminal and requires external synchronization.

Destroy does all cleanup synchronously:

1. flush all `local_live_delta`
2. drain all worker local caches to global class pools
3. free all active slot-resident nodes
4. free nodes still sitting in worker `open_batch`
5. free nodes still sitting in the global retire queue
6. free all pool backing blocks
7. destroy arrays and delete the store

No epoch logic is needed during destroy because the caller guarantees no concurrent readers or writers.

## 11.14 Failure behavior on retire batch allocation failure

Opening a new retire batch allocates a `RetireBatch` metadata object with `malloc`.

If that allocation fails:

- in debug builds, `ASSERT(false, ...)` fires
- in release builds, the function returns without enqueuing the retired node

Current consequence in release builds:

- the retired node is no longer in a slot
- it is not immediately freed
- it is effectively leaked instead of being unsafely reclaimed

This is an intentional safety-over-correctness fallback in the current code:

- it avoids use-after-free
- it accepts a leak in this rare metadata-OOM path

This is an implementation limitation worth knowing.

---

## 12. Concurrency Model and Memory Ordering

## 12.1 Threading contract

The external threading contract is unchanged:

- each calling thread must use a stable, registered `worker_id`
- one `worker_id` is intended to map to one execution context

`v2` depends on this for:

- `WorkerState` ownership
- local allocator caches
- open retire batch ownership

Violating that contract is undefined at the implementation level even if the API may fail soft in some cases.

## 12.2 Slot synchronization

Slots are the primary linearization points.

Writers publish new nodes via:

- `compare_exchange_strong(..., memory_order_acq_rel, memory_order_acquire)`

Readers consume nodes via:

- `load(memory_order_acquire)`

This ensures:

- all node initialization is visible before a reader uses the node
- slot replacement is atomic per slot

## 12.3 Worker registration and quiescent publication

Worker registration:

- `registered.store(1, release)`
- `quiescent_epoch.store(current_epoch, release)`

Maintenance observes:

- `registered.load(acquire)`
- `quiescent_epoch.load(acquire)`

This keeps quiescent publication visible in a simple release/acquire pattern.

## 12.4 Allocation pool synchronization

Global class free lists use CAS-based stacks.

Typical patterns:

- load with acquire when consuming
- CAS with acq_rel on successful pop
- release on push

Worker-local caches need no atomics because they are owned by the stable worker.

## 12.5 Global retire queue synchronization

The queue is guarded by:

- `retire_queue_lock` (`atomic_flag`)

This is not a mutex, but it is a real serialized critical section.

The queue is not on the read path and is only touched:

- when batches are flushed
- when maintenance pops reclaimable batches

## 12.6 Maintenance serialization

`maintenance_token` prevents concurrent maintenance runners.

It is acquired with CAS and released with a plain store.

This ensures:

- at most one thread advances `global_epoch` and drains batches at a time
- pending work is not duplicated

## 12.7 `live_bytes` synchronization

`local_live_delta` is thread-local per worker.

Global accounting updates use:

- `fetch_add(..., relaxed)`
- `fetch_sub(..., relaxed)`

Readers of `live_bytes` use acquire loads when they need a current-ish view.

This is sufficient because `live_bytes` is approximate accounting, not a correctness-critical pointer visibility mechanism.

## 12.8 Stats synchronization

Stats use:

- relaxed increments on update
- acquire loads on snapshot

They are intentionally best-effort.

The snapshot is not a globally serialized "transactional" view. It is good enough for performance analysis.

---

## 13. Trade-offs and Design Rationale

## 13.1 Why keep immutable nodes

Pros:

- simple publication semantics
- safe `KvValueView`
- readers do not need hazard pointers or refcounts

Cons:

- every overwrite still allocates and copies
- write-heavy workloads still pay meaningful memory bandwidth cost

`v2` accepts this because it solves the reclamation bottleneck without changing the external API.

## 13.2 Why batch retirement

Pros:

- avoids rescanning the same retired nodes on every quiescent call
- turns many tiny retire events into fewer queue events
- makes cleanup work easier to budget

Cons:

- reclamation is coarser
- a node may wait in an open batch before it is even visible to maintenance
- one more metadata object (`RetireBatch`) is needed per batch

## 13.3 Why use a global queue with a spinlock

Pros:

- simple and predictable
- avoids complicated lock-free multi-producer/multi-consumer queue code
- low overhead at current flush frequency

Cons:

- not strictly lock-free globally
- a batch flush occasionally pays serialized queue access

This is a deliberate compromise. The queue is not touched on every operation.

## 13.4 Why advance epochs only in maintenance

Pros:

- greatly reduces shared atomic write traffic
- makes `kv_store_quiescent()` cheap in the common case

Cons:

- reclamation precision is epoch-coarser than `v1`
- memory can stay queued slightly longer

Given the target workload, this is the right trade.

## 13.5 Why maintenance is cooperative and not background-threaded

Pros:

- no extra thread management
- no synchronization with a dedicated reaper thread
- deterministic "work happens only when callers interact"

Cons:

- reclamation progress depends on continued calls into the store
- if traffic stops, queued retired nodes may sit until the next quiescent call or destroy

This matches the rest of the system's event-loop-driven model.

## 13.6 Why same-size overwrites skip trim

Pros:

- removes useless capacity work in common overwrite-heavy steady state
- especially helps workloads where keys and value sizes are stable

Cons:

- relies on `over_capacity` to eventually force trim when needed
- adds one global hint bit and more state to reason about

## 13.7 What `v2` does not solve

`v2` is not a full write-scaling solution by itself.

Remaining fundamental write costs include:

- hashing
- key compare and slot scan
- node allocation
- key/value memcpy
- slot CAS
- eventual retirement and free

So `v2` significantly reduces *reclamation overhead*, but it does not eliminate the basic cost of copy-on-write mutation.

---

## 14. Failure Modes and Edge Cases

## 14.1 `SET` can return `OOM` without physical OOM

`KvSetStatus::OOM` may mean:

- actual allocator failure
- object larger than configured capacity
- placement failure after 128 retries

This is policy-driven, not purely allocator-driven.

## 14.2 Capacity is best-effort

The store tries to converge below `capacity_bytes`, but:

- writers can temporarily overshoot
- trim can fail to find victims quickly
- retired but not yet reclaimed memory is not included in `live_bytes`

Therefore capacity is approximate.

## 14.3 Low-traffic reclamation latency

If writes are sparse and `quiescent()` is infrequent:

- batches may sit in `open_batch`
- queued batches may wait until the next periodic or pressure-triggered maintenance

This is safe but can delay memory return.

## 14.4 Maintenance locality skew

One worker can end up running more maintenance and therefore receiving more freed pooled objects into its local caches.

This can create allocator locality skew, but it is usually acceptable because:

- caches eventually drain
- all pooled memory is globally reusable

## 14.5 FIFO queue conservatism

The queue does not skip an ineligible head batch to reclaim younger eligible batches behind it.

This can delay reclamation slightly, but keeps the logic simple and bounded.

## 14.6 Stats reset is counters-only

`kv_store_internal_stats_reset()` resets stats counters only.

It does **not**:

- clear retire queue state
- clear batches
- change epochs
- change `over_capacity`

It exists strictly for measurement hygiene.

---

## 15. Internal Stats and What They Mean

The internal stats API exists so benchmarks can tell whether the intended fast paths are actually happening.

Examples:

- `quiescent_fast_returns / quiescent_calls`
  - high ratio means `kv_store_quiescent()` is mostly staying cheap
- `retire_batches_enqueued` and `retired_nodes_enqueued`
  - show batch formation behavior
- `maintenance_runs`
  - shows how often cleanup pressure forces real work
- `set_overwrite_same_size`
  - confirms the same-size overwrite optimization is being exercised
- `set_allocations`
  - indicates how many `SET`s reached an allocate path
- `set_cas_failures`
  - exposes hot-bucket contention

The benchmark harness resets these counters after preload so benchmark results reflect the actual measured phase rather than setup.

---

## 16. End-to-End Behavior by API

## 16.1 `kv_store_create`

Creates the store by:

1. normalizing config
2. choosing power-of-two shard and bucket geometry
3. initializing globals and stats
4. initializing worker state
5. allocating shard slot arrays
6. placement-constructing each slot atomic to `nullptr`

No nodes are allocated up front.

## 16.2 `kv_store_register_worker`

Marks a worker as participating in:

- get/set access
- quiescent publication
- local allocator caching
- batch retirement ownership

Registration is idempotent for the same worker ID.

## 16.3 `kv_store_get`

Performs:

- read-only bounded probe
- optional lazy expiration unlink
- sampled `refbit` touch
- zero-copy value view return

No allocation on hit.

## 16.4 `kv_store_set`

Performs:

- bounded candidate classification
- lazy allocation
- CAS publish of a fully initialized immutable node
- local accounting delta update
- retirement of replaced/removed nodes
- optional trim

## 16.5 `kv_store_quiescent`

Performs:

- account flush
- quiescent publication
- optional batch flush
- optional maintenance

In the intended steady state, this is usually a cheap fast-return function.

## 16.6 `kv_store_destroy`

Performs synchronous, full cleanup.

It is the only path that guarantees all remaining queued/open retired memory is released immediately.

---

## 17. Comparison to V1

### Same in `v1` and `v2`

- public API
- immutable nodes
- sharded 2-bucket/4-slot table
- lazy expiration
- approximate second-chance eviction
- pooled allocator design

### Different in `v2`

- `quiescent()` is no longer "scan my whole retired list"
- retirement is batched
- reclaim work is done in bounded maintenance passes
- epoch advancement happens during maintenance, not every quiescent call
- `SET` allocates lazily
- same-size overwrites skip trim unless capacity pressure already exists
- internal stats are exported

### Main practical effect

The `v2` design specifically targets the `v1` failure mode where a mixed workload paid too much repeated reclamation overhead on latency-sensitive worker threads.

---

## 18. Future Improvement Directions

The current `v2` implementation is a targeted improvement, not the final endpoint.

Likely future directions if more write scaling is needed:

1. eliminate or reduce full-value copy-on-write cost
2. reduce batch metadata allocation overhead
3. make the retire queue lock-free if queue contention becomes measurable
4. add more precise instrumentation for:
   - open-batch age
   - queued retired bytes over time
   - trim cost
5. consider a read-pin / read-release API if the project wants in-place mutation in the future

Those changes would be materially larger than the current `v2` scope.

---

## 19. Summary

`KvStore v2` keeps the simple, bounded, immutable-node hash-table core of `v1`, but changes the cleanup strategy from:

- "revisit retired nodes repeatedly on every quiescent call"

to:

- "accumulate retired nodes into batches, publish quiescent state cheaply, and reclaim in bounded maintenance passes"

That design:

- preserves read safety for raw `KvValueView`
- keeps the read path fast
- reduces mixed-workload reclaim overhead
- preserves approximate capacity enforcement
- accepts slightly coarser cleanup timing in exchange for better throughput and lower hot-path latency

The result is a more scalable write-adjacent control path while keeping the same external store contract.
