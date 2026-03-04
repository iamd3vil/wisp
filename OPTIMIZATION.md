# Wisp Optimization Roadmap

## 1. Current Performance

Wisp exceeds NATS server performance on most workloads:

| Workload | Wisp | NATS | Delta |
|---|---|---|---|
| 64B 1:1 | 5.13M msg/s | 3.84M msg/s | **+33.7%** |
| 1KB 1:1 | 2.09M msg/s | 2.09M msg/s | **parity** |
| 8KB 1:1 | 397K msg/s | 428K msg/s | -7.3% |
| Many subjects (5000) | 3.83M msg/s | 1.48M msg/s | **+158%** |
| Fan-out 100 subs | ~100K pub/s | ~30K pub/s | **~3.3×** |

Remaining gap: 8KB large payloads (-7.3%) due to memcpy cost of copying payload into contiguous write buffer.

---

## 2. Completed Work

These optimizations are already landed in the codebase (see `BENCHMARK_JOURNAL.md` for measurements):

- **Sharded subscription registry.** Split subscriptions across 64 `RwLock<SubMap>` shards keyed by the first subject token via ahash, plus a dedicated wildcard bucket with an `AtomicBool` fast-path.
- **Dispatch caching.** Per-subject dispatch resolution cache with bounded size + LRU-style eviction metadata.
- **Surgical cache invalidation.** Targeted invalidation on SUB/UNSUB/disconnect instead of global cache clears.
- **UNSUB cleanup.** Real shard + `sid_map` removal on UNSUB, with targeted invalidation.
- **Header-prefix caching + no per-dispatch header allocation path.** `MSG <subject> <sid> ` cached per dispatch, writer assembles message from cached prefix + payload.
- **Contiguous write buffer (replaced writev).** Flat `Vec<u8>` write buffer with `write_all` replaces scatter-gather writev with 5-7 IoSlices per message. Eliminated complex `PendingWrite` enum, VecDeque, partial-write tracking. **+33.7% on 64B, +10% on 1KB.**
- **Fast-path PUB parser.** Inline single-pass PUB parser using `memchr` bypasses generic command dispatch for the hot path.
- **Removed `async_trait` boxing.** Replaced with native RPITIT (`impl Future` in traits). Eliminated malloc/free per `handle_pub` call (~14% of CPU).
- **ahash for dispatch cache.** Switched DashMap from SipHash to ahash (~9.5% of reader CPU).
- **Reader payload buffer reuse.** Reused `BytesMut` for PUB payload reads.
- **Borrowed parse args.** `parse_pub_args` / `parse_sub_args` / `parse_unsub_args` return borrows to avoid hot-path `String` allocations.
- **ASCII fast-path parsing.** Non-CONNECT commands use ASCII validation + unchecked UTF-8 conversion.
- **Client-ID merge dedup path.** Replaced sort+dedup path with merge of literal/wildcard subscriber lists.
- **Literal-only PUB subjects.** Wildcard tokens (`*`, `>`) are rejected in PUB commands.
- **SID storage as `Arc<str>`.** Cheap cloning for dispatch-side SID handling.

---

## 3. Immediate Opportunities

These are concrete, scoped changes that can be implemented and benchmarked independently. Each should land with a flamegraph or allocation profile comparing before/after.

### 3.1 Surgical Dispatch Cache Invalidation ✅ Done

**Problem.** `invalidate_dispatch_cache()` calls `self.dispatch_cache.clear()`, nuking every cached dispatch list whenever any subscription change occurs — a single SUB, UNSUB, or client disconnect wipes the entire cache. With 10,000 active subjects and one new subscription, all 10,000 dispatch lists get rebuilt on the next publish.

**Current code** (`handler.rs`):
```rust
fn invalidate_dispatch_cache(&self) {
    self.dispatch_cache.clear(); // Scorched earth
}
```

**Proposed fix.** Replace the blanket `clear()` with targeted invalidation:

- **On SUB for a literal subject** (e.g., `orders.AAPL`): remove only `dispatch_cache["orders.AAPL"]`.
- **On SUB for a wildcard subject** (e.g., `orders.*`): iterate the dispatch cache and remove any key that the wildcard would match. This is more expensive but wildcard subscriptions are infrequent relative to publishes.
- **On UNSUB / disconnect**: remove only the subjects that the disconnecting client was subscribed to. The `sid_map` already has this information — iterate the client's `sid_map` entries and remove matching cache keys.

This preserves the cache for the vast majority of subjects and turns a thundering-herd rebuild into a targeted refresh. For workloads with high subject cardinality (e.g., per-instrument market data with thousands of symbols), this is likely the single largest remaining throughput win.

**Validation.** Add a cache hit/miss counter (`AtomicU64`) and compare hit ratios before/after under a `nats bench` workload with 1000+ subjects and occasional subscribe/unsubscribe churn.

### 3.2 Pre-Cached MSG Header Prefix in SubscriberDispatch ✅ Done (and extended)

**Problem.** In `handle_pub` (handler.rs:288-294), `protocol::format_msg_header()` is called once per subscriber per published message. Each call allocates a new `BytesMut`, writes `MSG <subject> <sid> [reply-to] <size>\r\n`, and freezes it into `Bytes`. For a subject with 1000 subscribers, that's 1000 heap allocations per publish.

The header/payload split is already done (from the `SendMessage` work), but the header is still re-built from scratch for every subscriber. The only part that varies per subscriber is the SID. The `MSG <subject> ` prefix is identical for all subscribers.

**Proposed fix.** Two-level optimization:

1. **Per-dispatch cached prefix.** Store `MSG <subject> <sid> ` as an `Arc<[u8]>` inside `SubscriberDispatch` at cache-build time. At publish time, only append `[reply-to] <size>\r\n` per subscriber — and `<size>\r\n` is the same for all subscribers of a given message, so compute it once.

2. **Optimal path for no reply-to (common case).** When `reply_to` is `None`, the entire header except for the size digits and CRLF is pre-cached. At publish time, the per-subscriber work reduces to: copy cached prefix → append size digits (itoa into a stack buffer) → append `\r\n`. No heap allocation needed — write directly into a `SmallVec<[u8; 128]>` or even use `IoSlice` to avoid the copy entirely.

**Expected impact.** Eliminates the hottest per-message allocation in wide fan-out scenarios. At 3.87M msg/s aggregate, even small per-message savings compound significantly.

### 3.3 UNSUB Cleanup ✅ Done (except max_msgs auto-expiry)

**Problem.** `handle_unsub` currently logs the event and calls `invalidate_dispatch_cache()` but does not actually remove the subscription from the shard maps or `sid_map`. This means:

- Stale subscriptions cause messages to be dispatched to clients that have unsubscribed.
- The dispatch cache rebuilds with the stale subscriber still present.
- `max_msgs` auto-unsubscribe is not implemented.

**Proposed fix.**

1. Look up the subject associated with `(client_id, sid)` via `sid_map`.
2. Remove the entry from the appropriate subscription shard (literal or wildcard).
3. Remove the SID from `sid_map`.
4. Invalidate only the affected subject(s) in the dispatch cache (per 3.1).
5. If `max_msgs` is `Some(n)`, store a decrement counter and trigger cleanup automatically when it hits zero.

### 3.4 Borrowed Subjects Through the Parse → Handle Path ✅ Done

**Problem.** `parse_pub_args` returns `(String, Option<String>, usize)`, allocating a new heap `String` for the subject (and optionally reply-to) on every single PUB command. These strings are derived from `args_str`, which is a `&str` borrow from the line buffer that lives long enough for the entire `handle_command_bytes` call.

```rust
pub fn parse_pub_args(args_str: &str) -> ServerResult<(String, Option<String>, usize)> {
    // ...
    Ok((subj.to_string(), None, size)) // Heap allocation here
}
```

**Proposed fix.** Return borrowed slices:

```rust
pub fn parse_pub_args(args_str: &str) -> ServerResult<(&str, Option<&str>, usize)> {
    // ...
    Ok((subj, None, size)) // Zero-copy
}
```

The handler trait already accepts `subject: &str`, so the only changes are in the parse functions and the call sites. Same applies to `parse_sub_args` and `parse_unsub_args`.

**Impact.** Eliminates 1-2 String allocations per PUB command. At 3.87M msg/s, that's ~4-8M fewer allocations/sec.

### 3.5 Reader Payload Buffer Reuse ✅ Done

**Problem.** Every PUB command allocates a fresh `BytesMut`:

```rust
let mut payload_buffer = BytesMut::with_capacity(size + 2);
payload_buffer.resize(size + 2, 0);
self.reader.read_exact(&mut payload_buffer).await?;
```

**Proposed fix.** Hoist a reusable `BytesMut` onto `ClientConnectionLogic`:

```rust
struct ClientConnectionLogic<H: NatsServerHandler> {
    // ... existing fields ...
    payload_buffer: BytesMut,
}
```

On each PUB, `clear()` + `reserve()` reuses the underlying allocation if capacity is sufficient (which it almost always will be since payload sizes are bounded by `MAX_PAYLOAD` and most messages are far smaller than the high-water mark). After `split_to(size).freeze()`, the `BytesMut` retains its allocation for the next message.

### 3.6 Writer Flush Coalescing ✅ Done (superseded by batched writev queue)

**Problem.** The writer's `try_recv()` drain loop works well under burst load, but under moderate steady-state load, messages often arrive one at a time: `recv().await` → write one message → `try_recv()` finds nothing → `flush()` → syscall. This means one `write(2)` + `flush()` per message.

**Proposed fix.** Add a dual-trigger flush policy:

- **Byte threshold:** flush when buffered bytes exceed 32KB (or configurable). This naturally batches under load.
- **Idle timer:** if no more messages arrive within ~1ms of the last write, flush. This bounds tail latency.

```rust
loop {
    tokio::select! {
        Some(cmd) = write_task_rx.recv() => {
            write_command(&mut writer, cmd).await?;
            // Drain pending
            while let Ok(cmd) = write_task_rx.try_recv() {
                write_command(&mut writer, cmd).await?;
            }
            // Don't flush yet — wait for more or timeout
            flush_deadline = Instant::now() + Duration::from_millis(1);
        }
        _ = tokio::time::sleep_until(flush_deadline) => {
            writer.flush().await?;
        }
    }
}
```

Also consider increasing `BufWriter` capacity from 8KB to 64KB to reduce the frequency of implicit flushes when the buffer fills.

**Validation.** Measure syscall rate using `strace -c` before/after. Target: 5-10x reduction in `write(2)` calls under sustained load.

### 3.7 Bounded Dispatch Cache with LRU Eviction ✅ Done

**Problem.** The `dispatch_cache` grows without bound. A long-lived server with high subject churn (e.g., subjects like `quotes.<symbol>.<timestamp>`) will accumulate stale entries that consume memory and slow down DashMap operations.

**Proposed fix.** Cap the dispatch cache at a configurable size (e.g., 100,000 entries) with LRU eviction. Options:

- `quick_cache::Cache` or `moka::Cache` for concurrent LRU with configurable capacity.
- A TTL (e.g., 30s) for automatic stale entry cleanup.
- Simple hit counter (`AtomicU64`) per entry with periodic sweep.

**Validation.** Expose cache size, hit rate, and eviction count as metrics. Measure memory growth over a 1-hour sustained bench with subject churn.

### 3.8 Dispatch Vector Pooling

**Problem.** When dispatch cache entries are invalidated, the `Vec<SubscriberDispatch>` is dropped and freed. Under subscription churn, this creates allocation thrash as Vecs are repeatedly allocated and freed.

**Proposed fix.** Use `SmallVec<[SubscriberDispatch; 8]>` for the common case where a subject has fewer than 8 subscribers (avoids heap allocation entirely). For larger subscriber counts, maintain a pool via `crossbeam::queue::ArrayQueue` that recycles Vec capacity.

---

## 4. Medium-Term Optimizations

These require more design work or structural changes but can push throughput meaningfully past NATS.

### 4.1 Wildcard SID Resolution: Reverse Index

**Problem.** `resolve_sid_for_subject` (handler.rs:152-166) does an exact lookup first, then falls back to a linear scan over all of a client's subscriptions doing `subject_matches()` on each pattern:

```rust
for entry in subject_map.value().iter() {
    if Self::subject_matches(entry.key(), subject) {
        return Some(Arc::<str>::from(entry.value().as_str()));
    }
}
```

For a client with 500 wildcard subscriptions, this is O(500) per message per subscriber on cache miss.

**Mitigating factor.** `build_dispatches` resolves SID/header-prefix data at cache-build time, so this linear scan only runs on cache miss. With surgical invalidation (3.1), cache misses become rare.

**Proposed fix (for cache-cold and wildcard-heavy workloads).** Build a trie or radix tree per client indexing wildcard patterns by token structure. At dispatch time, walk the trie with concrete subject tokens, matching `*` and `>` nodes. This turns the lookup from O(N) pattern scans to O(depth) where depth is the number of tokens in the subject (typically 2-4).

### 4.2 Avoid UTF-8 Validation on the Hot Path ✅ Done

**Problem.** Every command's arguments are validated as UTF-8 in `server.rs:253`:

```rust
let args_str = std::str::from_utf8(args_bytes).map_err(|_| { ... })?;
```

NATS subjects are ASCII-only. The UTF-8 validation checks for multi-byte sequence validity, which is redundant work.

**Proposed fix.** For PUB and SUB commands, use `unsafe { std::str::from_utf8_unchecked(args_bytes) }` on the hot path. Safety argument: NATS wire protocol allows only ASCII in subjects and SIDs, and the parser validates structure. Alternatively, use `args_bytes.is_ascii()` as a cheaper check. Keep safe `from_utf8` for CONNECT (which carries JSON with potentially non-ASCII fields).

### 4.3 Client ID Deduplication Without Sorting ✅ Done (merge-path variant)

**Problem.** `build_dispatches` collects client IDs from shard + wildcard lookups, then deduplicates via:

```rust
client_ids.sort_unstable();
client_ids.dedup();
```

**Proposed fix.**

- **Bitset:** Client IDs are sequential from an `AtomicU64`. Use a fixed-size bitset (`[u64; 16]` = 1024 clients) for O(1) insert and O(n) iteration.
- **Pre-deduplication:** If `SubMap` guarantees no duplicates within a shard, duplicates only arise between the literal and wildcard shards. A two-pointer merge of two sorted lists is O(n + m) without the full sort.

### 4.4 Write Vectored I/O (writev) ✅ Done (custom batched writer)

**Problem.** The writer does three sequential `write_all` calls for `SendMessage`:

```rust
writer.write_all(&header).await?;
writer.write_all(&payload).await?;
writer.write_all(CRLF).await?;
```

**Proposed fix.** Use `write_vectored` with `IoSlice` to submit all three in one operation. When combined with flush coalescing (3.6), multiple messages can be batched into a single `writev(2)` syscall.

**Caveat.** `BufWriter` may not pass `write_vectored` through. A custom buffered writer or direct buffer management may be needed.

### 4.5 Per-Subject Publish Counters

Add lightweight counters to dispatch cache entries:

```rust
struct CachedDispatch {
    dispatches: Arc<Vec<SubscriberDispatch>>,
    publish_count: AtomicU64,
    last_publish_ns: AtomicU64,
}
```

Use for LRU/LFU eviction, identifying hot subjects for pre-computed header optimization, and alerting on unbounded subscriber growth. `AtomicU64` increment is ~1ns on x86, negligible in the fan-out path.

---

## 5. Longer-Term Investigations

### 5.1 io_uring for Network I/O

Replace tokio's epoll-based reactor with io_uring via `tokio-uring`. io_uring can batch I/O operations into a single `io_uring_enter` syscall and supports zero-copy sends (`IORING_OP_SEND_ZC`). Pursue after measuring that syscall overhead is still a significant fraction of CPU time after flush coalescing (3.6) and write vectored (4.4).

### 5.2 Shared Memory Fan-Out for Co-located Clients

For subscribers on the same machine, a shared memory transport eliminates the kernel network stack entirely. Write the message once into an mmap'd ring buffer; subscriber processes read from it with their own cursors (Disruptor pattern). This is how Aeron's IPC channel achieves 10-50M msg/s. Would be a separate transport alongside TCP.

### 5.3 Interest Graph Instrumentation

Add tracing spans around the critical dispatch path: shard lookup latency, cache hit/miss ratio, channel send latency, writer flush latency, per-client backpressure events. Gate with a `WISP_TRACE=dispatch` env var so there's zero overhead unless explicitly enabled.

### 5.4 Queue Groups and Fair Dispatch

The current cache stores a flat `Vec<SubscriberDispatch>`. Queue groups need different dispatch logic where only one member receives each message. Options:

- **Inline round-robin:** `AtomicUsize` index per queue group, lock-free but can skew under contention.
- **Weighted random:** Select member weighted by remaining channel capacity, providing natural backpressure-aware load balancing.
- **Separate dispatch path:** Queue group subjects bypass regular fan-out; cache stores regular and queue-group subscribers separately.

### 5.5 Permission and Account Hooks

Design trait interfaces now for auth, permissions, and account isolation so future work doesn't rewrite the fast path:

- `AuthHandler` trait called once at CONNECT.
- `PermissionCheck` trait on SUB and PUB. The PUB check must be fast enough for every publish — consider a bitset or bloom filter for allowed subjects.
- Account-level subject namespacing with explicit cross-account delivery.

### 5.6 JetStream-Style Persistence

If durable messaging is needed, the persistence layer should subscribe like a normal client, receive messages through standard dispatch, and write to a WAL (`redb`, `sled`, or raw file) in a dedicated task. The publish path remains unaware of persistence. Acks flow through the standard reply-to mechanism.

---

## 6. Validation Strategy

### 6.1 Benchmark Matrix

Maintain a repeatable `nats bench` suite:

| Scenario | Publishers | Subscribers | Subjects | Payload | Current | Target |
|---|---|---|---|---|---|---|
| Single fan-out | 1 | 1 | 1 | 64B | 3.87M msg/s | 4.5M msg/s |
| Single fan-out | 1 | 1 | 1 | 1KB | 1.60M msg/s | 2.0M msg/s |
| Wide fan-out | 1 | 100 | 1 | 128B | TBD | 500k msg/s |
| Many subjects | 10 | 10 each | 1000 | 128B | TBD | 1M msg/s agg |
| Large payload | 1 | 10 | 1 | 8KB | TBD | 500k msg/s |
| Wildcard heavy | 10 | 50 (wildcard) | 1000 | 128B | TBD | 400k msg/s |
| Sub churn | 1 | 10 (cycling) | 100 | 128B | TBD | Cache hit >95% |

### 6.2 Per-Run Metrics

For every optimization, capture:

- **msgs/sec** and **fan-out ratio** (from `nats bench`)
- **p50/p99/p999 latency** (from `nats bench --latency`)
- **syscall counts** (via `strace -c` or `bpftrace`)
- **allocations/sec** (via `heaptrack` or `dhat`)
- **memory high-water mark** (via `/proc/<pid>/status` VmHWM)
- **CPU flamegraph** (via `perf record -g` + `flamegraph.pl`)

### 6.3 Regression Suite

Protocol conformance tests covering:

- Standard PUB/SUB/UNSUB/PING/PONG flows.
- Wildcard subscriptions: `*`, `>`, `foo.*`, `foo.>`, `*.bar`, `foo.*.baz`.
- Edge cases: empty payload, MAX_PAYLOAD boundary, zero-length subject (should error), invalid UTF-8 in CONNECT JSON.
- Slow consumer behavior: subscriber that reads slowly while others are fast.
- Rapid subscribe/unsubscribe cycles to stress cache invalidation.
- Connection drop during PUB payload read (partial payload).
- UNSUB with `max_msgs` auto-expiry.

### 6.4 Profiling Workflow

Every optimization PR should include:

1. Baseline flamegraph + allocation profile on the most relevant benchmark scenario.
2. The same profiles after the change.
3. Written summary of what moved and by how much.
4. `nats bench` numbers for at least Single fan-out (64B and 1KB).

---

## 7. Implementation Priority

Ordered by expected impact-to-effort ratio, given the current 3.87M msg/s baseline:

| Priority | Optimization | Section | Effort | Expected Impact | Status |
|---|---|---|---|---|---|
| P0 | Surgical cache invalidation | 3.1 | Medium | High — eliminates thundering rebuild under churn | ✅ Done |
| P0 | UNSUB cleanup | 3.3 | Medium | Correctness — stale dispatch prevention | ⚠️ Mostly done (`max_msgs` auto-expiry pending) |
| P0 | Borrowed subjects in parse | 3.4 | Low | Medium — ~4-8M fewer allocs/sec | ✅ Done |
| P1 | Pre-cached MSG header prefix | 3.2 | Medium | High — eliminates per-subscriber alloc in fan-out | ✅ Done (and extended to no per-dispatch header alloc path) |
| P1 | Reader payload buffer reuse | 3.5 | Low | Medium — removes per-PUB allocation | ✅ Done |
| P1 | Writer flush coalescing | 3.6 | Medium | Medium-High — reduces syscall rate | ✅ Done (evolved into custom batched writer) |
| P2 | Bounded dispatch cache / LRU | 3.7 | Medium | Memory safety under churn | ✅ Done |
| P2 | Dispatch vector pooling | 3.8 | Low | Low-Medium — reduces alloc churn | ⏳ Not done |
| P2 | Write vectored I/O | 4.4 | Medium | Medium — further syscall reduction | ✅ Done (custom batched writev queue) |
| P3 | Wildcard SID reverse index | 4.1 | High | High for wildcard-heavy workloads | ⏳ Not done |
| P3 | Skip UTF-8 validation | 4.2 | Low | Low — small constant factor | ✅ Done |
| P3 | Client ID dedup without sort | 4.3 | Low | Low — small constant factor | ✅ Done (merge-path variant) |
| P3 | Per-subject publish counters | 4.5 | Low | Observability, enables smarter eviction | ⏳ Not done |
