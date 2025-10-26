## Wisp Optimization Roadmap

### 1. Performance Target

Drive single-node throughput well beyond the current ~38k msgs/sec microbench without sacrificing correctness, while keeping the codebase simple enough to layer in full NATS semantics later.

### 2. Completed Work

- Converted client registry to `DashMap` and sharded subscriptions to cut lock contention.
- Introduced wildcard-aware dispatch caching that reuses parsed subscriber lists per subject.
- Switched parsing/IO hot paths to zero-allocation byte slices and shared buffers.
- Enforced literal-only subjects for `PUB` to stay protocol-compatible and avoid mismatched deliveries.

### 3. Immediate Opportunities (Pre-Feature Parity)

1. **Bounded Dispatch Cache**  
   - `src/handler.rs`: cap `dispatch_cache` size and expose LRU/TTL eviction so long-lived workloads with high subject churn don’t degrade.
   - Track counters so we can measure cache hit ratio before/after tuning.

2. **Reuse MSG Prefixes**  
   - Store `Arc<[u8]>` for `MSG <subject> <sid>` inside `SubscriberDispatch`.  
   - Update `protocol::format_msg` call sites to append only payload length + body; eliminate repeated prefix formatting.

3. **UNSUB Cleanup & Cache Invalidation**  
   - Implement real removal from shard maps and `sid_map` in `handle_unsub`.  
   - Drop related cache entries immediately to prevent dispatching to stale clients.

4. **Writer Flush Policy**  
   - Replace per-command flush in `src/server.rs` with batched `write_vectored` and idle-based flushing (e.g., flush on timer or when queued bytes exceed threshold).
   - Measure syscall rate before/after to confirm reductions.

5. **Reader Payload Buffer Reuse**  
   - Hoist a reusable `BytesMut` for `PUB` payloads in `ClientConnectionLogic::handle_command_bytes`, mirroring the shared line buffer to reduce allocator churn.

6. **SID Map Storage Tightening**  
   - Swap `String` entries for `Arc<str>` (or interned IDs) so cloning into dispatch structs is cheap and cache-friendly.

7. **Dispatch Vector Pooling**  
   - Keep a `Vec<SubscriberDispatch>` pool or adopt `smallvec` to reuse capacity when cache entries expire frequently.

### 4. Longer-Term Investigations

- **Interest Graph Instrumentation**: add tracing around shard lookups, cache hits, and channel back-pressure to surface hotspots directly from production benches.
- **Permission & Account Hooks**: design interfaces now so future feature work (auth, queue groups, JetStream) lands without rewriting the fast path.
- **Queue & Batch Semantics**: evaluate how to represent queue groups in the dispatch cache without losing per-member fairness; consider per-queue round-robin state.

### 5. Validation Strategy

1. Maintain a repeatable `nats bench` matrix (vary publishers, subjects, payload sizes).  
2. Track per-run metrics: msgs/sec, syscall counts (using `dtrace`/`bcc` where available), memory deltas.  
3. Every optimization should land with a flamegraph or allocation profile comparing before/after.  
4. Keep a regression suite for protocol edge cases (illegal subjects, invalid UTF-8, slow consumers) to ensure correctness while we chase speed.
