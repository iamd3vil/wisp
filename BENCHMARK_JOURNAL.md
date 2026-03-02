# Wisp Benchmark Journal

## 2026-03-02 — Surgical dispatch cache invalidation + UNSUB cleanup

Optimization scope:
- **3.1** Surgical dispatch cache invalidation (targeted instead of `dispatch_cache.clear()`).
- **3.3** UNSUB cleanup (remove sub from shard + SID map + targeted cache invalidation).

Test command (Wisp):
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Churn test setup:
- Same benchmark command with `START_SERVER=0` against a manually started Wisp instance.
- Parallel churn loop continuously does `CONNECT + SUB churn.literal + UNSUB 1 + PING` over short-lived connections.

### Raw results

| Scenario | Before | After | Delta |
|---|---:|---:|---:|
| No churn | 896,519 msg/s | 888,346 msg/s | -0.9% |
| With churn | 856,622 msg/s | 898,691 msg/s | +4.9% |

Additional post-change no-churn run:
- 875,461 msg/s (expected run-to-run variance on this setup).

### Notes
- The main goal of this change was churn resilience, not peak no-churn throughput.
- Churn penalty observed before (~4.5%) is effectively removed in this benchmark shape.

---

## 2026-03-02 — Borrowed parse arguments (parse → handle path)

Optimization scope:
- **3.4** Return borrowed slices from parser helpers:
  - `parse_pub_args`: `(&str, Option<&str>, usize)`
  - `parse_sub_args`: `(&str, Option<&str>, &str)`
  - `parse_unsub_args`: `(&str, Option<u64>)`
- Updated `server.rs` call sites to pass borrows directly to handlers.

### Raw results

| Scenario | Before (after 3.1+3.3) | After (with 3.4) | Delta |
|---|---:|---:|---:|
| No churn | 888,346 msg/s | 879,787 msg/s | -1.0% |
| With churn | 898,691 msg/s | 999,101 msg/s | +11.2% |

### Multi-run verification (5 runs each)

No-churn runs (publisher msg/s):
- 882,593
- 970,824
- 938,856
- 883,790
- 912,410

No-churn summary (publisher):
- Median: **912,410 msg/s**
- Mean: 917,694 msg/s
- Min/Max: 882,593 / 970,824

Churn setup for multi-run:
- Manually started Wisp (`START_SERVER=0` for benchmark command).
- Separate persistent churn client keeps one connection open and loops:
  `SUB churn.literal <sid> -> UNSUB <sid> -> PING`, with 1ms sleep.

Churn runs (publisher msg/s):
- 899,846
- 941,083
- 840,986
- 819,881
- 880,081

Churn summary (publisher):
- Median: **880,081 msg/s**
- Mean: 876,375 msg/s
- Min/Max: 819,881 / 941,083

### Notes
- Multi-run data confirms noticeable run-to-run variance on this machine.
- Under the persistent churn harness, throughput is ~3.5% lower than no-churn at the median.
- Expected impact of 3.4 remains allocation reduction first; throughput deltas are secondary and noisy without allocation profiling.

---

## 2026-03-02 — Pre-cached MSG header prefix in dispatch cache

Optimization scope:
- **3.2** Cache `MSG <subject> <sid> ` as `Bytes` in `SubscriberDispatch` at dispatch-cache build time.
- In `handle_pub`, compute payload size string once (`itoa`) and assemble headers from cached prefix:
  - no-reply fast path: `prefix + size + CRLF`
  - reply path: `prefix + reply_to + space + size + CRLF`

Benchmark scenario (wide fan-out):
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher/subscriber-agg msg/s):
- run1: 18,877 / 944,200
- run2: 19,019 / 928,603
- run3: 19,329 / 942,232

After (publisher/subscriber-agg msg/s):
- run1: 33,972 / 1,630,238
- run2: 32,311 / 1,550,252
- run3: 33,729 / 1,615,882

Summary:
- Publisher median: **19,019 -> 33,729** (**+77.3%**)
- Subscriber aggregated median: **942,232 -> 1,615,882** (**+71.5%**)

Sanity check (non-fanout workload):
- `1 pub / 1 sub / 5000 topics / 2,000,000 msgs / 64B`
- Post-change run: **958,007 msg/s** publisher, **956,782 msg/s** subscriber.

---

## 2026-03-02 — Writer flush coalescing + larger BufWriter

Optimization scope:
- **3.6** Writer dual-trigger flushing:
  - `FLUSH_THRESHOLD_BYTES = 32KB`
  - `FLUSH_IDLE_MS = 1ms`
- Increase writer buffer capacity from **8KB -> 64KB**.
- Writer now tracks buffered byte count and flushes:
  - on threshold (throughput)
  - on idle timer (latency bound)
  - on shutdown/channel close (correctness)

### Benchmark: wide fan-out (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber-agg msg/s):
- 32,450 / 1,558,261
- 33,311 / 1,597,583
- 32,252 / 1,546,583

After (publisher / subscriber-agg msg/s):
- 33,652 / 1,721,808
- 34,985 / 1,714,063
- 32,892 / 1,582,829

Median delta:
- Publisher: **32,450 -> 33,652** (**+3.7%**)
- Subscriber aggregated: **1,558,261 -> 1,714,063** (**+10.0%**)

### Benchmark: non-fanout many-subjects (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber msg/s):
- 1,010,504 / 1,009,279
- 807,148 / 806,117
- 960,727 / 961,197

After (publisher / subscriber msg/s):
- 980,778 / 992,004
- 971,253 / 973,597
- 868,506 / 869,620

Median delta:
- Publisher: **960,727 -> 971,253** (**+1.1%**)
- Subscriber: **961,197 -> 973,597** (**+1.3%**)

---

## 2026-03-02 — Reader payload buffer reuse (PUB read path)

Optimization scope:
- **3.5** Hoist `BytesMut` payload read buffer onto `ClientConnectionLogic` and reuse across PUB commands.
- Replace per-PUB local allocation with:
  - `clear()`
  - `reserve(size + 2)`
  - `resize(size + 2, 0)`
  - `read_exact(...)`

### Benchmark: wide fan-out (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber-agg msg/s):
- 33,652 / 1,721,808
- 34,985 / 1,714,063
- 32,892 / 1,582,829

After (publisher / subscriber-agg msg/s):
- 35,100 / 1,722,304
- 33,516 / 1,619,411
- 34,562 / 1,685,009

Median delta:
- Publisher: **33,652 -> 34,562** (**+2.7%**)
- Subscriber aggregated: **1,714,063 -> 1,685,009** (**-1.7%**)

### Benchmark: non-fanout many-subjects (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber msg/s):
- 980,778 / 992,004
- 971,253 / 973,597
- 868,506 / 869,620

After (publisher / subscriber msg/s):
- 1,074,291 / 1,075,723
- 1,122,518 / 1,125,808
- 972,466 / 974,474

Median delta:
- Publisher: **971,253 -> 1,074,291** (**+10.6%**)
- Subscriber: **973,597 -> 1,075,723** (**+10.5%**)

### Notes
- Results are noisy in fan-out subscriber aggregate throughput; publisher side trends positive.
- Non-fanout scenario shows a strong positive shift across these runs.

---

## 2026-03-02 — Bounded dispatch cache with LRU-style eviction

Optimization scope:
- **3.7** Bound `dispatch_cache` size and evict least-recently-used style entries when over capacity.
- Reworked cache value to store:
  - `dispatches: Arc<Vec<SubscriberDispatch>>`
  - `last_access_tick: AtomicU64`
- Added access tick updates on cache hit and insertion-time eviction pass.
- Capacity is configurable with env var:
  - `WISP_DISPATCH_CACHE_CAPACITY` (default: `100000`).

### Benchmark: wide fan-out (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber-agg msg/s):
- 35,100 / 1,722,304
- 33,516 / 1,619,411
- 34,562 / 1,685,009

After (publisher / subscriber-agg msg/s):
- 35,253 / 1,707,313
- 35,225 / 1,696,927
- 32,946 / 1,677,228

Median delta:
- Publisher: **34,562 -> 35,225** (**+1.9%**)
- Subscriber aggregated: **1,685,009 -> 1,696,927** (**+0.7%**)

### Benchmark: non-fanout many-subjects (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber msg/s):
- 1,074,291 / 1,075,723
- 1,122,518 / 1,125,808
- 972,466 / 974,474

After (publisher / subscriber msg/s):
- 1,092,784 / 1,094,176
- 1,079,488 / 1,081,630
- 1,150,961 / 1,153,948

Median delta:
- Publisher: **1,074,291 -> 1,092,784** (**+1.7%**)
- Subscriber: **1,075,723 -> 1,094,176** (**+1.7%**)

### Capacity sanity check
- Default cap (`100000`) with `5000` topics and `300000` msgs:
  - Publisher: **600,930 msg/s**
- Small cap (`WISP_DISPATCH_CACHE_CAPACITY=100`) on same workload:
  - Publisher: **137,927 msg/s**

Interpretation:
- Bounded cache works as intended and prevents unbounded growth.
- Very small caps materially hurt throughput in high-cardinality workloads due to frequent rebuilds.
