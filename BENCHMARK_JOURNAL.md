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
