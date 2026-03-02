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

---

## 2026-03-02 — ASCII-only fast path + merge dedup for dispatch client IDs

Optimization scope:
- **4.2** Avoid generic UTF-8 validation on hot commands:
  - Keep strict UTF-8 parsing for `CONNECT` args.
  - For non-CONNECT commands, validate `is_ascii()` and use `from_utf8_unchecked`.
- **4.3 (partial)** Replace `sort_unstable + dedup` on merged subscriber IDs with a direct merge of sorted literal/wildcard subscriber lists.

### Benchmark: wide fan-out (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber-agg msg/s):
- 33,990 / 1,709,682
- 34,822 / 1,688,467
- 35,379 / 1,732,444

After (publisher / subscriber-agg msg/s):
- 34,106 / 1,742,168
- 35,146 / 1,697,615
- 34,411 / 1,668,736

Median delta:
- Publisher: **34,822 -> 34,411** (**-1.2%**)
- Subscriber aggregated: **1,709,682 -> 1,697,615** (**-0.7%**)

### Benchmark: non-fanout many-subjects
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Before (3 runs, publisher / subscriber msg/s):
- 1,115,626 / 1,126,571
- 1,060,609 / 1,061,934
- 1,179,066 / 1,179,044

After (5 runs, publisher / subscriber msg/s):
- 941,259 / 942,625
- 1,117,479 / 1,128,954
- 987,608 / 993,815
- 1,126,634 / 1,129,361
- 1,143,858 / 1,146,101

Median delta:
- Publisher: **1,115,626 -> 1,117,479** (**+0.2%**)
- Subscriber: **1,126,571 -> 1,128,954** (**+0.2%**)

### Notes
- Net throughput impact is near-neutral in these runs (tiny deltas, high variance).
- Protocol behavior check: non-ASCII non-CONNECT args now return `-ERR` consistently.

---

## 2026-03-02 — Batched writev queue (without BufWriter)

Optimization scope:
- **4.4** Move writer path from `BufWriter + write_all` to batched vectored writes over socket write half.
- Maintain explicit flush policy in writer task:
  - `FLUSH_THRESHOLD_BYTES = 64KB`
  - `FLUSH_IDLE_MS = 1ms`
- Queue pending writes as structured chunks and flush via `write_vectored` with partial-write handling.
- Use larger vectored batch cap (`MAX_IO_SLICES = 1024`).

### Benchmark: wide fan-out (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=50 TOPICS=1 MSGS=30000 SIZE=128 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber-agg msg/s):
- 50,948 / 2,430,933
- 49,285 / 2,493,045
- 50,089 / 2,520,881

After (publisher / subscriber-agg msg/s):
- 65,925 / 3,305,228
- 65,614 / 3,322,424
- 64,900 / 3,316,692

Median delta:
- Publisher: **50,089 -> 65,614** (**+31.0%**)
- Subscriber aggregated: **2,493,045 -> 3,316,692** (**+33.0%**)

### Benchmark: non-fanout many-subjects (3 runs)
Command:
- `SERVER_URL=nats://127.0.0.1:4222 START_SERVER=1 PUBLISHERS=1 SUBSCRIBERS=1 TOPICS=5000 MSGS=2000000 SIZE=64 BUILD_RELEASE=0 ./benchmark.sh`

Before (publisher / subscriber msg/s):
- 1,797,616 / 1,802,078
- 1,713,600 / 1,717,508
- 1,784,167 / 1,784,934

After (publisher / subscriber msg/s):
- 1,601,232 / 1,603,900
- 1,710,309 / 1,719,351
- 1,766,405 / 1,771,934

Median delta:
- Publisher: **1,784,167 -> 1,710,309** (**-4.1%**)
- Subscriber: **1,784,934 -> 1,719,351** (**-3.7%**)

### Notes
- This version strongly improves wide fan-out throughput, but regresses single-subscriber many-subject throughput.
- Workload-driven tradeoff: keep as-is if wide fan-out is primary target; otherwise consider a hybrid writer path.

---

## 2026-03-02 — Wisp vs NATS (wide fan-out, 5-run comparison)

Workload:
- `1 pub / 100 sub / 1 topic / 30000 msgs / 128B`

Method:
- 5 runs each for `nats-server` and `wisp`.
- Metrics from `nats bench` output (publisher stats + subscriber aggregated stats).
- Approximate server CPU/RSS sampled via `ps -p <pid> -o %cpu,rss` every 100ms during each run.
- Raw data saved to: `profiling/compare/wide100_5run_compare.json`.

NATS runs (pub / sub-agg / avg us / p99 us / p99.9 us):
- 31,950 / 3,185,057 / 8.80 / 0.23 / 8.85
- 31,541 / 3,135,388 / 8.58 / 0.28 / 7.32
- 31,336 / 3,115,821 / 8.98 / 0.20 / 7.96
- 30,354 / 3,027,699 / 9.35 / 0.27 / 10.68
- 29,656 / 2,953,378 / 9.83 / 3.69 / 6.49

Wisp runs (pub / sub-agg / avg us / p99 us / p99.9 us):
- 31,046 / 3,142,669 / 9.63 / 0.37 / 12.59
- 31,217 / 3,170,163 / 9.23 / 0.61 / 14.96
- 30,144 / 3,060,033 / 8.65 / 0.71 / 19.18
- 30,498 / 3,085,589 / 9.25 / 0.27 / 6.76
- 30,768 / 3,089,843 / 9.29 / 0.21 / 6.16

Median summary:
- NATS:
  - pub: **31,336 msg/s**
  - sub-agg: **3,115,821 msg/s**
  - avg: **8.98 us**
  - p99: **0.27 us**
  - p99.9: **7.96 us**
  - CPU avg: **43.1%**
  - RSS max: **27,208 KB**
- Wisp:
  - pub: **30,768 msg/s**
  - sub-agg: **3,089,843 msg/s**
  - avg: **9.25 us**
  - p99: **0.37 us**
  - p99.9: **12.59 us**
  - CPU avg: **62.8%**
  - RSS max: **26,604 KB**

Delta (Wisp vs NATS, median):
- pub msgs/sec: **-1.81%**
- sub-agg msgs/sec: **-0.83%**
- avg latency: **+3.01%**
- p99 latency: **+37.04%**
- p99.9 latency: **+58.17%**
- CPU avg: **+45.66%**
- RSS max: **-2.22%**

Interpretation:
- In this stricter 5-run wide fan-out comparison, Wisp is very close but currently trails NATS on throughput and tail latency, while using more CPU.
- Memory footprint is slightly lower on Wisp.

---

## 2026-03-02 — Writer tuning sweep (flush threshold / idle / slice cap)

Change:
- Made writer batching knobs runtime-configurable via env vars:
  - `WISP_WRITEV_FLUSH_THRESHOLD_BYTES` (default `65536`)
  - `WISP_WRITEV_FLUSH_IDLE_US` (default `1000`)
  - `WISP_WRITEV_MAX_IO_SLICES` (default `1024`)

Sweep workload:
- `1 pub / 100 sub / 1 topic / 30000 msgs / 128B`
- 3 runs per config (median shown)

Results:
- `64k / 1000us / 1024` (baseline):
  - pub **31,479**, sub-agg **3,168,371**, avg **9.04us**, p99 **0.30us**, p99.9 **7.35us**
- `32k / 500us / 512`:
  - pub **29,865**, sub-agg **2,997,521**, avg **9.12us**, p99 **0.43us**, p99.9 **14.17us**
- `32k / 200us / 512`:
  - pub **19,839**, sub-agg **1,957,748**, avg **10.13us**, p99 **0.29us**, p99.9 **8.54us**
- `16k / 200us / 256`:
  - pub **21,778**, sub-agg **2,137,040**, avg **9.73us**, p99 **0.41us**, p99.9 **11.81us**
- `16k / 500us / 256`:
  - pub **18,788**, sub-agg **1,876,259**, avg **9.75us**, p99 **0.31us**, p99.9 **8.18us**
- `64k / 200us / 1024`:
  - pub **9,327**, sub-agg **935,399**, avg **21.31us**, p99 **0.72us**, p99.9 **13.66us**

Observation:
- Baseline remained best for throughput in this sweep.
- More aggressive timer settings (especially `200us`) caused substantial regressions.

Focused follow-up (5 runs):
- Baseline `64k/1000us/1024` median:
  - pub **33,056**, sub-agg **3,381,282**, avg **8.80us**, p99 **0.68us**, p99.9 **16.52us**
- `64k/1000us/512` median:
  - pub **32,667**, sub-agg **3,267,172**, avg **8.54us**, p99 **0.39us**, p99.9 **11.32us**

Tradeoff:
- `64k/1000us/512` improves tail latency noticeably vs baseline (`p99.9: 16.52 -> 11.32us`) with moderate throughput cost (`sub-agg: 3.381M -> 3.267M`, ~3.4%).

---

## 2026-03-02 — Eliminate per-dispatch header allocations (structured SendMessage parts)

Optimization scope:
- Replace per-subscriber prebuilt `header: Bytes` in `ServerCommand::SendMessage` with structured parts:
  - `header_prefix` (`MSG <subject> <sid> `, cached in dispatch)
  - optional `reply_to`
  - `payload_len`
  - `payload`
- Writer now formats payload size into a stack buffer (`[u8; 20]`) once per queued message and emits all parts via batched `write_vectored`.
- Removes `format_msg_header_no_reply/with_reply` allocations from hot pub dispatch path.

### Wide fan-out benchmark (5 runs)
Workload:
- `1 pub / 100 sub / 1 topic / 30000 msgs / 128B`

Wisp after change (runs):
- 75,476 / 5,983,971 / 3.80us / 0.27us / 9.51us
- 76,529 / 6,240,793 / 3.96us / 0.33us / 10.62us
- 75,424 / 5,827,403 / 3.71us / 0.35us / 10.08us
- 75,589 / 5,561,797 / 3.55us / 0.23us / 7.67us
- 72,468 / 6,213,326 / 4.00us / 0.40us / 11.24us

Medians (Wisp after):
- pub: **75,476 msg/s**
- sub-agg: **5,983,971 msg/s**
- avg: **3.80us**
- p99: **0.33us**
- p99.9: **10.08us**

Compared to prior Wisp median in wide fan-out section (`30,768 / 3,089,843 / 9.25us / 0.37us / 12.59us`):
- pub: **+145.3%**
- sub-agg: **+93.7%**
- avg latency: **-58.9%**
- p99 latency: **-10.8%**
- p99.9 latency: **-19.9%**

NATS comparison (same workload, 5 runs, medians):
- NATS medians: **30,553 msg/s**, **3,041,035 msg/s**, avg **8.43us**, p99 **0.37us**, p99.9 **11.80us**
- Delta (Wisp vs NATS):
  - pub: **+147.0%**
  - sub-agg: **+96.8%**
  - avg latency: **-54.9%**
  - p99 latency: **-10.8%**
  - p99.9 latency: **-14.6%**

### Non-fanout many-subject benchmark (5 runs)
Workload:
- `1 pub / 1 sub / 5000 topics / 2,000,000 msgs / 64B`

Wisp after medians:
- pub: **1,655,234 msg/s**
- sub: **1,656,961 msg/s**
- avg: **0.36us**
- p99: **0.25us**
- p99.9: **7.24us**

Compared to prior Wisp median (`1,710,309 / 1,719,351`):
- pub: **-3.2%**
- sub: **-3.6%**

Interpretation:
- This change is a major win for wide fan-out throughput and latency.
- It slightly regresses the 1-subscriber many-subject case.
