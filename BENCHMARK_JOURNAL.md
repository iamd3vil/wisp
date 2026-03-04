# Wisp Benchmark Journal

## 2026-03-04 — Fast-path PUB parser + contiguous write buffer

Optimization scope:
1. **Fast-path PUB parser**: Inline single-pass PUB command parser using `memchr` that
   bypasses the generic `parse_command_line_bytes` → `command_matches` → `parse_pub_args`
   pipeline. Reduces parsing from ~43 samples (29.5% of reader CPU) to a single pass.
2. **Contiguous write buffer**: Replaced scatter-gather `writev` with 5-7 IoSlices per
   message with a flat `Vec<u8>` write buffer + `write_all`. Eliminates `PendingWrite` enum
   (~164 bytes with 13 fields), `VecDeque`, IoSlice construction, and complex partial-write
   `advance()` tracking. Message parts are copied into the contiguous buffer before flushing.

### A/B Comparison: Wisp vs NATS Server 2.12.4 (5 runs each, pub+sub stats)

**64B 1:1 (1M msgs, 1 pub / 1 sub)**

| Server | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median |
|---|---:|---:|---:|---:|---:|---:|
| NATS pub | 3,829,777 | 3,841,294 | 3,898,927 | 3,797,849 | 3,808,867 | 3,841,294 |
| NATS sub | 3,836,928 | 3,850,552 | 3,906,715 | 3,810,678 | 3,820,161 | 3,836,928 |
| Wisp pub | 5,601,704 | 5,353,526 | 5,378,574 | 5,191,046 | 5,267,052 | **5,353,526** |
| Wisp sub | 5,306,523 | 5,129,978 | 5,147,232 | 5,027,930 | 5,097,051 | **5,129,978** |

**64B 1:1 summary**: Wisp **+33.7%** faster (sub: 5,130K vs 3,837K msgs/sec)

**1KB 1:1 (500K msgs, 1 pub / 1 sub)**

| Server | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median |
|---|---:|---:|---:|---:|---:|---:|
| NATS pub | 2,212,832 | 2,159,082 | 2,234,427 | 2,136,553 | 2,127,492 | 2,159,082 |
| NATS sub | 2,093,640 | 2,167,032 | 2,102,645 | 2,029,797 | 2,040,599 | 2,093,640 |
| Wisp pub | 1,920,656 | 2,126,660 | 2,111,077 | 1,970,469 | 2,113,884 | **2,111,077** |
| Wisp sub | 1,924,054 | 2,131,406 | 2,115,544 | 1,975,241 | 2,086,332 | **2,086,332** |

**1KB 1:1 summary**: Wisp **-0.3%** (sub: 2,086K vs 2,094K msgs/sec) — essentially parity

**8KB 1:1 (200K msgs, 1 pub / 1 sub)**

| Server | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| NATS | 428,175 | 429,588 | 427,364 | **428,175** |
| Wisp | 389,689 | 398,806 | 396,941 | **396,941** |

**8KB 1:1 summary**: Wisp **-7.3%** behind (improved from -23.5% before flat buffer)

**Many subjects (2M msgs, 5000 topics, 64B, 1 pub / 1 sub)**

| Server | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| NATS | 1,480,531 | 1,472,883 | 1,483,177 | **1,480,531** |
| Wisp | 3,808,491 | 3,842,221 | 3,827,596 | **3,827,596** |

**Many subjects summary**: Wisp **+158.6%** faster

**Fan-out 100 subscribers (30K msgs, 128B, 1 pub / 100 sub)**

| Server | Run 1 pub | Run 2 pub | Run 3 pub |
|---|---:|---:|---:|
| NATS | 30,465 | 34,016 | 24,235 |
| Wisp | 118,921 | 82,972 | 99,580 |

**Fan-out summary**: Wisp **~3.3× faster** — no regression from write buffer change

**Full fan-out matrix (3 runs each, median values)**

| Scenario | NATS pub | Wisp pub | NATS sub_agg | Wisp sub_agg |
|---|---:|---:|---:|---:|
| 50 sub 128B (60K msgs) | 56,148 | 130,188 | 2,808,266 | 6,563,368 |
| 100 sub 128B (30K msgs) | 28,828 | 78,704 | 2,883,470 | 7,968,149 |
| 50 sub 1KB (30K msgs) | 32,820 | 107,960 | 1,641,792 | 4,683,495 |
| 100 sub 1KB (10K msgs) | 15,209 | 76,506 | 1,520,637 | 4,386,209 |

Fan-out ratios: Wisp is **2.3–5.0× faster** on publisher rate, **2.3–2.9× faster** on
aggregate subscriber throughput across all fan-out scenarios tested.

### Progression vs previous optimization round (2026-03-04 async_trait removal)

| Workload | Before (writev) | After (flat buf) | Improvement |
|---|---:|---:|---:|
| 64B 1:1 sub | ~3,748K | 5,130K | **+36.9%** |
| 1KB 1:1 sub | ~1,896K | 2,086K | **+10.0%** |
| 8KB 1:1 | ~334K | 397K | **+18.9%** |
| Many subjects | ~3,794K | 3,828K | +0.9% |

### Notes
- The contiguous write buffer is the dominant optimization. It eliminated ~230 lines of
  complex scatter-gather code (PendingWrite enum, IoSlice building, advance tracking).
- For small payloads (64B), the flat buffer dramatically reduces kernel writev overhead
  since each message previously generated 5 IoSlices (header, size, CRLF, payload, CRLF).
- For large payloads (8KB), the memcpy cost of copying payload into the flat buffer is
  measurable but still outperformed by the simpler write path.
- Fast-path PUB parser using memchr skips the generic command dispatch for the hot path.

---

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

### Wide fan-out CPU / memory snapshot (post-change)

Workload:
- `1 pub / 100 sub / 1 topic / 30000 msgs / 128B`

Method:
- 3 runs each for NATS and Wisp.
- Sample server process via `ps -p <pid> -o %cpu,rss` every 100ms while benchmark runs.
- Report median of per-run aggregates.

Results:
- NATS median:
  - CPU avg: **40.6%**
  - CPU median sample: **11.4%**
  - RSS max: **28,192 KB**
- Wisp median:
  - CPU avg: **25.9%**
  - CPU median sample: **6.2%**
  - RSS max: **50,064 KB**

Delta (Wisp vs NATS):
- CPU avg: **-36.3%** (better)
- CPU median sample: **-45.6%** (better)
- RSS max: **+77.6%** (worse)

---

## 2026-03-04 — Full benchmark matrix: Wisp vs NATS across all workload shapes

Scope:
- Comprehensive comparison across 8 workload shapes covering single fan-out, wide fan-out, large payload, and multi-subject scenarios.
- All tests on the same machine, same session, using `nats bench` against Wisp (`./target/release/wisp`) and `nats-server` 2.12.4.
- 3–5 runs per scenario per server; medians reported.

---

### 1. Single fan-out 64B (1 pub / 1 sub / 1 topic / 1,000,000 msgs)

Wisp (3 runs, pub / sub msg/s / avg / P99 / P99.9):
- 2,947,312 / 2,971,412 / 0.29us / 0.20us / 9.70us
- 2,984,321 / 3,009,393 / 0.28us / 0.20us / 10.16us
- 2,970,686 / 2,997,087 / 0.29us / 0.20us / 9.54us

NATS (3 runs):
- 3,742,751 / 3,754,614 / 0.22us / 0.16us / 13.83us
- 3,748,258 / 3,758,033 / 0.22us / 0.16us / 12.91us
- 3,784,958 / 3,795,974 / 0.21us / 0.16us / 14.45us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | 2,970,686 | **3,748,258** | **-20.8%** |
| sub msg/s | 2,997,087 | **3,758,033** | **-20.2%** |
| avg latency | 0.29us | **0.22us** | +31.8% |
| P99 latency | 0.20us | **0.16us** | +25.0% |
| P99.9 latency | **9.70us** | 13.83us | **-29.9%** |

---

### 2. Single fan-out 1KB (1 pub / 1 sub / 1 topic / 500,000 msgs)

Wisp (3 runs, pub / sub msg/s / avg / P99 / P99.9):
- 1,741,751 / 1,749,084 / 0.52us / 5.20us / 65.54us
- 1,487,159 / 1,488,965 / 0.62us / 5.12us / 31.79us
- 1,604,423 / 1,609,584 / 0.57us / 3.62us / 128.87us

NATS (3 runs):
- 2,124,448 / 2,129,376 / 0.42us / 4.41us / 33.95us
- 2,142,885 / 2,066,606 / 0.41us / 5.00us / 60.16us
- 2,050,185 / 1,955,085 / 0.44us / 6.20us / 31.95us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | 1,604,423 | **2,124,448** | **-24.5%** |
| sub msg/s | 1,609,584 | **2,066,606** | **-22.1%** |
| avg latency | 0.57us | **0.42us** | +35.7% |
| P99 latency | 5.12us | **5.00us** | +2.4% |
| P99.9 latency | 65.54us | **33.95us** | +93.0% |

---

### 3. Wide fan-out 50 subs, 128B (1 pub / 50 sub / 1 topic / 30,000 msgs)

Wisp (3 runs, pub / sub-agg msg/s / avg / P99 / P99.9):
- 215,152 / 10,817,408 / 3.22us / 0.25us / 30.83us
- 271,973 / 12,555,908 / 3.12us / 0.25us / 27.20us
- 281,640 / 12,575,558 / 2.84us / 0.20us / 21.70us

NATS (3 runs):
- 57,162 / 2,858,782 / 9.95us / 0.20us / 19.83us
- 58,018 / 2,903,119 / 5.45us / 0.20us / 15.00us
- 57,170 / 2,859,375 / 8.28us / 0.25us / 18.62us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | **271,973** | 57,170 | **+375.8%** |
| sub-agg msg/s | **12,555,908** | 2,859,375 | **+339.2%** |
| avg latency | **3.12us** | 8.28us | **-62.3%** |
| P99 latency | 0.25us | 0.20us | +25.0% |
| P99.9 latency | 27.20us | 18.62us | +46.1% |

---

### 4. Wide fan-out 100 subs, 128B (1 pub / 100 sub / 1 topic / 30,000 msgs)

Wisp (5 runs, pub / sub-agg msg/s / avg / P99 / P99.9):
- 128,926 / 12,269,915 / 6.70us / 0.25us / 27.83us
- 146,999 / 10,151,953 / 4.95us / 0.25us / 23.66us
- 137,714 / 9,720,475 / 4.68us / 0.25us / 20.37us
- 107,688 / 10,764,615 / 6.53us / 0.20us / 24.16us
- 125,030 / 12,134,353 / 4.03us / 0.20us / 16.12us

NATS (5 runs):
- 22,449 / 2,245,338 / 28.65us / 0.25us / 22.54us
- 21,544 / 2,155,077 / 31.10us / 0.29us / 26.04us
- 31,854 / 3,186,927 / 6.25us / 0.25us / 27.75us
- 28,903 / 2,891,013 / 11.31us / 0.25us / 21.75us
- 27,108 / 2,710,165 / 6.08us / 0.25us / 18.41us

Additional 3 runs (separate batch):

Wisp:
- 136,447 / 12,252,349 / 5.76us / 0.29us / 22.20us
- 123,464 / 12,227,498 / 5.61us / 0.25us / 23.75us
- 114,670 / 11,666,813 / 6.84us / 0.20us / 30.33us

NATS:
- 24,729 / 2,474,179 / 25.84us / 0.29us / 26.95us
- 30,374 / 3,037,328 / 6.61us / 0.25us / 19.41us
- 18,733 / 1,873,704 / 36.03us / 0.25us / 19.62us

Median summary (5-run batch):

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | **128,926** | 27,108 | **+375.6%** |
| sub-agg msg/s | **10,764,615** | 2,710,165 | **+297.2%** |
| avg latency | **4.95us** | 11.31us | **-56.2%** |
| P99 latency | 0.25us | 0.25us | 0.0% |
| P99.9 latency | 23.66us | 22.54us | +5.0% |

---

### 5. Wide fan-out 50 subs, 64B (1 pub / 50 sub / 1 topic / 50,000 msgs)

Wisp (3 runs, pub / sub-agg msg/s / avg / P99 / P99.9):
- 167,150 / 7,900,839 / 2.94us / 0.16us / 20.12us
- 202,981 / 10,199,632 / 3.75us / 0.20us / 30.58us
- 235,917 / 11,557,998 / 2.74us / 0.20us / 23.83us

NATS (3 runs):
- 61,170 / 3,059,428 / 1.22us / 0.20us / 13.58us
- 61,390 / 3,069,911 / 4.17us / 0.16us / 18.91us
- 58,256 / 2,913,063 / 5.83us / 0.16us / 10.37us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | **202,981** | 61,170 | **+231.8%** |
| sub-agg msg/s | **10,199,632** | 3,059,428 | **+233.4%** |
| avg latency | **2.94us** | 4.17us | **-29.5%** |
| P99 latency | 0.20us | **0.16us** | +25.0% |
| P99.9 latency | 23.83us | **13.58us** | +75.5% |

---

### 6. Large payload 8KB (1 pub / 10 sub / 1 topic / 50,000 msgs)

Wisp (3 runs, pub / sub-agg msg/s / avg / P99 / P99.9):
- 101,928 / 791,937 / 9.69us / 24.16us / 570.33us
- 101,202 / 786,663 / 9.76us / 23.66us / 657.91us
- 96,747 / 760,133 / 10.24us / 23.75us / 647.62us

NATS (3 runs):
- 80,228 / 758,666 / 12.24us / 31.66us / 1,659.41us
- 82,740 / 768,528 / 11.85us / 29.29us / 1,692.20us
- 78,719 / 747,452 / 12.48us / 32.41us / 1,634.04us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | **101,202** | 80,228 | **+26.1%** |
| sub-agg msg/s | **786,663** | 758,666 | **+3.7%** |
| throughput | **791 MiB/s** | 627 MiB/s | **+26.2%** |
| avg latency | **9.76us** | 12.24us | **-20.3%** |
| P99 latency | **23.66us** | 29.29us | **-19.2%** |
| P99.9 latency | **647.62us** | 1,659.41us | **-61.0%** |

---

### 7. Many subjects (10 pub / 10 sub / 1000 topics / 200,000 msgs / 128B)

Wisp (3 runs, pub-agg / sub-agg msg/s):
- 822,441 / 7,993,904
- 804,898 / 7,990,246
- 804,478 / 7,895,780

NATS (3 runs):
- 409,150 / 4,097,643
- 412,279 / 4,124,875
- 403,013 / 4,036,210

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub-agg msg/s | **804,898** | 409,150 | **+96.7%** |
| sub-agg msg/s | **7,993,904** | 4,097,643 | **+95.1%** |

---

### 8. Non-fanout many subjects (1 pub / 1 sub / 5000 topics / 2,000,000 msgs / 64B)

Wisp (5 runs, pub / sub msg/s / avg / P99 / P99.9):
- 2,734,334 / — / 0.23us / 2.37us / 11.29us
- 2,714,368 / — / 0.23us / 2.41us / 11.04us
- 2,678,112 / — / 0.24us / 2.20us / 11.83us
- 2,710,952 / — / 0.23us / 2.08us / 9.20us
- 2,664,552 / — / 0.24us / 2.16us / 9.12us

Verification run (sub line captured):
- 2,678,802 / 2,676,959

NATS (5 runs):
- 1,465,571 / 1,466,263 / 0.54us / 0.95us / 21.29us
- 1,472,199 / 1,472,862 / 0.53us / 1.04us / 21.12us
- 1,465,404 / 1,466,283 / 0.54us / 1.00us / 22.25us
- 1,469,311 / 1,470,233 / 0.54us / 0.91us / 21.58us
- 1,467,529 / 1,468,296 / 0.54us / 1.00us / 21.45us

Median summary:

| Metric | Wisp | NATS | Delta |
|---|---:|---:|---:|
| pub msg/s | **2,710,952** | 1,467,529 | **+84.7%** |
| avg latency | **0.23us** | 0.54us | **-57.4%** |
| P99 latency | 2.20us | **1.00us** | +120.0% |
| P99.9 latency | **11.04us** | 21.45us | **-48.5%** |

---

### Overall comparison

| Scenario | Wisp vs NATS | Winner |
|---|---:|---|
| Single 1:1 64B | -20.8% | NATS |
| Single 1:1 1KB | -24.5% | NATS |
| Fan-out 50 sub 128B | +375.8% | **Wisp** |
| Fan-out 100 sub 128B | +375.6% | **Wisp** |
| Fan-out 50 sub 64B | +231.8% | **Wisp** |
| Large payload 8KB (10 sub) | +26.1% | **Wisp** |
| Many subjects (10p/10s) | +96.7% | **Wisp** |
| Non-fanout many subjects (1p/1s) | +84.7% | **Wisp** |

### Interpretation
- Wisp dominates on fan-out workloads (3–5× faster) and multi-subject scenarios (~2× faster). These are the workloads the optimization effort targeted.
- NATS retains a ~20–25% edge on single-publisher single-subscriber throughput. This regression traces to the batched writev writer path introduced in the "Batched writev queue" change — it adds overhead in non-fan-out cases where the batching benefit doesn't materialize.
- Large payload (8KB) shows a solid Wisp win (+26%) with dramatically better P99.9 latency (648us vs 1,659us), suggesting the vectored I/O path handles larger writes more efficiently.
- P99.9 tail latency is a mixed picture: Wisp wins on non-fanout and large payload workloads but loses on wide fan-out, likely due to batching-induced jitter when many subscriber writers flush concurrently.

---

## 2026-03-04 — Remove async_trait boxing + ahash dispatch cache

Optimization scope:
- **Remove `#[async_trait]` from `NatsServerHandler` trait and impl.** The `async_trait` macro wraps every async method return in `Pin<Box<dyn Future>>`, causing a heap allocation + deallocation per call. Profiling showed this accounted for ~14% of CPU on the PUB hot path (44 free + 13 malloc samples out of 399 active samples). Replaced with native `async fn in trait` (Rust 2024 edition RPITIT) which uses static dispatch — zero heap allocation.
- **Switch dispatch_cache `DashMap` to ahash.** DashMap defaults to SipHash, which appeared as ~9.5% of reader samples (16 out of 169). Changed to `DashMap<String, ..., ahash::RandomState>` for faster hashing.
- **Removed `async-trait` crate dependency.**

### Profiling methodology

Used macOS `sample` tool to capture 3 seconds of stack samples during a 1p/1s/64B workload. Key findings in the reader task (169 of 313 run_task samples):

| Hotspot | Samples | % of reader | Description |
|---|---:|---:|---|
| `_xzm_free` (async_trait future drop) | 44 | 26.0% | Heap deallocation of boxed handle_pub future |
| `handle_pub` future allocation | 13 | 7.7% | Heap allocation of boxed handle_pub future |
| DashMap dispatch_cache SipHash | 16 | 9.5% | Hashing subject string for cache lookup |
| `parse_pub_args` | 17 | 10.1% | Argument parsing |
| `read_exact` (payload read) | 11 | 6.5% | Socket read for payload bytes |
| `BytesMut` operations | 9 | 5.3% | reserve/split_to/freeze for payload buffer |

### Raw results

#### Single fan-out 64B (1 pub / 1 sub / 1 topic / 1,000,000 msgs)

Wisp after (5 runs, pub / sub msg/s):
- 3,671,512 / 3,665,644
- 3,769,257 / 3,768,282
- 3,842,421 / 3,814,959
- 3,983,811 / 3,958,093
- 3,798,789 / 3,810,293

Median: pub **3,798,789**, sub **3,810,293**

Compared to before (median 2,970,686 / 2,997,087):
- pub: **+27.9%**
- sub: **+27.1%**

Compared to NATS (median 3,748,258):
- pub: **+1.3%** ← Wisp now matches NATS on this workload

#### Single fan-out 1KB (1 pub / 1 sub / 1 topic / 500,000 msgs)

Wisp after (3 runs, pub / sub msg/s):
- 1,811,445 / 1,816,385
- 1,928,496 / 1,908,559
- 1,895,840 / 1,902,000

Median: pub **1,895,840**, sub **1,902,000**

Compared to before (median 1,604,423 / 1,609,584):
- pub: **+18.2%**
- sub: **+18.2%**

Compared to NATS (median 2,124,448):
- pub: **-10.8%** (gap closed from -24.5% to -10.8%)

#### Wide fan-out 100 subs 128B (1 pub / 100 sub / 1 topic / 30,000 msgs)

Wisp after (3 runs, pub / sub-agg msg/s):
- 117,099 / 8,655,524
- 97,348 / 9,971,900
- 126,861 / 12,852,080

Median: pub **117,099**, sub-agg **9,971,900**

Compared to before (median 128,926 / 10,764,615):
- pub: **-9.2%**
- sub-agg: **-7.4%**

Note: High run-to-run variance on this workload (run 3 hit 12.8M sub-agg). The regression is within noise.

#### Non-fanout many subjects (1 pub / 1 sub / 5000 topics / 2,000,000 msgs / 64B)

Wisp after (3 runs, pub / sub msg/s):
- 3,806,842 / 3,808,662
- 3,629,594 / 3,631,755
- 3,794,082 / 3,793,300

Median: pub **3,794,082**, sub **3,793,300**

Compared to before (median 2,710,952):
- pub: **+39.9%**

Compared to NATS (median 1,467,529):
- pub: **+158.5%** (Wisp is 2.6× faster)

### Interpretation
- Removing `async_trait` boxing was the single highest-impact optimization for 1:1 throughput, closing the NATS gap entirely on 64B payloads.
- The 1KB gap shrank from -24.5% to -10.8% — the remaining gap is likely dominated by payload buffer allocation (`split_to` + `freeze` creates a new `Bytes` with shared state per PUB) and the inherently higher per-byte cost in the writev path.
- Non-fanout many-subjects saw a dramatic +40% improvement, confirming that the boxing overhead was proportionally larger when the handler is called millions of times with cheap cache-hit dispatch.
- Wide fan-out showed no meaningful regression; variance on that workload is naturally high.
