# Load test results: KV warm-up collapse — callback vs paced engine

*Date: 2026-07-15. Produced by the `loadtest` module (see the README "Load testing" section).
Workload: 20 KV watchers started simultaneously against a bucket of 18,000 keys × 12 KiB = 211 MiB,
production-fidelity handler (per-entry cache update + ~5 µs spin), compute pool restricted to
2 threads, 60 s warm-up timeout. Apple M3 Max, OpenJDK 21, `-Xmx4g`, embedded nats-server 2.12.1,
jnats 2.25.1. Both columns measured back-to-back in one session on the #12 branch (rebased on
master).*

## Comparison

| Metric | callback engine (current `master`) | paced engine (#12) |
|---|---|---|
| Warm-ups (of 20) | 16 success / **4 timeout** | **20 success** |
| Warm-up times (min / median / max) | 18.7 / 51.5 / 59.5 s | **1.9 / 2.1 / 2.2 s** |
| Consume wall time | 60.0 s (timeout-bound) | **2.2 s** |
| Messages handled (of 360,000) | 294,875 | **360,000** |
| Throughput | 4,912 msg/s (57.6 MiB/s) | **163,339 msg/s (1.9 GiB/s)** |
| Dropped messages / slow-consumer events | 0 / 0 | 0 / 0 |
| Consumer recreations (sampled lower bound) | **≥16** | **0** |
| Liveness probe lag (p99 / max) | 21 ms / **12,065 ms — DEAD** | 32 / 32 ms |
| Peak heap (for 211 MiB of data) | **3,737 MiB** | **675 MiB** |
| Verdict | PROBLEM REPRODUCED | CLEAN |

The callback engine is receipt-paced: pulls are replenished as messages arrive, so the burst
buffers client-side without bound (3.7 GiB), starves the 2-thread compute pool (12 s liveness
stall, heartbeat-driven consumer recreations, warm-up timeouts). The paced engine couples pulls to
processing: the buffer stays bounded by one pull batch, so drops and recreations are impossible by
construction, and the same workload completes ~27× faster.

Raw `RESULT_JSON` lines:

```json
{"scenario": "unlimited", "watchers": 20, "keys": 18000, "valueSizeBytes": 12288, "computeThreads": 2, "spinMicros": 5, "populateMillis": 762, "consumeMillis": 60027, "warmupSuccess": 16, "warmupTimeout": 4, "watcherFailures": 0, "messagesHandled": 294875, "msgPerSec": 4912, "dropped": 0, "slowConsumers": 0, "recreations": 16, "errors": 0, "exceptions": 1, "outgoingDiscards": 0, "livenessLagP99Ms": 21, "livenessLagMaxMs": 12065, "peakHeapMiB": 3737, "reproduced": true}
{"scenario": "paced", "watchers": 20, "keys": 18000, "valueSizeBytes": 12288, "computeThreads": 2, "spinMicros": 5, "populateMillis": 772, "consumeMillis": 2204, "warmupSuccess": 20, "warmupTimeout": 0, "watcherFailures": 0, "messagesHandled": 360000, "msgPerSec": 163339, "dropped": 0, "slowConsumers": 0, "recreations": 0, "errors": 0, "exceptions": 0, "outgoingDiscards": 0, "livenessLagP99Ms": 32, "livenessLagMaxMs": 32, "peakHeapMiB": 675, "reproduced": false}
```

## Reproducing

```bash
sbt 'loadtest/run scenario=unlimited watchers=20 keys=18000 valueSize=12288 warmupTimeoutSec=60 computeThreads=2'
sbt 'loadtest/run scenario=paced     watchers=20 keys=18000 valueSize=12288 warmupTimeoutSec=60 computeThreads=2'
```

Exit codes: 0 = finished (see verdict), 1 = bad args, 2 = watchdog kill (thread dump printed),
3 = crash. Reproduction of the callback pathology needs the constrained pool (`computeThreads=2`).

## Steady-state results: the paced engine's empty-window latency tail

*Date: 2026-07-30. Produced by the `loadtest` module in `mode=steady`.
Workload: 16 KV watchers on a small bucket, one update every 5 s for 5 minutes (960 timed
deliveries per run), zero-cost handler, 4 compute threads, `storage=memory`, publisher on its own
connection. Apple M3 Max, OpenJDK 21, `-Xms2g -Xmx2g`, embedded nats-server 2.12.1, jnats 2.25.1. The
warm-up results above are unaffected by this: a backlog-bound drain never observes an idle pull window.*

The measurement that found the bug, and the same measurement after the deadline-slack fix:

| Metric                       | paced, before        | paced, after | callback (reference) |
|------------------------------|----------------------|--------------|----------------------|
| latency p50                  | 4.2 ms               | 4.8 ms       | 4.0 ms               |
| latency p99                  | **438 ms**           | **22.8 ms**  | 15.1 ms              |
| latency max                  | **501 ms**           | **23.1 ms**  | 18.0 ms              |
| arrivals > 200 ms            | **16 / 960 (1.67%)** | **0 / 960**  | 0 / 960              |
| consumer→server msgs / 295 s | 301                  | **160**      | —                    |

`max = 501 ms` against a hard-coded 500 ms `EarlyEmptyGuard` is the signature; the `kv.put` ack on the
same runs was 8-24 ms, so the delay is on the consume side, not in the write path. The halved pull
traffic is the independent witness that the *mechanism* went away rather than the symptom being
suppressed: the engine went from two pull cycles per window to one. `PacedOrderedConsumerContextSpec`
pins that ratio automatically ("an expired pull costs one new pull, not two": 11 pulls over six
one-second windows before the fix, 6 after).

Two cautions when reading a run like this. Latency is timed from the `put` call, so a publisher-side
stall inflates every watcher's sample identically - one callback run showed a 567 ms "tail" that the
`kv.put` ack column identified as a 566 ms publish, not a consumer effect. And because the fleet
subscribes simultaneously its pull windows are phase-aligned, so a guard hit appears as exactly
`watchers` slow samples; consumers that start at different times de-phase, making the effect ~1.7% of
(update, consumer) pairs rather than 1.7% of updates being late for everyone.

Reproduce with:

```bash
sbt 'loadtest/run mode=steady scenario=paced watchers=16 keys=200 valueSize=512 updates=60 \
  periodMs=5000 primeUpdates=200 spinMicros=0 computeThreads=4 storage=memory settleMs=1000 \
  namePollMs=0 publisherConnection=true warmupTimeoutSec=60 drainTimeoutSec=30'
```

## Legacy results (pre-#10 master, 2026-07-14)

Recorded before #10 hardcoded unlimited pending limits into every JetStream dispatcher; the
**baseline** configuration (jnats default limits, 512 Ki msgs / 64 MiB) is no longer reachable via
the public API and reproduced the original incident's client-side drops. Preserved as recorded:

| Metric | baseline (default limits) | unlimited (0,0) |
|---|---|---|
| Warm-ups (of 20) | 11 ok / 4 timeout / 5 failed | 9 ok / 7 timeout / 4 failed |
| Dropped messages (consume phase) | **3,581** | 0 |
| Slow-consumer events | **243** | 0 |
| Consumer recreations (sampled lower bound) | ≥17 | ≥9 |
| Watcher failures (JS control-plane timeout) | 5 | 4 |
| Liveness probe max lag | 10,062 ms — DEAD | 4,143 ms |
| Peak heap | 1,445 MiB | 1,705 MiB |
| Verdict | PROBLEM REPRODUCED | PROBLEM REPRODUCED |

```json
{"scenario": "baseline", "watchers": 20, "keys": 18000, "valueSizeBytes": 12288, "computeThreads": 2, "spinMicros": 5, "populateMillis": 690, "consumeMillis": 60025, "warmupSuccess": 11, "warmupTimeout": 4, "watcherFailures": 5, "messagesHandled": 235881, "msgPerSec": 3930, "dropped": 3581, "slowConsumers": 243, "recreations": 17, "errors": 0, "exceptions": 1, "outgoingDiscards": 0, "livenessLagP99Ms": 17, "livenessLagMaxMs": 10062, "peakHeapMiB": 1445, "reproduced": true}
{"scenario": "unlimited", "watchers": 20, "keys": 18000, "valueSizeBytes": 12288, "computeThreads": 2, "spinMicros": 5, "populateMillis": 738, "consumeMillis": 60030, "warmupSuccess": 9, "warmupTimeout": 7, "watcherFailures": 4, "messagesHandled": 168026, "msgPerSec": 2799, "dropped": 0, "slowConsumers": 0, "recreations": 9, "errors": 0, "exceptions": 1, "outgoingDiscards": 0, "livenessLagP99Ms": 6, "livenessLagMaxMs": 4143, "peakHeapMiB": 1705, "reproduced": true}
```

Legacy takeaway: default pending limits produced drops → sequence gaps → the recreation feedback
loop; unlimited limits removed the drops but kept the overload (control-plane starvation, unbounded
memory). Pending-limit tuning changed *which* pathology occurred, not *whether* the system
degraded — the paced engine above is the structural fix.
