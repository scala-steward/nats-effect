package com.evolution.natseffect.loadtest

import cats.effect.implicits.*
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolution.natseffect.jetstream.{JetStream, KeyValue, KvWatchMode, MessageSubscription, SubscriptionWithWarmup, Warmup}
import com.evolution.natseffect.{Nats, Options}
import io.nats.client.api.{KeyValueEntry, KeyValueOperation}

import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import scala.concurrent.duration.{Duration, DurationInt, DurationLong, FiniteDuration, NANOSECONDS}

final case class SteadyResult(
  config: Config,
  populateTime: FiniteDuration,
  warmupOutcomes: List[Either[Throwable, Warmup.Result]],
  publishWallTime: FiniteDuration,
  deliveryWallTime: FiniteDuration,
  publishedMeasured: Long,
  deliveredMeasured: Long,
  expectedMeasured: Long,
  latenciesMicros: Vector[Long],
  ackLatenciesMicros: Vector[Long],
  messagesHandled: Long,
  clientOutMsgs: Long,
  clientInMsgs: Long,
  droppedMessages: Long,
  slowConsumerEvents: Long,
  errorsOccurred: Long,
  exceptionsOccurred: Long,
  recreations: Int,
  livenessLagsMillis: Vector[Long],
  peakHeapBytes: Long,
  liveHeapBytes: Long,
  quietOutMsgs: Long,
  outgoingDiscards: Long
)

/** Steady-state (post-warmup) measurement: the watchers are brought up and allowed to catch up on the pre-populated bucket, then live
  * updates are published at a controlled rate and every delivery is timed end-to-end.
  *
  * Why this exists next to [[Runner]]: the warmup mode measures a *cold backlog burst*, which is a catch-up throughput and survival test.
  * It says nothing about what an engine costs per update once caught up - the regime a settings-distribution service actually spends its
  * life in, and the regime where a strictly processing-paced engine could plausibly *lose* (it pulls one batch at a time, so an idle
  * consumer's next message can wait for a fresh pull, whereas a receipt-paced engine keeps a pull outstanding).
  *
  * Latency definition: `publish call issued -> that entry reaching its watcher's handler`, sampled inside the handler after the cache
  * update and *before* the synthetic spin - so a sample excludes its own handler cost but includes any queued predecessors', which is
  * precisely the queueing-delay signal that separates the two engines. Measured with `System.nanoTime()` in a single JVM, so there is no
  * clock skew. It *includes* the KV write path (put -> stream append -> ack), which is identical for both engines and is reported
  * separately so the consume-side share can be bounded. The engine-to-engine comparison is apples-to-apples; the absolute number is not a
  * pure consume latency.
  */
object SteadyRunner {

  /** Header written into every measured update: magic (4 bytes) + publish `System.nanoTime()` (8 bytes). The magic is what makes a measured
    * update unambiguous - populated and prime values are zero-filled in this header, and a bare "nonzero timestamp" test would misinterpret
    * random filler bytes as a stamp and record a garbage latency.
    */
  val StampBytes: Int = 12

  private val Magic: Int = 0xc0ffee01

  /** Bounds the latency sample memory (8 bytes per sample) for very large runs; excess samples are counted but not stored. */
  private val MaxSamples: Int = 4000000

  private val PollInterval: FiniteDuration = 20.millis

  /** A phase is considered finished when the delivery counter has not moved for this long, even if the expected count was not reached -
    * otherwise a run that drops or stalls would hang until the drain timeout with no partial data. It must exceed the longest stall an
    * engine can legitimately take (a GC pause, or the paced engine's 500 ms empty-window guard), or the tail of a healthy phase is cut off
    * and reported as a shortfall.
    */
  private val StableFor: FiniteDuration = 5.seconds

  def run(config: Config, counters: Counters): IO[SteadyResult] =
    Runner.resources(config, counters).use {
      case (kv, connection) =>
        val stats = connection.statistics

        val publisherKv: Resource[IO, KeyValue[IO]] =
          if (config.publisherConnection) publisherClient(config, kv.bucketName) else Resource.pure(kv)

        val capacity = math.min(config.updates.toLong * config.watchers, MaxSamples.toLong).toInt
        val recorder = new LatencyRecorder(capacity)
        val ackTimes = new LatencyRecorder(math.min(config.updates, MaxSamples))

        for {
          populateTime <- populate(kv, config)
          _            <- IO.println(
            s"populated ${config.keys} keys in ${populateTime.toMillis / 1000.0}s, starting ${config.watchers} watchers"
          )

          result <- Probes.livenessProbe(counters).surround {
            (watchers(kv, config, counters, recorder), publisherKv).tupled.use {
              case (handles, pubKv) =>
                for {
                  warmups <- handles.parTraverse(_.awaitWarmup.attempt)
                  _       <- IO.println(s"warmups done: ${warmups.count(_.isRight)}/${config.watchers}")

                  // Prime: unmeasured updates through the whole path (JIT, code cache, KV history churn, consumer
                  // steady state) so the measured phase is not measuring the first-call cost of the engine.
                  _ <- publishPhase(pubKv, config, count = config.primeUpdates, stamped = false, rateHz = 0, periodMillis = 0, ackTimes)
                    .whenA(config.primeUpdates > 0)
                  _ <- awaitStable(IO.delay(counters.messagesHandled.get), config.drainTimeout)
                  _ <- Runner.settle(config)

                  // Quiet phase: no traffic at all. Its protocol-message delta is the engine's *idle* cost - the pull
                  // and consumer-info requests it issues while nothing is happening - and it leaves the consumers in
                  // whatever regime a long silence puts them in before the measured updates start.
                  quietOut <- idleTraffic(stats.flatMap(_.outMsgs), config.quiet)
                  _        <- IO.println(
                    s"primed with ${config.primeUpdates} updates" +
                      (if (config.quiet > Duration.Zero) s", ${config.quiet.toSeconds}s quiet: $quietOut client->server msgs" else "") +
                      ", starting measured phase"
                  )

                  // droppedBaseline is taken here, not after populate: prime- and quiet-phase drops are not part of
                  // the measurement and would otherwise be attributed to it.
                  droppedBaseline <- stats.flatMap(_.droppedCount)
                  outBaseline     <- stats.flatMap(_.outMsgs)
                  inBaseline      <- stats.flatMap(_.inMsgs)

                  t0          <- IO.delay(System.nanoTime())
                  publishTime <- publishPhase(
                    pubKv,
                    config,
                    count = config.updates,
                    stamped = true,
                    rateHz = config.rateHz,
                    periodMillis = config.periodMillis,
                    ackTimes
                  )
                  expected                    = config.updates.toLong * config.watchers
                  _                          <- awaitCount(IO.delay(recorder.count), expected, config.drainTimeout)
                  lastDelivery               <- IO.delay(recorder.lastDeliveryNanos)
                  (dropped, outMsgs, inMsgs) <- (
                    stats.flatMap(_.droppedCount).map(_ - droppedBaseline),
                    stats.flatMap(_.outMsgs).map(_ - outBaseline),
                    stats.flatMap(_.inMsgs).map(_ - inBaseline)
                  ).tupled
                  names <- handles.traverse(_.names.get)
                  // After the measured phase, so the collection cannot perturb it: what the fleet actually retains,
                  // as opposed to peak allocated-minus-free, which in a large heap is mostly uncollected garbage.
                  liveHeap <- IO.delay(System.gc()) *> IO.sleep(200.millis) *> IO.delay {
                    System.gc()
                    Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
                  }
                } yield SteadyResult(
                  config = config,
                  populateTime = populateTime,
                  warmupOutcomes = warmups,
                  publishWallTime = publishTime,
                  deliveryWallTime = FiniteDuration(math.max(0L, lastDelivery - t0), "ns"),
                  publishedMeasured = config.updates.toLong,
                  deliveredMeasured = recorder.count,
                  expectedMeasured = expected,
                  latenciesMicros = recorder.samplesMicros,
                  ackLatenciesMicros = ackTimes.samplesMicros,
                  messagesHandled = counters.messagesHandled.get,
                  clientOutMsgs = outMsgs,
                  clientInMsgs = inMsgs,
                  droppedMessages = dropped,
                  slowConsumerEvents = counters.slowConsumerEvents.get,
                  errorsOccurred = counters.errorsOccurred.get,
                  exceptionsOccurred = counters.exceptionsOccurred.get,
                  recreations = names.map(ns => (ns.size - 1).max(0)).sum,
                  livenessLagsMillis = counters.livenessSamples,
                  peakHeapBytes = counters.peakHeapBytes.get,
                  liveHeapBytes = liveHeap,
                  quietOutMsgs = quietOut,
                  // A publish lost to a full outgoing queue would otherwise surface as an undelivered update
                  // and be charged to the consumer.
                  outgoingDiscards = counters.outgoingDiscards.get
                )
            }
          }
        } yield result
    }

  /** A second connection for the publisher, so the measured deliveries and the update writes do not share one socket and one jnats reader
    * thread. A side benefit: the watcher connection's `outMsgs` then counts *only* the engine's own upkeep (pulls, consumer info), making
    * it a clean cross-engine measure of consumer-side protocol traffic.
    */
  private def publisherClient(config: Config, bucketName: String): Resource[IO, KeyValue[IO]] =
    for {
      connection <- Nats.connect(
        Options[IO]()
          .withNatsServerUris(Vector(s"nats://127.0.0.1:${config.port}"))
          .withConnectionName(Some("loadtest-publisher"))
      )
      js <- JetStream.fromConnection[IO](connection).toResource
      kv <- js.keyValue(bucketName).toResource
    } yield kv

  /** Like [[Runner.populate]], but the stamp header is left zeroed so the warmup replay of these values is never mistaken for a measured
    * update.
    */
  private def populate(kv: KeyValue[IO], config: Config): IO[FiniteDuration] =
    for {
      value <- IO.delay(freshValue(config.valueSizeBytes))
      t0    <- IO.monotonic
      _     <- (0 until config.keys).toVector.parTraverseN(config.populateParallelism) { i =>
        kv.put(key(i), value).void
      }
      t1 <- IO.monotonic
    } yield t1 - t0

  final private case class WatcherHandle(awaitWarmup: IO[Warmup.Result], names: Ref[IO, Set[String]])

  private def watchers(
    kv: KeyValue[IO],
    config: Config,
    counters: Counters,
    recorder: LatencyRecorder
  ): Resource[IO, List[WatcherHandle]] =
    List.fill(config.watchers)(()).traverse(_ => watcher(kv, config, counters, recorder))

  /** One watcher on the engine selected by the scenario. The handler mirrors [[Runner.watcher]]'s production-fidelity cache update and
    * spin, plus the latency read - the extra work (a length check, a 12-byte decode and one recorded sample) is identical for both engines.
    */
  private def watcher(
    kv: KeyValue[IO],
    config: Config,
    counters: Counters,
    recorder: LatencyRecorder
  ): Resource[IO, WatcherHandle] =
    for {
      names <- Ref.of[IO, Set[String]](Set.empty).toResource
      cache <- Ref.of[IO, Map[String, Long]](Map.empty).toResource

      handler = (kvEntry: KeyValueEntry) => {
        val cacheUpdate = kvEntry.getOperation match {
          case KeyValueOperation.PUT =>
            cache.update(
              _.updatedWith(kvEntry.getKey) {
                case Some(oldRevision) => Some(if (kvEntry.getRevision > oldRevision) kvEntry.getRevision else oldRevision)
                case None              => Some(kvEntry.getRevision)
              }
            )
          case KeyValueOperation.DELETE | KeyValueOperation.PURGE =>
            cache.update(_.updatedWith(kvEntry.getKey) {
              case Some(oldRevision) => if (kvEntry.getRevision > oldRevision) None else Some(oldRevision)
              case None              => None
            })
        }

        cacheUpdate *> IO.delay {
          counters.messagesHandled.incrementAndGet()
          readStamp(kvEntry.getValue).foreach(stamp => recorder.record(System.nanoTime() - stamp))
          Runner.spin(config.handlerSpinMicros)
        }
      }

      sub <- watch(kv, config, handler)
      _   <- pollConsumerName(sub.subscription, names, config.namePoll).foreverM.background
        .whenA(config.namePoll > Duration.Zero)
    } yield WatcherHandle(sub.warmupLatch.get, names)

  /** Exhaustive on purpose, as in [[Runner]]: a new scenario must pick its consume engine here or compilation fails, rather than silently
    * measuring the wrong semantics.
    */
  private def watch(
    kv: KeyValue[IO],
    config: Config,
    handler: KeyValueEntry => IO[Unit]
  ): Resource[IO, SubscriptionWithWarmup[IO]] =
    config.scenario match {
      case Scenario.Baseline | Scenario.Unlimited =>
        kv.watchAll(KvWatchMode.LatestValues, handler, config.warmupTimeout)
      case Scenario.Paced =>
        kv.watchAllPaced(KvWatchMode.LatestValues, handler, config.warmupTimeout)
    }

  private def pollConsumerName(
    subscription: MessageSubscription[IO],
    names: Ref[IO, Set[String]],
    every: FiniteDuration
  ): IO[Unit] =
    subscription.getConsumerName.attempt.flatMap {
      case Right(name) if name != null => names.update(_ + name)
      case _                           => IO.unit
    } *> IO.sleep(every)

  /** Publishes `count` updates, round-robin over the bucket's keys.
    *
    * `rateHz == 0` means open the throttle: `publishers`-way concurrent puts, as fast as the write path accepts them (the publisher becomes
    * the bottleneck, which is the saturation case). Otherwise puts are paced in fixed 2 ms ticks - fine-grained enough to shape a rate, and
    * coarse enough that the CE scheduler can actually honour the sleeps. Falling behind the target rate is not corrected: the achieved rate
    * is reported instead, so a publisher-bound run is visible rather than silently mislabelled.
    */
  private def publishPhase(
    kv: KeyValue[IO],
    config: Config,
    count: Int,
    stamped: Boolean,
    rateHz: Int,
    periodMillis: Int,
    acks: LatencyRecorder
  ): IO[FiniteDuration] = {
    // The put round-trip is timed separately because it is *inside* the end-to-end latency and identical for both
    // engines: reporting it lets the consume-side share of a latency number be bounded instead of guessed at.
    def put(i: Int): IO[Unit] =
      IO.delay {
        val value = freshValue(config.valueSizeBytes)
        if (stamped) writeStamp(value)
        value
      }.flatMap { value =>
        for {
          t0 <- IO.delay(System.nanoTime())
          _  <- kv.put(key(i % config.keys), value)
          _  <- IO.delay(if (stamped) acks.record(System.nanoTime() - t0))
        } yield ()
      }

    val issue =
      if (periodMillis > 0) {
        // One update per period, deadline-scheduled so a slow put cannot make the cadence drift.
        val tick = periodMillis.toLong.millis
        IO.monotonic.flatMap { start =>
          (0 until count).toList.traverse_ { i =>
            val deadline = start + tick * i.toLong
            IO.monotonic.flatMap(now => IO.sleep(deadline - now).whenA(deadline > now)) *> put(i)
          }
        }
      } else if (rateHz <= 0) (0 until count).toVector.parTraverseN(config.publishers)(put).void
      else {
        // Shape the rate as `perTick` puts every `tick`. Ticks are ~2 ms or longer - short enough to spread a high
        // rate evenly, long enough for the CE scheduler to honour the sleep - so the tick *period* is what varies at
        // low rates (2 updates/s is one put every 500 ms, not four puts every 2 s).
        val perTick = math.max(1, math.round(rateHz * 2.0 / 1000.0).toInt)
        val tick    = FiniteDuration(math.round(perTick.toDouble / rateHz * 1e9), NANOSECONDS)
        val ticks   = (count + perTick - 1) / perTick

        IO.monotonic.flatMap { start =>
          (0 until ticks).toList.traverse_ { t =>
            val from     = t * perTick
            val to       = math.min(count, from + perTick)
            val deadline = start + tick * t.toLong

            IO.monotonic.flatMap(now => IO.sleep(deadline - now).whenA(deadline > now)) *>
              (from until to).toVector.parTraverseN(math.max(config.publishers, perTick))(put).void
          }
        }
      }

    for {
      t0 <- IO.monotonic
      _  <- issue
      t1 <- IO.monotonic
    } yield t1 - t0
  }

  /** Waits until `read` reaches `target`, or stops moving for [[StableFor]], or `timeout` elapses. A stalled or lossy run therefore still
    * produces a report (with the shortfall visible) instead of hanging to the watchdog.
    */
  private def awaitCount(read: IO[Long], target: Long, timeout: FiniteDuration): IO[Unit] = {
    def loop(last: Long, unchangedFor: FiniteDuration): IO[Unit] =
      read.flatMap { current =>
        if (current >= target) IO.unit
        else if (unchangedFor >= StableFor) IO.unit
        else
          IO.sleep(PollInterval) *>
            loop(current, if (current == last) unchangedFor + PollInterval else Duration.Zero)
      }

    loop(-1L, Duration.Zero).timeoutTo(timeout, IO.unit)
  }

  private def awaitStable(read: IO[Long], timeout: FiniteDuration): IO[Unit] =
    awaitCount(read, Long.MaxValue, timeout)

  /** Client->server protocol messages issued over a silent interval: with no publishes and no deliveries, everything counted here is the
    * engine's own upkeep (pull requests, consumer info). Returns 0 without waiting when the interval is zero.
    */
  private def idleTraffic(readOutMsgs: IO[Long], quiet: FiniteDuration): IO[Long] =
    if (quiet <= Duration.Zero) IO.pure(0L)
    else
      for {
        before <- readOutMsgs
        _      <- IO.sleep(quiet)
        after  <- readOutMsgs
      } yield after - before

  private def key(i: Int): String = f"key$i%06d"

  private def freshValue(size: Int): Array[Byte] = {
    val a = new Array[Byte](size)
    // Deterministic filler: the value bytes are never inspected, and per-message Random.nextBytes on the publisher
    // path would charge the measured phase for RNG work rather than for the engine.
    var i = StampBytes
    while (i < size) {
      a(i) = (i & 0x7f).toByte
      i += 1
    }
    a
  }

  private def writeStamp(a: Array[Byte]): Unit = {
    val now = System.nanoTime()
    a(0) = ((Magic >>> 24) & 0xff).toByte
    a(1) = ((Magic >>> 16) & 0xff).toByte
    a(2) = ((Magic >>> 8) & 0xff).toByte
    a(3) = (Magic & 0xff).toByte
    var i = 0
    while (i < 8) {
      a(4 + i) = ((now >>> (56 - 8 * i)) & 0xffL).toByte
      i += 1
    }
  }

  private def readStamp(value: Array[Byte]): Option[Long] =
    if (value == null || value.length < StampBytes) None
    else {
      val magic =
        ((value(0) & 0xff) << 24) | ((value(1) & 0xff) << 16) | ((value(2) & 0xff) << 8) | (value(3) & 0xff)
      if (magic != Magic) None
      else {
        var v = 0L
        var i = 0
        while (i < 8) {
          v = (v << 8) | (value(4 + i) & 0xffL)
          i += 1
        }
        Some(v)
      }
    }
}

/** Lock-free latency sink: an `AtomicLongArray` slot per delivery plus the wall-clock instant of the most recent one (which is what turns a
  * publish start into a delivery wall time). Volatile writes rather than a plain array so a report thread that observes the count also
  * observes the samples; the cost - one CAS and one volatile store per message - is paid identically by both engines.
  */
final class LatencyRecorder(capacity: Int) {

  private val samples  = new AtomicLongArray(capacity)
  private val index    = new AtomicLong(0)
  private val lastSeen = new AtomicLong(0)

  def record(latencyNanos: Long): Unit = {
    val i = index.getAndIncrement()
    if (i < capacity) samples.set(i.toInt, latencyNanos)
    lastSeen.set(System.nanoTime())
  }

  def count: Long = index.get

  def lastDeliveryNanos: Long = lastSeen.get

  def samplesMicros: Vector[Long] = {
    val n = math.min(count, capacity.toLong).toInt
    (0 until n).map(i => samples.get(i) / 1000).toVector
  }
}
