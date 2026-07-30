package com.evolution.natseffect.loadtest

import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt, DurationLong, FiniteDuration}

sealed trait Scenario {
  def name: String
}

object Scenario {

  /** Historically: jnats default pending limits (512Ki messages / 64 MiB per dispatcher), reproducing the incident's client-side drops,
    * slow-consumer events, ordered-consumer recreations, warmup timeouts and liveness stalls. Since #10 hardcoded unlimited pending limits
    * for JetStream dispatchers, this configuration is no longer reachable via the public API - on current master this scenario is identical
    * to [[Unlimited]]; the recorded drop-reproduction numbers in docs/loadtest-results.md are from pre-#10 master.
    */
  case object Baseline extends Scenario { val name = "baseline" }

  /** The interim fix, baked into master by #10: unlimited pending limits on JetStream dispatchers - no drops, but the overload remains
    * (control-plane timeouts, heartbeat-driven recreations, unbounded memory for genuinely slow consumers).
    */
  case object Unlimited extends Scenario { val name = "unlimited" }

  /** The paced consume engine (#12): processing-paced pulls, KV watch via KeyValue.watchAllPaced. */
  case object Paced extends Scenario { val name = "paced" }

  val all: List[Scenario] = List(Baseline, Unlimited, Paced)

  def parse(s: String): Either[String, Scenario] =
    all.find(_.name == s).toRight(s"Unknown scenario '$s', expected one of: ${all.map(_.name).mkString(", ")}")
}

/** What the run measures. The engine under test is chosen by [[Scenario]]; the mode chooses the *workload* it is measured on. */
sealed trait Mode {
  def name: String
}

object Mode {

  /** The original workload: a cold backlog burst - N watchers started simultaneously against a pre-populated bucket, measured to warmup
    * completion. Answers "how fast does a fleet catch up, and does it survive doing so".
    */
  case object Warmup extends Mode { val name = "warmup" }

  /** Post-warmup live updates at a controlled rate, measuring end-to-end per-update latency (publish call -> handler completion) and
    * sustained delivery throughput. Answers "what does the engine cost per update once caught up".
    */
  case object Steady extends Mode { val name = "steady" }

  val all: List[Mode] = List(Warmup, Steady)

  def parse(s: String): Either[String, Mode] =
    all.find(_.name == s).toRight(s"Unknown mode '$s', expected one of: ${all.map(_.name).mkString(", ")}")
}

final case class Config(
  scenario: Scenario,
  watchers: Int = 20,
  keys: Int = 18000,
  valueSizeBytes: Int = 12 * 1024,
  warmupTimeout: FiniteDuration = 60.seconds,
  handlerSpinMicros: Int = 5,
  populateParallelism: Int = 64,
  port: Int = 4331,
  computeThreads: Int = 2,
  globalTimeout: FiniteDuration = 6.minutes,
  mode: Mode = Mode.Warmup,
  updates: Int = 20000,
  rateHz: Int = 0,
  periodMillis: Int = 0,
  primeUpdates: Int = 2000,
  publishers: Int = 8,
  drainTimeout: FiniteDuration = 60.seconds,
  quiet: FiniteDuration = Duration.Zero,
  memoryStorage: Boolean = false,
  settle: FiniteDuration = Duration.Zero,
  namePoll: FiniteDuration = 50.millis,
  publisherConnection: Boolean = false
) {
  def totalBytes: Long = keys.toLong * valueSizeBytes

  def describe: String =
    s"mode=${mode.name} scenario=${scenario.name} watchers=$watchers keys=$keys valueSize=${valueSizeBytes}B " +
      s"warmupTimeout=${warmupTimeout.toSeconds}s spinMicros=$handlerSpinMicros computeThreads=$computeThreads port=$port " +
      s"globalTimeout=${globalTimeout.toSeconds}s storage=${if (memoryStorage) "memory" else "file"} " +
      s"settleMs=${settle.toMillis} namePollMs=${namePoll.toMillis} publisherConnection=$publisherConnection" + (
        if (mode == Mode.Steady)
          s" updates=$updates rate=${if (periodMillis > 0) s"1 per ${periodMillis}ms" else if (rateHz == 0) "max" else s"${rateHz}Hz"} primeUpdates=$primeUpdates " +
            s"publishers=$publishers quietSec=${quiet.toSeconds} drainTimeout=${drainTimeout.toSeconds}s"
        else ""
      )
}

object Config {

  /** Parses key=value arguments, e.g.: scenario=baseline watchers=20 keys=18000 valueSize=12288 warmupTimeoutSec=60 computeThreads=2. Every
    * numeric argument is range-checked here so that a typo fails fast with exit code 1 instead of surfacing as an obscure runtime crash
    * (or, for computeThreads=0, a compute pool with zero workers that would hang the app before the watchdog is even armed).
    */
  def parse(args: List[String]): Either[String, Config] = {
    val (badArgs, pairs) = args.partitionMap { arg =>
      arg.split("=", 2) match {
        case Array(k, v) => Right(k -> v)
        case _           => Left(s"Expected key=value argument, got '$arg'")
      }
    }

    badArgs.headOption.toLeft(pairs.toMap).flatMap { map =>
      // Every key passed to int() or choice() registers itself, so the unknown-argument check below cannot drift from the parse sites.
      val consumedKeys = mutable.Set("scenario", "mode")

      def int(key: String, default: Int, min: Int, max: Int = Int.MaxValue): Either[String, Int] = {
        consumedKeys += key
        map.get(key) match {
          case None    => Right(default)
          case Some(v) =>
            v.toIntOption
              .filter(i => i >= min && i <= max)
              .toRight(s"'$key' must be an integer >= $min${if (max != Int.MaxValue) s" and <= $max" else ""}, got '$v'")
        }
      }

      // For non-int knobs, so a typo like 'storage=mem' or 'publisherConnection=1' is rejected instead of silently
      // reading as the default.
      def choice(key: String, allowed: List[String]): Either[String, Option[String]] = {
        consumedKeys += key
        map.get(key) match {
          case None    => Right(None)
          case Some(v) =>
            Either.cond(allowed.contains(v), Some(v), s"'$key' must be one of: ${allowed.mkString(", ")}, got '$v'")
        }
      }

      for {
        scenario <- Scenario.parse(map.getOrElse("scenario", Scenario.Baseline.name))
        mode     <- Mode.parse(map.getOrElse("mode", Mode.Warmup.name))
        watchers <- int("watchers", 20, min = 1)
        keys     <- int("keys", 18000, min = 1)
        // 12 bytes is the steady-mode stamp header (magic + publish nanoTime) written into every measured update.
        valueSize   <- int("valueSize", 12 * 1024, min = if (mode == Mode.Steady) SteadyRunner.StampBytes else 1)
        warmupSec   <- int("warmupTimeoutSec", 60, min = 1)
        spinMicros  <- int("spinMicros", 5, min = 0)
        populatePar <- int("populateParallelism", 64, min = 1)
        port        <- int("port", 4331, min = 1, max = 65535)
        threads     <- int("computeThreads", 2, min = 1)
        // Steady-mode knobs; ignored (and left at their defaults) in warmup mode.
        updates <- int("updates", 20000, min = 1)
        rateHz  <- int("rateHz", 0, min = 0)
        // Explicit inter-update period, for rates below 1 Hz that rateHz cannot express. Overrides rateHz.
        periodMillis <- int("periodMs", 0, min = 0)
        primeUpdates <- int("primeUpdates", 2000, min = 0)
        publishers   <- int("publishers", 8, min = 1)
        drainSec     <- int("drainTimeoutSec", 60, min = 1)
        // Quiet gap between the prime and measured phases. Longer than the pull window, it puts a paced consumer
        // into its idle pull regime before the measured updates arrive.
        quietSec <- int("quietSec", 0, min = 0)
        // Measurement hygiene knobs; the effect of each is documented at its use site in Runner / SteadyRunner.
        memoryStorage       <- choice("storage", List("file", "memory")).map(_.contains("memory"))
        settleMs            <- int("settleMs", 0, min = 0)
        namePollMs          <- int("namePollMs", 50, min = 0)
        publisherConnection <- choice("publisherConnection", List("true", "false")).map(_.contains("true"))
        // The watchdog must outlive a legitimate warmup, so the default derives from warmupTimeout and an explicit
        // value below warmup + 30s is rejected - otherwise a healthy run would be halted as a spurious DEADLOCK.
        // In steady mode it must additionally outlive the paced publish phase and the drain wait.
        publishSec = (mode, periodMillis, rateHz) match {
          case (Mode.Steady, period, _) if period > 0 => updates * period / 1000
          case (Mode.Steady, _, rate) if rate > 0     => (updates + primeUpdates) / rate
          case _                                      => 0
        }
        timeoutSec <- int(
          "timeoutSec",
          default = math.max(
            360,
            math.max(warmupSec * 3 + 30, warmupSec + publishSec * 2 + drainSec * 2 + quietSec * 2 + 120)
          ),
          min = warmupSec + 30
        )
        unknown = map.keySet -- consumedKeys
        _      <- Either.cond(unknown.isEmpty, (), s"Unknown arguments: ${unknown.mkString(", ")}")
      } yield Config(
        scenario = scenario,
        watchers = watchers,
        keys = keys,
        valueSizeBytes = valueSize,
        warmupTimeout = warmupSec.toLong.seconds,
        handlerSpinMicros = spinMicros,
        populateParallelism = populatePar,
        port = port,
        computeThreads = threads,
        globalTimeout = timeoutSec.toLong.seconds,
        mode = mode,
        updates = updates,
        rateHz = rateHz,
        periodMillis = periodMillis,
        primeUpdates = primeUpdates,
        publishers = publishers,
        drainTimeout = drainSec.toLong.seconds,
        quiet = quietSec.toLong.seconds,
        memoryStorage = memoryStorage,
        settle = settleMs.toLong.millis,
        namePoll = namePollMs.toLong.millis,
        publisherConnection = publisherConnection
      )
    }
  }
}
