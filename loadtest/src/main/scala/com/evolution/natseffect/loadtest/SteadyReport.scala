package com.evolution.natseffect.loadtest

import com.evolution.natseffect.jetstream.Warmup

object SteadyReport {

  def render(r: SteadyResult): String = {
    val cfg = r.config

    val warmupOk = r.warmupOutcomes.count {
      case Right(_: Warmup.Result.Success) => true
      case _                               => false
    }
    val warmupBad = r.warmupOutcomes.size - warmupOk

    val latencies = r.latenciesMicros.sorted
    val lagSorted = r.livenessLagsMillis.sorted

    val publishSeconds  = r.publishWallTime.toNanos / 1e9
    val deliverySeconds = r.deliveryWallTime.toNanos / 1e9

    // Publish rate: updates the harness issued per second. Delivery rate: handler completions per second over the
    // window from the first publish to the last delivery - i.e. it charges the engine for any drain tail after the
    // publisher stopped, which is exactly where a consumer that cannot keep up shows up.
    val publishRate  = if (publishSeconds > 0) r.publishedMeasured / publishSeconds else 0.0
    val deliveryRate = if (deliverySeconds > 0) r.deliveredMeasured / deliverySeconds else 0.0
    val perWatcher   = if (cfg.watchers > 0) deliveryRate / cfg.watchers else 0.0

    val shortfall = r.expectedMeasured - r.deliveredMeasured

    def p(q: Double): Long = Probes.percentile(latencies, q)

    val mean = if (latencies.isEmpty) 0.0 else latencies.map(_.toDouble).sum / latencies.size

    val lagMax       = if (lagSorted.isEmpty) 0L else lagSorted.last
    val livenessDead = lagMax >= 5000

    val problems = List(
      Option.when(shortfall > 0)(s"undelivered=$shortfall"),
      Option.when(warmupBad > 0)(s"warmupNotOk=$warmupBad"),
      Option.when(r.droppedMessages > 0)(s"drops=${r.droppedMessages}"),
      Option.when(r.slowConsumerEvents > 0)(s"slowConsumers=${r.slowConsumerEvents}"),
      Option.when(r.recreations > 0)(s"recreations>=${r.recreations}"),
      Option.when(livenessDead)(s"livenessMaxLag=${lagMax}ms"),
      Option.when(r.outgoingDiscards > 0)(s"outgoingDiscards=${r.outgoingDiscards}"),
      // Percentiles taken from a truncated prefix describe the start of the run, not the run.
      Option.when(r.deliveredMeasured > latencies.size.toLong)(
        s"latencySamplesTruncated=${r.deliveredMeasured - latencies.size}"
      )
    ).flatten

    def line(label: String, value: String) = f"  $label%-22s $value"

    val sb = new StringBuilder
    sb.append("\n=== nats-effect steady-state KV update test ===\n")
    sb.append(line("mode", cfg.mode.name) + "\n")
    sb.append(line("scenario", cfg.scenario.name) + "\n")
    sb.append(line("watchers", cfg.watchers.toString) + "\n")
    sb.append(
      line("bucket", f"${cfg.keys} keys x ${cfg.valueSizeBytes / 1024.0}%.1f KiB = ${cfg.totalBytes / 1024.0 / 1024.0}%.0f MiB") + "\n"
    )
    sb.append(line("compute threads", cfg.computeThreads.toString) + "\n")
    sb.append(line("handler cost", s"~${cfg.handlerSpinMicros} us/msg (spin)") + "\n")
    sb.append(line("target rate", targetRate(cfg)) + "\n")
    sb.append("--- phases ---\n")
    sb.append(line("populate", fmt(r.populateTime.toNanos / 1e9)) + "\n")
    sb.append(line("warmups", s"ok=$warmupOk notOk=$warmupBad") + "\n")
    sb.append(line("prime updates", cfg.primeUpdates.toString) + "\n")
    sb.append("--- throughput ---\n")
    sb.append(line("published", f"${r.publishedMeasured} in ${fmt(publishSeconds)} ($publishRate%.0f updates/s)") + "\n")
    sb.append(
      line(
        "delivered",
        f"${r.deliveredMeasured}/${r.expectedMeasured} in ${fmt(deliverySeconds)} " +
          f"($deliveryRate%.0f msg/s total, $perWatcher%.0f msg/s per watcher)"
      ) + "\n"
    )
    sb.append(line("drain tail", fmt(math.max(0.0, deliverySeconds - publishSeconds))) + "\n")
    sb.append("--- end-to-end latency (publish call -> handler entry) ---\n")
    sb.append(
      line(
        "latency us",
        if (latencies.isEmpty) "no samples"
        else f"p50=${p(50)} p90=${p(90)} p99=${p(99)} p999=${p(99.9)} max=${latencies.last} mean=$mean%.0f (n=${latencies.size})"
      ) + "\n"
    )
    val over50  = latencies.count(_ > 50000)
    val over200 = latencies.count(_ > 200000)
    val over500 = latencies.count(_ > 500000)
    val pct     = (n: Int) => if (latencies.isEmpty) 0.0 else 100.0 * n / latencies.size
    sb.append(
      line("slow arrivals", f">50ms: $over50 (${pct(over50)}%.2f%%)  >200ms: $over200 (${pct(over200)}%.2f%%)  >500ms: $over500") + "\n"
    )

    // The put ack is NOT a component of the end-to-end latency above, despite sharing its start point: the server
    // fans a stored entry out to the watchers before it answers the publisher, so a delivery routinely completes
    // *before* its own put ack returns (which is why e2e p50 can sit below ack p50). It is a same-on-both-engines
    // reference for what the write path costs, not a term to subtract.
    val acks = r.ackLatenciesMicros.sorted
    sb.append(
      line(
        "kv.put ack (parallel)",
        if (acks.isEmpty) "no samples"
        else
          f"p50=${Probes.percentile(acks, 50)} p99=${Probes.percentile(acks, 99)} max=${acks.last}" +
            " (write-path reference, not subtractable)"
      ) + "\n"
    )
    sb.append("--- cost ---\n")
    sb.append(
      line(
        "protocol msgs",
        f"out=${r.clientOutMsgs} in=${r.clientInMsgs}" +
          (if (r.publishedMeasured > 0) f" (${r.clientOutMsgs.toDouble / r.publishedMeasured}%.2f out/update)" else "")
      ) + "\n"
    )
    sb.append(
      line(
        "idle traffic",
        if (cfg.quiet.toSeconds > 0)
          f"${r.quietOutMsgs} client->server msgs in ${cfg.quiet.toSeconds}s quiet " +
            f"(${r.quietOutMsgs.toDouble / cfg.quiet.toSeconds / math.max(1, cfg.watchers)}%.2f/s per watcher)"
        else "not measured (quietSec=0)"
      ) + "\n"
    )
    sb.append(line("heap", s"peak(used)=${r.peakHeapBytes / 1024 / 1024} MiB live(after gc)=${r.liveHeapBytes / 1024 / 1024} MiB") + "\n")
    sb.append(
      line("liveness lag", s"p50=${Probes.percentile(lagSorted, 50)}ms p99=${Probes.percentile(lagSorted, 99)}ms max=${lagMax}ms") + "\n"
    )
    sb.append("--- pathology ---\n")
    sb.append(line("dropped / slowCons", s"${r.droppedMessages} / ${r.slowConsumerEvents}") + "\n")
    sb.append(line("outgoing discards", r.outgoingDiscards.toString) + "\n")
    sb.append(line("recreations", s">=${r.recreations}") + "\n")
    sb.append(line("errors / exceptions", s"${r.errorsOccurred} / ${r.exceptionsOccurred}") + "\n")
    sb.append("--- verdict ---\n")
    sb.append(s"  ${if (problems.isEmpty) "CLEAN" else s"DEGRADED (${problems.mkString(", ")})"}\n")

    sb.append(
      "RESULT_JSON " + jsonLine(
        "mode"             -> quote(cfg.mode.name),
        "scenario"         -> quote(cfg.scenario.name),
        "watchers"         -> cfg.watchers.toString,
        "keys"             -> cfg.keys.toString,
        "valueSizeBytes"   -> cfg.valueSizeBytes.toString,
        "computeThreads"   -> cfg.computeThreads.toString,
        "spinMicros"       -> cfg.handlerSpinMicros.toString,
        "targetRateHz"     -> cfg.rateHz.toString,
        "periodMs"         -> cfg.periodMillis.toString,
        "updates"          -> r.publishedMeasured.toString,
        "publishMillis"    -> r.publishWallTime.toMillis.toString,
        "publishRate"      -> math.round(publishRate).toString,
        "delivered"        -> r.deliveredMeasured.toString,
        "expected"         -> r.expectedMeasured.toString,
        "deliveryMillis"   -> r.deliveryWallTime.toMillis.toString,
        "deliveryRate"     -> math.round(deliveryRate).toString,
        "perWatcherRate"   -> math.round(perWatcher).toString,
        "latP50Micros"     -> p(50).toString,
        "latP90Micros"     -> p(90).toString,
        "latP99Micros"     -> p(99).toString,
        "latP999Micros"    -> p(99.9).toString,
        "latMaxMicros"     -> (if (latencies.isEmpty) "0" else latencies.last.toString),
        "latMeanMicros"    -> math.round(mean).toString,
        "latSamples"       -> latencies.size.toString,
        "latOver50ms"      -> over50.toString,
        "latOver200ms"     -> over200.toString,
        "latOver500ms"     -> over500.toString,
        "ackP50Micros"     -> Probes.percentile(acks, 50).toString,
        "ackP99Micros"     -> Probes.percentile(acks, 99).toString,
        "clientOutMsgs"    -> r.clientOutMsgs.toString,
        "clientInMsgs"     -> r.clientInMsgs.toString,
        "warmupOk"         -> warmupOk.toString,
        "dropped"          -> r.droppedMessages.toString,
        "slowConsumers"    -> r.slowConsumerEvents.toString,
        "outgoingDiscards" -> r.outgoingDiscards.toString,
        "recreations"      -> r.recreations.toString,
        "errors"           -> r.errorsOccurred.toString,
        "exceptions"       -> r.exceptionsOccurred.toString,
        "livenessLagMaxMs" -> lagMax.toString,
        "quietSec"         -> cfg.quiet.toSeconds.toString,
        "quietOutMsgs"     -> r.quietOutMsgs.toString,
        "peakHeapMiB"      -> (r.peakHeapBytes / 1024 / 1024).toString,
        "liveHeapMiB"      -> (r.liveHeapBytes / 1024 / 1024).toString,
        "messagesHandled"  -> r.messagesHandled.toString,
        "degraded"         -> problems.nonEmpty.toString
      ) + "\n"
    )

    sb.result()
  }

  /** The publish cadence actually in force, so the human report and RESULT_JSON cannot disagree about what was measured. */
  private def targetRate(cfg: Config): String =
    if (cfg.periodMillis > 0) s"1 update per ${cfg.periodMillis}ms"
    else if (cfg.rateHz == 0) "max (publisher-bound)"
    else s"${cfg.rateHz} updates/s"

  private def fmt(seconds: Double): String = f"$seconds%.2f s"

  private def quote(s: String): String = "\"" + s + "\""

  private def jsonLine(fields: (String, String)*): String =
    fields.map { case (k, v) => s"${quote(k)}: $v" }.mkString("{", ", ", "}")
}
