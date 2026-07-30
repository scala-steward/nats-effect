# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A steady-state workload in the (manual-only, unpublished) `loadtest` module. `mode=steady` brings the watcher fleet up on a
  pre-populated bucket, then publishes live updates at a controlled rate and times every delivery end to end (`put` call to handler
  entry), reporting latency percentiles, slow-arrival counts, delivery throughput, protocol messages per update and idle pull traffic
  over a silent interval. The existing cold-backlog workload is unchanged and remains the default (`mode=warmup`). Arguments and
  report fields are documented in the README "Load testing" section.

### Fixed

- The paced consume engine (`KeyValue.watchPaced` / `watchAllPaced`, `StreamContext.createOrderedPacedConsumer` /
  `getPacedConsumerContext`) mistook the server's routine expiry of each pull request for a pull terminus arriving a full window early.
  The engine armed its window deadline when the pull was published, while the server starts the same `expiresIn` clock only on receipt,
  so the client always timed out first and the server's `408` landed at the head of the next window — the exact shape the 500 ms
  early-terminus guard throttles. On a healthy idle consumer this cost a 500 ms non-draining sleep and a redundant pull roughly once
  per window: at a low update rate ~1.7% of deliveries were delayed by up to 500 ms, and idle pull traffic was double what the pacing
  needs. The window deadline now carries 500 ms of slack, so a server expiry is observed inside its own window, while a genuinely early
  terminus (a deleted consumer answers immediately) is still throttled. Measured after the fix: no arrival above 50 ms and half the
  pull traffic (details in `docs/loadtest-results.md`). No API or configuration change; the callback engine is unaffected.

## [1.3.1] - 2026-07-28

### Added

- Pure KV key validation on the `KeyValue` companion object: `validateKey` (write/read keys) and
  `validateKeyWildcardAllowed` (`watch`/`keys` filter patterns), returning `Either[InvalidKeyError, Unit]` with the
  offending key and the jnats reason. Both delegate to the jnats validators (KV key grammar plus strict subject
  grammar), so callers can validate keys before a write instead of catching and sniffing `IllegalArgumentException`
  from `put`.

## [1.3.0] - 2026-07-23

### Added

- `KeyValue.keysDetailed(...)` (three overloads mirroring `keys`) returning `KeysResult(keys, warmup)`, which exposes
  the `Warmup.Result` of the underlying warmup. This lets callers detect when a key listing was cut short by the
  warmup `timeout` (`Warmup.Result.Timeout`) versus completed fully (`Warmup.Result.Success`).

### Fixed

- `KeyValue.keys(...)` could silently return a **partial** key set when warmup hit its `timeout` before all pending
  messages were drained, with no signal to the caller — a silent-data-loss hazard for any `list`-style operation built
  on `keys`. Fixed additively (non-breaking): `keys` behaviour is unchanged and it now delegates to `keysDetailed`;
  callers that need completeness guarantees should switch to `keysDetailed` and inspect `warmup` (e.g. raise, retry with
  a longer timeout, or return partial-with-a-flag). No existing caller breaks. See Option A in `TASK.md`.
