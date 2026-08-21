# Roadmap

## Phase 0 — Contract and inventory

Define the broker API, caller identities, workload profiles, priority classes, cancellation behavior, and model inventory. Identify every direct local Ollama caller and document the supported MAIN-PC health signals.

Ready to advance when every intended caller has a migration owner and no unresolved ambiguity remains around local versus cloud routing.

## Phase 1 — MVP: safe serialized admission (implemented in isolated form)

Build a single broker process with a persistent queue and one active local workload. Implement priority dispatch, exclusive Ollama access, target-model readiness checks, controlled unload/switch, profile limits for context/output, and bounded keepalive.

Ready to advance when representative interactive, photo, cron, and batch-video requests cannot overlap on the GPU; cancelled or failed work releases the lock; and restart recovery leaves no stuck workload.

The MVP is an always-on OpenClaw-host control plane with a SQLite WAL queue for the `mainpc-gpu` resource; MAIN-PC is only a WOL-started Ollama executor. It excludes cloud and CPU paths plus existing Whisper GPU workers, which require a later separate resource profile. It performs no migration or production configuration change.

## Phase 2 — Integrations

Move OpenClaw, Hermes, and local pipelines to the broker API. Block or alert on new direct Ollama calls. Preserve separate cloud-photo concurrency, because it does not use MAIN-PC VRAM.

Ready to advance when production callers use the broker exclusively for local inference and fallback behavior is documented and exercised.

First add and test an Ollama-compatible streaming proxy contract, then migrate one explicit canary caller. Do not redirect a caller merely because its process happens to run on OpenClaw or M101.

## Phase 3 — Observability and operations

Expose queue depth and wait time, active workload, priority, selected model, loaded models and VRAM from Ollama `/api/ps`, profile-limit rejections, cancellations, and model-switch reasons. Add health checks, structured logs, and operational dashboards/alerts.

Ready to advance when an operator can explain every delayed, rejected, cancelled, or switched request from telemetry alone.

## Phase 4 — Controlled rollout and tuning

Roll out by caller class: interactive traffic first, then photo, cron, and batch video. Tune profile limits and keepalive using observed queue and VRAM data. Keep a documented rollback path to the serialized lock-and-queue mode.

Complete when VRAM races are absent under representative load, interactive work meets its wait-time target, and a rollback drill succeeds without orphaning work.
