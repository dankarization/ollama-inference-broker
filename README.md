# Local Inference Broker

Private control plane for one GPU-backed Ollama host. It prevents competing local workloads from racing for VRAM and gives the work that matters to a person priority over scheduled and batch jobs. It is intentionally an isolated MVP: no current caller or production route is changed.

## Problem

Ollama shares loaded model weights, but every concurrent request consumes its own KV cache and generation or image buffers. Two different models need both sets of weights too. Large contexts make this especially easy to exhaust. Ollama can load and serve models, but it does not understand the priority of OpenClaw, Hermes, or pipeline work.

The broker is the single admission point for all local inference. Callers do not call Ollama directly.

```text
OpenClaw always-on host: broker + SQLite WAL      MAIN-PC (wakeable executor)
OpenClaw / M101 jobs ──> queue, policy, API ──> Ollama ──> one GPU / VRAM
                         resource: mainpc-gpu        ^
                                                    WOL when a job dispatches
```

Cloud-routed workloads, CPU jobs, and current Whisper GPU workers remain outside this path: they do not consume `mainpc-gpu` (Whisper will get its own explicit resource profile later) and must not be serialized with it. The broker runs outside MAIN-PC so it survives a WOL cycle; MAIN-PC has no queue state.

## MVP API and runbook

Run only in a lab/staging shell: `MAINPC_MAC=2c:f0:5d:74:6a:44 python -m broker`. It binds localhost by default and does not edit OpenClaw, Ollama, Shutterstock, M101, or MAIN-PC configuration.

- `POST /v1/jobs` with `{ "profile":"interactive", "kind":"chat|generate", "payload":{...} }` returns a persisted job (`202`).
- `GET /v1/jobs/{id}`, `POST /v1/jobs/{id}/cancel`, `GET /v1/metrics` expose lifecycle and MAIN-PC `/api/ps` model/VRAM data.
- `/api/chat` and `/api/generate` currently return `501`: an Ollama-compatible streaming proxy is deliberately deferred until the client migration contract is tested.

The server chooses profiles, model, context, output cap and keepalive; a caller-supplied `model`, `num_ctx`, `num_predict`, or `keep_alive` cannot escalate those limits. SQLite uses WAL. Dispatch is one job at a time, priority then FIFO: interactive, photo, cron, batch-video. On model change it wakes MAIN-PC, observes `/api/ps`, unloads incompatible models, requests/readiness-checks the target, and only then runs the job. A stale running lease is requeued on restart.

Safe canary acceptance: submit mock/staging interactive, photo, cron and batch jobs; verify one remote request at a time, priority/FIFO order, WOL/readiness and unload-before-switch telemetry; restart with an expired lease and observe requeue; then cancel a queued job. No live caller is migrated before those checks pass and rollback is simply stopping the broker with no route changes.

## Development

`python -m unittest discover -s tests -v` runs adapter-mocked tests and never sends WOL or Ollama traffic.

## Initial policy

- Admit one active local GPU workload at a time.
- Priority order: interactive human request, local photo processing, cron, then batch video.
- Before a model change, unload an incompatible loaded model, wait until the target model is ready, then start the request.
- Default to one request per model. Each workload profile caps context and maximum output.
- Keep a model resident for 2–5 minutes only when its queue has follow-up work; otherwise unload it.
- Make cancellation, queue position, dispatch decisions, and model-switch reasons explicit.

## Scope

The broker owns admission, ordering, model residency, request profiles, and operational telemetry. Ollama remains the inference runtime. It is not a replacement for cloud routing, task orchestration, or caller-specific business logic.

See [ROADMAP.md](ROADMAP.md) for the staged delivery plan.
