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

- `POST /v1/jobs` with `{ "profile":"interactive", "kind":"chat|generate", "payload":{...} }` returns a persisted job (`202`). `source` defaults to `profile`. Non-fixed sources must also send an integer `priority` from 1 to 10.
- `GET /v1/jobs/{id}`, `POST /v1/jobs/{id}/cancel`, `GET /v1/metrics` expose lifecycle and MAIN-PC `/api/ps` model/VRAM data.
- `/api/chat` and `/api/generate` currently return `501`: an Ollama-compatible streaming proxy is deliberately deferred until the client migration contract is tested.

The server chooses profiles, model, context, output cap and keepalive; a caller-supplied `model`, `num_ctx`, `num_predict`, or `keep_alive` cannot escalate those limits. SQLite uses WAL. Dispatch is one job at a time, strict priority then FIFO. A smaller number is more important; this is deliberately not aging-based, so lower priority work cannot jump a waiting higher-priority job.

| Source / config key | Fixed priority |
| --- | ---: |
| OpenClaw interactive/open session (`interactive`) | 1 |
| OpenClaw cron (`cron`) | 2 |
| Shutterstock (`shutterstock`) | 3 |
| Olya (`olya`) | 4 |

The literal key is `olya`; it labels a source and does not imply an integration. These four priorities are broker-owned: callers may omit `priority` or repeat the fixed value, but cannot override it. Other sources provide `source` plus a validated whole-number priority from 1 (highest) to 10 (lowest), for example `{ "profile":"batch-video", "source":"maintenance", "priority":7, ... }`. On model change it wakes MAIN-PC, observes `/api/ps`, unloads incompatible models, requests/readiness-checks the target, and only then runs the job. A stale running lease is requeued on restart.

Safe canary acceptance: submit mock/staging interactive, cron, Shutterstock, Olya and dynamic-priority jobs; verify one remote request at a time, priority/FIFO order, WOL/readiness and unload-before-switch telemetry; restart with an expired lease and observe requeue; then cancel a queued job. No live caller is migrated before those checks pass and rollback is simply stopping the broker with no route changes.

## Development

`python -m unittest discover -s tests -v` runs adapter-mocked tests and never sends WOL or Ollama traffic.

## Initial policy

- Admit one active local GPU workload at a time.
- Priority: 1 is highest and 10 is lowest. Fixed policy is interactive/open session 1, cron 2, Shutterstock 3, and `olya` 4; other sources select 1–10 at enqueue time.
- Before a model change, unload an incompatible loaded model, wait until the target model is ready, then start the request.
- Default to one request per model. Each workload profile caps context and maximum output.
- Keep a model resident for 2–5 minutes only when its queue has follow-up work; otherwise unload it.
- Make cancellation, queue position, dispatch decisions, and model-switch reasons explicit.

## Scope

The broker owns admission, ordering, model residency, request profiles, and operational telemetry. Ollama remains the inference runtime. It is not a replacement for cloud routing, task orchestration, or caller-specific business logic.

See [ROADMAP.md](ROADMAP.md) for the staged delivery plan.
