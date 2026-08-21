# Local Inference Broker

Private control plane for one GPU-backed Ollama host. It prevents competing local workloads from racing for VRAM and gives the work that matters to a person priority over scheduled and batch jobs.

## Problem

Ollama shares loaded model weights, but every concurrent request consumes its own KV cache and generation or image buffers. Two different models need both sets of weights too. Large contexts make this especially easy to exhaust. Ollama can load and serve models, but it does not understand the priority of OpenClaw, Hermes, or pipeline work.

The broker is the single admission point for all local inference. Callers do not call Ollama directly.

```text
OpenClaw chats ─┐
Hermes ─────────┼──> Local Inference Broker ───> Ollama ───> MAIN-PC GPU / VRAM
Pipelines ──────┘          queue, priority,
                              limits, locks
```

Cloud-routed workloads remain outside this path: they do not consume the MAIN-PC GPU and must not be serialized with local work.

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
