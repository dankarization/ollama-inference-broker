# Whisper executor: opt-in integration

The `stt-whisper` lane runs speech recognition under the same durable
`mainpc-gpu` claim as Ollama jobs. The feature is disabled unless all ASR settings
are supplied. Existing jobs, profiles and production configuration are retained.

## Request flow

1. The server-side STT proxy uploads audio to the authenticated broker API.
2. The broker stages a bounded private artifact and queues its ID, SHA-256 and size.
3. Dispatch waits for Ollama's resident models to expire naturally. It never
   unloads them on behalf of ASR. The selected ASR lane may wait while they remain.
4. A request-specific GPU fence is committed before sending audio to the worker.
5. The worker unloads its own Whisper model, then acknowledges that request ID.
6. The broker stores raw text in its private spool; SQLite stores a result reference.
7. The STT proxy retrieves and acknowledges the result, then applies its existing
   glossary once. The worker's initial prompt is a separate decoding input.

## Configuration

Set these only in a private deployment environment, not in this repository:

| Variable | Purpose |
| --- | --- |
| `BROKER_ASR_SPOOL` | Dedicated owner-only directory, mode 0700 |
| `BROKER_ASR_TOKEN_FILE` | Owner-only token file for the STT proxy, at least 32 characters |
| `BROKER_WHISPER_URL` | Fixed HTTP(S) origin of the private worker |
| `BROKER_WHISPER_TOKEN_FILE` | Separate owner-only worker credential |

Add source `stt-whisper` to the approved dispatch policy or dispatch allowlist.
Do not modify existing sources, weights, storage policies or queued jobs.
Keep the worker reachable only from the broker and an explicitly approved
temporary diagnostic client. Neither endpoint is intended for public exposure.
Use a trusted private transport or HTTPS. No secrets belong in CLI arguments.

The worker must implement authenticated `/v1/status` and `/v1/transcribe`, report
model `whisper-medium-int8-mainpc-cuda`, and accept UUID `X-Request-Id` headers.
Status must include `request_tracking`, `active_jobs`, `model_loaded`,
`last_request_id`, and `last_request_gpu_released`. An idle status without the
matching completed request ID is **not** a release acknowledgement.

## Authenticated API

Every request below requires `Authorization: Bearer ...` from the ASR token file.

- `POST /v1/transcriptions`: raw binary audio, `Content-Length` (up to 64 MiB),
  `Idempotency-Key` (16–100 ASCII letters/digits/underscore/hyphen). Returns 202
  with `id` and `state`. Same key and same input reuse the durable job; conflicting
  input is rejected. A terminal acknowledged result cannot be resurrected by retry.
- `GET /v1/transcriptions/{id}`: state, `gpu_release_pending`, and raw `result`
  once completed. Missing/acknowledged result returns 410.
- `POST /v1/transcriptions/{id}/cancel`: queued work is cancelled immediately;
  running work keeps the GPU claim until the worker finishes.
- `POST /v1/transcriptions/{id}/ack`: delete this terminal job's audio/result
  artifacts after recording a durable receipt. This does not release the GPU
  fence; a completed warm result can be acknowledged immediately.

The legacy queue submission and mutation routes reject ASR operations. Public
status/dashboard data contains only artifact references, never raw audio or text.
The spool allows 512 MiB of staged files and 32 outstanding ASR jobs. Uploads are
serialized, capped at 60 seconds and two admitted HTTP handlers. Abandoned files
older than 24 hours are pruned on the next authenticated upload; queued/running
artifacts and all artifacts while a fence exists are preserved. There is no
background deletion timer. Admission reserves up to 2 MiB per future result
within the same spool cap. Idempotent retries do not reserve a duplicate copy.

## Ambiguous failures and rollback

Timeout/disconnection never proves the GPU is free. The durable
`gpu_executor_fences` row blocks **all** further GPU dispatch until the worker
acknowledges the same request with no active job and no loaded Whisper model.
`/healthz` then includes `gpu_executor: awaiting_worker_release` and its job ID.
The STT client must not fall back to another GPU backend for such an outcome.

The worker durably records its in-flight UUID before inference. A replacement
can acknowledge that same UUID only after confirming the old process exited
and its own engine is clean. Inaccessible process state remains fail-closed.
Cold and warm successful transcripts complete immediately; the model remains
resident for `MODEL_IDLE_SECONDS` (Whisper default 1200 seconds). The idle monitor
unloads under the inference lock and publishes request-specific release proof.
Warm reuse requires the worker UUID to match the existing durable fence.

For a legacy restart without a usable journal, stop dispatch and verify the old
worker process exited and its GPU allocation is gone. The local recovery CLI
requires both explicit operator confirmations, an exact request/job identity,
no running broker jobs and authenticated proof of an unloaded clean worker:

```sh
python3 -m broker.asr_recovery --database /path/to/broker.sqlite3 \
  --request-id EXACT-FENCED-UUID --job-id EXACT-FENCED-JOB-UUID \
  --worker-url http://configured-worker:port --worker-token-file /private/worker.token \
  --confirm-old-executor-exited --confirm-dispatch-stopped
```

The CLI records the operator decision durably and deletes only that exact fence
in one transaction. It is not a network administrative endpoint and never
edits source policy, stops processes or bypasses an unconfirmed native unload.

Before rollback: stop new ASR admissions, cancel/drain queued ASR work, wait for
the current worker and all fences to resolve, and save the queue database/config.
Only then disable ASR settings or restore an older broker release. An older
release does not understand the new fence table and must not run with a fence.

This does not bring existing out-of-band GigaAM/Ollama callers under the queue.
Migrating them is a separate cutover gate. Do not claim global GPU exclusivity
until those callers are accounted for. CUDA consumption and quality still need
a real worker canary; 4 GiB free is an admission threshold, not a measurement.

## Validation

Run the normal Linux suite:

```sh
python3 -B -m unittest discover -s tests
```

Run the cross-project HTTP contract with an isolated checkout of the matching
STT branch (no real GPU, production database or production endpoints):

```sh
STT_SOURCE_ROOT=/path/to/stt-checkout python3 -B -m unittest discover -s tests
```

The ASR tests cover idempotency/conflicts, artifact integrity, authorization,
legacy-route rejection, cancellation, timeout fences, restart recovery, resident
Ollama preservation, and raw-result isolation. The contract uses both real HTTP
servers and the STT client with a fake engine.
