# Producer-owned storage protocol

This stage is additive. Legacy admission and result fetch remain available. A
job enters the producer-owned lifecycle only when its source is explicitly
allowlisted with `producer_storage_enabled=true` **and** admission includes a
`producer_storage` capability. The shipped production policy keeps
`ack_required=false`, `legacy_result_fallback=true`, and
`compaction_enabled=false`.

All producer capability requests, full/compact status and receipt reads,
cancel/retry mutations, ACKs, and maintenance calls require
`Authorization: Bearer …` matching the owner-only file configured by
`BROKER_STORAGE_TOKEN_FILE`. The broker never logs the header or token. If the
file is not configured, the new storage API fails closed while legacy APIs
continue to work. Production creates the token locally with mode `0600`; it is
not stored in Git or operational evidence.

## Admission and canonical evidence

`POST /v1/jobs` accepts the existing body plus:

```json
{
  "producer_storage": {
    "producer_attempt_id": "attempt-1",
    "input": {
      "mode": "producer_owned",
      "storage_ref": "producer://inputs/object-42",
      "content_hash": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "byte_size": 123
    },
    "result": {
      "mode": "producer_owned",
      "schema_version": "result-v1"
    }
  }
}
```

Hashes cover UTF-8 JSON encoded with sorted keys and compact separators. Input
hash and byte size must match the canonical broker payload. A repeated
correlation with a different profile, request kind, or capability identity
fails closed and preserves the original job. After a correlation is admitted
with `producer_storage`, a retry that omits the capability is also a conflict;
it cannot recover the producer-owned job through the unauthenticated legacy
admission path. An exact retry is resolved against the durable capability
identity before current source storage/admission policy, so a lost admission
response remains recoverable after a safe policy rollback. Legacy sources
outside the dedicated Olya/Syncopia idempotency contracts may still admit
multiple jobs with the same `external_id` when no producer-storage capability
is present.

`POST /v1/jobs/{id}/input-received` confirms the declared input reference,
hash, size, source, and producer attempt. `GET /v1/jobs/{id}/status` returns a
payload-free lifecycle and artifact view. Existing `GET /v1/jobs/{id}` remains
the legacy full-result API for legacy jobs; a job admitted with
`producer_storage` requires the storage bearer token before this endpoint or
its attempt history can return data. Unauthenticated history, audit, and
correlation reads omit producer-storage jobs, while authenticated reads retain
the complete operational evidence.

## Result ACK

After atomically persisting and reading back a result, the producer sends:

```json
{
  "job_id": "broker-job-id",
  "producer": "olya-vision",
  "producer_attempt_id": "attempt-1",
  "storage_ref": "olya://vision/object/42/result/sha256-...",
  "result_hash": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "result_bytes": 123,
  "schema_version": "result-v1",
  "persisted_at": "2026-09-05T04:00:00Z"
}
```

`POST /v1/jobs/{id}/ack` validates path/job/source/attempt/hash/bytes/schema and
an internal URI without query, fragment, or credentials. The exact repeat
returns the same durable receipt. A disagreement returns HTTP 409 with
`ack_conflict`, records bounded payload-free diagnostics, and retains the full
broker result. `GET /v1/jobs/{id}/receipt` recovers the receipt after a timeout
or restart. An identical conflicting retry is idempotent: it does not append a
second conflict row or audit event.

## Compaction guards

`POST /v1/maintenance/compact` supports `preview`, `quarantine`, and `compact`.
Mutation requires `confirm=true`. All four source guards must be true:

1. `producer_storage_enabled=true`;
2. `ack_required=true`;
3. `compaction_enabled=true`;
4. `legacy_result_fallback=false`.

The job must also have persisted `ack_required=true` and
`legacy_result_fallback=false` from its own admission, be `completed`, have a
matching durable receipt and ACKed result artifact, and pass the configured
grace periods. A completed input is execution garbage and is not a prerequisite
for result compaction: the broker retains its hash/byte evidence but clears the
inline payload at the completed transition. Enabling destructive source flags
later never adopts results admitted under additive/legacy defaults.
`quarantine` only marks eligibility. `compact` removes inline payload/result
after quarantine.
Each compact transaction is capped by both `limit` (default 100 rows) and
`max_bytes` (default 16 MiB, hard maximum 64 MiB); an individually oversized
row is held for explicit operator handling rather than violating the WAL
budget.
Running, cancel-requested, unacknowledged, legacy, unresolved, and conflicting
jobs cannot qualify for ACKed-result compaction. The initial laptop rollout
leaves these mutations disabled.

## Retention-bounded storage

Retention is a second opt-in gate. `retention_enabled` defaults to `false`, so
deploying the code does not compact existing data. An enabled source must set
finite values for:

- `acked_body_retention_seconds`;
- `unacked_terminal_retention_seconds`;
- `failed_cancelled_retention_seconds`;
- `metadata_retention_seconds`;
- `receipt_retention_seconds`;
- `tombstone_retention_seconds`;
- `inline_budget_bytes`.

The default source budget is 128 MiB. A separate 768 MiB global terminal-body
budget applies backpressure to new admissions when retained terminal bodies
reach either limit. Both checks use trigger-maintained terminal usage and do
not count queued, running, cancel-requested, or retryable payloads: those bodies
are required for execution, so their growth remains visible but cannot be
discarded or mistaken for reclaimable retention usage.

New admissions also preserve a filesystem reserve: the larger of 2 GiB and 5%
of the database filesystem. The check uses current free space plus the incoming
canonical payload size, applies to active payloads without imposing a queue-size
quota, and fails closed when filesystem capacity cannot be read. Idempotent
replay of an already durable job remains available under disk pressure.

Queued, running, cancel-requested, and still-retryable payloads always remain
inline and byte-identical. Completed payloads are cleared immediately because a
completed job cannot be retried. A producer-owned completed result remains
fail-closed until a matching durable ACK; a broker-temporary completed result
expires after `unacked_terminal_retention_seconds`. Failed/cancelled payloads
remain retryable until `failed_cancelled_retention_seconds`, then become
`metadata_only` and explicit retry fails closed. The retained metadata includes
identity, source/state/timestamps, attempts, hashes/byte sizes, bounded error
diagnostics, and receipts where present.

The dispatcher runs at most one small retention cycle per minute. Each source
cycle quarantines/compacts at most 25 rows and 4 MiB, then expires at most 25
metadata rows and receipts. Purged jobs leave a compact identity/hash
tombstone, preserving dedicated-source idempotency without payload or result
bodies. Tombstones also have a finite TTL. `storage_source_usage` is maintained by triggers, so admission and
health do not scan the payload-heavy jobs table.

Physical shrink requires one approved out-of-place repack. Repacked databases
use SQLite `auto_vacuum=INCREMENTAL`; later bounded maintenance reclaims up to
1024 pages per cycle. The live file is never vacuumed in place.

## WAL policy and evidence

The broker configures `wal_autocheckpoint=4096` pages, a 64 MiB
`journal_size_limit`, a 128 MiB alert budget, and periodic non-destructive
`PASSIVE` checkpoints. Runtime never runs `TRUNCATE` or a full `VACUUM`.
Incremental vacuum is enabled only on a separately verified repacked database.
Inspect `GET /v1/storage/health` and `GET /v1/metrics` for DB/WAL bytes, configured
limits, and the last checkpoint result. The unacked and quarantined health
counts use dedicated partial indexes, so polling does not scan payload-heavy
legacy history while holding the broker lock.
Startup and the migration CLI reject pragma values outside SQLite's signed
integer ranges and verify that SQLite applied both values exactly.
The shared migration also backfills producer-storage audit visibility and
builds the same public audit/history partial indexes used after restart, so a
copy preflight exercises the complete startup schema cost. The queued-at
compatibility backfill uses a persistent partial index. After the one-time
migration, startup checks an empty index instead of scanning every payload row.
Storage health uses partial indexes and trigger-maintained source usage counters.

Apply the schema to an explicit safe copy and write a payload-free report:

```bash
python3 -m broker.migration \
  --database /path/to/broker.schema-copy.sqlite3 \
  --report /path/to/migration-report.json
```

The command refuses a missing path. Its report contains only schema names,
counts, integrity results, and file/WAL policy metrics. It also refuses a
`--report` path that resolves to the database itself through a direct path,
symlink, or hard link.

Forecast retention against a read-only database or copy:

```bash
python3 -m broker.retention_migration \
  --database /path/to/broker.read-only-copy.sqlite3 \
  --report /path/to/retention-forecast.json
```

Historical adoption uses one source-scoped manifest per producer. Each entry
contains job identity, producer attempt, input/result CAS references, canonical
hashes/byte sizes, persisted timestamps, and readback timestamps. The tool
recomputes hashes from copied inline JSON and rejects active, failed,
cancelled, missing, cross-source, or conflicting jobs. It never mutates the
source database:

See [`retention-manifest.example.json`](retention-manifest.example.json) for
the complete manifest shape. Placeholder hashes are not valid evidence.

```bash
python3 -m broker.retention_migration \
  --database /path/to/broker.read-only-copy.sqlite3 \
  --policy /path/to/staged-sources.json \
  --manifest /path/to/olya-vision.manifest.json \
  --output-database /path/to/broker.repacked.sqlite3 \
  --report /path/to/repack-report.json
```

`--floor-output` creates a clearly marked, non-deployable lower-bound artifact
by removing terminal bodies without durability evidence. Use it only to prove
the physical floor imposed by queued/running data. It is not a migration
candidate.

For the terminal-lifecycle contract, build a deployable out-of-place database
with the exact live policy (including disabled historical sources):

```bash
python3 -m broker.retention_migration \
  --database /path/to/broker.snapshot.sqlite3 \
  --policy /path/to/staged-sources.json \
  --terminal-output /path/to/broker.terminal-repacked.sqlite3 \
  --report /path/to/terminal-repack-report.json
```

The tool hashes bodies before clearing them, preserves active/FIFO and recent
retry-body SHA-256 identities, uses a disposable staging file, and publishes
only after `integrity_check`, FK, WAL=0, and strict `<2 GiB` checks pass.

Stage safe flags in the **live** policy without copying the repository's sample
weights over operator changes:

```bash
python3 -m broker.storage_policy \
  --policy ~/.local/state/ollama-inference-broker/sources.json \
  --output /path/to/staged-sources.json
```

The tool fails unless normalized `enabled`, `weight`, and
`admission_allowed` values are byte-for-byte equivalent before and after. It
allowlists only currently present approved producer sources, leaves unknown
and `uncensored-eval` sources untouched, and always stages ACK optional,
legacy fallback on, and compaction off. `--apply` performs an fsync + atomic
replace after the operator preserves the original policy for rollback. The
apply aborts if any process replaces or modifies the live policy after it was
read. It also takes the same adjacent owner-only lock used by runtime
source-control writers, closing the compare/replace race and preserving
concurrent source-control changes.

## Rollout and rollback

Before restart, drain new claims while allowing the current lease to finish,
then create a consistent SQLite backup, restore-read it, preserve the previous
release and policy, and record state counts. Because the additive job columns
would be visible through the unpatched parent's legacy `SELECT *` serializer,
prepare an immutable metadata-safe rollback release before migration:

```bash
python3 -m broker.rollback_guard \
  --source-release /path/to/reviewed-parent-release \
  --output-release /path/to/rollback-protected-release
```

The tool accepts only the complete reviewed parent release manifest and its
`broker/service.py` hash, then copies the release without Git/cache state. It
injects the complete legacy-hidden storage-field filter, suppresses payload/result
bodies for producer-storage rows, marks rollback-time audit events with the
persisted producer-storage visibility bit, suppresses those audit rows from
legacy public reads, and blocks unauthenticated per-job/bulk mutation plus
legacy dispatch for producer-storage jobs. After copying, it verifies the
staged pre-patch tree against the same complete manifest, closing source-copy
races. It then compiles the patched source, fsyncs every regular file and
directory before publishing the artifact, and reports only paths and hashes.
The output must be outside the source release tree. Build the source from the
exact reviewed parent archive; any added, missing, changed, or concurrently
replaced release file fails the manifest check. Verify the rollback copy with
`compileall` and the legacy API test before continuing.
Deploy a new immutable release and atomically switch the service working
directory. After restart verify HTTP health,
payload-free storage health, unchanged source weights, state counts, receipt
tables, `quick_check`, `foreign_key_check`, and journal logs.

Rollback atomically activates the prepared metadata-safe parent release and
original policy, then restarts the service. The additive tables/columns are
intentionally left in place; the guarded legacy serializer hides them. Never
activate the unpatched parent after schema migration, and never delete or
reverse-migrate the production DB during rollback.

### Retention rollout checklist

1. Freeze the source-policy fingerprint and take a consistent DB+WAL backup.
2. Generate producer manifests from durable stores; independently read back
   every referenced object before setting `readback_at`.
3. Run forecast and reject any manifest error, active-row selection, conflict,
   or source mismatch.
4. Build the out-of-place database, require `integrity_check=ok`, zero foreign
   key violations, WAL `0`, exact queued/running identity/FIFO parity, and the
   expected source/state counts.
5. Preserve the old DB and WAL as the canonical rollback pair. Stop the broker,
   re-check queue/leases, atomically swap the verified repack, then enable
   retention for one source only.
6. Restart and prove HTTP readiness, storage health, usage counters, bounded
   WAL, producer read/ACK compatibility, and automatic incremental shrink.
7. Roll back by stopping the broker, restoring the preserved DB+WAL and policy,
   activating the previous immutable release, and rechecking queue identity.
   Do not reverse-migrate or VACUUM the rollback database.
