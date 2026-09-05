# Producer-owned storage protocol

This stage is additive. Legacy admission and result fetch remain available. A
job enters the producer-owned lifecycle only when its source is explicitly
allowlisted with `producer_storage_enabled=true` **and** admission includes a
`producer_storage` capability. The shipped production policy keeps
`ack_required=false`, `legacy_result_fallback=true`, and
`compaction_enabled=false`.

All producer capability requests, compact status/receipt reads, ACKs, and
maintenance calls require `Authorization: Bearer …` matching the owner-only
file configured by `BROKER_STORAGE_TOKEN_FILE`. The broker never logs the
header or token. If the file is not configured, the new storage API fails
closed while legacy APIs continue to work. Production creates the token
locally with mode `0600`; it is not stored in Git or operational evidence.

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
correlation with different capability identity fails closed and preserves the
original job.

`POST /v1/jobs/{id}/input-received` confirms the declared input reference,
hash, size, source, and producer attempt. `GET /v1/jobs/{id}/status` returns a
payload-free lifecycle and artifact view. Existing `GET /v1/jobs/{id}` remains
the legacy full-result API.

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
or restart.

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
grace periods. Enabling destructive source flags later never adopts jobs
admitted under additive/legacy defaults. `quarantine` only marks eligibility.
`compact` removes inline payload/result after quarantine.
Each compact transaction is capped by both `limit` (default 100 rows) and
`max_bytes` (default 16 MiB, hard maximum 64 MiB); an individually oversized
row is held for explicit operator handling rather than violating the WAL
budget.
Running, cancel-requested, unacknowledged, legacy, unresolved, and conflicting
jobs cannot qualify. The initial laptop rollout leaves these mutations
disabled. There is no TTL purge, VACUUM, rebuild, or historical adoption in
this stage.

## WAL policy and evidence

The broker configures `wal_autocheckpoint=4096` pages, a 64 MiB
`journal_size_limit`, a 128 MiB alert budget, and periodic non-destructive
`PASSIVE` checkpoints. It never runs `TRUNCATE` or `VACUUM`. Inspect
`GET /v1/storage/health` and `GET /v1/metrics` for DB/WAL bytes, configured
limits, and the last checkpoint result.

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
read, preserving concurrent source-control changes.

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

The tool accepts only the reviewed parent `broker/service.py` hash, copies the
release without Git/cache state, injects the same 22-field legacy response
filter, compiles the patched source, and reports only paths and hashes. Verify
the rollback copy with `compileall` and the legacy API test before continuing.
Deploy a new immutable release and atomically switch the service working
directory. After restart verify HTTP health,
payload-free storage health, unchanged source weights, state counts, receipt
tables, `quick_check`, `foreign_key_check`, and journal logs.

Rollback atomically activates the prepared metadata-safe parent release and
original policy, then restarts the service. The additive tables/columns are
intentionally left in place; the guarded legacy serializer hides them. Never
activate the unpatched parent after schema migration, and never delete or
reverse-migrate the production DB during rollback.
