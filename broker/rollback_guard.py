"""Build an immutable rollback release that keeps storage metadata private."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import tempfile
from pathlib import Path


EXPECTED_PARENT_SERVICE_SHA256 = (
    "b8c9d7b716c825fe80443cc8ad7c1a4fc53560466c3400d0938ac3e76b1c6806"
)
EXPECTED_PARENT_RELEASE_SHA256 = (
    "4cc520b1fb1633a08105e90d47fb95fc6a5cbacb360558cbe9fe10e8de33fea9"
)
LEGACY_HIDDEN_STORAGE_FIELDS = frozenset({
    "input_storage_mode", "input_ref", "input_hash", "input_bytes",
    "input_received_at", "result_storage_mode", "result_ref", "result_hash",
    "result_bytes", "delivery_state", "delivery_attempt_count",
    "last_delivery_at", "acked_at", "retention_until", "compaction_after",
    "artifact_schema_version", "compaction_state", "producer_attempt_id",
    "ack_required", "legacy_result_fallback", "quarantined_at", "compacted_at",
})
DECLARATION_ANCHOR = 'LOGGER = logging.getLogger("ollama_inference_broker.audit")\n'
SERIALIZER_ANCHOR = (
    '        data.pop("priority", None)  # inert historic column is never public\n'
)
SERIALIZER_GUARD = (
    "        producer_storage = (\n"
    "            data.get('producer_attempt_id') is not None\n"
    "            or data.get('input_storage_mode') != 'broker_temporary'\n"
    "            or data.get('result_storage_mode') != 'broker_temporary'\n"
    "        )\n"
    "        for field in LEGACY_HIDDEN_STORAGE_FIELDS:\n"
    "            data.pop(field, None)\n"
    "        if producer_storage:\n"
    "            data.pop('payload', None)\n"
    "            data.pop('result_json', None)\n"
    "            return data\n"
)
AUDIT_METADATA_ANCHOR = "        safe_metadata = metadata or {}\n"
AUDIT_METADATA_GUARD = (
    "        producer_storage = bool(\n"
    "            job_id is not None and self.db.execute(\n"
    "                \"SELECT 1 FROM jobs WHERE id=? AND (\"\n"
    "                \"producer_attempt_id IS NOT NULL \"\n"
    "                \"OR input_storage_mode!='broker_temporary' \"\n"
    "                \"OR result_storage_mode!='broker_temporary')\",\n"
    "                (job_id,),\n"
    "            ).fetchone()\n"
    "        )\n"
)
AUDIT_INSERT_ANCHOR = (
    "        self.db.execute(\n"
    "            \"INSERT INTO audit_events(occurred,event_type,job_id,source,attempt_no,\"\n"
    "            \"from_state,to_state,reason,metadata_json) VALUES(?,?,?,?,?,?,?,?,?)\",\n"
    "            (timestamp, event_type, job_id, source, attempt_no, from_state,\n"
    "             to_state, reason, json.dumps(safe_metadata, sort_keys=True)),\n"
    "        )\n"
)
AUDIT_INSERT_GUARD = (
    "        self.db.execute(\n"
    "            \"INSERT INTO audit_events(occurred,event_type,job_id,source,attempt_no,\"\n"
    "            \"from_state,to_state,reason,metadata_json,producer_storage) \"\n"
    "            \"VALUES(?,?,?,?,?,?,?,?,?,?)\",\n"
    "            (timestamp, event_type, job_id, source, attempt_no, from_state,\n"
    "             to_state, reason, json.dumps(safe_metadata, sort_keys=True),\n"
    "             producer_storage),\n"
    "        )\n"
)
MUTATION_JOB_ANCHOR = (
    '                "SELECT state,source,attempt_count FROM jobs WHERE id=?", (job_id,)\n'
)
MUTATION_JOB_GUARD = (
    '                "SELECT state,source,attempt_count FROM jobs WHERE id=? "\n'
    '                "AND producer_attempt_id IS NULL "\n'
    '                "AND input_storage_mode=\'broker_temporary\' "\n'
    '                "AND result_storage_mode=\'broker_temporary\'", (job_id,)\n'
)
BULK_CANCEL_ANCHOR = (
    '                "SELECT id,attempt_count FROM jobs WHERE source=? AND state=\'queued\' "\n'
    '                "ORDER BY queued_at,id", (source,),\n'
)
BULK_CANCEL_GUARD = (
    '                "SELECT id,attempt_count FROM jobs WHERE source=? AND state=\'queued\' "\n'
    '                "AND producer_attempt_id IS NULL "\n'
    '                "AND input_storage_mode=\'broker_temporary\' "\n'
    '                "AND result_storage_mode=\'broker_temporary\' "\n'
    '                "ORDER BY queued_at,id", (source,),\n'
)
BULK_RETRY_ANCHOR = (
    '                "SELECT id,attempt_count FROM jobs WHERE source=? AND state=\'failed\' "\n'
    '                "ORDER BY finished,id", (source,),\n'
)
BULK_RETRY_GUARD = (
    '                "SELECT id,attempt_count FROM jobs WHERE source=? AND state=\'failed\' "\n'
    '                "AND producer_attempt_id IS NULL "\n'
    '                "AND input_storage_mode=\'broker_temporary\' "\n'
    '                "AND result_storage_mode=\'broker_temporary\' "\n'
    '                "ORDER BY finished,id", (source,),\n'
)
DISPATCH_ANCHOR = (
    '            "FROM jobs INDEXED BY jobs_queued_candidates WHERE state=\'queued\'"\n'
)
DISPATCH_GUARD = (
    '            "FROM jobs INDEXED BY jobs_queued_candidates WHERE state=\'queued\' "\n'
    '            "AND producer_attempt_id IS NULL "\n'
    '            "AND input_storage_mode=\'broker_temporary\' "\n'
    '            "AND result_storage_mode=\'broker_temporary\'"\n'
)
AUDIT_READER_ANCHOR = (
    ") -> list[dict[str, Any]]:\n"
    "    clauses: list[str] = []\n"
    "    values: list[Any] = []\n"
)
AUDIT_READER_GUARD = (
    ") -> list[dict[str, Any]]:\n"
    "    clauses: list[str] = ['producer_storage=0']\n"
    "    values: list[Any] = []\n"
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _release_manifest_sha256(root: Path) -> str:
    """Hash every shipped path, executable bit and byte without host metadata."""
    ignored = {".git", "__pycache__", ".pytest_cache"}
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root)
        if any(part in ignored for part in relative.parts):
            continue
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if stat.S_ISREG(metadata.st_mode):
            mode = b"100755" if metadata.st_mode & stat.S_IXUSR else b"100644"
            content = path.read_bytes()
        elif stat.S_ISLNK(metadata.st_mode):
            mode = b"120000"
            content = os.readlink(path).encode("utf-8")
        else:
            raise ValueError(f"source release contains unsupported path: {relative}")
        entry = mode + b" " + relative.as_posix().encode("utf-8") + b"\0" + content
        digest.update(len(entry).to_bytes(8, "big"))
        digest.update(entry)
    return digest.hexdigest()


def _fsync_tree(root: Path) -> None:
    directories: list[Path] = []
    for current, _subdirectories, filenames in os.walk(root, followlinks=False):
        directory = Path(current)
        directories.append(directory)
        for filename in filenames:
            path = directory / filename
            if not stat.S_ISREG(os.lstat(path).st_mode):
                continue
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    for directory in reversed(directories):
        descriptor = os.open(
            directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def harden_legacy_service(source: str) -> str:
    if source.count(DECLARATION_ANCHOR) != 1:
        raise ValueError("legacy service logging anchor is missing or ambiguous")
    if source.count(SERIALIZER_ANCHOR) != 1:
        raise ValueError("legacy service serializer anchor is missing or ambiguous")
    if source.count(AUDIT_METADATA_ANCHOR) != 1:
        raise ValueError("legacy service audit metadata anchor is missing or ambiguous")
    if source.count(AUDIT_INSERT_ANCHOR) != 1:
        raise ValueError("legacy service audit insert anchor is missing or ambiguous")
    if source.count(MUTATION_JOB_ANCHOR) != 2:
        raise ValueError("legacy per-job mutation anchor is missing or ambiguous")
    if source.count(BULK_CANCEL_ANCHOR) != 1:
        raise ValueError("legacy bulk cancel anchor is missing or ambiguous")
    if source.count(BULK_RETRY_ANCHOR) != 1:
        raise ValueError("legacy bulk retry anchor is missing or ambiguous")
    if source.count(DISPATCH_ANCHOR) != 1:
        raise ValueError("legacy service dispatch anchor is missing or ambiguous")
    declaration = (
        DECLARATION_ANCHOR
        + "LEGACY_HIDDEN_STORAGE_FIELDS = frozenset({\n"
        + "".join(f'    {field!r},\n' for field in sorted(LEGACY_HIDDEN_STORAGE_FIELDS))
        + "})\n"
    )
    hardened = source.replace(DECLARATION_ANCHOR, declaration, 1)
    hardened = hardened.replace(
        SERIALIZER_ANCHOR, SERIALIZER_ANCHOR + SERIALIZER_GUARD, 1,
    )
    hardened = hardened.replace(
        AUDIT_METADATA_ANCHOR,
        AUDIT_METADATA_ANCHOR + AUDIT_METADATA_GUARD,
        1,
    )
    hardened = hardened.replace(AUDIT_INSERT_ANCHOR, AUDIT_INSERT_GUARD, 1)
    hardened = hardened.replace(MUTATION_JOB_ANCHOR, MUTATION_JOB_GUARD)
    hardened = hardened.replace(BULK_CANCEL_ANCHOR, BULK_CANCEL_GUARD, 1)
    hardened = hardened.replace(BULK_RETRY_ANCHOR, BULK_RETRY_GUARD, 1)
    hardened = hardened.replace(DISPATCH_ANCHOR, DISPATCH_GUARD, 1)
    compile(hardened, "broker/service.py", "exec")
    return hardened


def harden_legacy_analytics(source: str) -> str:
    if source.count(AUDIT_READER_ANCHOR) != 1:
        raise ValueError("legacy audit reader anchor is missing or ambiguous")
    hardened = source.replace(AUDIT_READER_ANCHOR, AUDIT_READER_GUARD, 1)
    compile(hardened, "broker/analytics.py", "exec")
    return hardened


def prepare_rollback_release(
    source_release: Path,
    output_release: Path,
    *,
    expected_service_sha256: str = EXPECTED_PARENT_SERVICE_SHA256,
    expected_release_sha256: str = EXPECTED_PARENT_RELEASE_SHA256,
) -> dict[str, object]:
    source_release = source_release.resolve()
    output_release = output_release.resolve()
    source_service = source_release / "broker" / "service.py"
    if not source_service.is_file():
        raise ValueError("source release has no broker/service.py")
    if output_release.exists():
        raise ValueError("output rollback release already exists")
    if source_release == output_release:
        raise ValueError("output rollback release must differ from source release")
    if source_release in output_release.parents:
        raise ValueError("output rollback release must be outside source release")
    original_bytes = source_service.read_bytes()
    original_hash = _sha256(original_bytes)
    if original_hash != expected_service_sha256:
        raise ValueError(
            "source service does not match the reviewed rollback parent: "
            + original_hash
        )
    source_release_hash = _release_manifest_sha256(source_release)
    if source_release_hash != expected_release_sha256:
        raise ValueError(
            "source release does not match the complete reviewed rollback parent: "
            + source_release_hash
        )
    output_release.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(tempfile.mkdtemp(
        prefix=f".{output_release.name}.", dir=output_release.parent,
    ))
    staged = temporary_root / "release"
    try:
        shutil.copytree(
            source_release,
            staged,
            symlinks=True,
            ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"),
        )
        staged_source_hash = _release_manifest_sha256(staged)
        if staged_source_hash != expected_release_sha256:
            raise ValueError(
                "staged release does not match the complete reviewed rollback parent: "
                + staged_source_hash
            )
        target_service = staged / "broker" / "service.py"
        hardened = harden_legacy_service(target_service.read_text(encoding="utf-8"))
        mode = target_service.stat().st_mode & 0o7777
        target_service.write_text(hardened, encoding="utf-8")
        os.chmod(target_service, mode)
        target_analytics = staged / "broker" / "analytics.py"
        analytics_mode = target_analytics.stat().st_mode & 0o7777
        target_analytics.write_text(
            harden_legacy_analytics(target_analytics.read_text(encoding="utf-8")),
            encoding="utf-8",
        )
        os.chmod(target_analytics, analytics_mode)
        _fsync_tree(staged)
        os.replace(staged, output_release)
        directory = os.open(output_release.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)

    hardened_hash = _sha256((output_release / "broker" / "service.py").read_bytes())
    rollback_release_hash = _release_manifest_sha256(output_release)
    return {
        "source_release": str(source_release),
        "rollback_release": str(output_release),
        "source_service_sha256": original_hash,
        "rollback_service_sha256": hardened_hash,
        "source_release_sha256": source_release_hash,
        "staged_source_release_sha256": staged_source_hash,
        "rollback_release_sha256": rollback_release_hash,
        "hidden_storage_fields": len(LEGACY_HIDDEN_STORAGE_FIELDS),
        "producer_storage_dispatch_blocked": True,
        "producer_storage_mutations_blocked": True,
        "producer_storage_bodies_suppressed": True,
        "producer_storage_audit_classified": True,
        "producer_storage_audit_hidden": True,
        "payloads_in_report": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy and harden the reviewed parent release for safe rollback",
    )
    parser.add_argument("--source-release", required=True, type=Path)
    parser.add_argument("--output-release", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = prepare_rollback_release(args.source_release, args.output_release)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
