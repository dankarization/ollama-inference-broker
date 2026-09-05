"""Build an immutable rollback release that keeps storage metadata private."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path


EXPECTED_PARENT_SERVICE_SHA256 = (
    "b8c9d7b716c825fe80443cc8ad7c1a4fc53560466c3400d0938ac3e76b1c6806"
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
    "        for field in LEGACY_HIDDEN_STORAGE_FIELDS:\n"
    "            data.pop(field, None)\n"
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def harden_legacy_service(source: str) -> str:
    if source.count(DECLARATION_ANCHOR) != 1:
        raise ValueError("legacy service logging anchor is missing or ambiguous")
    if source.count(SERIALIZER_ANCHOR) != 1:
        raise ValueError("legacy service serializer anchor is missing or ambiguous")
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
    compile(hardened, "broker/service.py", "exec")
    return hardened


def prepare_rollback_release(
    source_release: Path,
    output_release: Path,
    *,
    expected_service_sha256: str = EXPECTED_PARENT_SERVICE_SHA256,
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
    original_bytes = source_service.read_bytes()
    original_hash = _sha256(original_bytes)
    if original_hash != expected_service_sha256:
        raise ValueError(
            "source service does not match the reviewed rollback parent: "
            + original_hash
        )
    hardened = harden_legacy_service(original_bytes.decode("utf-8"))

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
        target_service = staged / "broker" / "service.py"
        mode = target_service.stat().st_mode & 0o7777
        target_service.write_text(hardened, encoding="utf-8")
        os.chmod(target_service, mode)
        os.replace(staged, output_release)
        directory = os.open(output_release.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)

    hardened_hash = _sha256((output_release / "broker" / "service.py").read_bytes())
    return {
        "source_release": str(source_release),
        "rollback_release": str(output_release),
        "source_service_sha256": original_hash,
        "rollback_service_sha256": hardened_hash,
        "hidden_storage_fields": len(LEGACY_HIDDEN_STORAGE_FIELDS),
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
