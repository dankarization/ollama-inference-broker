"""Safely stage producer-storage flags without changing scheduler controls."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .policy import normalize_source_policy, source_policy_write_lock


SOURCE_STORAGE_MODES = {
    "shutterstock-video": "producer_owned",
    "olya-vision": "producer_owned",
    "olya-decision": "producer_owned",
    "syncopia-telegram-memory": "hybrid",
}
SCHEDULER_FIELDS = ("enabled", "weight", "admission_allowed")


class ConcurrentPolicyUpdateError(RuntimeError):
    pass


def read_snapshot(path: Path) -> tuple[bytes, tuple[int, int, int, int, int, str]]:
    """Read a file and bind its bytes to one stable filesystem identity."""
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        content = handle.read()
        after = os.fstat(handle.fileno())
    fields = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns")
    if any(getattr(before, field) != getattr(after, field) for field in fields):
        raise ConcurrentPolicyUpdateError("policy changed while it was being read")
    try:
        current = path.stat()
    except OSError as exc:
        raise ConcurrentPolicyUpdateError(
            "policy disappeared while it was being read"
        ) from exc
    if any(getattr(after, field) != getattr(current, field) for field in fields):
        raise ConcurrentPolicyUpdateError("policy was replaced while it was being read")
    return content, (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        hashlib.sha256(content).hexdigest(),
    )


def scheduler_fingerprint(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    normalized = normalize_source_policy(raw)
    return {
        source: {field: entry[field] for field in SCHEDULER_FIELDS}
        for source, entry in normalized.items()
    }


def stage_storage_policy(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Add only safe rollout flags and prove scheduling fields are unchanged."""
    before = scheduler_fingerprint(raw)
    staged = json.loads(json.dumps(raw))
    configured = staged["sources"]
    allowlisted: list[str] = []
    for source, mode in SOURCE_STORAGE_MODES.items():
        if source not in configured:
            continue
        configured[source].update({
            "producer_storage_enabled": True,
            "producer_storage_mode": mode,
            "ack_required": False,
            "compaction_enabled": False,
            "legacy_result_fallback": True,
            "retention_enabled": False,
        })
        allowlisted.append(source)
    if not allowlisted:
        raise ValueError("policy contains none of the approved producer storage sources")
    after = scheduler_fingerprint(staged)
    if before != after:
        raise ValueError("storage policy staging changed scheduler or admission controls")
    normalize_source_policy(staged)
    return staged, {
        "scheduler_controls_unchanged": True,
        "allowlisted_sources": sorted(allowlisted),
        "ack_required": False,
        "compaction_enabled": False,
        "legacy_result_fallback": True,
        "retention_enabled": False,
    }


def write_atomic(
    path: Path,
    value: dict[str, Any],
    *,
    expected_fingerprint: tuple[int, int, int, int, int, str] | None = None,
) -> None:
    with source_policy_write_lock(path):
        try:
            mode = path.stat().st_mode & 0o7777
        except OSError as exc:
            raise ConcurrentPolicyUpdateError(
                "policy disappeared after staging; refusing to replace it"
            ) from exc
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(value, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, mode)
            if expected_fingerprint is not None:
                _, current_fingerprint = read_snapshot(path)
                if current_fingerprint != expected_fingerprint:
                    raise ConcurrentPolicyUpdateError(
                        "policy changed after staging; refusing to replace it"
                    )
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def same_file(left: Path, right: Path) -> bool:
    """Detect identical destinations, including symlinks and hard links."""
    try:
        return left.samefile(right)
    except OSError:
        return left.resolve() == right.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add safe producer-storage flags while preserving source controls",
    )
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.output is not None:
        parser.error("use either --apply or --output")
    if not args.policy.is_file():
        parser.error("--policy must name an existing JSON file")
    if args.output is not None and same_file(args.policy, args.output):
        parser.error("use --apply instead of writing --output over --policy")
    try:
        encoded, fingerprint = read_snapshot(args.policy)
    except ConcurrentPolicyUpdateError as exc:
        parser.error(str(exc))
    raw = json.loads(encoded)
    staged, report = stage_storage_policy(raw)
    if args.apply:
        try:
            write_atomic(args.policy, staged, expected_fingerprint=fingerprint)
        except ConcurrentPolicyUpdateError as exc:
            parser.error(str(exc))
    elif args.output is not None:
        args.output.write_text(json.dumps(staged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
