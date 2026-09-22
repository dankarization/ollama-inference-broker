"""Dependency-light source-policy validation shared by runtime observers."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import math
import os
from pathlib import Path
from typing import Any, Iterator


class SourcePolicyError(ValueError):
    pass


RETENTION_DEFAULTS = {
    "retention_enabled": False,
    "acked_body_retention_seconds": 3_600,
    "unacked_terminal_retention_seconds": 7 * 86_400,
    "failed_cancelled_retention_seconds": 7 * 86_400,
    "metadata_retention_seconds": 180 * 86_400,
    "receipt_retention_seconds": 365 * 86_400,
    "tombstone_retention_seconds": 5 * 365 * 86_400,
    "inline_budget_bytes": 128 * 1024 * 1024,
}


@contextmanager
def source_policy_write_lock(path: str | Path) -> Iterator[None]:
    """Serialize every cooperating writer for one source-policy file."""
    policy_path = Path(path)
    lock_path = policy_path.with_name(f".{policy_path.name}.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def normalize_source_policy(raw: Any) -> dict[str, dict[str, Any]]:
    """Validate a policy document and apply its documented defaults."""
    if not isinstance(raw, dict):
        raise SourcePolicyError("policy must contain an object 'sources'")
    sources = raw.get("sources")
    if not isinstance(sources, dict):
        raise SourcePolicyError("policy must contain an object 'sources'")
    normalized: dict[str, dict[str, Any]] = {}
    for name, entry in sources.items():
        if not isinstance(name, str) or not name:
            raise SourcePolicyError("source names must be non-empty strings")
        if not isinstance(entry, dict):
            raise SourcePolicyError(f"source {name!r} must be an object")
        unknown = set(entry) - {
            "enabled", "weight", "admission_allowed",
            "producer_storage_enabled", "producer_storage_mode", "ack_required",
            "compaction_enabled", "legacy_result_fallback",
            "compaction_grace_seconds", "quarantine_grace_seconds",
            *RETENTION_DEFAULTS,
        }
        if unknown:
            raise SourcePolicyError(
                f"source {name!r} has unknown keys: {', '.join(sorted(unknown))}"
            )
        enabled = entry.get("enabled", True)
        admission_allowed = entry.get("admission_allowed", True)
        weight = entry.get("weight", 1.0)
        producer_storage_enabled = entry.get("producer_storage_enabled", False)
        producer_storage_mode = entry.get("producer_storage_mode", "broker_temporary")
        ack_required = entry.get("ack_required", False)
        compaction_enabled = entry.get("compaction_enabled", False)
        legacy_result_fallback = entry.get("legacy_result_fallback", True)
        compaction_grace_seconds = entry.get("compaction_grace_seconds", 86400)
        quarantine_grace_seconds = entry.get("quarantine_grace_seconds", 86400)
        retention = {
            name: entry.get(name, default)
            for name, default in RETENTION_DEFAULTS.items()
        }
        if not isinstance(enabled, bool):
            raise SourcePolicyError(f"source {name!r} enabled must be a boolean")
        if not isinstance(admission_allowed, bool):
            raise SourcePolicyError(
                f"source {name!r} admission_allowed must be a boolean"
            )
        for field, value in (
            ("producer_storage_enabled", producer_storage_enabled),
            ("ack_required", ack_required),
            ("compaction_enabled", compaction_enabled),
            ("legacy_result_fallback", legacy_result_fallback),
            ("retention_enabled", retention["retention_enabled"]),
        ):
            if not isinstance(value, bool):
                raise SourcePolicyError(f"source {name!r} {field} must be a boolean")
        if (
            not isinstance(producer_storage_mode, str)
            or producer_storage_mode not in {"broker_temporary", "producer_owned", "hybrid"}
        ):
            raise SourcePolicyError(
                f"source {name!r} producer_storage_mode must be broker_temporary, "
                "producer_owned, or hybrid"
            )
        for field, value in (
            ("compaction_grace_seconds", compaction_grace_seconds),
            ("quarantine_grace_seconds", quarantine_grace_seconds),
            ("acked_body_retention_seconds", retention["acked_body_retention_seconds"]),
            ("unacked_terminal_retention_seconds", retention["unacked_terminal_retention_seconds"]),
            ("failed_cancelled_retention_seconds", retention["failed_cancelled_retention_seconds"]),
            ("metadata_retention_seconds", retention["metadata_retention_seconds"]),
            ("receipt_retention_seconds", retention["receipt_retention_seconds"]),
            ("tombstone_retention_seconds", retention["tombstone_retention_seconds"]),
        ):
            try:
                finite = math.isfinite(value)
            except (OverflowError, TypeError):
                finite = False
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not finite
                or value < 0
            ):
                raise SourcePolicyError(
                    f"source {name!r} {field} must be a finite non-negative number"
                )
        inline_budget_bytes = retention["inline_budget_bytes"]
        if (
            isinstance(inline_budget_bytes, bool)
            or not isinstance(inline_budget_bytes, int)
            or inline_budget_bytes <= 0
        ):
            raise SourcePolicyError(
                f"source {name!r} inline_budget_bytes must be a positive integer"
            )
        if compaction_enabled and not producer_storage_enabled:
            raise SourcePolicyError(
                f"source {name!r} cannot enable compaction without producer storage"
            )
        if retention["retention_enabled"] and not (
            producer_storage_enabled
            and ack_required
            and compaction_enabled
            and not legacy_result_fallback
        ):
            raise SourcePolicyError(
                f"source {name!r} retention requires producer storage, mandatory ACK, "
                "compaction, and disabled legacy fallback"
            )
        if retention["retention_enabled"]:
            body_deadlines = (
                retention["acked_body_retention_seconds"],
                retention["unacked_terminal_retention_seconds"],
                retention["failed_cancelled_retention_seconds"],
            )
            if any(
                value > retention["metadata_retention_seconds"]
                for value in body_deadlines
            ):
                raise SourcePolicyError(
                    f"source {name!r} body retention must not exceed metadata retention"
                )
            if retention["tombstone_retention_seconds"] < max(
                retention["metadata_retention_seconds"],
                retention["receipt_retention_seconds"],
            ):
                raise SourcePolicyError(
                    f"source {name!r} tombstone retention must cover metadata and receipts"
                )
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight <= 0:
            raise SourcePolicyError(f"source {name!r} weight must be a positive number")
        normalized[name] = {
            "enabled": enabled,
            "weight": float(weight),
            "admission_allowed": admission_allowed,
            "producer_storage_enabled": producer_storage_enabled,
            "producer_storage_mode": producer_storage_mode,
            "ack_required": ack_required,
            "compaction_enabled": compaction_enabled,
            "legacy_result_fallback": legacy_result_fallback,
            "compaction_grace_seconds": float(compaction_grace_seconds),
            "quarantine_grace_seconds": float(quarantine_grace_seconds),
            **{
                field: float(value)
                for field, value in retention.items()
                if field.endswith("_seconds")
            },
            "retention_enabled": retention["retention_enabled"],
            "inline_budget_bytes": inline_budget_bytes,
        }
    return normalized
