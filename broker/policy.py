"""Dependency-light source-policy validation shared by runtime observers."""

from __future__ import annotations

import math
from typing import Any


class SourcePolicyError(ValueError):
    pass


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
        ):
            if not isinstance(value, bool):
                raise SourcePolicyError(f"source {name!r} {field} must be a boolean")
        if producer_storage_mode not in {"broker_temporary", "producer_owned", "hybrid"}:
            raise SourcePolicyError(
                f"source {name!r} producer_storage_mode must be broker_temporary, "
                "producer_owned, or hybrid"
            )
        for field, value in (
            ("compaction_grace_seconds", compaction_grace_seconds),
            ("quarantine_grace_seconds", quarantine_grace_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise SourcePolicyError(
                    f"source {name!r} {field} must be a finite non-negative number"
                )
        if compaction_enabled and not producer_storage_enabled:
            raise SourcePolicyError(
                f"source {name!r} cannot enable compaction without producer storage"
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
        }
    return normalized
