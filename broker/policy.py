"""Dependency-light source-policy validation shared by runtime observers."""

from __future__ import annotations

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
        unknown = set(entry) - {"enabled", "weight"}
        if unknown:
            raise SourcePolicyError(
                f"source {name!r} has unknown keys: {', '.join(sorted(unknown))}"
            )
        enabled = entry.get("enabled", True)
        weight = entry.get("weight", 1.0)
        if not isinstance(enabled, bool):
            raise SourcePolicyError(f"source {name!r} enabled must be a boolean")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight <= 0:
            raise SourcePolicyError(f"source {name!r} weight must be a positive number")
        normalized[name] = {"enabled": enabled, "weight": float(weight)}
    return normalized
