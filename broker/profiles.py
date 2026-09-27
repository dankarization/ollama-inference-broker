from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    model: str
    max_context: int
    max_output: int
    keep_alive_seconds: int
    # These are admission/dispatch limits, not caller-controlled Ollama options.
    max_concurrency: int = 1
    min_interval_seconds: int = 0
    request_timeout_seconds: int = 300
    max_images: int = 0
    max_schema_bytes: int = 0
    # Retained for durable jobs admitted with legacy evaluation profiles.
    default_context: int | None = None


# This is deliberately server-owned. Callers can select a profile, never a model
# or unbounded generation parameters.
PROFILES = {
    "interactive": Profile("interactive", "nemotron3:33b", 16_384, 2_048, 180),
    "cron": Profile("cron", "nemotron3:33b", 8_192, 1_024, 120),
    # Preserve the original profile name for already-queued agent turns.
    "openclaw": Profile("openclaw", "qwen3.8:ad-iq2-xs", 131_072, 16_384, 300,
                        request_timeout_seconds=900),
    "openclaw-gemma4": Profile("openclaw-gemma4", "gemma4:12b", 262_144, 16_384, 300,
                               request_timeout_seconds=900, max_images=16),
    "openclaw-qwen3-vl": Profile("openclaw-qwen3-vl", "qwen3-vl:30b", 212_992, 16_384, 300,
                                request_timeout_seconds=900, max_images=16),
    "openclaw-nemotron3": Profile("openclaw-nemotron3", "nemotron3:33b", 131_072, 8_192, 300,
                                 request_timeout_seconds=900, max_images=16),
    "openclaw-unc-rvn-iq2xxs": Profile("openclaw-unc-rvn-iq2xxs", "qwen3.8:unc-rvn-iq2xxs",
                                      131_072, 16_384, 300, request_timeout_seconds=900),
    "openclaw-ministral3": Profile("openclaw-ministral3", "frob/ministral-3:14b-thinking-q4_K_M",
                                   262_144, 16_384, 300, request_timeout_seconds=900, max_images=16),
    "openclaw-nemotron-nano": Profile("openclaw-nemotron-nano", "nemotron-3-nano:30b-a3b-q4_K_M",
                                      262_144, 16_384, 300, request_timeout_seconds=900),
    "openclaw-gpt-oss": Profile("openclaw-gpt-oss", "gpt-oss:20b", 131_072, 16_384, 300,
                                request_timeout_seconds=900),
    # Shutterstock photos stay on cloud/OmniRoute and deliberately have no
    # broker profile. Only the local video workload may acquire MAIN-PC.
    # The video lane is multimodal: one chunk arrives as base64 frames plus a
    # JSON Schema contract, so the profile admits bounded media input.
    "shutterstock-video": Profile(
        "shutterstock-video", "nemotron3:33b", 16_384, 1_024, 120,
        max_images=12, max_schema_bytes=32_768, request_timeout_seconds=600,
    ),
    # `olya` is intentionally only a source/profile key. Do not infer an
    # integration from this name; its model can be changed in broker config.
    "olya": Profile("olya", "nemotron3:33b", 8_192, 1_024, 120),
    # Photos-only Olya keeps its verified two-model routing. Both profiles use
    # one dedicated weighted source; callers may select only these exact
    # server-owned profiles via the bounded synchronous VLM endpoint.
    "olya-vision-gemma": Profile(
        "olya-vision-gemma", "gemma4:12b", 245_760, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=600,
        max_images=16, max_schema_bytes=65_536,
    ),
    "olya-vision-qwen": Profile(
        "olya-vision-qwen", "qwen3-vl:30b", 212_992, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=600,
        max_images=16, max_schema_bytes=65_536,
    ),
    # Text-only apartment decision lane. The exact local model is pinned
    # server-side; callers cannot replace it or expand its runtime limits.
    "olya-decision-qwen38": Profile(
        "olya-decision-qwen38", "qwen3.8:ad-iq2-xs", 32_768, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=300,
        max_schema_bytes=16_384,
    ),
    # Phase-2 Telegram-memory extraction is a separate text-only source.  The
    # 64k context and schema/freeform thinking modes are enforced again by its
    # compatibility contract; callers cannot borrow the Olya lane or widen limits.
    "syncopia-memory-qwen38": Profile(
        "syncopia-memory-qwen38", "qwen3.8:ad-iq2-xs", 65_536, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=900,
        max_schema_bytes=65_536,
    ),
    # Seven isolated, text-only comparison lanes.  They deliberately share one
    # source so their evaluations cannot borrow any production caller's share.
    # 64k is the normal comparison default; 128k is an explicit stress ceiling.
    "uncensored-eval-rvn-iq2m": Profile(
        "uncensored-eval-rvn-iq2m", "qwen3.8:unc-rvn-iq2m", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-rvn-iq2s": Profile(
        "uncensored-eval-rvn-iq2s", "qwen3.8:unc-rvn-iq2s", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-rvn-iq2xs": Profile(
        "uncensored-eval-rvn-iq2xs", "qwen3.8:unc-rvn-iq2xs", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-rvn-iq2xxs": Profile(
        "uncensored-eval-rvn-iq2xxs", "qwen3.8:unc-rvn-iq2xxs", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-huihui-q2kxl": Profile(
        "uncensored-eval-huihui-q2kxl", "qwen3.8:unc-huihui-q2kxl", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-unleashed-q2kxl": Profile(
        "uncensored-eval-unleashed-q2kxl", "qwen3.8:unc-unleashed-q2kxl", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "uncensored-eval-hauhau-iq2m": Profile(
        "uncensored-eval-hauhau-iq2m", "qwen3.8:unc-hauhau-iq2m", 131_072, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=7_200, default_context=65_536,
    ),
    "batch-video": Profile("batch-video", "nemotron3:33b", 8_192, 512, 120),
    # This is an intentionally separate source from the active Shutterstock
    # worker.  It is the only VLM/media contract eligible for a future canary.
    "shutterstock-canary": Profile(
        "shutterstock-canary", "qwen3-vl:30b", 16_384, 512, 60,
        max_concurrency=1, min_interval_seconds=60, request_timeout_seconds=300,
        max_images=4, max_schema_bytes=16_384,
    ),
}

_OPENCLAW_PROFILE_NAMES = (
    "openclaw", "openclaw-gemma4", "openclaw-qwen3-vl", "openclaw-nemotron3",
    "openclaw-unc-rvn-iq2xxs", "openclaw-ministral3", "openclaw-nemotron-nano",
    "openclaw-gpt-oss",
)
OPENCLAW_PROFILES_BY_MODEL = {
    PROFILES[name].model: name for name in _OPENCLAW_PROFILE_NAMES
}
OPENCLAW_PROFILE_NAMES = frozenset(OPENCLAW_PROFILES_BY_MODEL.values())
