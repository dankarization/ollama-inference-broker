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


# This is deliberately server-owned. Callers can select a profile, never a model
# or unbounded generation parameters.
PROFILES = {
    "interactive": Profile("interactive", "nemotron3:33b", 16_384, 2_048, 180),
    "cron": Profile("cron", "nemotron3:33b", 8_192, 1_024, 120),
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
    # 64k context and low thinking mode are enforced again by its compatibility
    # contract; callers cannot borrow the Olya lane or widen either limit.
    "syncopia-memory-qwen38": Profile(
        "syncopia-memory-qwen38", "qwen3.8:ad-iq2-xs", 65_536, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=300,
        max_schema_bytes=65_536,
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

# Lower is more important. These classes are policy compiled into the broker,
# never trusted from a caller-provided priority field.
FIXED_SOURCE_PRIORITIES = {
    "interactive": 1,  # OpenClaw interactive/open session
    "cron": 2,         # OpenClaw cron
    "shutterstock-video": 3,
    "olya": 8,
    "olya-vision": 8,
    "olya-decision": 6,
    "syncopia-telegram-memory": 4,
    "shutterstock-canary": 5,
}
MIN_PRIORITY = 1
MAX_PRIORITY = 10
