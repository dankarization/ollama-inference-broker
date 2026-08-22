from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    model: str
    max_context: int
    max_output: int
    keep_alive_seconds: int


# This is deliberately server-owned. Callers can select a profile, never a model
# or unbounded generation parameters.
PROFILES = {
    "interactive": Profile("interactive", "nemotron3:33b", 16_384, 2_048, 180),
    "cron": Profile("cron", "nemotron3:33b", 8_192, 1_024, 120),
    "shutterstock": Profile("shutterstock", "qwen3-vl:32b", 16_384, 1_024, 120),
    # `olya` is intentionally only a source/profile key. Do not infer an
    # integration from this name; its model can be changed in broker config.
    "olya": Profile("olya", "nemotron3:33b", 8_192, 1_024, 120),
    "batch-video": Profile("batch-video", "nemotron3:33b", 8_192, 512, 120),
}

# Lower is more important. These classes are policy compiled into the broker,
# never trusted from a caller-provided priority field.
FIXED_SOURCE_PRIORITIES = {
    "interactive": 1,  # OpenClaw interactive/open session
    "cron": 2,         # OpenClaw cron
    "shutterstock": 5,
    "olya": 8,
}
MIN_PRIORITY = 1
MAX_PRIORITY = 10
