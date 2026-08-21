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
    "photo": Profile("photo", "qwen3-vl:32b", 16_384, 1_024, 120),
    "cron": Profile("cron", "nemotron3:33b", 8_192, 1_024, 120),
    "batch-video": Profile("batch-video", "nemotron3:33b", 8_192, 512, 120),
}

PRIORITY = {"interactive": 0, "photo": 1, "cron": 2, "batch-video": 3}
