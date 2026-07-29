"""Local-first, deterministic core for the Telegram news bot."""

from .config import AppConfig, Capability, ConfigError, load_config, validate_capabilities
from .runtime import FIXTURE_EPOCH, Clock, FixtureClock, Sleeper, SystemClock, SystemSleeper

__all__ = [
    "AppConfig",
    "Capability",
    "Clock",
    "ConfigError",
    "FIXTURE_EPOCH",
    "FixtureClock",
    "Sleeper",
    "SystemClock",
    "SystemSleeper",
    "load_config",
    "validate_capabilities",
]
