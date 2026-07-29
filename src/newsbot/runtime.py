"""Injected time and sleep primitives for deterministic local execution."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Protocol

FIXTURE_EPOCH = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


class Clock(Protocol):
    def now(self) -> datetime:
        """Return the current UTC-aware time."""


class Sleeper(Protocol):
    async def sleep(self, delay: float) -> None:
        """Suspend for a non-negative number of seconds."""


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class SystemSleeper:
    async def sleep(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("delay must be non-negative")
        await asyncio.sleep(delay)


class FixtureClock:
    """A manually advanced UTC clock for fixtures and replayable workflows."""

    def __init__(self, initial: datetime = FIXTURE_EPOCH) -> None:
        self._current = _require_utc(initial)

    def now(self) -> datetime:
        return self._current

    def advance(self, amount: timedelta | float | int) -> datetime:
        delta = amount if isinstance(amount, timedelta) else timedelta(seconds=amount)
        if delta.total_seconds() < 0:
            raise ValueError("fixture time cannot move backwards")
        self._current += delta
        return self._current


class FixtureSleeper:
    """A no-wall-clock sleeper that advances the injected fixture clock."""

    def __init__(self, clock: FixtureClock) -> None:
        self._clock = clock

    async def sleep(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("delay must be non-negative")
        self._clock.advance(delay)
