from __future__ import annotations

import asyncio
import logging
import random
from pathlib import Path


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def build_logger(name: str = "amazon_rank_crawler") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class DelayController:
    """Central place for human-like waits and backoff jitter."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    async def sleep_random(self, min_seconds: float, max_seconds: float, label: str) -> None:
        seconds = random.uniform(min_seconds, max_seconds)
        self.logger.debug("Delay %s for %.2fs", label, seconds)
        await asyncio.sleep(seconds)

    async def sleep_backoff(self, attempt: int, base: float = 1.2, cap: float = 12.0) -> None:
        seconds = min(cap, base * (2 ** max(attempt - 1, 0)) + random.uniform(0.1, 1.0))
        self.logger.warning("Retry backoff: sleeping %.2fs before next attempt", seconds)
        await asyncio.sleep(seconds)


class ConcurrencyController:
    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    async def run(self, coroutine):
        async with self._semaphore:
            return await coroutine
