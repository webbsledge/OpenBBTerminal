"""Groq rate limiter — RPM / RPD / TPM / TPD plus audio-seconds buckets."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
from langchain_core.rate_limiters import BaseRateLimiter


@dataclass(frozen=True)
class GroqLimits:
    """Published per-model quotas. ``None`` means "no published cap"."""

    rpm: int | None = None
    rpd: int | None = None
    tpm: int | None = None
    tpd: int | None = None
    audio_per_hour: int | None = None
    audio_per_day: int | None = None


# Pulled directly from Groq's published rate-limits tables (chat + STT).
GROQ_LIMITS: dict[str, GroqLimits] = {
    # Chat / completion models.
    "llama-3.1-8b-instant": GroqLimits(rpm=30, rpd=14_400, tpm=6_000, tpd=500_000),
    "llama-3.3-70b-versatile": GroqLimits(rpm=30, rpd=1_000, tpm=12_000, tpd=100_000),
    "meta-llama/llama-4-scout-17b-16e-instruct": GroqLimits(
        rpm=30, rpd=1_000, tpm=30_000, tpd=500_000
    ),
    "moonshotai/kimi-k2-instruct": GroqLimits(
        rpm=60, rpd=1_000, tpm=10_000, tpd=300_000
    ),
    "qwen/qwen3-32b": GroqLimits(rpm=60, rpd=1_000, tpm=6_000, tpd=500_000),
    "openai/gpt-oss-120b": GroqLimits(rpm=30, rpd=1_000, tpm=8_000, tpd=200_000),
    "openai/gpt-oss-20b": GroqLimits(rpm=30, rpd=1_000, tpm=8_000, tpd=200_000),
    "groq/compound": GroqLimits(rpm=30, rpd=250, tpm=70_000),
    "groq/compound-mini": GroqLimits(rpm=30, rpd=250, tpm=70_000),
    "allam-2-7b": GroqLimits(rpm=30, rpd=7_000, tpm=6_000, tpd=500_000),
    # Speech-to-text models.
    "whisper-large-v3": GroqLimits(
        rpm=20, rpd=2_000, audio_per_hour=7_200, audio_per_day=28_800
    ),
    "whisper-large-v3-turbo": GroqLimits(
        rpm=20, rpd=2_000, audio_per_hour=7_200, audio_per_day=28_800
    ),
}

# Conservative fallback for unlisted / preview models. Free-tier-shaped.
_DEFAULT_LIMITS = GroqLimits(rpm=30, rpd=1_000, tpm=6_000, tpd=100_000)


@dataclass
class _Bucket:
    """Token-bucket-style counter that refills over a fixed period."""

    capacity: float
    period_seconds: float
    available: float
    last_refill: float

    @classmethod
    def of(cls, capacity: float, period_seconds: float) -> _Bucket:
        return cls(
            capacity=capacity,
            period_seconds=period_seconds,
            available=float(capacity),
            last_refill=time.monotonic(),
        )

    def refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        per_second = self.capacity / self.period_seconds
        self.available = min(self.capacity, self.available + per_second * elapsed)
        self.last_refill = now

    def consume(self, n: float) -> None:
        self.refill()
        self.available -= n

    def time_until_at_least_one(self) -> float:
        self.refill()
        if self.available >= 1:
            return 0.0
        deficit = 1.0 - self.available
        per_second = self.capacity / self.period_seconds
        return deficit / per_second


class GroqRateLimiter(BaseRateLimiter):
    """Multi-dimensional Groq rate limiter."""

    def __init__(
        self,
        *,
        rpm: int,
        rpd: int | None = None,
        tpm: int | None = None,
        tpd: int | None = None,
        audio_per_hour: int | None = None,
        audio_per_day: int | None = None,
        check_every_n_seconds: float = 0.1,
    ) -> None:
        if rpm <= 0:
            raise ValueError("rpm must be > 0")
        self._req_min = _Bucket.of(rpm, 60.0)
        self._req_day = _Bucket.of(rpd, 86_400.0) if rpd else None
        self._tok_min = _Bucket.of(tpm, 60.0) if tpm else None
        self._tok_day = _Bucket.of(tpd, 86_400.0) if tpd else None
        self._audio_hour = (
            _Bucket.of(audio_per_hour, 3_600.0) if audio_per_hour else None
        )
        self._audio_day = _Bucket.of(audio_per_day, 86_400.0) if audio_per_day else None
        self._check_every = max(0.01, check_every_n_seconds)
        self._lock = threading.Lock()
        self._alock = asyncio.Lock()

    @classmethod
    def from_limits(cls, limits: GroqLimits, **overrides: Any) -> GroqRateLimiter:
        kwargs: dict[str, Any] = {
            "rpm": limits.rpm or 30,
            "rpd": limits.rpd,
            "tpm": limits.tpm,
            "tpd": limits.tpd,
            "audio_per_hour": limits.audio_per_hour,
            "audio_per_day": limits.audio_per_day,
        }
        kwargs.update(overrides)
        return cls(**kwargs)

    def acquire(self, *, blocking: bool = True) -> bool:  # noqa: D401
        while True:
            with self._lock:
                wait = self._compute_wait()
                if wait <= 0.0:
                    self._consume_request()
                    return True
            if not blocking:
                return False
            time.sleep(min(wait, self._check_every))

    async def aacquire(self, *, blocking: bool = True) -> bool:  # noqa: D401
        while True:
            async with self._alock:
                with self._lock:
                    wait = self._compute_wait()
                    if wait <= 0.0:
                        self._consume_request()
                        return True
            if not blocking:
                return False
            await asyncio.sleep(min(wait, self._check_every))

    def _compute_wait(self) -> float:
        candidates = [self._req_min.time_until_at_least_one()]
        if self._req_day is not None:
            candidates.append(self._req_day.time_until_at_least_one())
        if self._tok_min is not None:
            candidates.append(self._tok_min.time_until_at_least_one())
        if self._tok_day is not None:
            candidates.append(self._tok_day.time_until_at_least_one())
        if self._audio_hour is not None:
            candidates.append(self._audio_hour.time_until_at_least_one())
        if self._audio_day is not None:
            candidates.append(self._audio_day.time_until_at_least_one())
        return max(candidates)

    def _consume_request(self) -> None:
        self._req_min.consume(1.0)
        if self._req_day is not None:
            self._req_day.consume(1.0)

    def record_tokens(self, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            if self._tok_min is not None:
                self._tok_min.consume(n)
            if self._tok_day is not None:
                self._tok_day.consume(n)

    def record_audio_seconds(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            if self._audio_hour is not None:
                self._audio_hour.consume(seconds)
            if self._audio_day is not None:
                self._audio_day.consume(seconds)

    @property
    def callback_handler(self) -> BaseCallbackHandler:
        return _GroqUsageHandler(self)

    def snapshot(self) -> dict[str, float | None]:
        with self._lock:
            for b in (
                self._req_min,
                self._req_day,
                self._tok_min,
                self._tok_day,
                self._audio_hour,
                self._audio_day,
            ):
                if b is not None:
                    b.refill()
            return {
                "rpm_remaining": self._req_min.available,
                "rpd_remaining": (self._req_day.available if self._req_day else None),
                "tpm_remaining": (self._tok_min.available if self._tok_min else None),
                "tpd_remaining": (self._tok_day.available if self._tok_day else None),
                "audio_seconds_per_hour_remaining": (
                    self._audio_hour.available if self._audio_hour else None
                ),
                "audio_seconds_per_day_remaining": (
                    self._audio_day.available if self._audio_day else None
                ),
            }


class _GroqUsageHandler(BaseCallbackHandler):
    """Reads ``response.usage_metadata`` and feeds it into a limiter."""

    raise_error = False

    def __init__(self, limiter: GroqRateLimiter) -> None:
        self._limiter = limiter

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        usage = (response.llm_output or {}).get("token_usage") or {}
        total = int(usage.get("total_tokens") or 0)
        if total <= 0:
            for gen_list in response.generations:
                for gen in gen_list:
                    msg = getattr(gen, "message", None)
                    meta = getattr(msg, "usage_metadata", None)
                    if isinstance(meta, dict):
                        total += int(meta.get("total_tokens") or 0)
        if total > 0:
            self._limiter.record_tokens(total)


_LIMITER_CACHE: dict[tuple[str, str], GroqRateLimiter] = {}
_CACHE_LOCK = threading.Lock()


def get_limiter(*, api_key: str, model_name: str) -> GroqRateLimiter:
    """Return the process-shared limiter for ``(api_key, model_name)``."""
    cache_key = (api_key, model_name)
    with _CACHE_LOCK:
        existing = _LIMITER_CACHE.get(cache_key)
        if existing is not None:
            return existing
        limits = GROQ_LIMITS.get(model_name) or _DEFAULT_LIMITS
        limiter = GroqRateLimiter.from_limits(limits)
        _LIMITER_CACHE[cache_key] = limiter
        return limiter


def reset_cache() -> None:
    """Test hook: drop every cached limiter so the next ``get_limiter`` rebuilds."""
    with _CACHE_LOCK:
        _LIMITER_CACHE.clear()
