"""Range and shape validation helpers shared by model providers."""

from __future__ import annotations


def check_range(name: str, value: float | None, lo: float, hi: float) -> None:
    if value is None:
        return
    if not (lo <= value <= hi):
        raise ValueError(f"{name} must be between {lo} and {hi} (got {value})")


def check_min(name: str, value: int | None, lo: int) -> None:
    if value is None:
        return
    if value < lo:
        raise ValueError(f"{name} must be >= {lo} (got {value})")
