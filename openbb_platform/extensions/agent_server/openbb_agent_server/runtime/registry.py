"""Entry-point-based plugin loader."""

from __future__ import annotations

import inspect
import logging
from importlib.metadata import EntryPoint, entry_points
from typing import Any, TypeVar

logger = logging.getLogger("openbb_agent_server.registry")

T = TypeVar("T")


def _eps(group: str) -> dict[str, EntryPoint]:
    return {ep.name: ep for ep in entry_points(group=group)}


def _accepted_kwargs(cls: type) -> set[str] | None:
    """Return the names ``cls.__init__`` accepts, or ``None`` if it takes ``**kwargs``."""
    try:
        sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return None
    accepted: set[str] = set()
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            return None
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            continue
        accepted.add(name)
    return accepted


def load(group: str, name: str, config: dict[str, Any] | None = None) -> Any:
    """Load and instantiate one plugin from ``group`` named ``name``."""
    eps = _eps(group)
    ep = eps.get(name)
    if ep is None:
        raise KeyError(
            f"Plugin {name!r} not found in entry-point group {group!r}; "
            f"available: {sorted(eps)}"
        )
    cls = ep.load()
    logger.debug("loaded plugin %s.%s -> %s", group, name, cls)
    cfg = dict(config or {})
    accepted = _accepted_kwargs(cls)
    if accepted is not None:
        unknown = [k for k in cfg if k not in accepted]
        if unknown:
            logger.warning(
                "plugin %s.%s: dropping unknown config key(s) %r — check "
                "your config layout (likely misplaced under [model.config] "
                "or [profile.config] when it should be at the parent table)",
                group,
                name,
                unknown,
            )
            for k in unknown:
                cfg.pop(k, None)
    return cls(**cfg)


def available(group: str) -> list[str]:
    """List installed plugin names in ``group``."""
    return sorted(_eps(group))
