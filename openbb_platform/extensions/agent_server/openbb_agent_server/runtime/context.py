"""Per-request ``RunContext`` and the contextvar that propagates it."""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from openbb_agent_server.runtime.principal import UserPrincipal


class WidgetRef(BaseModel):
    """One Workspace-supplied widget the user has selected as context."""

    model_config = ConfigDict(extra="allow")

    uuid: str
    widget_id: str = ""
    origin: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    data: Any = None  # Filled in only after a ``get_widget_data`` round-trip.


class FileRef(BaseModel):
    """One uploaded file (PDF / image / spreadsheet / raw)."""

    model_config = ConfigDict(extra="allow")

    name: str
    mime: str | None = None
    data_base64: str | None = None
    url: str | None = None


class RunContext(BaseModel):
    """Bundles identity + request payload for one ``/v1/query`` exchange."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    principal: UserPrincipal
    trace_id: str
    run_id: str
    conversation_id: str
    agent_name: str = "default"
    timezone: str | None = None
    widgets: tuple[WidgetRef, ...] = ()
    uploaded_files: tuple[FileRef, ...] = ()
    api_keys: dict[str, str] = Field(default_factory=dict)
    api_urls: dict[str, str] = Field(default_factory=dict)
    tools: tuple[dict[str, Any], ...] = ()
    workspace_options: frozenset[str] = frozenset()

    def has_workspace_option(self, slug: str) -> bool:
        """Return True iff the user has enabled the named custom feature."""
        return slug in self.workspace_options


_current: contextvars.ContextVar[RunContext | None] = contextvars.ContextVar(
    "openbb_agent_server.run_context",
    default=None,
)

_runtime_state: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "openbb_agent_server.runtime_state",
    default=None,
)


def current() -> RunContext:
    """Return the current ``RunContext``."""
    ctx = _current.get()
    if ctx is None:
        raise LookupError("No RunContext bound on this task")
    return ctx


def runtime_state() -> dict[str, Any]:
    """Return the per-run mutable scratch dict, scoped to this run."""
    state = _runtime_state.get()
    if state is None:
        raise LookupError("No runtime state bound on this task")
    return state


@contextmanager
def bind(ctx: RunContext) -> Iterator[RunContext]:
    """Bind ``ctx`` + a fresh runtime-state dict for the ``with`` block."""
    state: dict[str, Any] = {}
    ctx_token = _current.set(ctx)
    state_token = _runtime_state.set(state)
    try:
        yield ctx
    finally:
        from openbb_agent_server.runtime.jobs import cleanup_state

        cleanup_state(state)
        _runtime_state.reset(state_token)
        _current.reset(ctx_token)
