"""``python_module`` tool source."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Sequence
from typing import Any

from langchain_core.tools import BaseTool

from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource


def _resolve(spec: str) -> Any:
    if ":" not in spec:
        raise ValueError(
            f"python_module tool spec must be 'pkg.mod:attribute', got {spec!r}"
        )
    module, attr = spec.split(":", 1)
    return getattr(importlib.import_module(module), attr)


def _flatten(value: Any) -> list[BaseTool]:
    import contextlib

    if callable(value) and not isinstance(value, BaseTool):
        with contextlib.suppress(TypeError):
            value = value()
    if isinstance(value, BaseTool):
        return [value]
    if isinstance(value, (list, tuple)):
        out: list[BaseTool] = []
        for item in value:
            out.extend(_flatten(item))
        return out
    raise TypeError(
        f"python_module spec resolved to unsupported type {type(value).__name__}"
    )


class PythonModuleToolSource(ToolSource):
    """Discover LangChain tools from dotted-path locations."""

    name = "python_module"

    def __init__(self, *, modules: Sequence[str] | None = None) -> None:
        self._specs = tuple(modules or ())

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[BaseTool]:
        specs: Iterable[str] = config.get("modules", self._specs)
        out: list[BaseTool] = []
        for spec in specs:
            out.extend(_flatten(_resolve(spec)))
        return out
