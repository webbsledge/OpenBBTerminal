"""Plugin ABCs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

from fastapi import Request

from openbb_agent_server.runtime.principal import UserPrincipal

if TYPE_CHECKING:  # pragma: no cover — types-only
    from openbb_agent_server.runtime.context import RunContext


class AuthBackend(ABC):
    """Resolve a request's credentials into a ``UserPrincipal``."""

    name: str

    @abstractmethod
    async def authenticate(self, request: Request) -> UserPrincipal:
        """Resolve the principal or raise ``HTTPException``."""


class ModelProvider(ABC):
    """Build a LangChain chat model for the current run."""

    name: str

    @abstractmethod
    def build(self, ctx: RunContext, config: dict[str, Any]) -> Any:
        """Return a ``BaseChatModel``-compatible instance."""


class ToolSource(ABC):
    """Yield the agent's LangChain tools for one run."""

    name: str

    @abstractmethod
    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[Any]:
        """Return a list of LangChain ``BaseTool`` instances."""


class SubAgentSpec(Protocol):
    """One sub-agent declaration as deepagents wants it."""

    name: str
    description: str
    system_prompt: str
    tools: tuple[str, ...]
    model: str | None


class Middleware(ABC):
    """A deepagents middleware factory."""

    name: str

    @abstractmethod
    def build(self, ctx: RunContext, config: dict[str, Any]) -> Any:
        """Return a deepagents middleware instance."""


class CheckpointerProvider(ABC):
    """Build (and lifecycle-manage) the LangGraph checkpointer."""

    name: str

    @abstractmethod
    async def open(self, settings: Any) -> Any:
        """Open and ``setup()`` the saver. Returns the live instance."""

    @abstractmethod
    async def close(self, saver: Any) -> None:
        """Tear down any owned resources (connections, files)."""
