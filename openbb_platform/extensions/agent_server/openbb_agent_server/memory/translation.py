"""NVIDIA NIM translation adapter."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger("openbb_agent_server.memory.translation")


class NvidiaTranslator:
    r"""Async translation client backed by an NVIDIA-hosted instruct model.

    The Riva translate model takes a chat completion of the form::

        system: You are a precise translation engine. ...
        user:   <source-lang>→<target-lang>:\n<text>

    and returns the translated string in the assistant turn. The exact
    prompt template can shift between model versions; the system
    instruction here was tuned for ``riva-translate-4b-instruct-v1_1``
    and is intentionally conservative (no commentary, preserve
    formatting / code fences / markdown).
    """

    def __init__(
        self,
        *,
        model: str = "nvidia/riva-translate-4b-instruct-v1_1",
        api_key: str | None = None,
        base_url: str | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = 2048,
    ) -> None:
        self._model = model
        self._api_key = api_key or os.environ.get("NVIDIA_API_KEY")
        self._base_url = base_url
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._client: Any | None = None

    def _build_client(self) -> Any:
        try:
            from langchain_nvidia_ai_endpoints import ChatNVIDIA
        except ImportError as exc:  # pragma: no cover — install hint
            raise RuntimeError(
                "NvidiaTranslator requires langchain-nvidia-ai-endpoints. "
                "Install the agent_server with the [nvidia] extra."
            ) from exc

        if not self._api_key:
            raise RuntimeError(
                "NvidiaTranslator: NVIDIA_API_KEY is not set. Provide it "
                "via the environment, user_settings.json, or the "
                "constructor."
            )

        kwargs: dict[str, Any] = {
            "model": self._model,
            "api_key": self._api_key,
            "temperature": self._temperature,
        }
        if self._base_url:
            kwargs["base_url"] = self._base_url
        if self._max_tokens is not None:
            kwargs["max_tokens"] = self._max_tokens
        return ChatNVIDIA(**kwargs)

    @staticmethod
    def _build_messages(
        text: str,
        source_language: str,
        target_language: str,
    ) -> list[Any]:
        from langchain_core.messages import HumanMessage, SystemMessage

        system = (
            "You are a precise translation engine. Translate the user's "
            "text into the target language. Output ONLY the translation, "
            "with no preface, commentary, or quoted source. Preserve "
            "Markdown, code fences, bullet structure, numbers, and "
            "proper nouns exactly. If the text is already in the target "
            "language, return it unchanged."
        )
        src = source_language.strip() or "auto-detect"
        tgt = target_language.strip() or "English"
        user = f"Translate from {src} to {tgt}:\n\n{text}"
        return [SystemMessage(content=system), HumanMessage(content=user)]

    async def translate(
        self,
        text: str,
        *,
        source_language: str = "auto",
        target_language: str = "English",
    ) -> str:
        """Return ``text`` translated into ``target_language``."""
        if not text or not text.strip():
            return ""
        if self._client is None:
            self._client = self._build_client()
        messages = self._build_messages(text, source_language, target_language)

        # ChatNVIDIA exposes ``ainvoke`` (async) and ``invoke`` (sync).
        # Prefer async; fall back to thread for older builds.
        ainvoke = getattr(self._client, "ainvoke", None)
        if ainvoke is not None:
            response = await ainvoke(messages)
        else:
            response = await asyncio.to_thread(self._client.invoke, messages)

        content = getattr(response, "content", None)
        if isinstance(content, list):
            # Block-list shape (Anthropic-style). Pull text blocks only.
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(str(block.get("text", "")))
                elif isinstance(block, str):
                    parts.append(block)
            return "".join(parts).strip()
        return (str(content) if content is not None else "").strip()
