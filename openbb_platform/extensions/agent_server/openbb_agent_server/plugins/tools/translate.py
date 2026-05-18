"""``translate`` tool source."""

from __future__ import annotations

import logging
from typing import Any

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field

from openbb_agent_server.memory.translation import NvidiaTranslator
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource

logger = logging.getLogger("openbb_agent_server.tools.translate")


class _TranslateArgs(BaseModel):
    text: str = Field(description="Text to translate.")
    source_language: str = Field(
        default="auto",
        description=(
            "Source language name (English label, e.g. 'French', "
            "'Mandarin', 'Spanish'). 'auto' lets the model detect."
        ),
    )
    target_language: str = Field(
        default="English",
        description="Target language name (English label).",
    )


class NvidiaTranslateToolSource(ToolSource):
    """Bind one NvidiaTranslator per agent run."""

    name = "translate"

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
        self._api_key = api_key
        self._base_url = base_url
        self._temperature = temperature
        self._max_tokens = max_tokens

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[BaseTool]:
        api_key = (
            ctx.api_keys.get("NVIDIA_API_KEY") or config.get("api_key") or self._api_key
        )
        translator = NvidiaTranslator(
            model=config.get("model", self._model),
            api_key=api_key,
            base_url=config.get("base_url", self._base_url),
            temperature=float(config.get("temperature", self._temperature)),
            max_tokens=config.get("max_tokens", self._max_tokens),
        )

        async def translate(
            text: str,
            source_language: str = "auto",
            target_language: str = "English",
        ) -> str:
            try:
                return await translator.translate(
                    text,
                    source_language=source_language,
                    target_language=target_language,
                )
            except Exception as exc:
                logger.warning("translate tool failed: %s", exc)
                return f"translation failed: {exc}"

        return [
            StructuredTool.from_function(
                coroutine=translate,
                name="translate",
                description=(
                    "Translate a piece of text from one language to another "
                    "using NVIDIA's Riva translate model. Inputs: ``text`` "
                    "(string to translate), ``source_language`` (default "
                    "'auto'), ``target_language`` (default 'English'). "
                    "Markdown, code fences, and numbers are preserved. "
                    "Returns the translated string; on failure, returns a "
                    "short error message starting with 'translation failed:'."
                ),
                args_schema=_TranslateArgs,
            )
        ]
