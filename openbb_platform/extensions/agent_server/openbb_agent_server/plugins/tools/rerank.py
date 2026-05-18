"""``rerank`` tool source."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, BeforeValidator, Field

from openbb_agent_server.memory.reranker import NvidiaReranker
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource

logger = logging.getLogger("openbb_agent_server.tools.rerank")


def _decode_if_string(value: Any) -> Any:
    """Tolerate models that emit list args as JSON strings."""
    import json

    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


_LooseList = Annotated[list, BeforeValidator(_decode_if_string)]


class _RerankArgs(BaseModel):
    query: str = Field(description="Query to rank candidates against.")
    candidates: _LooseList = Field(
        description=(
            "List of strings to rerank. Each entry is one candidate "
            "passage / search result / document chunk."
        )
    )
    top_k: int = Field(
        default=5,
        ge=1,
        le=200,
        description="How many top results to return.",
    )


class NvidiaRerankToolSource(ToolSource):
    """Bind one NvidiaReranker per agent run."""

    name = "rerank"

    def __init__(
        self,
        *,
        model: str = "nv-rerank-qa-mistral-4b:1",
        api_key: str | None = None,
        base_url: str | None = None,
        truncate: str = "END",
    ) -> None:
        self._model = model
        self._api_key = api_key
        self._base_url = base_url
        self._truncate = truncate

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[BaseTool]:
        api_key = (
            ctx.api_keys.get("NVIDIA_API_KEY") or config.get("api_key") or self._api_key
        )
        reranker = NvidiaReranker(
            model=config.get("model", self._model),
            api_key=api_key,
            base_url=config.get("base_url", self._base_url),
            truncate=config.get("truncate", self._truncate),
        )

        async def rerank(
            query: str,
            candidates: list[str],
            top_k: int = 5,
        ) -> list[dict[str, Any]]:
            if not candidates:
                return []
            tagged = [(str(i), str(c)) for i, c in enumerate(candidates)]
            try:
                ranked = await reranker.rerank(query, tagged, top_k=top_k)
            except Exception as exc:
                logger.warning("rerank tool failed: %s", exc)
                return [
                    {"index": i, "score": 0.0, "text": str(c)}
                    for i, c in enumerate(candidates[:top_k])
                ]
            by_idx = {str(i): c for i, c in enumerate(candidates)}
            return [
                {"index": int(rid), "score": rscore, "text": by_idx.get(rid, "")}
                for rid, rscore in ranked
            ]

        return [
            StructuredTool.from_function(
                coroutine=rerank,
                name="rerank",
                description=(
                    "Re-rank a list of candidate passages by relevance to "
                    "a query, using NVIDIA's cross-encoder reranker. "
                    "Inputs: ``query`` (string), ``candidates`` (list of "
                    "strings), ``top_k`` (int, default 5). Returns a list "
                    "of ``{index, score, text}`` objects sorted by "
                    "descending relevance. Use this AFTER assembling a "
                    "candidate list from web_search / tool output / "
                    "sub-agent results to pick the most relevant before "
                    "reading them in detail."
                ),
                args_schema=_RerankArgs,
            )
        ]
