"""NVIDIA NIM reranker adapter."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Sequence
from typing import Any

logger = logging.getLogger("openbb_agent_server.memory.reranker")


class NvidiaReranker:
    """Cross-encoder reranker backed by an NVIDIA NIM model."""

    def __init__(
        self,
        *,
        model: str = "nv-rerank-qa-mistral-4b:1",
        api_key: str | None = None,
        base_url: str | None = None,
        truncate: str = "END",
        top_n: int | None = None,
    ) -> None:
        self._model = model
        self._api_key = api_key or os.environ.get("NVIDIA_API_KEY")
        self._base_url = base_url
        self._truncate = truncate
        self._top_n = top_n
        self._client: Any | None = None

    def _build_client(self) -> Any:
        try:
            from langchain_nvidia_ai_endpoints import NVIDIARerank
        except ImportError as exc:  # pragma: no cover — install hint
            raise RuntimeError(
                "NvidiaReranker requires langchain-nvidia-ai-endpoints. "
                "Install the agent_server with the [nvidia] extra."
            ) from exc

        if not self._api_key:
            raise RuntimeError(
                "NvidiaReranker: NVIDIA_API_KEY is not set. Provide it "
                "via the environment, user_settings.json, or the "
                "constructor."
            )

        kwargs: dict[str, Any] = {
            "model": self._model,
            "api_key": self._api_key,
            "truncate": self._truncate,
        }
        if self._base_url:
            kwargs["base_url"] = self._base_url
        if self._top_n is not None:
            kwargs["top_n"] = int(self._top_n)
        return NVIDIARerank(**kwargs)

    async def rerank(
        self,
        query: str,
        candidates: Sequence[tuple[str, str]],
        *,
        top_k: int | None = None,
    ) -> list[tuple[str, float]]:
        """Re-rank ``candidates`` against ``query``."""
        if not candidates:
            return []
        if not query or not query.strip():
            return [(cid, 0.0) for cid, _ in candidates[: top_k or len(candidates)]]

        if self._client is None:
            self._client = self._build_client()

        from langchain_core.documents import Document

        docs = [
            Document(page_content=text, metadata={"_rerank_id": cid})
            for cid, text in candidates
        ]

        acompress = getattr(self._client, "acompress_documents", None)
        if acompress is not None:
            ranked = await acompress(documents=docs, query=query)
        else:
            ranked = await asyncio.to_thread(
                self._client.compress_documents,
                documents=docs,
                query=query,
            )

        out: list[tuple[str, float]] = []
        for d in ranked:
            md = getattr(d, "metadata", {}) or {}
            cid = md.get("_rerank_id")
            if cid is None:
                continue
            score = md.get("relevance_score")
            if score is None:
                score = md.get("score", 0.0)
            try:
                out.append((str(cid), float(score)))
            except (TypeError, ValueError):
                out.append((str(cid), 0.0))

        if top_k is not None:
            out = out[: int(top_k)]
        return out
