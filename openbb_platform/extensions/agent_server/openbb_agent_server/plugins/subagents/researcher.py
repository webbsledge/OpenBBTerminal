"""``researcher`` subagent — web/news/filings retrieval + citations."""

from __future__ import annotations

from typing import Any

SYSTEM_PROMPT = """\
You are a research subagent. Use the available tools to retrieve
authoritative sources for the user's question (filings, primary
documents, news), summarise crisply, and ALWAYS attach citations
(``source_url`` or named ``source``) to every factual claim.

If a tool fails or returns nothing, say so explicitly — do not invent.
"""


def factory(**_config: Any) -> dict[str, Any]:
    return {
        "name": "researcher",
        "description": (
            "Use when the user asks for facts, sources, filings, news, or "
            "any answer that requires up-to-date retrieval and citation."
        ),
        "system_prompt": SYSTEM_PROMPT,
    }


class ResearcherSubAgent:
    """Researcher subagent declaration. Returned shape is a deepagents ``SubAgent``."""

    name = "researcher"
    description = (
        "Use when the user asks for facts, sources, filings, news, or any "
        "answer that requires up-to-date retrieval and citation."
    )
    system_prompt = SYSTEM_PROMPT
    tools: tuple[str, ...] = ()
    model: str | None = None
