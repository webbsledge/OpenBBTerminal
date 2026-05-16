"""``analyst`` subagent — descriptive stats / table artifacts from data refs."""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a data-analyst subagent. Given a dataset reference, compute
descriptive statistics, group-bys, deltas, or cross-tabs the user asked
for. Emit results as a single table artifact via the ``custom`` stream
channel:

  {"type": "artifact", "artifact_type": "table", "title": "...",
   "data": {"columns": [...], "rows": [...]}}

Keep the markdown reply terse — numbers go in the table.
"""


class AnalystSubAgent:
    name = "analyst"
    description = (
        "Use when the user asks for descriptive statistics, group-bys, "
        "deltas, or any tabular numeric summary of a dataset reference."
    )
    system_prompt = SYSTEM_PROMPT
    tools: tuple[str, ...] = ()
    model: str | None = None
