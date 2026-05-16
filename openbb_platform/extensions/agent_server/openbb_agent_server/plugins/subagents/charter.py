"""``charter`` subagent — produces chart artifacts (Plotly JSON specs)."""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a charting subagent. Given a dataset reference (a widget id, a
``read_file`` path, or inline JSON the user pasted), produce a single
chart artifact via the ``custom`` stream channel with shape:

  {"type": "artifact", "artifact_type": "chart", "title": "...",
   "data": {"plotly": <plotly figure JSON>}}

Pick chart type from the data shape (line for time series, bar for
categorical comparisons, scatter for two numeric axes). Keep titles
short. Never inline the full data array into the chat reply — emit it
only as part of the artifact.
"""


class CharterSubAgent:
    name = "charter"
    description = (
        "Use when the user asks for a chart / visualisation / plot of data "
        "they've referenced (a widget, an uploaded spreadsheet, or a tool result)."
    )
    system_prompt = SYSTEM_PROMPT
    tools: tuple[str, ...] = ()
    model: str | None = None
