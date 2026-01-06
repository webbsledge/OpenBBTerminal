"""JSON output adapter."""

import json
from typing import Any

import pandas as pd

from openbb_cli.session import Session

session = Session()


class JsonOutput:
    """JSON output adapter - serializes complete data without truncation."""

    def display(
        self,
        data: Any,
        title: str = "",
        export: bool = False,
        chart: bool = False,
    ) -> None:
        """Display as JSON."""
        if export:
            return

        # Handle chart - just serialize the data, not the chart itself
        if chart:
            session.console.print(
                "[yellow]Chart display not supported in JSON mode. Showing data instead.[/yellow]"
            )

        # Extract raw results and output directly as JSON
        results = None
        
        if hasattr(data, "model_dump"):
            # OBBject - get results from model_dump
            results = data.model_dump().get("results")
        elif isinstance(data, pd.DataFrame):
            # DataFrame - convert to dict
            results = data.to_dict(orient="records")
        elif isinstance(data, pd.Series):
            # Series - convert to dict
            results = data.to_dict()
        else:
            # Everything else - use as-is
            results = data
        
        # Output as compact JSON
        try:
            json_str = json.dumps(results, separators=(",", ":"), default=str)
            print(json_str)
        except Exception as e:
            # Fallback: convert to string
            session.console.print(f"[red]JSON serialization error: {e}[/red]")
            print(str(results))
