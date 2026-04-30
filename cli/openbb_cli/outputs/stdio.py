"""STDIO output adapter."""

from typing import Any

import pandas as pd

from openbb_cli.session import Session

session = Session()


class StdioOutput:
    """STDIO output adapter - outputs complete TSV for pipe-friendly parsing."""

    def display(
        self,
        data: Any,
        title: str = "",
        export: bool = False,
        chart: bool = False,
    ) -> None:
        """Display as TSV (tab-separated values)."""
        if export:
            return

        # Handle chart - just output the data
        if chart:
            session.console.print(
                "[yellow]Chart display not supported in STDIO mode. Showing data instead.[/yellow]"
            )

        # Handle OBBject - extract results manually
        if hasattr(data, "model_dump"):
            results = data.model_dump().get("results")
            if results is None:
                return

            # Convert results to DataFrame
            if isinstance(results, pd.DataFrame):
                pass
            elif isinstance(results, list):
                pd.DataFrame(results)
            elif isinstance(results, dict):
                pd.DataFrame([results])
            else:
                # Scalar - output as single value
                return
        elif isinstance(data, pd.DataFrame):
            pass
        elif isinstance(data, pd.Series):
            data.to_frame()
        elif isinstance(data, dict):
            pd.DataFrame.from_dict(data, orient="columns")
        elif isinstance(data, (list, tuple)):
            pd.DataFrame(data)
        else:
            # Scalar - output as single value
            return

        # Output complete TSV without truncation
        # Using to_csv with tab separator for pipe-friendly output
