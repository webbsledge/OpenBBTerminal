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
                print("No results")
                return
            
            # Convert results to DataFrame
            if isinstance(results, pd.DataFrame):
                df = results
            elif isinstance(results, list):
                df = pd.DataFrame(results)
            elif isinstance(results, dict):
                df = pd.DataFrame([results])
            else:
                # Scalar - output as single value
                print(str(results))
                return
        elif isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pd.Series):
            df = data.to_frame()
        elif isinstance(data, dict):
            df = pd.DataFrame.from_dict(data, orient="columns")
        elif isinstance(data, (list, tuple)):
            df = pd.DataFrame(data)
        else:
            # Scalar - output as single value
            print(str(data))
            return

        # Output complete TSV without truncation
        # Using to_csv with tab separator for pipe-friendly output
        tsv_str = df.to_csv(sep="\t", index=False)
        print(tsv_str, end="")
