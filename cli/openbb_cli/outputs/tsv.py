"""TSV output adapter."""

from typing import Any

import pandas as pd


class TsvOutput:
    """TSV output adapter - outputs complete DataFrame for pipe-friendly parsing."""

    def display(
        self,
        data: Any,
        title: str = "",
        export: bool = False,
        chart: bool = False,
    ) -> None:
        """Display DataFrame using to_string()."""
        if export:
            return

        # Handle chart - print the chart object directly
        if chart:
            if hasattr(data, "chart") and data.chart is not None:
                print(data.chart)
                return

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

        # Output complete DataFrame using to_string()
        print(df.dropna(how="all", axis=1).to_string())
