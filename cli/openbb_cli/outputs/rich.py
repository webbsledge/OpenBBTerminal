"""Rich table output adapter."""

from typing import Any

import pandas as pd

from openbb_cli.controllers.utils import print_rich_table
from openbb_cli.session import Session

session = Session()


class RichTableOutput:
    """Rich table output adapter with truncation for terminal display."""

    def display(
        self,
        data: Any,
        title: str = "",
        export: bool = False,
        chart: bool = False,
    ) -> None:
        """Display using rich table with row/column limits."""
        if export:
            return

        # Handle OBBject instances - extract results manually
        if hasattr(data, "model_dump"):
            # Handle chart display if requested
            if chart:
                try:
                    data.show()
                    return
                except Exception as e:
                    session.console.print(f"[yellow]Chart not available: {e}[/yellow]")
                    # Fall through to show table instead

            # Check if we should use interactive window for OBBjects
            if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                try:
                    data.charting.table()
                    return
                except Exception as e:
                    session.console.print(
                        f"[yellow]Interactive table not available: {e}[/yellow]"
                    )
                    # Fall through to rich table

            # Extract results from OBBject
            results = data.model_dump().get("results")
            
            if results is None:
                session.console.print("[yellow]No results to display[/yellow]")
                return
            
            # Try to convert to DataFrame for display
            try:
                if isinstance(results, pd.DataFrame):
                    df = results
                elif isinstance(results, list):
                    if not results:
                        session.console.print("[yellow]Empty results[/yellow]")
                        return
                    # Try DataFrame conversion
                    df = pd.DataFrame(results)
                elif isinstance(results, dict):
                    df = pd.DataFrame([results])
                else:
                    # Non-tabular data - use PyWry if available, else print
                    if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                        try:
                            html_content = f"<pre>{results}</pre>"
                            session.backend.send_html(html_content)
                            return
                        except Exception:
                            pass
                    session.console.print(results)
                    return
            except Exception as e:
                # DataFrame conversion failed - use PyWry if available, else show as plain text
                if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                    try:
                        html_content = f"<pre>{results}</pre>"
                        session.backend.send_html(html_content)
                        return
                    except Exception:
                        pass
                session.console.print(f"[yellow]Cannot display as table: {e}[/yellow]")
                session.console.print(results)
                return
                
        elif isinstance(data, pd.DataFrame):
            df = data
            # Check if we should use interactive window for plain DataFrames
            if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                try:
                    # Wrap DataFrame in OBBject for charting
                    from openbb import obb
                    temp_obbject = obb.OBBject(results=df)
                    temp_obbject.charting.table()
                    return
                except Exception:
                    # Fall through to rich table if PyWry fails
                    pass
            if df.empty:
                session.console.print("[yellow]Empty DataFrame[/yellow]")
                return
        elif isinstance(data, pd.Series):
            df = data.to_frame()
        elif isinstance(data, dict):
            try:
                df = pd.DataFrame.from_dict(data, orient="columns")
            except Exception:
                # Can't convert to DataFrame - use PyWry if available, else show as text
                if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                    try:
                        import json
                        html_content = f"<pre>{json.dumps(data, indent=2, default=str)}</pre>"
                        session.backend.send_html(html_content)
                        return
                    except Exception:
                        pass
                session.console.print(data)
                return
        elif isinstance(data, (list, tuple)):
            if not data:
                session.console.print("[yellow]Empty data[/yellow]")
                return
            try:
                df = pd.DataFrame(data)
            except Exception:
                # Can't convert to DataFrame - use PyWry if available, else show as text
                if session.settings.USE_INTERACTIVE_DF and session.backend is not None:
                    try:
                        import json
                        html_content = f"<pre>{json.dumps(data, indent=2, default=str)}</pre>"
                        session.backend.send_html(html_content)
                        return
                    except Exception:
                        pass
                session.console.print(data)
                return
        else:
            # Scalar or other type - just print it
            session.console.print(data)
            return

        if isinstance(df.columns, pd.RangeIndex):
            df.columns = [str(i) for i in df.columns]

        print_rich_table(df=df, show_index=True, title=title, export=export)
