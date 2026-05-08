"""Pydantic models for the OpenBB Platform API extension.

Re-exports the public surface so callers can write
``from openbb_platform_api.models import OmniWidgetInput`` rather than
having to know the exact module each model lives in.
"""

# Relative imports for reliable submodule attribute binding on
# Python 3.10 — see CPython issue #40500.
from .query import OmniWidgetInput
from .response import MetricResponseModel, OmniWidgetResponseModel, PdfResponseModel

__all__ = [
    "MetricResponseModel",
    "OmniWidgetInput",
    "OmniWidgetResponseModel",
    "PdfResponseModel",
]
