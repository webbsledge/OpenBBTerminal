"""Utility helpers for ``openbb-mcp``.

Submodules bound explicitly so ``mock.patch`` finds them as
attributes on this package (CPython 3.10 namespace-package quirk).
"""

from openbb_mcp_server.utils import app_import, fastapi

__all__ = [
    "app_import",
    "fastapi",
]
