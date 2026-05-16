"""OpenBB Platform Agent Server — pluggable, multi-tenant agent backend."""

from __future__ import annotations

import warnings

__version__ = "0.1.0"

# ``langgraph.checkpoint.serde.jsonplus`` instantiates a module-level
# ``Reviver()`` without passing ``allowed_objects``; langchain_core 1.3.3
# fires a pending-deprecation warning at that import. We don't own the
# call site and the future default (``'core'``) is what langgraph already
# relies on, so the migration is a no-op for us. langchain_core's own
# ``surface_langchain_deprecation_warnings`` prepends a ``default`` filter
# for its warning subclasses, so we have to import it first and then
# install ours to land at the front of the filter list.
from langchain_core._api.deprecation import (  # noqa: E402
    LangChainPendingDeprecationWarning,
)

warnings.filterwarnings(
    "ignore",
    message=r"The default value of `allowed_objects` will change.*",
    category=LangChainPendingDeprecationWarning,
)
