"""Test bootstrap for ``openbb-mcp-server``.

Pre-imports openbb-core submodules that the test suite patches via
dotted ``mock.patch`` paths AND explicitly binds them as attributes on
their parent namespace packages. ``openbb_core.api`` and
``openbb_core.app.service`` are namespace packages on Python 3.10
(no ``__init__.py``); ``__import__("openbb_core.api.app_loader")``
populates ``sys.modules`` but doesn't always set ``app_loader`` as
an attribute on ``openbb_core.api``, breaking ``mock.patch`` resolution.
The explicit ``setattr`` after import locks the binding.
"""

import sys

import openbb_core.api
import openbb_core.api.app_loader  # noqa: F401
import openbb_core.api.exception_handlers  # noqa: F401
import openbb_core.app.model
import openbb_core.app.model.credentials  # noqa: F401
import openbb_core.app.service
import openbb_core.app.service.system_service  # noqa: F401
import openbb_core.app.service.user_service  # noqa: F401

for _parent, _children in (
    ("openbb_core.api", ("app_loader", "exception_handlers")),
    ("openbb_core.app.model", ("credentials",)),
    ("openbb_core.app.service", ("system_service", "user_service")),
):
    _pkg = sys.modules[_parent]
    for _child in _children:
        setattr(_pkg, _child, sys.modules[f"{_parent}.{_child}"])
