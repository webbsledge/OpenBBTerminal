"""Test bootstrap for ``openbb-platform-api``.

Pre-imports every launcher / openbb-core submodule that the test
suite patches via ``mock.patch("dotted.path.attr", ...)``.

``mock.patch`` resolves dotted paths by walking attributes on the
parent package (``getattr(parent, "submodule")``). On Python 3.10,
``from X.Y import Z`` inside a package ``__init__`` doesn't always
bind ``Y`` as an attribute on ``X`` — see CPython issue #40500. The
import succeeds (``sys.modules["X.Y"]`` is populated) but
``getattr(X, "Y")`` raises ``AttributeError``, which in turn breaks
``mock.patch``'s resolver. Python 3.11+ fixed this.

Two-step fix per submodule:

1. ``import X.Y`` — populates ``sys.modules`` and (usually) binds
   the attribute on the parent.
2. Belt-and-suspenders: explicitly assign
   ``parent.submodule = sys.modules["parent.submodule"]``. On 3.11+
   this is a no-op (the attribute is already there); on 3.10 it
   patches over the missing binding so ``mock.patch`` can find it.
"""

import sys

# openbb-core service singletons — touched by patches in test_app.py
# and by the launcher's bootstrap / app.app code paths under test.
# openbb-core auth + provider plumbing — referenced by
# ``import_app``'s exception-handler wiring.
import openbb_core
import openbb_core.api
import openbb_core.api.exception_handlers  # noqa: F401
import openbb_core.app
import openbb_core.app.service
import openbb_core.app.service.system_service  # noqa: F401
import openbb_core.app.service.user_service  # noqa: F401

# Launcher submodules — args / bootstrap / config / spec / middleware
# all get patched directly via their dotted module paths in the test
# suite. NOTE: we deliberately do NOT import
# ``openbb_platform_api.app.app`` here — that module's body calls
# ``parse_args()`` and (when ``--app`` isn't supplied) imports
# ``openbb_core.api.rest_api``, both of which have heavy side
# effects that the tests want to control on a per-test basis via
# ``importlib.import_module`` reloads.
import openbb_platform_api
import openbb_platform_api.app
import openbb_platform_api.app.args  # noqa: F401
import openbb_platform_api.app.bootstrap  # noqa: F401
import openbb_platform_api.app.config  # noqa: F401
import openbb_platform_api.app.middleware  # noqa: F401
import openbb_platform_api.app.spec  # noqa: F401
import openbb_platform_api.service
import openbb_platform_api.service.agents_service  # noqa: F401
import openbb_platform_api.service.apps_service  # noqa: F401
import openbb_platform_api.service.widgets_service  # noqa: F401


def _force_bind(parent_dotted: str, child: str) -> None:
    """Assign ``parent.<child> = sys.modules['parent.child']``.

    Workaround for CPython issue #40500 (py3.10 only): even after
    ``import parent.child`` runs, ``getattr(parent, "child")`` can
    still raise ``AttributeError`` when the import was triggered
    from inside the parent's ``__init__``. Direct assignment
    establishes the binding so ``mock.patch``'s resolver can find
    the submodule on the parent. No-op on py3.11+ where the
    binding is already set.
    """
    parent = sys.modules[parent_dotted]
    full = f"{parent_dotted}.{child}"
    setattr(parent, child, sys.modules[full])


for _parent, _children in (
    (
        "openbb_core.app.service",
        ("system_service", "user_service"),
    ),
    ("openbb_core.api", ("exception_handlers",)),
    (
        "openbb_platform_api.app",
        ("args", "bootstrap", "config", "middleware", "spec"),
    ),
    (
        "openbb_platform_api.service",
        ("agents_service", "apps_service", "widgets_service"),
    ),
):
    for _child in _children:
        _force_bind(_parent, _child)
