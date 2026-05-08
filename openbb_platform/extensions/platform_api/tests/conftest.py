"""Test bootstrap for ``openbb-platform-api``.

Forces eager submodule imports that the launcher source code lazily
triggers from inside function bodies (``args.parse_args`` imports
``bootstrap`` and ``config`` lazily; ``app.app`` imports
``rest_api`` only when no ``--app`` is supplied; openbb-core's
service singletons are constructed on demand). On Python 3.11+ that
laziness is fine because ``mock.patch._importer`` falls back to
``__import__(submodule_path)`` and the bind-as-attribute side effect
takes hold consistently.

Python 3.10 is stricter — there are documented edge cases where the
fallback ``__import__`` doesn't rebind the attribute on the parent
module (the import succeeds but ``getattr(parent, "submodule")``
still raises ``AttributeError``). Our test suite hits that with
patches like ``patch("openbb_core.app.service.user_service.UserService.read_from_file")``
and ``patch("openbb_platform_api.app.args.parse_args", ...)``, which
both rely on the submodule being directly accessible as an attribute
on its parent package.

The fix is to import every submodule path the tests touch at
session-scoped import time so the bindings are stable for the entire
run, regardless of test order or interpreter version.
"""

# openbb-core service singletons — touched by patches in test_app.py
# and by the launcher's bootstrap / app.app code paths under test.
# openbb-core auth + provider plumbing — referenced by
# ``import_app``'s exception-handler wiring.
import openbb_core.api.exception_handlers  # noqa: F401
import openbb_core.app.service.system_service  # noqa: F401
import openbb_core.app.service.user_service  # noqa: F401

# Launcher submodules — args / bootstrap / config / spec / middleware
# all get patched directly via their dotted module paths in the test
# suite. Pre-importing them locks the parent-module binding. NOTE:
# we deliberately do NOT import ``openbb_platform_api.app.app`` here —
# that module's body calls ``parse_args()`` and (when ``--app`` isn't
# supplied) imports ``openbb_core.api.rest_api``, both of which have
# heavy side effects that the tests want to control on a per-test
# basis via ``importlib.import_module`` reloads.
import openbb_platform_api.app.args  # noqa: F401
import openbb_platform_api.app.bootstrap  # noqa: F401
import openbb_platform_api.app.config  # noqa: F401
import openbb_platform_api.app.middleware  # noqa: F401
import openbb_platform_api.app.spec  # noqa: F401
import openbb_platform_api.service.agents_service  # noqa: F401
import openbb_platform_api.service.apps_service  # noqa: F401
import openbb_platform_api.service.widgets_service  # noqa: F401
