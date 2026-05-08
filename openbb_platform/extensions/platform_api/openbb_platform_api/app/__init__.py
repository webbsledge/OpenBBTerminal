"""FastAPI app boot subpackage.

* ``args`` — command-line argument parsing for the ``openbb-api``
  launcher.
* ``bootstrap`` — pluggable FastAPI app loading
  (``import_app``) and platform-extension detection.
* ``app`` — the launcher entry point. Holds the FastAPI app instance,
  registers route handlers, exposes ``main`` / ``launch_api``.
"""

# Relative imports (rather than ``from openbb_platform_api.app.X
# import ...``) so the submodule attribute binding is reliable on
# Python 3.10 — see CPython issue #40500. The absolute form imports
# the module fine but doesn't always set ``args`` / ``bootstrap`` as
# an attribute on this package, which breaks
# ``mock.patch("openbb_platform_api.app.bootstrap.X")`` resolution
# (the resolver does ``getattr(parent, "bootstrap")`` and raises
# AttributeError). The relative-import form sets the binding
# unconditionally on every supported interpreter.
from .args import LAUNCH_SCRIPT_DESCRIPTION, parse_args
from .bootstrap import check_for_platform_extensions, import_app

__all__ = [
    "LAUNCH_SCRIPT_DESCRIPTION",
    "check_for_platform_extensions",
    "import_app",
    "parse_args",
]
