"""Backwards-compatibility shim.

The launcher's helpers were split across purpose-specific modules in
the V5 layout reorganization:

* ``parse_args`` and ``LAUNCH_SCRIPT_DESCRIPTION`` moved to
  ``openbb_platform_api.app.args``.
* ``import_app`` moved to ``openbb_platform_api.app.bootstrap``.
* ``check_port`` and ``get_user_settings`` moved to
  ``openbb_platform_api.utils.network``.
* ``get_widgets_json`` plus its module-level ``FIRST_RUN`` /
  ``PATH_WIDGETS`` state moved to
  ``openbb_platform_api.service.widgets_service``.

This module preserves every legacy import path so external callers
keep working — new code should import from the dedicated modules.
"""

from openbb_platform_api.app.args import LAUNCH_SCRIPT_DESCRIPTION, parse_args
from openbb_platform_api.app.bootstrap import import_app
from openbb_platform_api.service import widgets_service
from openbb_platform_api.service.widgets_service import (
    get_widgets_json,
    logger,
)
from openbb_platform_api.utils.network import check_port, get_user_settings


def __getattr__(name):
    """Re-export the mutable launcher state from ``widgets_service``.

    ``FIRST_RUN`` and ``PATH_WIDGETS`` are state, not constants — code
    flips ``FIRST_RUN`` to ``False`` after the first request, so a
    plain top-level import would silently snapshot the boot-time value.
    Routing the attribute access through ``__getattr__`` keeps the
    legacy import path live without forking the state.
    """
    if name == "FIRST_RUN":
        return widgets_service.FIRST_RUN
    if name == "PATH_WIDGETS":
        return widgets_service.PATH_WIDGETS
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "LAUNCH_SCRIPT_DESCRIPTION",
    "check_port",
    "get_user_settings",
    "get_widgets_json",
    "import_app",
    "logger",
    "parse_args",
]
