"""Pure utility helpers for the OpenBB Platform API extension.

* ``network`` — port probing and user-settings loading.
* ``openapi`` — OpenAPI schema → widget-config translation.
* ``widgets`` — widgets.json builder + provider-aware schema modifier.
* ``merge_widgets`` — discover & merge router-attached widget endpoints.
"""

# Relative import for reliable submodule attribute binding on
# Python 3.10 — see CPython issue #40500.
from .network import check_port, get_user_settings

__all__ = [
    "check_port",
    "get_user_settings",
]
