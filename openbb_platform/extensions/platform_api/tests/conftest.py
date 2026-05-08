"""Test bootstrap for ``openbb-platform-api``.

Pre-imports openbb-core submodules that the test suite patches via
dotted ``mock.patch`` paths.
"""

import openbb_core.api.exception_handlers  # noqa: F401
import openbb_core.app.service.system_service  # noqa: F401
import openbb_core.app.service.user_service  # noqa: F401
