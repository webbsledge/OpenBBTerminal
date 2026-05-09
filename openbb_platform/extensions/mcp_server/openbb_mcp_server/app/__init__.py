"""MCP server boot subpackage.

* ``args`` — command-line argument parsing for the ``openbb-mcp``
  launcher.
* ``bootstrap`` — pluggable FastAPI app loading (``import_app``).
* ``cli_tools`` — registers ``openbb-cli`` dispatcher tools when the
  optional ``[cli]`` extra is installed.
* ``config`` — layered TOML cascade + ``[env]`` injection bootstrap.
* ``middleware`` — auth + HTTP middleware hook resolver.
* ``spec`` — synthesize a FastAPI proxy app from an ``openbb-cli``
  generated ``.spec`` file.
* ``app`` — the launcher entry point. Holds ``create_mcp_server`` and
  ``launch_mcp``.

Submodules are imported here (rather than via the dotted ``from
openbb_mcp_server.app.X import …`` form) so they're explicitly bound
as attributes on this package — ``mock.patch`` resolution stays
reliable across CPython 3.10's namespace-package quirk and any test
ordering that re-imports cached parents.
"""

from openbb_mcp_server.app import (
    args,
    bootstrap,
    cli_tools,
    config,
    middleware,
    spec,
)

__all__ = [
    "args",
    "bootstrap",
    "cli_tools",
    "config",
    "middleware",
    "spec",
]
