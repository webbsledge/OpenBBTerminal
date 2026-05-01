"""OpenBB Platform CLI entry point.

The default mode is **non-TTY**: argv → one command → JSON response → exit. This
is the form CI tools and agents reach for. Interactive REPL behavior is opt-in
via ``-i`` / ``--interactive``.

Three modes:

* ``openbb <command.path> [--key value ...]`` — one-shot dispatch.
* ``openbb --batch`` — NDJSON pipe protocol on stdin/stdout.
* ``openbb -i`` — interactive REPL with rich output (legacy behavior).

A ``--server URL`` flag (or ``OPENBB_SERVER_URL`` env var) routes the
non-interactive paths through an ``openbb-platform-api`` HTTP backend instead
of importing ``openbb`` in-process.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sys
from pathlib import Path

from openbb_cli.utils.utils import change_logging_sub_app, reset_logging_sub_app


def _parse_header_kv(token: str) -> tuple[str, str]:
    """Split a ``KEY=VALUE`` or ``KEY: VALUE`` header token.

    The HTTP spec uses ``KEY: VALUE``; the shell-friendly ``KEY=VALUE`` is
    accepted because shells often strip or mangle quoted colon-separated
    forms in ``-H`` arguments. Whichever separator appears first wins.
    """
    eq = token.find("=")
    colon = token.find(":")
    if eq == -1 and colon == -1:
        raise ValueError(f"--header must be 'KEY=VALUE' or 'KEY: VALUE'; got {token!r}")
    if eq != -1 and (colon == -1 or eq < colon):
        key, value = token[:eq], token[eq + 1 :]
    else:
        key, value = token[:colon], token[colon + 1 :]
    return key.strip(), value.strip()


def _resolve_headers(
    cli_headers: list[str] | None, header_file: str | None
) -> dict[str, str] | None:
    """Merge headers from ``--header-file`` (low priority) and ``--header``
    flags (high priority) into a single dict.
    """
    headers: dict[str, str] = {}
    if header_file:
        try:
            file_data = json.loads(Path(header_file).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            sys.stderr.write(f"--header-file: {exc}\n")
            return None
        if not isinstance(file_data, dict):
            sys.stderr.write("--header-file must contain a JSON object.\n")
            return None
        for k, v in file_data.items():
            headers[str(k)] = str(v)
    for token in cli_headers or []:
        k, v = _parse_header_kv(token)
        headers[k] = v
    return headers or None


def _build_dispatcher(server_url: str | None, headers: dict[str, str] | None = None):
    if server_url:
        # Auto-discover per-command HTTP methods from the server's OpenAPI doc;
        # without this the dispatcher POSTs every command and 405s on GET-only
        # endpoints (which is most of OpenBB Platform).
        from openbb_cli.dispatchers.http import http_dispatcher_from_server

        return http_dispatcher_from_server(server_url, headers=headers)
    from openbb_cli.dispatchers import LocalDispatcher

    return LocalDispatcher()


def _launch_repl(
    dev: bool,
    debug: bool,
    spec_path: str | None = None,
    server_url: str | None = None,
    headers: dict[str, str] | None = None,
) -> int:
    """Launch the interactive REPL.

    Backend selection priority: ``--spec`` > ``--server`` > in-process ``obb``.
    The spec path imports nothing from ``openbb`` — every menu, parser, and
    command dispatch goes through the spec + an HTTP dispatcher. ``headers``
    are sent on every dispatched request and on the OpenAPI fetch (when
    ``--server`` is the source).
    """
    sys.stdout.write("Loading...\n")
    sys.stdout.flush()
    from openbb_cli.config.setup import bootstrap
    from openbb_cli.controllers.cli_controller import launch

    backend: object | None = None
    if spec_path:
        from openbb_cli.backend import SpecBackend
        from openbb_cli.dispatchers.http import http_dispatcher_from_spec
        from openbb_cli.dispatchers.spec import load_spec

        spec_doc = load_spec(spec_path)
        backend = SpecBackend(
            spec_doc, http_dispatcher_from_spec(spec_doc, headers=headers)
        )
    elif server_url:
        from openbb_cli.backend import SpecBackend
        from openbb_cli.dispatchers.http import http_dispatcher_from_server
        from openbb_cli.dispatchers.openapi_schema import fetch_openapi
        from openbb_cli.dispatchers.spec import build_spec_document

        # Fetch the spec once at REPL startup; subsequent navigation is local.
        openapi = fetch_openapi(server_url, headers=headers)
        spec_doc = build_spec_document(openapi, base_url=server_url)
        backend = SpecBackend(
            spec_doc, http_dispatcher_from_server(server_url, headers=headers)
        )

    bootstrap()
    if backend is None:
        # Legacy in-process path: defer to ``launch`` which delegates to
        # ``parse_args_and_run`` for argv parsing.
        launch(dev, debug)
        return 0
    # Backend-driven REPL: bypass the legacy argv re-parser (which expects
    # different flags) and go straight to ``run_cli`` with the chosen backend.
    from openbb_cli.controllers.cli_controller import run_cli, session

    if debug:
        session.settings.DEBUG_MODE = True
    if dev:
        session.settings.DEV_BACKEND = True
    run_cli(backend=backend)  # type: ignore[arg-type]
    return 0


def _generate_spec(
    server_url: str | None,
    output_path: str,
    openapi_path: str | None,
    headers: dict[str, str] | None = None,
) -> int:
    """Fetch the server's OpenAPI document and write a precomputed .spec file."""
    if not server_url:
        sys.stderr.write(
            "--generate-spec requires --server URL (or OPENBB_SERVER_URL env var).\n"
        )
        return 2

    from openbb_cli.dispatchers.openapi_schema import fetch_openapi
    from openbb_cli.dispatchers.spec import build_spec_document, write_spec

    openapi = fetch_openapi(server_url, path=openapi_path, headers=headers)
    spec_doc = build_spec_document(openapi, base_url=server_url)
    write_spec(output_path, spec_doc)
    sys.stdout.write(f"wrote {len(spec_doc['commands'])} commands to {output_path}\n")
    return 0


def _run_spec_one_shot(
    spec_path: str,
    command_argv: list[str],
    headers: dict[str, str] | None = None,
) -> int:
    """Dispatch a single command using a precomputed .spec file.

    Skips OpenAPI fetch + parse entirely. The spec carries the server URL it
    was generated against and the per-command HTTP methods, so the caller
    doesn't need ``--server`` and the dispatcher routes GETs and POSTs
    correctly. ``headers`` are sent with the request.
    """
    from openbb_cli.dispatchers.http import http_dispatcher_from_spec
    from openbb_cli.dispatchers.protocol import Request, Response
    from openbb_cli.dispatchers.runtime import _to_json_line
    from openbb_cli.dispatchers.spec import (
        SpecCommandError,
        load_spec,
        parse_command_argv,
    )

    spec_doc = load_spec(spec_path)

    if not command_argv:
        sys.stderr.write("usage: openbb --spec PATH <command.path> [--key value]\n")
        return 2

    try:
        command, params = parse_command_argv(spec_doc, command_argv)
    except SpecCommandError as exc:
        sys.stderr.write(f"{exc}\n")
        return 2

    async def _dispatch_and_close() -> Response:
        dispatcher = http_dispatcher_from_spec(spec_doc, headers=headers)
        try:
            return await dispatcher.dispatch(Request(command=command, params=params))
        finally:
            await dispatcher.aclose()

    response: Response = asyncio.run(_dispatch_and_close())
    sys.stdout.write(_to_json_line(response) + "\n")
    sys.stdout.flush()
    return 0 if response.ok else 1


def main(argv: list[str] | None = None) -> int:
    """Use the main entry point for the OpenBB Platform CLI."""
    from openbb_cli.dispatchers.runtime import build_parser, run_argv, run_batch

    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    headers = _resolve_headers(getattr(args, "header", []), args.header_file)
    if headers is None and (args.header or args.header_file):
        # _resolve_headers returned None due to a parsing failure; the
        # error message has already been written to stderr.
        return 2

    if args.generate_spec:
        return _generate_spec(args.server, args.output, args.openapi_path, headers)

    # ``-i`` wins over ``--spec`` / ``--server`` so they can act as backend
    # sources for the REPL instead of triggering one-shot dispatch.
    if args.interactive:
        return _launch_repl(args.dev, args.debug, args.spec, args.server, headers)

    if args.spec:
        return _run_spec_one_shot(args.spec, args.command, headers)

    dispatcher = _build_dispatcher(args.server, headers=headers)

    if args.batch:
        return run_batch(dispatcher)

    if not args.command:
        parser.print_help(sys.stderr)
        return 2

    return run_argv(dispatcher, args.command)


if __name__ == "__main__":
    initial_logging_sub_app = change_logging_sub_app()
    try:
        sys.exit(main())
    except BrokenPipeError:
        # ``head``, ``grep -m``, etc. closing stdout early is normal in shells.
        # Exit cleanly without spamming a traceback.
        with contextlib.suppress(Exception):
            sys.stdout.close()
        sys.exit(0)
    except Exception:
        logging.exception("An unexpected error occurred")
        sys.exit(1)
    finally:
        reset_logging_sub_app(initial_logging_sub_app)
