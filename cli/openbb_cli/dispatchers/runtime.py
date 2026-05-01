"""Runtime entry points for the dispatcher subsystem.

Two surface forms share the same Dispatcher and same wire format:

* ``run_argv`` — convert argv to one Request, dispatch, print, exit.
* ``run_batch`` — read NDJSON from a stream, fan requests out as concurrent
  asyncio Tasks, write responses to stdout as each completes (out-of-order).
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import json
import os
import sys
from collections.abc import Iterable
from typing import IO, Any

from openbb_cli.dispatchers.base import Dispatcher
from openbb_cli.dispatchers.protocol import Request, Response, ResponseError

DEFAULT_BATCH_CONCURRENCY = 8


_JSON_LITERALS = {"true": True, "false": False, "null": None}


def _coerce_literal(text: str) -> Any:
    """Best-effort coerce a CLI string to a Python literal.

    `--limit=10` → 10, `--flag=true` → True, `--names=[1,2]` → [1, 2],
    `--name=foo` → "foo". JSON-style ``true``/``false``/``null`` are
    supported in addition to Python literals.
    """
    if text in _JSON_LITERALS:
        return _JSON_LITERALS[text]
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return text


def parse_argv(argv: Iterable[str]) -> Request:
    """Convert positional+kv argv into a Request.

    Form: ``<command.path> [--key value] [--key=value] [--flag]``

    The first positional token is the dotted command path; everything after is
    parameter key/value pairs. Boolean flags (no value) become ``True``.
    """
    args = list(argv)
    if not args:
        raise SystemExit("usage: openbb <command.path> [--key value | --key=value]")
    command = args[0]
    params: dict[str, Any] = {}
    i = 1
    while i < len(args):
        token = args[i]
        if not token.startswith("--"):
            raise SystemExit(f"Unexpected positional argument: {token!r}")
        body = token[2:]
        if "=" in body:
            key, _, value = body.partition("=")
            params[key.replace("-", "_")] = _coerce_literal(value)
            i += 1
            continue
        # Either next token is a value, or this is a bool flag.
        if i + 1 < len(args) and not args[i + 1].startswith("--"):
            params[body.replace("-", "_")] = _coerce_literal(args[i + 1])
            i += 2
        else:
            params[body.replace("-", "_")] = True
            i += 1
    return Request(command=command, params=params)


async def _run_one(dispatcher: Dispatcher, request: Request) -> Response:
    return await dispatcher.dispatch(request)


def _to_json_line(response: Response) -> str:
    """Serialize a Response to a single JSON line, tolerating non-serializable nested values.

    OBBject results carry pandas DataFrames, charts (``OpenBBFigure``), datetime
    fields, and other Python objects that ``pydantic_core.to_json`` rejects.
    Going through ``model_dump`` + ``json.dumps(..., default=str)`` lets those
    fall back to their string repr instead of crashing the CLI.
    """
    return json.dumps(response.model_dump(), default=str)


def run_argv(dispatcher: Dispatcher, argv: Iterable[str]) -> int:
    """Dispatch a single command from argv and print the JSON response.

    Returns a process exit code: 0 on success, 1 on dispatcher-reported error.
    """
    request = parse_argv(argv)
    response = asyncio.run(_run_one(dispatcher, request))
    sys.stdout.write(_to_json_line(response) + "\n")
    sys.stdout.flush()
    return 0 if response.ok else 1


async def _batch_loop(
    dispatcher: Dispatcher,
    reader: IO[str],
    writer: IO[str],
    *,
    concurrency: int,
) -> int:
    """Core async loop for ``run_batch``.

    Reads NDJSON requests, schedules each as its own Task. Writes responses to
    ``writer`` in completion order. A bounded semaphore caps concurrent
    in-flight tasks so memory does not balloon under fast producers.
    """
    sem = asyncio.Semaphore(concurrency)
    in_flight: set[asyncio.Task[Response]] = set()
    write_lock = asyncio.Lock()
    failures = 0

    async def _drain(task: asyncio.Task[Response]) -> None:
        nonlocal failures
        try:
            response = await task
        finally:
            sem.release()
        async with write_lock:
            writer.write(_to_json_line(response) + "\n")
            writer.flush()
        if not response.ok:
            failures += 1

    loop = asyncio.get_running_loop()

    def _readline() -> str:
        return reader.readline()

    while True:
        line = await loop.run_in_executor(None, _readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        payload: Any = None
        try:
            payload = json.loads(line)
            request = Request.model_validate(payload)
        except Exception as exc:  # noqa: BLE001 — surface parse failures as errors
            err = Response(
                id=payload.get("id") if isinstance(payload, dict) else None,
                ok=False,
                error=ResponseError(type="RequestParseError", message=str(exc)),
            )
            async with write_lock:
                writer.write(err.model_dump_json() + "\n")
                writer.flush()
            failures += 1
            continue

        await sem.acquire()
        task = asyncio.create_task(_run_one(dispatcher, request))
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)
        asyncio.create_task(_drain(task))

    if in_flight:
        await asyncio.gather(*in_flight, return_exceptions=True)
    return 0 if failures == 0 else 1


def run_batch(
    dispatcher: Dispatcher,
    reader: IO[str] | None = None,
    writer: IO[str] | None = None,
    *,
    concurrency: int | None = None,
) -> int:
    """Read NDJSON requests from ``reader``, write responses to ``writer``."""
    reader = reader if reader is not None else sys.stdin
    writer = writer if writer is not None else sys.stdout
    if concurrency is None:
        concurrency = int(
            os.environ.get("OPENBB_CLI_BATCH_CONCURRENCY", DEFAULT_BATCH_CONCURRENCY)
        )

    async def _run() -> int:
        try:
            return await _batch_loop(
                dispatcher, reader, writer, concurrency=concurrency
            )
        finally:
            await dispatcher.aclose()

    return asyncio.run(_run())


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser used by ``openbb_cli.cli:main``."""
    parser = argparse.ArgumentParser(
        prog="openbb",
        description="OpenBB Platform CLI. Default mode is non-TTY.",
        add_help=True,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Enter the interactive REPL with rich output and prompt-toolkit completion.",
    )
    mode.add_argument(
        "--batch",
        action="store_true",
        help="Read NDJSON requests from stdin; emit NDJSON responses to stdout.",
    )
    mode.add_argument(
        "--generate-spec",
        action="store_true",
        help=(
            "Fetch ``openapi.json`` from --server and write a precomputed "
            ".spec file to --output. Use --spec on subsequent invocations "
            "to skip the OpenAPI fetch + parse on every call."
        ),
    )
    parser.add_argument(
        "--server",
        metavar="URL",
        default=os.environ.get("OPENBB_SERVER_URL"),
        help=(
            "Dispatch through an openbb-platform-api server at URL "
            "(env: OPENBB_SERVER_URL). Default: in-process LocalDispatcher."
        ),
    )
    parser.add_argument(
        "--spec",
        metavar="PATH",
        default=os.environ.get("OPENBB_SPEC_PATH"),
        help=(
            "Use a precomputed .spec file (built via --generate-spec) instead "
            "of fetching openapi.json. The spec carries the server URL it was "
            "generated against, so --server is not required when using --spec. "
            "(env: OPENBB_SPEC_PATH)"
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="PATH",
        default="openbb.spec",
        help="Output path for --generate-spec (default: openbb.spec).",
    )
    parser.add_argument(
        "--openapi-path",
        metavar="PATH",
        default=None,
        help=(
            "Path (or full URL) to the OpenAPI document on the server. "
            "Defaults to /openapi.json. Servers that publish under a "
            "different name (e.g. NY Fed at /static/docs/markets-api.yml) "
            "need this. JSON or YAML accepted."
        ),
    )
    parser.add_argument(
        "-H",
        "--header",
        metavar="KEY=VALUE",
        action="append",
        default=[],
        help=(
            "Custom HTTP header to send on every dispatch and on the "
            "OpenAPI fetch. Repeatable: ``-H 'Authorization: Bearer xxx' "
            "-H 'X-Tenant: acme'``. Both ``KEY=VALUE`` and ``KEY: VALUE`` "
            "forms are accepted."
        ),
    )
    parser.add_argument(
        "--header-file",
        metavar="PATH",
        default=os.environ.get("OPENBB_HEADER_FILE"),
        help=(
            "Read additional headers from a JSON file (object of string "
            "values). Headers from --header take precedence on conflicts. "
            "(env: OPENBB_HEADER_FILE)"
        ),
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Developer mode (verbose backend hooks).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug logging.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Dotted command path followed by --key value pairs.",
    )
    return parser
