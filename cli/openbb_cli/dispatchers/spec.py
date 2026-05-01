"""Pre-computed spec format for instant one-shot CLI startup.

The OpenAPI schema describes 280+ endpoints in 2+ MB of JSON. Parsing it on
every CLI invocation costs ~50ms even before the HTTP roundtrip. A ``.spec``
file is a normalized, denormalized snapshot of just the bits the CLI consumes:

* Per-command URL + method + parameter list (already digested types/choices).
* Router/menu classification.
* Reference descriptions for help text.

Loading a spec is a single ``json.loads`` and dictionary lookup — no schema
walk, no ``anyOf`` resolution, no provider-extension scanning. Generate once
when the server's surface changes, ship the file alongside whatever invokes
the CLI, and one-shot dispatches become near-instant.

Usage::

    openbb --generate-spec --server http://api:6900 -o cli.spec
    openbb --spec cli.spec equity.price.historical --symbol AAPL
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openbb_cli.dispatchers.openapi_schema import (
    _escape_help,
    _provider_choices,
    _resolve_schema,
    build_reference,
    build_router_map,
    detect_api_prefix,
    url_to_command,
)

SPEC_VERSION = 1


_TYPE_TO_NAME: dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}
_NAME_TO_TYPE: dict[str, type] = {v: k for k, v in _TYPE_TO_NAME.items()}


def _normalize_parameter(param: dict[str, Any]) -> dict[str, Any] | None:
    """Translate one OpenAPI parameter into the spec-file parameter form.

    Returns ``None`` for parameters the CLI does not surface (e.g. ``chart``).
    Records the ``in`` location (``query`` / ``path`` / ``header``) so the
    dispatcher knows whether to substitute the value into the URL template
    or send it as a query / body parameter.
    """
    name = param.get("name")
    if not name or name == "chart":
        return None

    location = param.get("in", "query")
    schema = param.get("schema", {}) or {}
    py_type, enum_choices, is_list = _resolve_schema(schema)
    choices: list[Any] = list(enum_choices)
    for c in _provider_choices(schema):
        if c not in choices:
            choices.append(c)

    required = bool(param.get("required")) or location == "path"
    if "default" in schema and location != "path":
        required = False

    return {
        "name": name,
        "in": location,
        "type": _TYPE_TO_NAME.get(py_type, "string"),
        "is_list": is_list,
        "required": required,
        "default": schema.get("default"),
        "choices": choices,
        "help": param.get("description") or schema.get("description"),
    }


def build_command_spec(
    spec: dict[str, Any], *, api_prefix: str = "/api/v1"
) -> dict[str, dict[str, Any]]:
    """Return ``{dotted_command: {url_path, method, description, parameters[]}}``.

    The output is the per-command portion of the on-disk spec file — already
    digested into argparse-ready primitives. Collisions (multiple URLs that
    strip to the same dotted name after dropping ``{path_params}``) are
    disambiguated by appending ``_2``, ``_3``, … in URL-iteration order.
    """
    out: dict[str, dict[str, Any]] = {}
    for url, methods in spec.get("paths", {}).items():
        method: str | None
        op: dict[str, Any] | None
        if "get" in methods:
            method, op = "get", methods["get"]
        elif "post" in methods:
            method, op = "post", methods["post"]
        else:
            continue
        base_cmd = url_to_command(url, api_prefix=api_prefix)
        if not base_cmd:
            continue
        cmd = base_cmd
        suffix = 2
        while cmd in out:
            cmd = f"{base_cmd}_{suffix}"
            suffix += 1
        params: list[dict[str, Any]] = []
        for raw in op.get("parameters", []) or []:
            normalized = _normalize_parameter(raw)
            if normalized is not None:
                params.append(normalized)
        out[cmd] = {
            "url_path": url,
            "method": method,
            "description": (op.get("description") or op.get("summary") or "").strip(),
            "parameters": params,
        }
    return out


def build_spec_document(
    openapi: dict[str, Any],
    *,
    base_url: str,
    api_prefix: str | None = None,
) -> dict[str, Any]:
    """Build the on-disk spec dict from a fetched OpenAPI document.

    ``api_prefix`` defaults to the longest path-prefix shared by every URL
    in the spec — OpenBB Platform's ``/api/v1``, NY Fed's ``/api``, the empty
    prefix, etc. Pass an explicit value to override the heuristic.
    """
    effective_prefix = (
        api_prefix if api_prefix is not None else detect_api_prefix(openapi)
    )
    return {
        "version": SPEC_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url.rstrip("/"),
        "api_prefix": (("/" + effective_prefix.strip("/")) if effective_prefix else ""),
        "commands": build_command_spec(openapi, api_prefix=effective_prefix),
        "routers": build_router_map(openapi, api_prefix=effective_prefix),
        "reference": build_reference(openapi, api_prefix=effective_prefix),
    }


def write_spec(path: str | Path, spec_doc: dict[str, Any]) -> None:
    """Write the spec doc to ``path`` as compact JSON."""
    Path(path).write_text(json.dumps(spec_doc, separators=(",", ":")))


def load_spec(path: str | Path) -> dict[str, Any]:
    """Load a spec doc; reject incompatible versions early."""
    spec_doc = json.loads(Path(path).read_text())
    version = spec_doc.get("version")
    if version != SPEC_VERSION:
        raise ValueError(
            f"Spec file at {path} has version {version!r}; expected {SPEC_VERSION}. "
            "Regenerate with --generate-spec."
        )
    return spec_doc


def parser_from_command_spec(cmd_spec: dict[str, Any]) -> argparse.ArgumentParser:
    """Build an ArgumentParser from one entry in ``spec["commands"]``."""
    parser = argparse.ArgumentParser(
        prog=cmd_spec.get("url_path", "cmd").rsplit("/", 1)[-1] or "cmd",
        description=cmd_spec.get("description") or None,
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    for p in cmd_spec.get("parameters", []) or []:
        try:
            _add_normalized_parameter(parser, p)
        except argparse.ArgumentError:
            continue
    return parser


def _add_normalized_parameter(
    parser: argparse.ArgumentParser, p: dict[str, Any]
) -> None:
    """Translate one normalized parameter dict into a parser argument."""
    flag = f"--{p['name']}"
    py_type = _NAME_TO_TYPE.get(p.get("type", "string"), str)
    kwargs: dict[str, Any] = {"dest": p["name"], "help": _escape_help(p.get("help"))}

    if py_type is bool:
        kwargs.update(action="store_true", default=bool(p.get("default") or False))
        parser.add_argument(flag, **kwargs)
        return

    kwargs["type"] = py_type
    if p.get("default") is not None:
        kwargs["default"] = p["default"]
    if p.get("choices"):
        kwargs["choices"] = list(p["choices"])
    if p.get("is_list"):
        kwargs["nargs"] = "+"
    if p.get("required") and "default" not in kwargs:
        kwargs["required"] = True
    parser.add_argument(flag, **kwargs)


class SpecCommandError(KeyError):
    """Raised when a dotted command path is not present in the spec."""


def parse_command_argv(
    spec_doc: dict[str, Any], argv: list[str]
) -> tuple[str, dict[str, Any]]:
    """Resolve ``argv`` against ``spec_doc['commands']`` → (command, params).

    The first token is the dotted command path; everything after is parsed
    by the parser built from that command's normalized parameter list.
    """
    if not argv:
        raise SpecCommandError(
            "missing command — usage: openbb --spec PATH <command.path> [--key value]"
        )
    command, rest = argv[0], list(argv[1:])
    cmd_spec = spec_doc.get("commands", {}).get(command)
    if cmd_spec is None:
        raise SpecCommandError(f"command not in spec: {command!r}")
    parser = parser_from_command_spec(cmd_spec)
    ns = parser.parse_args(rest)
    params = {k: v for k, v in vars(ns).items() if v is not None}
    return command, params
