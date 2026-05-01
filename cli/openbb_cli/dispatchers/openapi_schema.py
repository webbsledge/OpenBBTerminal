"""Build CLI menu / parser structures from an OpenAPI 3.x schema.

Mirrors the surface of in-process ``obb`` reflection used by
``cli_controller._generate_platform_commands`` and ``PlatformControllerFactory``,
so the HTTP dispatcher path can offer the same REPL menu / argparse / completer
behavior without importing ``openbb`` locally.

This is the schema discovery layer only — it returns argparse parsers and a
router map. Wiring those into the existing controller machinery is a separate
step that lives in ``cli_controller`` once this layer is stable.

Supports both JSON and YAML specs. Handles three parameter locations:

* ``query`` — sent as URL query string on GET, JSON body on POST.
* ``path`` — substituted into the URL template (e.g. ``{operation}``) at
  dispatch time. Path params are always required.
* Other locations (``header``, ``cookie``, ``body``) are not currently
  surfaced; OpenBB Platform and NY Fed Markets API don't use them in the
  CLI surface.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import httpx

# OpenAPI primitive types we know how to translate to argparse types.
_TYPE_MAP: dict[str, type] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
}

# Standard OpenAPI schema keys; anything else is a per-provider extension
# (``fred``, ``fmp``, ``yfinance``, …) that we strip when reading the union.
_OPENAPI_RESERVED_SCHEMA_KEYS = frozenset(
    {
        "type",
        "format",
        "title",
        "description",
        "default",
        "enum",
        "const",
        "anyOf",
        "oneOf",
        "allOf",
        "items",
        "properties",
        "additionalProperties",
        "required",
        "nullable",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "pattern",
    }
)


def _resolve_schema(schema: dict[str, Any]) -> tuple[type, list[Any], bool]:
    """Resolve an OpenAPI parameter schema to ``(py_type, choices, is_list)``.

    Optionals (``anyOf`` containing ``{"type": "null"}``) collapse to their
    non-null variant; multi-type unions fall back to ``str`` and union the
    enums (matching how ``ArgparseTranslator`` handles ``str | int`` etc.).
    """
    if "anyOf" in schema:
        non_null = [s for s in schema["anyOf"] if s.get("type") != "null"]
        if not non_null:
            return (str, [], False)
        if len(non_null) == 1:
            return _resolve_schema(non_null[0])
        choices: list[Any] = []
        is_list = False
        for s in non_null:
            if s.get("type") == "array":
                is_list = True
            for v in s.get("enum", []):
                if v not in choices:
                    choices.append(v)
        return (str, choices, is_list)

    if schema.get("type") == "array":
        items = schema.get("items", {}) or {}
        py_type, item_choices, _ = _resolve_schema(items)
        return (py_type, item_choices, True)

    if "const" in schema:
        # Pinned single value — surface as a 1-element choice list.
        return (type(schema["const"]), [schema["const"]], False)

    py_type = _TYPE_MAP.get(schema.get("type", "string"), str)
    return (py_type, list(schema.get("enum", [])), False)


def _provider_choices(schema: dict[str, Any]) -> list[Any]:
    """Union of per-provider ``choices`` lists found in OpenBB extension keys.

    OpenBB embeds per-provider metadata under non-OpenAPI-reserved keys, e.g.
    ``"fred": {"choices": [...]}``. When a parameter is shared across providers,
    we want the parser's ``choices`` to be the union of all providers' lists.
    """
    out: list[Any] = []
    for key, value in schema.items():
        if key in _OPENAPI_RESERVED_SCHEMA_KEYS:
            continue
        if isinstance(value, dict) and isinstance(value.get("choices"), list):
            for c in value["choices"]:
                if c not in out:
                    out.append(c)
    return out


def _escape_help(text: str | None) -> str | None:
    """Escape lone ``%`` so argparse's %-formatting validator accepts the help text."""
    if text is None:
        return None
    return text.replace("%", "%%")


def parameter_to_kwargs(param: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Translate one OpenAPI parameter to ``(flag, add_argument kwargs)``.

    Returns ``None`` for parameters the CLI does not surface (e.g. ``chart``,
    which is handled by an output-adapter flag, not the command parser).
    """
    name = param.get("name")
    if not name or name == "chart":
        return None

    schema = param.get("schema", {}) or {}
    py_type, enum_choices, is_list = _resolve_schema(schema)
    provider_choices = _provider_choices(schema)
    # Merge provider-specific choices with the schema-level enum, preserving order.
    choices: list[Any] = list(enum_choices)
    for c in provider_choices:
        if c not in choices:
            choices.append(c)

    kwargs: dict[str, Any] = {
        "dest": name,
        "help": _escape_help(param.get("description") or schema.get("description")),
    }

    if py_type is bool:
        kwargs.update(action="store_true", default=bool(schema.get("default", False)))
        return (f"--{name}", kwargs)

    kwargs["type"] = py_type
    if "default" in schema:
        kwargs["default"] = schema["default"]
    if choices:
        kwargs["choices"] = choices
    if is_list:
        kwargs["nargs"] = "+"
    if param.get("required") and "default" not in kwargs:
        kwargs["required"] = True

    return (f"--{name}", kwargs)


def build_parser_from_operation(op: dict[str, Any]) -> argparse.ArgumentParser:
    """Build an ``ArgumentParser`` from an OpenAPI operation object."""
    parser = argparse.ArgumentParser(
        prog=op.get("operationId", "cmd"),
        description=(op.get("description") or op.get("summary") or "").strip() or None,
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    for param in op.get("parameters", []):
        translated = parameter_to_kwargs(param)
        if translated is None:
            continue
        flag, kwargs = translated
        try:
            parser.add_argument(flag, **kwargs)
        except argparse.ArgumentError:
            # Duplicate name across providers — argparse rejects on second add.
            # Skip; the first wins (matches in-process ArgparseTranslator).
            continue
    return parser


def _strip_placeholders(segment: str) -> str:
    """Remove ``{placeholder}`` substrings from a URL segment.

    ``latest.{format}`` → ``latest``, ``{operation}`` → ``""``,
    ``timeseries.csv`` → ``timeseries.csv`` (no placeholders, untouched).
    """
    out = segment
    while "{" in out and "}" in out:
        i = out.find("{")
        j = out.find("}", i)
        if j < 0:
            break
        out = out[:i] + out[j + 1 :]
    return out.strip(".")


def detect_api_prefix(spec: dict[str, Any]) -> str:
    """Compute the longest slash-segmented prefix shared by every path.

    Lets one ``--generate-spec`` invocation work against arbitrary OpenAPI
    servers — OpenBB Platform's ``/api/v1``, NY Fed's ``/api``, the empty
    prefix some specs use, etc. Falls back to ``/api/v1`` when the spec is
    empty or paths share no common segments.

    Each path's last segment is excluded from the comparison so the
    detected prefix never absorbs a leaf endpoint name.
    """
    paths = list(spec.get("paths", {}).keys())
    if not paths:
        return "/api/v1"
    parts_lists = [p.split("/")[:-1] for p in paths]
    common: list[str] = parts_lists[0]
    for parts in parts_lists[1:]:
        new_common: list[str] = []
        for a, b in zip(common, parts):
            if a == b:
                new_common.append(a)
            else:
                break
        common = new_common
        if not common:
            break
    prefix = "/".join(common).rstrip("/")
    return prefix or ""


def url_to_command(url: str, api_prefix: str = "/api/v1") -> str:
    """``/api/v1/commodity/price/spot`` → ``commodity.price.spot``.

    URL templates with ``{path_params}`` have those placeholders dropped,
    yielding a clean dotted command that the user types. Path params are
    surfaced as required CLI flags and substituted into the URL at dispatch
    time. Example: ``/api/ambs/{operation}/{status}/{include}/latest.{format}``
    → ``ambs.latest``.
    """
    prefix_parts = [p for p in api_prefix.strip("/").split("/") if p]
    parts = [p for p in url.strip("/").split("/") if p]
    if parts[: len(prefix_parts)] == prefix_parts:
        parts = parts[len(prefix_parts) :]
    cleaned = [_strip_placeholders(p) for p in parts]
    return ".".join(p for p in cleaned if p)


def build_command_index(
    spec: dict[str, Any], *, api_prefix: str = "/api/v1"
) -> dict[str, argparse.ArgumentParser]:
    """Map each ``paths`` entry in the spec to an ArgumentParser by dotted command."""
    index: dict[str, argparse.ArgumentParser] = {}
    for url, methods in spec.get("paths", {}).items():
        op = methods.get("get") or methods.get("post")
        if not op:
            continue
        index[url_to_command(url, api_prefix=api_prefix)] = build_parser_from_operation(
            op
        )
    return index


def build_router_map(
    spec: dict[str, Any], *, api_prefix: str = "/api/v1"
) -> dict[str, str]:
    """Classify every dotted path in ``spec`` as ``"menu"`` or ``"command"``.

    Mirrors ``cli_controller.PLATFORM_ROUTERS``. Every non-leaf prefix becomes
    a ``"menu"``; each leaf becomes a ``"command"``. ``commodity.price.spot``
    therefore yields ``commodity`` → menu, ``commodity.price`` → menu,
    ``commodity.price.spot`` → command.
    """
    out: dict[str, str] = {}
    for url, methods in spec.get("paths", {}).items():
        if not (methods.get("get") or methods.get("post")):
            continue
        cmd = url_to_command(url, api_prefix=api_prefix)
        if not cmd:
            continue
        parts = cmd.split(".")
        for i in range(1, len(parts)):
            out[".".join(parts[:i])] = "menu"
        out.setdefault(cmd, "command")
    return out


def build_reference(
    spec: dict[str, Any], *, api_prefix: str = "/api/v1"
) -> dict[str, Any]:
    """Mimic ``obb.reference`` — ``paths`` for commands, ``routers`` for menus.

    Keys are slash-style (``/equity/price/historical``, ``/equity/price/``)
    matching the in-process ``obb.reference`` layout that ``cli_controller``
    and ``base_platform_controller`` already index against.
    """
    prefix_parts = [p for p in api_prefix.strip("/").split("/") if p]
    tag_descriptions: dict[str, str] = {
        t["name"]: t.get("description", "")
        for t in spec.get("tags", [])
        if isinstance(t, dict) and t.get("name")
    }

    paths_out: dict[str, dict[str, Any]] = {}
    routers_out: dict[str, dict[str, Any]] = {}
    for url, methods in spec.get("paths", {}).items():
        op = methods.get("get") or methods.get("post")
        if not op:
            continue
        parts = [p for p in url.strip("/").split("/") if p]
        if parts[: len(prefix_parts)] == prefix_parts:
            parts = parts[len(prefix_parts) :]
        cli_path = "/" + "/".join(parts)
        paths_out[cli_path] = {
            "description": (op.get("description") or op.get("summary") or "").strip(),
        }
        for i in range(1, len(parts)):
            menu_path = "/" + "/".join(parts[:i]) + "/"
            if menu_path in routers_out:
                continue
            tags = op.get("tags") or []
            if tags and tags[0] in tag_descriptions:
                routers_out[menu_path] = {"description": tag_descriptions[tags[0]]}
            else:
                routers_out[menu_path] = {"description": ""}
    return {"paths": paths_out, "routers": routers_out}


def _parse_spec_text(text: str, *, content_type: str = "") -> dict[str, Any]:
    """Parse a fetched spec body, choosing JSON or YAML by content sniff."""
    stripped = text.lstrip()
    if stripped.startswith(("{", "[")):
        return json.loads(text)
    if "yaml" in content_type or "yml" in content_type:
        return _yaml_load(text)
    # Heuristic: an OpenAPI YAML doc starts with ``openapi:`` (or ``swagger:``).
    if stripped[:8] in ("openapi:", "swagger:"):
        return _yaml_load(text)
    # Last resort: try JSON, fall back to YAML.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return _yaml_load(text)


def _yaml_load(text: str) -> dict[str, Any]:
    """Lazy YAML import — keeps the import graph light when JSON specs suffice."""
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required to parse YAML OpenAPI specs. "
            "Install with: pip install pyyaml"
        ) from exc
    return yaml.safe_load(text)


def fetch_openapi(
    base_url: str,
    *,
    timeout: float = 10.0,
    path: str | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fetch the OpenAPI spec at ``base_url`` and return the parsed dict.

    By default tries ``{base_url}/openapi.json``. Override with ``path`` for
    servers that publish under a different name (e.g. NY Fed's
    ``/static/docs/markets-api.yml``). Both JSON and YAML payloads are
    supported. ``headers`` (e.g. ``Authorization``) are merged with a
    default ``User-Agent`` and sent with the request.
    """
    full_url = (
        path
        if path and (path.startswith("http://") or path.startswith("https://"))
        else f"{base_url.rstrip('/')}{path or '/openapi.json'}"
    )
    merged_headers = {"User-Agent": "openbb-cli/1.0"}
    if headers:
        merged_headers.update(headers)
    response = httpx.get(
        full_url,
        timeout=timeout,
        follow_redirects=True,
        headers=merged_headers,
    )
    response.raise_for_status()
    return _parse_spec_text(
        response.text, content_type=response.headers.get("content-type", "")
    )
