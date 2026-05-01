"""Tests for the OpenAPI schema loader.

These exercise the pure-translation layer against synthetic spec fragments —
no live server required. The goal is to lock in the parameter → argparse
mapping behavior so it stays consistent as the CLI grows to consume the spec
from a remote ``openbb-platform-api`` server.
"""

from __future__ import annotations

import argparse

import pytest

from openbb_cli.dispatchers.openapi_schema import (
    _provider_choices,
    _resolve_schema,
    build_command_index,
    build_parser_from_operation,
    build_reference,
    build_router_map,
    parameter_to_kwargs,
    url_to_command,
)


def test_resolve_schema_primitive_string():
    assert _resolve_schema({"type": "string"}) == (str, [], False)


def test_resolve_schema_primitive_int():
    assert _resolve_schema({"type": "integer"}) == (int, [], False)


def test_resolve_schema_primitive_number():
    assert _resolve_schema({"type": "number"}) == (float, [], False)


def test_resolve_schema_boolean():
    assert _resolve_schema({"type": "boolean"}) == (bool, [], False)


def test_resolve_schema_enum():
    py_type, choices, is_list = _resolve_schema({"type": "string", "enum": ["a", "b"]})
    assert py_type is str and choices == ["a", "b"] and is_list is False


def test_resolve_schema_array_of_strings():
    py_type, choices, is_list = _resolve_schema(
        {"type": "array", "items": {"type": "string"}}
    )
    assert py_type is str and is_list is True and choices == []


def test_resolve_schema_optional_collapses_to_inner():
    """``anyOf: [{type: string}, {type: null}]`` → str."""
    py_type, _, _ = _resolve_schema({"anyOf": [{"type": "string"}, {"type": "null"}]})
    assert py_type is str


def test_resolve_schema_union_of_str_int_falls_back_to_str():
    py_type, _, _ = _resolve_schema(
        {"anyOf": [{"type": "string"}, {"type": "integer"}]}
    )
    assert py_type is str


def test_resolve_schema_union_of_enums_unions_choices():
    _, choices, _ = _resolve_schema(
        {
            "anyOf": [
                {"type": "string", "enum": ["a", "b"]},
                {"type": "string", "enum": ["b", "c"]},
            ]
        }
    )
    assert set(choices) == {"a", "b", "c"}


def test_resolve_schema_array_in_union_marked_as_list():
    _, _, is_list = _resolve_schema(
        {"anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}]}
    )
    assert is_list is True


def test_resolve_schema_const_treated_as_single_choice():
    py_type, choices, _ = _resolve_schema({"const": "fred"})
    assert py_type is str and choices == ["fred"]


def test_provider_choices_skips_reserved_keys():
    schema = {
        "type": "string",
        "enum": ["a", "b"],
        "default": "a",
        "title": "x",
    }
    assert _provider_choices(schema) == []


def test_provider_choices_unions_per_provider_choices():
    schema = {
        "type": "string",
        "fmp": {"choices": ["1m", "5m"]},
        "yfinance": {"choices": ["5m", "1d"]},
    }
    assert set(_provider_choices(schema)) == {"1m", "5m", "1d"}


def test_parameter_to_kwargs_skips_chart():
    """``chart`` is handled by the output adapter, not the command parser."""
    assert parameter_to_kwargs({"name": "chart", "schema": {"type": "boolean"}}) is None


def test_parameter_to_kwargs_required_string():
    flag, kwargs = parameter_to_kwargs(
        {"name": "symbol", "required": True, "schema": {"type": "string"}}
    )
    assert flag == "--symbol"
    assert kwargs["type"] is str
    assert kwargs.get("required") is True


def test_parameter_to_kwargs_optional_with_default():
    flag, kwargs = parameter_to_kwargs(
        {
            "name": "limit",
            "required": False,
            "schema": {"type": "integer", "default": 10},
        }
    )
    assert flag == "--limit"
    assert kwargs["type"] is int
    assert kwargs["default"] == 10
    assert "required" not in kwargs


def test_parameter_to_kwargs_boolean_flag():
    _, kwargs = parameter_to_kwargs(
        {"name": "verbose", "schema": {"type": "boolean", "default": False}}
    )
    assert kwargs["action"] == "store_true"
    assert kwargs["default"] is False


def test_parameter_to_kwargs_list_uses_nargs_plus():
    _, kwargs = parameter_to_kwargs(
        {
            "name": "symbols",
            "schema": {"type": "array", "items": {"type": "string"}},
        }
    )
    assert kwargs["nargs"] == "+"


def test_parameter_to_kwargs_enum_emits_choices():
    _, kwargs = parameter_to_kwargs(
        {"name": "side", "schema": {"type": "string", "enum": ["buy", "sell"]}}
    )
    assert kwargs["choices"] == ["buy", "sell"]


def test_parameter_to_kwargs_provider_choices_unioned_with_enum():
    """Per-provider ``choices`` are merged with the schema-level enum."""
    _, kwargs = parameter_to_kwargs(
        {
            "name": "interval",
            "schema": {
                "type": "string",
                "enum": ["1m", "5m"],
                "fmp": {"choices": ["5m", "1h"]},
                "yfinance": {"choices": ["1d"]},
            },
        }
    )
    assert set(kwargs["choices"]) == {"1m", "5m", "1h", "1d"}


def test_parameter_to_kwargs_help_percent_escaped():
    """A ``%`` in the description is doubled so argparse's validator accepts it."""
    _, kwargs = parameter_to_kwargs(
        {"name": "x", "description": "100% sure", "schema": {"type": "string"}}
    )
    assert kwargs["help"] == "100%% sure"


def test_parameter_to_kwargs_no_name_returns_none():
    assert parameter_to_kwargs({"schema": {"type": "string"}}) is None


def test_build_parser_uses_operation_metadata():
    op = {
        "operationId": "my_cmd",
        "description": "Do the thing.",
        "parameters": [
            {"name": "name", "required": True, "schema": {"type": "string"}},
            {"name": "n", "schema": {"type": "integer", "default": 5}},
        ],
    }
    parser = build_parser_from_operation(op)
    assert parser.prog == "my_cmd"
    assert "Do the thing." in (parser.description or "")
    ns = parser.parse_args(["--name", "x", "--n", "7"])
    assert ns.name == "x" and ns.n == 7


def test_build_parser_required_arg_enforced():
    op = {
        "operationId": "x",
        "parameters": [
            {"name": "needed", "required": True, "schema": {"type": "string"}}
        ],
    }
    parser = build_parser_from_operation(op)
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_build_parser_invalid_choice_rejected():
    op = {
        "operationId": "x",
        "parameters": [
            {
                "name": "side",
                "schema": {"type": "string", "enum": ["buy", "sell"]},
            }
        ],
    }
    parser = build_parser_from_operation(op)
    with pytest.raises(SystemExit):
        parser.parse_args(["--side", "hold"])


def test_build_parser_skips_chart_parameter():
    op = {
        "operationId": "x",
        "parameters": [
            {"name": "chart", "schema": {"type": "boolean"}},
            {"name": "symbol", "schema": {"type": "string", "default": "X"}},
        ],
    }
    parser = build_parser_from_operation(op)
    optstrings = {opt for action in parser._actions for opt in action.option_strings}
    assert "--chart" not in optstrings
    assert "--symbol" in optstrings


def test_build_parser_skips_invalid_parameter_silently():
    """Duplicate flags raise ``ArgumentError`` and are skipped — first wins."""
    op = {
        "operationId": "x",
        "parameters": [
            {"name": "dup", "schema": {"type": "string", "default": "first"}},
            {"name": "dup", "schema": {"type": "integer", "default": 99}},
        ],
    }
    parser = build_parser_from_operation(op)
    ns = parser.parse_args([])
    assert ns.dup == "first"


@pytest.mark.parametrize(
    "url, expected",
    [
        ("/api/v1/equity/price/historical", "equity.price.historical"),
        ("/api/v1/commodity/price/spot", "commodity.price.spot"),
        ("/api/v1/economy", "economy"),
        ("/economy/cpi", "economy.cpi"),
    ],
)
def test_url_to_command(url, expected):
    assert url_to_command(url) == expected


def test_url_to_command_custom_prefix():
    assert url_to_command("/api/v2/foo/bar", api_prefix="/api/v2") == "foo.bar"


def test_build_command_index_keyed_by_dotted_path():
    spec = {
        "paths": {
            "/api/v1/equity/price/historical": {
                "get": {
                    "operationId": "eqh",
                    "parameters": [
                        {
                            "name": "symbol",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                }
            }
        }
    }
    index = build_command_index(spec)
    assert "equity.price.historical" in index
    assert isinstance(index["equity.price.historical"], argparse.ArgumentParser)


def test_build_command_index_skips_paths_without_known_method():
    spec = {"paths": {"/api/v1/x": {"delete": {"operationId": "x"}}}}
    assert build_command_index(spec) == {}


def test_build_command_index_uses_post_when_get_missing():
    spec = {
        "paths": {
            "/api/v1/run": {
                "post": {
                    "operationId": "run",
                    "parameters": [{"name": "in", "schema": {"type": "string"}}],
                }
            }
        }
    }
    assert "run" in build_command_index(spec)


def test_build_router_map_classifies_menus_and_commands():
    spec = {
        "paths": {
            "/api/v1/commodity/price/spot": {"get": {"operationId": "x"}},
            "/api/v1/equity/quote": {"get": {"operationId": "y"}},
        }
    }
    out = build_router_map(spec)
    assert out["commodity"] == "menu"
    assert out["commodity.price"] == "menu"
    assert out["commodity.price.spot"] == "command"
    assert out["equity"] == "menu"
    assert out["equity.quote"] == "command"


def test_build_router_map_ignores_paths_without_methods():
    spec = {"paths": {"/api/v1/foo": {}}}
    assert build_router_map(spec) == {}


def test_build_reference_paths_carry_descriptions():
    spec = {
        "paths": {
            "/api/v1/equity/quote": {
                "get": {
                    "operationId": "x",
                    "summary": "Quote",
                    "description": "Get a quote.",
                    "tags": ["equity"],
                }
            }
        },
        "tags": [{"name": "equity", "description": "Equity data."}],
    }
    ref = build_reference(spec)
    assert ref["paths"]["/equity/quote"]["description"] == "Get a quote."
    assert ref["routers"]["/equity/"]["description"] == "Equity data."


def test_build_reference_falls_back_to_summary_when_no_description():
    spec = {
        "paths": {
            "/api/v1/equity/quote": {
                "get": {"operationId": "x", "summary": "Just a quote.", "tags": []}
            }
        }
    }
    assert (
        build_reference(spec)["paths"]["/equity/quote"]["description"]
        == "Just a quote."
    )


def test_build_reference_router_default_description_when_tag_missing():
    spec = {
        "paths": {
            "/api/v1/x/y": {"get": {"operationId": "z", "tags": ["unknown_tag"]}},
        },
        "tags": [],
    }
    ref = build_reference(spec)
    assert ref["routers"]["/x/"]["description"] == ""


def test_resolve_schema_anyof_all_null_returns_str():
    """Edge: ``anyOf: [{type:null}]`` (no non-null types) collapses to str."""
    py_type, choices, is_list = _resolve_schema({"anyOf": [{"type": "null"}]})
    assert py_type is str and choices == [] and is_list is False


def test_resolve_schema_array_in_array_recurses():
    py_type, choices, is_list = _resolve_schema(
        {"type": "array", "items": {"type": "integer", "enum": [1, 2]}}
    )
    assert py_type is int and is_list is True and choices == [1, 2]


def test_resolve_schema_union_skips_duplicate_enum_values():
    """Duplicate enum values across union members are de-duped."""
    _, choices, _ = _resolve_schema(
        {
            "anyOf": [
                {"type": "string", "enum": ["a", "b"]},
                {"type": "string", "enum": ["a", "c"]},
            ]
        }
    )
    assert choices == ["a", "b", "c"]


def test_strip_placeholders_unbalanced_brace_breaks():
    """``{foo without closing brace`` is left untouched (the while loop breaks)."""
    from openbb_cli.dispatchers.openapi_schema import _strip_placeholders

    assert _strip_placeholders("{foo") == "{foo"


def test_strip_placeholders_close_before_open_breaks_loop():
    """``}foo{`` enters the loop (both braces present) but ``find('}', i)`` returns -1."""
    from openbb_cli.dispatchers.openapi_schema import _strip_placeholders

    assert _strip_placeholders("}foo{") == "}foo{"


def test_url_to_command_with_no_prefix_match_uses_full_path():
    """Custom prefix that isn't actually present in the URL → no stripping."""
    from openbb_cli.dispatchers.openapi_schema import url_to_command

    assert url_to_command("/x/y/z", api_prefix="/api/v1") == "x.y.z"


def test_detect_api_prefix_empty_spec_falls_back_to_default():
    from openbb_cli.dispatchers.openapi_schema import detect_api_prefix

    assert detect_api_prefix({}) == "/api/v1"
    assert detect_api_prefix({"paths": {}}) == "/api/v1"


def test_detect_api_prefix_two_paths_diverging_immediately():
    from openbb_cli.dispatchers.openapi_schema import detect_api_prefix

    spec = {"paths": {"/foo/x": {}, "/bar/y": {}}}
    assert detect_api_prefix(spec) == ""


def test_detect_api_prefix_finds_common_segments():
    from openbb_cli.dispatchers.openapi_schema import detect_api_prefix

    spec = {"paths": {"/api/foo/x": {}, "/api/bar/y": {}}}
    assert detect_api_prefix(spec) == "/api"


def test_parse_spec_text_recognizes_json_object():
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    out = _parse_spec_text('{"openapi": "3.0.0"}')
    assert out == {"openapi": "3.0.0"}


def test_parse_spec_text_recognizes_yaml_via_content_type():
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    out = _parse_spec_text("openapi: 3.0.0\npaths: {}", content_type="application/yaml")
    assert out == {"openapi": "3.0.0", "paths": {}}


def test_parse_spec_text_recognizes_yaml_via_heuristic():
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    out = _parse_spec_text("openapi: 3.0.0")
    assert out == {"openapi": "3.0.0"}


def test_parse_spec_text_swagger_heuristic():
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    out = _parse_spec_text("swagger: '2.0'\ninfo:\n  title: x")
    assert out["swagger"] == "2.0"


def test_parse_spec_text_falls_back_to_yaml_when_json_fails():
    """Last-resort branch: not JSON-shape, no content-type hint, no heuristic match."""
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    out = _parse_spec_text("foo: bar")
    assert out == {"foo": "bar"}


def test_parse_spec_text_recognizes_json_array_form():
    """A bare JSON array also goes through the JSON branch."""
    from openbb_cli.dispatchers.openapi_schema import _parse_spec_text

    assert _parse_spec_text("[1, 2]") == [1, 2]


def test_fetch_openapi_default_path_and_user_agent(monkeypatch):
    """Default appends ``/openapi.json`` and merges the default User-Agent."""
    from openbb_cli.dispatchers import openapi_schema

    captured: dict[str, object] = {}

    class _Resp:
        text = '{"openapi": "3.0.0"}'
        headers = {"content-type": "application/json"}

        def raise_for_status(self):
            return None

    def fake_get(url, *, timeout, follow_redirects, headers):
        captured["url"] = url
        captured["headers"] = headers
        return _Resp()

    monkeypatch.setattr(openapi_schema.httpx, "get", fake_get)
    out = openapi_schema.fetch_openapi("http://h")
    assert out == {"openapi": "3.0.0"}
    assert captured["url"] == "http://h/openapi.json"
    assert captured["headers"]["User-Agent"] == "openbb-cli/1.0"


def test_fetch_openapi_custom_path_appended(monkeypatch):
    from openbb_cli.dispatchers import openapi_schema

    captured: dict[str, object] = {}

    class _Resp:
        text = "{}"
        headers = {"content-type": "application/json"}

        def raise_for_status(self):
            return None

    def fake_get(url, *, timeout, follow_redirects, headers):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr(openapi_schema.httpx, "get", fake_get)
    openapi_schema.fetch_openapi("http://h/", path="/static/spec.yml")
    assert captured["url"] == "http://h/static/spec.yml"


def test_fetch_openapi_full_url_path_passes_through(monkeypatch):
    """If ``path`` is already absolute, ``base_url`` is ignored."""
    from openbb_cli.dispatchers import openapi_schema

    captured: dict[str, object] = {}

    class _Resp:
        text = "{}"
        headers = {"content-type": "application/json"}

        def raise_for_status(self):
            return None

    def fake_get(url, *, timeout, follow_redirects, headers):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr(openapi_schema.httpx, "get", fake_get)
    openapi_schema.fetch_openapi("http://h", path="https://elsewhere/spec.json")
    assert captured["url"] == "https://elsewhere/spec.json"


def test_resolve_schema_union_with_array_member_marks_is_list():
    """``anyOf: [array, scalar]`` propagates ``is_list=True``."""
    _, _, is_list = _resolve_schema(
        {
            "anyOf": [
                {"type": "array", "items": {"type": "string"}},
                {"type": "string"},
            ]
        }
    )
    assert is_list is True


def test_strip_placeholders_collapses_balanced_braces():
    """``latest.{format}`` → ``latest``."""
    from openbb_cli.dispatchers.openapi_schema import _strip_placeholders

    assert _strip_placeholders("latest.{format}") == "latest"


def test_detect_api_prefix_breaks_when_common_emptied():
    """Three paths whose very first segment differs across them hit the
    ``not common: break`` arm."""
    from openbb_cli.dispatchers.openapi_schema import detect_api_prefix

    spec = {"paths": {"/x/y": {}, "a/y/z": {}, "/x/q": {}}}
    detect_api_prefix(spec)


def test_build_router_map_skips_empty_command():
    """A URL that strips to an empty dotted command is skipped."""
    from openbb_cli.dispatchers.openapi_schema import build_router_map

    spec = {"paths": {"/api/v1": {"get": {"operationId": "x"}}}}
    assert build_router_map(spec) == {}


def test_build_reference_skips_paths_without_known_methods():
    """A path with only DELETE/PUT is not surfaced."""
    from openbb_cli.dispatchers.openapi_schema import build_reference

    spec = {"paths": {"/api/v1/x": {"delete": {"operationId": "x"}}}}
    out = build_reference(spec)
    assert out["paths"] == {}
    assert out["routers"] == {}


def test_build_reference_dedupes_menu_paths():
    """Two paths under the same menu → router entry recorded once."""
    from openbb_cli.dispatchers.openapi_schema import build_reference

    spec = {
        "paths": {
            "/api/v1/equity/quote": {"get": {"operationId": "q", "tags": ["e"]}},
            "/api/v1/equity/profile": {"get": {"operationId": "p", "tags": ["e"]}},
        },
        "tags": [{"name": "e", "description": "Equity."}],
    }
    out = build_reference(spec)
    assert len(out["routers"]) == 1
    assert out["routers"]["/equity/"]["description"] == "Equity."


def test_fetch_openapi_merges_caller_headers(monkeypatch):
    """Caller-supplied headers are merged with the default User-Agent."""
    from openbb_cli.dispatchers import openapi_schema

    captured: dict[str, object] = {}

    class _Resp:
        text = "{}"
        headers = {"content-type": "application/json"}

        def raise_for_status(self):
            return None

    def fake_get(url, *, timeout, follow_redirects, headers):
        captured["headers"] = headers
        return _Resp()

    monkeypatch.setattr(openapi_schema.httpx, "get", fake_get)
    openapi_schema.fetch_openapi("http://h", headers={"Authorization": "Bearer x"})
    assert captured["headers"]["User-Agent"] == "openbb-cli/1.0"
    assert captured["headers"]["Authorization"] == "Bearer x"
