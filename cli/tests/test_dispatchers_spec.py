"""Tests for the precomputed .spec format and command lookup."""

from __future__ import annotations

import json

import pytest

from openbb_cli.dispatchers.spec import (
    SPEC_VERSION,
    SpecCommandError,
    _normalize_parameter,
    build_command_spec,
    build_spec_document,
    load_spec,
    parse_command_argv,
    parser_from_command_spec,
    write_spec,
)


def test_normalize_parameter_skips_chart():
    assert (
        _normalize_parameter({"name": "chart", "schema": {"type": "boolean"}}) is None
    )


def test_normalize_parameter_required_string():
    out = _normalize_parameter(
        {"name": "symbol", "required": True, "schema": {"type": "string"}}
    )
    assert out == {
        "name": "symbol",
        "in": "query",
        "type": "string",
        "is_list": False,
        "required": True,
        "default": None,
        "choices": [],
        "help": None,
    }


def test_normalize_parameter_path_param_is_required_even_without_required_flag():
    """OpenAPI lets ``in: path`` parameters omit ``required: true`` — normalize."""
    out = _normalize_parameter(
        {"name": "operation", "in": "path", "schema": {"type": "string"}}
    )
    assert out["in"] == "path"
    assert out["required"] is True


def test_normalize_parameter_records_query_location_by_default():
    out = _normalize_parameter({"name": "x", "schema": {"type": "string"}})
    assert out["in"] == "query"


def test_normalize_parameter_with_default_not_required():
    """A schema-level default flips ``required`` off — same as argparse semantics."""
    out = _normalize_parameter(
        {
            "name": "limit",
            "required": True,
            "schema": {"type": "integer", "default": 10},
        }
    )
    assert out["required"] is False
    assert out["default"] == 10


def test_normalize_parameter_unions_provider_choices_with_enum():
    out = _normalize_parameter(
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
    assert set(out["choices"]) == {"1m", "5m", "1h", "1d"}


def test_normalize_parameter_array_marked_as_list():
    out = _normalize_parameter(
        {"name": "symbols", "schema": {"type": "array", "items": {"type": "string"}}}
    )
    assert out["is_list"] is True
    assert out["type"] == "string"


def test_normalize_parameter_help_falls_back_to_schema_description():
    out = _normalize_parameter(
        {"name": "x", "schema": {"type": "string", "description": "from schema"}}
    )
    assert out["help"] == "from schema"


def test_build_command_spec_keys_by_dotted_path():
    openapi = {
        "paths": {
            "/api/v1/equity/quote": {
                "get": {
                    "operationId": "eq",
                    "summary": "Quote",
                    "description": "Get quote.",
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
    out = build_command_spec(openapi)
    assert "equity.quote" in out
    entry = out["equity.quote"]
    assert entry["url_path"] == "/api/v1/equity/quote"
    assert entry["method"] == "get"
    assert entry["description"] == "Get quote."
    assert entry["parameters"][0]["name"] == "symbol"


def test_build_command_spec_uses_post_when_get_missing():
    openapi = {"paths": {"/api/v1/x": {"post": {"operationId": "x"}}}}
    assert build_command_spec(openapi)["x"]["method"] == "post"


def test_build_command_spec_skips_other_methods():
    openapi = {"paths": {"/api/v1/x": {"delete": {"operationId": "x"}}}}
    assert build_command_spec(openapi) == {}


def test_build_command_spec_drops_chart_parameter():
    openapi = {
        "paths": {
            "/api/v1/x": {
                "get": {
                    "operationId": "x",
                    "parameters": [
                        {"name": "chart", "schema": {"type": "boolean"}},
                        {"name": "symbol", "schema": {"type": "string"}},
                    ],
                }
            }
        }
    }
    params = build_command_spec(openapi)["x"]["parameters"]
    assert {p["name"] for p in params} == {"symbol"}


def test_build_spec_document_carries_metadata():
    openapi = {
        "paths": {
            "/api/v1/x": {"get": {"operationId": "x", "summary": "X", "tags": ["x"]}}
        },
        "tags": [{"name": "x", "description": "X stuff."}],
    }
    doc = build_spec_document(openapi, base_url="http://h:1/")
    assert doc["version"] == SPEC_VERSION
    assert doc["base_url"] == "http://h:1"
    assert doc["api_prefix"] == "/api/v1"
    assert "x" in doc["commands"]
    assert "x" in doc["routers"]
    assert "/x" in doc["reference"]["paths"]


def test_write_then_load_round_trip(tmp_path):
    openapi = {"paths": {"/api/v1/x": {"get": {"operationId": "x", "parameters": []}}}}
    doc = build_spec_document(openapi, base_url="http://h")
    target = tmp_path / "out.spec"
    write_spec(target, doc)
    loaded = load_spec(target)
    assert loaded["base_url"] == "http://h"
    assert "x" in loaded["commands"]


def test_load_spec_rejects_unknown_version(tmp_path):
    target = tmp_path / "bad.spec"
    target.write_text(json.dumps({"version": SPEC_VERSION + 99, "commands": {}}))
    with pytest.raises(ValueError, match="version"):
        load_spec(target)


def test_write_spec_is_compact(tmp_path):
    """Spec files should be compact (no whitespace) to keep them small on disk."""
    target = tmp_path / "x.spec"
    write_spec(target, {"version": SPEC_VERSION, "commands": {}})
    text = target.read_text()
    assert "\n" not in text
    assert ", " not in text
    assert ": " not in text


def test_parser_from_command_spec_required_arg_enforced():
    cmd_spec = {
        "url_path": "/api/v1/x",
        "method": "get",
        "description": "x",
        "parameters": [
            {
                "name": "symbol",
                "type": "string",
                "is_list": False,
                "required": True,
                "default": None,
                "choices": [],
                "help": None,
            }
        ],
    }
    parser = parser_from_command_spec(cmd_spec)
    with pytest.raises(SystemExit):
        parser.parse_args([])
    ns = parser.parse_args(["--symbol", "AAPL"])
    assert ns.symbol == "AAPL"


def test_parser_from_command_spec_choices_list_default():
    cmd_spec = {
        "url_path": "/api/v1/x",
        "method": "get",
        "description": "",
        "parameters": [
            {
                "name": "interval",
                "type": "string",
                "is_list": False,
                "required": False,
                "default": "1d",
                "choices": ["1m", "5m", "1d"],
                "help": None,
            },
            {
                "name": "symbols",
                "type": "string",
                "is_list": True,
                "required": False,
                "default": None,
                "choices": [],
                "help": None,
            },
        ],
    }
    parser = parser_from_command_spec(cmd_spec)
    ns = parser.parse_args(["--symbols", "A", "B", "C"])
    assert ns.symbols == ["A", "B", "C"]
    assert ns.interval == "1d"
    with pytest.raises(SystemExit):
        parser.parse_args(["--interval", "bogus"])


def test_parser_from_command_spec_boolean_flag():
    cmd_spec = {
        "url_path": "/api/v1/x",
        "method": "get",
        "description": "",
        "parameters": [
            {
                "name": "verbose",
                "type": "boolean",
                "is_list": False,
                "required": False,
                "default": False,
                "choices": [],
                "help": None,
            }
        ],
    }
    parser = parser_from_command_spec(cmd_spec)
    ns = parser.parse_args(["--verbose"])
    assert ns.verbose is True


def test_parser_from_command_spec_skips_duplicate_parameter():
    """Duplicate names in the spec — first wins, second silently dropped."""
    cmd_spec = {
        "url_path": "/api/v1/x",
        "method": "get",
        "description": "",
        "parameters": [
            {
                "name": "x",
                "type": "string",
                "is_list": False,
                "required": False,
                "default": "first",
                "choices": [],
                "help": None,
            },
            {
                "name": "x",
                "type": "integer",
                "is_list": False,
                "required": False,
                "default": 99,
                "choices": [],
                "help": None,
            },
        ],
    }
    parser = parser_from_command_spec(cmd_spec)
    ns = parser.parse_args([])
    assert ns.x == "first"


def _spec_with(commands: dict) -> dict:
    return {
        "version": SPEC_VERSION,
        "base_url": "http://h",
        "api_prefix": "/api/v1",
        "commands": commands,
        "routers": {},
        "reference": {"paths": {}, "routers": {}},
    }


def test_parse_command_argv_returns_command_and_params():
    spec_doc = _spec_with(
        {
            "equity.quote": {
                "url_path": "/api/v1/equity/quote",
                "method": "get",
                "description": "",
                "parameters": [
                    {
                        "name": "symbol",
                        "type": "string",
                        "is_list": False,
                        "required": True,
                        "default": None,
                        "choices": [],
                        "help": None,
                    }
                ],
            }
        }
    )
    cmd, params = parse_command_argv(spec_doc, ["equity.quote", "--symbol", "AAPL"])
    assert cmd == "equity.quote"
    assert params == {"symbol": "AAPL"}


def test_parse_command_argv_empty_argv_raises():
    with pytest.raises(SpecCommandError, match="missing command"):
        parse_command_argv(_spec_with({}), [])


def test_parse_command_argv_unknown_command_raises():
    with pytest.raises(SpecCommandError, match="not in spec"):
        parse_command_argv(_spec_with({}), ["nope"])


def test_build_command_spec_skips_empty_command():
    """A URL that strips to an empty dotted command is skipped."""
    spec = {"paths": {"/api/v1": {"get": {"operationId": "x", "parameters": []}}}}
    assert build_command_spec(spec) == {}


def test_build_command_spec_disambiguates_collisions_with_numeric_suffix():
    """Two URLs that strip to the same dotted command get ``_2`` / ``_3`` suffixes."""
    spec = {
        "paths": {
            "/api/x/{a}": {"get": {"operationId": "first", "parameters": []}},
            "/api/x/{b}/{c}": {"get": {"operationId": "second", "parameters": []}},
            "/api/x/{d}": {"get": {"operationId": "third", "parameters": []}},
        }
    }
    out = build_command_spec(spec, api_prefix="/api")
    keys = sorted(out.keys())
    assert keys == ["x", "x_2", "x_3"]
    assert out["x"]["url_path"] == "/api/x/{a}"
    assert out["x_2"]["url_path"] == "/api/x/{b}/{c}"
    assert out["x_3"]["url_path"] == "/api/x/{d}"


def test_parse_command_argv_drops_none_params():
    """Optional flags left at their default ``None`` shouldn't go on the wire."""
    spec_doc = _spec_with(
        {
            "x": {
                "url_path": "/api/v1/x",
                "method": "get",
                "description": "",
                "parameters": [
                    {
                        "name": "a",
                        "type": "string",
                        "is_list": False,
                        "required": False,
                        "default": None,
                        "choices": [],
                        "help": None,
                    },
                    {
                        "name": "b",
                        "type": "string",
                        "is_list": False,
                        "required": False,
                        "default": "hi",
                        "choices": [],
                        "help": None,
                    },
                ],
            }
        }
    )
    _, params = parse_command_argv(spec_doc, ["x"])
    assert params == {"b": "hi"}
