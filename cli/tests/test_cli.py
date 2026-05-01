"""Tests for openbb_cli.cli — entry-point routing and end-to-end dispatch.

End-to-end tests drive ``cli.main`` against the real generated ``obb``
namespace via ``run_in_obb``; routing-only tests still inject test doubles
at the dispatcher boundary because routing is independent of obb.
"""

import json
from unittest.mock import MagicMock, patch

from openbb_cli import cli

# ── _build_dispatcher ───────────────────────────────────────────────


def test_build_dispatcher_local_when_no_server():
    with patch("openbb_cli.dispatchers.LocalDispatcher") as ld:
        ld.return_value = MagicMock(name="local")
        d = cli._build_dispatcher(None)
    assert d is ld.return_value


def test_build_dispatcher_http_when_server():
    """``--server URL`` builds via ``http_dispatcher_from_server`` (which fetches openapi)."""
    with patch("openbb_cli.dispatchers.http.http_dispatcher_from_server") as factory:
        factory.return_value = MagicMock(name="http")
        d = cli._build_dispatcher("http://api.local")
    factory.assert_called_once_with("http://api.local", headers=None)
    assert d is factory.return_value


# ── main routing ────────────────────────────────────────────────────


def test_main_dispatches_one_shot_when_command_given():
    with (
        patch("openbb_cli.cli._build_dispatcher") as bd,
        patch("openbb_cli.dispatchers.runtime.run_argv", return_value=0) as ra,
        patch("openbb_cli.dispatchers.runtime.run_batch") as rb,
    ):
        bd.return_value = MagicMock(name="dispatcher")
        rc = cli.main(["economy.gdp", "--provider=oecd"])
    assert rc == 0
    ra.assert_called_once()
    rb.assert_not_called()


def test_main_routes_to_batch_when_flag_set():
    with (
        patch("openbb_cli.cli._build_dispatcher") as bd,
        patch("openbb_cli.dispatchers.runtime.run_batch", return_value=0) as rb,
        patch("openbb_cli.dispatchers.runtime.run_argv") as ra,
    ):
        bd.return_value = MagicMock(name="dispatcher")
        rc = cli.main(["--batch"])
    assert rc == 0
    rb.assert_called_once()
    ra.assert_not_called()


def test_main_launches_repl_when_interactive():
    with patch("openbb_cli.cli._launch_repl", return_value=0) as repl:
        rc = cli.main(["-i"])
    assert rc == 0
    repl.assert_called_once()


def test_main_help_exit_when_no_command(capsys):
    """Default mode with no positional command exits with code 2."""
    with patch("openbb_cli.cli._build_dispatcher") as bd:
        bd.return_value = MagicMock(name="dispatcher")
        rc = cli.main([])
    assert rc == 2
    err = capsys.readouterr().err
    assert "usage" in err.lower()


def test_main_passes_dev_debug_to_repl():
    with patch("openbb_cli.cli._launch_repl", return_value=0) as repl:
        cli.main(["-i", "--dev", "--debug"])
    # ``_launch_repl(dev, debug, spec_path, server_url, headers)`` — all
    # backend-source / headers args default to None for a bare ``-i``.
    repl.assert_called_once_with(True, True, None, None, None)


def test_launch_repl_imports_and_calls_legacy_launch(capsys):
    """_launch_repl prints Loading and goes through the legacy in-process path
    when no spec / server is provided."""
    with (
        patch("openbb_cli.config.setup.bootstrap") as bs,
        patch("openbb_cli.controllers.cli_controller.launch") as launch_,
    ):
        rc = cli._launch_repl(False, False)
    assert rc == 0
    assert "Loading..." in capsys.readouterr().out
    bs.assert_called_once()
    # Legacy path delegates to ``launch(dev, debug)`` (no ``backend`` kwarg) so
    # ``parse_args_and_run`` runs and re-parses sys.argv inside the controller.
    launch_.assert_called_once_with(False, False)


# ── headers --------------------------------------------------------


def test_parse_header_kv_equals_form():
    from openbb_cli.cli import _parse_header_kv

    assert _parse_header_kv("X-API-Key=abc123") == ("X-API-Key", "abc123")


def test_parse_header_kv_colon_form():
    from openbb_cli.cli import _parse_header_kv

    assert _parse_header_kv("Authorization: Bearer xyz") == (
        "Authorization",
        "Bearer xyz",
    )


def test_parse_header_kv_equals_wins_when_first():
    """``KEY=VALUE: with embedded colon`` keeps the colon in the value."""
    from openbb_cli.cli import _parse_header_kv

    assert _parse_header_kv("X-Trace=foo:bar:baz") == ("X-Trace", "foo:bar:baz")


def test_parse_header_kv_colon_wins_when_first():
    """``KEY: VALUE with embedded =`` keeps the equals in the value."""
    from openbb_cli.cli import _parse_header_kv

    assert _parse_header_kv("Authorization: Bearer x=y=z") == (
        "Authorization",
        "Bearer x=y=z",
    )


def test_parse_header_kv_rejects_no_separator():
    import pytest

    from openbb_cli.cli import _parse_header_kv

    with pytest.raises(ValueError, match="must be"):
        _parse_header_kv("nokeyvalue")


def test_resolve_headers_merges_file_and_cli(tmp_path):
    from openbb_cli.cli import _resolve_headers

    header_file = tmp_path / "h.json"
    header_file.write_text('{"X-API-Key": "from-file", "X-Tenant": "shared"}')
    out = _resolve_headers(["X-API-Key=from-cli", "X-Trace=tid"], str(header_file))
    # CLI ``--header`` overrides file-supplied entries with the same key.
    assert out == {
        "X-API-Key": "from-cli",
        "X-Tenant": "shared",
        "X-Trace": "tid",
    }


def test_resolve_headers_returns_none_when_empty():
    from openbb_cli.cli import _resolve_headers

    assert _resolve_headers([], None) is None
    assert _resolve_headers(None, None) is None


def test_resolve_headers_rejects_non_object_file(tmp_path, capsys):
    from openbb_cli.cli import _resolve_headers

    bad = tmp_path / "h.json"
    bad.write_text("[1, 2]")  # array, not object
    out = _resolve_headers([], str(bad))
    assert out is None
    err = capsys.readouterr().err
    assert "must contain a JSON object" in err


def test_resolve_headers_rejects_invalid_json(tmp_path, capsys):
    from openbb_cli.cli import _resolve_headers

    bad = tmp_path / "h.json"
    bad.write_text("not-json")
    assert _resolve_headers([], str(bad)) is None
    assert "Expecting" in capsys.readouterr().err  # json error


def test_main_threads_headers_to_one_shot(tmp_path):
    """``-H KEY=VALUE`` flows from main into ``_run_spec_one_shot``."""
    import json

    from openbb_cli.dispatchers.spec import SPEC_VERSION

    spec_file = tmp_path / "h.spec"
    spec_file.write_text(
        json.dumps(
            {
                "version": SPEC_VERSION,
                "base_url": "http://h",
                "api_prefix": "/api",
                "commands": {},
                "routers": {},
                "reference": {"paths": {}, "routers": {}},
            }
        )
    )
    with patch("openbb_cli.cli._run_spec_one_shot", return_value=0) as one_shot:
        cli.main(
            [
                "--spec",
                str(spec_file),
                "-H",
                "Authorization: Bearer xyz",
                "fxs.list.counterparties",
                "--format",
                "json",
            ]
        )
    one_shot.assert_called_once()
    args = one_shot.call_args.args
    assert args[0] == str(spec_file)
    assert args[1] == [
        "fxs.list.counterparties",
        "--format",
        "json",
    ]
    assert args[2] == {"Authorization": "Bearer xyz"}


def test_launch_repl_with_spec_uses_spec_backend(tmp_path):
    """``--spec PATH`` builds a ``SpecBackend`` and threads it into ``run_cli``."""
    import json

    from openbb_cli.dispatchers.spec import SPEC_VERSION

    spec_file = tmp_path / "fake.spec"
    spec_file.write_text(
        json.dumps(
            {
                "version": SPEC_VERSION,
                "base_url": "http://h",
                "api_prefix": "/api",
                "commands": {},
                "routers": {},
                "reference": {"paths": {}, "routers": {}},
            }
        )
    )
    with (
        patch("openbb_cli.config.setup.bootstrap"),
        patch("openbb_cli.controllers.cli_controller.run_cli") as run_cli_,
    ):
        cli._launch_repl(False, False, str(spec_file), None)
    backend_arg = run_cli_.call_args[1]["backend"]
    assert backend_arg is not None
    # SpecBackend reads ``routers`` from the spec doc.
    assert backend_arg.routers == {}


# ── end-to-end through real obb ────────────────────────────────────


def test_e2e_dispatches_real_command_to_stdout(run_in_obb):
    """``openbb cli_test.echo --value=hello`` resolves through real obb."""
    result = run_in_obb("""
        import io, sys
        from openbb_cli.cli import main

        captured = io.StringIO()
        sys.stdout = captured
        try:
            rc = main(["cli_test.echo", "--value=hello"])
        finally:
            sys.stdout = sys.__stdout__
        RESULT = {"rc": rc, "out": captured.getvalue()}
    """)
    assert result["rc"] == 0
    payload = json.loads(result["out"].strip())
    assert payload["ok"] is True
    assert payload["result"]["results"] == {"echo": "hello"}


def test_e2e_dispatch_failure_returns_nonzero(run_in_obb):
    """``openbb cli_test.bomb`` returns rc=1 with structured error in stdout."""
    result = run_in_obb("""
        import io, sys
        from openbb_cli.cli import main

        captured = io.StringIO()
        sys.stdout = captured
        try:
            rc = main(["cli_test.bomb"])
        finally:
            sys.stdout = sys.__stdout__
        RESULT = {"rc": rc, "out": captured.getvalue()}
    """)
    assert result["rc"] == 1
    payload = json.loads(result["out"].strip())
    assert payload["ok"] is False
    # openbb-core wraps user-raised exceptions in OpenBBError at the runner boundary.
    assert payload["error"]["type"] == "OpenBBError"


def test_e2e_batch_protocol_round_trip(run_in_obb):
    """NDJSON in → NDJSON out across multiple commands."""
    result = run_in_obb("""
        import io, json, sys
        from openbb_cli.dispatchers import LocalDispatcher
        from openbb_cli.dispatchers.runtime import run_batch

        reader = io.StringIO(
            json.dumps({"id": "a", "command": "cli_test.echo", "params": {"value": "x"}})
            + "\\n"
            + json.dumps({"id": "b", "command": "cli_test.bomb"})
            + "\\n"
        )
        writer = io.StringIO()
        rc = run_batch(LocalDispatcher(), reader=reader, writer=writer, concurrency=2)
        RESULT = {"rc": rc, "lines": [
            json.loads(line) for line in writer.getvalue().splitlines() if line
        ]}
    """)
    assert result["rc"] == 1  # at least one failure (bomb)
    by_id = {line["id"]: line for line in result["lines"]}
    assert by_id["a"]["ok"] is True
    assert by_id["a"]["result"]["results"] == {"echo": "x"}
    assert by_id["b"]["ok"] is False
    assert by_id["b"]["error"]["type"] == "OpenBBError"
