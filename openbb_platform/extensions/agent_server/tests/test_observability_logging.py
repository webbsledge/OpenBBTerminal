"""Trace-aware logging tests."""

from __future__ import annotations

import json
import logging

import pytest

from openbb_agent_server.observability.logging import (
    TRACE,
    JsonTraceFormatter,
    PIIRedactionFilter,
    TraceContextFilter,
    install_trace_logging,
    redact_pii,
    trace,
)
from openbb_agent_server.runtime import context as run_context
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.identity import hash_user_id
from openbb_agent_server.runtime.principal import UserPrincipal


def _record(message: str = "hi") -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_filter_attaches_blank_trace_outside_context() -> None:
    f = TraceContextFilter()
    rec = _record()
    assert f.filter(rec)
    assert rec.trace_id == ""
    assert rec.run_id == ""
    assert rec.user_id == ""


def test_filter_attaches_trace_inside_context() -> None:
    f = TraceContextFilter()
    rec = _record()
    ctx = RunContext(
        principal=UserPrincipal(user_id="alice"),
        trace_id="t1",
        run_id="r1",
        conversation_id="c1",
    )
    with run_context.bind(ctx):
        f.filter(rec)
    assert rec.trace_id == "t1"
    assert rec.run_id == "r1"
    assert rec.user_id == "alice"
    assert rec.trace == {
        "trace_id": "t1",
        "run_id": "r1",
        "conversation_id": "c1",
        "user_id": "alice",
    }


def test_json_formatter_emits_one_line_json() -> None:
    fmt = JsonTraceFormatter()
    rec = _record("hello")
    rec.trace = {"trace_id": "t"}
    out = fmt.format(rec)
    payload = json.loads(out)
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["trace"] == {"trace_id": "t"}


def test_json_formatter_renders_exc_info() -> None:
    fmt = JsonTraceFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = logging.LogRecord(
            "t", logging.ERROR, __file__, 1, "broke", (), sys.exc_info()
        )
        rec.trace = {}
    out = json.loads(fmt.format(rec))
    assert "ValueError: boom" in out["exc_info"]


def test_install_is_idempotent() -> None:
    root = logging.getLogger()
    before_filters = len(root.filters)
    install_trace_logging()
    after_first = len(root.filters)
    install_trace_logging()
    after_second = len(root.filters)
    assert after_first >= before_filters
    assert after_second == after_first


def test_trace_level_registered() -> None:
    assert TRACE < logging.DEBUG
    assert logging.getLevelName(TRACE) == "TRACE"


def test_trace_helper_is_callable() -> None:
    assert callable(trace)


def test_trace_emits_when_level_enabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("openbb_agent_server.test_trace_emit")
    logger.setLevel(TRACE)
    caplog.set_level(TRACE, logger=logger.name)
    trace(logger, "hello %s", "world")
    assert any("hello world" in r.getMessage() for r in caplog.records)


def test_trace_silent_when_disabled() -> None:
    logger = logging.getLogger("openbb_agent_server.test_trace_silent")
    logger.setLevel(logging.WARNING)
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger.addHandler(_Capture())
    try:
        trace(logger, "should not fire")
    finally:
        logger.handlers.clear()
    assert records == []


def test_redact_pii_replaces_email_with_hash() -> None:
    out = redact_pii("contact alice@example.com for details")
    assert "alice@example.com" not in out
    assert hash_user_id("alice@example.com") in out


def test_redact_pii_strips_bearer_tokens() -> None:
    out = redact_pii("Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.body.sig")
    assert "eyJhbGciOiJSUzI1NiJ9" not in out
    assert "<redacted>" in out


def test_redact_pii_strips_authorization_header_form() -> None:
    out = redact_pii("authorization: some-opaque-token")
    assert "some-opaque-token" not in out
    assert "<redacted>" in out


def test_redact_pii_strips_api_key_patterns() -> None:
    cases = [
        ("sk-abc123def456ghi", "sk-abc123def456ghi"),
        ("nvapi-zzzzzzzzzzzz", "nvapi-zzzzzzzzzzzz"),
        ("tvly-1234567890abcdef", "tvly-1234567890abcdef"),
        ("X-API-KEY=secret-value-xyz", "secret-value-xyz"),
    ]
    for raw, secret in cases:
        out = redact_pii(raw)
        assert "<redacted-key>" in out, raw
        assert secret not in out or out.startswith("<redacted-key>")


def test_redact_pii_passes_through_clean_strings() -> None:
    assert redact_pii("nothing sensitive here") == "nothing sensitive here"


def test_redact_pii_handles_non_string_inputs() -> None:
    assert redact_pii(None) is None  # type: ignore[arg-type]
    assert redact_pii("") == ""


def test_pii_redaction_filter_scrubs_message() -> None:
    f = PIIRedactionFilter()
    record = logging.LogRecord(
        "x", logging.INFO, "x", 0, "user alice@example.com signed in", None, None
    )
    f.filter(record)
    assert "alice@example.com" not in record.msg
    assert hash_user_id("alice@example.com") in record.msg


def test_pii_redaction_filter_scrubs_string_args() -> None:
    f = PIIRedactionFilter()
    record = logging.LogRecord(
        "x",
        logging.INFO,
        "x",
        0,
        "user %s logged in",
        ("alice@example.com",),
        None,
    )
    f.filter(record)
    assert record.args is not None
    assert "alice@example.com" not in record.args[0]


def test_pii_redaction_filter_scrubs_dict_args() -> None:
    """Python's ``LogRecord`` unwraps a single-dict args; the filter follows."""
    f = PIIRedactionFilter()
    record = logging.LogRecord(
        "x",
        logging.INFO,
        "x",
        0,
        "%(to)s",
        {"to": "bob@example.com", "ok": True},
        None,
    )
    f.filter(record)
    payload = record.args
    assert isinstance(payload, dict)
    assert "bob@example.com" not in payload["to"]
    assert payload["ok"] is True


def test_pii_redaction_filter_scrubs_list_args() -> None:
    f = PIIRedactionFilter()
    record = logging.LogRecord(
        "x", logging.INFO, "x", 0, "%s", ([{"e": "a@b.com"}],), None
    )
    f.filter(record)
    assert record.args is not None
    [items] = record.args
    assert "a@b.com" not in items[0]["e"]


def test_pii_redaction_filter_preserves_non_string_args() -> None:
    f = PIIRedactionFilter()
    record = logging.LogRecord("x", logging.INFO, "x", 0, "n=%d", (42,), None)
    f.filter(record)
    assert record.args == (42,)


def test_pii_redaction_filter_recurses_tuple_args() -> None:
    f = PIIRedactionFilter()
    record = logging.LogRecord(
        "x", logging.INFO, "x", 0, "%s", (("a@b.com", "ok"),), None
    )
    f.filter(record)
    assert record.args is not None
    [tpl] = record.args
    assert isinstance(tpl, tuple)
    assert "a@b.com" not in tpl[0]


def test_json_formatter_redacts_message_and_exception() -> None:
    try:
        raise ValueError("boom alice@example.com")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "t",
            logging.ERROR,
            __file__,
            1,
            "user alice@example.com",
            (),
            sys.exc_info(),
        )
        record.trace = {}
    blob = json.loads(JsonTraceFormatter().format(record))
    assert "alice@example.com" not in blob["message"]
    assert "alice@example.com" not in blob["exc_info"]
