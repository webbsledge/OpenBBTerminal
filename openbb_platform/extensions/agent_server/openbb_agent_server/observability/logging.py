"""Trace-aware structured logging with PII redaction + ``TRACE`` level."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from openbb_agent_server.runtime import context as run_context
from openbb_agent_server.runtime.identity import redact_email_in_text

TRACE = 5

if logging.getLevelName(TRACE) == f"Level {TRACE}":
    logging.addLevelName(TRACE, "TRACE")

# Channel the CLI ``--log-level`` through to ``create_app`` (and the
# ``--reload`` worker subprocess) so the chosen level survives.
LOG_LEVEL_ENV = "OPENBB_AGENT_LOG_LEVEL"

# Third-party loggers that emit one DEBUG line per DB cursor op / HTTP
# byte / event-loop tick — pure noise that buries the agent's own logs
# at ``--log-level debug`` / ``trace``. Capped at WARNING.
_NOISY_THIRD_PARTY_LOGGERS: tuple[str, ...] = (
    "aiosqlite",
    "asyncio",
    "sqlalchemy.engine",
    "sqlalchemy.pool",
    "httpcore",
    "httpx",
    "urllib3",
    "watchfiles",
    "python_multipart",
    "multipart",
)


def resolve_level(level: int | str | None) -> int:
    """Resolve a level int / name (including ``TRACE``) to a numeric level.

    ``None`` consults the ``OPENBB_AGENT_LOG_LEVEL`` env var, then falls
    back to ``INFO``. An unknown name also falls back to ``INFO`` rather
    than raising — a bad ``--log-level`` should not crash the server.
    """
    if level is None:
        level = os.environ.get(LOG_LEVEL_ENV) or logging.INFO
    if isinstance(level, int):
        return level
    resolved = logging.getLevelName(str(level).strip().upper())
    return resolved if isinstance(resolved, int) else logging.INFO


def trace(logger: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
    """Emit a ``TRACE``-level record on ``logger``."""
    if logger.isEnabledFor(TRACE):
        logger.log(TRACE, message, *args, **kwargs)


_BEARER_RE = re.compile(r"(?i)Bearer\s+[A-Za-z0-9._\-+/=]+")
_AUTH_HEADER_RE = re.compile(r"(?i)(authorization)\s*[:=]\s*[A-Za-z0-9._\-+/= ]+")
_API_KEY_RE = re.compile(
    r"(?i)\b(api[_-]?key|x-api-key|sk-[A-Za-z0-9]+|nvapi-[A-Za-z0-9]+|"
    r"grok-[A-Za-z0-9]+|gsk_[A-Za-z0-9]+|tvly-[A-Za-z0-9]+)[A-Za-z0-9._\-]*"
)


def redact_pii(text: str) -> str:
    """Strip emails / bearer tokens / API-key-shaped strings from ``text``."""
    if not isinstance(text, str) or not text:
        return text
    out = redact_email_in_text(text)
    out = _BEARER_RE.sub("Bearer <redacted>", out)
    out = _AUTH_HEADER_RE.sub(r"\1: <redacted>", out)
    out = _API_KEY_RE.sub("<redacted-key>", out)
    return out


class TraceContextFilter(logging.Filter):
    """Inject trace IDs from the contextvar into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            ctx = run_context.current()
        except LookupError:
            ctx = None
        if ctx is not None:
            record.trace_id = ctx.trace_id
            record.run_id = ctx.run_id
            record.conversation_id = ctx.conversation_id
            record.user_id = ctx.principal.user_id
            record.trace = {
                "trace_id": ctx.trace_id,
                "run_id": ctx.run_id,
                "conversation_id": ctx.conversation_id,
                "user_id": ctx.principal.user_id,
            }
        else:
            record.trace_id = ""
            record.run_id = ""
            record.conversation_id = ""
            record.user_id = ""
            record.trace = {}
        return True


class PIIRedactionFilter(logging.Filter):
    """Redact emails / bearer tokens / API keys from every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.msg
        if isinstance(msg, str):
            record.msg = redact_pii(msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = _redact_arg(record.args)
            else:
                record.args = tuple(_redact_arg(a) for a in record.args)
        return True


def _redact_arg(arg: Any) -> Any:
    if isinstance(arg, str):
        return redact_pii(arg)
    if isinstance(arg, dict):
        return {k: _redact_arg(v) for k, v in arg.items()}
    if isinstance(arg, (list, tuple)):
        seq = [_redact_arg(v) for v in arg]
        return type(arg)(seq) if isinstance(arg, tuple) else seq
    return arg


class JsonTraceFormatter(logging.Formatter):
    """One-line JSON formatter that surfaces the trace IDs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": redact_pii(record.getMessage()),
            "trace": getattr(record, "trace", {}),
        }
        if record.exc_info:
            payload["exc_info"] = redact_pii(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


def _quiet_noisy_loggers(root_level: int) -> None:
    """Cap chatty third-party loggers so ``--log-level debug``/``trace``
    surfaces the agent's own logs instead of drowning in DB / HTTP / async
    plumbing. Each is pinned to ``WARNING`` (or the root level if that is
    already coarser, so ``--log-level error`` still wins).
    """
    quiet_level = max(root_level, logging.WARNING)
    for name in _NOISY_THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(quiet_level)


def install_trace_logging(level: int | str | None = None) -> None:
    """Attach trace + redaction filters and a JSON formatter to root.

    ``level`` may be an int, a level name (including ``"TRACE"``), or
    ``None`` — in which case ``OPENBB_AGENT_LOG_LEVEL`` is consulted,
    falling back to ``INFO``.
    """
    root = logging.getLogger()
    root_level = resolve_level(level)
    root.setLevel(root_level)
    _quiet_noisy_loggers(root_level)
    if any(isinstance(f, TraceContextFilter) for f in root.filters):
        return
    root.addFilter(TraceContextFilter())
    root.addFilter(PIIRedactionFilter())
    handler = logging.StreamHandler()
    handler.setFormatter(JsonTraceFormatter())
    handler.addFilter(TraceContextFilter())
    handler.addFilter(PIIRedactionFilter())
    root.addHandler(handler)
