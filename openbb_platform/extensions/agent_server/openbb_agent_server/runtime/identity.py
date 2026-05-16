"""Stable hashing helper that maps emails / external IDs to ``user_id`` strings."""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re

logger = logging.getLogger("openbb_agent_server.runtime.identity")

_PEPPER_ENV = "OPENBB_AGENT_USER_ID_PEPPER"
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,255}\.[A-Za-z]{2,}\b")
_HASH_PREFIX = "u-"
_HASH_BYTES = 12


def _pepper() -> bytes:
    return os.environ.get(_PEPPER_ENV, "").encode("utf-8")


def warn_if_pepper_unset() -> None:
    """Log a startup WARNING when the user-id pepper is unset.

    Called once from ``create_app`` so the missing-pepper notice is a
    deterministic boot-time config check — not a lazy warning that
    interleaves into whichever request first resolves a user id (auth
    runs before the request's trace context exists, so the lazy
    variant logged with an empty ``trace`` and looked unrelated to the
    request it appeared inside).
    """
    if not os.environ.get(_PEPPER_ENV):
        logger.warning(
            "%s is not set — falling back to an empty pepper. Set a stable "
            "secret value before going to production; rotating it later "
            "orphans every user's persisted data.",
            _PEPPER_ENV,
        )


def hash_user_id(value: str) -> str:
    """Return a stable opaque ``user_id`` for an email or external identifier."""
    normalized = (value or "").strip().lower()
    if not normalized:
        raise ValueError("hash_user_id: value must be non-empty")
    digest = hmac.new(_pepper(), normalized.encode("utf-8"), hashlib.sha256).digest()
    return f"{_HASH_PREFIX}{digest.hex()[: 2 * _HASH_BYTES]}"


def is_email(value: str) -> bool:
    """Return True iff ``value`` looks like an RFC-5321-ish email address."""
    if not value:
        return False
    return _EMAIL_RE.fullmatch(value.strip()) is not None


def redact_email_in_text(text: str) -> str:
    """Replace every email address in ``text`` with its ``user_id`` hash."""
    if not text:
        return text
    return _EMAIL_RE.sub(lambda m: hash_user_id(m.group(0)), text)


__all__ = [
    "hash_user_id",
    "is_email",
    "redact_email_in_text",
    "warn_if_pepper_unset",
]
