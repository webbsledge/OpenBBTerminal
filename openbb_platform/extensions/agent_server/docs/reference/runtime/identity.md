# `openbb_agent_server.runtime.identity`

Stable hashing helpers that map emails / external identifiers to opaque `user_id` strings. The hash is HMAC-SHA256 keyed on a pepper read from the `OPENBB_AGENT_USER_ID_PEPPER` environment variable; the first 24 hex characters (12 bytes) of the digest are prefixed with `u-`.

**Source:** [`openbb_agent_server/runtime/identity.py`](../../../openbb_agent_server/runtime/identity.py)

## Pepper configuration

`OPENBB_AGENT_USER_ID_PEPPER` is a free-form string used as the HMAC key. If unset, the helper logs one WARNING line on first use and falls back to an empty pepper. Set a stable secret before going to production — **rotating the pepper orphans every user's persisted data** (every existing row keys on the old hash).

## Functions

### `hash_user_id(value: str) -> str`

Return a stable opaque `user_id` of the form `"u-<24 hex chars>"` for an email or external identifier. The input is `.strip().lower()`-normalised before hashing, so case / whitespace variants map to the same hash. Raises `ValueError` if `value` is empty after normalisation.

### `is_email(value: str) -> bool`

True iff `value` matches a loose RFC-5321-ish email regex (`{local}@{domain}.{tld}` where local ≤ 64 chars, domain ≤ 255 chars, TLD ≥ 2 letters). Used by auth backends and log-redaction passes to decide whether to apply `hash_user_id`.

### `redact_email_in_text(text: str) -> str`

Replace every email-looking substring inside `text` with its `hash_user_id` value. Used by `observability/logging.py` to keep emails out of log lines.
