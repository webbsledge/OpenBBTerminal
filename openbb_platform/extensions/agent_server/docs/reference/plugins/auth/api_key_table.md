# `openbb_agent_server.plugins.auth.api_key_table`

Multi-user API-key backend. Plaintext keys have the shape `oba_<key_id>.<secret>` and are verified against argon2id hashes stored in the `api_keys` SQL table. The plaintext exists only at issuance time — the table stores the hash, not the secret. Each key carries its own scope list and is scoped to one `user_id`.

**Source:** [`openbb_agent_server/plugins/auth/api_key_table.py`](../../../../openbb_agent_server/plugins/auth/api_key_table.py)

## Classes

### `IssuedKey`

Plain dataclass-like wrapper returned by `ApiKeyTableAuthBackend.issue()` — the **only** time plaintext is ever returned. Attributes: `plaintext`, `key_id`, `user_id`, `scopes`, `label`. Callers must surface or persist `plaintext` immediately; subsequent reads of the row return only the hash.

### `ApiKeyTableAuthBackend`

Plugin entry-point name: `api_key_table`. Not selected by default.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `db_url` | `str` | required | SQLAlchemy async URL (e.g. `sqlite+aiosqlite:///./data/agent_server.sqlite3` or `postgresql+asyncpg://...`). Opens an `AsyncEngine` and `async_sessionmaker(expire_on_commit=False)` for the lifetime of the backend. |

`PasswordHasher()` is instantiated with argon2-cffi's defaults — argon2id, memory cost / time cost set by the upstream library. The verifier raises `VerifyMismatchError` on mismatch.

#### Key format

Plaintext: `oba_<key_id>.<secret>`, with:

- `KEY_PREFIX = "oba_"` (literal four-char prefix; lets the operator grep logs for accidental leaks).
- `key_id = secrets.token_urlsafe(8)` — the table primary key. Stored in plaintext (it's only an identifier).
- `KEY_SEP = "."` (single dot separating id and secret).
- `secret = secrets.token_urlsafe(32)` — hashed with argon2id before storage; never persisted in clear.

`_split(raw)` validates the prefix and the dot separator; either missing → `HTTPException(403, "invalid api key")`.

#### `async authenticate(request) -> UserPrincipal`

1. `_extract_key(request)` reads `X-API-Key` then falls back to `Authorization: Bearer ...`. Missing → `HTTPException(401, "missing api key")`.
2. `_split(raw)` parses out `(key_id, secret)`.
3. Loads `ApiKey` row by `key_id`. Missing or `revoked_at is not None` → `HTTPException(403, "invalid api key")`.
4. `self._hasher.verify(row.hashed_secret, secret)` — argon2 verify. Mismatch → `HTTPException(403, "invalid api key")`.
5. Loads the linked `User` row. Missing → `HTTPException(403, "invalid api key")` (covers an orphaned key after a user delete).
6. Returns `UserPrincipal(user_id=user.user_id, display_name=user.display_name, email=user.email, scopes=tuple(row.scopes or ()))`. Scopes come from the **key**, not the user — different keys for the same user can carry different scopes.

#### `async issue(*, user_id, scopes=("agent:query", "memory:read"), label=None, display_name=None, email=None) -> IssuedKey`

Upserts the `User` row, mints a fresh `(key_id, secret)`, hashes the secret with argon2id, and inserts an `ApiKey` row. Returns an `IssuedKey` with the plaintext `oba_<key_id>.<secret>` — the only time plaintext is ever exposed. Subsequent `list_keys()` calls return metadata only. On an existing user, fills in missing `display_name` / `email` and bumps `last_seen_at`.

#### `async revoke(*, key_id) -> bool`

Sets `revoked_at = now()` on the row. Returns `True` if a row was found, `False` otherwise. The hash is intentionally **not** deleted — auditing benefits from keeping the row.

#### `async list_keys(*, user_id=None) -> list[dict]`

Returns non-secret metadata (`key_id`, `user_id`, `label`, `scopes`, `created_at`, `revoked_at`) for every key or every key belonging to one user. Plaintext / hash never appear in the response.

#### `async aclose() -> None`

Disposes the underlying `AsyncEngine` so the backend cleans up its pool on shutdown.

## TOML config example

```toml
[auth]
backend = "api_key_table"

[auth_config]
db_url = "sqlite+aiosqlite:///./data/agent_server.sqlite3"
# Postgres example:
# db_url = "postgresql+asyncpg://agent:secret@localhost/agent"
```

## See also

- [`../../../operating/auth.md`](../../../operating/auth.md) — the `openbb-agent-server keys` CLI (`issue` / `list` / `revoke`) wraps `issue() / list_keys() / revoke()` here.
- [`../../persistence/models.md`](../../persistence/models.md) — `User` and `ApiKey` table schemas.
- [`../../runtime/principal.md`](../../runtime/principal.md) — the `UserPrincipal` shape.
- [`bearer_static`](bearer_static.md) — single-user dev fallback.
- [`oidc_jwt`](oidc_jwt.md) — federated alternative when an upstream IdP already issues tokens.
