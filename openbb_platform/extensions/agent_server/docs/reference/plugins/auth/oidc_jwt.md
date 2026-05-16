# `openbb_agent_server.plugins.auth.oidc_jwt`

Federated JWT bearer-token backend. Verifies the `Authorization: Bearer <jwt>` header against a JWKS URL via `PyJWKClient`, then maps standard OIDC claims onto a `UserPrincipal`. JWKS keys are cached client-side so steady-state authentication doesn't fan out to the IdP per request.

**Source:** [`openbb_agent_server/plugins/auth/oidc_jwt.py`](../../../../openbb_agent_server/plugins/auth/oidc_jwt.py)

## Classes

### `OidcJwtAuthBackend`

Plugin entry-point name: `oidc_jwt`. Not selected by default.

#### Constructor

| Kwarg | Type | Default | Effect |
| --- | --- | --- | --- |
| `jwks_url` | `str` | required | The IdP's JWKS endpoint (typically `<issuer>/.well-known/jwks.json`). `__init__` raises `RuntimeError` if missing or empty. |
| `audience` | `str \| None` | unset | Expected `aud` claim. When set, `jwt.decode` raises `InvalidAudienceError` on mismatch (→ 403). When unset, audience is not checked. |
| `issuer` | `str \| None` | unset | Expected `iss` claim. When set, `jwt.decode` raises `InvalidIssuerError` on mismatch (→ 403). When unset, issuer is not checked. |
| `algorithms` | `tuple[str, ...]` | `("RS256",)` | Allowed signature algorithms. Pinned — never inferred from the token header. |
| `jwks_cache_seconds` | `int` | `3600` | TTL passed to `PyJWKClient(lifespan=...)`. Keys are also fingerprint-cached (`cache_keys=True`). |

`PyJWKClient(jwks_url, cache_keys=True, lifespan=jwks_cache_seconds)` is instantiated once per backend instance — startup is the only blocking JWKS fetch; subsequent verifies hit the cache until `lifespan` expires.

#### `async authenticate(request) -> UserPrincipal`

1. Reads `Authorization` (case-insensitive `"bearer "` prefix). Missing → `HTTPException(401, "missing bearer token")`.
2. `self._jwks_client.get_signing_key_from_jwt(token).key` — picks the JWK by the token's `kid` header. Any failure (network error, missing kid, unknown kid) logs a `WARNING` and raises `HTTPException(403, "cannot verify signing key")`.
3. `jwt.decode(token, key=signing_key, algorithms=self._algorithms, audience=self._audience, issuer=self._issuer)` — verifies signature, `exp`, `nbf`, `iat`, and (when configured) `aud` / `iss`. Any `jwt.InvalidTokenError` subclass → `HTTPException(403, f"invalid token: {exc}")`.
4. Claim → `UserPrincipal` mapping:

| Principal field | Source claim | Notes |
| --- | --- | --- |
| `user_id` | `sub` | Required. Missing → `HTTPException(403, "token missing sub")`. |
| `display_name` | `name` then `preferred_username` | First non-empty wins; both unset → `None`. |
| `email` | `email` | Forwarded as-is. |
| `scopes` | `scope` then `scopes` (see `_extract_scopes`) | Space-separated string is `.split()`'d; list/tuple is coerced via `str()`; anything else → `()`. |
| `raw_claims` | full `claims` dict | Stashed so downstream tools (e.g. tenancy enforcement) can read non-standard claims. |

## TOML config example

```toml
[auth]
backend = "oidc_jwt"

[auth_config]
jwks_url = "https://login.example.com/.well-known/jwks.json"
audience = "openbb-agent-server"
issuer = "https://login.example.com/"
algorithms = ["RS256", "ES256"]
jwks_cache_seconds = 3600
```

## See also

- [`../../../operating/auth.md`](../../../operating/auth.md) — choosing between `api_key_table` and `oidc_jwt`.
- [`../../../developing/writing-an-auth-backend.md`](../../../developing/writing-an-auth-backend.md) — the `AuthBackend` plugin protocol.
- [`../../runtime/principal.md`](../../runtime/principal.md) — the `UserPrincipal` model and how `raw_claims` is consumed.
- [`openbb_workspace`](openbb_workspace.md) — gateway-trust alternative when an upstream proxy already terminates OIDC.
