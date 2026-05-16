# `openbb_agent_server.plugins.auth`

Built-in `AuthBackend` implementations. The runtime selects exactly one via `AgentServerSettings.auth_backend` — defaulting to `"none"` (single-user dev). Each backend resolves an inbound HTTP request to a `UserPrincipal`; the principal flows through the `RunContext` and gates every persistence read/write by `user_id` and `scopes`.

- [`none`](none.md) — no authentication; every request resolves to the shared `anonymous` principal with scopes `("agent:query", "memory:read")`. **Default.** Dev-only.
- [`bearer_static`](bearer_static.md) — single shared bearer token from config or `OPENBB_AGENT_AUTH_BEARER`. Dev / test only — every authenticated caller becomes the same `static-user`.
- [`api_key_table`](api_key_table.md) — multi-user, multi-key. Plaintext keys are `oba_<key_id>.<secret>` and stored as argon2id hashes in the `api_keys` SQL table. Per-key scopes; `issue` / `revoke` / `list_keys` admin methods power the `openbb-agent-server keys` CLI.
- [`oidc_jwt`](oidc_jwt.md) — federated JWT verification against a JWKS URL. Configurable `audience`, `issuer`, `algorithms` and JWKS cache TTL. Maps `sub` → `user_id`, `name`/`preferred_username` → `display_name`, `email` → `email`, `scope`/`scopes` → `scopes`; full claims retained in `raw_claims`.
- [`openbb_workspace`](openbb_workspace.md) — gateway-trust mode for deployments behind the OpenBB Workspace. Reads `X-OpenBB-User: <email>`, validates RFC-5321 format, hashes through `hash_user_id` for `user_id`, retains the raw email as `email`.

**Source:** [`openbb_agent_server/plugins/auth/__init__.py`](../../../../openbb_agent_server/plugins/auth/__init__.py)

## See also

- [`../../../operating/auth.md`](../../../operating/auth.md) — production guidance, the `keys` CLI, and how to migrate off `none`.
- [`../../../developing/writing-an-auth-backend.md`](../../../developing/writing-an-auth-backend.md) — the `AuthBackend` plugin protocol.
- [`../../runtime/principal.md`](../../runtime/principal.md) — the `UserPrincipal` model returned by every backend.
- [`../../runtime/identity.md`](../../runtime/identity.md) — `hash_user_id` / `is_email`, used by `openbb_workspace`.
