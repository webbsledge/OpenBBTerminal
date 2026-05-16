# `openbb_agent_server.plugins.tools.snowflake_tools`

Snowflake tool source — the full Snowflake feature surface as LangChain tools, including the Cortex AI suite. One plugin entry-point (`snowflake`) registers twenty tools covering query, catalog introspection, Cortex SQL, and the Cortex REST endpoints.

**Source:** [`openbb_agent_server/plugins/tools/snowflake_tools/__init__.py`](../../../../../openbb_agent_server/plugins/tools/snowflake_tools/__init__.py)

## Modules

| Module | Purpose |
| --- | --- |
| [`source`](source.md) | `SnowflakeToolSource` — the plugin entry-point. Registers all twenty tools, handles credential layering, and renders query results as table artifacts. |
| [`client`](client.md) | `SnowflakeCredentials`, `QueryResult`, `SnowflakeClient` — connection management, session-expiry retry, read-only enforcement, and auto LIMIT injection. |
| [`cortex`](cortex.md) | Cortex SQL helpers (`cortex_complete`, `cortex_summarize`, …) and REST helpers (`cortex_search`, `cortex_analyst`) plus the KeyPair JWT minter and `auth_headers` resolver. |
| [`safety`](safety.md) | SQL guard — statement classification, `enforce_read_only`, `inject_limit`. Wraps `sqlglot` with the `snowflake` dialect. |

## Credential layering

`source._credentials_from_ctx(ctx, base)` resolves credentials in this order (later wins):

1. Plugin-constructor `credentials` dict.
2. `SNOWFLAKE_*` env vars.
3. `ctx.api_keys` values for the same uppercase names.

Read-only mode is on by default; toggle via `[agent.tool_source_config.snowflake].read_only = false` only when the agent must mutate data.

## Required extras

Install the agent_server with the `[snowflake]` extra. Cortex REST also requires `cryptography` and `pyjwt` (transitive deps of the Snowflake connector for KeyPair auth).

## Related

- [Operating: configuration](../../../../operating/configuration.md) — `[agent.tool_source_config.snowflake]` and credential forwarding.
- [`runtime/emit.py`](../../../runtime/emit.md) — table-artifact + citation wire shape used by query results and `cortex_search` hits.
- [Writing a tool source](../../../../developing/writing-a-tool-source.md) — model for plug-ins that wrap another data warehouse the same way.
