# `openbb_agent_server.plugins.models.snowflake_provider`

Wraps `langchain_community.chat_models.ChatSnowflakeCortex` — Snowflake's Cortex Complete service. Targets models that Snowflake hosts inside the account boundary (Anthropic Claude, Meta Llama, Mistral, etc.) so prompts and tool outputs never leave the customer's Snowflake region. Authenticates through Snowflake account credentials (account / user / password / role / warehouse / database / schema), pulled from `ctx.api_keys` with constructor fallbacks. Supports both username-password auth and pre-existing Snowpark `Session` reuse via the `session` kwarg.

**Source:** [`openbb_agent_server/plugins/models/snowflake_provider.py`](../../../../openbb_agent_server/plugins/models/snowflake_provider.py)

## Classes

### `SnowflakeProvider`

Plugin entry-point name: `snowflake`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatSnowflakeCortex`, calls `build_kwargs(ctx, config)`, and constructs the wrapper. `ChatSnowflakeCortex` opens a live Snowpark session at construction time, which is why this build path is marked `# pragma: no cover` — unit tests assert on `build_kwargs` instead.

`build_kwargs(ctx, config) -> dict[str, Any]` is the testable seam — it returns the kwargs that would feed `ChatSnowflakeCortex` without actually opening a session.

## Parameters

Keyword-only. **No numeric validation is performed in `__init__`** — Snowflake Cortex's accepted parameter ranges are model-dependent and validated server-side. Unknown kwargs spill into `**extra` and are passed through to `ChatSnowflakeCortex` as-is.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"claude-3-5-sonnet"` | Cortex model alias. Overridable by `config["model_name"]`. |
| `cortex_function` | `str` | `"complete"` | Cortex function. `"complete"` is the chat path. |
| `temperature` | `float` | `0.0` | Forwarded as-is. |
| `max_tokens` | `int \| None` | `None` | Forwarded when non-`None`. |
| `top_p` | `float \| None` | `None` | Forwarded when non-`None`. |
| `account` | `str \| None` | `None` | Snowflake account locator. Static fallback for `SNOWFLAKE_ACCOUNT`. |
| `user` | `str \| None` | `None` | Snowflake username. Static fallback for `SNOWFLAKE_USERNAME` / `SNOWFLAKE_USER`. |
| `password` | `str \| None` | `None` | Snowflake password. Static fallback for `SNOWFLAKE_PASSWORD`. |
| `role` | `str \| None` | `None` | Snowflake role. Static fallback for `SNOWFLAKE_ROLE`. |
| `warehouse` | `str \| None` | `None` | Snowflake warehouse. Static fallback for `SNOWFLAKE_WAREHOUSE`. |
| `database` | `str \| None` | `None` | Snowflake database. Static fallback for `SNOWFLAKE_DATABASE`. |
| `schema` | `str \| None` | `None` | Snowflake schema. Static fallback for `SNOWFLAKE_SCHEMA`. |
| `session` | `Any` | `None` | Pre-existing Snowpark `Session`. When set, takes precedence over account/user/password — the wrapper reuses the existing session instead of opening a new one. |
| `**extra` | — | — | Catch-all spilled into the kwargs dict; supports forward-compat fields added by upstream without a provider release. |

## API key resolution

Each Snowflake field falls back in order:

| Cortex kwarg | `ctx.api_keys` lookup | Constructor fallback |
| --- | --- | --- |
| `snowflake_account` | `SNOWFLAKE_ACCOUNT` | `account` |
| `snowflake_username` | `SNOWFLAKE_USERNAME`, then `SNOWFLAKE_USER` | `user` |
| `snowflake_password` | `SNOWFLAKE_PASSWORD` | `password` |
| `snowflake_role` | `SNOWFLAKE_ROLE` | `role` |
| `snowflake_warehouse` | `SNOWFLAKE_WAREHOUSE` | `warehouse` |
| `snowflake_database` | `SNOWFLAKE_DATABASE` | `database` |
| `snowflake_schema` | `SNOWFLAKE_SCHEMA` | `schema` |

`_pick(ctx, *names)` walks the lookup list and returns the first non-empty value; if none are set the constructor kwarg is used. When `session` is set the credential lookup is still performed (and forwarded), but `ChatSnowflakeCortex` uses the session for routing — the credentials are effectively ignored in practice.

## Tool choice and parallel calls

Snowflake Cortex Complete does **not** support OpenAI-style function calling. There is no `tool_choice` field and `bind_tools` on the resulting LangChain model is a no-op for tool routing — Cortex models will not emit `tool_calls`. Use this provider only for chat workflows whose tool use is orchestrated entirely outside the model (e.g. fixed pipelines, RAG-only) or accept that the agent loop will fall back to text-only output.

## TOML example

```toml
[agent.model]
type = "snowflake"

[agent.model.config]
model_name = "claude-3-5-sonnet"
cortex_function = "complete"
temperature = 0.0
max_tokens = 4096
account = "myorg-myaccount"
warehouse = "OPENBB_WH"
database = "OPENBB_DB"
schema = "AGENT"
```

In production, leave the credential fields blank in TOML and rely on `ctx.api_keys` populated from the tenant's secret store.

## Notes

- Compliance is the main reason to pick this provider: prompts, tool outputs, and model responses never leave the Snowflake region.
- The `session` kwarg lets the agent server reuse a session created elsewhere (e.g. by an OAuth-authenticated UI). When set, `ChatSnowflakeCortex` skips its internal connection setup.
- Validation is intentionally minimal — Cortex's accepted parameter ranges vary per backing model. Bad values surface as Snowflake errors at first call rather than at profile load.
- `**extra` is a forward-compat hatch: any new field upstream adds (e.g. `system_prompt`, `safe_mode`) can be set in the profile config without a provider patch.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
