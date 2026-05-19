# `openbb_agent_server.plugins.models.bedrock_provider`

Wraps `langchain_aws.ChatBedrock` for any model on AWS Bedrock — Anthropic Claude, Meta Llama, Amazon Titan, Mistral, etc. Authenticates through standard AWS credential chains (env vars, profile, instance role) rather than an API key. Routes `top_p` / `top_k` / `stop_sequences` through Bedrock's per-model `model_kwargs` because those fields vary by underlying model family. Supports Bedrock Guardrails and the newer Converse API. Optional install: `[bedrock]`.

**Source:** [`openbb_agent_server/plugins/models/bedrock_provider.py`](../../../../openbb_agent_server/plugins/models/bedrock_provider.py)

## Classes

### `BedrockProvider`

Plugin entry-point name: `bedrock`.

`build(ctx, config) -> BaseChatModel` lazy-imports `langchain_aws.ChatBedrock` (raising an install hint if missing) and returns a freshly-constructed instance.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 1.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_min("top_k", …, 1)`, `check_min("max_tokens", …, 1)`, `check_min("max_retries", …, 0)`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"anthropic.claude-opus-4-7-v1:0"` | Bedrock model id. Forwarded as `model_id`. Overridable by `config["model_name"]`. |
| `region_name` | `str \| None` | `None` | AWS region. Falls back to boto3 default chain. |
| `credentials_profile_name` | `str \| None` | `None` | Named AWS profile for credentials lookup. |
| `endpoint_url` | `str \| None` | `None` | Custom Bedrock endpoint (VPC PrivateLink, FIPS endpoints). |
| `provider` | `str \| None` | `None` | Explicit family hint (`"anthropic"`, `"meta"`, …) — only needed when ChatBedrock cannot infer it from `model_name`. |
| `temperature` | `float` | `0.0` | `[0, 1]`. |
| `max_tokens` | `int \| None` | `None` | Generated-token ceiling. `>= 1`. |
| `top_p` | `float \| None` | `None` | `[0, 1]`. Routed via `model_kwargs`. |
| `top_k` | `int \| None` | `None` | `>= 1`. Routed via `model_kwargs`. |
| `stop_sequences` | `list[str] \| None` | `None` | Routed via `model_kwargs`. Copied via `list(...)`. |
| `timeout` | `int \| None` | `None` | Per-request timeout (seconds). |
| `max_retries` | `int` | `2` | `>= 0`. |
| `guardrails` | `dict[str, Any] \| None` | `None` | Bedrock Guardrails config (e.g. `{"guardrailIdentifier": "...", "guardrailVersion": "1"}`). |
| `beta_use_converse_api` | `bool` | `False` | Toggle Bedrock's Converse API path (tool use, multimodal). |
| `streaming` | `bool` | `True` | SSE deltas. |

## API key resolution

Bedrock does not use an API key. Authentication is through the standard AWS credential chain (in order): explicit credentials on the boto3 client (not exposed by this provider), `credentials_profile_name`, environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`), shared credentials file, EC2 instance role / ECS task role / IRSA in EKS. The runtime's `ctx.api_keys` is not consulted.

Region resolution: `region_name` kwarg → `AWS_REGION` / `AWS_DEFAULT_REGION` → boto3 default config.

## Tool choice and parallel calls

`tool_choice` and parallel-call behaviour are model-family dependent — set per-call by middleware. The Converse API (`beta_use_converse_api=True`) is the path that supports OpenAI-style tool calling across all model families that Bedrock has wired up; the InvokeModel path varies by provider. Claude on Bedrock supports parallel tool calls; Llama / Mistral support a single call per turn.

## TOML example

Anthropic Claude on Bedrock with the Converse API and a guardrail:

```toml
[agent.model]
type = "bedrock"

[agent.model.config]
model_name = "anthropic.claude-opus-4-7-v1:0"
region_name = "us-east-1"
credentials_profile_name = "openbb-prod"
temperature = 0.0
max_tokens = 16384
beta_use_converse_api = true

[agent.model.config.guardrails]
guardrailIdentifier = "arn:aws:bedrock:us-east-1:111122223333:guardrail/abc123"
guardrailVersion = "1"
```

## Notes

- `top_p` / `top_k` / `stop_sequences` are sent inside `model_kwargs` because their wire encoding depends on the underlying model family — ChatBedrock spreads them into the per-family request body.
- `model_kwargs` is only attached when at least one of the three is set, so the field doesn't appear empty on the wire.
- `beta_use_converse_api=True` is required for OpenAI-style tool use on non-Claude models and for multimodal inputs across the board.
- `provider` is rarely needed: ChatBedrock infers the family from the `model_id` prefix (`"anthropic."`, `"meta."`, `"mistral."`, etc.). Set it only when running a custom model whose id does not follow that convention.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
