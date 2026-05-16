# `openbb_agent_server.plugins.models.nvidia_provider`

Wraps `langchain_nvidia_ai_endpoints.ChatNVIDIA` and exposes the full NIM "View Code" parameter panel — sampling, reasoning controls (`reasoning_effort`, `reasoning_budget`, `chat_template_kwargs`), penalties, custom `base_url` for self-hosted NIM, and per-request `default_headers`. Reasoning-model fields and the penalty pair ride via `model_kwargs` rather than directly so langchain's "transferring to model_kwargs for you" warning never fires. Also overrides the noisy ChatNVIDIA `supports_tools` allow-list (`_silence_unknown_tools_warning`) so the bind-time warning is silenced for tool-using models the allow-list hasn't yet been updated for. Optional install: `[nvidia]`.

**Source:** [`openbb_agent_server/plugins/models/nvidia_provider.py`](../../../../openbb_agent_server/plugins/models/nvidia_provider.py)

## Classes

### `NvidiaProvider`

Plugin entry-point name: `nvidia`.

`build(ctx, config) -> BaseChatModel` lazy-imports `ChatNVIDIA`, constructs the model, and calls `_silence_unknown_tools_warning(model)` to flip the per-model `supports_tools` flag on the registry entry before returning.

## Parameters

Keyword-only. Validation in `__init__` via [`_validation.py`](_validation.md): `check_range("temperature", …, 0.0, 2.0)`, `check_range("top_p", …, 0.0, 1.0)`, `check_range("frequency_penalty"/"presence_penalty", …, -2.0, 2.0)`, `check_min("max_tokens"/"max_completion_tokens", …, 1)`. `reasoning_effort` ∈ `{"none", "low", "medium", "high"}`. `reasoning_budget` must be `-1` (NIM's "no enforcement" sentinel) or `>= 0`.

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_name` | `str` | `"meta/llama-3.3-70b-instruct"` | NIM model path. Overridable by `config["model_name"]`. |
| `api_key` | `str \| None` | `None` | Static fallback. See API-key resolution. |
| `base_url` | `str \| None` | `None` | For self-hosted NIM. Omit for the public NVIDIA API. |
| `temperature` | `float` | `0.0` | `[0, 2]`. |
| `max_tokens` | `int \| None` | `None` | NIM legacy name. `>= 1`. |
| `max_completion_tokens` | `int \| None` | `None` | Reasoning-model spelling, aliased to `max_tokens`. If both are set, `max_tokens` wins. Forwarded as `max_completion_tokens` (avoids ChatNVIDIA's deprecation warning). |
| `top_p` | `float \| None` | `None` | `[0, 1]`. |
| `frequency_penalty` | `float \| None` | `None` | `[-2, 2]`. Forwarded via `model_kwargs`. |
| `presence_penalty` | `float \| None` | `None` | `[-2, 2]`. Forwarded via `model_kwargs`. |
| `seed` | `int \| None` | `None` | Best-effort determinism. |
| `stop` | `list[str] \| str \| None` | `None` | Stop sequences. |
| `streaming` | `bool` | `True` | Translated to ChatNVIDIA's inverted `disable_streaming` at build time. |
| `reasoning_effort` | `str \| None` | `None` | One of `{"none", "low", "medium", "high"}`. Forwarded via `model_kwargs`. |
| `reasoning_budget` | `int \| None` | `None` | Max thinking tokens; `-1` disables enforcement. Forwarded via `model_kwargs`. Most useful paired with `reasoning_effort="high"`. |
| `chat_template_kwargs` | `dict[str, Any] \| None` | `None` | Legacy path for `reasoning_budget` on older NIM builds. Forwarded via `model_kwargs`. |
| `default_headers` | `dict[str, str] \| None` | `None` | Per-request headers (e.g. `X-Tenant`). |
| `extra_body` | `dict[str, Any] \| None` | `None` | Body-field passthrough merged into `model_kwargs`. |
| `model_kwargs` | `dict[str, Any] \| None` | `None` | Body-field passthrough merged into `model_kwargs`. Wins on collision with `extra_body`. |

## API key resolution

1. `ctx.api_keys["NVIDIA_API_KEY"]`.
2. `self._api_key`.

If neither resolves, `api_key` is omitted from the ChatNVIDIA kwargs and the SDK falls back to its own env-var lookup (`NVIDIA_API_KEY`).

## Tool choice and parallel calls

`tool_choice` is set per-call by middleware. NIM passes through whichever modes the backing model supports — most NIM-hosted instruct models accept the OpenAI `auto` / `required` / explicit-function shapes; reasoning models often emit a single tool call per turn. `_silence_unknown_tools_warning` flips `client.model.supports_tools = True` to suppress the "Model 'X' is not known to support tools" warning ChatNVIDIA emits for models outside its hardcoded allow-list (Mistral Large 3, MiniMax M2, qwen3-thinking, etc.) — this is a noise fix only; it does not enable tools where the backend cannot run them.

## TOML example

NIM-hosted reasoning model:

```toml
[agent.model]
type = "nvidia"

[agent.model.config]
model_name = "deepseek-ai/deepseek-r1"
temperature = 0.0
max_tokens = 8192
reasoning_effort = "high"
reasoning_budget = 16000
default_headers = { "X-Tenant" = "acme" }
```

Self-hosted NIM instruct model:

```toml
[agent.model]
type = "nvidia"

[agent.model.config]
base_url = "https://nim.internal.example.com/v1"
model_name = "meta/llama-3.3-70b-instruct"
temperature = 0.2
presence_penalty = 0.0
```

## Notes

- `streaming` is inverted at build time because ChatNVIDIA's native field is `disable_streaming` — the provider exposes the normal positive flag and translates.
- Non-native fields (`frequency_penalty`, `presence_penalty`, `reasoning_effort`, `reasoning_budget`, `chat_template_kwargs`) are merged into `kwargs["model_kwargs"]` manually so langchain's auto-transfer warning does not fire. Collision precedence (highest wins): user `model_kwargs` > `chat_template_kwargs` slot > per-field slots > `extra_body`.
- `_silence_unknown_tools_warning` is best-effort: if ChatNVIDIA's internal `_client.model.supports_tools` attribute moves or disappears in a future release, the override is a no-op and the worst case is the warning we tried to silence reappears.
- The legacy `max_tokens` field is auto-mapped to `max_completion_tokens` on the wire so newer reasoning models work without changing the profile.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../operating/configuration.md`](../../../operating/configuration.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
