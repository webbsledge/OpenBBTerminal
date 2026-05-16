# `openbb_agent_server.plugins.models._validation`

Range / shape validation helpers shared by every model provider's `__init__`. Each provider imports these to enforce the small surface of numeric constraints common to chat-completion APIs (temperature in `[0, 1]` or `[0, 2]`, penalty fields in `[-2, 2]`, positive-integer caps for token / retry counts) without re-implementing the checks.

**Source:** [`openbb_agent_server/plugins/models/_validation.py`](../../../../openbb_agent_server/plugins/models/_validation.py)

## Functions

### `check_range(name, value, lo, hi) -> None`

Inclusive bounds check for a single optional float.

| Parameter | Type | Effect |
| --- | --- | --- |
| `name` | `str` | Label used in the `ValueError` message — pass the kwarg name (`"temperature"`, `"top_p"`, etc.) so the failure points at the offending field. |
| `value` | `float \| None` | Value to check. `None` is a no-op — the caller has not set the field. |
| `lo` | `float` | Inclusive lower bound. |
| `hi` | `float` | Inclusive upper bound. |

Raises `ValueError(f"{name} must be between {lo} and {hi} (got {value})")` when `value` falls outside `[lo, hi]`.

Typical use sites:

- `check_range("temperature", temperature, 0.0, 1.0)` — Anthropic, Bedrock.
- `check_range("temperature", temperature, 0.0, 2.0)` — OpenAI, OpenAI-compat, NVIDIA, Groq, Google, Vertex.
- `check_range("top_p", top_p, 0.0, 1.0)` — every sampling model.
- `check_range("presence_penalty" / "frequency_penalty", value, -2.0, 2.0)` — OpenAI, OpenAI-compat, NVIDIA.

### `check_min(name, value, lo) -> None`

Inclusive lower-bound check for a single optional integer.

| Parameter | Type | Effect |
| --- | --- | --- |
| `name` | `str` | Label used in the `ValueError` message. |
| `value` | `int \| None` | Value to check. `None` is a no-op. |
| `lo` | `int` | Inclusive lower bound. |

Raises `ValueError(f"{name} must be >= {lo} (got {value})")` when `value < lo`.

Typical use sites:

- `check_min("max_tokens", max_tokens, 1)` — every provider that exposes a token ceiling.
- `check_min("top_k", top_k, 1)` — Anthropic, Bedrock, Vertex, Google.
- `check_min("max_retries", max_retries, 0)` — every provider.
- `check_min("n", n, 1)` — OpenAI, OpenAI-compat, Groq.
- `check_min("thinking_budget", thinking_budget, 0)` — Vertex, Google.

## Notes

- Both helpers short-circuit on `None`, so callers wire optional kwargs through unconditionally — no per-call `if value is not None` guard needed inside the provider.
- Neither helper coerces types. Pass `int` to `check_min`; passing `float` works but the error message reads awkwardly. Choice-set validation (e.g. `reasoning_effort in {"low", "medium", "high"}`) is inlined in each provider rather than abstracted here.
- The provider performs validation eagerly in `__init__`, **not** in `build`. A malformed profile therefore fails at agent-server startup (registry load) rather than at the first `model_call`.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
