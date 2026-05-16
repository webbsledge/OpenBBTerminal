# `openbb_agent_server.plugins.models.fake_provider`

Deterministic in-process model for tests, demos, and offline development. Wraps `langchain_core.language_models.fake_chat_models.GenericFakeChatModel` with a `_ToolAwareFakeChatModel` subclass that accepts `bind_tools` (a no-op pass-through) and emits a `_stream` / `_astream` implementation so the agent server's SSE pipeline sees realistic message chunks — including synthetic `tool_call_chunks` when the scripted response is an `AIMessage` with `tool_calls`.

**Source:** [`openbb_agent_server/plugins/models/fake_provider.py`](../../../../openbb_agent_server/plugins/models/fake_provider.py)

## Classes

### `_ToolAwareFakeChatModel`

Internal — `GenericFakeChatModel` with three additions:

- `bind_tools(tools, *, tool_choice=None, **kwargs)` returns `self`. The model does not actually route tools; this just keeps the agent loop from blowing up on `AttributeError`.
- `_stream(messages, stop, run_manager, **kwargs)` pulls the next scripted message from `self.messages`. `str` → a single `AIMessageChunk(content=..., chunk_position="last")`. `AIMessage` → `AIMessageChunk(content=..., tool_call_chunks=[...], chunk_position="last")` with one chunk per `tool_call` (each containing `name`, `args` JSON-encoded, `id`, `index`). Anything else stringifies. Yields a single `ChatGenerationChunk`.
- `_astream` simply re-yields from `_stream` to satisfy async callers.

### `FakeProvider`

Plugin entry-point name: `fake`.

`__init__(*, responses=None, **_ignored)` resolves the scripted response list:

1. The `responses` kwarg.
2. `OPENBB_AGENT_FAKE_RESPONSES` env var, JSON-decoded.
3. `DEFAULT_RESPONSES = ("OK.",)`.

The result is stored as a tuple. `**_ignored` swallows any extra kwargs so test fixtures can shovel arbitrary config in without raising.

`build(ctx, config)` reads `config.get("responses", self._responses)` so individual runs can override the scripted list per-call, then constructs a fresh `_ToolAwareFakeChatModel` with an `iter([AIMessage(content=r) for r in responses])`. **Fresh iterator per build** — concurrent runs do not share cursor state, so two simultaneous runs each see the responses from position 0.

## Parameters

| Kwarg | Type | Default | Notes |
| --- | --- | --- | --- |
| `responses` | `tuple[str, ...] \| list[str] \| None` | `None` → env → `("OK.",)` | Scripted plain-text responses. Each one is wrapped in an `AIMessage` per build. |
| `**_ignored` | — | — | Discarded; lets test rigs pass any extra kwargs. |

Per-run config keys (read from `config` in `build`):

| Config key | Type | Default | Effect |
| --- | --- | --- | --- |
| `responses` | `Sequence[str] \| Sequence[AIMessage]` | `self._responses` | Override the scripted list for this run only. Pass `AIMessage(content=..., tool_calls=[...])` entries to script tool calls. |

## API key resolution

Not used. `FakeProvider` never reads `ctx.api_keys`.

## Tool choice and parallel calls

`bind_tools` returns `self`; there is no real tool-call routing. To exercise the agent's tool-calling code path against this model, script the response list with `AIMessage` entries whose `tool_calls=[...]` field is populated — `_stream` will faithfully emit the corresponding `tool_call_chunks` and the agent loop will see them as if the model had emitted them. Parallel tool calls are supported by emitting multiple entries in `tool_calls` on a single `AIMessage` — each becomes its own indexed `tool_call_chunk`.

## TOML example

```toml
[agent.model]
type = "fake"

[agent.model.config]
responses = ["Hello.", "Goodbye."]
```

Or via environment:

```bash
export OPENBB_AGENT_FAKE_RESPONSES='["Hello.", "Goodbye."]'
```

Scripted tool call from a test:

```python
from langchain_core.messages import AIMessage

responses = [
    AIMessage(
        content="",
        tool_calls=[{"name": "get_quote", "args": {"symbol": "AAPL"}, "id": "call_1"}],
    ),
    "Final answer.",
]
provider = FakeProvider(responses=responses)
```

## Notes

- No external dependencies — `FakeProvider` is always installed and is the default when running unit tests against the agent server.
- The model has no notion of context, history, or stop conditions; it strictly returns the next scripted message regardless of input. Construct your script to match the sequence of model calls your agent loop will make.
- `_ignored = **_ignored` deliberately silently accepts unknown kwargs so the same TOML can be used to swap in `fake` during smoke-tests of a profile that was authored for `anthropic` / `openai` / etc.
- The streaming path emits `chunk_position="last"` on every chunk because each scripted response is delivered in a single chunk — partial-token streaming is not simulated.

See also: [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
