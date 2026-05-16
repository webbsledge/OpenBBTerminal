# `openbb_agent_server.memory.writer`

Post-turn memory writer. After the agent has streamed its final answer, the writer runs a small `BaseChatModel` over the (human, ai) transcript to extract durable, user-scoped facts and writes each one into the `MemoryStore`. Fire-and-forget — the SSE response closes before the write completes. Gated on the `memory:write` scope.

**Source:** [`openbb_agent_server/memory/writer.py`](../../../openbb_agent_server/memory/writer.py)

## `EXTRACTOR_SYSTEM_PROMPT`

```
Extract durable, user-scoped facts and preferences from the conversation
below — things worth remembering across future sessions. Skip volatile
chatter (today's stock prices, transient task state, model errors).

Output one line per memory, plain text, no numbering. If nothing is
worth remembering, output exactly: NONE
```

The instruction is deliberately narrow — the writer is NOT a summariser. Volatile rows (prices, task state) are filtered out by prompt rather than by post-processing. A reply of literally `NONE` results in zero writes.

## `async def write_memories(...) -> int`

```python
async def write_memories(
    *,
    principal: UserPrincipal,
    store: MemoryStore,
    extractor: BaseChatModel,
    human_text: str,
    ai_text: str,
    trace_id: str,
) -> int
```

| Arg | Purpose |
| --- | --- |
| `principal` | Must carry `memory:write` — otherwise returns `0` immediately. |
| `store` | `MemoryStore` to write into. |
| `extractor` | Any LangChain `BaseChatModel` (typically the same model as the agent). Called once with `[SystemMessage, HumanMessage]`. |
| `human_text` / `ai_text` | The two turn bodies. Either may be empty; if both are empty, returns `0`. |
| `trace_id` | Stamped onto every written memory's `source_trace_id`. |

Returns the number of memories written. Extraction failures are caught and logged at WARNING — the response stream NEVER fails because the post-turn writer choked.

### Parsing

The extractor's reply is split per-line, stripped of `- `, `* `, `•`, and tab prefixes, and rows shorter than 7 chars are dropped. If the reply contains the bare token `NONE` anywhere, NO memories are written.

## `def schedule(...) -> asyncio.Task[int] | None`

```python
def schedule(
    *,
    principal: UserPrincipal,
    store: MemoryStore | None,
    extractor: BaseChatModel | None,
    human_text: str,
    ai_text: str,
    trace_id: str,
) -> asyncio.Task[int] | None
```

Fire-and-forget wrapper used by the router. Returns the task so callers can `await` it in tests; production never does. Returns `None` (a no-op) when:

- `store` or `extractor` is `None` (writer not configured); or
- the principal lacks `memory:write`.

The task name is `f"memory-write:{trace_id}"` for log identification.

## Wiring

The writer runs as a post-turn middleware step in `app/router.py`. It receives the last human turn and the agent's final answer (after the adapter has stripped `<thinking>` tags) and never sees tool messages or sub-agent prose. See [`developing/writing-a-middleware.md`](../../developing/writing-a-middleware.md) for the middleware contract.

## See also

- [`memory/store.md`](store.md) — sink.
- [`runtime/principal.md`](../runtime/principal.md) — scope check.
- [`guides/memory-and-recall.md`](../../guides/memory-and-recall.md) — end-to-end.
