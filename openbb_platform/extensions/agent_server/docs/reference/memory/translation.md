# `openbb_agent_server.memory.translation`

Async translation client backed by an NVIDIA-hosted instruct model (Riva-translate by default). Used by the per-turn ingestion pipeline to translate non-English file / message chunks into English before they're embedded — so cross-lingual recall returns hits that the embedder actually understands.

**Source:** [`openbb_agent_server/memory/translation.py`](../../../openbb_agent_server/memory/translation.py)

## `class NvidiaTranslator`

```python
NvidiaTranslator(
    *,
    model: str = "nvidia/riva-translate-4b-instruct-v1_1",
    api_key: str | None = None,
    base_url: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = 2048,
)
```

| Arg | Type | Default | Purpose |
| --- | --- | --- | --- |
| `model` | `str` | `"nvidia/riva-translate-4b-instruct-v1_1"` | NIM model id. Tuned for Riva-translate; other instruct models work with the same prompt template. |
| `api_key` | `str \| None` | `os.environ["NVIDIA_API_KEY"]` | API key. Required at first `translate()` call. |
| `base_url` | `str \| None` | `None` | Optional NIM endpoint override. |
| `temperature` | `float` | `0.0` | Deterministic by default. |
| `max_tokens` | `int \| None` | `2048` | Per-chunk output cap. |

The underlying `ChatNVIDIA` client is built lazily on first `translate()` call.

### Prompt template

A two-turn chat completion:

```
system: You are a precise translation engine. Translate the user's text into
        the target language. Output ONLY the translation, with no preface,
        commentary, or quoted source. Preserve Markdown, code fences,
        bullet structure, numbers, and proper nouns exactly. If the text
        is already in the target language, return it unchanged.
user:   Translate from {source_language} to {target_language}:

        {text}
```

The system instruction is conservative on purpose — Riva-translate has been observed adding commentary, quoting the source, or wrapping output in code fences. Other instruct models with a similar structure work; tune the system message if you swap the model.

### `async def translate(text, *, source_language="auto", target_language="English") -> str`

Translate `text` into `target_language`. Empty / whitespace-only input returns `""` without a network call. The implementation prefers `ainvoke` on the underlying chat model; for older NVIDIA-endpoints releases it falls back to `asyncio.to_thread(self._client.invoke, …)`.

Block-list `content` shapes (Anthropic-style) are flattened to plain text by joining every `{"type": "text", "text": "..."}` block.

### Used by

- [`memory/ingestion.md`](ingestion.md) — applied to each non-English prose chunk when `translate_for_ingestion` is enabled. Code chunks are NEVER translated.
- The `translate` tool (`plugins/tools/translate`) — agent-callable translator.

## See also

- [`memory/factory.md`](factory.md) — construction.
- [`operating/memory.md`](../../operating/memory.md) — when to enable translation.
