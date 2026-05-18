# `openbb_agent_server.memory.ingestion`

Per-turn ingestion of long uploaded files and long human messages. Splits content into semantic chunks via LangChain's `RecursiveCharacterTextSplitter` (language-aware for code), optionally translates prose chunks into English, and writes each chunk to the `MemoryStore` with a `kind` of `context_text` or `context_code`. Run from `app/router.py` BEFORE the agent is invoked so the first model call already has the chunks in recall scope.

**Source:** [`openbb_agent_server/memory/ingestion.py`](../../../openbb_agent_server/memory/ingestion.py)

## `async def ingest_request_context(...)`

```python
async def ingest_request_context(
    *,
    principal: UserPrincipal,
    store: MemoryStore,
    body: Any,                       # QueryRequest
    trace_id: str,
    char_threshold: int = 2000,
    chunk_chars: int = 1500,
    chunk_overlap: int = 200,
    translator: NvidiaTranslator | None = None,
    translate_target_lang: str = "English",
) -> int
```

| Arg | Purpose |
| --- | --- |
| `principal` | The authenticated user. Must carry `memory:write`; otherwise `ingest_request_context` returns `0` without writing. |
| `store` | The `MemoryStore` to write into (typically `SqliteMemoryStore`). |
| `body` | The `QueryRequest`. The function walks `body.uploaded_files` and `body.messages` looking for long content. |
| `trace_id` | Stamped onto every written chunk's `source_trace_id`. |
| `char_threshold` | Sources shorter than this are skipped entirely. Default 2000. |
| `chunk_chars` | Target chunk size. Default 1500. |
| `chunk_overlap` | Adjacent-chunk overlap. Default 200. |
| `translator` | Optional [`NvidiaTranslator`](translation.md). When set, each non-code chunk that looks non-English (≥5% non-ASCII) is translated before storage. |
| `translate_target_lang` | Target language for the translator. Default `"English"`. |

Returns the count of chunks written. The function is best-effort: per-chunk write / translate exceptions are logged and skipped, never raised.

### Source walk

1. **Uploaded files.** For each `FileRef` with text-like MIME or extension, the base64 payload is decoded through the matching LangChain document loader (`CSVLoader` / `BSHTMLLoader` / `JSONLoader` / `TextLoader`) — falling back to a raw UTF-8 / latin-1 decode on loader failure. PDFs are NOT ingested here; the `pdf_extract` tool handles them on demand.
2. **Long human messages.** Every `role:"human"` message whose `content` string is `>= char_threshold` chars is treated as a source.

### Chunker selection

For each source the code / prose verdict from [`classifier.looks_like_code`](classifier.md) chooses between:

- **Code path.** `RecursiveCharacterTextSplitter.from_language(language, ...)` with a `Language` enum mapped from the file extension (`.py` → `PYTHON`, `.go` → `GO`, `.md` → `MARKDOWN`, etc.). The splitter respects language-aware separators (function boundaries, class blocks, …).
- **Prose path.** The default character-recursive splitter.

Empty / whitespace-only chunks are dropped.

### Tagging

Each stored chunk is prefixed:

```
[file:<name> chunk <i>/<N>[, translated→English]]
<chunk text>
```

The header is what the recall path uses to render citations and the agent uses to ground "where did this come from?" questions.

### Translation

When `translator` is supplied and the chunk:

- is NOT code (per `looks_like_code`)
- AND looks non-English (`≥5%` non-ASCII bytes in the first 4 KiB OR target is not English)

… the chunk is run through `translator.translate(...)`. The translated body replaces the stored text; the original is dropped on the floor (intentional — recall keys off semantic meaning, not the source-language phrasing). Translation failures fall back to the original body and log a warning.

## `def chunk_text(text, *, chunk_chars=1500, overlap=200, language=None) -> list[str]`

Standalone chunker. Used by `ingest_request_context` and reusable by callers who need to mirror the chunking. Validates `chunk_chars > 0` and `0 <= overlap < chunk_chars`.

## TOML

```toml
[agent]
ingest_char_threshold = 2000
ingest_chunk_chars    = 1500
ingest_chunk_overlap  = 200
translate_for_ingestion = true
ingest_target_language  = "English"
```

## See also

- [`memory/classifier.md`](classifier.md) — code vs. prose routing.
- [`memory/sqlite_store.md`](sqlite_store.md) — sink for ingested chunks.
- [`memory/translation.md`](translation.md) — optional translator.
- [`guides/memory-and-recall.md`](../../guides/memory-and-recall.md) — guide.
