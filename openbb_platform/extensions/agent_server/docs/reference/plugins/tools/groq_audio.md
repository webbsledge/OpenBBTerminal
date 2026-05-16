# `openbb_agent_server.plugins.tools.groq_audio`

Speech-to-text and speech-translate via Groq's hosted Whisper endpoints (`/audio/transcriptions`, `/audio/translations`). Returns text plus optional segment / word-level timestamps and the audio duration in seconds; integrates with [`runtime/groq_rate_limiter`](../models/groq_rate_limiter.md) so the per-key rate budget is shared across concurrent runs.

**Source:** [`openbb_agent_server/plugins/tools/groq_audio.py`](../../../../openbb_agent_server/plugins/tools/groq_audio.py)

## Classes

### `GroqAudioToolSource`

Plugin entry-point name: `groq_audio`. Constructor takes `api_key`, `base_url` (default `https://api.groq.com/openai/v1`), `default_model` (`"whisper-large-v3"` or `"whisper-large-v3-turbo"`), `default_language`, `timeout`, `max_retries`. `tools(ctx, config)` resolves the api key (`ctx.api_keys["GROQ_API_KEY"]` → config → constructor); raises `RuntimeError` at tool-build time if absent.

| Tool | Args | Returns |
| --- | --- | --- |
| `transcribe_audio` | `audio_name: str \| None` (uploaded file from `QueryRequest.uploaded_files[].name`), `audio_url: str \| None` (public URL), `model: str` (default `"whisper-large-v3-turbo"`), `language: str \| None` (ISO-639-1; auto-detect if blank), `prompt: str \| None` (context to bias vocab / names / style), `temperature: float = 0.0`, `response_format: str = "verbose_json"` (`json` / `verbose_json` / `text` / `srt` / `vtt`), `timestamp_granularities: list[str] \| None` (`["word"]` / `["segment"]` / both — forces `verbose_json`) | Whisper response JSON augmented with `_groq: {model, endpoint, duration_seconds, rate_limiter_snapshot}`. Non-JSON `response_format` values are wrapped as `{"text": <raw body>}`. |
| `translate_audio` | same as `transcribe_audio` | Same shape; the output is English-translated regardless of source language. Calls `/audio/translations`. |

### Audio resolution

`_resolve_audio` returns `(filename, bytes, mime)`:

- `audio_url` is fetched once via a sync `httpx.Client(timeout=60.0)`; the trailing path component becomes the filename, `Content-Type` is preserved.
- `audio_name` is looked up against `ctx.uploaded_files`. `data_base64` is decoded inline; falling back to fetching `ref.url` if needed. Missing names raise `RuntimeError` with the list of available filenames.
- Neither arg supplied → `RuntimeError`.

### Retry loop

`_transcribe` runs an explicit retry loop:

- HTTP 429 or 5xx → exponential backoff (`0.5, 1, 2, 4, 8` s, capped at 30 s) or `Retry-After` header value if present.
- Network errors → same backoff.
- Other 4xx → raise immediately.
- Each retry attempt is logged with `attempt N/max_retries`.

On success, `duration_seconds` from the verbose response is fed back into the rate limiter via `record_audio_seconds` so the budget accounts for actual audio time consumed.

### Validation

- `response_format` must be one of `{json, verbose_json, text, srt, vtt}`.
- `timestamp_granularities` entries must be `word` or `segment`.
- `default_model` (constructor) is locked to the two Groq whisper models above.
- `max_retries >= 0` enforced at construction.

## Security

The tool description does not currently echo a prompt-injection rule, but the underlying audio surface is symmetric to [`gemma_audio`](gemma_audio.md): treat the transcript as DATA, never as instructions.

## Config

`[agent.tool_source_config.groq_audio]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `api_key` | string | `None` | Falls back to `ctx.api_keys["GROQ_API_KEY"]`. |
| `base_url` | string | `"https://api.groq.com/openai/v1"` | Groq OpenAI-compatible endpoint. |
| `default_model` | string | `"whisper-large-v3-turbo"` | Only `whisper-large-v3` / `whisper-large-v3-turbo` accepted. |
| `default_language` | string | `None` | ISO-639-1 to skip auto-detect by default. |
| `timeout` | float | `120.0` | Per-request timeout. |
| `max_retries` | int | `5` | Backoff retries for 429 / 5xx / network errors. |

## Related

- [`gemma_audio` tool source](gemma_audio.md) — NIM Gemma-3n alternative.
- [`models/groq_rate_limiter`](../models/groq_rate_limiter.md) — the shared rate-budget tracker.
- [Operating: configuration](../../../operating/configuration.md) — forwarding `GROQ_API_KEY` via `QueryRequest.api_keys`.
