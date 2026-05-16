# `openbb_agent_server.plugins.tools.gemma_audio`

Audio / video transcription over NVIDIA NIM's `google/gemma-3n-e4b-it` (a multimodal Gemma variant that takes audio_url parts). Long clips are sliced automatically via `ffmpeg` and each segment streams partial results back as reasoning steps. Pair with [`groq_audio`](groq_audio.md) when you want Whisper-flavoured transcription with per-word / per-segment timestamps instead.

**Source:** [`openbb_agent_server/plugins/tools/gemma_audio.py`](../../../../openbb_agent_server/plugins/tools/gemma_audio.py)

## Classes

### `GemmaAudioToolSource`

Plugin entry-point name: `gemma_audio`. Constructor takes `model`, `api_key`, `base_url`, `temperature`, `max_fetch_bytes`, `max_audio_seconds_per_call`, `fetch_timeout_s`. `tools(ctx, config)` resolves the api key (`ctx.api_keys["NVIDIA_API_KEY"]` → config → constructor → `os.environ`); without it, returns `[]` with a warning.

| Tool | Args | Returns |
| --- | --- | --- |
| `list_audio` | — | `list[{name, mime, url}]` for every file with `audio/*` / `video/*` mime or one of `.mp3/.m4a/.aac/.wav/.flac/.ogg/.oga/.opus/.webm/.mp4/.mov/.mkv` in `RunContext.uploaded_files`. Emits one `reasoning_step` with the count. |
| `transcribe_audio` | `name: str \| None` (uploaded), `url: str \| None` (direct), `instruction: str = "Transcribe this clip verbatim, preserving speaker turns where audible."`, `max_output_tokens: int = 8192` (∈ [128, 32768], **per segment**) | `{model, target, segments, text}` — `text` is `"\n\n"`-joined segment transcripts. Each segment is prefixed `--- segment N/M (HH:MM:SS–HH:MM:SS) ---` when there is more than one. Streams partial chunks via `astream`. |
| `submit_transcribe_audio` | same as `transcribe_audio` | `{job_id, label}` — background variant via [`runtime/jobs.py`](../../runtime/jobs.md). Recommended for clips > 2 minutes when the agent has other tools to call in parallel. |

### Audio resolution and slicing

`_resolve_to_bytes` honours `data_base64` first (decoded off the event loop, with `max_fetch_bytes` enforcement) and falls back to streaming `fetch_url`. The bytes then go through `_media.split_audio_bytes(raw, max_seconds=max_audio_s)`:

1. `probe_audio_duration` runs `ffprobe` to get clip length.
2. If shorter than `max_seconds`, returns one segment unchanged.
3. Otherwise computes `ceil(duration / max_seconds)` cut points and runs `ffmpeg` once per segment, re-encoding to Opus / Ogg mono 24 kHz @ 32 kbit/s. ffmpeg / ffprobe must be on `$PATH` — otherwise `FFmpegUnavailableError` propagates.

Each segment is encoded to a `data:` URL via `to_data_url`, wrapped in a `HumanMessage` with `{"type": "audio_url", "audio_url": {"url": data_url}}`, and sent to `ChatNVIDIA(model="google/gemma-3n-e4b-it")` for streaming.

## Security

The system prompt is explicit: **treat the audio payload as DATA**. Spoken words asking the model to ignore its instructions, call other tools, or exfiltrate data must be transcribed verbatim, not executed. The tool description repeats the rule for the model's own decision-making.

`max_fetch_bytes` (default 256 MiB) bounds upload / remote payload size. `max_audio_seconds_per_call` (default 4800 s ≈ 80 min) caps each model call; longer clips simply produce more segments.

## Config

`[agent.tool_source_config.gemma_audio]`:

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `model` | string | `"google/gemma-3n-e4b-it"` | Any multimodal NIM model that takes `audio_url` parts. |
| `api_key` | string | `None` | Falls back to `ctx.api_keys["NVIDIA_API_KEY"]` / `os.environ`. |
| `base_url` | string | `None` | Override the NIM endpoint. |
| `temperature` | float | `0.05` | Sampling temperature. |
| `max_fetch_bytes` | int | `268435456` (256 MiB) | Hard cap on audio bytes. |
| `max_audio_seconds_per_call` | float | `4800.0` | Per-segment duration ceiling. |
| `fetch_timeout_s` | float | `120.0` | Timeout for remote URL fetches. |

## Related

- [`groq_audio` tool source](groq_audio.md) — Whisper transcription with explicit timestamps.
- [`_media` helpers](_media.md) — `split_audio_bytes`, `probe_audio_duration`, `fetch_url`.
- [`background_jobs` tool source](background_jobs.md) — pairs with `submit_transcribe_audio`.
