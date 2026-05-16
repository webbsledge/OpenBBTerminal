# `openbb_agent_server.plugins.models.groq_rate_limiter`

Process-shared multi-dimensional rate limiter implementing Groq's published quotas. Each `(api_key, model_name)` pair gets a single `GroqRateLimiter` instance backed by independent token-bucket counters for requests-per-minute (RPM), requests-per-day (RPD), tokens-per-minute (TPM), tokens-per-day (TPD), audio-seconds-per-hour, and audio-seconds-per-day. `acquire` / `aacquire` block until **every** active bucket has at least one unit available, so the next request only fires once every dimension is in-quota. A `BaseCallbackHandler` feeds post-call token usage back into the TPM / TPD buckets so per-request token costs are accounted for.

**Source:** [`openbb_agent_server/plugins/models/groq_rate_limiter.py`](../../../../openbb_agent_server/plugins/models/groq_rate_limiter.py)

## Classes

### `GroqLimits`

Frozen dataclass — the per-model quota tuple. `None` for any field means "no published cap" and the corresponding bucket is **not constructed** at all (so it never blocks).

| Field | Type | Period | Notes |
| --- | --- | --- | --- |
| `rpm` | `int \| None` | 60 s | Requests-per-minute. Falls back to `30` inside `from_limits` if `None` so `GroqRateLimiter.__init__` (which requires `rpm > 0`) does not reject. |
| `rpd` | `int \| None` | 86 400 s | Requests-per-day. |
| `tpm` | `int \| None` | 60 s | Tokens-per-minute. |
| `tpd` | `int \| None` | 86 400 s | Tokens-per-day. |
| `audio_per_hour` | `int \| None` | 3 600 s | Whisper input audio-seconds per hour. |
| `audio_per_day` | `int \| None` | 86 400 s | Whisper input audio-seconds per day. |

### Published-limits table

`GROQ_LIMITS` is a module-level `dict[str, GroqLimits]` pulled from Groq's published rate-limits page. Unknown models fall back to `_DEFAULT_LIMITS = GroqLimits(rpm=30, rpd=1_000, tpm=6_000, tpd=100_000)`.

| Model | RPM | RPD | TPM | TPD | Audio/h | Audio/d |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `llama-3.1-8b-instant` | 30 | 14 400 | 6 000 | 500 000 | — | — |
| `llama-3.3-70b-versatile` | 30 | 1 000 | 12 000 | 100 000 | — | — |
| `meta-llama/llama-4-scout-17b-16e-instruct` | 30 | 1 000 | 30 000 | 500 000 | — | — |
| `moonshotai/kimi-k2-instruct` | 60 | 1 000 | 10 000 | 300 000 | — | — |
| `qwen/qwen3-32b` | 60 | 1 000 | 6 000 | 500 000 | — | — |
| `openai/gpt-oss-120b` | 30 | 1 000 | 8 000 | 200 000 | — | — |
| `openai/gpt-oss-20b` | 30 | 1 000 | 8 000 | 200 000 | — | — |
| `groq/compound` | 30 | 250 | 70 000 | — | — | — |
| `groq/compound-mini` | 30 | 250 | 70 000 | — | — | — |
| `allam-2-7b` | 30 | 7 000 | 6 000 | 500 000 | — | — |
| `whisper-large-v3` | 20 | 2 000 | — | — | 7 200 | 28 800 |
| `whisper-large-v3-turbo` | 20 | 2 000 | — | — | 7 200 | 28 800 |

### `_Bucket` (internal)

Token-bucket counter that refills continuously at `capacity / period_seconds` per second up to `capacity`. `refill()` recomputes available tokens from `time.monotonic()` deltas; `consume(n)` refills then subtracts (negative `available` is permitted — the wait time is computed from the deficit); `time_until_at_least_one()` returns the seconds until `available >= 1`.

### `GroqRateLimiter`

Subclass of `langchain_core.rate_limiters.BaseRateLimiter`. Implements `acquire` / `aacquire` plus token / audio recording.

`__init__(*, rpm, rpd, tpm, tpd, audio_per_hour, audio_per_day, check_every_n_seconds=0.1)` raises `ValueError` if `rpm <= 0`. Only the fields that are non-`None` materialise as `_Bucket` instances; everything else is `None` and skipped during wait computation.

| Method | Purpose |
| --- | --- |
| `from_limits(limits, **overrides) -> GroqRateLimiter` | Classmethod constructor from a `GroqLimits` dataclass. `overrides` win — e.g. `from_limits(limits, rpm=600)`. |
| `acquire(*, blocking=True) -> bool` | Sync: loops until every active bucket reports `time_until_at_least_one() <= 0`, then consumes one request from RPM and (if present) RPD. Returns `True` on consume; with `blocking=False`, returns `False` immediately when waiting would be required. Sleeps in slices bounded by `check_every_n_seconds`. |
| `aacquire(*, blocking=True) -> bool` | Async sibling. Uses `asyncio.Lock` around the sync `threading.Lock` so concurrent coroutines coordinate cleanly. |
| `record_tokens(n)` | Consume `n` from TPM and TPD (when those buckets exist). No-op for `n <= 0`. |
| `record_audio_seconds(seconds)` | Consume `seconds` from `audio_per_hour` / `audio_per_day` (when present). No-op for `seconds <= 0`. |
| `callback_handler -> BaseCallbackHandler` | Returns a fresh `_GroqUsageHandler` bound to this limiter. The handler reads `LLMResult.llm_output["token_usage"]["total_tokens"]` (or falls back to `gen.message.usage_metadata["total_tokens"]`) on `on_llm_end` and feeds the count into `record_tokens`. `raise_error = False` — callback failures are swallowed by langchain. |
| `snapshot() -> dict` | Refreshes every bucket and returns the current `available` count for each as `{rpm_remaining, rpd_remaining, tpm_remaining, tpd_remaining, audio_seconds_per_hour_remaining, audio_seconds_per_day_remaining}`. Inactive buckets read `None`. Useful for `/metrics`-style introspection. |

## Functions

### `get_limiter(*, api_key, model_name) -> GroqRateLimiter`

Process-shared cache lookup. Returns the existing limiter for `(api_key, model_name)` or constructs a new one from `GROQ_LIMITS[model_name]` (or `_DEFAULT_LIMITS` when the model is unknown) via `GroqRateLimiter.from_limits(...)`. Thread-safe via `_CACHE_LOCK`. This is what `GroqProvider.build` calls when no `rate_limit` override is set.

### `reset_cache() -> None`

Drop every cached limiter — exposed for tests that need clean state between runs. Production code should not call this; the live runtime depends on long-lived cached buckets to enforce daily quotas across many requests.

## Sharing semantics

- Cache key is `(api_key, model_name)`. Different keys → independent quotas (one tenant cannot exhaust another's RPM). Different models on the same key → independent quotas (Groq publishes per-model limits).
- All concurrent runs in the same process hitting the same `(api_key, model_name)` contend on the same bucket set — that is intentional, since Groq enforces per-key per-model quotas server-side.
- Across processes (multi-worker uvicorn, scaled-out deployments) limiters are not shared; each worker enforces its own copy of the published quotas. With `N` workers the effective rate is `N × bucket_rate`. Configure the agent server to a single worker per Groq key, or accept the multiplier and lean on Groq's server-side 429s plus `max_retries`.

## Notes

- `_compute_wait` returns `max()` of every active bucket's wait — the slowest bucket dictates pacing. Without that, satisfying RPM alone could still 429 you on TPM.
- `_consume_request` does *not* preemptively consume tokens; the request bucket spends immediately on `acquire`, and the token buckets are reconciled afterwards by the callback handler. This means a single oversized request can drive TPM negative — subsequent calls will wait long enough to amortise the deficit.
- The audio buckets are only meaningful for the Whisper STT routes. Chat runs never touch them (`record_audio_seconds` is unused from the chat path) so they sit at capacity.
- Resilience to clock changes: `_Bucket` uses `time.monotonic()`, so NTP adjustments and DST transitions do not corrupt the available-token math.

See also: [`groq_provider`](groq_provider.md), [`writing-a-model-provider.md`](../../../developing/writing-a-model-provider.md), [`../../runtime/plugins.md`](../../runtime/plugins.md).
