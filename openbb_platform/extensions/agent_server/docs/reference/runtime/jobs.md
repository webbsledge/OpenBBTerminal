# `openbb_agent_server.runtime.jobs`

Per-run background-task registry. Tools that kick off long work (PDF parsing, audio transcription, expensive search) submit it here and return a `job_id`; later tool calls poll status, wait on the job, or cancel. State is per-run — every job is automatically cancelled when the run ends.

**Source:** [`openbb_agent_server/runtime/jobs.py`](../../../openbb_agent_server/runtime/jobs.py)

## `class JobState(str, Enum)`

| Value | Meaning |
| --- | --- |
| `pending` | Submitted but not yet started (currently unused — `submit()` moves straight to `running`). |
| `running` | Task is in flight. |
| `done` | Awaited successfully; `result` is populated. |
| `error` | Raised; `error` carries `"<TypeName>: <message>"`. |
| `canceled` | Cancelled via `cancel()` / `cancel_all()` / run-end cleanup. |

`WAIT_TIMEOUT = "TIMEOUT"` is a sentinel state returned by `wait()` when the timeout expires without the job completing — it's NOT a member of `JobState` so callers know it's a transient response, not the terminal state.

## `class JobRegistry`

Single per-run registry. Bound on the runtime-state dict under `_background_jobs` — `get_registry()` lazily creates one on first access.

### `def submit(target, *, label, metadata=None) -> str`

Schedule `target` and return its job id. `target` may be a coroutine OR a zero-arg callable returning one; the wrapper handles both. Returns the `job_id` (12-byte URL-safe token).

| Arg | Purpose |
| --- | --- |
| `target` | `Awaitable[Any]` or `Callable[[], Awaitable[Any]]`. Other types raise `TypeError`. |
| `label` | Human-readable name surfaced in `status()` / `list_all()` (e.g. `"transcribe:recording.mp3"`). |
| `metadata` | Optional dict copied onto the job; surfaced via snapshots. |

The created `asyncio.Task` is named `f"job:{label}:{job_id}"` for log identification.

### `def status(job_id) -> dict[str, Any]`

Non-blocking snapshot of one job. Includes `job_id`, `label`, `state`, `started_at`, `finished_at`, optional `metadata`, optional `error`. Does NOT include `result` — use `wait()` for terminal results.

### `def list_all() -> list[dict[str, Any]]`

Snapshots of every registered job, oldest-first. No `result` in the snapshots.

### `async def wait(job_id, *, timeout_s=None) -> dict[str, Any]`

Await the job to completion (or `timeout_s`). On timeout returns the snapshot with `state = "TIMEOUT"`; on success / error / cancel returns the snapshot with `result` populated (only on `done`).

Internally uses `asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)` so a timeout on the awaiter does NOT cancel the underlying task — other callers can still wait on the same job.

### `def cancel(job_id) -> bool`

Request cancellation. Returns `False` if the job already terminated. Returns `True` iff `task.cancel()` returned `True` (which means the task accepted the cancel — async cancellation is cooperative).

### `def cancel_all() -> int`

Cancel every still-running task. Returns the count cancelled. Called by `cleanup_state(state)` when the run ends.

## Module helpers

### `def get_registry() -> JobRegistry`

Return the per-run registry, creating one in `runtime_state()` on first call. Plugins use this directly: `get_registry().submit(coro, label=…)`.

### `def cleanup_state(state) -> int`

Cancel every background job stored under `_STATE_KEY` in `state`. The router calls this in the `finally` block of every run so an aborted exchange doesn't leak background tasks.

## Lifecycle

The registry lives on `runtime_state()` (per-run scratch dict). When `runtime.context.bind(ctx)` exits, the surrounding context manager calls `cleanup_state(state)`, which cancels every still-running job. Submitting a job that needs to outlive the run is intentionally NOT supported — outliving-the-run work belongs in a separate process (or a checkpointer + pending-run flow).

## See also

- [`runtime/context.md`](context.md) — owner of the scratch dict.
- [`plugins/tools/background_jobs.md`](../plugins/tools/background_jobs.md) — agent-callable surface that consumes this.
- [`guides/background-jobs.md`](../../guides/background-jobs.md) — guide.
