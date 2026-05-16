# `openbb_agent_server.plugins.tools.background_jobs`

Poll, wait on, or cancel long-running jobs fanned out by `submit_*` tools (vision, audio, etc.). Backed by the per-process [`runtime/jobs.py`](../../runtime/jobs.md) registry. Pair this source with any tool that exposes a `submit_<name>` variant — without `background_jobs` bound, the model has no way to retrieve the result.

**Source:** [`openbb_agent_server/plugins/tools/background_jobs.py`](../../../../openbb_agent_server/plugins/tools/background_jobs.py)

## Classes

### `BackgroundJobsToolSource`

Plugin entry-point name: `background_jobs`. No constructor arguments. `tools(ctx, config)` registers four `StructuredTool`s.

| Tool | Args | Returns |
| --- | --- | --- |
| `list_background_jobs` | — | `list[{job_id, label, state, started_at, finished_at}]`. `state` is one of `running` / `done` / `error` / `canceled`. Emits one `reasoning_step` with the count. |
| `check_job` | `job_id: str` (id returned by a previous `submit_*` call) | Same shape as a `list_background_jobs` entry. Non-blocking. Unknown ids raise `ValueError`. |
| `wait_for_job` | `job_id: str`, `timeout_s: float = 300.0` (∈ [1.0, 3600.0]) | Final state + `result` (whatever the underlying tool would have returned synchronously). On timeout, `state` is `TIMEOUT` — the job keeps running; call again to wait longer. Emits one `reasoning_step` with `job_id` and `state`. |
| `cancel_job` | `job_id: str` | `{job_id, cancelled}` — `cancelled` is `True` iff the task was alive at the call time. Already-finished jobs return `False` and are unaffected. Emits one `reasoning_step` with `job_id` and `cancelled`. |

### Behaviour

- All four tools share the same `JobRegistry` (`get_registry()`). The registry is process-global; jobs do not persist across server restarts.
- `wait_for_job` is the only `coroutine=`-registered tool — it awaits a `Future` internally rather than spin-polling.
- `TIMEOUT` is not an error: a `wait_for_job` that times out returns a result dict the model can inspect and then re-invoke `wait_for_job` with a fresh budget.
- `KeyError` from the registry (unknown id) is rewrapped as `ValueError` so the agent sees a clean error string.

### Lifecycle

A typical fan-out turn looks like:

1. Model calls `submit_understand_image` (or any other `submit_*` tool) → gets `{job_id, label}`.
2. Model continues with other work.
3. Model calls `check_job(job_id)` to poll, or `wait_for_job(job_id)` to block.
4. On completion (`state == "done"`), the `result` field carries the underlying tool's return value.
5. Optional: model calls `cancel_job(job_id)` to abort a still-running job.

## Config

`[agent.tool_source_config.background_jobs]` is currently empty — the tools depend only on the per-process job registry.

## Related

- [`runtime/jobs.py`](../../runtime/jobs.md) — the `JobRegistry` implementation.
- [`vision_qa`](vision_qa.md), [`paligemma_vision`](paligemma_vision.md), [`gemma_audio`](gemma_audio.md) — every tool source that exposes a `submit_*` variant.
