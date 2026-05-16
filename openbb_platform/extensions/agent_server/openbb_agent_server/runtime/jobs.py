"""Per-run background-task registry."""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from openbb_agent_server.runtime.context import runtime_state

logger = logging.getLogger("openbb_agent_server.jobs")


_STATE_KEY = "_background_jobs"


class JobState(str, Enum):
    """Lifecycle states for a background job."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"
    CANCELED = "canceled"


WAIT_TIMEOUT = "TIMEOUT"


@dataclass
class _Job:
    id: str
    label: str
    state: JobState
    started_at: float
    task: asyncio.Task[Any] | None = None
    finished_at: float | None = None
    result: Any = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class JobRegistry:
    """Per-run asyncio-task registry."""

    def __init__(self) -> None:
        self._jobs: dict[str, _Job] = {}

    def submit(
        self,
        target: Any,
        *,
        label: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Schedule ``target`` as a background task and return its job id."""
        coro_obj: Awaitable[Any] | None
        factory: Callable[[], Awaitable[Any]] | None
        if asyncio.iscoroutine(target):
            coro_obj = target
            factory = None
        elif callable(target):
            coro_obj = None
            factory = target
        else:
            raise TypeError(
                "JobRegistry.submit: target must be a coroutine OR a "
                f"zero-arg callable returning one; got {type(target).__name__}"
            )

        job_id = secrets.token_urlsafe(12)
        job = _Job(
            id=job_id,
            label=label,
            state=JobState.RUNNING,
            started_at=time.time(),
            metadata=dict(metadata or {}),
        )
        job.task = asyncio.create_task(
            self._run(job, coro_obj, factory),
            name=f"job:{label}:{job_id}",
        )
        self._jobs[job_id] = job
        logger.debug("job submitted id=%s label=%s", job_id, label)
        return job_id

    async def _run(
        self,
        job: _Job,
        coro: Awaitable[Any] | None,
        factory: Callable[[], Awaitable[Any]] | None,
    ) -> None:
        if coro is None and factory is not None:
            coro = factory()
        if coro is None:  # pragma: no cover - submit() validates upstream
            job.state = JobState.ERROR
            job.error = "submit() received neither coroutine nor factory"
            job.finished_at = time.time()
            return
        try:
            job.result = await coro
        except asyncio.CancelledError:
            if asyncio.iscoroutine(coro):
                coro.close()
            job.state = JobState.CANCELED
            if job.finished_at is None:  # pragma: no cover - cancel() sets it first
                job.finished_at = time.time()
            logger.debug("job cancelled id=%s", job.id)
            raise
        except Exception as exc:  # noqa: BLE001 — recorded onto the job
            job.state = JobState.ERROR
            job.error = f"{type(exc).__name__}: {exc}"
            job.finished_at = time.time()
            logger.warning(
                "job failed id=%s label=%s error=%s",
                job.id,
                job.label,
                job.error,
            )
            return
        job.state = JobState.DONE
        job.finished_at = time.time()
        logger.debug("job finished id=%s", job.id)

    def status(self, job_id: str) -> dict[str, Any]:
        """Return a non-blocking snapshot of one job."""
        return self._snapshot(self._require(job_id), include_result=False)

    def list_all(self) -> list[dict[str, Any]]:
        """Return all registered jobs, oldest-first."""
        return [
            self._snapshot(j, include_result=False)
            for j in sorted(self._jobs.values(), key=lambda j: j.started_at)
        ]

    async def wait(
        self, job_id: str, *, timeout_s: float | None = None
    ) -> dict[str, Any]:
        """Await a job to completion or ``timeout_s``."""
        job = self._require(job_id)
        if job.state in (JobState.DONE, JobState.ERROR, JobState.CANCELED):
            return self._snapshot(job, include_result=True)
        assert job.task is not None  # noqa: S101
        try:
            await asyncio.wait_for(asyncio.shield(job.task), timeout=timeout_s)
        except asyncio.TimeoutError:
            return {
                **self._snapshot(job, include_result=False),
                "state": WAIT_TIMEOUT,
            }
        return self._snapshot(job, include_result=True)

    def cancel(self, job_id: str) -> bool:
        """Request cancellation. Returns ``True`` iff cancellable."""
        job = self._jobs.get(job_id)
        if job is None or job.state in (
            JobState.DONE,
            JobState.ERROR,
            JobState.CANCELED,
        ):
            return False
        assert job.task is not None  # noqa: S101
        cancelled = job.task.cancel()
        if cancelled:
            job.state = JobState.CANCELED
            if job.finished_at is None:
                job.finished_at = time.time()
        return cancelled

    def cancel_all(self) -> int:
        """Cancel every still-running task. Returns count cancelled."""
        n = 0
        now = time.time()
        for job in self._jobs.values():
            if (
                job.state == JobState.RUNNING
                and job.task is not None
                and job.task.cancel()
            ):
                job.state = JobState.CANCELED
                if job.finished_at is None:
                    job.finished_at = now
                n += 1
        if n:
            logger.debug("cancel_all cancelled %d background jobs", n)
        return n

    def _require(self, job_id: str) -> _Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"no background job {job_id!r} in this run")
        return job

    def _snapshot(self, job: _Job, *, include_result: bool) -> dict[str, Any]:
        out: dict[str, Any] = {
            "job_id": job.id,
            "label": job.label,
            "state": job.state.value,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
        }
        if job.metadata:
            out["metadata"] = dict(job.metadata)
        if job.error:
            out["error"] = job.error
        if include_result and job.state == JobState.DONE:
            out["result"] = job.result
        return out


def get_registry() -> JobRegistry:
    """Return the :class:`JobRegistry` for the current run, creating one if absent."""
    state = runtime_state()
    reg = state.get(_STATE_KEY)
    if reg is None:
        reg = JobRegistry()
        state[_STATE_KEY] = reg
    return reg


def cleanup_state(state: dict[str, Any]) -> int:
    """Cancel every background job in ``state``."""
    reg = state.get(_STATE_KEY)
    if reg is None:
        return 0
    return reg.cancel_all()


__all__ = [
    "JobRegistry",
    "JobState",
    "WAIT_TIMEOUT",
    "cleanup_state",
    "get_registry",
]
