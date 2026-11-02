"""CronJob — APScheduler callable with status and audit bookkeeping."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from octop.infra.cron.delivery import CronDeliveryCommand
from octop.infra.cron.exec_script import run_script
from octop.infra.cron.task_type import normalize_cron_task_type
from octop.infra.db.repos.audit import ACTOR_SYSTEM

if TYPE_CHECKING:
    from octop.infra.cron.delivery import CronDeliveryService
    from octop.infra.db.repos.audit import AuditRepo
    from octop.infra.db.repos.cron import CronJobRepo, CronJobRow

logger = logging.getLogger(__name__)


class CronJob:
    def __init__(
        self,
        *,
        cron_id: str,
        name: str,
        agent_id: str,
        prompt: str,
        fresh_thread: bool,
        session_key: str,
        model: str | None,
        task_type: str,
        mcp_servers: list[str] | None,
        user_id: int,
        delivery_service: CronDeliveryService,
        cron_repo: CronJobRepo,
        audit_repo: AuditRepo,
        script: str | None = None,
        script_timeout_s: int | None = None,
    ) -> None:
        self._cron_id = cron_id
        self._name = name
        self._agent_id = agent_id
        self._prompt = prompt
        self._fresh_thread = fresh_thread
        self._session_key = session_key
        self._model = model
        self._task_type = normalize_cron_task_type(task_type)
        self._mcp_servers = list(mcp_servers or [])
        self._user_id = user_id
        self._delivery_service = delivery_service
        self._cron_repo = cron_repo
        self._audit_repo = audit_repo
        self._script = script
        self._script_timeout_s = script_timeout_s

    @classmethod
    def from_row(
        cls,
        row: CronJobRow,
        *,
        delivery_service: CronDeliveryService,
        cron_repo: CronJobRepo,
        audit_repo: AuditRepo,
    ) -> CronJob:
        return cls(
            cron_id=row.cron_id,
            name=row.name,
            agent_id=row.agent_id,
            prompt=row.prompt,
            fresh_thread=bool(row.fresh_thread),
            session_key=row.session_key,
            model=row.model,
            task_type=row.task_type,
            mcp_servers=list(row.mcp_servers),
            user_id=row.user_id,
            delivery_service=delivery_service,
            cron_repo=cron_repo,
            audit_repo=audit_repo,
            script=row.script,
            script_timeout_s=row.script_timeout_s,
        )

    def _record_failure(self, *, ts: int, error: str) -> None:
        from octop.infra.metrics import METRICS  # noqa: PLC0415

        METRICS.inc("cron_errors_total")
        self._cron_repo.set_run_status(self._cron_id, ts=ts, status="error", error=error)
        self._audit_repo.write(
            actor=ACTOR_SYSTEM,
            action="cron.run_failed",
            target=self._cron_id,
            payload=error,
        )

    async def _prompt_from_script(self, *, ts: int, raise_on_error: bool) -> str | None:
        """Return the prompt to deliver, or ``None`` when the run stops before delivery.

        A script replaces the stored prompt with its stdout. Empty output means
        "nothing to say": the run is recorded as ``skipped`` and nothing is
        delivered, so it costs neither tokens nor a conversation turn.
        """
        if not self._script:
            return self._prompt

        from octop.infra.metrics import METRICS  # noqa: PLC0415

        try:
            outcome = await run_script(self._script, timeout_s=self._script_timeout_s)
        except Exception as exc:
            logger.exception("cron job %s script failed", self._cron_id)
            self._record_failure(ts=ts, error=str(exc))
            if raise_on_error:
                raise
            return None

        if not outcome.ok:
            reason = "timeout" if outcome.timed_out else f"exit={outcome.exit_code}"
            if outcome.stderr_tail:
                reason = f"{reason}: {outcome.stderr_tail}"
            logger.warning("cron job %s script failed: %s", self._cron_id, reason)
            self._record_failure(ts=ts, error=reason)
            if raise_on_error:
                raise RuntimeError(reason)
            return None

        text = outcome.stdout.strip()
        if not text:
            METRICS.inc("cron_skipped_total")
            self._cron_repo.set_run_status(self._cron_id, ts=ts, status="skipped", error=None)
            self._audit_repo.write(
                actor=ACTOR_SYSTEM,
                action="cron.run_skipped",
                target=self._cron_id,
                payload="script produced no output",
            )
            logger.info("cron job %s skipped: script produced no output", self._cron_id)
            return None
        return text

    async def run(self, *, raise_on_error: bool = False) -> None:
        from octop.infra.metrics import METRICS  # noqa: PLC0415

        METRICS.inc("cron_runs_total")
        ts = int(time.time())

        prompt = await self._prompt_from_script(ts=ts, raise_on_error=raise_on_error)
        if prompt is None:
            return

        try:
            await self._delivery_service.deliver(
                CronDeliveryCommand(
                    cron_id=self._cron_id,
                    cron_name=self._name,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                    session_key=self._session_key,
                    prompt=prompt,
                    fresh_thread=self._fresh_thread,
                    task_type=self._task_type,
                    model=self._model,
                    mcp_servers=tuple(self._mcp_servers),
                )
            )
        except Exception as exc:
            METRICS.inc("cron_errors_total")
            logger.exception("cron job %s failed", self._cron_id)
            err = str(exc)
            self._cron_repo.set_run_status(self._cron_id, ts=ts, status="error", error=err)
            self._audit_repo.write(
                actor=ACTOR_SYSTEM,
                action="cron.run_failed",
                target=self._cron_id,
                payload=err,
            )
            if raise_on_error:
                raise
            return
        self._cron_repo.set_run_status(self._cron_id, ts=ts, status="ok", error=None)
        self._audit_repo.write(
            actor=ACTOR_SYSTEM,
            action="cron.run_ok",
            target=self._cron_id,
        )
