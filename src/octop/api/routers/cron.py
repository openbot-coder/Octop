"""Cron router."""

from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from octop.api.common.agent import assert_agent_access, require_agent_row, user_owns_agent
from octop.api.deps import current_user, get_server
from octop.infra.agents.experts.catalog import resolve_agent_display_task_examples
from octop.infra.cron.exec_script import normalize_script_spec, normalize_script_timeout
from octop.infra.cron.task_type import (
    normalize_cron_task_type,
    require_cron_name,
    require_cron_prompt,
)
from octop.infra.cron.trigger import build_trigger
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.utils.ulid import new_cron_id

router = APIRouter()


class CronCreateBody(BaseModel):
    name: str | None = None
    trigger: str = Field(
        description="Examples: cron:0 9 * * *, interval:3600, date:2030-01-01T08:00:00, or agently:INSTANCE_ID for new mail events."
    )
    prompt: str | None = None
    session_key: str | None = None
    fresh_thread: bool = False
    enabled: bool = True
    model: str | None = None
    task_type: str = "text"
    mcp_servers: list[str] = Field(default_factory=list)
    script: str | None = Field(
        default=None,
        description=(
            "Command line to run before delivery; its stdout becomes the prompt. "
            "Must resolve inside OCTOP_CRON_SCRIPT_ROOTS. Admin only. "
            "Empty output skips the run without contacting the model."
        ),
    )
    script_timeout_s: int | None = Field(
        default=None,
        description="Seconds before the script is killed (1-3600, default 300).",
    )


class CronPatchBody(BaseModel):
    name: str | None = None
    trigger: str | None = Field(
        default=None,
        description="Time schedule or agently:INSTANCE_ID for new mail events.",
    )
    prompt: str | None = None
    session_key: str | None = None
    fresh_thread: bool | None = None
    enabled: bool | None = None
    model: str | None = None
    task_type: str | None = None
    mcp_servers: list[str] | None = None
    script: str | None = None
    script_timeout_s: int | None = None


class CronExamplesResponse(BaseModel):
    """Compat mirror of welcome ``task_examples`` (workspace → catalog → name)."""

    task_examples: dict[str, list[str]] | None = None


def _get_cron_manager(server: Any) -> Any:
    assert server.app_runtime is not None
    return server.app_runtime.cron_manager


def _user_may_manage_agent_cron(*, agent_row: Any, user: Any) -> bool:
    return user_owns_agent(agent_row, user)


def _assert_cron_manage(
    *, agent_id: str, agent_row: Any, user: Any, row: Any | None = None
) -> None:
    if row is not None and row.agent_id != agent_id:
        raise OctopError(ErrorCode.NOT_FOUND, "cron job not found")
    if not _user_may_manage_agent_cron(agent_row=agent_row, user=user):
        raise OctopError(ErrorCode.FORBIDDEN, "cron job not accessible to user")


def _resolve_prompt_and_script(
    *,
    user: Any,
    prompt: str | None,
    script: str | None,
    script_timeout_s: int | None,
) -> tuple[str, str | None, int | None]:
    """Validate the prompt/script pair. Attaching a script is admin-only."""
    if not script:
        text = (prompt or "").strip()
        if not text:
            raise OctopError(
                ErrorCode.CRON_PROMPT_REQUIRED,
                "prompt is required when script is not set",
            )
        return require_cron_prompt(text), None, None
    if not getattr(user, "is_admin", False):
        raise OctopError(ErrorCode.FORBIDDEN, "admin required to schedule a script")
    text = (prompt or "").strip()
    return (
        require_cron_prompt(text) if text else "",
        normalize_script_spec(script),
        normalize_script_timeout(script_timeout_s),
    )


@router.get("/cron/settings", summary="Cron server settings")
async def cron_settings(
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, str]:
    """Return process-level cron configuration (compat; prefer ``GET /settings/timezone``)."""
    return {"timezone": server.services.config.default_timezone}


@router.get(
    "/agents/{agent_id}/cron/examples",
    summary="Task page example prompts",
    response_model=CronExamplesResponse,
)
async def cron_examples(
    agent_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> CronExamplesResponse:
    """Same resolution as ``GET …/chat/welcome`` ``task_examples`` (API compat)."""
    assert_agent_access(server, agent_id, user)
    assert server.app_runtime is not None
    registry = server.app_runtime.agent_registry
    examples = await resolve_agent_display_task_examples(
        workspace=registry.workspace_for_agent(agent_id),
        row=registry.get_row(agent_id),
        catalog=getattr(server, "expert_catalog", None),
    )
    return CronExamplesResponse(task_examples=examples)


@router.get("/agents/{agent_id}/cron", summary="List cron jobs")
async def list_cron(
    agent_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> list[dict[str, Any]]:
    """List scheduled jobs for an agent."""
    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    if not _user_may_manage_agent_cron(agent_row=agent_row, user=user):
        return []
    return [
        r.to_public_dict(include_agent=True)
        for r in _get_cron_manager(server).list_by_agent(agent_id)
    ]


@router.post("/agents/{agent_id}/cron", status_code=201, summary="Create cron job")
async def create_cron(
    agent_id: str,
    body: CronCreateBody,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Schedule a prompt on a timer or on new mail from a visible Agent Mail instance."""
    from octop.api.common.validators import validate_chat_mcp_servers  # noqa: PLC0415
    from octop.infra.cron.manager import CronCreateSpec  # noqa: PLC0415

    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    _assert_cron_manage(agent_id=agent_id, agent_row=agent_row, user=user)
    prompt, script, script_timeout_s = _resolve_prompt_and_script(
        user=user,
        prompt=body.prompt,
        script=body.script,
        script_timeout_s=body.script_timeout_s,
    )
    cron_id = new_cron_id()
    mcp_servers = (
        await validate_chat_mcp_servers(server, user_id=user.id, names=body.mcp_servers) or []
    )
    spec = CronCreateSpec(
        cron_id=cron_id,
        agent_id=agent_id,
        user_id=user.id,
        name=require_cron_name(body.name, prompt=prompt or script or "", cron_id=cron_id),
        trigger=body.trigger,
        prompt=prompt,
        script=script,
        script_timeout_s=script_timeout_s,
        session_key=body.session_key,
        fresh_thread=body.fresh_thread,
        enabled=body.enabled,
        model=(body.model or "").strip() or None,
        task_type=normalize_cron_task_type(body.task_type),
        mcp_servers=mcp_servers,
        username=user.username,
    )
    row = await _get_cron_manager(server).create(spec)
    return cast(dict[str, Any], row.to_public_dict(include_agent=True))


@router.get("/agents/{agent_id}/cron/{cron_id}", summary="Get cron job")
async def get_cron(
    agent_id: str,
    cron_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Return one scheduled job by id."""
    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    row = _get_cron_manager(server).get(cron_id)
    if row is None:
        raise OctopError(ErrorCode.NOT_FOUND, "cron job not found")
    _assert_cron_manage(agent_id=agent_id, agent_row=agent_row, user=user, row=row)
    return cast(dict[str, Any], row.to_public_dict(include_agent=True))


@router.patch("/agents/{agent_id}/cron/{cron_id}", summary="Update cron job")
async def patch_cron(
    agent_id: str,
    cron_id: str,
    body: CronPatchBody,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> dict[str, Any]:
    """Update trigger, prompt, session binding, or enabled flag."""
    from octop.api.common.validators import validate_chat_mcp_servers  # noqa: PLC0415
    from octop.infra.db.repos._base import UNSET  # noqa: PLC0415

    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    mgr = _get_cron_manager(server)
    existing = mgr.get(cron_id)
    if existing is None:
        raise OctopError(ErrorCode.NOT_FOUND, "cron job not found")
    _assert_cron_manage(agent_id=agent_id, agent_row=agent_row, user=user, row=existing)
    if body.trigger is not None:
        build_trigger(body.trigger)
    patch_fields = body.model_dump(exclude_unset=True)
    prompt = require_cron_prompt(body.prompt) if body.prompt is not None else None
    mcp_arg: object = UNSET
    if "mcp_servers" in patch_fields:
        mcp_arg = await validate_chat_mcp_servers(
            server, user_id=user.id, names=body.mcp_servers or []
        )
    script_arg: object = UNSET
    timeout_arg: object = UNSET
    if "script" in patch_fields:
        if body.script:
            if not getattr(user, "is_admin", False):
                raise OctopError(ErrorCode.FORBIDDEN, "admin required to schedule a script")
            script_arg = normalize_script_spec(body.script)
        else:
            script_arg = None
            timeout_arg = None
    if "script_timeout_s" in patch_fields:
        timeout_arg = normalize_script_timeout(body.script_timeout_s)
    row = await mgr.update(
        cron_id,
        trigger=body.trigger,
        name=(
            require_cron_name(
                body.name,
                prompt=prompt or existing.prompt or existing.script or "",
                cron_id=cron_id,
            )
            if "name" in patch_fields
            else None
        ),
        prompt=prompt,
        session_key=body.session_key,
        fresh_thread=body.fresh_thread,
        enabled=int(body.enabled) if body.enabled is not None else None,
        task_type=normalize_cron_task_type(body.task_type) if body.task_type is not None else None,
        **({"model": (body.model or "").strip() or None} if "model" in patch_fields else {}),
        mcp_servers=mcp_arg,
        script=script_arg,
        script_timeout_s=timeout_arg,
    )
    return cast(dict[str, Any], row.to_public_dict(include_agent=True))


@router.delete("/agents/{agent_id}/cron/{cron_id}", status_code=204, summary="Delete cron job")
async def delete_cron(
    agent_id: str,
    cron_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> None:
    """Remove a scheduled job."""
    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    mgr = _get_cron_manager(server)
    existing = mgr.get(cron_id)
    if existing is None:
        raise OctopError(ErrorCode.NOT_FOUND, "cron job not found")
    _assert_cron_manage(agent_id=agent_id, agent_row=agent_row, user=user, row=existing)
    await mgr.delete(cron_id)


@router.post("/agents/{agent_id}/cron/{cron_id}/run-now", status_code=204, summary="Run cron now")
async def run_now(
    agent_id: str,
    cron_id: str,
    user: Any = Depends(current_user),
    server: Any = Depends(get_server),
) -> None:
    """Trigger an immediate one-off run without waiting for the schedule."""
    agent_row = require_agent_row(agent_id, user=user, as_user=None, server=server)
    mgr = _get_cron_manager(server)
    existing = mgr.get(cron_id)
    if existing is None:
        raise OctopError(ErrorCode.NOT_FOUND, "cron job not found")
    _assert_cron_manage(agent_id=agent_id, agent_row=agent_row, user=user, row=existing)
    await mgr.run_now(cron_id)
