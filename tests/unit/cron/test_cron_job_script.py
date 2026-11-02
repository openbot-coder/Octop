"""tests/unit/cron/test_cron_job_script.py — CronJob's script stage and its status bookkeeping."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from octop.infra.cron.job import CronJob
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.audit import AuditRepo
from octop.infra.db.repos.cron import CronJobRepo
from octop.infra.db.repos.sessions import SessionRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.gateway.threads import ThreadRegistry


@pytest.fixture
def setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    monkeypatch.setenv("OCTOP_CRON_SCRIPT_ROOTS", str(scripts))

    db = SqlitePool(tmp_path / "x.db")
    run_migrations(db)
    UserRepo(db).create(username="u", password_hash="h", role="user")
    AgentRepo(db).create(agent_id="a1", user_id=1, name="bot")
    cron_repo = CronJobRepo(db)
    session_key = ThreadRegistry.make_key(
        agent_id="a1",
        channel_type="cron",
        channel_subject_id="j1",
        channel_chat_type=ThreadRegistry.CHAT_TYPE_DM,
    )
    SessionRepo(db).upsert(
        session_key=session_key,
        agent_id="a1",
        user_id=1,
        channel_type="cron",
        chat_type=ThreadRegistry.CHAT_TYPE_DM,
        thread_id="thr_seed",
        channel_subject_id="j1",
        channel_chat_type=ThreadRegistry.CHAT_TYPE_DM,
        channel_metadata={"channel_type": "cron", "user_id": 1},
    )
    cid = cron_repo.create(
        cron_id="j1",
        agent_id="a1",
        user_id=1,
        trigger="interval:60",
        prompt="say hi",
        session_key=session_key,
    )
    return cron_repo, AuditRepo(db), cid, session_key, scripts


def _write(scripts: Path, name: str, body: str) -> Path:
    path = scripts / name
    path.write_text(body, encoding="utf-8")
    return path


def _job(
    *,
    setup,
    delivery_service: MagicMock,
    script: str | None = None,
    script_timeout_s: int | None = None,
    task_type: str = "agent",
    prompt: str = "hi",
) -> CronJob:
    cron_repo, audit, cid, session_key, _ = setup
    return CronJob(
        cron_id=cid,
        name="say hi",
        agent_id="a1",
        prompt=prompt,
        fresh_thread=False,
        session_key=session_key,
        model=None,
        task_type=task_type,
        mcp_servers=None,
        user_id=1,
        delivery_service=delivery_service,
        cron_repo=cron_repo,
        audit_repo=audit,
        script=script,
        script_timeout_s=script_timeout_s,
    )


@pytest.mark.asyncio
async def test_no_script_keeps_the_stored_prompt(setup) -> None:
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    await _job(setup=setup, delivery_service=delivery).run()

    assert delivery.deliver.await_args.args[0].prompt == "hi"


@pytest.mark.asyncio
async def test_script_stdout_replaces_prompt(setup) -> None:
    cron_repo, _, cid, _, scripts = setup
    _write(scripts, "daily.py", "print('from the script')")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "daily.py"))
    await job.run()

    assert delivery.deliver.await_args.args[0].prompt == "from the script"
    assert cron_repo.get(cid).last_status == "ok"


@pytest.mark.asyncio
async def test_script_plus_text_task_delivers_stdout_without_the_model(setup) -> None:
    _, _, _, _, scripts = setup
    _write(scripts, "quote.py", "print('600519 up 2%')")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(
        setup=setup,
        delivery_service=delivery,
        script=str(scripts / "quote.py"),
        task_type="text",
    )
    await job.run()

    command = delivery.deliver.await_args.args[0]
    assert command.task_type == "text"
    assert command.prompt == "600519 up 2%"


@pytest.mark.asyncio
async def test_empty_script_output_skips_the_run(setup) -> None:
    cron_repo, audit, cid, _, scripts = setup
    _write(scripts, "silent.py", "pass")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "silent.py"))
    await job.run()

    delivery.deliver.assert_not_awaited()
    row = cron_repo.get(cid)
    assert row.last_status == "skipped"
    assert row.last_error is None
    assert audit.query(action="cron.run_skipped")


@pytest.mark.asyncio
async def test_whitespace_only_output_skips_the_run(setup) -> None:
    cron_repo, _, cid, _, scripts = setup
    _write(scripts, "blank.py", "print('   \\n\\t ')")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "blank.py"))
    await job.run()

    delivery.deliver.assert_not_awaited()
    assert cron_repo.get(cid).last_status == "skipped"


@pytest.mark.asyncio
async def test_non_zero_exit_records_error_without_delivery(setup) -> None:
    cron_repo, audit, cid, _, scripts = setup
    _write(scripts, "boom.py", "import sys\nsys.stderr.write('kaboom')\nsys.exit(2)")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "boom.py"))
    await job.run()

    delivery.deliver.assert_not_awaited()
    row = cron_repo.get(cid)
    assert row.last_status == "error"
    assert "exit=2" in (row.last_error or "")
    assert "kaboom" in (row.last_error or "")
    assert audit.query(action="cron.run_failed")


@pytest.mark.asyncio
async def test_timeout_records_error(setup) -> None:
    cron_repo, _, cid, _, scripts = setup
    _write(scripts, "slow.py", "import time\ntime.sleep(30)")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(
        setup=setup,
        delivery_service=delivery,
        script=str(scripts / "slow.py"),
        script_timeout_s=1,
    )
    await job.run()

    delivery.deliver.assert_not_awaited()
    row = cron_repo.get(cid)
    assert row.last_status == "error"
    assert "timeout" in (row.last_error or "")


@pytest.mark.asyncio
async def test_missing_script_is_recorded_as_error(setup) -> None:
    cron_repo, _, cid, _, scripts = setup
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "gone.py"))
    await job.run()

    delivery.deliver.assert_not_awaited()
    row = cron_repo.get(cid)
    assert row.last_status == "error"
    assert "script not found" in (row.last_error or "")


@pytest.mark.asyncio
async def test_failing_script_raises_when_caller_waits(setup) -> None:
    _, _, _, _, scripts = setup
    _write(scripts, "boom.py", "import sys\nsys.exit(4)")
    delivery = MagicMock()
    delivery.deliver = AsyncMock()

    job = _job(setup=setup, delivery_service=delivery, script=str(scripts / "boom.py"))
    with pytest.raises(RuntimeError):
        await job.run(raise_on_error=True)


def test_script_persists_on_the_row(setup) -> None:
    cron_repo, _, cid, _, scripts = setup
    cron_repo.update(cid, script=str(scripts / "daily.py"), script_timeout_s=90)

    row = cron_repo.get(cid)
    assert row is not None
    assert row.script == str(scripts / "daily.py")
    assert row.script_timeout_s == 90
    assert row.to_public_dict()["script"] == str(scripts / "daily.py")

    cron_repo.update(cid, script=None, script_timeout_s=None)
    cleared = cron_repo.get(cid)
    assert cleared is not None
    assert cleared.script is None
    assert cleared.script_timeout_s is None


def test_from_row_carries_the_script(setup) -> None:
    cron_repo, audit, cid, _, scripts = setup
    cron_repo.update(cid, script=str(scripts / "daily.py"), script_timeout_s=120)
    row = cron_repo.get(cid)
    assert row is not None

    job = CronJob.from_row(
        row,
        delivery_service=MagicMock(),
        cron_repo=cron_repo,
        audit_repo=audit,
    )
    assert job._script == str(scripts / "daily.py")
    assert job._script_timeout_s == 120
