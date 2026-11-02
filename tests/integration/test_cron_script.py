"""tests/integration/test_cron_script.py — API surface for script-sourced cron jobs.

Kept in its own module: ``test_cron_api`` overrides the conftest ``env`` fixture,
which makes ``env_with_provider`` unresolvable from a fixture defined there.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
async def admin_agent(env_with_provider, tmp_path, monkeypatch):
    """Admin-owned agent plus a whitelisted script directory."""
    client, _srv, admin_auth = env_with_provider
    scripts = Path(tmp_path) / "scripts"
    scripts.mkdir()
    monkeypatch.setenv("OCTOP_CRON_SCRIPT_ROOTS", str(scripts))

    r = await client.post(
        "/api/agents",
        headers=admin_auth,
        json={
            "name": "admin-script-bot",
            "config": {"providers": ["openai"], "default_model": "openai:gpt-4o-mini"},
        },
    )
    r.raise_for_status()
    yield client, admin_auth, r.json()["agent_id"], scripts


async def test_script_requires_admin(env_alice_bob_agent: Any) -> None:
    """A script is server-side execution, so only admins may attach one."""
    client, _srv, alice_auth, _bob_auth, agent_id = env_alice_bob_agent
    r = await client.post(
        f"/api/agents/{agent_id}/cron",
        headers=alice_auth,
        json={"trigger": "interval:60", "script": "daily.py"},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "FORBIDDEN"


async def test_prompt_and_script_are_independent(admin_agent) -> None:
    client, admin_auth, agent_id, scripts = admin_agent
    (scripts / "daily.py").write_text("print('hello')", encoding="utf-8")

    r = await client.post(
        f"/api/agents/{agent_id}/cron",
        headers=admin_auth,
        json={"trigger": "interval:60", "script": "daily.py"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["prompt"] == ""
    assert str(scripts / "daily.py") in body["script"]

    r = await client.patch(
        f"/api/agents/{agent_id}/cron/{body['id']}",
        headers=admin_auth,
        json={"script": None},
    )
    assert r.status_code == 200
    assert r.json()["script"] is None


async def test_script_outside_roots_is_rejected(admin_agent) -> None:
    client, admin_auth, agent_id, _scripts = admin_agent
    r = await client.post(
        f"/api/agents/{agent_id}/cron",
        headers=admin_auth,
        json={"trigger": "interval:60", "script": "/etc/passwd"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "CRON_SCRIPT_DENIED"


async def test_prompt_is_required_without_script(env_alice_bob_agent: Any) -> None:
    client, _srv, alice_auth, _bob_auth, agent_id = env_alice_bob_agent
    r = await client.post(
        f"/api/agents/{agent_id}/cron",
        headers=alice_auth,
        json={"trigger": "interval:60"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "CRON_PROMPT_REQUIRED"
