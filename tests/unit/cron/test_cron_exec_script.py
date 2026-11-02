"""tests/unit/cron/test_cron_exec_script.py — script resolution and execution guards."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from octop.infra.cron.exec_script import (
    DEFAULT_SCRIPT_TIMEOUT_SECONDS,
    MAX_SCRIPT_TIMEOUT_SECONDS,
    STDERR_TAIL_BYTES,
    STDOUT_MAX_BYTES,
    normalize_script_spec,
    normalize_script_timeout,
    resolve_script,
    run_script,
    script_roots,
)
from octop.infra.errors import ErrorCode, OctopError


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    monkeypatch.setenv("OCTOP_CRON_SCRIPT_ROOTS", str(scripts))
    return scripts


def _write(root: Path, name: str, body: str) -> Path:
    path = root / name
    path.write_text(body, encoding="utf-8")
    return path


def test_script_roots_defaults_to_home_scripts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OCTOP_CRON_SCRIPT_ROOTS", raising=False)
    roots = script_roots()
    assert len(roots) == 1
    assert roots[0].name == "scripts"


def test_script_roots_reads_every_configured_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    monkeypatch.setenv("OCTOP_CRON_SCRIPT_ROOTS", f"{first}{os.pathsep}{second}")
    assert [p.name for p in script_roots()] == ["first", "second"]


def test_resolve_rejects_empty_spec(root: Path) -> None:
    with pytest.raises(OctopError) as info:
        resolve_script("   ")
    assert info.value.code == ErrorCode.CRON_SCRIPT_INVALID


def test_resolve_rejects_unbalanced_quotes(root: Path) -> None:
    with pytest.raises(OctopError) as info:
        resolve_script("'unterminated")
    assert info.value.code == ErrorCode.CRON_SCRIPT_INVALID


def test_resolve_reports_missing_file(root: Path) -> None:
    with pytest.raises(OctopError) as info:
        resolve_script(str(root / "nope.py"))
    assert info.value.code == ErrorCode.CRON_SCRIPT_DENIED


def test_resolve_rejects_path_outside_roots(root: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside.py"
    outside.write_text("print('x')", encoding="utf-8")
    with pytest.raises(OctopError) as info:
        resolve_script(str(outside))
    assert info.value.code == ErrorCode.CRON_SCRIPT_DENIED


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs privileges on Windows")
def test_resolve_rejects_symlink_escape(root: Path, tmp_path: Path) -> None:
    secret = tmp_path / "secret.py"
    secret.write_text("print('x')", encoding="utf-8")
    link = root / "link.py"
    link.symlink_to(secret)
    with pytest.raises(OctopError) as info:
        resolve_script(str(link))
    assert info.value.code == ErrorCode.CRON_SCRIPT_DENIED


def test_resolve_accepts_relative_name_and_args(root: Path) -> None:
    _write(root, "daily.py", "print('hi')")
    path, args = resolve_script("daily.py --symbol 600519")
    assert path == (root / "daily.py").resolve()
    assert args == ("--symbol", "600519")


def test_normalize_spec_returns_absolute_command(root: Path) -> None:
    _write(root, "daily.py", "print('hi')")
    normalized = normalize_script_spec("daily.py --flag")
    assert str((root / "daily.py").resolve()) in normalized
    assert "--flag" in normalized


def test_normalize_timeout_defaults() -> None:
    assert normalize_script_timeout(None) == DEFAULT_SCRIPT_TIMEOUT_SECONDS


def test_normalize_timeout_accepts_in_range() -> None:
    assert normalize_script_timeout(60) == 60
    assert normalize_script_timeout(MAX_SCRIPT_TIMEOUT_SECONDS) == MAX_SCRIPT_TIMEOUT_SECONDS


@pytest.mark.parametrize("value", [0, -1, MAX_SCRIPT_TIMEOUT_SECONDS + 1])
def test_normalize_timeout_rejects_out_of_range(value: int) -> None:
    with pytest.raises(OctopError) as info:
        normalize_script_timeout(value)
    assert info.value.code == ErrorCode.CRON_SCRIPT_INVALID


@pytest.mark.parametrize("value", ["60", True])
def test_normalize_timeout_rejects_non_int(value: object) -> None:
    with pytest.raises(OctopError) as info:
        normalize_script_timeout(value)  # type: ignore[arg-type]
    assert info.value.code == ErrorCode.CRON_SCRIPT_INVALID


@pytest.mark.asyncio
async def test_run_script_captures_stdout(root: Path) -> None:
    _write(root, "hello.py", "print('hello world')")
    outcome = await run_script(str(root / "hello.py"))
    assert outcome.ok
    assert outcome.stdout.strip() == "hello world"
    assert outcome.exit_code == 0


@pytest.mark.asyncio
async def test_run_script_reports_empty_output(root: Path) -> None:
    _write(root, "silent.py", "pass")
    outcome = await run_script(str(root / "silent.py"))
    assert outcome.ok
    assert outcome.stdout.strip() == ""


@pytest.mark.asyncio
async def test_run_script_passes_arguments(root: Path) -> None:
    _write(root, "echo_args.py", "import sys\nprint(' '.join(sys.argv[1:]))")
    outcome = await run_script(f"{root / 'echo_args.py'} a b")
    assert outcome.stdout.strip() == "a b"


@pytest.mark.asyncio
async def test_run_script_uses_script_directory_as_cwd(root: Path) -> None:
    _write(root, "cwd.py", "import os\nprint(os.path.basename(os.getcwd()))")
    outcome = await run_script(str(root / "cwd.py"))
    assert outcome.stdout.strip() == "scripts"


@pytest.mark.asyncio
async def test_run_script_does_not_inherit_environment(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OCTOP_TEST_SECRET", "leaked")
    _write(root, "env.py", "import os\nprint(os.environ.get('OCTOP_TEST_SECRET', 'absent'))")
    outcome = await run_script(str(root / "env.py"))
    assert outcome.stdout.strip() == "absent"


@pytest.mark.asyncio
async def test_run_script_reports_non_zero_exit(root: Path) -> None:
    _write(root, "boom.py", "import sys\nsys.stderr.write('bad things')\nsys.exit(3)")
    outcome = await run_script(str(root / "boom.py"))
    assert not outcome.ok
    assert outcome.exit_code == 3
    assert outcome.stderr_tail == "bad things"


@pytest.mark.asyncio
async def test_run_script_truncates_large_stdout(root: Path) -> None:
    _write(root, "flood.py", f"print('x' * {STDOUT_MAX_BYTES * 2})")
    outcome = await run_script(str(root / "flood.py"))
    assert outcome.stdout.endswith("bytes]")
    assert len(outcome.stdout) < STDOUT_MAX_BYTES * 2


@pytest.mark.asyncio
async def test_run_script_keeps_only_stderr_tail(root: Path) -> None:
    _write(
        root,
        "noisy.py",
        f"import sys\nsys.stderr.write('y' * {STDERR_TAIL_BYTES * 3})\nsys.exit(1)",
    )
    outcome = await run_script(str(root / "noisy.py"))
    assert len(outcome.stderr_tail) == STDERR_TAIL_BYTES


@pytest.mark.asyncio
async def test_run_script_times_out(root: Path) -> None:
    _write(root, "slow.py", "import time\ntime.sleep(30)")
    outcome = await run_script(str(root / "slow.py"), timeout_s=1)
    assert outcome.timed_out
    assert not outcome.ok


@pytest.mark.asyncio
async def test_run_script_rejects_unknown_path(root: Path) -> None:
    with pytest.raises(OctopError) as info:
        await run_script(str(root / "missing.py"))
    assert info.value.code == ErrorCode.CRON_SCRIPT_DENIED
