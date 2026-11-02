"""Run a cron job's content-producing script in a constrained subprocess.

A script is *not* a sandbox: it runs as a local child process of the server.
The guards here only keep the job inside an administrator-controlled directory,
out of a shell, and bounded in time and output size.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shlex
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from octop.infra.errors import ErrorCode, OctopError

logger = logging.getLogger(__name__)

DEFAULT_SCRIPT_TIMEOUT_SECONDS = 300
MAX_SCRIPT_TIMEOUT_SECONDS = 3600
STDOUT_MAX_BYTES = 32 * 1024
STDERR_TAIL_BYTES = 4 * 1024

SCRIPT_ROOTS_ENV = "OCTOP_CRON_SCRIPT_ROOTS"
_ENV_ALLOWLIST = ("PATH", "HOME", "LANG", "LC_ALL", "TZ", "PYTHONPATH")
_TRUNCATION_MARK = "\n…[truncated {n} bytes]"


@dataclass(frozen=True)
class ScriptOutcome:
    """Result of one script run."""

    exit_code: int
    stdout: str
    stderr_tail: str
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return not self.timed_out and self.exit_code == 0


def script_roots() -> tuple[Path, ...]:
    """Directories a cron script may live in (``OCTOP_CRON_SCRIPT_ROOTS``)."""
    raw = os.environ.get(SCRIPT_ROOTS_ENV, "")
    configured = [item for item in raw.split(os.pathsep) if item.strip()]
    if not configured:
        return ((Path.home() / ".octop" / "scripts").resolve(),)
    return tuple(Path(item).expanduser().resolve() for item in configured)


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _split_spec(text: str) -> list[str]:
    """Split *text* into argv, keeping Windows backslashes intact.

    ``shlex`` in POSIX mode treats ``\\`` as an escape character, which mangles
    ``C:\\scripts\\daily.py``. On Windows we therefore split without POSIX rules
    and strip the quoting ourselves, so both raw paths and ``shlex.join`` output
    round-trip.
    """
    posix = sys.platform != "win32"
    try:
        argv = shlex.split(text, posix=posix)
    except ValueError as exc:
        raise OctopError(ErrorCode.CRON_SCRIPT_INVALID, f"script cannot be parsed: {exc}") from exc
    if posix:
        return argv
    return [
        token[1:-1] if len(token) >= 2 and token[0] == token[-1] and token[0] in "\"'" else token
        for token in argv
    ]


def resolve_script(spec: str) -> tuple[Path, tuple[str, ...]]:
    """Split *spec* into an absolute executable inside an allowed root and its argv tail."""
    text = (spec or "").strip()
    if not text:
        raise OctopError(ErrorCode.CRON_SCRIPT_INVALID, "script must not be empty")
    argv = _split_spec(text)
    if not argv:
        raise OctopError(ErrorCode.CRON_SCRIPT_INVALID, "script must not be empty")

    executable = Path(argv[0]).expanduser()
    if not executable.is_absolute():
        executable = script_roots()[0] / executable
    try:
        resolved = executable.resolve(strict=True)
    except OSError as exc:
        raise OctopError(
            ErrorCode.CRON_SCRIPT_DENIED, f"script not found: {argv[0]}"
        ) from exc

    roots = script_roots()
    if not any(_is_within(resolved, root) for root in roots):
        allowed = os.pathsep.join(str(root) for root in roots)
        raise OctopError(
            ErrorCode.CRON_SCRIPT_DENIED,
            f"script must live under {allowed}",
        )
    if not resolved.is_file():
        raise OctopError(ErrorCode.CRON_SCRIPT_DENIED, f"script is not a file: {resolved}")
    return resolved, tuple(argv[1:])


def normalize_script_spec(spec: str) -> str:
    """Validate *spec* and return it in canonical (absolute path + args) form."""
    path, args = resolve_script(spec)
    return shlex.join([str(path), *args])


def normalize_script_timeout(value: int | None) -> int:
    """Clamp-free validation of the per-job timeout."""
    if value is None:
        return DEFAULT_SCRIPT_TIMEOUT_SECONDS
    if not isinstance(value, int) or isinstance(value, bool):
        raise OctopError(ErrorCode.CRON_SCRIPT_INVALID, "script_timeout_s must be an integer")
    if value < 1 or value > MAX_SCRIPT_TIMEOUT_SECONDS:
        raise OctopError(
            ErrorCode.CRON_SCRIPT_INVALID,
            f"script_timeout_s must be between 1 and {MAX_SCRIPT_TIMEOUT_SECONDS}",
        )
    return value


def _decode_head(raw: bytes, limit: int) -> str:
    if len(raw) <= limit:
        return raw.decode("utf-8", "replace")
    head = raw[:limit].decode("utf-8", "replace")
    return head + _TRUNCATION_MARK.format(n=len(raw) - limit)


def _decode_tail(raw: bytes, limit: int) -> str:
    return raw[-limit:].decode("utf-8", "replace") if raw else ""


def _child_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in _ENV_ALLOWLIST if key in os.environ}
    env.setdefault("PATH", os.defpath)
    return env


async def _terminate(proc: asyncio.subprocess.Process) -> None:
    """Kill the child and, where possible, everything it spawned."""
    if proc.returncode is not None:
        return
    if sys.platform == "win32":  # pragma: no cover - Windows-only branch
        with contextlib.suppress(Exception):
            killer = await asyncio.create_subprocess_exec(
                "taskkill",
                "/F",
                "/T",
                "/PID",
                str(proc.pid),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await killer.wait()
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
    else:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(proc.wait(), timeout=5)


async def run_script(spec: str, *, timeout_s: int | None = None) -> ScriptOutcome:
    """Execute *spec* and capture its output. Never raises for a non-zero exit."""
    path, args = resolve_script(spec)
    timeout = normalize_script_timeout(timeout_s)

    # A ``.py`` script runs under the server interpreter, which keeps the spec
    # portable (no shebang handling, no PATH lookups, works on Windows too).
    cmd = (
        [sys.executable, str(path), *args]
        if path.suffix.lower() == ".py"
        else [str(path), *args]
    )

    try:
        if sys.platform == "win32":  # pragma: no cover - Windows-only branch
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(path.parent),
                env=_child_env(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(path.parent),
                env=_child_env(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
    except OSError as exc:
        raise OctopError(
            ErrorCode.CRON_SCRIPT_DENIED, f"script could not start: {exc}"
        ) from exc

    try:
        raw_out, raw_err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        await _terminate(proc)
        logger.warning("cron script %s timed out after %ss", path, timeout)
        return ScriptOutcome(exit_code=-1, stdout="", stderr_tail="", timed_out=True)

    exit_code = proc.returncode if proc.returncode is not None else -1
    return ScriptOutcome(
        exit_code=exit_code,
        stdout=_decode_head(raw_out or b"", STDOUT_MAX_BYTES),
        stderr_tail=_decode_tail(raw_err or b"", STDERR_TAIL_BYTES),
    )
