"""Tests for the /update slash command in the classic CLI and TUI launcher.

Verifies that ``HermesCLI._handle_update_command`` correctly:
- Refuses to run under a managed install (Homebrew, Docker, etc.)
- Refuses to run when the interactive tmux session 'agy' is missing
- Dispatches the AGY update workflow via tmux paste-buffer and returns ``True`` on confirmation
- Cancels cleanly on a "no"-shaped answer or unrecognized input
- Cancels cleanly when ``_prompt_text_input_modal`` returns None (timeout / modal dismissed)
"""

from __future__ import annotations

from types import SimpleNamespace
import os
import subprocess
from unittest.mock import MagicMock, patch
import pytest

@pytest.fixture(autouse=True)
def mock_hermes_home(tmp_path):
    with patch("hermes_constants.get_hermes_home", return_value=str(tmp_path)), \
         patch("hermes_cli.agy_watcher.get_hermes_home", return_value=str(tmp_path), create=True):
        yield tmp_path

from cli import HermesCLI


def _bound(fn, instance):
    """Bind an unbound method to a stand-in instance."""
    return fn.__get__(instance, type(instance))


def _make_self(modal_response):
    """Build a minimal stand-in 'self' for ``_handle_update_command``.

    Uses the same SimpleNamespace pattern as ``test_destructive_slash_confirm``
    so we don't need a full ``HermesCLI`` construction.
    ``_prompt_text_input_modal`` is stubbed to return *modal_response*
    directly so tests can drive the entire confirmation branch without
    touching stdin or prompt_toolkit internals.
    """
    self_ = SimpleNamespace(
        _app=None,
        _pending_relaunch=None,
        _prompt_text_input_modal=lambda **_kw: modal_response,
    )
    self_._normalize_slash_confirm_choice = _bound(
        HermesCLI._normalize_slash_confirm_choice, self_
    )
    return self_


def _call(self_):
    """Invoke the real ``_handle_update_command`` on the stub."""
    return HermesCLI._handle_update_command(self_)


# ---------------------------------------------------------------------------
# Managed-install guard
# ---------------------------------------------------------------------------


def test_managed_install_refuses_and_does_not_set_pending_relaunch(capsys):
    """Under a managed install (brew/docker), /update prints a hint and
    returns without setting ``_pending_relaunch``."""
    self_ = SimpleNamespace(
        _app=None,
        _pending_relaunch=None,
        _prompt_text_input_modal=lambda **_kw: pytest.fail("Modal should not be called"),
    )
    self_._normalize_slash_confirm_choice = _bound(
        HermesCLI._normalize_slash_confirm_choice, self_
    )
    with (
        patch("hermes_cli.config.is_managed", return_value=True),
        patch(
            "hermes_cli.config.format_managed_message",
            return_value="Use `sudo nixos-rebuild switch` to update.",
        ),
    ):
        result = _call(self_)

    out = capsys.readouterr().out
    assert "sudo nixos-rebuild switch" in out
    assert self_._pending_relaunch is None
    assert not result


# ---------------------------------------------------------------------------
# Session-missing check
# ---------------------------------------------------------------------------


def test_session_missing_refuses_and_returns_false(capsys):
    """When tmux session 'agy' does not exist, /update prints an error and returns False."""
    self_ = SimpleNamespace(
        _app=None,
        _pending_relaunch=None,
        _prompt_text_input_modal=lambda **_kw: pytest.fail("Modal should not be called"),
    )
    self_._normalize_slash_confirm_choice = _bound(
        HermesCLI._normalize_slash_confirm_choice, self_
    )
    mock_run = MagicMock()
    mock_run.return_value = SimpleNamespace(returncode=1, stdout=b"", stderr=b"session not found")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
    ):
        result = _call(self_)

    out = capsys.readouterr().out
    assert "tmux session 'agy' not found" in out
    assert self_._pending_relaunch is None
    assert result is False
    import subprocess
    mock_run.assert_called_once_with(
        ["tmux", "has-session", "-t", "agy"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Confirmation proceeds and dispatches to agy tmux session
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES", "1", "ok"])
def test_affirmative_answer_dispatches_to_agy_tmux_and_returns_true(answer, capsys, mock_hermes_home):
    self_ = _make_self(modal_response=answer)

    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "capture-pane"]:
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    mock_run = MagicMock(side_effect=_mock_run)

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
        patch("subprocess.Popen"),
    ):
        result = _call(self_)

    assert self_._pending_relaunch is None
    assert result is True
    out = capsys.readouterr().out
    assert "Dispatched update task to tmux session 'agy'" in out

    calls = mock_run.call_args_list
    assert len(calls) == 5
    assert calls[0][0][0] == ["tmux", "has-session", "-t", "agy"]
    assert calls[1][0][0] == ["tmux", "capture-pane", "-p", "-t", "agy"]
    assert calls[2][0][0] == ["tmux", "load-buffer", "-"]
    prompt_sent = calls[2][1]["input"].decode("utf-8")
    assert "Read " in prompt_sent
    assert "execute it as an isolated one-shot task" in prompt_sent

    packet_files = list(mock_hermes_home.glob("agy-update-task-*.md"))
    assert len(packet_files) == 1
    packet_content = packet_files[0].read_text()

    assert "repository checks" in packet_content
    assert "Fetch origin and upstream" in packet_content
    assert "Integrate upstream/main into main" in packet_content
    assert "documented build" in packet_content
    assert "Stage and commit only the intended update changes" in packet_content
    assert "unset COPILOT_GITHUB_TOKEN only" in packet_content
    assert "Do NOT unset GITHUB_TOKEN" in packet_content
    assert "github-highamperage.env" in packet_content
    assert "Verify the personal token owner" in packet_content
    assert "require login 'highamperage'" in packet_content
    assert "ordinary 'git push origin main'" in packet_content
    assert "Never force-push" in packet_content
    assert "Verify origin/main equals HEAD after pushing" in packet_content
    assert "systemctl --user restart hermes-gateway" in packet_content
    assert "`systemctl --user is-active hermes-gateway` returns active" in packet_content
    assert "If restart fails, the workflow must end with AGY FAILED" in packet_content
    assert "AGY DONE" in packet_content

    assert calls[3][0][0] == ["tmux", "paste-buffer", "-t", "agy"]
    assert calls[4][0][0] == ["tmux", "send-keys", "-t", "agy", "Enter"]

# ---------------------------------------------------------------------------
# Cancellation paths — _pending_relaunch must stay None
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("answer", ["n", "N", "no", "NO", " no "])
def test_negative_answer_cancels(answer, capsys):
    """Any "no"-shaped answer cancels without dispatching or setting ``_pending_relaunch``."""
    self_ = _make_self(modal_response=answer)

    mock_run = MagicMock()
    mock_run.return_value = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
    ):
        result = _call(self_)

    assert self_._pending_relaunch is None
    assert not result
    assert "Dispatched update task" not in capsys.readouterr().out
    # has-session and capture-pane should have been called before confirmation modal
    assert mock_run.call_count == 2


def test_none_response_cancels(capsys):
    """``None`` from the modal (timeout or dismiss) cancels cleanly."""
    self_ = _make_self(modal_response=None)

    mock_run = MagicMock()
    mock_run.return_value = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
    ):
        result = _call(self_)

    assert self_._pending_relaunch is None
    assert not result
    assert mock_run.call_count == 2


@pytest.mark.parametrize("answer", ["nope", "cancel", "sure", "2", "3", "abort", ""])
def test_unrecognized_or_cancel_input_cancels(answer, capsys):
    """Unrecognised input and explicit "cancel" do not proceed."""
    self_ = _make_self(modal_response=answer)

    mock_run = MagicMock()
    mock_run.return_value = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
    ):
        result = _call(self_)

    assert self_._pending_relaunch is None
    assert not result
    assert mock_run.call_count == 2

import sys
import time
import json
from hermes_cli import agy_watcher

def test_active_update_refuses_dispatch(capsys, mock_hermes_home):
    self_ = _make_self(modal_response="y")

    # Create fake active state
    state_file = mock_hermes_home / "update_task.json"
    state_file.write_text(json.dumps({"task_token": "foo", "start_time": time.time(), "target": "agy"}))

    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "has-session"]:
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        if cmd[0:2] == ["tmux", "capture-pane"]:
            # Active update: no AGY DONE token
            return SimpleNamespace(returncode=0, stdout=b"some pane text\n", stderr=b"")
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", side_effect=_mock_run),
    ):
        result = _call(self_)

    out = capsys.readouterr().out
    assert "An update task is already active" in out
    assert result is False

def test_stale_update_recovers_dispatch(capsys, mock_hermes_home):
    self_ = _make_self(modal_response="y")

    state_file = mock_hermes_home / "update_task.json"
    # Old start time
    state_file.write_text(json.dumps({"task_token": "foo", "start_time": time.time() - 4000, "target": "agy"}))

    def _mock_run(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", side_effect=_mock_run),
        patch("subprocess.Popen"),
    ):
        result = _call(self_)

    # Should recover and dispatch
    assert result is True
    # The original state file should have been replaced with a new one
    new_state = json.loads(state_file.read_text())
    assert new_state["task_token"] != "foo"

def test_dispatch_failure_aborts_cleanly(capsys, mock_hermes_home):
    for fail_cmd_prefix in [["tmux", "load-buffer"], ["tmux", "paste-buffer"], ["tmux", "send-keys"]]:
        self_ = _make_self(modal_response="y")

        def _mock_run(cmd, **kwargs):
            if cmd[0:2] == ["tmux", "has-session"]:
                return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
            if cmd[0:2] == ["tmux", "capture-pane"]:
                return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
            if cmd[0:2] == fail_cmd_prefix:
                return SimpleNamespace(returncode=1, stdout=b"", stderr=b"fake error")
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        with (
            patch("hermes_cli.config.is_managed", return_value=False),
            patch("subprocess.run", side_effect=_mock_run),
            patch("subprocess.Popen") as mock_popen,
        ):
            result = _call(self_)

        out = capsys.readouterr().out
        assert "Failed" in out
        assert "fake error" in out
        assert result is False
        mock_popen.assert_not_called()
        # State file and packet file should be removed
        assert not (mock_hermes_home / "update_task.json").exists()
        packet_files = list(mock_hermes_home.glob("agy-update-task-*.md"))
        assert len(packet_files) == 0

def test_watcher_success(tmp_path, mock_hermes_home):
    tty_path = tmp_path / "tty.log"
    token = "test-token-123"

    state_file = mock_hermes_home / "update_task.json"
    state_file.write_text(json.dumps({"task_token": token, "start_time": time.time(), "target": "agy", }))

    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "has-session"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[0:2] == ["tmux", "capture-pane"]:
            return SimpleNamespace(returncode=0, stdout=f"Some progress\nAGY DONE {token}\n".encode('utf-8'), stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with (
        patch.object(sys, "argv", ["agy_watcher.py", str(tty_path), token]),
        patch("subprocess.run", side_effect=_mock_run),
        patch("time.sleep"),
        patch("time.time", return_value=1000.0)
    ):
        agy_watcher.main()

    content = tty_path.read_text()
    assert "[Watcher] Polling AGY update progress" in content
    assert "[Watcher] ✓ Update workflow completed" in content

    # State file should be cleaned up
    assert not state_file.exists()


def test_watcher_failed(tmp_path, mock_hermes_home):
    tty_path = tmp_path / "tty.log"
    token = "test-token-123"

    state_file = mock_hermes_home / "update_task.json"
    state_file.write_text(json.dumps({"task_token": token, "start_time": time.time(), "target": "agy"}))

    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "has-session"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[0:2] == ["tmux", "capture-pane"]:
            return SimpleNamespace(returncode=0, stdout=f"Some progress\nAGY FAILED {token}\n".encode('utf-8'), stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with (
        patch.object(sys, "argv", ["agy_watcher.py", str(tty_path), token]),
        patch("subprocess.run", side_effect=_mock_run),
        patch("time.sleep"),
        patch("time.time", return_value=1000.0)
    ):
        agy_watcher.main()

    content = tty_path.read_text()
    assert "[Watcher] ✗ Update workflow failed." in content

def test_watcher_stale_pane_output_ignored(tmp_path, mock_hermes_home):

    tty_path = tmp_path / "tty.log"
    token = "current-token"

    state_file = mock_hermes_home / "update_task.json"
    state_file.write_text(json.dumps({"task_token": token, "start_time": time.time(), "target": "agy", }))

    call_count = [0]
    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "has-session"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[0:2] == ["tmux", "capture-pane"]:
            call_count[0] += 1
            if call_count[0] == 1:
                return SimpleNamespace(returncode=0, stdout="Old line 1\nAGY DONE stale-token\n".encode('utf-8'), stderr="")
            if call_count[0] == 2:
                # First line is skipped because baseline=1
                return SimpleNamespace(returncode=0, stdout="Old line 1\nNew progress\n".encode('utf-8'), stderr="")
            return SimpleNamespace(returncode=0, stdout=f"Old line 1\nNew progress\nAGY DONE {token}\n".encode('utf-8'), stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with (
        patch.object(sys, "argv", ["agy_watcher.py", str(tty_path), token]),
        patch("subprocess.run", side_effect=_mock_run),
        patch("time.sleep"),
        patch("time.time", return_value=1000.0)
    ):
        agy_watcher.main()

    content = tty_path.read_text()
    assert "[AGY] New progress" in content
    assert "[AGY] Old line 1" not in content

def test_watcher_timeout(tmp_path, mock_hermes_home):
    tty_path = tmp_path / "tty.log"
    token = "tok"

    def _mock_run(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    time_vals = [1000.0, 3000.0]
    def _mock_time():
        return time_vals.pop(0) if time_vals else 3000.0

    with (
        patch.object(sys, "argv", ["agy_watcher.py", str(tty_path), token]),
        patch("subprocess.run", side_effect=_mock_run),
        patch("time.sleep"),
        patch("time.time", side_effect=_mock_time)
    ):
        agy_watcher.main()

    content = tty_path.read_text()
    assert "Timeout exceeded. Stopping watcher." in content

def test_watcher_tmux_disappears(tmp_path, mock_hermes_home):
    tty_path = tmp_path / "tty.log"
    token = "tok"

    def _mock_run(cmd, **kwargs):
        if cmd[0:2] == ["tmux", "has-session"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch.object(sys, "argv", ["agy_watcher.py", str(tty_path), token]),
        patch("subprocess.run", side_effect=_mock_run),
        patch("time.sleep"),
        patch("time.time", return_value=1000.0)
    ):
        agy_watcher.main()

    content = tty_path.read_text()
    assert "tmux session 'agy' disappeared or failed" in content
