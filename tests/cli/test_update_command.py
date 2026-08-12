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
from unittest.mock import MagicMock, patch

import pytest

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
    mock_run.assert_called_once_with(
        ["tmux", "has-session", "-t", "agy"],
        stdout=-1,
        stderr=-1,
    )


# ---------------------------------------------------------------------------
# Confirmation proceeds and dispatches to agy tmux session
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES", "1", "ok"])
def test_affirmative_answer_dispatches_to_agy_tmux_and_returns_true(answer, capsys):
    """Recognised affirmative answers ("y", "yes", "1", "ok") dispatch the AGY prompt
    via tmux paste-buffer, return ``True``, and do NOT set ``_pending_relaunch``."""
    self_ = _make_self(modal_response=answer)

    mock_run = MagicMock()
    mock_run.return_value = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    with (
        patch("hermes_cli.config.is_managed", return_value=False),
        patch("subprocess.run", mock_run),
    ):
        result = _call(self_)

    assert self_._pending_relaunch is None
    assert result is True
    out = capsys.readouterr().out
    assert "Dispatched update task to tmux session 'agy'" in out

    # Verify subprocess calls: has-session, load-buffer, paste-buffer, send-keys
    calls = mock_run.call_args_list
    assert len(calls) == 4
    assert calls[0][0][0] == ["tmux", "has-session", "-t", "agy"]
    assert calls[1][0][0] == ["tmux", "load-buffer", "-"]
    prompt_sent = calls[1][1]["input"].decode("utf-8")
    assert "Execute self-contained update workflow" in prompt_sent
    assert "repository checks" in prompt_sent
    assert "local commit" in prompt_sent
    assert "upstream merge" in prompt_sent
    assert "origin/main push and pull" in prompt_sent
    assert "documented build" in prompt_sent
    assert "no tests" in prompt_sent
    assert "no reset" in prompt_sent
    assert "no force-push" in prompt_sent
    assert "stop on conflicts" in prompt_sent
    assert "AGY DONE" in prompt_sent
    assert calls[2][0][0] == ["tmux", "paste-buffer", "-t", "agy"]
    assert calls[3][0][0] == ["tmux", "send-keys", "-t", "agy", "Enter"]


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
    # Only has-session should have been called before confirmation modal
    assert mock_run.call_count == 1


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
    assert mock_run.call_count == 1


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
    assert mock_run.call_count == 1
