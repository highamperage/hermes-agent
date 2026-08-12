"""Tests for fork-aware upstream sync in hermes_cli.update_cmd."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from hermes_cli.update_cmd import _sync_with_upstream_if_needed


def _make_subprocess_run(
    *,
    has_upstream: bool = True,
    current_branch: str = "main",
    is_dirty: bool = False,
    fetch_ok: bool = True,
    is_ancestor: bool = False,
    merge_ok: bool = True,
    conflicts: list[str] | None = None,
    push_ok: bool = True,
    push_stderr: str = "Permission denied",
):
    """Factory to create a mocked subprocess.run side_effect for git commands."""
    def fake_run(cmd, cwd=None, capture_output=None, text=None, encoding=None, errors=None, check=False):
        cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd

        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""

        if "remote get-url upstream" in cmd_str:
            if not has_upstream:
                res.returncode = 1
                res.stderr = "error: No such remote 'upstream'"
            return res

        if "rev-parse --abbrev-ref HEAD" in cmd_str:
            res.stdout = f"{current_branch}\n"
            return res

        if "status --porcelain" in cmd_str:
            res.stdout = " M dirty_file.py\n" if is_dirty else ""
            return res

        if "fetch upstream" in cmd_str:
            if not fetch_ok:
                res.returncode = 1
                res.stderr = "fatal: couldn't find remote ref main"
            return res

        if "merge-base --is-ancestor" in cmd_str:
            res.returncode = 0 if is_ancestor else 1
            return res

        if f"merge upstream/{current_branch}" in cmd_str:
            if not merge_ok:
                res.returncode = 1
                res.stderr = "Automatic merge failed; fix conflicts and then commit the result."
            return res

        if "diff --name-only --diff-filter=U" in cmd_str:
            c_list = conflicts or ["conflicting_file.py"]
            res.stdout = "\n".join(c_list) + "\n"
            return res

        if "merge --abort" in cmd_str:
            res.returncode = 0
            return res

        if f"push origin {current_branch}" in cmd_str:
            if not push_ok:
                res.returncode = 1
                res.stderr = push_stderr
            return res

        return res

    return fake_run


def test_no_upstream_remote_preserves_behavior(tmp_path):
    """When no 'upstream' remote exists, fork sync is skipped and returns True."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(has_upstream=False)

    with patch("subprocess.run", side_effect=fake_run), patch("builtins.input", return_value="n"):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is True


def test_upstream_already_current(tmp_path, capsys):
    """When upstream/main is already contained in HEAD, report current and return True."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(has_upstream=True, is_ancestor=True)

    with patch("subprocess.run", side_effect=fake_run):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is True
    out = capsys.readouterr().out
    assert "Upstream is already current" in out or "already up to date" in out.lower()


def test_clean_upstream_merge_followed_by_push(tmp_path, capsys):
    """Clean merge of upstream/main followed by successful push returns True."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(
        has_upstream=True,
        is_ancestor=False,
        merge_ok=True,
        push_ok=True,
    )

    with patch("subprocess.run", side_effect=fake_run), patch(
        "hermes_cli.update_cmd._validate_critical_files_syntax",
        return_value=(True, None, None),
    ):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is True
    out = capsys.readouterr().out
    assert "Merging upstream/main into main" in out
    assert "Successfully merged upstream/main and pushed to origin/main" in out


def test_dirty_worktree_skips_sync(tmp_path, capsys):
    """When worktree has uncommitted changes, explain skip and return True."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(has_upstream=True, is_dirty=True)

    with patch("subprocess.run", side_effect=fake_run):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is True
    out = capsys.readouterr().out
    assert "Worktree is dirty" in out
    assert "skipping upstream fork sync" in out


def test_conflict_aborts_and_returns_actionable_prompt(tmp_path, capsys):
    """Merge conflict aborts merge cleanly, outputs conflicting files, and returns False."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(
        has_upstream=True,
        is_ancestor=False,
        merge_ok=False,
        conflicts=["gateway/platforms/whatsapp.py", "tests/test_wa.py"],
    )

    with patch("subprocess.run", side_effect=fake_run):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is False
    out = capsys.readouterr().out
    assert "Merge conflict detected" in out
    assert "gateway/platforms/whatsapp.py" in out
    assert "tests/test_wa.py" in out
    assert "Merge aborted to keep worktree clean" in out
    assert "Ask AGY to resolve the conflicts" in out


def test_push_failure_reported(tmp_path, capsys):
    """Failed push after clean merge reports error, leaves merge intact, and returns False."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(
        has_upstream=True,
        is_ancestor=False,
        merge_ok=True,
        push_ok=False,
        push_stderr="fatal: Authentication failed",
    )

    with patch("subprocess.run", side_effect=fake_run), patch(
        "hermes_cli.update_cmd._validate_critical_files_syntax",
        return_value=(True, None, None),
    ):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is False
    out = capsys.readouterr().out
    assert "Push to origin/main failed" in out
    assert "Authentication failed" in out
    assert "The upstream merge succeeded locally, but could not be pushed" in out

def test_no_force_push_arguments(tmp_path):
    """Ensure upstream sync does not contain any force-push arguments."""
    git_cmd = ["git"]
    fake_run = _make_subprocess_run(has_upstream=True, is_ancestor=False)

    with patch("subprocess.run", side_effect=fake_run) as mock_run, patch(
        "hermes_cli.update_cmd._validate_critical_files_syntax",
        return_value=(True, None, None),
    ):
        result = _sync_with_upstream_if_needed(git_cmd, tmp_path)

    assert result is True

    for call_args in mock_run.call_args_list:
        cmd = call_args[0][0]
        cmd_str = " ".join(cmd)
        assert "--force" not in cmd_str
        assert "--force-with-lease" not in cmd_str
