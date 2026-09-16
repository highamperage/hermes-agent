"""Hermes update pipeline: dispatchers (``_cmd_update_impl``/``_cmd_update_check``) + git plumbing.

Each concern lives in ``update_cmd_<concern>.py`` and is re-imported here so
``hermes_cli.update_cmd.<name>`` keeps resolving (and stays monkeypatchable). Imports are one-way:
main -> update_cmd -> update_cmd_*; ``_m()`` resolves ``hermes_cli.main`` at call time.
"""

import logging
from contextlib import suppress
import os
import shlex
import shutil  # noqa: F401  (tests patch update_cmd.shutil.*; split modules resolve it here)
import subprocess
import sys
import time as _time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hermes_cli.config import get_hermes_home  # noqa: F401  (re-exported; patched via update_cmd)
from hermes_cli.update_cmd_common import _best_effort
from hermes_constants import get_default_hermes_root, venv_python_path

# Re-exports: every split-module name stays reachable (and monkeypatchable) as update_cmd.<name>.
from hermes_cli.update_abort_recovery import (  # noqa: F401
    _abort_recovery_is_complete, _qualified_serve_skips, _recover_gateway_restart_after_abort,
    _serve_unit_recovery_available, _surviving_pre_update_serve_runtimes,
    _warn_stale_serve_runtimes)
from hermes_cli.update_cmd_windows import (  # noqa: F401
    _HOLDER_VALUE_FLAGS_FALLBACK, _clear_windows_venv_holders_or_exit,
    _cold_start_windows_gateway_after_update, _desktop_owns_gateway_lifecycle,
    _detect_venv_python_processes, _format_venv_python_holders_message,
    _handoff_reapable_backend_pids, _hermes_holder_subcommand, _holder_value_flags,
    _holder_value_flags_cache, _ledger_manual_serve_holders, _ledger_reapable_backend_pids,
    _leftover_pausable_gateway_pids, _looks_like_desktop_control_plane,
    _orphaned_desktop_backend_pids, _pause_windows_gateways_for_update,
    _refresh_bootstrap_cache_scripts, _refresh_windows_gateway_launchers,
    _refuse_gateway_ancestor_tree_kill, _relaunch_stopped_serves,
    _restore_windows_gateway_service, _resume_windows_gateways_after_update,
    _resume_windows_gateways_and_merge_outcome, _self_and_non_gateway_ancestor_pids,
    _serve_relaunch_commands, _start_windows_gateway_service, _stop_process_trees,
    _stop_windows_gateway_service, _venv_launcher_ancestors,
    _wait_for_windows_update_gateway_exit, _write_update_planned_stop_marker)
from hermes_cli.update_cmd_fleet import (  # noqa: F401
    _FLEET_RESTART_PENDING_NAME, _FRESH_RESTART_SUPERVISORS, _GatewayRestartOutcome,
    _apply_pending_fleet_restart_catchup, _clear_fleet_restart_pending_marker,
    _current_checkout_sha, _defer_fleet_restart_after_update, _drain_or_signal_gateway_for_update, _fleet_probe_expected_runtimes,
    _fleet_restart_pending_marker_path, _for_each_systemd_gateway_unit,
    _gateway_recovery_partition, _gateway_service_matches_profile, _pending_fleet_restart_needed,
    _receipt_looks_unfinished, _receipt_reports_stale_runtime, _resolve_manage_cmd,
    _restart_gateway_fleet_after_update, _restart_launchd_gateway_after_update,
    _restart_macos_launchd_gateways, _restart_phase_failure_is_incomplete,
    _restart_systemd_gateway_units, _restart_systemd_gateway_units_best_effort,
    _run_pending_fleet_restart, _service_restart_sec,
    _service_unit_supports_graceful_sigusr1_restart, _surviving_gateway_pids_after_failed_restart,
    _systemctl, _systemctl_reset_and_restart, _verify_fleet_after_update,
    _wait_for_service_active, _warn_gateway_restart_phase_aborted,
    _warn_incomplete_gateway_fleet_restart, _warn_pending_fleet_restart,
    _warn_pending_fleet_restart_on_startup, _write_fleet_restart_pending_marker,
    _write_gateway_update_exit_code)
from hermes_cli.update_cmd_zip import (  # noqa: F401
    _ZIP_PRESERVED_TOP_LEVEL, _ZIP_STAGING_ARTIFACT_SUFFIXES, _abort_zip_update_if_dirty_tree,
    _atomic_replace_dir, _commit_staged_replacements, _discard_staged,
    _is_zip_preserved_entry_status_line, _is_zip_staging_artifact_status_line, _stage_replacement,
    _update_via_zip, _zip_overlay_block_reason)
from hermes_cli.update_cmd_stash import (  # noqa: F401
    _AUTOSTASH_NAME_PREFIX, _AUTOSTASH_WARN_AGE_DAYS, _discard_stashed_changes,
    _git_untracked_paths, _park_stashed_changes, _print_stash_cleanup_guidance,
    _reject_unsafe_stash_restore, _resolve_stash_selector, _restore_stashed_changes,
    _restored_python_paths, _stash_apply_failed_only_on_existing_untracked,
    _stash_local_changes_if_needed, _warn_orphaned_update_autostashes)
from hermes_cli.update_cmd_config import (  # noqa: F401
    _LAST_SIBLING_SNAPSHOTS, _check_and_apply_config_migration, _migrate_sibling_profile_configs,
    _print_items, _reload_config_modules, _run_config_check_fresh, _run_migrate_config_fresh)
from hermes_cli.update_cmd_deps import (  # noqa: F401
    _INSTALL_DEFINING_FILES, _SELF_LOCKING_NATIVE_MODULES, _UPDATE_CRITICAL_MODULES,
    _abort_dependency_sync_if_self_locked, _capture_active_lazy_features,
    _capture_active_tool_dependencies, _critical_module_import_failures,
    _defer_update_for_self_lock, _dependency_sync_would_rewrite, _desktop_app_present,
    _detect_self_loaded_native_modules, _editable_install_is_current, _ensure_uv_for_termux,
    _ensure_venv_pip, _install_psutil_android_compat, _is_android_python, _npm_bin_exists,
    _npm_lockfile_changed, _npm_manifest_paths, _npm_manifests_digest, _path_uid,
    _rebuild_desktop_after_update, _record_npm_lockfile_hash, _refresh_active_lazy_features,
    _refresh_active_memory_provider_dependencies, _refuse_update_if_venv_foreign_owned,
    _repair_node_deps_on_current_checkout, _restore_active_tool_dependencies,
    _sync_python_dependencies_after_pull, _update_node_dependencies,
    _upgrade_pip_before_lazy_refresh, _validate_critical_modules_import,
    _venv_core_imports_healthy, _venv_foreign_owned_paths, _web_build_toolchain_ready,
    _web_toolchain_roots)
from hermes_cli.update_cmd_git import (  # noqa: F401
    OFFICIAL_REPO_URL, OFFICIAL_REPO_URLS, SKIP_UPSTREAM_PROMPT_FILE, _ORPHAN_RESCUE_REFS_TO_KEEP,
    _ORPHAN_RESCUE_REF_MAX_AGE_DAYS, _add_upstream_remote, _assess_parked_branch_switch,
    _branch_head_label, _branch_head_suffix, _classify_fetch_failure, _count_commits_between,
    _discard_lockfile_churn, _ensure_non_trampoline_git, _get_origin_url, _git_is_trampoline,
    _has_upstream_remote, _is_fork, _locate_real_git, _mark_skip_upstream_prompt,
    _normalize_managed_eol, _portable_git_candidates, _print_fetch_failure,
    _print_parked_branch_kept_notice, _print_parked_branch_skip_warning,
    _prune_orphan_rescue_refs, _should_skip_upstream_prompt, _sync_fork_with_upstream,
    _sync_with_upstream_if_needed)
from hermes_cli.update_cmd_maint import (  # noqa: F401
    _PRE_UPDATE_SNAPSHOT_KEEP, _PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE,
    _STALE_PURGE_PROTECTED,
    _UPDATE_RUNTIME_RELOAD_MODULES, _clear_stale_sqlite_sidecars,
    _ensure_acp_launcher, _ensure_fhs_path_guard, _finish_dashboard_update_cleanup,
    _format_time_ago, _post_update_sqlite_runtime_status, _print_bundled_skills_sync_report,
    _print_curator_first_run_notice, _print_curator_recent_run_notice,
    _print_fts_optimize_available_notice, _print_update_completion, _print_update_summary,
    _print_verified_update_completion, _purge_stale_hermes_modules, _read_project_version,
    _reload_process_scan_modules, _reload_updated_runtime_modules,
    _resolve_pre_update_backup_mode, _restore_state_db_from_snapshot,
    _run_post_update_maintenance, _run_pre_update_backup, _stale_purge_prefixes,
    _sweep_bytecode_after_update,
    _update_complete_message, _verify_and_restore_one_state_db,
    _verify_and_restore_state_dbs_post_update)
logger = logging.getLogger(__name__)


def _m():
    """Lazy ``hermes_cli.main`` handle: keeps main-side test patches effective, import one-way."""
    from hermes_cli import main
    return main


def _updates_config() -> dict:
    """The ``updates:`` config section (``{}`` when absent/malformed); may raise on config errors."""
    from hermes_cli.config import load_config
    section = (load_config() or {}).get("updates", {})
    return section if isinstance(section, dict) else {}


def _no_prompt_git_kwargs() -> dict:
    """``subprocess.run`` kwargs for network git: a 401 (GitHub outage) would block forever on
    ``Username for ...``; disable only the *prompt* (credential helpers still run) so it fails fast."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "Never"}
    return {"stdin": subprocess.DEVNULL, "env": env}


# CLI-startup files (+ web_server.py, launched by a fresh Windows Desktop install) that must
# parse post-update; the syntax guard rolls back when one doesn't.
_UPDATE_CRITICAL_FILES = (
    "hermes_cli/main.py", "hermes_cli/config.py", "hermes_cli/__init__.py",
    "hermes_cli/web_server.py", "cli.py", "run_agent.py", "model_tools.py", "toolsets.py",
    "hermes_constants.py")


def _record_update_step(step: str, ok: bool, detail: str = "") -> None:
    """Best-effort ``update_receipt.record_step``; the receipt must never break an update."""
    with suppress(Exception):
        from hermes_cli.update_receipt import record_step
        record_step(step, ok, detail)


# A fetch whose transport dead-stalls (HTTP/2 to GitHub on some networks, a black-holed proxy)
# otherwise leaves `hermes update` on "Fetching updates..." forever (#93759, #95777). Five
# minutes is generous for a scoped single-branch fetch and still ends in a real error.
NETWORK_GIT_TIMEOUT_SECONDS = 300


def _git_run(git_cmd, args, cwd=None, *, check=False, network=False):
    """Run git capturing utf-8 text (default cwd: checkout); ``network=True`` disables the
    terminal prompt so an HTTP 401 fails fast instead of hanging, and bounds the wait."""
    try:
        return subprocess.run(
            git_cmd + args, cwd=_m().PROJECT_ROOT if cwd is None else cwd, capture_output=True,
            text=True, encoding="utf-8", errors="replace", check=check,
            **({"timeout": NETWORK_GIT_TIMEOUT_SECONDS, **_no_prompt_git_kwargs()} if network else {}))
    except subprocess.TimeoutExpired as exc:
        # subprocess.run already killed the child; the checkout stays consistent because
        # fetch writes to tmp_pack_* and only renames on success. Report as a failed run
        # so every caller's existing stderr path prints one clear line.
        result = subprocess.CompletedProcess(
            exc.cmd, 124, stdout="",
            stderr=f"git {args[0]} timed out after {NETWORK_GIT_TIMEOUT_SECONDS}s with no response from the remote")
        if check:
            raise subprocess.CalledProcessError(124, exc.cmd, output="", stderr=result.stderr) from exc
        return result


def _capture_head_sha(git_cmd, cwd) -> str | None:
    """Return the current HEAD SHA, or None if it can't be resolved."""
    try:
        result = _git_run(git_cmd, ["rev-parse", "HEAD"], cwd, check=True)
        return result.stdout.strip() or None
    except (subprocess.CalledProcessError, OSError):
        return None


def _validate_python_files_syntax(root, relpaths) -> tuple[bool, str | None, str | None]:
    """Compile *relpaths* under *root*; the .pyc goes to a temp dir, not ``__pycache__/`` (no
    race with test workers, no stale pyc for another interpreter)."""
    import py_compile
    import tempfile
    root = Path(root)
    with tempfile.TemporaryDirectory(prefix="hermes-syntax-check-") as tmpdir:
        for relpath in relpaths:
            path = root / relpath
            if not path.exists():
                continue
            cfile = Path(tmpdir) / (str(relpath).replace("/", "__") + "c")
            try:
                py_compile.compile(str(path), cfile=str(cfile), doraise=True)
            except py_compile.PyCompileError as exc:
                return False, str(path), str(exc)
            except OSError as exc:
                return False, str(path), f"could not read: {exc}"
    return True, None, None


def _validate_critical_files_syntax(root) -> tuple[bool, str | None, str | None]:
    """Compile ``_UPDATE_CRITICAL_FILES`` -> ``(ok, failing_path, error_message)``."""
    return _validate_python_files_syntax(root, _UPDATE_CRITICAL_FILES)


def _gateway_prompt(prompt_text: str, default: str = "", timeout: float = 300.0) -> str:
    """File-based IPC prompt for ``--gateway``: write a marker the gateway forwards to the
    messenger, poll for a response file, fall back to *default* on timeout."""
    import json as _json
    import uuid as _uuid
    from hermes_constants import get_hermes_home  # noqa: F811  (deliberate: constants variant)
    home = get_hermes_home()
    prompt_path, response_path = home / ".update_prompt.json", home / ".update_response"
    response_path.unlink(missing_ok=True)

    payload = {"prompt": prompt_text, "default": default, "id": str(_uuid.uuid4())}
    tmp = prompt_path.with_suffix(".tmp")
    tmp.write_text(_json.dumps(payload), encoding="utf-8")
    tmp.replace(prompt_path)

    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if response_path.exists():
            with suppress(OSError, ValueError):
                answer = response_path.read_text(encoding="utf-8").strip()
                response_path.unlink(missing_ok=True)
                prompt_path.unlink(missing_ok=True)
                return answer if answer else default
        _time.sleep(0.5)

    prompt_path.unlink(missing_ok=True)
    response_path.unlink(missing_ok=True)
    print(f"  (no response after {int(timeout)}s, using default: {default!r})")
    return default


def _called_process_error_cmd_parts(exc: subprocess.CalledProcessError) -> list[str]:
    """Normalize ``CalledProcessError.cmd`` into argv-style tokens."""
    cmd = exc.cmd
    if cmd is None:
        return []
    if isinstance(cmd, (str, bytes)):
        text = cmd.decode("utf-8", "replace") if isinstance(cmd, bytes) else cmd
        try:
            return shlex.split(text, posix=os.name != "nt")
        except ValueError:
            return text.split()
    return [str(part) for part in cmd]


def _called_process_error_is_git(exc: subprocess.CalledProcessError) -> bool:
    """True when the failed subprocess was git itself."""
    parts = _called_process_error_cmd_parts(exc)
    if not parts:
        return False
    # Windows argv may use backslashes; POSIX basename() would keep the whole path.
    name = os.path.basename(parts[0].replace("\\", "/")).lower()
    return name in {"git", "git.exe"}


def _called_process_error_is_python_dep_install(exc: subprocess.CalledProcessError) -> bool:
    """True when the failed subprocess was a uv/pip (or ensurepip) install."""
    parts = [part.lower() for part in _called_process_error_cmd_parts(exc)]
    if not parts:
        return False
    exe = os.path.basename(parts[0].replace("\\", "/"))
    return "ensurepip" in parts or ("install" in parts and (
        "pip" in parts or exe in {"pip", "pip.exe", "pip3", "pip3.exe", "uv", "uv.exe"}))


def _format_update_failure_stage(exc: subprocess.CalledProcessError) -> str:
    """Name the failed stage: git pull and dep install share one ``try``, and calling every
    CalledProcessError a git failure misled users and keyed the ZIP overlay on exception
    *type* rather than on git actually failing.

    See #85840, #87304.
    """
    if _called_process_error_is_python_dep_install(exc):
        return "Python dependency install failed"
    if _called_process_error_is_git(exc):
        return "Git update failed"
    return "Update step failed"


def _shim_quarantine_error_type() -> "type[BaseException]":
    """Strict-quarantine refusal type via ``_m()``; falls back to a never-raised private
    type when main.py lacks it (torn mid-update tree) so the ``except`` stays valid."""
    cls = getattr(_m(), "ShimQuarantineError", None)
    if isinstance(cls, type) and issubclass(cls, BaseException):
        return cls

    class _Never(Exception):
        pass

    return _Never


def _refuse_update_for_contended_shims(exc: BaseException) -> None:
    """Fail closed when live shims could not be quarantined: a rename failing every retry
    proves a holder without FILE_SHARE_DELETE, and installing anyway strands the venv between
    versions. The code swap is already committed; only the dep install is deferred (via the
    update-incomplete marker). Exits 2 so the receipt records a refusal, not a failure.

    See #87331.
    """
    print("✗ Cannot continue the update: live Hermes launcher(s) could not be")
    print("  moved aside:")
    for name in getattr(exc, "failed_shims", []) or ["hermes.exe"]:
        print(f"    {name}")
    print("  Another process is holding this install's venv — typically Hermes")
    print("  Desktop, a gateway, or another hermes REPL — and mutating the venv")
    print("  now would strand it half-updated.")
    print("  The dependency install has been deferred: close the process(es)")
    print("  above, then run any `hermes` command to finish it automatically.")
    # Idempotent (git path already dropped it); covers ZIP/repair paths so the deferral is never silent.
    _write_update_incomplete_marker()
    sys.exit(2)


def _should_zip_fallback_on_update_error(exc: BaseException) -> bool:
    """ZIP fallback is only for Windows git file-I/O breakage: after a dep-install failure the
    pull already succeeded, so a ZIP overlay can't fix it and would replace every top-level
    entry except venv/node_modules/.git/.env, deleting uncommitted and untracked files."""
    return (
        isinstance(exc, subprocess.CalledProcessError)
        and _m()._is_windows()
        and _called_process_error_is_git(exc))


def _print_called_process_error_tail(exc: subprocess.CalledProcessError, *, limit: int = 12) -> None:
    """Print a captured stderr/stdout tail when the failing call recorded one."""
    blob = exc.stderr or exc.stdout or ""
    if isinstance(blob, bytes):
        blob = blob.decode("utf-8", "replace")
    lines = [line for line in str(blob).splitlines() if line.strip()]
    if not lines:
        return
    print("  Last output:")
    for line in lines[-limit:]:
        print(f"    {line}")


def _zip_overlay_block_reason(
    root: Path, *, ignore_staging_artifacts: bool = False
) -> Optional[str]:
    """Why overlaying a ZIP onto ``root`` would destroy work, or None if safe.

    The ZIP path swaps every top-level entry (except a tiny preserve set) and
    then deletes the backups, so uncommitted edits and untracked files under
    a replaced directory are gone. Fail closed when git status cannot run:
    unknown dirtiness is not a license to clobber the tree (#87304).

    ``ignore_staging_artifacts`` is for the pre-swap re-check: phase 1 of the
    two-phase replace creates ``*.hermes-update-staging`` siblings inside the
    checkout, which git reports as untracked. Those are our own artifacts,
    not user work — without the filter the re-check would always refuse.
    """
    if not (root / ".git").exists():
        return None
    git_cmd = ["git"]
    if sys.platform == "win32":
        git_cmd = ["git", "-c", "windows.appendAtomically=false"]
    result = subprocess.run(
        # -uall: a user-level ``status.showUntrackedFiles = no`` git config
        # would otherwise hide untracked files and silently blind this guard.
        # --ignored=matching: gitignored files are still USER DATA the ZIP
        # overlay would permanently delete (logs, scratch files, local data)
        # — a .gitignore entry must not blind the guard either (#87392).
        # ``matching`` reports an ignored directory as one ``dir/`` line
        # instead of enumerating its contents (cheaper, same verdict for the
        # top-level filter below). NOTE: ``--ignored=all`` is NOT a valid
        # git mode — it exits 128 and would fail-close every ZIP update.
        git_cmd + ["status", "--porcelain", "--untracked-files=all", "--ignored=matching"],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        suffix = f" ({detail[0]})" if detail else ""
        return f"could not check the working tree{suffix}"
    lines = [line for line in (result.stdout or "").splitlines() if line.strip()]
    # --ignored=all reports the ZIP path's own preserved entries (venv,
    # node_modules are gitignored on every normal install). The swap never
    # touches those top-level entries, so they must not turn into a false
    # dirty-tree refusal. Everything else — including ignored files — blocks.
    lines = [line for line in lines if not _is_zip_preserved_entry_status_line(line)]
    if ignore_staging_artifacts:
        lines = [
            line for line in lines if not _is_zip_staging_artifact_status_line(line)
        ]
    if lines:
        return "the working tree has uncommitted changes or untracked files"
    return None


_ZIP_STAGING_ARTIFACT_SUFFIXES = (".hermes-update-staging", ".hermes-update-old")
# Single source of truth for the top-level entries the ZIP swap preserves —
# consumed by both the dirty-tree filter below and _update_via_zip's swap loop.
_ZIP_PRESERVED_TOP_LEVEL = {"venv", "node_modules", ".git", ".env"}


def _is_zip_preserved_entry_status_line(line: str) -> bool:
    """True when every path on a porcelain status line sits under a top-level
    entry the ZIP swap preserves.

    The ``" -> "`` two-path split applies ONLY to rename/copy status codes
    (R/C): porcelain v1 does not quote a plain filename containing spaces,
    so an ignored file literally named ``venv -> node_modules`` on an
    ``!!``/``??`` line must be treated as ONE path — splitting it would
    filter it as two preserved tops and fail-open into the destructive swap.
    Requiring EVERY path preserved keeps renames leaving a preserved dir
    (``R venv/x -> src/x``) blocking, fail-closed.
    """
    status, payload = (line[:2], line[3:]) if len(line) >= 3 else ("", line)
    is_rename = any(code in "RC" for code in status)
    paths = payload.split(" -> ") if is_rename else [payload]
    for path in paths:
        top_level = (
            path.strip().strip('"').replace("\\", "/").rstrip("/").split("/", 1)[0]
        )
        if top_level not in _ZIP_PRESERVED_TOP_LEVEL:
            return False
    return True


def _is_zip_staging_artifact_status_line(line: str) -> bool:
    """True when a porcelain status line is our own two-phase-swap artifact."""
    payload = line[3:] if len(line) >= 3 else line
    top_level = (
        payload.strip().strip('"').replace("\\", "/").rstrip("/").split("/", 1)[0]
    )
    return top_level.endswith(_ZIP_STAGING_ARTIFACT_SUFFIXES)


def _abort_zip_update_if_dirty_tree() -> None:
    """Refuse to overlay a ZIP onto a dirty git checkout (#87304)."""
    reason = _zip_overlay_block_reason(_m().PROJECT_ROOT)
    if reason is None:
        return
    print(f"✗ ZIP fallback refused: {reason}.")
    print(
        "  Overlaying the ZIP would overwrite uncommitted edits and permanently "
        "delete untracked files."
    )
    print("  Stash or commit your changes, then rerun `hermes update`.")
    print("  To inspect: git status --porcelain")
    _m().sys.exit(1)


def _read_project_version() -> str | None:
    """Read the ``version`` field from the checkout's pyproject.toml.

    Reads the on-disk file (not importlib.metadata) because after a git
    pull the installed distribution metadata still describes the OLD
    version; the file is the only source that reflects what was just
    pulled. Returns None on any failure — version reporting is cosmetic
    and must never break an update.
    """
    try:
        import tomllib

        with open(_m().PROJECT_ROOT / "pyproject.toml", "rb") as fh:  # windows-footgun: ok — binary mode, tomllib requires bytes
            version = tomllib.load(fh).get("project", {}).get("version")
        return str(version) if version else None
    except Exception:
        return None


def _update_complete_message(pre_version: str | None) -> str:
    """Completion line with the version transition when it is known.

    Ported from PrimeIntellect-ai/prime-agent#630: after a successful
    self-update, show both versions (``v0.19.4 → v0.20.0``) so the user
    can see what they actually got. Falls back to the plain message when
    either side is unknown or the version did not change (e.g. several
    commits landed within one release).
    """
    post_version = _read_project_version()
    if pre_version and post_version and pre_version != post_version:
        return f"✓ Update complete! (v{pre_version} → v{post_version})"
    if post_version:
        return f"✓ Update complete! (v{post_version})"
    return "✓ Update complete!"
 
 
def _clear_stale_sqlite_sidecars(db_path: Path) -> None:
    """Delete the WAL / shared-memory / rollback-journal files next to *db_path*.

    Call this immediately before overwriting a database file with a snapshot
    image. Quick snapshots are produced by ``backup._safe_copy_db`` through
    ``sqlite3.backup()``, so the image is already checkpointed and owns no WAL —
    which is exactly why ``backup._EXCLUDED_SUFFIXES`` refuses to ship sidecars
    inside a snapshot. Copying the image over the destination replaces only the
    main database file, so any ``-wal`` / ``-shm`` left behind by the *old*
    database (a crashed writer, or a second Hermes process the updater's drain
    did not stop) survives and is replayed over the fresh image on the next
    open. The result passes ``PRAGMA integrity_check`` while serving the old
    database's contents, and the first checkpoint folds it in permanently.

    Removing them is safe here specifically: they belong to a database the
    caller has already declared corrupt and is about to discard.
    """
    for suffix in ("-wal", "-shm", "-journal"):
        db_path.with_name(db_path.name + suffix).unlink(missing_ok=True)


def _print_update_summary(
    *,
    node_failures: list,
    desktop_build_ok: bool,
    pre_update_version: str | None,
) -> None:
    """Final update banner. A failed Desktop rebuild is non-fatal for the
    Python side, but must not print ``✓ Update complete!`` (#88251)."""
    print()
    if node_failures or not desktop_build_ok:
        parts = []
        if node_failures:
            parts.append(
                f"Node.js dependencies for {', '.join(node_failures)} did not refresh"
            )
        if not desktop_build_ok:
            parts.append(
                "the desktop app was not rebuilt and is still on the previous build"
            )
        print("⚠ Update partially complete — " + "; ".join(parts) + ".")
        if node_failures:
            print("  Code and Python deps are updated, but the dashboard/TUI may")
            print("  be in a mixed state until the Node deps are rebuilt.")
        if not desktop_build_ok:
            print("  Run `hermes desktop` to retry the desktop rebuild.")
    else:
        _print_update_completion(_update_complete_message(pre_update_version))


def _write_gateway_update_exit_code(ok: bool) -> None:
    path = get_hermes_home() / ".update_exit_code"
    try:
        path.write_text("0" if ok else "1", encoding="utf-8")
    except OSError:
        pass


def _restore_state_db_from_snapshot(state_path: Path, snap_state: Path) -> bool:
    """Replace *state_path* with the snapshot image at *snap_state*.

    Shared by both post-update auto-restore paths (the ZIP update and the git
    pull). The destination's stale sidecars are cleared before the copy, so the
    restored image cannot be silently overwritten by the corrupt database's WAL
    replay — see :func:`_clear_stale_sqlite_sidecars`.

    Refuses (returns ``False``) while another process still holds the database
    or its sidecars open: copying a snapshot over a live writer's inode makes
    the writer's page cache and WAL index disagree with the file bytes, and
    its next checkpoint writes pages at offsets that no longer mean what it
    thinks — the #90950 page-1 clobber. ``None`` (scan unavailable) proceeds:
    the updater has already drained gateways, and refusing on "unknown" would
    disable auto-restore on every non-Linux host.

    Returns ``True`` when the restored file passes an integrity check. Raises
    ``OSError`` if the copy itself fails, which callers already report.
    """
    from hermes_cli.backup import _foreign_db_holder_pids, verify_sqlite_integrity

    holders = _foreign_db_holder_pids(state_path)
    if holders:
        print(
            f"  ✗ Auto-restore refused: process(es) {holders} still hold "
            "state.db or its WAL open. Stop them (hermes gateway stop), "
            "then restore manually with /snapshot restore."
        )
        return False
    _clear_stale_sqlite_sidecars(state_path)
    shutil.copy2(snap_state, state_path)
    restored = verify_sqlite_integrity(
        state_path, check_header=True, run_pragma=True
    )
    return bool(restored.get("valid"))


def _update_via_zip(args, *, had_desktop_app_before_update: bool = False) -> bool:
    """Update Hermes Agent by downloading a ZIP archive.

    Used on Windows when git file I/O is broken (antivirus, NTFS filter
    drivers causing 'Invalid argument' errors on file creation).

    Returns ``False`` when a Desktop rebuild ran and failed; ``True`` otherwise.
    """
    active_tool_dependencies = _m()._capture_active_tool_dependencies()

    import tempfile
    import zipfile
    from urllib.request import urlretrieve

    # Snapshot the pre-update version before files are replaced so the
    # completion line can report the transition (prime-agent#630 port).
    pre_update_version = _read_project_version()

    # The ZIP fallback exists for Windows git-file-I/O breakage. It pulls a
    # static archive from GitHub, which is fine for the default "main"
    # channel but would silently ignore --branch and update from main even
    # if the user asked for something else — exactly the silent-divergence
    # bug --branch was added to prevent. Refuse to proceed in that case
    # rather than lie.
    branch = _m()._resolve_update_branch(args)
    if branch != "main":
        print(
            f"✗ --branch={branch} is not supported on the Windows ZIP-fallback "
            "update path."
        )
        print(
            "  This path runs when git file I/O is broken on the system. "
            "Either resolve the git-side breakage (typically an antivirus "
            "or NTFS filter holding files open) and rerun `hermes update "
            f"--branch {branch}`, or update against main with `hermes update`."
        )
        _m().sys.exit(1)
    _abort_zip_update_if_dirty_tree()
    zip_url = (
        f"https://github.com/NousResearch/hermes-agent/archive/refs/heads/{branch}.zip"
    )

    print("→ Downloading latest version...")
    tmp_dir = tempfile.mkdtemp(prefix="hermes-update-")
    try:
        zip_path = os.path.join(tmp_dir, f"hermes-agent-{branch}.zip")
        urlretrieve(zip_url, zip_path)

        print("→ Extracting...")
        import stat as _stat
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Validate paths to prevent zip-slip (path traversal) AND reject
            # symlink members. A GitHub source ZIP for hermes-agent itself
            # should never contain symlinks — they'd point outside the
            # extracted tree and let an attacker who can compromise the
            # update mirror plant arbitrary files via the update path.
            tmp_dir_real = os.path.realpath(tmp_dir)
            for member in zf.infolist():
                member_path = os.path.realpath(os.path.join(tmp_dir, member.filename))
                if (
                    not member_path.startswith(tmp_dir_real + os.sep)
                    and member_path != tmp_dir_real
                ):
                    raise ValueError(
                        f"Zip-slip detected: {member.filename} escapes extraction directory"
                    )
                # Unix mode lives in the upper 16 bits of external_attr;
                # mask to the file-type bits.
                mode = (member.external_attr >> 16) & 0o170000
                if _stat.S_ISLNK(mode):
                    raise ValueError(
                        f"ZIP contains unsupported symlink member: {member.filename}"
                    )
            zf.extractall(tmp_dir)

        # GitHub ZIPs extract to hermes-agent-<branch>/
        extracted = os.path.join(tmp_dir, f"hermes-agent-{branch}")
        if not os.path.isdir(extracted):
            # Try to find it
            for d in os.listdir(tmp_dir):
                candidate = os.path.join(tmp_dir, d)
                if os.path.isdir(candidate) and d != "__MACOSX":
                    extracted = candidate
                    break

        # Copy updated files over existing installation, preserving venv/node_modules/.git
        preserve = _ZIP_PRESERVED_TOP_LEVEL
        entries = [i for i in os.listdir(extracted) if i not in preserve]

        # Two-phase replace (#76104). Phase 1 copies every entry — directories
        # AND top-level files — to a sibling staging path without touching
        # anything live; phase 2 swaps them all in with same-filesystem
        # renames and rolls back every swap if any one fails. Replacing
        # entries one-at-a-time (the previous shape) meant an interruption
        # partway left `agent/` new and `tools/` stale — all files valid, the
        # tree unbootable. Files matter as much as directories here: the repo
        # root holds 20 first-party modules (run_agent.py, cli.py,
        # hermes_constants.py, ...).
        #
        # Staging costs one extra copy of the tree on disk. Check up front so
        # we fail with a clear message instead of running out mid-copy.
        need = sum(
            os.path.getsize(os.path.join(dirpath, f))
            for entry in entries
            for dirpath, _dirs, files in os.walk(os.path.join(extracted, entry))
            for f in files
        ) + sum(
            os.path.getsize(os.path.join(extracted, e))
            for e in entries
            if os.path.isfile(os.path.join(extracted, e))
        )
        # Only the staging copy is new — the live tree already occupies its
        # space and the swaps are renames, not copies. Ask for the staging
        # copy plus 20% headroom rather than a full 2x, which would block
        # updates that would have succeeded on exactly the space-constrained
        # machines most likely to hit this path.
        required = int(need * 1.2)
        free = shutil.disk_usage(str(_m().PROJECT_ROOT)).free
        if free < required:
            raise RuntimeError(
                f"not enough free disk space to stage the update safely "
                f"(need ~{required // (1024 * 1024)} MB, have "
                f"{free // (1024 * 1024)} MB)"
            )

        staged: list[tuple[str, str]] = []
        try:
            for item in entries:
                src = os.path.join(extracted, item)
                dst = os.path.join(str(_m().PROJECT_ROOT), item)
                staged.append((_stage_replacement(src, dst), dst))
                # #70337/#87331: the GitHub source ZIP contains only source —
                # apps/desktop/release/ (the BUILT desktop app, win-unpacked/
                # Hermes.exe) exists only in the LIVE tree. Swapping `apps`
                # without it deletes the desktop build and breaks the
                # shortcut. Graft the live release dir into the staged copy
                # BEFORE the swap so the commit preserves it atomically.
                if item == "apps":
                    live_release = os.path.join(dst, "desktop", "release")
                    staged_release = os.path.join(
                        staged[-1][0], "desktop", "release"
                    )
                    if os.path.isdir(live_release) and not os.path.exists(
                        staged_release
                    ):
                        os.makedirs(os.path.dirname(staged_release), exist_ok=True)
                        shutil.copytree(live_release, staged_release)
        except Exception:
            # Nothing is live yet; drop the partial staging copies so a retry
            # starts from the same free space this attempt did.
            _discard_staged(staged)
            raise

        try:
            # Re-check the tree right before the swap (#87304 TOCTOU): the
            # download + extract + staging window above can take minutes, and
            # work created in it would be destroyed by the commit below. Our
            # own phase-1 staging siblings are filtered out — they are the
            # expected artifacts of getting here, not user work.
            recheck_reason = _zip_overlay_block_reason(
                _m().PROJECT_ROOT, ignore_staging_artifacts=True
            )
            if recheck_reason is not None:
                _discard_staged(staged)
                print(f"✗ ZIP fallback aborted before the swap: {recheck_reason}.")
                print(
                    "  Files appeared in the checkout while the update was "
                    "downloading; committing the swap would delete them."
                )
                print("  Stash or commit your changes, then rerun `hermes update`.")
                _m().sys.exit(1)
            _commit_staged_replacements(staged)
        except Exception:
            # The rollback already restored every swapped entry, but staging
            # copies for the not-yet-swapped entries (potentially most of a
            # full tree) are still on disk. Drop them, or the retry's
            # up-front free-space check — which runs BEFORE the lazy
            # per-entry leftover cleanup — fails on litter this attempt
            # left behind: the exact "retry fails harder" failure mode
            # _discard_staged exists to prevent. Safe post-rollback: swapped
            # entries' staging paths were renamed away, and _discard_staged
            # skips paths that no longer exist.
            _discard_staged(staged)
            raise
        update_count = len(staged)

        print(f"✓ Updated {update_count} items from ZIP")

    except Exception as e:
        print(f"✗ ZIP update failed: {e}")
        # The two-phase replace either commits every entry or rolls them all
        # back, so a failure here does not leave a mixed-version tree — don't
        # scare the user toward a reinstall they don't need.
        print("  Your existing install was left in place.")
        print(
            "  Re-run `hermes update` to retry; if the agent won't start, "
            "reinstall from https://hermes-agent.nousresearch.com"
        )
        _m().sys.exit(1)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # Clear stale bytecode after ZIP extraction
    removed = _m()._clear_bytecode_cache(_m().PROJECT_ROOT)
    if removed:
        print(
            f"  ✓ Cleared {removed} stale __pycache__ director{'y' if removed == 1 else 'ies'}"
        )
    _m()._record_bytecode_fingerprint()
    _m()._refresh_bootstrap_cache_scripts(branch)

    # Reinstall Python dependencies. Prefer .[all], but if one optional extra
    # breaks on this machine, keep base deps and reinstall the remaining extras
    # individually so update does not silently strip working capabilities.
    #
    # Self-lock deferral (relocated preflight — #86735): the ZIP code swap
    # above is already committed; defer only the dependency sync when this
    # process holds a native extension the sync must rewrite.
    _m()._abort_dependency_sync_if_self_locked()
    print("→ Updating Python dependencies...")

    from hermes_cli.managed_uv import ensure_uv, update_managed_uv

    # Keep managed uv current — runs `uv self update` if we already have one.
    update_managed_uv()

    uv_bin = ensure_uv()

    pip_cmd = [_m().sys.executable, "-m", "pip"]
    if not uv_bin:
        uv_bin = _ensure_uv_for_termux(pip_cmd)
    if uv_bin:
        # Same third-party UV-env isolation as the main update path (#83914):
        # a user-level UV_PYTHON_INSTALL_DIR / UV_PYTHON from unrelated
        # software must not steer which interpreter uv resolves here.
        from hermes_cli.managed_uv import managed_python_env

        uv_env = managed_python_env()
        uv_env["VIRTUAL_ENV"] = str(_m().PROJECT_ROOT / "venv")
        if _m()._is_termux_env(uv_env):
            uv_env.pop("PYTHONPATH", None)
            uv_env.pop("PYTHONHOME", None)
        try:
            _m()._install_python_dependencies_with_optional_fallback([uv_bin, "pip"], env=uv_env)
        except _shim_quarantine_error_type() as _sqe:
            # #87331: this runs inside the ZIP-fallback error handler, so the
            # boundary except clause in cmd_update cannot catch it — refuse
            # here with the same defer-via-marker contract.
            _refuse_update_for_contended_shims(_sqe)
    else:
        # Use sys.executable to explicitly call the venv's pip module,
        # avoiding PEP 668 'externally-managed-environment' errors on Debian/Ubuntu.
        # Some environments lose pip inside the venv; bootstrap it back with
        # ensurepip before trying the editable install.
        try:
            subprocess.run(
                pip_cmd + ["--version"],
                cwd=_m().PROJECT_ROOT,
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError:
            subprocess.run(
                [_m().sys.executable, "-m", "ensurepip", "--upgrade", "--default-pip"],
                cwd=_m().PROJECT_ROOT,
                check=True,
            )
        _m()._install_python_dependencies_with_optional_fallback(pip_cmd)

    install_prefix = [uv_bin, "pip"] if uv_bin else pip_cmd
    install_env = uv_env if uv_bin else None
    _m()._restore_active_tool_dependencies(
        active_tool_dependencies,
        install_prefix,
        env=install_env,
    )

    # ZIP path parity: heal the active memory provider's bridge packages
    # after the dependency reinstall, same as the git-pull path (#53272,
    # #70636).
    _m()._refresh_active_memory_provider_dependencies()

    # Now that dependencies are installed, verify the tree actually imports.
    # The copy loop above replaces top-level entries one at a time in
    # os.listdir order, so an interruption between (say) `agent/` and `tools/`
    # leaves a tree whose files all parse but cannot be imported together —
    # the ImportError-on-startup class this guard exists to catch. Deliberately
    # placed *after* the dependency reinstall so a genuinely-new third-party
    # requirement isn't misreported as a partial copy. There is no SHA to roll
    # back to here, so surface it with a concrete recovery step rather than
    # reporting a successful update over a bricked install.
    import_ok, failing_module, import_error = _validate_critical_modules_import(
        _m().PROJECT_ROOT
    )
    if not import_ok:
        print()
        print("✗ Update left the install in an unimportable state:")
        print(f"  {failing_module}: {import_error}")
        print()
        print("  This usually means the copy was interrupted partway through.")
        print("  Re-run `hermes update` to complete it.")
        _m().sys.exit(1)

    node_failures = _update_node_dependencies()
    _m()._build_web_ui(_m().PROJECT_ROOT / "web")
    desktop_build_ok = _rebuild_desktop_after_update(
        _m().PROJECT_ROOT / "apps" / "desktop",
        had_desktop_app_before_update=had_desktop_app_before_update,
    )

    # Sync skills
    try:
        from tools.skills_sync import sync_skills

        print("→ Syncing bundled skills...")
        result = sync_skills(quiet=True)
        if result["copied"]:
            print(f"  + {len(result['copied'])} new: {', '.join(result['copied'])}")
        if result.get("updated"):
            print(
                f"  ↑ {len(result['updated'])} updated: {', '.join(result['updated'])}"
            )
        if result.get("user_modified"):
            print(f"  ~ {len(result['user_modified'])} user-modified (kept)")
            print(
                "    → see them: hermes skills list-modified  "
                "(diff/reset to resume updates)"
            )
        if result.get("cleaned"):
            print(f"  − {len(result['cleaned'])} removed from manifest")
        if result.get("relocated"):
            print(
                f"  → {len(result['relocated'])} moved to new upstream paths: "
                f"{', '.join(result['relocated'])}"
            )
        if not result["copied"] and not result.get("updated"):
            print("  ✓ Skills are up to date")
    except Exception:
        pass

    # Seed the model-catalog disk cache from the freshly-unpacked checkout
    # (same rationale as the git-pull path in _cmd_update_impl). Non-fatal.
    try:
        from hermes_cli.model_catalog import seed_cache_from_checkout

        if seed_cache_from_checkout(_m().PROJECT_ROOT):
            print("  ✓ Model catalog cache refreshed from checkout")
    except Exception as e:
        logger.debug("Model catalog seed during zip update failed: %s", e)

    # ── Post-update state.db integrity guard (#68474) ─────────────────
    # Same as the git-pull path: verify state.db survived the ZIP update
    # and auto-restore from the most recent pre-update snapshot if needed.
    try:
        from hermes_cli.backup import _quick_snapshot_root, verify_sqlite_integrity

        _state_path = get_hermes_home() / "state.db"
        if _state_path.exists():
            _state_ok = verify_sqlite_integrity(
                _state_path, check_header=True, run_pragma=True
            )
            if not _state_ok.get("valid"):
                print()
                print(
                    "⚠ state.db is corrupted after update: "
                    + _state_ok.get("message", "unknown error")
                )
                _snap_root = _quick_snapshot_root(get_hermes_home())
                if _snap_root.exists():
                    _snap_dirs = sorted(
                        (d for d in _snap_root.iterdir() if d.is_dir()),
                        reverse=True,
                    )
                    for _snap_dir in _snap_dirs:
                        _snap_state = _snap_dir / "state.db"
                        if _snap_state.exists():
                            _snap_ok = verify_sqlite_integrity(
                                _snap_state, check_header=True, run_pragma=True
                            )
                            if _snap_ok.get("valid"):
                                try:
                                    if _restore_state_db_from_snapshot(
                                        _state_path, _snap_state
                                    ):
                                        print(
                                            "  ✓ Auto-restored from snapshot "
                                            f"{_snap_dir.name}"
                                        )
                                    else:
                                        print(
                                            "  ✗ Auto-restore FAILED — restored "
                                            "copy also failed integrity"
                                        )
                                    break
                                except OSError as _exc:
                                    print(
                                        f"  ✗ Auto-restore file copy failed: {_exc}"
                                    )
                                    break
    except Exception as exc:
        logger.debug(
            "Post-update state.db integrity check (zip path) failed: %s", exc
        )

    _print_update_summary(
        node_failures=node_failures,
        desktop_build_ok=desktop_build_ok,
        pre_update_version=pre_update_version,
    )
    try:
        _print_curator_first_run_notice()
    except Exception as e:
        logger.debug("Curator first-run notice failed: %s", e)
    try:
        _print_curator_recent_run_notice()
    except Exception as e:
        logger.debug("Curator recent-run notice failed: %s", e)
    # Don't stop a working dashboard when the Node refresh failed — see the
    # git-update path for rationale (#30271).
    _finish_dashboard_update_cleanup(node_failures)
    try:
        from hermes_cli.update_receipt import finalize_update_receipt

        finalize_update_receipt(
            "success" if (desktop_build_ok and not node_failures) else "partial"
        )
    except Exception as _receipt_exc:
        logger.debug("Update receipt finalize (zip path) failed: %s", _receipt_exc)
    return desktop_build_ok

def _commit_and_push_local_changes_if_needed(git_cmd: list[str], cwd: Path, branch: str) -> None:
    status = subprocess.run(
        git_cmd + ["status", "--porcelain"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        check=True,
    )
    if not status.stdout.strip():
        return None

    # If the index has unmerged entries (e.g. from an interrupted merge/rebase),
    # commit will fail. Clear the conflict state with `git reset` first.
    unmerged = subprocess.run(
        git_cmd + ["ls-files", "--unmerged"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if unmerged.stdout.strip():
        print("→ Clearing unmerged index entries from a previous conflict...")
        subprocess.run(git_cmd + ["reset"], cwd=cwd, capture_output=True)

    from datetime import datetime, timezone

    subprocess.run(git_cmd + ["add", "-A"], cwd=cwd, capture_output=True)

    staged = subprocess.run(
        git_cmd + ["status", "--porcelain"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if not staged.stdout.strip():
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit_msg = f"chore: auto-commit before hermes update ({timestamp})"
    print("→ Local changes detected — committing before update...")

    commit_res = subprocess.run(
        git_cmd + ["commit", "-m", commit_msg],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if commit_res.returncode != 0:
        print(f"✗ Update failed: Auto-commit failed: {commit_res.stderr.strip()}")
        return False

    track_check = subprocess.run(
        git_cmd + ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )

    push_args = ["push", "origin", branch]
    if track_check.returncode != 0:
        push_args = ["push", "-u", "origin", branch]

    print(f"→ Pushing auto-commit to origin/{branch}...")

    env_file = Path("/home/hermes/.hermes/github-highamperage.env")
    push_env = os.environ.copy()
    push_env.pop("COPILOT_GITHUB_TOKEN", None)
    push_env["GIT_TERMINAL_PROMPT"] = "0"
    push_env["GIT_ASKPASS"] = "echo"

    github_token = None
    github_username = None
    if env_file.exists():
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k == "GITHUB_TOKEN":
                        github_token = v
                    elif k == "GITHUB_USERNAME":
                        github_username = v

    if not github_token or not github_username:
        print("✗ Update failed: Missing GITHUB_TOKEN or GITHUB_USERNAME in github-highamperage.env")
        return False

    import urllib.request
    import json
    try:
        req = urllib.request.Request("https://api.github.com/user")
        req.add_header("Authorization", f"Bearer {github_token}")
        with urllib.request.urlopen(req, timeout=10) as response:
            user_data = json.loads(response.read().decode())
            if user_data.get("login") != "highamperage":
                print(f"✗ Update failed: GitHub token owner is {user_data.get('login')}, expected highamperage")
                return False
    except Exception as e:
        print(f"✗ Update failed: Could not verify GitHub token owner: {e}")
        return False

    import tempfile
    helper_fd, helper_path = tempfile.mkstemp(prefix="hermes-git-cred-", suffix=".py", text=True)
    try:
        with os.fdopen(helper_fd, 'w') as f:
            f.write("import os\n")
            f.write("print('username=' + os.environ.get('_HERMES_UPDATE_USER', ''))\n")
            f.write("print('password=' + os.environ.get('_HERMES_UPDATE_TOKEN', ''))\n")

        os.chmod(helper_path, 0o700)

        push_env["_HERMES_UPDATE_USER"] = github_username
        push_env["_HERMES_UPDATE_TOKEN"] = github_token

        py_exe = sys.executable.replace('\\', '/')
        h_path = helper_path.replace('\\', '/')
        helper_cmd = f'!\"{py_exe}\" \"{h_path}\"'

        push_res = subprocess.run(
            git_cmd + ["-c", "credential.helper=", "-c", f"credential.helper={helper_cmd}", *push_args],
            cwd=cwd,
            env=push_env,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if push_res.returncode == 0:
            print(f"✓ Committed and pushed local changes to origin/{branch} before updating.")
        else:
            print(f"✗ Update failed: Failed to push to origin/{branch}.")
            err_lines = push_res.stderr.strip().splitlines()
            if err_lines:
                err_msg = err_lines[0].replace(github_token, "***") if github_token else err_lines[0]
                print(f"  ({err_msg})")
            return False
    finally:
        try:
            os.remove(helper_path)
        except OSError:
            pass


def _stash_local_changes_if_needed(git_cmd: list[str], cwd: Path) -> Optional[str]:
    status = subprocess.run(
        git_cmd + ["status", "--porcelain"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        check=True,
    )
    if not status.stdout.strip():
        return None

    # If the index has unmerged entries (e.g. from an interrupted merge/rebase),
    # git stash will fail with "needs merge / could not write index".  Clear the
    # conflict state with `git reset` so the stash can proceed.  Working-tree
    # changes are preserved; only the index conflict markers are dropped.
    unmerged = subprocess.run(
        git_cmd + ["ls-files", "--unmerged"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if unmerged.stdout.strip():
        print("→ Clearing unmerged index entries from a previous conflict...")
        subprocess.run(git_cmd + ["reset"], cwd=cwd, capture_output=True)

    from datetime import datetime, timezone

    stash_name = datetime.now(timezone.utc).strftime(
        "hermes-update-autostash-%Y%m%d-%H%M%S"
    )
    print("→ Local changes detected — stashing before update...")
    prev_stash = subprocess.run(
        git_cmd + ["rev-parse", "--verify", "refs/stash"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    ).stdout.strip()
    push = subprocess.run(
        git_cmd + ["stash", "push", "--include-untracked", "-m", stash_name],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if push.stdout.strip():
        print(push.stdout.strip())
    stash_probe = subprocess.run(
        git_cmd + ["rev-parse", "--verify", "refs/stash"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    stash_ref = stash_probe.stdout.strip()
    stash_created = (
        stash_probe.returncode == 0 and bool(stash_ref) and stash_ref != prev_stash
    )

    if push.returncode != 0:
        if stash_created:
            # git stash push exits non-zero when it saved everything but could
            # not delete some swept untracked files from the working tree
            # (e.g. a root-owned directory: "warning: failed to remove ...:
            # Permission denied").  The stash entry is complete — the changes
            # are safe — so this is not a failure.  Leave the undeletable
            # files in place and continue the update.
            if push.stderr.strip():
                print(push.stderr.strip())
            print(
                "  ⚠ Some untracked files could not be removed from the "
                "working tree (permission denied)."
            )
            print(
                "    They were still saved to the stash and were left in "
                "place — the update will continue."
            )
            # A partially-failed stash push also aborts its working-tree
            # cleanup for TRACKED modifications — they are saved in the stash
            # but still dirty the tree, which would break the checkout/pull
            # that follows. Safe to reset: everything is in the stash entry.
            subprocess.run(
                git_cmd + ["reset", "--hard", "HEAD"],
                cwd=cwd,
                capture_output=True,
            )
        else:
            # No stash entry was created: the changes were NOT saved.  This
            # is a real failure — bail out before the update touches HEAD.
            print("✗ Could not stash local changes — update aborted.")
            if push.stderr.strip():
                print(f"  {push.stderr.strip().splitlines()[0]}")
            print(
                "  Commit, stash, or clean up your local changes manually, "
                "then re-run `hermes update`."
            )
            raise subprocess.CalledProcessError(
                push.returncode, push.args, output=push.stdout, stderr=push.stderr
            )

    return stash_ref

def _resolve_stash_selector(
    git_cmd: list[str], cwd: Path, stash_ref: str
) -> Optional[str]:
    stash_list = subprocess.run(
        git_cmd + ["stash", "list", "--format=%gd %H"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        check=True,
    )
    for line in stash_list.stdout.splitlines():
        selector, _, commit = line.partition(" ")
        if commit.strip() == stash_ref:
            return selector.strip()
    return None

def _print_stash_cleanup_guidance(
    stash_ref: str, stash_selector: Optional[str] = None
) -> None:
    print(
        "  Check `git status` first so you don't accidentally reapply the same change twice."
    )
    print("  Find the saved entry with: git stash list --format='%gd %H %s'")
    if stash_selector:
        print(f"  Remove it with: git stash drop {stash_selector}")
    else:
        print(
            f"  Look for commit {stash_ref}, then drop its selector with: git stash drop stash@{{N}}"
        )

def _stash_apply_failed_only_on_existing_untracked(stderr: str) -> bool:
    """True when a ``git stash apply`` failure is ONLY about untracked files
    that already exist in the working tree.

    This is the tail end of the permission-denied autostash class: ``git stash
    push --include-untracked`` swept undeletable files (e.g. a root-owned
    ``packaging/`` directory) into the stash but could not remove them from
    disk.  On restore, git applies all tracked changes, then refuses to
    overwrite those still-present files (``already exists, no checkout`` /
    ``could not restore untracked files from stash``) and exits non-zero even
    though nothing was lost.  Any other error line (e.g. ``would be
    overwritten by merge`` / ``Aborting``) means the tracked apply itself
    failed and this returns False.
    """
    lines = [ln.strip() for ln in (stderr or "").splitlines() if ln.strip()]
    if not lines:
        return False
    saw_untracked_error = False
    for ln in lines:
        if "already exists, no checkout" in ln:
            saw_untracked_error = True
        elif "could not restore untracked files from stash" in ln:
            saw_untracked_error = True
        elif ln.startswith(("warning:", "hint:")):
            continue
        else:
            return False
    return saw_untracked_error

def _park_stashed_changes(stash_ref: str) -> None:
    """Leave a pre-update autostash parked instead of re-applying it.

    Used by ``hermes update --keep-stash`` (the desktop updater's mode): the
    stash made the update possible on a dirty tree, but local source edits
    must never be silently re-applied onto the updated code. Nothing is
    lost — the entry stays in ``git stash`` with printed recovery guidance.
    """
    print()
    print("ℹ️  Local changes were stashed before updating and were NOT re-applied (--keep-stash).")
    print(f"  Stash ref: {stash_ref}")
    print(f"  Restore manually with: git stash apply {stash_ref}")


def _restore_stashed_changes(
    git_cmd: list[str],
    cwd: Path,
    stash_ref: str,
    prompt_user: bool = False,
    input_fn=None,
) -> bool:
    if prompt_user:
        print()
        print("⚠ Local changes were stashed before updating.")
        print(
            "  Restoring them may reapply local customizations onto the updated codebase."
        )
        print("  Review the result afterward if Hermes behaves unexpectedly.")
        print("Restore local changes now? [Y/n]")
        if input_fn is not None:
            response = input_fn("Restore local changes now? [Y/n]", "y")
        else:
            try:
                response = input().strip().lower()
            except (EOFError, UnicodeDecodeError):
                # Mirror the config-migration prompt's fix: don't let a
                # terminal-encoding issue or a closed stdin crash the
                # update mid-restore. Falls through to the existing
                # skip-restore path below, which already explains how to
                # restore manually from git stash.
                response = "n"
        if response not in {"", "y", "yes"}:
            print("Skipped restoring local changes.")
            print("Your changes are still preserved in git stash.")
            print(f"Restore manually with: git stash apply {stash_ref}")
            return False

    print("→ Restoring local changes...")
    restore = subprocess.run(
        git_cmd + ["stash", "apply", stash_ref],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )

    # Check for unmerged (conflicted) files — can happen even when returncode is 0
    unmerged = subprocess.run(
        git_cmd + ["diff", "--name-only", "--diff-filter=U"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    has_conflicts = bool(unmerged.stdout.strip())

    if restore.returncode != 0 and not has_conflicts and (
        _stash_apply_failed_only_on_existing_untracked(restore.stderr)
    ):
        # Permission-denied autostash tail end: the tracked changes applied
        # cleanly; the only "failure" is untracked files that never left the
        # working tree (git could not delete them at stash time, so it now
        # refuses to overwrite them). Their content was never touched —
        # nothing is lost. Treat as restored.
        print(
            "  ⚠ Some stashed untracked files already exist in the working "
            "tree and were kept as-is."
        )
    elif restore.returncode != 0 or has_conflicts:
        print("✗ Update pulled new code, but restoring local changes hit conflicts.")
        if restore.stdout.strip():
            print(restore.stdout.strip())
        if restore.stderr.strip():
            print(restore.stderr.strip())

        # Show which files conflicted
        conflicted_files = unmerged.stdout.strip()
        if conflicted_files:
            print("\nConflicted files:")
            for f in conflicted_files.splitlines():
                print(f"  • {f}")

        print("\nYour stashed changes are preserved — nothing is lost.")
        print(f"  Stash ref: {stash_ref}")

        # Always reset to clean state — leaving conflict markers in source
        # files makes hermes completely unrunnable (SyntaxError on import).
        # The user's changes are safe in the stash for manual recovery.
        subprocess.run(
            git_cmd + ["reset", "--hard", "HEAD"],
            cwd=cwd,
            capture_output=True,
        )
        print("Working tree reset to clean state.")
        print(f"Restore your changes later with: git stash apply {stash_ref}")
        # Don't sys.exit — the code update itself succeeded, only the stash
        # restore had conflicts.  Let cmd_update continue with pip install,
        # skill sync, and gateway restart.
        return False

    stash_selector = _resolve_stash_selector(git_cmd, cwd, stash_ref)
    if stash_selector is None:
        print(
            "⚠ Local changes were restored, but Hermes couldn't find the stash entry to drop."
        )
        print(
            "  The stash was left in place. You can remove it manually after checking the result."
        )
        _print_stash_cleanup_guidance(stash_ref)
    else:
        drop = subprocess.run(
            git_cmd + ["stash", "drop", stash_selector],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if drop.returncode != 0:
            print(
                "⚠ Local changes were restored, but Hermes couldn't drop the saved stash entry."
            )
            if drop.stdout.strip():
                print(drop.stdout.strip())
            if drop.stderr.strip():
                print(drop.stderr.strip())
            print(
                "  The stash was left in place. You can remove it manually after checking the result."
            )
            _print_stash_cleanup_guidance(stash_ref, stash_selector)

    print("⚠ Local changes were restored on top of the updated codebase.")
    print("  Review `git diff` / `git status` if Hermes behaves unexpectedly.")
    return True

def _discard_stashed_changes(
    git_cmd: list[str],
    cwd: Path,
    stash_ref: str,
) -> bool:
    """Throw away a stash created before an update, without applying it.

    Used only on a NON-interactive update when the user has set
    ``updates.non_interactive_local_changes: discard`` — i.e. they've opted out
    of keeping local source edits on this machine. Drops the stash entry
    instead of re-applying it, so the working tree stays clean at the freshly
    pulled HEAD. Unlike ``git reset --hard`` + ``git clean -fd``, this only
    affects what was stashed (tracked changes + the untracked files we
    explicitly captured) — ignored paths like node_modules/venv/build outputs
    are never touched, since they were never stashed.

    Returns True if the stash was dropped, False on a git failure (in which
    case the stash is left in place for safety).
    """
    stash_selector = _resolve_stash_selector(git_cmd, cwd, stash_ref)
    if stash_selector is None:
        print(
            "⚠ Configured to discard local changes on non-interactive update, "
            "but Hermes couldn't find the stash entry to drop."
        )
        _print_stash_cleanup_guidance(stash_ref)
        return False

    drop = subprocess.run(
        git_cmd + ["stash", "drop", stash_selector],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    if drop.returncode != 0:
        print(
            "⚠ Configured to discard local changes, but Hermes couldn't drop "
            "the saved stash entry."
        )
        if drop.stderr.strip():
            print(f"  {drop.stderr.strip().splitlines()[0]}")
        _print_stash_cleanup_guidance(stash_ref, stash_selector)
        return False

    print("→ Discarded local source changes (updates.non_interactive_local_changes=discard).")
    return True

OFFICIAL_REPO_URLS = {
    "https://github.com/NousResearch/hermes-agent.git",
    "git@github.com:NousResearch/hermes-agent.git",
    "https://github.com/NousResearch/hermes-agent",
    "git@github.com:NousResearch/hermes-agent",
}

OFFICIAL_REPO_URL = "https://github.com/NousResearch/hermes-agent.git"

SKIP_UPSTREAM_PROMPT_FILE = ".skip_upstream_prompt"

def _get_origin_url(git_cmd: list[str], cwd: Path) -> Optional[str]:
    """Get the URL of the origin remote, or None if not set."""
    try:
        result = subprocess.run(
            git_cmd + ["remote", "get-url", "origin"],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None

def _is_fork(origin_url: Optional[str]) -> bool:
    """Check if the origin remote points to a fork (not the official repo)."""
    if not origin_url:
        return False
    # Normalize URL for comparison (strip trailing .git if present)
    normalized = origin_url.rstrip("/")
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    for official in OFFICIAL_REPO_URLS:
        official_normalized = official.rstrip("/")
        if official_normalized.endswith(".git"):
            official_normalized = official_normalized[:-4]
        if normalized == official_normalized:
            return False
    return True

def _has_upstream_remote(git_cmd: list[str], cwd: Path) -> bool:
    """Check if an 'upstream' remote already exists."""
    try:
        result = subprocess.run(
            git_cmd + ["remote", "get-url", "upstream"],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        return result.returncode == 0
    except Exception:
        return False

def _add_upstream_remote(git_cmd: list[str], cwd: Path) -> bool:
    """Add the official repo as the 'upstream' remote. Returns True on success."""
    try:
        result = subprocess.run(
            git_cmd + ["remote", "add", "upstream", OFFICIAL_REPO_URL],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        return result.returncode == 0
    except Exception:
        return False

def _count_commits_between(git_cmd: list[str], cwd: Path, base: str, head: str) -> int:
    """Count commits on `head` that are not on `base`. Returns -1 on error."""
    try:
        result = subprocess.run(
            git_cmd + ["rev-list", "--count", f"{base}..{head}"],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode == 0:
            return int(result.stdout.strip())
    except Exception:
        pass
    return -1

def _should_skip_upstream_prompt() -> bool:
    """Check if user previously declined to add upstream."""
    from hermes_constants import get_hermes_home

    return (get_hermes_home() / SKIP_UPSTREAM_PROMPT_FILE).exists()

def _mark_skip_upstream_prompt():
    """Create marker file to skip future upstream prompts."""
    try:
        from hermes_constants import get_hermes_home

        (get_hermes_home() / SKIP_UPSTREAM_PROMPT_FILE).touch()
    except Exception:
        pass

def _sync_with_upstream_if_needed(git_cmd: list[str], cwd: Path) -> bool:
    """Check if fork has an upstream remote and sync from upstream before updating.

    Returns True if update should continue (sync succeeded, or no sync needed).
    Returns False if update should abort (merge conflict, syntax check failure, push failure).
    """
    has_upstream = _has_upstream_remote(git_cmd, cwd)
    if not has_upstream:
        if _should_skip_upstream_prompt():
            return True

        # Ask user if they want to add upstream
        print()
        print("ℹ Your fork is not tracking the official Hermes repository.")
        print("  This means you may miss updates from NousResearch/hermes-agent.")
        print()
        try:
            response = (
                input("Add official repo as 'upstream' remote? [Y/n]: ").strip().lower()
            )
        except (EOFError, KeyboardInterrupt, UnicodeDecodeError):
            print()
            response = "n"

        if response in {"", "y", "yes"}:
            print("→ Adding upstream remote...")
            if _add_upstream_remote(git_cmd, cwd):
                print(
                    "  ✓ Added upstream: https://github.com/NousResearch/hermes-agent.git"
                )
                has_upstream = True
            else:
                print("  ✗ Failed to add upstream remote. Skipping upstream sync.")
                return True
        else:
            print(
                "  Skipped. Run 'git remote add upstream https://github.com/NousResearch/hermes-agent.git' to add later."
            )
            _mark_skip_upstream_prompt()
            return True

    # Determine branch to sync against (current checked-out branch or default main)
    current_branch = "main"
    try:
        res = subprocess.run(
            git_cmd + ["rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
            check=True,
        )
        out = res.stdout.strip()
        if out and out != "HEAD":
            current_branch = out
    except Exception:
        pass

    # Verify worktree is clean before attempting any merge
    try:
        res = subprocess.run(
            git_cmd + ["status", "--porcelain"],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
            check=True,
        )
        if res.stdout.strip():
            print("  ⚠ Worktree is dirty — skipping upstream fork sync to preserve uncommitted changes.")
            return True
    except Exception:
        pass

    # Fetch upstream branch
    print()
    print(f"→ Fetching upstream {current_branch}...")
    try:
        fetch_res = subprocess.run(
            git_cmd + ["fetch", "upstream", current_branch],
            cwd=cwd,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if fetch_res.returncode != 0:
            print("  ✗ Failed to fetch from upstream. Skipping upstream sync.")
            return True
    except Exception:
        print("  ✗ Failed to fetch from upstream. Skipping upstream sync.")
        return True

    # Check if upstream is already contained in HEAD
    upstream_ref = f"upstream/{current_branch}"
    try:
        ancestor_res = subprocess.run(
            git_cmd + ["merge-base", "--is-ancestor", upstream_ref, "HEAD"],
            cwd=cwd,
            capture_output=True,
        )
        if ancestor_res.returncode == 0:
            print("  ✓ Upstream is already current.")
            return True
    except Exception:
        pass

    # Upstream has new commits not contained in HEAD -> Merge upstream into current_branch
    print()
    print(f"→ Upstream has new changes. Merging {upstream_ref} into {current_branch}...")
    merge_res = subprocess.run(
        git_cmd + ["merge", upstream_ref, "--no-edit"],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )

    if merge_res.returncode != 0:
        # Merge conflict detected! Find conflicting files
        conflicts = []
        try:
            diff_res = subprocess.run(
                git_cmd + ["diff", "--name-only", "--diff-filter=U"],
                cwd=cwd,
                capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            )
            conflicts = [f.strip() for f in diff_res.stdout.splitlines() if f.strip()]
        except Exception:
            pass

        # Abort merge so worktree remains clean
        subprocess.run(
            git_cmd + ["merge", "--abort"],
            cwd=cwd,
            capture_output=True,
        )

        print()
        print(f"✗ Merge conflict detected between local changes and {upstream_ref}.")
        if conflicts:
            print("Conflicting files:")
            for f in conflicts:
                print(f"  - {f}")
        print("Merge aborted to keep worktree clean.")
        print()
        print("Options to resolve:")
        print(f"  1. Ask AGY to resolve the conflicts for you.")
        print(f"  2. Manually merge: git merge {upstream_ref}, resolve conflicts, and re-run update.")
        return False

    # Validate post-merge critical files syntax
    syntax_ok, failing_file, err_msg = _validate_critical_files_syntax(cwd)
    if not syntax_ok:
        print(f"✗ Post-merge syntax check failed on {failing_file}: {err_msg}")
        return False

    # Push successful merge to origin
    print(f"→ Pushing merged updates to origin/{current_branch}...")
    push_res = subprocess.run(
        git_cmd + ["push", "origin", current_branch],
        cwd=cwd,
        capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )

    if push_res.returncode != 0:
        stderr = push_res.stderr.strip()
        print()
        print(f"✗ Push to origin/{current_branch} failed.")
        if stderr:
            print(f"  {stderr.splitlines()[0]}")
        print("The upstream merge succeeded locally, but could not be pushed to origin.")
        print(f"Please check your git push credentials or run 'git push origin {current_branch}' manually.")
        return False

    print(f"  ✓ Successfully merged {upstream_ref} and pushed to origin/{current_branch}.")
    return True

def _invalidate_update_cache():
    """Delete the update-check cache for ALL profiles: the repo is shared, so one profile's
    update makes every profile current and a stale "commits behind" banner would linger."""
    default_home = get_default_hermes_root()
    profiles_root = default_home / "profiles"
    homes = [default_home]
    if profiles_root.is_dir():
        homes += [entry for entry in profiles_root.iterdir() if entry.is_dir()]
    for home in homes:
        with suppress(Exception):
            (home / ".update_check").unlink(missing_ok=True)


def _write_marker_file(path: Path, *, label: str) -> None:
    """Drop an update-recovery breadcrumb. Never raises."""
    if _m()._pytest_owns_live_checkout(path.parent):
        logger.debug("Skipping %s marker under pytest (live checkout)", label)
        return
    try:
        path.write_text(f"started={_time.time()}\npid={os.getpid()}\n", encoding="utf-8")
    except OSError as exc:
        logger.debug("Could not write %s marker: %s", label, exc)


def _write_update_incomplete_marker() -> None:
    """Drop the interrupted core-install breadcrumb. Never raises."""
    _write_marker_file(_m()._update_marker_path(), label="update-incomplete")


def _write_lazy_refresh_incomplete_marker() -> None:
    """Drop the interrupted lazy-refresh breadcrumb. Never raises."""
    _write_marker_file(_m()._lazy_refresh_marker_path(), label="lazy-refresh-incomplete")


def _format_concurrent_instances_message(matches: list[tuple[int, str]], scripts_dir: Path) -> str:
    """Explanation + remediation hint for the Windows concurrent-hermes.exe gate."""
    shim = scripts_dir / "hermes.exe"
    lines = [
        "✗ Another hermes.exe is running:",
        *(f"    PID {pid}  {name}" for pid, name in matches),
        "",
        f"  Updating now would fail to overwrite {shim} because",
        "  Windows blocks REPLACE on a running executable.",
        "",
        "  Close Hermes Desktop, exit any open `hermes` REPLs, and",
        "  stop the gateway (`hermes gateway stop`) before retrying.",
        ""]
    if matches:
        pid_args = " ".join(f"/PID {pid}" for pid, _ in matches)
        lines += [
            "  If you've already closed everything and these PIDs are",
            "  stale, terminate them directly, then retry the update:",
            f"      taskkill {pid_args} /F",
            ""]
    lines += [
        "  Override with `hermes update --force` if you've already",
        "  confirmed those processes will not write to the venv."]
    return "\n".join(lines)


def _classify_concurrent_instance(pid: int) -> str:
    """Classify ``pid`` as "gateway" / "non-gateway" / "unknown" (psutil can't read it). Uses
    ``_is_pausable_gateway`` (same matcher as the Desktop preflight and venv-holder guard) so
    "gateway" is exactly what the pause/restart machinery stops; "unknown" gates as non-gateway."""
    try:
        import psutil  # noqa: PLC0415
        cmdline_list = psutil.Process(int(pid)).cmdline()
    except Exception:
        return "unknown"

    from hermes_cli._scan_venv_blockers import _is_pausable_gateway  # noqa: PLC0415
    return "gateway" if _is_pausable_gateway(" ".join(cmdline_list or [])) else "non-gateway"


def _filter_non_gateway_concurrent_instances(matches: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """Drop gateway matches (the pause + post-update restart machinery handles them); anything else
    (TUI, Desktop backend child, another REPL) has no pause path, so the gate aborts."""
    return [(pid, name) for pid, name in matches if _classify_concurrent_instance(pid) != "gateway"]


def _log_only_write(text: str) -> None:
    """Write to update.log only: reaches past the ``_UpdateOutputStream`` stdout mirror so
    loud, low-signal subprocess output stays debuggable without flooding the terminal."""
    if not text:
        return
    stream = _m().sys.stdout
    log_file = getattr(stream, "_log", None)
    with suppress(Exception):
        if log_file is None:
            log_path = get_hermes_home() / "logs" / "update.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as fallback:
                fallback.write(text)
        else:
            log_file.write(text)
            log_file.flush()


def _run_logged_subprocess(cmd, *, cwd=None, env=None):
    """Stream combined build output to update.log, retaining it for failure reporting."""
    import codecs
    import io
    from hermes_cli._subprocess_compat import kill_process_tree, windows_hide_flags

    child_env = dict(os.environ if env is None else env)
    child_env.setdefault("PYTHONUNBUFFERED", "1")
    spawn = {"creationflags": windows_hide_flags()} if os.name == "nt" else {"process_group": 0}
    proc = subprocess.Popen(
        cmd, cwd=cwd, env=child_env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **spawn)
    # read1 delivers partial lines too; incremental decoding preserves split UTF-8
    # and the universal-newline behavior callers previously got from text=True.
    decoder = io.IncrementalNewlineDecoder(codecs.getincrementaldecoder("utf-8")("replace"), True)
    output = []
    try:
        while True:
            chunk = proc.stdout.read1(8192)
            text = decoder.decode(chunk, final=not chunk)
            output.append(text)
            _log_only_write(text)
            if not chunk:
                break
        return subprocess.CompletedProcess(cmd, proc.wait(), stdout="".join(output))
    except BaseException:
        # Unlike Popen.__exit__, do not wait for a cancelled build to finish.
        kill_process_tree(proc)
        with suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        raise
    finally:
        proc.stdout.close()


def _cmd_update_check(branch: str = "main", *, branch_explicit: bool = False):
    """``hermes update --check``: fetch and report without installing. ``branch_explicit`` is
    True iff --branch was passed (Docker installs print a notice instead of dropping the flag)."""
    # Same marker-first admission gate as the apply path, so --check never reports git
    # state for an install whose real update mechanism is an image pull.
    from hermes_cli.update_contract import evaluate_update_admission, record_refusal_receipt

    refusal = evaluate_update_admission(_m().PROJECT_ROOT)
    if refusal is not None:
        print(refusal.message)
        record_refusal_receipt(refusal)
        sys.exit(2)

    git_dir = _m().PROJECT_ROOT / ".git"
    if not git_dir.exists():
        print("✗ Not a git repository — cannot check for updates.")
        sys.exit(1)

    git_cmd = _base_git_cmd()

    # Interrupted fetches leave .git/*.lock behind ("File exists" forever); self-heal first.
    from hermes_cli.gitlock import clear_stale_git_locks, clear_stale_tmp_packs
    for lock_path in clear_stale_git_locks(_m().PROJECT_ROOT):
        print(f"  (removed stale git lock: {lock_path})")
    # Aborted fetches also strand tmp_pack_* debris (has reached 6 GB and corrupted the
    # pack dir); same age+process safety contract as the locks.
    swept = clear_stale_tmp_packs(_m().PROJECT_ROOT)
    if swept:
        print(f"  (removed {len(swept)} aborted-fetch pack temp file(s))")

    # Fetch only <branch> (a bare fetch pulls thousands of auto-generated branches). Prefer
    # upstream only for main (a fork's other branches have no upstream counterpart). Installer
    # checkouts are shallow: a plain fetch would unshallow them and rev-list would report a
    # bogus huge "behind" count, so fetch --depth 1 and report presence-only.
    is_shallow = _is_shallow_checkout(git_cmd)
    depth_args = ["--depth", "1"] if is_shallow else []

    # Probe locally for an 'upstream' remote before a network fetch non-forks always fail.
    fetch_result = None
    if branch == "main" and _git_run(git_cmd, ["remote", "get-url", "upstream"]).returncode == 0:
        print("→ Fetching from upstream...")
        fetch_result = _git_run(git_cmd, ["fetch"] + depth_args + ["upstream", branch], network=True)
    if fetch_result is not None and fetch_result.returncode == 0:
        compare_branch = f"upstream/{branch}"
    else:
        print("→ Fetching from origin...")
        fetch_result = _git_run(git_cmd, ["fetch"] + depth_args + ["origin", branch], network=True)
        compare_branch = f"origin/{branch}"

    if fetch_result.returncode != 0:
        _print_fetch_failure(fetch_result.stderr)
        sys.exit(1)

    if is_shallow:
        # The depth-1 fetch above leaves the previous tip behind as a ``.git/shallow`` graft
        # (git never removes old grafts); prune the stale ones so the file stops growing and
        # merge-base / the orphan-divergence heuristic keep working (#105951).
        from hermes_cli.gitlock import repair_broken_shallow_boundaries, prune_stale_shallow_grafts
        repaired = repair_broken_shallow_boundaries(_m().PROJECT_ROOT)
        if repaired:
            print(f"  (restored {repaired} broken shallow boundary(ies))")
        pruned = prune_stale_shallow_grafts(_m().PROJECT_ROOT)
        if pruned:
            print(f"  (pruned {pruned} stale shallow graft(s) left by past depth-1 checks)")

    # rev-list on a bogus ref exits 128 and (check=True) would traceback; verify first.
    verify_result = _git_run(git_cmd, ["rev-parse", "--verify", "--quiet", compare_branch])
    if verify_result.returncode != 0:
        print(f"✗ Branch '{branch}' not found on {compare_branch.split('/', 1)[0]}.")
        sys.exit(1)

    if is_shallow:
        # No history across the shallow boundary: compare tip SHAs, then recover the
        # exact count via the GitHub compare API (complete graph).
        head_sha, target_sha = _tip_shas(git_cmd, compare_branch)
        if head_sha and target_sha and head_sha == target_sha:
            print("✓ Already up to date.")
            return
        from hermes_cli.banner import _github_compare_behind
        # counted == 0 means local-ahead, not behind; None means the API could not count.
        _print_update_check_result(_github_compare_behind(head_sha, target_sha), compare_branch)
        return

    rev_result = _git_run(git_cmd, ["rev-list", f"HEAD..{compare_branch}", "--count"], check=True)
    _print_update_check_result(int(rev_result.stdout.strip()), compare_branch)


def _base_git_cmd() -> list[str]:
    """``git`` argv; Windows adds ``-c windows.appendAtomically=false`` (git can fail "unable to
    write loose object file: Invalid argument" on non-atomic appends)."""
    if sys.platform == "win32":
        return ["git", "-c", "windows.appendAtomically=false"]
    return ["git"]


def _is_shallow_checkout(git_cmd) -> bool:
    return _git_run(git_cmd, ["rev-parse", "--is-shallow-repository"]).stdout.strip() == "true"


def _tip_shas(git_cmd, target_ref: str) -> tuple[str, str]:
    """``(HEAD sha, <target_ref> sha)`` as printed by rev-parse ("" when unresolvable)."""
    return tuple(_git_run(git_cmd, ["rev-parse", ref]).stdout.strip() for ref in ("HEAD", target_ref))


def _print_update_check_result(behind: int | None, compare_branch: str) -> None:
    """Report ``--check``'s verdict: up to date, N commits behind, or behind by an unknown count."""
    if behind == 0:
        print("✓ Already up to date.")
        return
    if behind is not None:
        print(f"☤ Update available: {behind} {'commit' if behind == 1 else 'commits'} behind {compare_branch}.")
    else:
        print(f"☤ Update available (behind {compare_branch}).")
    from hermes_cli.config import recommended_update_command
    print(f"  Run '{recommended_update_command()}' to install.")


def _repair_venv_on_current_checkout(
    *, assume_yes, gateway_mode, pre_update_snapshot_id,
    had_desktop_app_before_update, active_lazy_features, active_tool_dependencies,
    _windows_gateway_resume) -> bool:
    """Reinstall ``.[all]`` + lazy/tool deps into an unhealthy (or handed-off) venv; returns
    whether the checkout can be reported complete."""
    # Self-lock deferral: the repair rewrites the venv too (same mapped-extension hazard).
    # See #86735.
    # Self-lock deferral (relocated preflight — #86735): if THIS process holds a native extension the sync
    # must rewrite, defer NOW — after the code swap, so only the dependency install is pending and the next
    # fresh launch completes it via the marker.
    _m()._abort_dependency_sync_if_self_locked(_windows_gateway_resume)
    _write_update_incomplete_marker()
    from hermes_cli.managed_uv import ensure_uv
    repair_uv = ensure_uv()
    # Venv gone entirely (repair interrupted after the old one was moved aside): recreate.
    venv_python_missing = not (
        venv_python_path(_m().PROJECT_ROOT / "venv", windows=_m()._is_windows())).exists()
    if venv_python_missing and repair_uv:
        print("→ Recreating virtual environment...")
        subprocess.run([repair_uv, "venv", "venv"], cwd=_m().PROJECT_ROOT, check=False)
    repair_prefix, repair_env = _pip_install_prefix(repair_uv)
    _m()._install_python_dependencies_with_optional_fallback(repair_prefix, env=repair_env, group="all")
    _m()._refresh_active_lazy_features(repair_prefix, env=repair_env, features=active_lazy_features)
    _m()._restore_active_tool_dependencies(active_tool_dependencies, repair_prefix, env=repair_env)
    # Core ``.[all]`` install finished. Clear the generic core breadcrumb before the lazy-refresh phase —
    # that phase uses its own marker so a later lazy failure cannot be "healed" by clearing the core marker
    # based on a narrow 7-package import probe (#58004 review).
    _m()._clear_update_incomplete_marker()
    healthy_after, detail_after = _venv_core_imports_healthy()
    if not healthy_after:
        print(f"⚠ Venv still unhealthy after repair: {detail_after}")
        print("  Close all Hermes windows/gateways and re-run: hermes update")
        return False
    print("✓ Dependencies repaired!")
    # The hand-off child never reaches the commits-pulled Node/web/Desktop
    # phase. Finish through the current-checkout repair path, whose npm digest
    # gate keeps this cheap when the pulled manifests did not change.
    return _repair_node_deps_on_current_checkout(
        _print_verified_update_completion,
        assume_yes=assume_yes,
        gateway_mode=gateway_mode,
        pre_update_snapshot_id=pre_update_snapshot_id,
        completion_message="✓ Update complete!",
        had_desktop_app_before_update=had_desktop_app_before_update,
    )


def _pip_install_prefix(uv_bin) -> tuple[list[str], dict | None]:
    """``(install prefix, env)``: ``uv pip`` isolated from third-party UV env vars (so a foreign
    UV_PYTHON_INSTALL_DIR can't hijack it), else ``sys.executable -m pip`` (avoids PEP 668 errors)."""
    if uv_bin:
        # Same third-party UV-env isolation as the main update path (#83914): a user-level
        # UV_PYTHON_INSTALL_DIR / UV_PYTHON from unrelated software must not steer which interpreter uv
        # resolves here.
        # See #83914.
        from hermes_cli.managed_uv import managed_python_env
        env = managed_python_env()
        env["VIRTUAL_ENV"] = str(_m().PROJECT_ROOT / "venv")
        return [uv_bin, "pip"], env
    return [sys.executable, "-m", "pip"], None


def _repair_current_checkout(
    *, assume_yes, gateway_mode, pre_update_snapshot_id,
    had_desktop_app_before_update, active_lazy_features, active_tool_dependencies,
    upstream_checked, _windows_gateway_resume) -> bool:
    """Already-up-to-date path: keep the managed runtime current, repair a broken venv.
    Returns whether the checkout can be reported complete."""
    # "No new commits" != safe interpreter: uv can keep the same CPython patch while
    # python-build-standalone refreshes the embedded SQLite; keep the boundary hook here too.
    from hermes_cli.managed_uv import ensure_uv, update_managed_uv
    runtime_repairs = []
    update_managed_uv(repair_observer=runtime_repairs.append)
    ensure_uv(repair_observer=runtime_repairs.append)
    runtime_repaired = next((result for result in runtime_repairs if result.repaired), None)

    # A current checkout does NOT imply a healthy install (a prior sync may have died
    # partway, e.g. Windows locked .pyd); probe or "Already up to date!" hides a bricked venv.
    healthy, detail = _venv_core_imports_healthy()
    # The Windows shim hand-off child is current BY DESIGN; its one job is the pending sync,
    # not venv health — without this it would print "Already up to date!" and skip it.
    handed_off_sync = os.environ.get(_m()._UPDATE_REEXEC_ENV) == "1"
    if handed_off_sync:
        print("→ Finishing the dependency install handed off by hermes.exe...")
    elif not healthy:
        print("⚠ Checkout is current, but the venv is unhealthy:")
        print(f"  {detail}")
        print("→ Repairing Python dependencies...")
    if handed_off_sync or not healthy:
        current_checkout_complete = _repair_venv_on_current_checkout(
            assume_yes=assume_yes, gateway_mode=gateway_mode,
            pre_update_snapshot_id=pre_update_snapshot_id,
            had_desktop_app_before_update=had_desktop_app_before_update,
            active_lazy_features=active_lazy_features,
            active_tool_dependencies=active_tool_dependencies,
            _windows_gateway_resume=_windows_gateway_resume)
    else:
        current_checkout_complete = _repair_node_deps_on_current_checkout(
            _print_verified_update_completion, assume_yes=assume_yes, gateway_mode=gateway_mode,
            pre_update_snapshot_id=pre_update_snapshot_id,
            completion_message=(
                "✓ Already up to date!" if upstream_checked
                else "✓ Up to date with your fork (official repo not checked)."),
            had_desktop_app_before_update=had_desktop_app_before_update)
    if runtime_repaired is not None and not _m()._is_windows():
        print()
        print("⚠ Restart required to finish the managed Python runtime repair.")
        print(
            "  Any running Hermes gateways, Desktop backends, or other "
            "long-lived processes still use the previous runtime.")
        print("  Restart each of them to pick up the repaired runtime.")
    return current_checkout_complete


def _reconcile_diverged_checkout(git_cmd, branch: str, pre_pull_sha) -> None:
    """Fast-forward failed: merge on a custom branch (local commits survive) or reset --hard on the
    same branch (rescue ref first when histories share no ancestor). ``sys.exit(1)`` on failure."""
    # A custom branch (local commits atop origin/<branch>) also can't ff, and reset --hard
    # would discard that work: merge instead, stop on conflict.
    _cur_branch = (_git_run(git_cmd, ["branch", "--show-current"]).stdout or "").strip()
    if _cur_branch and _cur_branch != branch:
        print(
            f"  ⚠ Checkout is on custom branch '{_cur_branch}' — "
            f"merging origin/{branch} instead of resetting so local commits survive...")
        # Best-effort safety tag as a recovery anchor.
        _git_run(git_cmd, ["tag", f"pre-update-{_time.strftime('%Y%m%d-%H%M%S')}"])
        if _git_run(git_cmd, ["merge", "--no-edit", f"origin/{branch}"]).returncode != 0:
            _git_run(git_cmd, ["merge", "--abort"])
            print("✗ Merge conflict between local commits and upstream — update stopped, nothing was changed.")
            print(f"  Resolve manually: cd {_m().PROJECT_ROOT} && git merge origin/{branch}")
            print("  Then re-run the update. Local work is untouched.")
            sys.exit(1)
        return
    # Same branch: a true upstream force-push/rebase; local changes are stashed, so reset.
    # Orphan divergence (no common ancestor: corrupted HEAD, re-init) would lose the whole
    # local graph, so park pre_pull_sha behind a rescue ref first.
    merge_base_result = _git_run(git_cmd, ["merge-base", "HEAD", f"origin/{branch}"])
    has_common_ancestor = merge_base_result.returncode == 0 and merge_base_result.stdout.strip()
    if not has_common_ancestor and pre_pull_sha:
        from datetime import datetime as _dt, timezone
        # SHA suffix so two updates in the same second get distinct refs.
        rescue_ref = (
            f"refs/hermes-update-backups/orphan-{branch}-"
            f"{_dt.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{pre_pull_sha[:12]}")
        head = f"  ⚠ Local history shares no common ancestor with origin/{branch} (orphan divergence) — "
        if _git_run(git_cmd, ["update-ref", rescue_ref, pre_pull_sha]).returncode == 0:
            print(
                f"{head}backed up current HEAD to {rescue_ref} before resetting. "
                f"This backup expires after {_ORPHAN_RESCUE_REF_MAX_AGE_DAYS} days.")
        else:
            # update-ref failure is intentionally non-fatal, but never claim a backup exists.
            print(
                f"{head}attempted to back up current HEAD to {rescue_ref} before resetting, "
                f"but the backup write failed (pre-reset SHA was {pre_pull_sha}).")
        _prune_orphan_rescue_refs(git_cmd, _m().PROJECT_ROOT, branch)
    print("  ⚠ Fast-forward not possible (history diverged), resetting to match remote...")
    reset_result = _git_run(git_cmd, ["reset", "--hard", f"origin/{branch}"])
    if reset_result.returncode != 0:
        print(f"✗ Failed to reset to origin/{branch}.")
        if reset_result.stderr.strip():
            print(f"  {reset_result.stderr.strip()}")
        print(f"  Try manually: git fetch origin && git reset --hard origin/{branch}")
        sys.exit(1)


def _rollback_if_pulled_syntax_error(git_cmd, pre_pull_sha) -> None:
    """Post-pull syntax guard: roll back to *pre_pull_sha* and ``sys.exit(1)`` when a critical
    file no longer compiles (a bad admin-merge past CI must not brick the CLI)."""
    syntax_ok, failing_path, syntax_error = _validate_critical_files_syntax(_m().PROJECT_ROOT)
    if syntax_ok:
        return
    print()
    print("✗ Pulled code has a syntax error in a critical file:")
    print(f"  {failing_path}")
    # py_compile errors can be multi-line; show enough for the SyntaxError text.
    for line in str(syntax_error).splitlines()[:6] if syntax_error else ():
        print(f"    {line}")
    print()
    if pre_pull_sha:
        print(f"→ Rolling back to {pre_pull_sha[:10]}...")
        rollback_result = _git_run(git_cmd, ["reset", "--hard", pre_pull_sha])
        if rollback_result.returncode == 0:
            print("  ✓ Rollback complete — your install is unchanged.")
            print("  Try ``hermes update`` again later once a fix lands.")
        else:
            print("  ✗ Rollback failed. Recover manually with:")
            print(f"    cd {_m().PROJECT_ROOT} && git reset --hard {pre_pull_sha}")
            if rollback_result.stderr.strip():
                print(f"    ({rollback_result.stderr.strip().splitlines()[0]})")
    else:
        print("  Could not capture pre-pull SHA — recover manually with:")
        print(f"    cd {_m().PROJECT_ROOT} && git reflog && git reset --hard <prev-sha>")
    sys.exit(1)


def _pull_updates(
    git_cmd, branch, auto_stash_ref, *, prompt_for_restore, gw_input_fn, discard_local_changes,
    keep_stash):
    """Fast-forward onto ``origin/<branch>`` and settle the autostash. Divergence by shape:
    custom branch -> merge, same branch -> reset, orphan history -> rescue ref first; a
    post-pull syntax error in a critical file rolls back. Exits on failure; returns pre-pull SHA."""
    update_succeeded = False
    # Pre-pull SHA for auto-rollback (stray conflict markers once bricked every updater).
    # Capture the pre-pull SHA so we can auto-roll-back if the new code has a syntax error in a
    # critical-path file (PR #28452 incident: orphan merge-conflict markers in hermes_cli/config.py bricked
    # every user who ran ``hermes update`` for the 7 minutes between the bad commit and the fix landing).
    pre_pull_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
    try:
        # merge --ff-only the already-fetched ref instead of `git pull`, which would do a
        # SECOND network fetch; identical in effect given the fresh tracking ref.
        if _git_run(git_cmd, ["merge", "--ff-only", f"origin/{branch}"]).returncode != 0:
            _reconcile_diverged_checkout(git_cmd, branch, pre_pull_sha)
        _rollback_if_pulled_syntax_error(git_cmd, pre_pull_sha)
        update_succeeded = True
    finally:
        if auto_stash_ref is not None:
            # No stash restore if the update failed — tree state is unknown.
            if not update_succeeded:
                print(f"  ℹ️  Local changes preserved in stash (ref: {auto_stash_ref})")
                print("  Restore manually with: git stash apply")
            elif discard_local_changes:
                # Non-interactive + updates.non_interactive_local_changes: discard.
                _m()._discard_stashed_changes(git_cmd, _m().PROJECT_ROOT, auto_stash_ref)
            elif keep_stash:
                # --keep-stash (desktop updater): leave edits parked rather than re-apply silently.
                _m()._park_stashed_changes(auto_stash_ref)
            else:
                _m()._restore_stashed_changes(
                    git_cmd, _m().PROJECT_ROOT, auto_stash_ref, prompt_user=prompt_for_restore,
                    input_fn=gw_input_fn)
    return pre_pull_sha


@dataclass
class _CheckoutPlan:
    """What the pre-pull checkout phase decided (see ``_prepare_checkout_for_update``)."""

    auto_stash_ref: "str | None"
    commit_count: int
    in_place_update: bool
    parked_branch_switched: bool
    prompt_for_restore: bool
    switch_block_reason: "str | None"
    upstream_checked: bool


def _apply_parked_branch_guard(
    git_cmd, branch, current_branch, *, switch_branch, _windows_gateway_resume
) -> tuple[bool, bool, "str | None"]:
    """Decide how a checkout parked on another branch is brought to *branch* (stash-switch-pull-
    switch-back used to "update" main while the running code stayed behind).

    By branch contents + updates.parked_branch_strategy: fully merged -> switch back;
    unmerged -> "switch" (default; loud "kept" notice) or "update_in_place" (merge origin/<target>
    INTO the branch, checkout never moves; --switch-branch overrides once); dirty/unverifiable ->
    touch nothing, warn, ``sys.exit(1)`` with the code update SKIPPED (also when the target is
    missing). Returns ``(parked_branch_switched, in_place_update, switch_block_reason)``.
    """
    if current_branch == branch or current_branch == "HEAD":
        return False, False, None
    switch_safe, switch_block_reason = _m()._assess_parked_branch_switch(
        git_cmd, _m().PROJECT_ROOT, current_branch, branch)
    if not switch_safe:
        _m()._print_parked_branch_skip_warning(
            git_cmd, _m().PROJECT_ROOT, current_branch, branch, switch_block_reason)
        print()
        print(f"⚠ Update finished — code update SKIPPED{_branch_head_suffix(git_cmd, _m().PROJECT_ROOT)}")
        _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
        sys.exit(1)
    if not switch_block_reason.startswith("unmerged:"):
        print(f"  ⚠ Checkout was parked on '{current_branch}' (fully merged) — switching back to {branch}...")
        return True, False, switch_block_reason
    _in_place_configured = False
    with _best_effort('Could not read updates.parked_branch_strategy: %s'):
        _in_place_configured = (
            _updates_config().get("parked_branch_strategy", "switch") == "update_in_place")
    if not _in_place_configured or switch_branch:
        _m()._print_parked_branch_kept_notice(
            current_branch, branch, switch_block_reason.split(":", 1)[1])
        return True, False, switch_block_reason
    # --branch typos used to surface via the checkout failing, which this path skips.
    if _git_run(git_cmd, ["rev-parse", "--verify", "--quiet", f"origin/{branch}"]).returncode != 0:
        print(f"✗ Branch '{branch}' does not exist locally or on origin.")
        sys.exit(1)
    print(
        f"  ℹ On branch '{current_branch}' — updating it in place from "
        f"origin/{branch} (no branch switch; local commits preserved).")
    return False, True, switch_block_reason


def _prepare_checkout_for_update(
    git_cmd, branch, current_branch, *, is_fork, assume_yes, gateway_mode, gw_input_fn,
    switch_branch, _windows_gateway_resume):
    """Parked-branch guard, land on the target, stash, count new commits. Exits when the
    checkout is unsafe to move or the target is missing. ``commit_count`` is 0 when up to
    date, -1 when tips differ but the shallow count is unrecoverable."""
    parked_branch_switched, in_place_update, switch_block_reason = _apply_parked_branch_guard(
        git_cmd, branch, current_branch, switch_branch=switch_branch,
        _windows_gateway_resume=_windows_gateway_resume)

    if not in_place_update and current_branch == "HEAD" != branch:
        print(f"  ⚠ Currently on detached HEAD — switching to {branch} for update...")
    auto_stash_ref = _m()._stash_local_changes_if_needed(git_cmd, _m().PROJECT_ROOT)
    if (
        not in_place_update and current_branch != branch
        and _git_run(git_cmd, ["checkout", branch]).returncode != 0):
        track_result = _git_run(git_cmd, ["checkout", "-B", branch, f"origin/{branch}"])
        if track_result.returncode != 0:
            # Restore the stash before bailing so the user isn't stranded.
            if auto_stash_ref is not None:
                _m()._restore_stashed_changes(
                    git_cmd, _m().PROJECT_ROOT, auto_stash_ref, prompt_user=False, input_fn=gw_input_fn)
            print(f"✗ Branch '{branch}' does not exist locally or on origin.")
            if track_result.stderr.strip():
                print(f"  {track_result.stderr.strip().splitlines()[0]}")
            sys.exit(1)

    prompt_for_restore = (
        auto_stash_ref is not None
        and not assume_yes
        and (gateway_mode or (sys.stdin.isatty() and sys.stdout.isatty())))

    # On shallow checkouts `rev-list --count` can report the entire remote ancestry. The
    # zero/nonzero gate is still sound; treat the shallow NUMBER as unknown and recover it
    # via the GitHub compare API when possible.
    result = _git_run(git_cmd, ["rev-list", f"HEAD..origin/{branch}", "--count"], check=True)
    commit_count = int(result.stdout.strip())

    apply_is_shallow = _is_shallow_checkout(git_cmd)
    if commit_count > 0 and apply_is_shallow:
        from hermes_cli.banner import _github_compare_behind
        counted = _github_compare_behind(*_tip_shas(git_cmd, f"origin/{branch}"))
        # counted == 0 means local-ahead: falls through to the up-to-date path.
        commit_count = counted if counted is not None else -1

    # A fork can match origin yet trail upstream, so the sync can move HEAD with
    # commit_count == 0; detect that BEFORE the no-update return so deps, restarts AND the
    # fleet matrix still run (it used to live in the early-return branch and verified nothing).
    # The sync can therefore advance HEAD even though the origin comparison found no commits. Detect that
    # BEFORE taking the no-update return so dependency refreshes, gateway restarts, AND the fleet version
    # matrix still run for the pulled code (#73108 — previously the sync lived inside the commit_count == 0
    # branch, which returns immediately after: an update that pulled hundreds of upstream commits printed
    # "Already up to date!" and verified nothing). Non-fork checkouts have no upstream question: origin IS
    # the official repo, so "Already up to date!" is fully verified there.
    upstream_checked = True
    if commit_count == 0 and is_fork and branch == "main":
        pre_sync_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
        upstream_checked = _m()._sync_with_upstream_if_needed(
            git_cmd, _m().PROJECT_ROOT, assume_yes=assume_yes, input_fn=gw_input_fn)
        post_sync_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
        if pre_sync_sha and post_sync_sha and pre_sync_sha != post_sync_sha:
            synced_count = _count_commits_between(
                git_cmd, _m().PROJECT_ROOT, pre_sync_sha, post_sync_sha)
            # HEAD moving is proof of an update even if the count can't be read.
            commit_count = max(1, synced_count)

    return _CheckoutPlan(
        auto_stash_ref=auto_stash_ref, commit_count=commit_count, in_place_update=in_place_update,
        parked_branch_switched=parked_branch_switched, prompt_for_restore=prompt_for_restore,
        switch_block_reason=switch_block_reason, upstream_checked=upstream_checked)


@dataclass
class _UpdateOptions:
    """Resolved ``hermes update`` inputs (flags, config, pre-update snapshots)."""

    active_lazy_features: object
    active_tool_dependencies: object
    pre_update_version: object
    gw_input_fn: object
    assume_yes: bool
    keep_stash: bool
    switch_branch: bool
    discard_local_changes: bool
    no_gateway_restart: bool = False


def _resolve_update_options(args, gateway_mode: bool) -> _UpdateOptions:
    """Snapshot pre-update state and resolve the flags/config ``_cmd_update_impl`` runs on."""
    # Snapshot before a managed-runtime refresh can replace site-packages, while the old
    # environment can still prove which optional backends were active.
    active_lazy_features = _m()._capture_active_lazy_features()
    active_tool_dependencies = _m()._capture_active_tool_dependencies()

    # Captured before any pull so the completion line can report the transition.
    # Snapshot the pre-update version before files are replaced so the completion line can report the
    # transition (prime-agent#630 port).
    # Snapshot the pre-update version before any code is pulled so the completion line can report the
    # transition (prime-agent#630 port).
    pre_update_version = _read_project_version()
    gw_input_fn = (
        (lambda prompt, default="": _gateway_prompt(prompt, default)) if gateway_mode else None)
    assume_yes = bool(getattr(args, "yes", False))
    # --keep-stash (desktop updater): never re-apply the autostash; only when an update
    # landed — abort/no-op paths still restore since the tree is unchanged.
    keep_stash = bool(getattr(args, "keep_stash", False))
    # --switch-branch: prefer switching over an in-place merge so an update never writes the
    # branch's history; only meaningful with parked_branch_strategy "update_in_place".
    # See #89507.
    switch_branch = bool(getattr(args, "switch_branch", False))
    # --no-gateway-restart (cron inside the gateway's own cgroup): update code
    # and dependencies but defer the fleet restart so the updater is not killed
    # by its own restart. The pending-restart marker is kept for catch-up.
    no_gateway_restart = bool(getattr(args, "no_gateway_restart", False))

    # Interactive terminals always stash-and-ask; only non-interactive updates consult
    # updates.non_interactive_local_changes (auto-restore vs discard).
    discard_local_changes = False
    if gateway_mode or assume_yes or not (sys.stdin.isatty() and sys.stdout.isatty()):
        # A config read failure must never change the safe default.
        with _best_effort("Could not read updates.non_interactive_local_changes: %s"):
            _mode = str(_updates_config().get("non_interactive_local_changes", "stash")).lower()
            discard_local_changes = _mode == "discard"
    return _UpdateOptions(
        active_lazy_features=active_lazy_features,
        active_tool_dependencies=active_tool_dependencies, pre_update_version=pre_update_version,
        gw_input_fn=gw_input_fn, assume_yes=assume_yes, keep_stash=keep_stash,
        switch_branch=switch_branch, discard_local_changes=discard_local_changes,
        no_gateway_restart=no_gateway_restart)


def _begin_update_receipt_and_plan(args):
    """Open the receipt, snapshot the fleet, refuse on Windows shim holders. Returns the
    pre-update plan (None if the probe failed); ``sys.exit(2)`` when a non-gateway hermes.exe
    holds the venv shim."""
    # Structured receipt: record what this run discovers/does/skips so silent failures are diagnosable.
    with _best_effort('Update receipt unavailable: %s'):
        # See #74973, #81193, #85753, #88848, #91277.
        from hermes_cli.update_receipt import begin_update_receipt
        begin_update_receipt()

    # Plan phase: snapshot runtimes/supervisors/version (read-only; probe failure records
    # nothing). Re-read AFTER the restart phase to reconcile — the plan is the worklist.
    # Plan phase (#91277 Phase 2): snapshot the pre-update fleet — every running Hermes runtime, its
    # supervisor, and its running code version — into the receipt, so a post-mortem can compare what the
    # update SAW against what it did. ``_pre_update_plan`` is read again AFTER the restart phase to
    # reconcile every planned runtime against the phase's bookkeeping (restart via declared mechanism — the
    # plan is the worklist, not just a printout).
    _pre_update_plan = None
    with _best_effort('Update plan phase failed: %s'):
        from hermes_cli.update_inventory import collect_runtime_inventory, record_plan_in_receipt
        _pre_update_plan = collect_runtime_inventory()
        record_plan_in_receipt(_pre_update_plan)
        if _pre_update_plan.runtimes:
            _n = len(_pre_update_plan.runtimes)
            _profiles = ", ".join(sorted({r.profile for r in _pre_update_plan.runtimes}))
            print(f"→ Fleet: {_n} running service(s) across profiles: {_profiles}")

    # Windows: another hermes.exe holding the venv shim means WinError 32 spam and a
    # deferred-rename leftover or silent ZIP fallback. Positively identified gateways are
    # paused/restarted by the update instead; anything else still aborts.
    # Continuing would result in a string of WinError 32 warnings and then either a deferred-rename leftover
    # or a failed git-pull fast path that silently falls back to the slower ZIP route. See issue #26670.
    # Exception (#37039): when every concurrent instance is a gateway runtime, the pause machinery a few
    # lines below (``_pause_windows_gateways_for_update``) stops it before any file mutation, and the
    # post-update restart phase brings it back. Aborting just to make the user run the same kill manually is
    # friction without benefit. Anything not positively identified as a gateway (TUI shell, Desktop backend
    # child, unreadable cmdline) still aborts exactly as before.
    if _m()._is_windows() and not getattr(args, "force", False):
        scripts_dir = _m()._venv_scripts_dir()
        concurrent = _m()._detect_concurrent_hermes_instances(scripts_dir) if scripts_dir is not None else []
        non_gateway = _m()._filter_non_gateway_concurrent_instances(concurrent) if concurrent else []
        if non_gateway:
            print(_format_concurrent_instances_message(non_gateway, scripts_dir))
            sys.exit(2)
    return _pre_update_plan


def _prepare_git_command() -> tuple[bool, list, bool]:
    """Return ``(use_zip_update, git_cmd, is_fork)``; ``sys.exit(1)`` when not a git repo
    on a non-Windows host (Windows falls back to ZIP: broken git file I/O, AV, NTFS filters)."""
    git_dir = _m().PROJECT_ROOT / ".git"
    use_zip_update = not git_dir.exists()
    if use_zip_update and sys.platform != "win32":
        print("✗ Not a git repository. Please reinstall:")
        print("  curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash")
        sys.exit(1)

    git_cmd = _base_git_cmd()
    if sys.platform == "win32" and git_dir.exists():
        _git_run(git_cmd, ["config", "windows.appendAtomically", "false"])
    # A broken Git-for-Windows trampoline refuses every call with a "BUG (fork bomb)" guard;
    # swap in a real binary up front so git survives instead of degrading to ZIP.
    # See #87876.
    git_cmd = _ensure_non_trampoline_git(git_cmd)

    # Before stash/branch logic: npm rewrites package-lock.json non-deterministically and
    # line-ending churn is machine-made dirt; both would otherwise force an autostash every update.
    _discard_lockfile_churn(git_cmd, _m().PROJECT_ROOT)
    _normalize_managed_eol(git_cmd, _m().PROJECT_ROOT)

    origin_url = _m()._get_origin_url(git_cmd, _m().PROJECT_ROOT)
    is_fork = _is_fork(origin_url)

    if is_fork:
        print("⚠ Updating from fork:")
        print(f"  {origin_url}")
        print()
    return use_zip_update, git_cmd, is_fork


def _verify_head_after_pull(
    git_cmd, branch: str, pre_pull_sha, *, in_place_update: bool, _windows_gateway_resume
) -> str | None:
    """Return the post-pull HEAD SHA; ``sys.exit(1)`` if the pull was a no-op or landed off-branch."""
    # A detached checkout pinned to a SHA can report "N new commit(s)" and a successful
    # merge --ff-only yet stay put; surface the no-op instead of claiming "Code updated!".
    # Verify HEAD actually moved (issue #79678). ``merge --ff-only`` succeeding only means the merge
    # completed, not that the update applied: a checkout that is pinned to a raw SHA (detached HEAD) can
    # report "N new commit(s)" against origin yet still sit on the old commit afterward (the branch-switch
    # step re-detaches to the SHA). Before this guard, ``hermes update`` printed "✓ Code updated!" and
    # reinstalled deps + rebuilt the desktop app against the stale tree — no error, no warning, ``hermes
    # doctor`` healthy. Compare pre-pull and post-pull HEAD; if they match, surface the no-op instead of
    # claiming success.
    post_pull_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
    if pre_pull_sha and post_pull_sha == pre_pull_sha:
        print()
        print("✗ Code did not move — update was a no-op.")
        print(
            f"  HEAD is pinned to {pre_pull_sha[:10]} (detached checkout); "
            f"origin/{branch} advanced but the working tree stayed put.")
        print(
            "  Reattach to the branch and retry: "
            f"git -C {_m().PROJECT_ROOT} checkout {branch} && hermes update")
        _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
        sys.exit(1)

    # HEAD must be on the target or "Code updated!" is a lie; an IN-PLACE update is the one
    # legitimate exception (origin/<target> merged INTO the checked-out branch).
    post_pull_branch = _current_branch_name(git_cmd)
    if not in_place_update and post_pull_branch and post_pull_branch not in {branch, "HEAD"}:
        print()
        print(
            f"✗ Update pulled origin/{branch}, but the checkout is on "
            f"'{post_pull_branch}' — not claiming success.")
        print(
            "  Switch to the target branch and retry: "
            f"git -C {_m().PROJECT_ROOT} checkout {branch} && hermes update")
        _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
        sys.exit(1)
    return post_pull_sha


def _current_branch_name(git_cmd, *, check: bool = False) -> str:
    """``rev-parse --abbrev-ref HEAD`` (literal "HEAD" when detached)."""
    return _git_run(git_cmd, ["rev-parse", "--abbrev-ref", "HEAD"], check=check).stdout.strip()


def _handle_update_called_process_error(
    e, args, gateway_mode: bool, had_desktop_app_before_update: bool) -> None:
    """Git/installer failure: ZIP-fallback when safe, else report and ``sys.exit(1)``."""
    stage = _format_update_failure_stage(e)
    if _should_zip_fallback_on_update_error(e):
        print(f"⚠ {stage}: {e}")
        print("→ Falling back to ZIP download...")
        print()
        desktop_build_ok = _update_via_zip(
            args, had_desktop_app_before_update=had_desktop_app_before_update)
        if gateway_mode:
            _write_gateway_update_exit_code(desktop_build_ok)
    else:
        if _called_process_error_is_python_dep_install(e):
            print(f"✗ {stage} (the code update itself succeeded).")
            _print_called_process_error_tail(e)
            print()
            print("  Hermes may not start until the dependencies are installed. Fix the error above")
            print("  (usually network or disk space), then run `hermes update` again.")
            if _m()._is_windows():
                print("  If `hermes update` itself will not start, retry through the venv interpreter:")
                print(
                    '    venv\\Scripts\\python.exe -c '
                    '"from hermes_cli.main import main; main()" update --yes')
        else:
            print(f"✗ {stage}.")
            print(f"  Details: {e}")
            _print_called_process_error_tail(e)
        _finalize_receipt("failed", 'Update receipt finalize failed: %s')
        sys.exit(1)


def _finalize_receipt(status: str, debug_message: str) -> None:
    """Best-effort ``finalize_update_receipt(status)``; the receipt must never break an update."""
    with _best_effort(debug_message):
        from hermes_cli.update_receipt import finalize_update_receipt
        finalize_update_receipt(status)


def _finish_already_up_to_date(
    git_cmd, branch: str, current_branch: str, _plan, *, assume_yes: bool, gateway_mode: bool,
    gw_input_fn, pre_update_snapshot_id, had_desktop_app_before_update: bool,
    active_lazy_features, active_tool_dependencies, _windows_gateway_resume,
    no_gateway_restart: bool = False) -> None:
    """"Already up to date" path: restore stash/branch, repair the checkout, catch up the fleet.
    ``sys.exit(1)`` when the repair is incomplete (after gateway exit code + partial receipt)."""
    _invalidate_update_cache()

    # Restore stash and switch back if we moved. EXCEPTION: a parked branch verified clean +
    # fully merged stays on the target — re-parking on the stale branch recreates the incident.
    if _plan.auto_stash_ref is not None:
        _m()._restore_stashed_changes(
            git_cmd, _m().PROJECT_ROOT, _plan.auto_stash_ref, prompt_user=_plan.prompt_for_restore,
            input_fn=gw_input_fn)
    if _plan.parked_branch_switched:
        if _plan.switch_block_reason.startswith("unmerged:"):
            _count = _plan.switch_block_reason.split(":", 1)[1]
            print(
                f"  ✓ Checkout was parked on '{current_branch}' — switched back to {branch}; "
                f"{_count} unmerged commit(s) kept on '{current_branch}'.")
        else:
            print(f"  ✓ Checkout was parked on '{current_branch}' (fully merged) — switched back to {branch}.")
    elif current_branch not in {branch, "HEAD"}:
        _git_run(git_cmd, ["checkout", current_branch])

    current_checkout_complete = _repair_current_checkout(
        assume_yes=assume_yes, gateway_mode=gateway_mode,
        pre_update_snapshot_id=pre_update_snapshot_id,
        had_desktop_app_before_update=had_desktop_app_before_update,
        active_lazy_features=active_lazy_features,
        active_tool_dependencies=active_tool_dependencies, upstream_checked=_plan.upstream_checked,
        _windows_gateway_resume=_windows_gateway_resume)
    _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
    # A prior pull may still owe the fleet a restart; catch up here too, BEFORE the exit
    # gate so a partial outcome can't strand the fleet on stale code.
    # Catch up even on the "Already up to date" path — that early return is what left the gateway on stale
    # code for two days. Runs BEFORE the runtime-verification exit gate below: a vulnerable SQLite runtime
    # demotes the outcome to partial, but must not strand the fleet on stale code (#91277 fleet contract —
    # the pending-restart check always executes). Under --no-gateway-restart the
    # catch-up is deferred instead (executing it would kill the cron's own gateway).
    _apply_pending_fleet_restart_catchup(defer=no_gateway_restart)
    if not current_checkout_complete:
        if gateway_mode:
            _write_gateway_update_exit_code(False)
        _finalize_receipt("partial", 'Update receipt finalize (current checkout) failed: %s')
        sys.exit(1)


def _apply_pulled_update(
    git_cmd, branch, pre_pull_sha, _plan, opts, *, gateway_mode, is_fork, desktop_dir,
    had_desktop_app_before_update, pre_update_snapshot_id, _pre_update_plan,
    _windows_gateway_resume) -> None:
    """Post-pull phase: verify HEAD, sync Python/Node/web/Desktop, maintenance, fleet restart."""
    _invalidate_update_cache()
    post_pull_sha = _verify_head_after_pull(
        git_cmd, branch, pre_pull_sha, in_place_update=_plan.in_place_update,
        _windows_gateway_resume=_windows_gateway_resume)

    # Gateways still serve pre-pull modules until the restart phase; an interrupt before a
    # completed restart leaves this marker so the next update catches up even when git is
    # current. Distinct from ``.update-incomplete`` (venv/install repair).
    # See #95294.
    _write_fleet_restart_pending_marker(expected_sha=post_pull_sha or "")
    # Stale .pyc would ImportError on gateway restart when new source references new names.
    _sweep_bytecode_after_update(branch)

    if is_fork and branch == "main":
        _m()._sync_with_upstream_if_needed(
            git_cmd, _m().PROJECT_ROOT, assume_yes=opts.assume_yes, input_fn=opts.gw_input_fn)

    # .[all], falling back to base + extras individually so one broken extra doesn't strip
    # the rest; the ownership preflight refuses first on foreign-owned (sudo-pip) venv files.
    _sync_python_dependencies_after_pull(
        git_cmd, branch, pre_pull_sha, active_lazy_features=opts.active_lazy_features,
        active_tool_dependencies=opts.active_tool_dependencies,
        _windows_gateway_resume=_windows_gateway_resume)

    node_failures = _update_node_dependencies()
    _m()._build_web_ui(_m().PROJECT_ROOT / "web")
    desktop_build_ok = _rebuild_desktop_after_update(
        desktop_dir, had_desktop_app_before_update=had_desktop_app_before_update)

    print()
    print(f"✓ Code updated!{_branch_head_suffix(git_cmd, _m().PROJECT_ROOT)}")

    update_complete = _run_post_update_maintenance(
        assume_yes=opts.assume_yes, gateway_mode=gateway_mode,
        pre_update_snapshot_id=pre_update_snapshot_id,
        had_desktop_app_before_update=had_desktop_app_before_update,
        node_failures=node_failures, desktop_build_ok=desktop_build_ok,
        pre_update_version=opts.pre_update_version)

    # Exit code *before* the restart: under --gateway this process lives in the gateway's
    # systemd cgroup and the systemctl-restart fallback SIGKILLs it (KillMode=mixed), so
    # the marker would never land and the new gateway's watcher would time out spuriously.
    if gateway_mode:
        _write_gateway_update_exit_code(update_complete)

    if opts.no_gateway_restart:
        # Cron inside the gateway's own cgroup: restarting the fleet now would
        # SIGUSR1-drain this updater's own gateway and systemd would SIGKILL the
        # updater with it. Defer instead — the pending marker written above is
        # kept for the next normal update. Skipping the restart phase also skips
        # its stale-module purge: this live interpreter still serves pre-update
        # code and must not have its sys.modules graph mutated mid-flight.
        # Windows pause/resume still runs (paused gateways must be resumed onto
        # pre-update code); only the fleet restart + verification are deferred.
        # The resume outcome is RETAINED (not discarded): a failed resume must
        # still make this update partial, exactly as on the normal path.
        resume_outcome = _GatewayRestartOutcome(
            incomplete=False, phase_errors=[], pre_restart_gateway_pids=[],
            restarted_services=[], failed_or_stale_units=[], relaunched_profiles=[],
            externally_supervised_profiles=[], killed_pids=set(),
        )
        _resume_windows_gateways_and_merge_outcome(
            resume_outcome, _windows_gateway_resume, gateway_mode)
        _defer_fleet_restart_after_update(
            update_complete=update_complete, resume_incomplete=resume_outcome.incomplete)
        return

    _restart = _restart_gateway_fleet_after_update(_pre_update_plan, gateway_mode)
    _resume_windows_gateways_and_merge_outcome(_restart, _windows_gateway_resume, gateway_mode)
    _verify_fleet_after_update(
        _restart, _pre_update_plan=_pre_update_plan, _windows_gateway_resume=_windows_gateway_resume,
        node_failures=node_failures, update_complete=update_complete)


def _cmd_update_impl(args, gateway_mode: bool):
    """Body of ``cmd_update`` — kept separate so the wrapper can always restore stdio even on
    ``sys.exit``. Self-lock deferral deliberately does NOT run here (pre-fetch it stranded users
    on the OLD checkout in an exit-2 loop); it runs right before the dependency sync."""
    opts = _resolve_update_options(args, gateway_mode)
    gw_input_fn, assume_yes = opts.gw_input_fn, opts.assume_yes

    print("☤ Updating Hermes Agent...")
    print()

    _pre_update_plan = _begin_update_receipt_and_plan(args)

    # Backup before any git/file mutation; the snapshot id (None if disabled/failed) feeds
    # the post-update cron-jobs safety net.
    pre_update_snapshot_id = _m()._run_pre_update_backup(args)
    _record_update_step(
        "pre_update_backup", pre_update_snapshot_id is not None,
        f"snapshot={pre_update_snapshot_id}" if pre_update_snapshot_id else "disabled or failed")

    _windows_gateway_resume = _m()._pause_windows_gateways_for_update()
    if _windows_gateway_resume:
        import atexit as _atexit
        _atexit.register(_m()._resume_windows_gateways_after_update, _windows_gateway_resume)

    # Any venv python still running (typically the Desktop `hermes serve` backend) keeps .pyd
    # locked and would corrupt the sync; refuse rather than race (the app respawns a killed
    # backend). NOT bypassed by --force (desktop updater, shim guard only); --force-venv is.
    if _m()._is_windows() and not getattr(args, "force_venv", False):
        _clear_windows_venv_holders_or_exit(args, gateway_mode, _windows_gateway_resume)

    # After every fail-closed venv guard, before either path can remove the release tree.
    # Self-lock deferral moved: the venv-holder sweep above excludes this process by design (a CLI `hermes
    # update` IS the venv python), and an updater that has imported a native venv extension cannot rewrite
    # its own mapped .pyd (#83569). That check used to run HERE — before the fetch — but firing pre-fetch
    # meant a deferral stranded the user on the OLD checkout, and any startup path that eagerly loaded
    # cryptography turned every Windows update into an exit-2 loop (#86735/#86780/#86781). It now runs via
    # _abort_dependency_sync_if_self_locked() after the code swap, immediately before the dependency sync —
    # the only phase the lock can actually break — and only when the sync would truly rewrite the loaded
    # distribution.
    desktop_dir = _m().PROJECT_ROOT / "apps" / "desktop"
    had_desktop_app_before_update = _desktop_app_present(desktop_dir)

    use_zip_update, git_cmd, is_fork = _prepare_git_command()

    if use_zip_update:
        try:
            desktop_build_ok = _update_via_zip(
                args, had_desktop_app_before_update=had_desktop_app_before_update)
        finally:
            _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
        if gateway_mode:
            _write_gateway_update_exit_code(desktop_build_ok)
        return

    try:
        # Run fork-aware upstream sync before origin fetch/pull
        if not _m()._sync_with_upstream_if_needed(git_cmd, _m().PROJECT_ROOT):
            _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
            sys.exit(1)

        # Resolve the target branch up front so the fetch can be scoped to it.
        # A bare `git fetch origin` pulls every ref, and this repo carries
        # thousands of auto-generated branches — an unscoped fetch can stall for
        # minutes on a non-single-branch checkout. Fetch only what we update
        # against.
        branch = _m()._resolve_update_branch(args)

        # Self-heal abandoned .git/*.lock files (crashed fetch) or the fetch fails "File exists".
        from hermes_cli.gitlock import clear_stale_git_locks, clear_stale_tmp_packs
        cleared = clear_stale_git_locks(_m().PROJECT_ROOT)
        if cleared:
            print("  (removed stale git lock(s): %s)" % ", ".join(cleared))
        swept = clear_stale_tmp_packs(_m().PROJECT_ROOT)
        if swept:
            print("  (removed %d aborted-fetch pack temp file(s))" % len(swept))
        # Shallow installer checkouts collect one `.git/shallow` graft per past depth-1 fetch
        # (#105951); stale grafts break merge-base and push this run into the divergence path.
        from hermes_cli.gitlock import repair_broken_shallow_boundaries, prune_stale_shallow_grafts
        repaired = repair_broken_shallow_boundaries(_m().PROJECT_ROOT)
        if repaired:
            print(f"  (restored {repaired} broken shallow boundary(ies))")
        pruned = prune_stale_shallow_grafts(_m().PROJECT_ROOT)
        if pruned:
            print(f"  (pruned {pruned} stale shallow graft(s) left by past depth-1 checks)")

        # Surface autostashes left by earlier updates (--keep-stash, failed restores).
        # Surface autostash entries left behind by earlier updates (#63717 problem 6) — parked --keep-stash
        # runs and failed restores preserve the stash but nothing ever mentioned it again.
        _m()._warn_orphaned_update_autostashes(git_cmd, _m().PROJECT_ROOT)

        print("→ Fetching updates...")
        fetch_result = _git_run(git_cmd, ["fetch", "origin", branch], network=True)
        if fetch_result.returncode != 0:
            _print_fetch_failure(fetch_result.stderr)
            sys.exit(1)

        # Get current branch (returns literal "HEAD" when detached)
        result = subprocess.run(
            git_cmd + ["rev-parse", "--abbrev-ref", "HEAD"],
            cwd=_m().PROJECT_ROOT,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
            check=True,
        )
        current_branch = result.stdout.strip()

        # Parked-branch guard (2026-08-17 live incident): the checkout can be
        # left parked on a stale feature branch by earlier tooling. Blindly
        # stash-switch-pull-switch-back "updates" main while the running code
        # stays days behind, then prints "✓ Code updated!".
        #
        # What happens next is routed by what the branch carries (which is
        # exactly what the guard measures) plus updates.parked_branch_strategy:
        #
        #   fully merged  -> a stale leftover with nothing to lose: switch
        #                    back to the target.
        #   unmerged: N   -> strategy "switch" (default): switch to the
        #                    target anyway — committed work is safe on the
        #                    branch (git checkout never discards commits) and
        #                    a loud "kept" notice names the branch + count.
        #                    Deterministic, so non-interactive callers
        #                    (desktop update button, gateway /update, cron)
        #                    always reach the target.
        #                    strategy "update_in_place": a maintained custom
        #                    branch (local patches on top of main) is updated
        #                    IN PLACE from origin/<target> — the checkout
        #                    never moves, local commits survive, the running
        #                    code advances. --switch-branch overrides back to
        #                    the switch path for one run.
        #   anything else -> dirty / unverifiable / opted out: touch nothing,
        #                    warn loudly, mark the code update SKIPPED, and
        #                    stop before the post-update steps reinforce the
        #                    stale tree.
        parked_branch_switched = False
        if current_branch != branch:
            label = (
                "detached HEAD"
                if current_branch == "HEAD"
                else f"branch '{current_branch}'"
            )
            print(f"  ⚠ Currently on {label} — switching to {branch} for update...")
            # Commit before checkout so uncommitted work isn't lost
            if _m()._commit_and_push_local_changes_if_needed(git_cmd, _m().PROJECT_ROOT, current_branch) is False:
                sys.exit(1)
            checkout_result = subprocess.run(
                git_cmd + ["checkout", branch],
                cwd=_m().PROJECT_ROOT,
                capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            )
            if checkout_result.returncode != 0:
                # Local checkout doesn't have this branch yet. Try to set
                # it up as a tracking branch of origin/<branch>. This is
                # the common case when the requested branch exists upstream
                # but was never checked out locally.
                track_result = subprocess.run(
                    git_cmd + ["checkout", "-B", branch, f"origin/{branch}"],
                    cwd=_m().PROJECT_ROOT,
                    capture_output=True,
                    text=True, encoding="utf-8", errors="replace",
                )
                if track_result.returncode != 0:
                    print(f"✗ Branch '{branch}' does not exist locally or on origin.")
                    if track_result.stderr.strip():
                        print(f"  {track_result.stderr.strip().splitlines()[0]}")
                    sys.exit(1)
        else:
            if _m()._commit_and_push_local_changes_if_needed(git_cmd, _m().PROJECT_ROOT, current_branch) is False:
                sys.exit(1)

        # Check if there are updates. On shallow checkouts `rev-list --count`
        # walks the truncated graph and can report the entire remote ancestry
        # (e.g. "Found 9980 new commit(s)" on a depth-1 install — #53479).
        # The zero/nonzero gate is still sound (HEAD == origin/<branch> counts
        # 0), so keep it, but treat the shallow NUMBER as unknown and recover
        # the real one via the GitHub compare API when possible.
        result = subprocess.run(
            git_cmd + ["rev-list", f"HEAD..origin/{branch}", "--count"],
            cwd=_m().PROJECT_ROOT,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
            check=True,
        )
        commit_count = int(result.stdout.strip())

        apply_is_shallow = (
            subprocess.run(
                git_cmd + ["rev-parse", "--is-shallow-repository"],
                cwd=_m().PROJECT_ROOT,
                capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            ).stdout.strip()
            == "true"
        )
        if commit_count > 0 and apply_is_shallow:
            from hermes_cli.banner import _github_compare_behind

            head_sha = subprocess.run(
                git_cmd + ["rev-parse", "HEAD"],
                cwd=_m().PROJECT_ROOT, capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            ).stdout.strip()
            target_sha = subprocess.run(
                git_cmd + ["rev-parse", f"origin/{branch}"],
                cwd=_m().PROJECT_ROOT, capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            ).stdout.strip()
            counted = _github_compare_behind(head_sha, target_sha)
            # counted == 0 means local-ahead (remote tip reachable from HEAD):
            # not behind, fall through to the up-to-date path.
            commit_count = counted if counted is not None else -1

        # A fork can match origin while still trailing upstream. The sync can
        # therefore advance HEAD even though the origin comparison found no
        # commits. Detect that BEFORE taking the no-update return so dependency
        # refreshes, gateway restarts, AND the fleet version matrix still run
        # for the pulled code (#73108 — previously the sync lived inside the
        # commit_count == 0 branch, which returns immediately after: an update
        # that pulled hundreds of upstream commits printed "Already up to
        # date!" and verified nothing).
        if commit_count == 0 and is_fork and branch == "main":
            pre_sync_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
            _m()._sync_with_upstream_if_needed(git_cmd, _m().PROJECT_ROOT)
            post_sync_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
            if pre_sync_sha and post_sync_sha and pre_sync_sha != post_sync_sha:
                synced_count = _count_commits_between(
                    git_cmd,
                    _m().PROJECT_ROOT,
                    pre_sync_sha,
                    post_sync_sha,
                )
                # HEAD moving is itself proof of an update. Keep the update
                # path active even if the informational count cannot be read.
                commit_count = max(1, synced_count)

        if commit_count == 0:
            _invalidate_update_cache()
            if current_branch not in {branch, "HEAD"}:
                subprocess.run(
                    git_cmd + ["checkout", current_branch],
                    cwd=_m().PROJECT_ROOT,
                    capture_output=True,
                    text=True, encoding="utf-8", errors="replace",
                    check=False,
                )

            # "No new commits" does not mean the managed interpreter is safe.
            # uv can retain the same CPython patch while python-build-standalone
            # refreshes the embedded SQLite underneath it. Keep the existing
            # update-boundary hook active on this retry path too.
            from hermes_cli.managed_uv import ensure_uv, update_managed_uv

            runtime_repairs = []
            update_managed_uv(repair_observer=runtime_repairs.append)
            ensure_uv(repair_observer=runtime_repairs.append)
            runtime_repaired = next(
                (result for result in runtime_repairs if result.repaired),
                None,
            )

            # A current checkout does NOT imply a healthy install: a previous
            # dependency sync may have failed partway (classic on Windows,
            # where a running gateway/desktop backend keeps .pyd files locked
            # and uv/pip dies with access-denied, stranding the venv between
            # versions). Probe the venv's core imports and repair if broken —
            # otherwise "Already up to date!" gaslights the user while their
            # install stays bricked.
            healthy, detail = _venv_core_imports_healthy()
            # The Windows shim hand-off spawns this child precisely to run a
            # sync its parent could not. The parent already pulled, so the
            # checkout is current BY DESIGN and venv health is not the
            # question — the pending sync is. Without this the child prints
            # "Already up to date!" and exits without doing the one job it
            # was spawned for.
            handed_off_sync = os.environ.get(_m()._UPDATE_REEXEC_ENV) == "1"
            if handed_off_sync:
                print("→ Finishing the dependency install handed off by hermes.exe...")
            elif not healthy:
                print("⚠ Checkout is current, but the venv is unhealthy:")
                print(f"  {detail}")
                print("→ Repairing Python dependencies...")
            if handed_off_sync or not healthy:
                # Self-lock deferral (#86735): the repair rewrites the venv
                # too — same mapped-extension hazard as the update sync.
                _m()._abort_dependency_sync_if_self_locked(_windows_gateway_resume)
                _write_update_incomplete_marker()
                from hermes_cli.managed_uv import ensure_uv

                repair_uv = ensure_uv()
                # A managed install whose venv is gone entirely (interrupted
                # repair after the old venv was moved aside) needs the venv
                # recreated before dependencies can be installed into it.
                venv_python_missing = not (
                    venv_python_path(
                        _m().PROJECT_ROOT / "venv", windows=_m()._is_windows()
                    )
                ).exists()
                if venv_python_missing and repair_uv:
                    print("→ Recreating virtual environment...")
                    subprocess.run(
                        [repair_uv, "venv", "venv"],
                        cwd=_m().PROJECT_ROOT,
                        check=False,
                    )
                if repair_uv:
                    # Isolated from third-party UV env vars (#83914), same as
                    # the main-path and git-path dependency syncs.
                    from hermes_cli.managed_uv import managed_python_env

                    repair_env = managed_python_env()
                    repair_env["VIRTUAL_ENV"] = str(_m().PROJECT_ROOT / "venv")
                    _m()._install_python_dependencies_with_optional_fallback(
                        [repair_uv, "pip"], env=repair_env, group="all"
                    )
                    _m()._refresh_active_lazy_features(
                        [repair_uv, "pip"],
                        env=repair_env,
                        features=active_lazy_features,
                    )
                    _m()._restore_active_tool_dependencies(
                        active_tool_dependencies,
                        [repair_uv, "pip"],
                        env=repair_env,
                    )
                else:
                    _m()._install_python_dependencies_with_optional_fallback(
                        [sys.executable, "-m", "pip"], group="all"
                    )
                    _m()._refresh_active_lazy_features(
                        [sys.executable, "-m", "pip"],
                        features=active_lazy_features,
                    )
                    _m()._restore_active_tool_dependencies(
                        active_tool_dependencies,
                        [sys.executable, "-m", "pip"],
                    )
                _m()._clear_update_incomplete_marker()
                healthy_after, detail_after = _venv_core_imports_healthy()
                if healthy_after:
                    print("✓ Dependencies repaired!")
                    _check_and_apply_config_migration(
                        assume_yes=assume_yes,
                        gateway_mode=gateway_mode,
                        pre_update_snapshot_id=pre_update_snapshot_id,
                    )
                    _print_update_completion("✓ Update complete!")
                else:
                    print(f"⚠ Venv still unhealthy after repair: {detail_after}")
                    print("  Close all Hermes windows/gateways and re-run: hermes update")
            else:
                _repair_node_deps_on_current_checkout(
                    _print_update_completion,
                    assume_yes=assume_yes,
                    gateway_mode=gateway_mode,
                    pre_update_snapshot_id=pre_update_snapshot_id,
                )
            if runtime_repaired is not None and not _m()._is_windows():
                print()
                print(
                    "⚠ Restart required to finish the managed Python runtime repair."
                )
                print(
                    "  Any running Hermes gateways, Desktop backends, or other "
                    "long-lived processes still use the previous runtime."
                )
                print("  Restart each of them to pick up the repaired runtime.")
            _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
            # Git is current, but a prior pull may still owe the fleet a
            # restart (#95294). Catch up even on the "Already up to date"
            # path — that early return is what left the gateway on stale
            # code for two days.
            _apply_pending_fleet_restart_catchup()
            return

        if commit_count > 0:
            print(f"→ Found {commit_count} new commit(s)")
        else:
            # Shallow, exact count unrecoverable — but the tips differ, so there IS an update.
            print("→ Updates available (commit count unknown on this shallow checkout)")

        print("→ Pulling updates...")
        update_succeeded = False
        # Capture the pre-pull SHA so we can auto-roll-back if the new code
        # has a syntax error in a critical-path file (PR #28452 incident:
        # orphan merge-conflict markers in hermes_cli/config.py bricked
        # every user who ran ``hermes update`` for the 7 minutes between
        # the bad commit and the fix landing).
        pre_pull_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
        # Merge the ref we already fetched above (→ Fetching updates...)
        # instead of `git pull`, which performs a SECOND network fetch of
        # the same branch (~0.5-1.5 s of redundant round-trip per update).
        # `merge --ff-only origin/<branch>` is byte-identical in effect to
        # `pull --ff-only origin <branch>` given the fresh tracking ref;
        # the divergence fallback below is unchanged.
        pull_result = subprocess.run(
            git_cmd + ["merge", "--ff-only", f"origin/{branch}"],
            cwd=_m().PROJECT_ROOT,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        )
        if pull_result.returncode != 0:
            # ff-only failed — local and remote have diverged (e.g. upstream
            # force-pushed or rebase).  Since local changes are already
            # stashed, reset to match the remote exactly.
            print(
                "  ⚠ Fast-forward not possible (history diverged), resetting to match remote..."
            )
            reset_result = subprocess.run(
                git_cmd + ["reset", "--hard", f"origin/{branch}"],
                cwd=_m().PROJECT_ROOT,
                capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            )
            if reset_result.returncode != 0:
                print(f"✗ Failed to reset to origin/{branch}.")
                if reset_result.stderr.strip():
                    print(f"  {reset_result.stderr.strip()}")
                print(
                    f"  Try manually: git fetch origin && git reset --hard origin/{branch}"
                )
                sys.exit(1)

        # Post-pull syntax guard: validate critical-path files actually
        # parse before declaring the update successful. If a bad commit
        # made it through CI (e.g. admin-merge bypass of a failing
        # ruff check), this catches it on the user side and rolls back
        # so the CLI stays bootable. The user can then retry ``hermes
        # update`` later once a fix lands upstream.
        syntax_ok, failing_path, syntax_error = _validate_critical_files_syntax(
            _m().PROJECT_ROOT
        )
        if not syntax_ok:
            print()
            print("✗ Pulled code has a syntax error in a critical file:")
            print(f"  {failing_path}")
            if syntax_error:
                # py_compile errors can be multi-line; show the first
                # ~6 lines so the user sees the actual SyntaxError text.
                for line in str(syntax_error).splitlines()[:6]:
                    print(f"    {line}")
            if pre_pull_sha:
                print()
                print(f"→ Rolling back to {pre_pull_sha[:10]}...")
                rollback_result = subprocess.run(
                    git_cmd + ["reset", "--hard", pre_pull_sha],
                    cwd=_m().PROJECT_ROOT,
                    capture_output=True,
                    text=True, encoding="utf-8", errors="replace",
                )
                if rollback_result.returncode == 0:
                    print("  ✓ Rollback complete — your install is unchanged.")
                    print("  Try ``hermes update`` again later once a fix lands.")
                else:
                    print("  ✗ Rollback failed. Recover manually with:")
                    print(f"    cd {_m().PROJECT_ROOT} && git reset --hard {pre_pull_sha}")
                    if rollback_result.stderr.strip():
                        print(f"    ({rollback_result.stderr.strip().splitlines()[0]})")
            else:
                print()
                print("  Could not capture pre-pull SHA — recover manually with:")
                print(f"    cd {_m().PROJECT_ROOT} && git reflog && git reset --hard <prev-sha>")
            sys.exit(1)

        update_succeeded = True
        _invalidate_update_cache()

        # Verify HEAD actually moved (issue #79678). ``merge --ff-only``
        # succeeding only means the merge completed, not that the update
        # applied: a checkout that is pinned to a raw SHA (detached HEAD) can
        # report "N new commit(s)" against origin yet still sit on the old
        # commit afterward (the branch-switch step re-detaches to the SHA).
        # Before this guard, ``hermes update`` printed "✓ Code updated!" and
        # reinstalled deps + rebuilt the desktop app against the stale tree —
        # no error, no warning, ``hermes doctor`` healthy. Compare pre-pull
        # and post-pull HEAD; if they match, surface the no-op instead of
        # claiming success.
        post_pull_sha = _capture_head_sha(git_cmd, _m().PROJECT_ROOT)
        if pre_pull_sha and post_pull_sha == pre_pull_sha:
            print()
            print("✗ Code did not move — update was a no-op.")
            print(
                f"  HEAD is pinned to {pre_pull_sha[:10]} (detached checkout); "
                f"origin/{branch} advanced but the working tree stayed put."
            )
            print(
                "  Reattach to the branch and retry: "
                f"git -C {_m().PROJECT_ROOT} checkout {branch} && hermes update"
            )
            _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
            sys.exit(1)

        # And verify HEAD actually sits on the target branch. The parked-
        # branch guard above should make this unreachable, but if any path
        # leaves the checkout attached elsewhere, "✓ Code updated!" would be
        # a lie — refuse to claim success (2026-08-17 incident class).
        #
        # An IN-PLACE branch update is the one legitimate way to end on a
        # non-target branch: origin/<target> was merged INTO the checked-out
        # branch, so the running code *is* up to date and HEAD staying put is
        # the whole point. Claiming failure there would make every update on a
        # real working branch exit 1 after doing exactly the right thing.
        post_pull_branch = subprocess.run(
            git_cmd + ["rev-parse", "--abbrev-ref", "HEAD"],
            cwd=_m().PROJECT_ROOT,
            capture_output=True,
            text=True, encoding="utf-8", errors="replace",
        ).stdout.strip()
        if (
            not in_place_update
            and post_pull_branch
            and post_pull_branch not in {branch, "HEAD"}
        ):
            print()
            print(
                f"✗ Update pulled origin/{branch}, but the checkout is on "
                f"'{post_pull_branch}' — not claiming success."
            )
            print(
                "  Switch to the target branch and retry: "
                f"git -C {_m().PROJECT_ROOT} checkout {branch} && hermes update"
            )
            _m()._resume_windows_gateways_after_update(_windows_gateway_resume)
            sys.exit(1)

        # #95294: HEAD advanced; running gateways still serve pre-pull
        # modules until the restart phase below. Any interrupt between here
        # and a completed (or no-op) restart leaves this marker so the next
        # ``hermes update`` can catch up even when git is already up to date.
        # Distinct from ``.update-incomplete`` (venv/install repair).
        _write_fleet_restart_pending_marker(expected_sha=post_pull_sha or "")

        # Clear stale .pyc bytecode cache — prevents ImportError on gateway
        # restart when updated source references names that didn't exist in
        # the old bytecode (e.g. get_hermes_home added to hermes_constants).
        removed = _m()._clear_bytecode_cache(_m().PROJECT_ROOT)
        if removed:
            print(
                f"  ✓ Cleared {removed} stale __pycache__ director{'y' if removed == 1 else 'ies'}"
            )
        _m()._record_bytecode_fingerprint()
        _m()._refresh_bootstrap_cache_scripts(branch)

        # Reinstall Python dependencies. Prefer .[all], but if one optional extra
        # breaks on this machine, keep base deps and reinstall the remaining extras
        # individually so update does not silently strip working capabilities.
        #
        # Self-lock deferral (relocated preflight — #86735): if THIS process
        # holds a native extension the sync must rewrite, defer NOW — after
        # the code swap, so only the dependency install is pending and the
        # next fresh launch completes it via the marker.
        _m()._abort_dependency_sync_if_self_locked(_windows_gateway_resume)
        #
        # Drop the core-install breadcrumb BEFORE touching the venv. If the
        # install is killed mid-flight (Ctrl-C, terminal close, WSL OOM), the
        # marker survives and the next ``hermes`` launch finishes the install
        # via ``_recover_from_interrupted_install``. Cleared after the core
        # ``.[all]`` install completes — lazy refresh uses a separate marker.
        _write_update_incomplete_marker()
        deps_current = _editable_install_is_current(
            git_cmd, _m().PROJECT_ROOT, pre_pull_sha
        )
        if deps_current:
            print("→ Python dependencies unchanged — skipping reinstall")
        else:
            print("→ Updating Python dependencies...")
        from hermes_cli.managed_uv import ensure_uv, update_managed_uv

        # Keep managed uv current — runs `uv self update` if we already have one.
        update_managed_uv()

        uv_bin = ensure_uv()

        pip_cmd = [sys.executable, "-m", "pip"]
        if not uv_bin:
            uv_bin = _ensure_uv_for_termux(pip_cmd)
        install_group = "all"

        if uv_bin:
            # Use official managed_python_env() isolation so third-party
            # UV_PYTHON_INSTALL_DIR (e.g. WorkBuddy) cannot hijack uv; then
            # point VIRTUAL_ENV at this install's venv.
            from hermes_cli.managed_uv import managed_python_env

            uv_env = managed_python_env()
            uv_env["VIRTUAL_ENV"] = str(_m().PROJECT_ROOT / "venv")
            if _m()._is_termux_env(uv_env):
                uv_env.pop("PYTHONPATH", None)
                uv_env.pop("PYTHONHOME", None)
                install_group = "termux-all"
                print("  → Termux detected: using uv + curated termux-all optional profile...")
            if not deps_current:
                if _m()._is_termux_env(uv_env) and _is_android_python():
                    print("  → Termux/Android detected: prebuilding psutil with Linux source path compatibility...")
                    _install_psutil_android_compat([uv_bin, "pip"], env=uv_env)
                _m()._install_python_dependencies_with_optional_fallback(
                    [uv_bin, "pip"], env=uv_env, group=install_group
                )
        else:
            # Use sys.executable to explicitly call the venv's pip module,
            # avoiding PEP 668 'externally-managed-environment' errors on Debian/Ubuntu.
            # Some environments lose pip inside the venv; bootstrap it back with
            # ensurepip before trying the editable install.
            pip_cmd = [sys.executable, "-m", "pip"]
            try:
                subprocess.run(
                    pip_cmd + ["--version"],
                    cwd=_m().PROJECT_ROOT,
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError:
                subprocess.run(
                    [sys.executable, "-m", "ensurepip", "--upgrade", "--default-pip"],
                    cwd=_m().PROJECT_ROOT,
                    check=True,
                )
            if _m()._is_termux_env():
                install_group = "termux-all"
                print("  → Termux detected: using curated termux-all optional profile...")
            if not deps_current:
                if _m()._is_termux_env() and _is_android_python():
                    print("  → Termux/Android detected: prebuilding psutil with Linux source path compatibility...")
                    _install_psutil_android_compat(pip_cmd)
                _m()._install_python_dependencies_with_optional_fallback(pip_cmd, group=install_group)

        install_prefix = [uv_bin, "pip"] if uv_bin else pip_cmd
        lazy_env = uv_env if uv_bin else None

        if deps_current:
            # The verification normally runs inside the install we just
            # skipped. Run it here so a wrong skip self-heals into a real
            # install (both verifiers reinstall what they find missing)
            # instead of leaving a venv nobody checked.
            _m()._verify_core_dependencies_installed(
                install_prefix, env=lazy_env, group=install_group
            )
            _m()._verify_console_scripts_installed(install_prefix, env=lazy_env)

        # Core ``.[all]`` install finished. Clear the generic core breadcrumb
        # before the lazy-refresh phase — that phase uses its own marker so a
        # later lazy failure cannot be "healed" by clearing the core marker
        # based on a narrow 7-package import probe (#58004 review).
        _m()._clear_update_incomplete_marker()

        # The update process is still the old Python interpreter process. Run
        # one final cache/module refresh immediately before lazy backend
        # refresh, which imports newly-pulled modules that may depend on fresh
        # symbols in hermes_constants or lazy_deps. The dependency install
        # above may also have regenerated bytecode from build-cache copies —
        # this second sweep catches those stragglers (#60242, #65240).
        removed = _m()._clear_bytecode_cache(_m().PROJECT_ROOT)
        if removed:
            print(
                f"  ✓ Cleared {removed} stale __pycache__ director{'y' if removed == 1 else 'ies'}"
            )
        _m()._record_bytecode_fingerprint()
        _m()._refresh_bootstrap_cache_scripts(branch)
        _m()._reload_updated_runtime_modules()

        # Upgrade pip before lazy refreshes — stale pip can fail source builds
        # and leave partially-written packages (#57828).
        _write_lazy_refresh_incomplete_marker()
        _m()._upgrade_pip_before_lazy_refresh(install_prefix, env=lazy_env)

        # Lazy refresh can corrupt the venv when a backend install fails.
        # Clear the lazy marker only when refresh/repair is confirmed healthy.
        lazy_ok = _m()._refresh_active_lazy_features(
            install_prefix,
            env=lazy_env,
            features=active_lazy_features,
        )
        if lazy_ok:
            _m()._clear_lazy_refresh_incomplete_marker()
        else:
            print(
                "  ⚠ Lazy-refresh recovery incomplete — run `hermes` again "
                "to finish import-based venv repair."
            )

        _m()._restore_active_tool_dependencies(
            active_tool_dependencies,
            install_prefix,
            env=lazy_env,
        )

        # Heal the active memory provider's bridge packages last — the core
        # reinstall + lazy refresh above may have stripped or downgraded
        # plugin.yaml-declared deps that aren't in extras (#53272, #70636).
        _m()._refresh_active_memory_provider_dependencies()

        # Everything that can legitimately produce a transient ImportError has
        # now run (bytecode sweep, dependency reinstall, lazy refresh), so a
        # module that still won't import is real breakage. Warn only — never
        # roll back here: `cannot import name X` is also the signature of the
        # stale-bytecode class (#6207, #60242), and the launch-time sweep in
        # _sweep_stale_bytecode_if_checkout_changed() self-heals that on the
        # next run. A destructive reset would undo a good update over a state
        # that fixes itself.
        import_ok, failing_module, import_error = _validate_critical_modules_import(
            _m().PROJECT_ROOT
        )
        if not import_ok:
            print()
            print(f"  ⚠ {failing_module} still fails to import after updating:")
            print(f"      {import_error}")
            print("    Run `hermes update` again — if it persists, reinstall:")
            print("    https://hermes-agent.nousresearch.com")

        node_failures = _update_node_dependencies()
        _m()._build_web_ui(_m().PROJECT_ROOT / "web")

        desktop_build_ok = _rebuild_desktop_after_update(
            desktop_dir,
            had_desktop_app_before_update=had_desktop_app_before_update,
            pre_update_snapshot_id=pre_update_snapshot_id, _pre_update_plan=_pre_update_plan,
            _windows_gateway_resume=_windows_gateway_resume)
    except _shim_quarantine_error_type() as e:
        # Strict quarantine refused BEFORE any installer ran — defer via marker, exit 2, no ZIP.
        # See #87331.
        _refuse_update_for_contended_shims(e)
    except subprocess.CalledProcessError as e:
        _handle_update_called_process_error(e, args, gateway_mode, had_desktop_app_before_update)


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
from typing import Optional  # noqa: F401,E402
from datetime import datetime  # noqa: F401,E402
import hashlib  # noqa: F401,E402
import json  # noqa: F401,E402
# ---- END PLUGIN-COMPAT ----
