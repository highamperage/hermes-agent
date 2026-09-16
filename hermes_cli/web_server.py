"""Hermes Agent — Web UI server: FastAPI app assembly, auth/host middleware, ``start_server``.

Route handlers live in ``web_routers/``; their helpers live in the sibling
``web_server_<concern>`` modules and are re-imported here so ``web_server.<name>``
stays the single late-binding seam tests monkeypatch (``web_deps.late``).
Usage: ``python -m hermes_cli.main web [--port 8080]``.
"""

from contextlib import asynccontextmanager

import asyncio
from collections import deque
import hmac
import logging
import os
import re
import secrets
import subprocess
import sys
import sysconfig
import threading
import time
import urllib.parse

from hermes_cli.install_identity import get_install_id as _shared_get_install_id
from hermes_cli.pty_session import run_reaper
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hermes_cli import __version__
from hermes_cli.config import load_config

try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
except ImportError:
    # First try lazy-installing the dashboard extras. Only the user actually
    # running `hermes dashboard` needs fastapi+uvicorn; lazy install keeps
    # them out of every other install path. After install, re-import.
    try:
        from tools.lazy_deps import ensure as _lazy_ensure
        _lazy_ensure("tool.dashboard", prompt=False)
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import JSONResponse
    except Exception:
        raise SystemExit(
            "Web UI requires fastapi and uvicorn.\n"
            f"Install with: {sys.executable} -m pip install 'fastapi' 'uvicorn[standard]'"
        )

WEB_DIST = Path(os.environ["HERMES_WEB_DIST"]) if "HERMES_WEB_DIST" in os.environ else Path(__file__).parent / "web_dist"
_log = logging.getLogger(__name__)


from hermes_cli.web_server_lifecycle import (  # noqa: E402
    PORT_IN_USE_EXIT_CODE,
    _dashboard_forwarded_allow_ips,
    _eager_reconcile_own_session_db,
    _maybe_open_browser,
    _port_bind_conflict,
    _read_bound_port,
    _report_port_in_use,
    _start_parent_death_watchdog,
    _warm_gateway_module,
    _write_dashboard_ready_file,
    _write_machine_sentinel_line,
)


def _start_desktop_cron_ticker(stop_event: "threading.Event", interval: int = 60) -> None:
    """Tick the cron scheduler from inside the desktop dashboard backend.

    The desktop spawns a ``hermes dashboard`` backend, not a gateway, so without
    this a cron created in the app would never fire (no live adapters; delivery
    falls back to the per-platform send path). The primary backend outlives the
    per-profile pool (reaped after ~10 idle minutes), so it ticks EVERY local
    profile's store like a multiplex gateway; external providers keep the
    single-store behavior (registries are not profile-scoped). Cross-process
    safe: the built-in tick takes the per-store ``cron/.tick.lock``.

    Every local profile's store is ticked, not just this backend's own (#69377's desktop sibling): the
    desktop pools per-profile backends and reaps them after ~10 idle minutes, so a secondary profile's
    ticker dies with its backend and that profile's jobs silently stop firing until the user next opens it
    ("tasks on the sleeping profile could be idle" — community report, Aug 2026).
    """
    from cron.scheduler_provider import InProcessCronScheduler, resolve_cron_scheduler

    provider = resolve_cron_scheduler()

    start_kwargs: dict = {"interval": interval}
    if isinstance(provider, InProcessCronScheduler):
        try:
            from hermes_cli.profiles import (
                _check_gateway_running, _served_by_running_multiplexer, profiles_to_serve)

            # Same served set as the multiplexer: default + every live profile under profiles/.
            # The ticker re-enumerates this callable every cycle. Passing a
            # startup snapshot leaves deleted profiles in the scheduler until
            # restart, which both writes their removed stores and keeps stale
            # profiles alive in Desktop's background work.
            profile_homes = lambda: list(profiles_to_serve(multiplex=True))
            initial_profile_homes = profile_homes()
            if initial_profile_homes:
                # Even one profile needs the per-tick gateway gate; otherwise
                # Desktop races its dedicated gateway for the same cron store.
                start_kwargs["profile_homes"] = profile_homes
                # Stand down, per tick, for a profile already owned by a gateway — its OWN
                # process, or the live default multiplexer (a served satellite has no gateway.pid
                # of its own). That gateway ticks with live adapters; winning the tick-lock race
                # here would deliver through the standalone path (#100489, #107485).
                start_kwargs["profile_gate"] = lambda name, home: not (
                    _check_gateway_running(Path(home))
                    or (name != "default" and _served_by_running_multiplexer(name)))
                from hermes_logging import enable_profile_log_routing

                enable_profile_log_routing(initial_profile_homes)
                _log.info(
                    "Desktop cron scheduler will tick %d profile(s): %s",
                    len(initial_profile_homes),
                    [name for name, _home in initial_profile_homes],
                )
        except Exception:
            # Fail open to the single-store ticker so the active profile keeps firing.
            _log.exception("Desktop cron: profile enumeration failed; ticking active profile only")

    _log.info("Desktop cron scheduler started (provider=%s, interval=%ds)", provider.name, interval)
    provider.start(stop_event, **start_kwargs)


# Desktop `serve` only (start_server(start_mcp_discovery_after_bind=True)):
# seconds after the READY sentinel before the MCP discovery thread starts.
_DESKTOP_MCP_DISCOVERY_DELAY_S = 1.0


@asynccontextmanager
async def _lifespan(app: "FastAPI"):
    app.state.event_channels = {}  # dict[str, set]
    app.state.event_lock = asyncio.Lock()
    app.state.pty_active_session_files = {}  # dict[str, Path]
    # Serializes chat-argv resolution so concurrent /api/pty connections don't
    # overlap ``npm install`` / ``npm run build``. Locks live on app.state (not
    # module globals) so they bind to the running loop, not the import-time one.
    app.state.chat_argv_lock = asyncio.Lock()

    # Bring state.db schema current BEFORE the first session-list poll
    # (#79531/#80037): a store left behind by `hermes update` otherwise 500s
    # every poll while the read-probe heal loses to sibling lock contention.
    # Daemon thread so a locked store never delays the socket (Desktop
    # ready-probe times out at 10s, GH-73083).
    threading.Thread(
        target=_eager_reconcile_own_session_db,
        daemon=True,
        name="statedb-eager-reconcile",
    ).start()

    # Import hermes_cli.gateway *before* the yield: on Windows + 3.11 the
    # import holds the GIL, so run_in_executor still froze the loop 15-22s and
    # the Desktop's 10s ready-probe timed out (GH-73083).
    _warm_gateway_module()

    # Snapshot the checkout revision so lazy-import paths (model picker) can
    # refuse with "restart required" after `hermes update` replaced the code
    # (#86207); the update flow does not reliably restart the dashboard.
    from gateway.code_skew import record_boot_fingerprint

    record_boot_fingerprint()

    # Hosted Bot rooms belong to the backend process. Recovery may need a
    # contended state.db migration, so keep it off the pre-yield path: Group
    # Chat must degrade on its own rather than block every Desktop feature.
    from tui_gateway import methods_groups as _hosted_groups
    import tui_gateway.server  # noqa: F401

    hosted_room_start_cancel = threading.Event()

    def _start_hosted_rooms() -> None:
        try:
            _hosted_groups.start_hosted_room_service()
        except Exception:
            _log.exception("Hosted Group Chat recovery failed during backend startup")
        finally:
            if hosted_room_start_cancel.is_set():
                _hosted_groups.stop_hosted_room_service(timeout=1.0)

    hosted_room_start_thread = threading.Thread(
        target=_start_hosted_rooms,
        daemon=True,
        name="hosted-room-startup",
    )
    hosted_room_start_thread.start()

    # Desktop-spawned backends (HERMES_DESKTOP=1) fire cron jobs themselves,
    # since the app has no gateway running the scheduler. Server `hermes
    # dashboard` is unaffected — it relies on its own gateway.
    cron_stop: "threading.Event | None" = None
    cron_thread: "threading.Thread | None" = None
    if os.getenv("HERMES_DESKTOP") == "1":
        # Reap an orphaned gateway from an abnormal previous exit (reparented to
        # launchd, still holding the platform WebSocket) before forking a fresh
        # one that would race the same credential (#77276). Runs
        # unconditionally; protection of a healthy standalone gateway lives
        # INSIDE the reaper (registration probed with cleanup_stale=False).
        try:
            from hermes_cli.gateway import _reap_unsupervised_gateway_orphans

            _reap_unsupervised_gateway_orphans()
        except Exception:
            _log.exception("Desktop startup: orphan gateway reap failed")

        cron_stop = threading.Event()
        cron_thread = threading.Thread(
            target=_start_desktop_cron_ticker,
            args=(cron_stop,),
            daemon=True,
            name="desktop-cron-ticker",
        )
        cron_thread.start()

    # Reap idle/dead keep-alive PTY sessions (30-min TTL).
    pty_reaper_task = asyncio.create_task(run_reaper(PTY_REGISTRY))
    # Periodic authenticated self-test feeding the ``dashboard`` component on /api/status.
    selftest_task = asyncio.create_task(_dashboard_selftest_loop())
    # Live auto-archive timer, independent of list requests.
    auto_archive_task = asyncio.create_task(_auto_archive_ticker_loop())

    # Managed local runtime (local_runtime.enabled): bring llama-server back so a
    # restart doesn't strand a llamacpp main model. Off-thread and best-effort;
    # failure falls back to cloud providers like a cold start. Server only —
    # models load on first inference (an empty router holds no VRAM).
    def _boot_local_runtime():
        try:
            from hermes_cli.config import load_config
            from hermes_cli.local_runtime.bootstrap import ensure_local_runtime

            ensure_local_runtime(load_config())
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning("local runtime boot failed: %s", exc)

    threading.Thread(target=_boot_local_runtime, daemon=True, name="local-runtime-boot").start()

    # Nous free tier: the ONE place its identity is created. Inventories credentials, mints only
    # when HERMES_GUEST_ONBOARDING=1, records the answer for setup.status / free_tier.status and
    # broadcasts `setup.ready`. Off-thread so a slow portal never delays the socket; the desktop's
    # first setup.status waits on the record (bounded) instead.
    from hermes_cli.free_tier_bootstrap import start_background_bootstrap

    start_background_bootstrap()

    try:
        yield
    finally:
        hosted_room_start_cancel.set()
        _hosted_groups.stop_hosted_room_service(timeout=5.0)
        hosted_room_start_thread.join(timeout=1.0)
        if cron_stop is not None:
            cron_stop.set()
        pty_reaper_task.cancel()
        selftest_task.cancel()
        auto_archive_task.cancel()
        await PTY_REGISTRY.close_all()
        # Stop the managed llama-server with its parent (an orphan pins VRAM).
        try:
            from hermes_cli.local_runtime.bootstrap import shutdown_local_runtime

            shutdown_local_runtime()
        except Exception:  # noqa: BLE001
            pass
        if os.getenv("HERMES_DESKTOP") == "1":
            _terminate_desktop_managed_gateway()


def _app_state_default(app: "FastAPI", name: str, factory):
    """Return ``app.state.<name>``, lazily creating it for non-``with`` TestClient usages.

    The lifespan normally initialises these on the running event loop (an
    asyncio.Lock created at import time binds to whatever loop was active then).
    """
    try:
        return getattr(app.state, name)
    except AttributeError:
        value = factory()
        setattr(app.state, name, value)
        return value


def _get_chat_argv_lock(app: "FastAPI") -> asyncio.Lock:
    return _app_state_default(app, "chat_argv_lock", asyncio.Lock)


def _get_pty_active_session_files(app: "FastAPI") -> dict[str, Path]:
    return _app_state_default(app, "pty_active_session_files", dict)


app = FastAPI(title="Hermes Agent", version=__version__, lifespan=_lifespan)


# Memory-provider OAuth connect routes live in the memory layer, not here.
from hermes_cli.memory_oauth import router as _memory_oauth_router  # noqa: E402

app.include_router(_memory_oauth_router)

# Session token for sensitive endpoints. The desktop shell mints it via
# HERMES_DASHBOARD_SESSION_TOKEN; otherwise fresh per server start. It dies with
# the process and is injected into the SPA HTML so only the web UI can use it.
def _resolve_session_token() -> str:
    return os.environ.get("HERMES_DASHBOARD_SESSION_TOKEN") or secrets.token_urlsafe(32)


_SESSION_TOKEN = _resolve_session_token()
_SESSION_HEADER_NAME = "X-Hermes-Session-Token"
_SSH_OWNER_NONCE: Optional[str] = None
_SSH_RUNTIME_PURELIB: Optional[Tuple[str, int, int]] = None
_SSH_RUNTIME_MARKER: Optional[str] = None


def _apply_ssh_session_token(token: str) -> None:
    global _SESSION_TOKEN
    if token:
        _SESSION_TOKEN = token


def _apply_ssh_owner_nonce(nonce: Optional[str]) -> None:
    global _SSH_OWNER_NONCE, _SSH_RUNTIME_PURELIB, _SSH_RUNTIME_MARKER
    _SSH_OWNER_NONCE = nonce
    _SSH_RUNTIME_PURELIB = None
    _SSH_RUNTIME_MARKER = None
    if nonce:
        try:
            purelib = sysconfig.get_paths()["purelib"]
        except (KeyError, OSError):
            return
        # Primary identity: a marker FILE in site-packages. A replaced venv
        # loses it deterministically; pip installs leave it. A bare (dev, ino)
        # snapshot alone is NOT enough: ext4 reuses directory inodes at once,
        # so `rm -rf venv && uv venv` can land on the same inode undetected.
        try:
            marker = os.path.join(purelib, f".hermes-ssh-runtime-{nonce}")
            with open(marker, "w", encoding="utf-8") as fh:
                fh.write(f"pid={os.getpid()}\n")
            _SSH_RUNTIME_MARKER = marker
        except OSError:
            pass  # read-only site-packages — fall back to the stat snapshot
        try:
            st = os.stat(purelib)
            _SSH_RUNTIME_PURELIB = (purelib, st.st_dev, st.st_ino)
        except OSError:
            pass


def _ssh_runtime_intact() -> bool:
    if _SSH_RUNTIME_MARKER is not None:
        return os.path.isfile(_SSH_RUNTIME_MARKER)
    # Fallback (read-only site-packages): directory identity snapshot — weaker
    # (inode reuse) but catches cross-device moves and version-bump paths.
    if _SSH_RUNTIME_PURELIB is None:
        return True
    purelib, device, inode = _SSH_RUNTIME_PURELIB
    try:
        st = os.stat(purelib)
    except OSError:
        return False
    return (st.st_dev, st.st_ino) == (device, inode)


# In-browser Chat tab (/chat, /api/pty, /api/ws): always enabled. A module
# constant (not an inlined True) so the WS endpoints and SPA token injection
# share one testable seam.
_DASHBOARD_EMBEDDED_CHAT_ENABLED = True

# Desktop file.attach sends a whole base64 data URL in one JSON-RPC frame;
# uvicorn's 16 MiB default rejects files under the 256 MiB raw attach cap.
_DESKTOP_ATTACHMENT_WS_MAX_BYTES = 384 * 1024 * 1024


# CORS: localhost origins only — allow_origins=["*"] on 0.0.0.0 would let any
# website read/modify config and secrets.
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoints that do NOT require the session token; everything else under /api/
# is gated below. Shared with the OAuth gate so the two allowlists cannot
# drift (/api/status once 401'd under the OAuth gate, breaking the portal probe).
from hermes_cli.dashboard_auth.public_paths import PUBLIC_API_PATHS as _PUBLIC_API_PATHS


def _has_valid_session_token(request: Request) -> bool:
    """True if the request carries a valid dashboard session token.

    The dedicated header avoids collisions with reverse proxies that already use
    ``Authorization`` (Caddy ``basic_auth``); the legacy Bearer path stays for
    older dashboard bundles.
    """
    session_header = request.headers.get(_SESSION_HEADER_NAME, "")
    if session_header and hmac.compare_digest(session_header.encode(), _SESSION_TOKEN.encode()):
        return True
    auth = request.headers.get("authorization", "")
    return hmac.compare_digest(auth.encode(), f"Bearer {_SESSION_TOKEN}".encode())


# Routes that may also authenticate via ``?token=`` (download links opened by
# the OS shell / a new tab, where no header can be set). Kept narrow.
_QUERY_TOKEN_API_PATHS: frozenset[str] = frozenset({"/api/files/download"})


def _has_valid_query_token(request: Request, path: str) -> bool:
    if path not in _QUERY_TOKEN_API_PATHS:
        return False
    token = request.query_params.get("token", "")
    return bool(token) and hmac.compare_digest(token.encode(), _SESSION_TOKEN.encode())


def _require_token(request: Request) -> None:
    """Authorize a sensitive endpoint, raising 401 if the caller isn't allowed.

    Loopback mode (``auth_required`` False): validate the SPA-injected
    ``_SESSION_TOKEN``. Gated mode: the token is NOT injected (cookie auth), and
    ``gated_auth_middleware`` already 401'd anything without a verified
    ``request.state.session`` — requiring the absent token here would make every
    ``_require_token`` endpoint unreachable behind the gate, so defer to it.
    """
    if getattr(request.app.state, "auth_required", False):
        ok = getattr(request.state, "session", None) is not None
    else:
        ok = _has_valid_session_token(request)
    if not ok:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Accepted Host values for loopback binds. DNS rebinding TTL-flips an attacker
# hostname to 127.0.0.1 so the browser treats it as same-origin; validating Host
# at the app layer rejects it. See GHSA-ppp5-vxwm-4cf7.
_LOOPBACK_HOST_VALUES: frozenset = frozenset({"localhost", "127.0.0.1", "::1"})


def _dashboard_public_hosts() -> frozenset[str]:
    """Return the exact hostname declared by ``dashboard.public_url``.

    One source of truth for OAuth redirects, Host and WS Origin validation.
    Malformed or unset values fail closed as an empty set.
    """
    from hermes_cli.dashboard_auth.prefix import resolve_public_url

    public_url = resolve_public_url()
    try:
        hostname = urllib.parse.urlparse(public_url).hostname if public_url else None
    except ValueError:
        hostname = None
    return frozenset({hostname.lower()}) if hostname else frozenset()


def should_require_auth(host: str, allow_public: bool = False) -> bool:
    """True iff the auth gate must be active: any non-loopback bind.

    RFC1918 / CGNAT / link-local are deliberately PUBLIC — a hostile LAN device
    is the threat model. ``allow_public`` (legacy ``--insecure``) is accepted for
    old launch scripts but IGNORED since the June 2026 hermes-0day campaign.
    """
    return host not in _LOOPBACK_HOST_VALUES


def should_require_dashboard_auth(
    host: str,
    trusted_public_hosts: Optional[frozenset[str]] = None,
) -> bool:
    """Gate required for a non-loopback bind OR a non-loopback ``dashboard.public_url``.

    Callers may pass the already-resolved host set so startup and request
    validation share one snapshot.
    """
    if trusted_public_hosts is None:
        trusted_public_hosts = _dashboard_public_hosts()
    return should_require_auth(host) or any(h not in _LOOPBACK_HOST_VALUES for h in trusted_public_hosts)


def _desktop_loopback_auth_exempt(
    host: str,
    ssh_session_token: Optional[str] = None,
    ssh_owner_nonce: Optional[str] = None,
) -> bool:
    """True for a Desktop-owned loopback backend (#96490).

    A non-loopback ``dashboard.public_url`` would otherwise engage the
    ticket-only gate for the private loopback backends Desktop spawns, whose
    per-spawn session token the gate's WS path refuses — Desktop could not boot.
    The public dashboard is a separate non-loopback process that stays gated, so
    this never opens the public surface. Requires ALL of: loopback bind,
    ``HERMES_DESKTOP=1``, and an operator-minted credential (env token, SSH
    session token, or owner nonce).
    """
    return (
        host in _LOOPBACK_HOST_VALUES
        and os.environ.get("HERMES_DESKTOP") == "1"
        and bool(os.environ.get("HERMES_DASHBOARD_SESSION_TOKEN") or ssh_session_token or ssh_owner_nonce)
    )


def _host_header_hostname(host_header: str) -> str:
    """Return a normalized hostname from a valid HTTP Host authority.

    Host headers are authorities, not full URLs. Reject ambiguous ports,
    malformed IPv6 brackets, and URL syntax so validation always fails closed.
    """
    value = (host_header or "").strip()
    if not value or "://" in value or any(c in value for c in '"\'<> \n\r\t/?#@'):
        return ""

    if value.startswith("["):
        close = value.find("]")
        if close == -1:
            return ""
        hostname = value[1:close]
        # Bracket notation is reserved for IPv6 literals.
        if ":" not in hostname:
            return ""
        suffix = value[close + 1:]
        if suffix and not re.fullmatch(r":\d+", suffix):
            return ""
        return hostname.lower()

    # Unbracketed IPv6 authorities are ambiguous with a port separator.
    if value.count(":") > 1:
        return ""
    if ":" in value:
        hostname, port = value.rsplit(":", 1)
        if not hostname or not port.isdigit():
            return ""
        return hostname.lower()
    return value.lower()


def _is_accepted_host(
    host_header: str,
    bound_host: str,
    trusted_public_hosts: frozenset[str] = frozenset(),
) -> bool:
    """True if the Host header targets the interface we bound to.

    Accepts:
    - Exact bound host (with or without port suffix)
    - Loopback aliases when bound to loopback
    - Exact operator-declared public hosts (with or without port suffix)
    - Any host when bound to 0.0.0.0 (explicit opt-in to non-loopback,
      no protection possible at this layer)
    """
    host_only = _host_header_hostname(host_header)
    if not host_only:
        return False
    # All-interfaces bind: no Host-layer defence is possible; rely on operator
    # network controls.
    if host_only in trusted_public_hosts or bound_host in {"0.0.0.0", "::"}:
        return True
    bound_lc = bound_host.lower()
    if bound_lc in _LOOPBACK_HOST_VALUES:
        return host_only in _LOOPBACK_HOST_VALUES
    return host_only == bound_lc


@app.middleware("http")
async def host_header_middleware(request: Request, call_next):
    """Reject requests whose Host header doesn't match the bound interface (DNS rebinding, GHSA-ppp5-vxwm-4cf7)."""
    # app.state.bound_host is set by start_server() at listen time.
    bound_host = getattr(app.state, "bound_host", None)
    if bound_host and not _is_accepted_host(
        request.headers.get("host", ""), bound_host, getattr(app.state, "trusted_public_hosts", frozenset())
    ):
        return JSONResponse(
            status_code=400,
            content={
                "detail": (
                    "Invalid Host header. Dashboard requests must use the "
                    "bound hostname or the configured public hostname."
                ),
            },
        )
    return await call_next(request)


@app.middleware("http")
async def _plugin_api_runtime_gate(request: Request, call_next):
    """Block requests to disabled plugin API routes at request time.

    :func:`_mount_plugin_api_routes` gates at import time; a plugin disabled
    while running keeps its router mounted until restart, so enforce on every
    ``/api/plugins/{name}/...`` request. Registered BEFORE the auth middlewares
    (runs AFTER them): an unauthenticated caller must get auth's 401, never this
    404, or the status code becomes a plugin-name oracle.
    """
    path = request.url.path
    # parts: ['', 'api', 'plugins', '<name>', ...]
    parts = path.split("/")
    plugin_name = parts[3] if path.startswith("/api/plugins/") and len(parts) >= 4 else ""
    # Only gate authenticated requests. Unauthenticated ones fall through so
    # auth_middleware / the OAuth gate return 401 first and this route can't
    # be used as a plugin-name oracle.
    if plugin_name and (
        getattr(request.state, "token_authenticated", False)
        or getattr(request.app.state, "auth_required", False)
        or _has_valid_session_token(request)
        or _has_valid_query_token(request, path)
    ):
        try:
            # Gate: only serve user plugins that are in plugins.enabled and not in plugins.disabled. This
            # prevents the frontend from loading JS/CSS from plugins the user has not explicitly activated.
            # (#46435)
            from hermes_cli.plugins_cmd import _get_enabled_set, _get_disabled_set
            enabled_set = _get_enabled_set()
            disabled_set = _get_disabled_set()
        except Exception:
            enabled_set = set()
            disabled_set = set()
        # Source from the cached plugin list; unknown => user plugin (safe default — blocks).
        plugin = next((p for p in _get_dashboard_plugins() if p.get("name") == plugin_name), None)
        source = plugin.get("source") if plugin else "user"
        blocked = plugin_name in disabled_set or (source == "user" and plugin_name not in enabled_set)
        if blocked and source in ("user", "bundled"):
            return JSONResponse(status_code=404, content={"detail": "Plugin not found"})
    return await call_next(request)


@app.middleware("http")
async def _dashboard_auth_gate(request: Request, call_next):
    """OAuth gate — active only when start_server flags ``auth_required``; pass-through on loopback.

    Registered between host_header and auth_middleware: host check → cookie auth → token auth.
    """
    from hermes_cli.dashboard_auth.middleware import gated_auth_middleware
    return await gated_auth_middleware(request, call_next)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Require the session token on all /api/ routes except the public list.

    Skipped for requests the token-auth seam already authenticated
    (``token_authenticated``) and when the OAuth gate is active — cookie auth is
    then authoritative and the loopback-only token path must not override it.
    """
    path = request.url.path
    if (
        not getattr(request.state, "token_authenticated", False)
        and not getattr(request.app.state, "auth_required", False)
        and path.startswith("/api/")
        and path not in _PUBLIC_API_PATHS
        and not path.startswith("/api/mcp/oauth/callback/")
        and not _has_valid_session_token(request)
        and not _has_valid_query_token(request, path)
    ):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.middleware("http")
async def _token_auth_seam(request: Request, call_next):
    """Outermost auth seam: bearer-token auth for opted-in routes (registered LAST = runs FIRST).

    A registered token route is owned here — authenticate, attach the principal
    + ``token_authenticated`` so downstream gates skip enforcement. Non-token
    routes pass through untouched.
    """
    from hermes_cli.dashboard_auth.token_auth import token_auth_middleware
    return await token_auth_middleware(request, call_next)


_DASHBOARD_HEALTH_WINDOW_SECONDS = 300.0


class DashboardHealth:
    """Dashboard-process health: rolling unhandled-error/5xx window + periodic self-test result.

    Feeds ``components`` on the PUBLIC ``/api/status``, so :meth:`snapshot`
    exports counts and enums only — never ``last_error_type``/``last_error_path``.
    """

    def __init__(self, window_seconds: float = _DASHBOARD_HEALTH_WINDOW_SECONDS) -> None:
        self.window_seconds = window_seconds
        self._error_times: "deque[float]" = deque(maxlen=256)
        self.last_error_type: Optional[str] = None
        self.last_error_path: Optional[str] = None  # internal-only, never serialized
        self.last_error_at: Optional[float] = None
        self.selftest_status: str = "unknown"  # unknown | ok | failing
        self.selftest_http_status: Optional[int] = None
        self.selftest_at: Optional[float] = None

    def record_error(self, exc_type: str, path: str) -> None:
        now = time.time()
        self._error_times.append(now)
        self.last_error_type = exc_type
        self.last_error_path = path
        self.last_error_at = now

    def record_selftest(self, passed: bool, http_status: Optional[int]) -> None:
        self.selftest_status = "ok" if passed else "failing"
        self.selftest_http_status = http_status
        self.selftest_at = time.time()

    def recent_error_count(self) -> int:
        cutoff = time.time() - self.window_seconds
        while self._error_times and self._error_times[0] < cutoff:
            self._error_times.popleft()
        return len(self._error_times)

    def snapshot(self) -> Dict[str, Any]:
        """Public component payload: status enum + counts + timestamps only."""
        errors = self.recent_error_count()
        status = "degraded" if (errors or self.selftest_status == "failing") else "ok"
        return {
            "status": status,
            "recent_unhandled_errors": errors,
            "last_error_at": self.last_error_at,
            "selftest": self.selftest_status,
        }


DASHBOARD_HEALTH = DashboardHealth()


@app.middleware("http")
async def _dashboard_health_middleware(request: Request, call_next):
    """Outermost middleware (registered last): count unhandled exceptions and 5xx; re-raises, never alters."""
    try:
        response = await call_next(request)
    except Exception as exc:
        DASHBOARD_HEALTH.record_error(type(exc).__name__, request.url.path)
        raise
    if response.status_code >= 500:
        DASHBOARD_HEALTH.record_error(f"http_{response.status_code}", request.url.path)
    return response


# Authenticated-route self-test: one in-process request per minute against a
# cheap DB-touching route, catching "liveness fine but every authed request 500s".
_DASHBOARD_SELFTEST_INTERVAL_SECONDS = 60.0
_DASHBOARD_SELFTEST_ROUTE = "/api/sessions?limit=1"


async def _dashboard_selftest_once() -> None:
    """Run one authenticated in-process self-test request and record it."""
    try:
        import httpx
    except ImportError:
        return  # optional dependency — leave status "unknown"
    try:
        # Loopback base_url so the Host-header middleware accepts the request.
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            resp = await client.get(_DASHBOARD_SELFTEST_ROUTE, headers={_SESSION_HEADER_NAME: _SESSION_TOKEN})
        DASHBOARD_HEALTH.record_selftest(resp.status_code == 200, resp.status_code)
    except Exception:
        DASHBOARD_HEALTH.record_selftest(False, None)


async def _dashboard_selftest_loop() -> None:
    """Periodic self-test driver started from the lifespan."""
    try:
        import httpx  # noqa: F401
    except ImportError:
        _log.debug("httpx unavailable — dashboard self-test disabled")
        return
    while True:
        await asyncio.sleep(_DASHBOARD_SELFTEST_INTERVAL_SECONDS)
        # OAuth-gated binds don't honour the session token; the probe would false-alarm 401.
        if getattr(app.state, "auth_required", False):
            continue
        await _dashboard_selftest_once()




# Action registries/spawner are owned by web_server_gateway; routers and tests reach them
# there, so this module reads them through the module too (one patch seam).
from hermes_cli import web_server_gateway as _gateway_mod  # noqa: E402
from hermes_cli.web_server_gateway import _ACTION_LOG_FILES, _terminate_desktop_managed_gateway  # noqa: E402
from hermes_cli.web_server_sessions import _auto_archive_ticker_loop  # noqa: E402
from hermes_cli.web_server_chat import PTY_REGISTRY  # noqa: E402
from hermes_cli.web_server_dashboard import (  # noqa: E402
    _discover_dashboard_plugins, _mount_plugin_api_routes, mount_spa,
)


_GATEWAY_HEALTH_URL = os.getenv("GATEWAY_HEALTH_URL")
_GATEWAY_HEALTH_TIMEOUT_MAX = 1.0
try:
    _GATEWAY_HEALTH_TIMEOUT = float(os.getenv("GATEWAY_HEALTH_TIMEOUT", "1"))
except (ValueError, TypeError):
    _log.warning(
        "Invalid GATEWAY_HEALTH_TIMEOUT value %r — using default 1.0s",
        os.getenv("GATEWAY_HEALTH_TIMEOUT"),
    )
    _GATEWAY_HEALTH_TIMEOUT = 1.0
if _GATEWAY_HEALTH_TIMEOUT <= 0:
    _log.warning(
        "Invalid non-positive GATEWAY_HEALTH_TIMEOUT value %.3fs — using default 1.0s",
        _GATEWAY_HEALTH_TIMEOUT,
    )
    _GATEWAY_HEALTH_TIMEOUT = 1.0
elif _GATEWAY_HEALTH_TIMEOUT > _GATEWAY_HEALTH_TIMEOUT_MAX:
    _log.warning(
        "Capping GATEWAY_HEALTH_TIMEOUT %.3fs to %.3fs for dashboard liveness probes",
        _GATEWAY_HEALTH_TIMEOUT,
        _GATEWAY_HEALTH_TIMEOUT_MAX,
    )
    _GATEWAY_HEALTH_TIMEOUT = _GATEWAY_HEALTH_TIMEOUT_MAX


_MANAGED_FILE_MAX_BYTES = 100 * 1024 * 1024
_FS_DATA_URL_MAX_BYTES = 16 * 1024 * 1024
# Multipart uploads stream to a temp file in fixed chunks and rename into
# place: constant memory, no base64 inflation, no proxy body-size 502s (NS-501).
_UPLOAD_CHUNK_BYTES = 1024 * 1024

# Stable install identity for /api/status: one uuid4 hex per physical install,
# persisted under the ROOT Hermes home (not the profile HERMES_HOME) so every
# profile reports the same id and the desktop can collapse duplicate roster rows
# for one backend. Must never change across restarts, so cached per process.
_INSTALL_ID_CACHE: Dict[str, Optional[str]] = {"root": None, "value": None}


def get_install_id() -> Optional[str]:
    """Process-lifetime-cached stable install id."""
    return _shared_get_install_id(cache=_INSTALL_ID_CACHE)


# Serializes config.yaml read-modify-write cycles for handlers on worker threads
# (asyncio.to_thread): config.py's _CONFIG_LOCK covers each load/save call, not
# the span between them, so two off-loop updates could drop each other's writes.
# RLock so nested helpers that also take it can't self-deadlock.
_CONFIG_MUTATION_LOCK = threading.RLock()

# A finished ``gateway-restart`` child does not mean the gateway is back (it
# exits once the restart is handed off), so in-flight reuse stops coalescing
# exactly when a stale frontend re-fires every few seconds (#89034: 77 restarts,
# state.db corrupted mid-FTS5-write). MAINTAINER DECISION: a fixed window, not
# "until healthy" — a gateway that never returns must not leave the action
# inert. 10s is above the ~3.5s storm spacing and below an operator's retry.
GATEWAY_RESTART_COOLDOWN_SECONDS = 10.0

# ``(monotonic spawn time, Popen, command)`` of the last restart. Deliberately
# NOT read from ``_ACTION_PROCS``: entries there vanish when the child exits.
_LAST_GATEWAY_RESTART: Optional[Tuple[float, subprocess.Popen, Tuple[str, ...]]] = None


def _spawn_gateway_restart(profile: Optional[str] = None) -> Tuple[subprocess.Popen, bool]:
    """Spawn ``hermes gateway restart``, reusing an in-flight or recent restart.

    Concurrent children race each other on the kill-and-start path, so a live
    child is reused; requests within ``GATEWAY_RESTART_COOLDOWN_SECONDS`` for the
    same profile coalesce onto the last spawn too (#89034). Orphaned gateways
    are reaped first so the fresh one doesn't stack a duplicate (#77276).
    Returns ``(proc, reused)``.
    """
    try:
        from hermes_cli.gateway import _reap_unsupervised_gateway_orphans

        _reap_unsupervised_gateway_orphans()
    except Exception:
        pass  # best-effort — don't block the restart on a reap failure

    global _LAST_GATEWAY_RESTART

    subcommand = _gateway_mod._gateway_subcommand(profile, "restart")
    existing = _gateway_mod._ACTION_PROCS.get("gateway-restart")
    if existing is not None and existing.poll() is None:
        existing_command = _gateway_mod._ACTION_COMMANDS.get("gateway-restart")
        if existing_command is None or existing_command == tuple(subcommand):
            return existing, True
        raise RuntimeError("gateway restart already in progress for another profile")

    recent = _LAST_GATEWAY_RESTART
    if recent is not None:
        spawned_at, recent_proc, recent_command = recent
        age = time.monotonic() - spawned_at if recent_command == tuple(subcommand) else None
        if age is not None and age < GATEWAY_RESTART_COOLDOWN_SECONDS:
            _log.info(
                "Coalescing gateway restart: one was started %.1fs ago "
                "(pid %s) and the gateway may still be coming back; not "
                "spawning another (#89034).",
                age,
                getattr(recent_proc, "pid", "?"),
            )
            return recent_proc, True

    proc = _gateway_mod._spawn_hermes_action(subcommand, "gateway-restart")
    _LAST_GATEWAY_RESTART = (time.monotonic(), proc, tuple(subcommand))
    return proc, False


# Collapses repeated identical ElevenLabs voice-list failures (the desktop
# re-polls on every settings focus) to one log line; re-arms on success or a
# changed signature.
_voice_list_last_error: Optional[str] = None


def _voice_list_error_logged_once(signature: Optional[str]) -> bool:
    """True if ``signature`` is new and should be logged now; ``None`` clears the latch."""
    global _voice_list_last_error
    if signature is None:
        _voice_list_last_error = None
        return False
    if signature == _voice_list_last_error:
        return False
    _voice_list_last_error = signature
    return True


_ACTION_LOG_FILES.setdefault("computer-use-grant", "action-computer-use-grant.log")


# ---------------------------------------------------------------------------
# Pairing endpoints — approve / revoke / list messaging pairing codes.
#
# These are how a remote admin onboards messaging users (Telegram, Discord, …)
# without shell access.  Wraps gateway.pairing.PairingStore directly.
# ---------------------------------------------------------------------------


def _pairing_store(profile: Optional[str] = None):
    """Pairing store for ``profile`` — the dashboard's own when unspecified.

    Every other admin endpoint scopes by profile, and the gateway already
    keeps one store per served profile (``gateway/run.py``). Without this the
    dashboard and desktop always read the global store, so an operator on a
    named profile approves into a whitelist their gateway never consults.

    ``PairingStore`` resolves the profile's home itself (``default`` maps back
    to the global store), so this only needs to validate the name — no
    ``_profile_scope`` needed, and nothing process-global is swapped across
    the ``await`` boundary.
    """
    from gateway.pairing import PairingStore

    requested = (profile or "").strip()
    if not requested or requested.lower() == "current":
        return PairingStore()

    _resolve_profile_dir(requested)  # 400/404 on an unknown profile

    return PairingStore(profile=requested)


@app.get("/api/pairing")
async def list_pairing(profile: Optional[str] = None):
    store = _pairing_store(profile)
    return {
        "pending": store.list_pending(),
        "approved": store.list_approved(),
    }


@app.post("/api/pairing/approve")
async def approve_pairing(body: PairingApprove):
    store = _pairing_store(body.profile)
    platform = (body.platform or "").lower().strip()
    # `request_id` is what an admin surface sends after listing pending
    # requests; `code` is the one-time code the user relays from their DM.
    # A GUI that only knows the older field name still works — a value with
    # request-id shape routes to the request path either way.
    target = (body.request_id or body.code or "").strip()
    if not platform or not target:
        raise HTTPException(
            status_code=400, detail="platform and request_id or code are required"
        )

    by_request_id = bool(body.request_id) or store.looks_like_request_id(target)
    if by_request_id:
        result = store.approve_request(platform, target)
    else:
        result = store.approve_code(platform, target.upper())

    if result:
        return {"ok": True, "user": result}
    # Lockout only gates the code path, so only report it there — otherwise a
    # stale request id would surface as a bogus 429 while the platform sat
    # locked out for an unrelated reason.
    if not by_request_id and store._is_locked_out(platform):
        raise HTTPException(
            status_code=429,
            detail=f"Platform '{platform}' is locked out after too many failed approvals.",
        )
    raise HTTPException(
        status_code=404,
        detail=f"Pairing request or code not found or expired for platform '{platform}'.",
    )


@app.post("/api/pairing/revoke")
async def revoke_pairing(body: PairingRevoke):
    store = _pairing_store(body.profile)
    platform = (body.platform or "").lower().strip()
    if not platform or not body.user_id:
        raise HTTPException(status_code=400, detail="platform and user_id are required")
    if store.revoke(platform, body.user_id):
        return {"ok": True}
    raise HTTPException(
        status_code=404,
        detail=f"User {body.user_id} not found in approved list for {platform}.",
    )


@app.post("/api/pairing/clear-pending")
async def clear_pending_pairing(profile: Optional[str] = None):
    store = _pairing_store(profile)
    count = store.clear_pending()
    return {"ok": True, "cleared": count}


# ---------------------------------------------------------------------------
# Webhook subscription endpoints — list / subscribe / remove.
#
# Wraps the same JSON store the CLI uses (hermes_cli.webhook); the webhook
# adapter hot-reloads it without a gateway restart.  Per-route HMAC secrets
# are redacted on read and surfaced once on create.
# ---------------------------------------------------------------------------


def _webhook_route_summary(name: str, route: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    return {
        "name": name,
        "description": route.get("description", ""),
        "events": list(route.get("events") or []),
        "deliver": route.get("deliver", "log"),
        "deliver_only": bool(route.get("deliver_only")),
        "prompt": route.get("prompt", ""),
        "script": route.get("script", ""),
        "skills": list(route.get("skills") or []),
        "created_at": route.get("created_at"),
        "url": f"{base_url}/webhooks/{name}",
        # Secret is masked on read; full value only returned on create.
        "secret_set": bool(route.get("secret")),
        # Default-enabled; only an explicit enabled:false turns a route off.
        "enabled": route.get("enabled", True) is not False,
    }


@app.get("/api/webhooks")
async def list_webhooks():
    import hermes_cli.webhook as wh

    base_url = wh._get_webhook_base_url()
    subs = wh._load_subscriptions()
    return {
        "enabled": wh._is_webhook_enabled(),
        "base_url": base_url,
        "subscriptions": [
            _webhook_route_summary(name, route, base_url)
            for name, route in subs.items()
        ],
    }


@app.post("/api/webhooks/enable")
async def enable_webhooks():
    try:
        _write_platform_enabled("webhook", True)
    except Exception as exc:
        _log.exception("Failed to enable webhook platform from dashboard")
        raise HTTPException(
            status_code=500,
            detail="Failed to enable webhook platform.",
        ) from exc

    restart_result = _restart_gateway_after_webhook_enable()
    return {
        "ok": True,
        "platform": "webhook",
        "enabled": True,
        "needs_restart": not restart_result["restart_started"],
        **restart_result,
    }


@app.post("/api/webhooks")
async def create_webhook(body: WebhookCreate):
    import re as _re
    import secrets as _secrets
    import time as _time
    import hermes_cli.webhook as wh

    if not wh._is_webhook_enabled():
        raise HTTPException(
            status_code=400,
            detail="Webhook platform is not enabled. Enable it from the Webhooks page first.",
        )

    name = (body.name or "").strip().lower().replace(" ", "-")
    if not _re.match(r"^[a-z0-9][a-z0-9_-]*$", name):
        raise HTTPException(
            status_code=400,
            detail="Invalid name. Use lowercase alphanumeric with hyphens/underscores.",
        )

    if body.deliver_only and body.deliver == "log":
        raise HTTPException(
            status_code=400,
            detail="Direct delivery requires a real target (telegram, discord, …), not 'log'.",
        )

    secret = body.secret or _secrets.token_urlsafe(32)
    route: Dict[str, Any] = {
        "description": body.description or f"Dashboard-created subscription: {name}",
        "events": [e.strip() for e in body.events if e.strip()],
        "secret": secret,
        "prompt": body.prompt or "",
        "skills": [s.strip() for s in body.skills if s.strip()],
        "deliver": body.deliver or "log",
        "created_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
    }
    if body.script and body.script.strip():
        route["script"] = body.script.strip()
    if body.deliver_only:
        route["deliver_only"] = True
    if body.deliver_chat_id:
        route["deliver_extra"] = {"chat_id": body.deliver_chat_id}

    subs = wh._load_subscriptions()
    subs[name] = route
    wh._save_subscriptions(subs)

    base_url = wh._get_webhook_base_url()
    summary = _webhook_route_summary(name, route, base_url)
    # Surface the secret exactly once, on create.
    summary["secret"] = secret
    return summary


@app.delete("/api/webhooks/{name}")
async def delete_webhook(name: str):
    import hermes_cli.webhook as wh

    key = (name or "").strip().lower()
    subs = wh._load_subscriptions()
    if key not in subs:
        raise HTTPException(status_code=404, detail=f"No subscription named '{key}'")
    del subs[key]
    wh._save_subscriptions(subs)
    return {"ok": True}


@app.put("/api/webhooks/{name}/enabled")
async def set_webhook_enabled(name: str, body: WebhookEnabledToggle):
    """Enable or disable a webhook route.

    Disabled routes stay in the subscriptions file (so they can be
    re-enabled) but the gateway rejects incoming events with 403.  The
    gateway hot-reloads the subscriptions file, so this takes effect on the
    next event without a restart.
    """
    import hermes_cli.webhook as wh

    key = (name or "").strip().lower()
    subs = wh._load_subscriptions()
    if key not in subs:
        raise HTTPException(status_code=404, detail=f"No subscription named '{key}'")
    subs[key]["enabled"] = bool(body.enabled)
    wh._save_subscriptions(subs)
    return {"ok": True, "name": key, "enabled": bool(body.enabled)}


# ---------------------------------------------------------------------------
# Gateway lifecycle endpoints — start / stop.
#
# restart + update already exist above; these complete the lifecycle so a
# remote admin can bring the gateway up or down without shell access.  Both
# spawn the real `hermes gateway <verb>` so behaviour matches the CLI exactly.
# Status is already surfaced by /api/status (gateway_running/state/platforms).
# ---------------------------------------------------------------------------


@app.post("/api/gateway/start")
async def start_gateway(profile: Optional[str] = None):
    try:
        proc = _spawn_hermes_action(_gateway_subcommand(profile, "start"), "gateway-start")
    except HTTPException:
        raise
    except Exception as exc:
        _log.exception("Failed to spawn gateway start")
        raise HTTPException(status_code=500, detail=f"Failed to start gateway: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "gateway-start"}


@app.post("/api/gateway/stop")
async def stop_gateway(profile: Optional[str] = None):
    try:
        proc = _spawn_hermes_action(_gateway_subcommand(profile, "stop"), "gateway-stop")
    except HTTPException:
        raise
    except Exception as exc:
        _log.exception("Failed to spawn gateway stop")
        raise HTTPException(status_code=500, detail=f"Failed to stop gateway: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "gateway-stop"}


# ---------------------------------------------------------------------------
# Credential pool endpoints — list / add / remove rotation keys.
#
# The credential pool (auth.json -> credential_pool.<provider>[]) holds the
# rotating API keys the agent round-robins through.  Secrets are redacted on
# read; only the agent ever sees the raw values at session start.
# ---------------------------------------------------------------------------


def _pool_entry_summary(entry: Any, index: int) -> Dict[str, Any]:
    """Redacted, display-safe view of one PooledCredential.

    ``index`` is 1-based to match CredentialPool.remove_index().
    """
    token = getattr(entry, "access_token", "") or ""
    return {
        "index": index,
        "id": getattr(entry, "id", None),
        "label": getattr(entry, "label", None),
        "auth_type": getattr(entry, "auth_type", None),
        "source": getattr(entry, "source", None),
        "priority": getattr(entry, "priority", 0),
        "last_status": getattr(entry, "last_status", None),
        "request_count": getattr(entry, "request_count", 0),
        "token_preview": redact_key(token) if token else "",
        "has_refresh": bool(getattr(entry, "refresh_token", None)),
    }


@app.get("/api/credentials/pool")
async def list_credential_pool():
    from agent.credential_pool import load_pool
    from hermes_cli.auth import read_credential_pool

    providers = []
    # read_credential_pool(None) lists every provider that has pooled entries;
    # load_pool() then gives us the rich PooledCredential objects per provider.
    raw_pool = read_credential_pool()
    for provider_id in sorted(raw_pool.keys()):
        try:
            pool = load_pool(provider_id)
        except Exception:
            _log.exception("load_pool(%s) failed", provider_id)
            continue
        entries = pool.entries()
        if not entries:
            continue
        providers.append({
            "provider": provider_id,
            "entries": [
                _pool_entry_summary(e, i) for i, e in enumerate(entries, start=1)
            ],
        })
    return {"providers": providers}


@app.post("/api/credentials/pool")
async def add_credential_pool_entry(body: CredentialPoolAdd):
    import uuid as _uuid
    from agent.credential_pool import (
        load_pool,
        PooledCredential,
        AUTH_TYPE_API_KEY,
        CUSTOM_POOL_PREFIX,
        SOURCE_MANUAL,
    )

    provider = (body.provider or "").strip().lower()
    api_key = (body.api_key or "").strip()
    if not provider or not api_key:
        raise HTTPException(status_code=400, detail="provider and api_key are required")

    try:
        pool = load_pool(provider)
        label = (body.label or "").strip() or f"key #{len(pool.entries()) + 1}"
        entry = PooledCredential(
            provider=provider,
            id=_uuid.uuid4().hex[:6],
            label=label,
            auth_type=AUTH_TYPE_API_KEY,
            priority=0,
            source=SOURCE_MANUAL,
            access_token=api_key,
        )
        pool.add_entry(entry)
        # Re-adding a credential is an explicit re-engagement signal: lift
        # every suppression for this provider so a source deleted earlier
        # (via DELETE below or `hermes auth remove`) can seed again.
        # Mirrors the `hermes auth add` behaviour in auth_commands.py.
        if not provider.startswith(CUSTOM_POOL_PREFIX):
            try:
                from hermes_cli.auth import (
                    _load_auth_store,
                    unsuppress_credential_source,
                )
                suppressed = _load_auth_store().get("suppressed_sources", {})
                for src in list(suppressed.get(provider, []) or []):
                    unsuppress_credential_source(provider, src)
            except Exception:
                _log.exception("unsuppress after pool add failed (non-fatal)")
    except HTTPException:
        raise
    except Exception as exc:
        _log.exception("POST /api/credentials/pool failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "provider": provider, "count": len(pool.entries())}


@app.delete("/api/credentials/pool/{provider}/{index}")
async def remove_credential_pool_entry(provider: str, index: int):
    """Remove a pool entry.  ``index`` is 1-based (matches the list response).

    Removal must be sticky (#55217): ``load_pool()`` re-seeds entries from
    their backing source (.env var, OAuth singleton file, custom-provider
    config) on every call, so deleting only the pool row silently reverts on
    the next dashboard refresh.  We dispatch through the same RemovalStep
    registry the CLI ``hermes auth remove`` uses: each source cleans up its
    external state and suppresses ``(provider, source)`` so the seeders skip
    it.  Manual entries have no registered step — nothing external to clean,
    no suppression needed (they aren't re-seeded).
    """
    from agent.credential_pool import load_pool
    from agent.credential_sources import find_removal_step
    from hermes_cli.auth import suppress_credential_source

    provider = (provider or "").strip().lower()
    try:
        pool = load_pool(provider)
        removed = pool.remove_index(index)
    except Exception as exc:
        _log.exception("DELETE /api/credentials/pool failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if removed is None:
        raise HTTPException(status_code=404, detail="No pool entry at that index")

    cleaned: List[str] = []
    hints: List[str] = []
    step = find_removal_step(provider, removed.source or "")
    if step is not None:
        try:
            result = step.remove_fn(provider, removed)
            cleaned = list(result.cleaned)
            hints = list(result.hints)
            if result.suppress:
                suppress_credential_source(provider, removed.source)
        except Exception:
            # Cleanup is best-effort, but suppression is the actual bug fix —
            # without it the entry resurrects on the next load_pool().  Apply
            # it even when source-specific cleanup blew up.
            _log.exception(
                "credential source cleanup failed for %s/%s; suppressing anyway",
                provider, removed.source,
            )
            try:
                suppress_credential_source(provider, removed.source)
            except Exception:
                _log.exception("suppress_credential_source failed")
    return {
        "ok": True,
        "provider": provider,
        "count": len(pool.entries()),
        "cleaned": cleaned,
        "hints": hints,
    }


# ---------------------------------------------------------------------------
# Memory provider endpoints — status / list providers / select / disable / reset.
#
# Provider setup is dashboard-native when a provider exposes get_config_schema().
# The dashboard never runs interactive provider setup hooks; activation is only
# allowed once the provider is discoverable, available, and has required config.
# ---------------------------------------------------------------------------


@app.get("/api/memory")
async def get_memory_status():
    # load_config(), file stats and provider discovery are disk reads — keep
    # them off the event loop.
    def _run():
        cfg = load_config()
        active = ""
        mem = cfg.get("memory")
        if isinstance(mem, dict):
            active = _normalize_memory_provider_name(mem.get("provider"))

        # Built-in memory file sizes (so the UI can show what a reset would erase).
        mem_dir = get_hermes_home() / "memories"
        files = {}
        for fname, key in (("MEMORY.md", "memory"), ("USER.md", "user")):
            path = mem_dir / fname
            files[key] = path.stat().st_size if path.exists() else 0

        return {
            "active": active,
            "providers": _discover_memory_provider_statuses(),
            "builtin_files": files,
        }

    return await asyncio.to_thread(_run)


@app.put("/api/memory/provider")
async def set_memory_provider(body: MemoryProviderSelect):
    provider = _normalize_memory_provider_name(body.provider)

    def _run():
        _require_memory_provider_ready(provider)

        with _CONFIG_MUTATION_LOCK:
            cfg = load_config()
            if not isinstance(cfg.get("memory"), dict):
                cfg["memory"] = {}
            cfg["memory"]["provider"] = provider
            save_config(cfg)
        return {"ok": True, "active": provider}

    return await asyncio.to_thread(_run)


@app.post("/api/memory/reset")
async def reset_memory(body: MemoryReset):
    target = (body.target or "all").strip().lower()
    if target not in {"all", "memory", "user"}:
        raise HTTPException(status_code=400, detail="target must be all, memory, or user")

    mem_dir = get_hermes_home() / "memories"
    deleted = []
    targets = []
    if target in {"all", "memory"}:
        targets.append("MEMORY.md")
    if target in {"all", "user"}:
        targets.append("USER.md")
    for fname in targets:
        path = mem_dir / fname
        if path.exists():
            try:
                path.unlink()
                deleted.append(fname)
            except OSError as exc:
                raise HTTPException(status_code=500, detail=f"Could not delete {fname}: {exc}")
    return {"ok": True, "deleted": deleted}


# ---------------------------------------------------------------------------
# Operations endpoints — doctor / security audit / backup / import /
# checkpoints / hooks.
#
# Diagnostic and maintenance commands.  The long-running / text-output ones
# (doctor, security audit, backup, import, skills install) are spawned as
# background actions whose logs the dashboard tails via
# /api/actions/{name}/status — same pattern as gateway restart and update.
# The cheap, structured reads (hooks list, checkpoints list) return JSON
# directly.
# ---------------------------------------------------------------------------


@app.post("/api/ops/doctor")
async def run_doctor():
    try:
        proc = _spawn_hermes_action(["doctor"], "doctor")
    except Exception as exc:
        _log.exception("Failed to spawn doctor")
        raise HTTPException(status_code=500, detail=f"Failed to run doctor: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "doctor"}


@app.post("/api/ops/security-audit")
async def run_security_audit():
    try:
        proc = _spawn_hermes_action(["security", "audit"], "security-audit")
    except Exception as exc:
        _log.exception("Failed to spawn security audit")
        raise HTTPException(status_code=500, detail=f"Failed to run security audit: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "security-audit"}


def _dashboard_backup_dir() -> Path:
    return get_hermes_home() / "backups"


def _new_dashboard_backup_path() -> Path:
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    return _dashboard_backup_dir() / f"hermes-backup-{stamp}-{secrets.token_hex(4)}.zip"


@app.post("/api/ops/backup")
async def run_backup(body: BackupRequest):
    args = ["backup"]
    archive: Optional[Path] = None
    output = (body.output or "").strip()
    if output:
        args.extend(["-o", output])
    else:
        archive = _new_dashboard_backup_path()
        try:
            archive.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Could not create backup directory: {exc}",
            )
        args.extend(["-o", str(archive)])
    try:
        proc = _spawn_hermes_action(args, "backup")
    except Exception as exc:
        _log.exception("Failed to spawn backup")
        raise HTTPException(status_code=500, detail=f"Failed to run backup: {exc}")
    response = {"ok": True, "pid": proc.pid, "name": "backup"}
    if archive is not None:
        response["archive"] = str(archive)
    return response


@app.get("/api/ops/backup/download")
async def download_dashboard_backup(archive: str):
    try:
        backup_dir = _dashboard_backup_dir().expanduser().resolve(strict=False)
        target = Path(archive).expanduser().resolve(strict=True)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Backup not found")
    except (OSError, RuntimeError):
        raise HTTPException(status_code=400, detail="Invalid backup path")

    if not _path_is_under(backup_dir, target):
        raise HTTPException(status_code=403, detail="Backup is outside the dashboard backup directory")
    if not target.is_file():
        raise HTTPException(status_code=404, detail="Backup not found")

    return FileResponse(
        path=str(target),
        media_type="application/zip",
        filename=target.name,
        content_disposition_type="attachment",
    )


@app.post("/api/ops/import")
async def run_import(body: ImportRequest):
    archive = (body.archive or "").strip()
    if not archive:
        raise HTTPException(status_code=400, detail="archive path is required")
    if not os.path.isfile(archive):
        raise HTTPException(status_code=404, detail=f"Archive not found: {archive}")
    args = ["import", archive]
    if body.force:
        args.append("--force")
    try:
        proc = _spawn_hermes_action(args, "import")
    except Exception as exc:
        _log.exception("Failed to spawn import")
        raise HTTPException(status_code=500, detail=f"Failed to run import: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "import"}


def _safe_backup_upload_name(filename: str | None) -> str:
    name = Path(filename or "backup.zip").name.strip()
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip(".-")
    if not name:
        name = "backup.zip"
    if not name.lower().endswith(".zip"):
        name = f"{name}.zip"
    return name


@app.post("/api/ops/import-upload")
async def run_import_upload(
    file: UploadFile = File(...),
    force: bool = Form(False),
):
    staging_dir = _dashboard_backup_dir()
    try:
        staging_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not create import staging directory: {exc}",
        )

    safe_name = _safe_backup_upload_name(file.filename)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    target = staging_dir / f"dashboard-import-{stamp}-{secrets.token_hex(4)}-{safe_name}"
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".upload",
        dir=str(staging_dir),
    )
    tmp_path = Path(tmp_name)
    total = 0
    renamed = False
    try:
        with os.fdopen(tmp_fd, "wb") as out:
            while True:
                chunk = await file.read(_UPLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MANAGED_FILE_MAX_BYTES:
                    raise HTTPException(status_code=413, detail="Archive is too large")
                out.write(chunk)
        os.replace(tmp_path, target)
        renamed = True
    except HTTPException:
        raise
    except PermissionError:
        raise HTTPException(
            status_code=403,
            detail="Import staging directory is not writable",
        )
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not write uploaded archive: {exc}",
        )
    finally:
        if not renamed:
            tmp_path.unlink(missing_ok=True)
        await file.close()

    if not zipfile.is_zipfile(target):
        target.unlink(missing_ok=True)
        raise HTTPException(
            status_code=400,
            detail="Uploaded archive is not a valid zip file",
        )

    args = ["import", str(target)]
    if force:
        args.append("--force")
    try:
        proc = _spawn_hermes_action(args, "import")
    except Exception as exc:
        _log.exception("Failed to spawn import")
        raise HTTPException(status_code=500, detail=f"Failed to run import: {exc}")
    return {
        "ok": True,
        "pid": proc.pid,
        "name": "import",
        "archive": str(target),
        "uploaded_bytes": total,
    }


@app.get("/api/ops/hooks")
async def list_hooks():
    """List configured shell hooks from config.yaml with consent + health.

    Reports each hook's allowlist (consent) status and whether the script is
    currently executable, plus the set of valid hook events so the create
    form can offer them.
    """
    def _run():
        from hermes_cli.config import load_config as _load_config
        from agent import shell_hooks

        try:
            from hermes_cli.plugins import VALID_HOOKS
            valid_events = sorted(VALID_HOOKS)
        except Exception:
            valid_events = []

        specs = []
        try:
            specs = shell_hooks.iter_configured_hooks(_load_config())
        except Exception:
            _log.exception("iter_configured_hooks failed")

        out = []
        for spec in specs:
            entry = None
            try:
                entry = shell_hooks.allowlist_entry_for(spec.event, spec.command)
            except Exception:
                pass
            executable = False
            try:
                executable = shell_hooks.script_is_executable(spec.command)
            except Exception:
                pass
            out.append({
                "event": spec.event,
                "matcher": spec.matcher,
                "command": spec.command,
                "timeout": spec.timeout,
                "allowed": entry is not None,
                "approved_at": (entry or {}).get("approved_at"),
                "executable": executable,
            })

        return {"hooks": out, "valid_events": valid_events}

    return await asyncio.to_thread(_run)


@app.post("/api/ops/hooks")
async def create_hook(body: HookCreate):
    """Add a shell hook to config.yaml (and optionally approve it).

    Shell hooks run arbitrary commands, so this is a privileged action: it
    writes to the ``hooks:`` config block and, when ``approve`` is set, records
    consent in the allowlist so the hook actually fires.  Takes effect on the
    next session / gateway restart.
    """
    from agent import shell_hooks

    event = (body.event or "").strip()
    command = (body.command or "").strip()
    if not event or not command:
        raise HTTPException(status_code=400, detail="event and command are required")

    try:
        from hermes_cli.plugins import VALID_HOOKS
        if event not in VALID_HOOKS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown event '{event}'. Valid: {', '.join(sorted(VALID_HOOKS))}",
            )
    except HTTPException:
        raise
    except Exception:
        pass

    def _run():
        with _CONFIG_MUTATION_LOCK:
            cfg = load_config()
            hooks_cfg = cfg.get("hooks")
            if not isinstance(hooks_cfg, dict):
                hooks_cfg = {}
                cfg["hooks"] = hooks_cfg
            entries = hooks_cfg.get(event)
            if not isinstance(entries, list):
                entries = []
                hooks_cfg[event] = entries

            new_entry: Dict[str, Any] = {"command": command}
            if body.matcher:
                new_entry["matcher"] = body.matcher
            if body.timeout is not None:
                new_entry["timeout"] = int(body.timeout)
            entries.append(new_entry)
            save_config(cfg)

        approved = False
        if body.approve:
            try:
                shell_hooks._record_approval(event, command)
                approved = True
            except Exception:
                _log.exception("hook consent record failed")

        return {"ok": True, "event": event, "command": command, "approved": approved}

    return await asyncio.to_thread(_run)


@app.delete("/api/ops/hooks")
async def delete_hook(body: HookDelete):
    """Remove a hook from config.yaml and revoke its consent allowlist entry."""
    from agent import shell_hooks

    event = (body.event or "").strip()
    command = (body.command or "").strip()
    if not event or not command:
        raise HTTPException(status_code=400, detail="event and command are required")

    def _run():
        removed = False
        with _CONFIG_MUTATION_LOCK:
            cfg = load_config()
            hooks_cfg = cfg.get("hooks")
            if isinstance(hooks_cfg, dict) and isinstance(hooks_cfg.get(event), list):
                before = len(hooks_cfg[event])
                hooks_cfg[event] = [
                    e for e in hooks_cfg[event]
                    if not (isinstance(e, dict) and e.get("command") == command)
                ]
                removed = len(hooks_cfg[event]) < before
                if not hooks_cfg[event]:
                    del hooks_cfg[event]
                if not hooks_cfg:
                    cfg.pop("hooks", None)
                save_config(cfg)

        # Revoke consent regardless so a re-add re-prompts.
        try:
            shell_hooks.revoke(command)
        except Exception:
            pass
        return removed

    removed = await asyncio.to_thread(_run)

    if not removed:
        raise HTTPException(status_code=404, detail="No matching hook found")
    return {"ok": True}


@app.get("/api/ops/checkpoints")
async def list_checkpoints():
    """List the /rollback shadow store checkpoints (read-only)."""
    # Checkpoints live under <hermes_home>/checkpoints/.  Surface a count +
    # total size so the dashboard can show what a prune would reclaim; the
    # actual prune is a spawned action so confirmation/pruning logic stays
    # in one place (the CLI).
    cp_dir = get_hermes_home() / "checkpoints"
    sessions = []
    total_bytes = 0
    if cp_dir.is_dir():
        with os.scandir(cp_dir) as scan:
            children = sorted((Path(e.path) for e in scan), key=lambda p: p.name)
        for child in children:
            if not child.is_dir():
                continue
            size = 0
            count = 0
            for f in child.rglob("*"):
                if f.is_file():
                    try:
                        size += f.stat().st_size
                        count += 1
                    except OSError:
                        pass
            total_bytes += size
            sessions.append({
                "session": child.name,
                "files": count,
                "bytes": size,
            })
    return {"sessions": sessions, "total_bytes": total_bytes}


@app.post("/api/ops/checkpoints/prune")
async def prune_checkpoints():
    try:
        proc = _spawn_hermes_action(["checkpoints", "prune"], "checkpoints-prune")
    except Exception as exc:
        _log.exception("Failed to spawn checkpoints prune")
        raise HTTPException(status_code=500, detail=f"Failed to prune checkpoints: {exc}")
    return {"ok": True, "pid": proc.pid, "name": "checkpoints-prune"}


# ---------------------------------------------------------------------------
# Skills hub endpoints — search / install / uninstall / update.
#
# Search and install touch the network (GitHub, hub sources) and run the same
# complex source-router pipeline the CLI uses, so they're spawned as background
# actions whose logs the dashboard tails.  The already-installed skill list +
# enable/disable toggle live in the existing /api/skills endpoints.
# ---------------------------------------------------------------------------


def _profile_cli_args(profile: Optional[str]) -> List[str]:
    """Return ``["-p", <name>]`` for a validated non-default profile.

    Hub install/uninstall/update run in a fresh ``hermes`` subprocess, and
    ``_apply_profile_override()`` reads ``-p`` from argv in the child — the
    only mechanism that reaches import-time-bound globals like
    ``skills_hub.SKILLS_DIR``. Empty/"current" means the dashboard's own
    profile (no args, legacy behavior).
    """
    requested = (profile or "").strip()
    if not requested or requested.lower() in {"current", "default"}:
        return []
    from hermes_cli import profiles as profiles_mod
    _resolve_profile_dir(requested)
    return ["-p", profiles_mod.normalize_profile_name(requested)]


def _hub_action_name(verb: str, key: str) -> str:
    """Unique per-skill hub action name (+ registered log file).

    ``_spawn_hermes_action`` tracks one process/log per name, so a shared
    "skills-install"/"skills-uninstall" would make concurrent row-level actions
    overwrite each other's status/log while the UI polls per identifier. Slug
    (readable) + hash (collision-proof) keys each action to its own row.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", key.lower()).strip("-")[:48] or "skill"
    digest = hashlib.sha1(key.encode()).hexdigest()[:8]
    name = f"skills-{verb}-{slug}-{digest}"
    _ACTION_LOG_FILES.setdefault(name, f"action-{name}.log")
    return name


from hermes_cli.web_routers import skills as _skills_routes  # noqa: E402

app.include_router(_skills_routes.hub_router)
from hermes_cli.web_routers.skills import (  # noqa: E402,F401 — legacy re-exports; tests call these via web_server.<name>
    install_skill_hub,
    uninstall_skill_hub,
    update_skills_hub,
    list_skills_hub_sources,
    search_skills_hub,
    preview_skill_hub,
    scan_skill_hub,
)






# Human-readable labels for each hub source id (matches `hermes skills search`
# provenance).  Keep in sync with create_source_router()'s source list.
_SKILL_HUB_SOURCE_LABELS = {
    "official": "Official (Nous)",
    "hermes-index": "Hermes Index",
    "skills-sh": "skills.sh",
    "well-known": "Well-Known",
    "url": "Direct URL",
    "github": "GitHub",
    "clawhub": "ClawHub",
    "lobehub": "LobeHub",
    "browse-sh": "browse.sh",
}


def _skill_meta_to_payload(m) -> dict:
    return {
        "name": m.name,
        "description": m.description,
        "source": m.source,
        "identifier": m.identifier,
        "trust_level": m.trust_level,
        "repo": m.repo,
        "tags": list(m.tags or []),
    }


def _installed_hub_identifiers(profile: Optional[str] = None) -> dict:
    """Map identifier -> installed lock entry for hub-installed skills.

    Lets the UI mark search results that are already installed.  Scoped to
    ``profile``'s skills/.hub/lock.json when provided (HubLockFile takes an
    explicit path, sidestepping the import-time LOCK_FILE binding).
    Best-effort: returns an empty dict if the lock file can't be read.
    """
    try:
        from tools.skills_hub import HubLockFile

        requested = (profile or "").strip()
        if requested and requested.lower() != "current":
            profile_dir = _resolve_profile_dir(requested)
            lock = HubLockFile(profile_dir / "skills" / ".hub" / "lock.json")
        else:
            lock = HubLockFile()
        out = {}
        for entry in lock.list_installed():
            ident = entry.get("identifier")
            if ident:
                out[ident] = {
                    "name": entry.get("name"),
                    "trust_level": entry.get("trust_level"),
                    "scan_verdict": entry.get("scan_verdict"),
                }
        return out
    except Exception:
        return {}










# ---------------------------------------------------------------------------
# Profile management endpoints (minimal — list/create/rename/delete + SOUL.md)
# ---------------------------------------------------------------------------


def _profile_attr(info, name: str, default: Any = None) -> Any:
    try:
        return getattr(info, name)
    except Exception:
        return default


def _profile_to_dict(info) -> Dict[str, Any]:
    return {
        "name": _profile_attr(info, "name", ""),
        "path": str(_profile_attr(info, "path", "")),
        "is_default": bool(_profile_attr(info, "is_default", False)),
        "model": _profile_attr(info, "model"),
        "provider": _profile_attr(info, "provider"),
        "has_env": bool(_profile_attr(info, "has_env", False)),
        "skill_count": int(_profile_attr(info, "skill_count", 0) or 0),
        "gateway_running": bool(_profile_attr(info, "gateway_running", False)),
        "description": _profile_attr(info, "description", "") or "",
        "description_auto": bool(_profile_attr(info, "description_auto", False)),
        "display_name": _profile_attr(info, "display_name", "") or "",
        "distribution_name": _profile_attr(info, "distribution_name"),
        "distribution_version": _profile_attr(info, "distribution_version"),
        "distribution_source": _profile_attr(info, "distribution_source"),
        "has_alias": _profile_attr(info, "alias_path") is not None,
    }


def _fallback_profile_dicts(profiles_mod) -> List[Dict[str, Any]]:
    def _safe(callable_, default):
        try:
            return callable_()
        except Exception:
            return default

    profiles: List[Dict[str, Any]] = []
    default_home = profiles_mod._get_default_hermes_home()
    if default_home.is_dir():
        model, provider = _safe(lambda: profiles_mod._read_config_model(default_home), (None, None))
        profiles.append({
            "name": "default",
            "path": str(default_home),
            "is_default": True,
            "model": model,
            "provider": provider,
            "has_env": (default_home / ".env").exists(),
            "skill_count": _safe(lambda: profiles_mod._count_skills(default_home), 0),
            "gateway_running": _safe(lambda: profiles_mod._check_gateway_running(default_home), False),
            "description": _safe(lambda: profiles_mod.read_profile_meta(default_home).get("description", ""), ""),
            "description_auto": _safe(lambda: profiles_mod.read_profile_meta(default_home).get("description_auto", False), False),
            "distribution_name": None,
            "distribution_version": None,
            "distribution_source": None,
            "has_alias": False,
        })

    profiles_root = profiles_mod._get_profiles_root()
    if profiles_root.is_dir():
        # Use os.scandir (context-managed) instead of Path.iterdir to avoid
        # leaking directory fds when an exception interrupts iteration — the
        # sidebar polls every few seconds so an fd leak exhausts RLIMIT_NOFILE
        # within days (#81547).
        with os.scandir(profiles_root) as scan:
            entries = sorted(scan, key=lambda e: e.name)
        for entry in entries:
            entry_path = Path(entry.path)
            if not entry.is_dir() or not profiles_mod._PROFILE_ID_RE.match(entry.name):
                continue
            model, provider = _safe(lambda entry=entry_path: profiles_mod._read_config_model(entry), (None, None))
            profiles.append({
                "name": entry.name,
                "path": str(entry_path),
                "is_default": False,
                "model": model,
                "provider": provider,
                "has_env": _safe(lambda entry=entry_path: (entry / ".env").exists(), False),
                "skill_count": _safe(lambda entry=entry_path: profiles_mod._count_skills(entry), 0),
                "gateway_running": _safe(lambda entry=entry_path: profiles_mod._check_gateway_running(entry), False),
                "description": _safe(lambda entry=entry_path: profiles_mod.read_profile_meta(entry).get("description", ""), ""),
                "description_auto": _safe(lambda entry=entry_path: profiles_mod.read_profile_meta(entry).get("description_auto", False), False),
                "distribution_name": None,
                "distribution_version": None,
                "distribution_source": None,
                "has_alias": False,
            })

    return profiles


def _resolve_profile_dir(name: str) -> Path:
    """Validate ``name`` and resolve to its directory or raise an HTTPException."""
    from hermes_cli import profiles as profiles_mod
    try:
        profiles_mod.validate_profile_name(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not profiles_mod.profile_exists(name):
        raise HTTPException(status_code=404, detail=f"Profile '{name}' does not exist.")
    return profiles_mod.get_profile_dir(name)


def _profile_setup_command(name: str) -> str:
    """Return the shell command used to configure a profile in the CLI."""
    _resolve_profile_dir(name)
    return "hermes setup" if name == "default" else f"{name} setup"


def _write_profile_model(profile_dir: Path, provider: str, model: str) -> None:
    """Write the main model assignment into a specific profile's config.yaml.

    Scopes ``load_config``/``save_config`` to ``profile_dir`` via the
    context-local HERMES_HOME override so the write lands in the target
    profile's config rather than the dashboard process's active profile.
    Clears any stale ``base_url`` / ``context_length`` the same way
    ``POST /api/model/set`` does, since the new model may differ.
    """
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override

    token = set_hermes_home_override(str(profile_dir))
    try:
        provider, model = _normalize_main_model_assignment(provider, model)
        cfg = load_config()
        cfg["model"] = _apply_main_model_assignment(cfg.get("model", {}), provider, model)
        save_config(cfg)
    finally:
        reset_hermes_home_override(token)


def _write_profile_mcp_servers(profile_dir: Path, servers: List["MCPServerCreate"]) -> int:
    """Write MCP server entries into a specific profile's config.yaml.

    Scopes ``load_config``/``save_config`` to ``profile_dir`` via the
    context-local HERMES_HOME override (same mechanism as
    ``_write_profile_model``) so the entries land in the target profile's
    config rather than the dashboard process's active profile.

    Mirrors the per-server shape the ``POST /api/mcp/servers`` endpoint builds,
    but batched so the whole profile-create write is a single config save.
    Returns the number of servers written.
    """
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from hermes_cli.mcp_config import _save_bearer_auth_token

    written = 0
    token = set_hermes_home_override(str(profile_dir))
    try:
        cfg = load_config()
        mcp = cfg.setdefault("mcp_servers", {})
        for server in servers:
            try:
                name, entry, bearer_token = _normalize_mcp_server_create(server)
            except ValueError as exc:
                display_name = (server.name or "").strip() or "<unnamed>"
                _log.warning(
                    "Profile-create: skipping MCP server '%s': %s",
                    display_name,
                    exc,
                )
                continue
            if bearer_token is not None:
                entry["headers"] = _save_bearer_auth_token(name, bearer_token)
            mcp[name] = entry
            written += 1
        if written:
            save_config(cfg)
        elif not mcp:
            # We created an empty mcp_servers dict but wrote nothing — don't
            # leave a stray empty key in the new profile's config.
            cfg.pop("mcp_servers", None)
            save_config(cfg)
    finally:
        reset_hermes_home_override(token)
    return written


def _disable_unselected_skills(profile_dir: Path, keep: List[str]) -> int:
    """Disable every installed skill in ``profile_dir`` not in ``keep``.

    Profiles manage skill activation via a *disabled* list — all installed
    skills are active by default and users opt out. The builder's skill step
    uses "replace" semantics: the user picks exactly which seeded built-in /
    optional skills stay active, and everything else gets added to the disabled
    list. (Hub skills are installed separately via subprocess and are active on
    install.) Scoped to the profile via the HERMES_HOME override. Returns the
    number of skills newly disabled.
    """
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from hermes_cli.skills_config import get_disabled_skills, save_disabled_skills

    keep_set = {s.strip() for s in keep if s and s.strip()}
    disabled_count = 0
    token = set_hermes_home_override(str(profile_dir))
    try:
        installed: List[str] = []
        skills_root = profile_dir / "skills"
        if skills_root.is_dir():
            for md in skills_root.rglob("SKILL.md"):
                installed.append(md.parent.name)
        cfg = load_config()
        disabled = get_disabled_skills(cfg)
        for name in installed:
            if name not in keep_set and name not in disabled:
                disabled.add(name)
                disabled_count += 1
        if disabled_count:
            save_disabled_skills(cfg, disabled)
    finally:
        reset_hermes_home_override(token)
    return disabled_count


app.include_router(_profiles_routes.router)
from hermes_cli.web_routers.profiles import (  # noqa: E402,F401 — legacy re-exports; tests call these via web_server.<name>
    list_profiles_endpoint,
    create_profile_endpoint,
    get_active_profile_endpoint,
    set_active_profile_endpoint,
    get_profile_setup_command,
    open_profile_terminal_endpoint,
    rename_profile_endpoint,
    delete_profile_endpoint,
    get_profile_soul,
    update_profile_soul,
    update_profile_description_endpoint,
    update_profile_model_endpoint,
    describe_profile_auto_endpoint,
)


























# ---------------------------------------------------------------------------
# Skills & Tools endpoints
#
# Every read/write below accepts an optional ``profile`` query param so the
# dashboard can manage ANY profile's skills/toolsets, not just the profile
# the dashboard process happens to be running under. Without this, "Set as
# active" on the Profiles page (which only flips the sticky ``active_profile``
# file for FUTURE CLI/gateway invocations) misled users into thinking skill
# toggles would land in the activated profile — they silently wrote into the
# dashboard's own config instead. See _profile_scope() for the mechanism.
# ---------------------------------------------------------------------------


_SKILLS_PROFILE_LOCK = threading.RLock()


@contextmanager
def _profile_scope(profile: Optional[str]):
    """Scope config + skill-directory resolution to ``profile`` for one request.

    Two seams must be redirected for skills/toolsets endpoints:

    1. ``load_config``/``save_config`` resolve ``get_hermes_home()`` at call
       time — the context-local override from ``set_hermes_home_override``
       reaches them (same pattern as ``_write_profile_model``).
    2. ``tools.skills_tool`` and ``tools.skill_manager_tool`` bind
       ``SKILLS_DIR`` at import time, so the override CANNOT reach them.
       Like ``_call_cron_for_profile`` does for cron's module globals,
       temporarily retarget both under a lock and restore them
       immediately after.

    ``tools.skills_sync`` (reset/diff/list-modified/opt-in/opt-out/
    repair-official) needs NO retargeting: since #65828 its directory
    lookups resolve at call time through the same contextvar override
    set in step 1.

    ``profile`` of None/""/"current" means "the dashboard's own profile" —
    config resolution is untouched, but the skill-module globals are still
    retargeted to the *current* ``get_hermes_home()`` so writes land in the
    live home even when the import-time binding is stale (e.g. the process
    imported the modules before a HERMES_HOME override, or under test
    isolation).
    """
    requested = (profile or "").strip()

    from hermes_constants import (
        get_hermes_home,
        set_hermes_home_override,
        reset_hermes_home_override,
    )
    from tools import skills_tool as _skills_tool
    from tools import skill_manager_tool as _skill_mgr

    token = None
    if not requested or requested.lower() == "current":
        profile_dir = get_hermes_home()
    else:
        profile_dir = _resolve_profile_dir(requested)
        token = set_hermes_home_override(str(profile_dir))

    with _SKILLS_PROFILE_LOCK:
        old_home = _skills_tool.HERMES_HOME
        old_skills_dir = _skills_tool.SKILLS_DIR
        old_mgr_home = _skill_mgr.HERMES_HOME
        old_mgr_skills_dir = _skill_mgr.SKILLS_DIR
        _skills_tool.HERMES_HOME = profile_dir
        _skills_tool.SKILLS_DIR = profile_dir / "skills"
        _skill_mgr.HERMES_HOME = profile_dir
        _skill_mgr.SKILLS_DIR = profile_dir / "skills"
        try:
            yield profile_dir if token is not None else None
        finally:
            _skills_tool.HERMES_HOME = old_home
            _skills_tool.SKILLS_DIR = old_skills_dir
            _skill_mgr.HERMES_HOME = old_mgr_home
            _skill_mgr.SKILLS_DIR = old_mgr_skills_dir
            if token is not None:
                reset_hermes_home_override(token)


@contextmanager
def _config_profile_scope(profile: Optional[str]):
    """Await-safe, config-only profile scope for handlers that ``await``.

    Unlike ``_profile_scope`` this touches ONLY the context-local
    ``set_hermes_home_override`` contextvar — it does NOT swap the
    process-global ``skills_tool``/``skill_manager`` module attributes.
    Those globals are shared across all event-loop tasks, so holding them
    across an ``await`` lets a concurrent skills request restore THIS
    request's profile dir on its ``finally`` (cross-contamination). The
    contextvar override is task-local and survives an ``await`` cleanly,
    which is all endpoints that resolve ``get_hermes_home()`` at call time
    (config, env, gateway status) actually need.

    None/""/"current" means the dashboard's own profile — no override.
    """
    requested = (profile or "").strip()
    if not requested or requested.lower() == "current":
        yield None
        return

    from hermes_constants import (
        set_hermes_home_override,
        reset_hermes_home_override,
    )

    profile_dir = _resolve_profile_dir(requested)
    token = set_hermes_home_override(str(profile_dir))
    try:
        yield profile_dir
    finally:
        reset_hermes_home_override(token)


app.include_router(_skills_routes.router)
from hermes_cli.web_routers.skills import (  # noqa: E402,F401 — legacy re-exports; tests call these via web_server.<name>
    get_skills,
    toggle_skill,
    get_skill_content,
    create_skill,
    update_skill_content,
)




def _clear_skills_prompt_cache() -> None:
    """Best-effort: invalidate the skills system-prompt snapshot after a write.

    Mirrors what ``skill_manage`` does so a dashboard-authored skill is picked
    up by the next session without a manual cache reset.
    """
    try:
        from agent.prompt_builder import clear_skills_system_prompt_cache
        clear_skills_system_prompt_cache(clear_snapshot=True)
    except Exception:
        pass








from hermes_cli.web_routers import tools as _tools_routes  # noqa: E402

app.include_router(_tools_routes.router)
from hermes_cli.web_routers.tools import (  # noqa: E402,F401 — legacy re-exports; tests call these via web_server.<name>
    get_toolsets,
    toggle_toolset,
    get_toolset_config,
    get_toolset_models,
    select_toolset_model,
    select_toolset_provider,
    save_toolset_env,
    run_toolset_post_setup,
    get_terminal_backends,
    select_terminal_backend,
    get_computer_use_status,
    grant_computer_use_permissions,
)






# Toolsets whose backends carry a selectable model catalog, mapped to the
# config.yaml section their `model` key lives in. Mirrors the CLI's
# post-selection model pickers (`_configure_imagegen_model_for_plugin` /
# `_configure_videogen_model_for_plugin` in tools_config.py).
_MODEL_CATALOG_TOOLSETS = {
    "image_gen": "image_gen",
    "video_gen": "video_gen",
}


def _resolve_toolset_model_plugin(ts_key: str, provider_row: dict) -> Optional[str]:
    """Map a provider picker row to its model-catalog plugin name.

    Plugin-backed rows carry ``image_gen_plugin_name`` / ``video_gen_plugin_name``;
    the managed "Nous Subscription" image row instead carries the legacy
    ``imagegen_backend: "fal"`` marker (same underlying FAL catalog).
    """
    if ts_key == "image_gen":
        return provider_row.get("image_gen_plugin_name") or (
            "fal" if provider_row.get("imagegen_backend") else None
        )
    if ts_key == "video_gen":
        return provider_row.get("video_gen_plugin_name")
    return None


def _toolset_model_catalog(ts_key: str, plugin_name: str):
    """Return ``(catalog_dict, default_model)`` for a toolset's plugin backend."""
    from hermes_cli.tools_config import (
        _plugin_image_gen_catalog,
        _plugin_video_gen_catalog,
    )

    if ts_key == "image_gen":
        return _plugin_image_gen_catalog(plugin_name)
    return _plugin_video_gen_catalog(plugin_name)


def _find_toolset_provider_row(ts_key: str, config: dict, provider: Optional[str]) -> Optional[dict]:
    """Resolve a provider picker row by name, or the active row when omitted."""
    from hermes_cli.tools_config import (
        TOOL_CATEGORIES,
        _is_provider_active,
        _visible_providers,
    )

    cat = TOOL_CATEGORIES.get(ts_key)
    if cat is None:
        return None
    rows = _visible_providers(cat, config, force_fresh=True)
    if provider:
        return next((p for p in rows if p.get("name") == provider), None)
    return next(
        (p for p in rows if _is_provider_active(p, config, force_fresh=True)), None
    )












# ---------------------------------------------------------------------------
# Terminal execution backend picker — the GUI counterpart of terminal.backend
# in config.yaml. Each row carries a fast, defensive health probe (Docker
# daemon reachable, SSH host configured, Modal/Daytona credentials present) so
# the Capabilities panel can render Ready / Needs setup guidance instead of a
# bare enum (issues #57738 / #63783). Probes must never raise — a probe
# failure renders as a status, not a 500.
# ---------------------------------------------------------------------------

# Table-driven backend metadata — kept in sync with the dispatch ladder in
# tools/terminal_tool.py::_create_environment and the terminal.backend enum
# surfaced in the desktop raw-config settings.
_TERMINAL_BACKENDS: List[Dict[str, str]] = [
    {
        "name": "local",
        "label": "Local",
        "description": "Run commands directly on this machine. No isolation.",
    },
    {
        "name": "docker",
        "label": "Docker",
        "description": "Run commands in an isolated Docker container with a persistent workspace.",
    },
    {
        "name": "singularity",
        "label": "Singularity / Apptainer",
        "description": "Run commands in a Singularity/Apptainer container (HPC-friendly, rootless).",
    },
    {
        "name": "modal",
        "label": "Modal",
        "description": "Run commands in a Modal cloud sandbox.",
    },
    {
        "name": "daytona",
        "label": "Daytona",
        "description": "Run commands in a Daytona cloud sandbox.",
    },
    {
        "name": "ssh",
        "label": "SSH",
        "description": "Run commands on a remote host over SSH.",
    },
]

_TERMINAL_BACKEND_NAMES = {row["name"] for row in _TERMINAL_BACKENDS}


def _plugin_terminal_backend_rows() -> List[Dict[str, str]]:
    """Picker rows for plugin-registered terminal backends (fail-soft)."""
    rows: List[Dict[str, str]] = []
    try:
        from hermes_cli.plugins import discover_plugins

        discover_plugins()  # idempotent — plugin state may not be loaded yet
    except Exception:
        pass
    try:
        from agent.terminal_env_registry import list_providers

        for provider in list_providers():
            try:
                rows.append({
                    "name": provider.name.strip().lower(),
                    "label": provider.display_name,
                    "description": provider.description,
                })
            except Exception:
                continue
    except Exception:
        return rows
    return rows


def _terminal_backend_rows() -> List[Dict[str, str]]:
    """Built-in picker rows plus plugin-registered backends (request time).

    Computed per request (mirrors ``_schema_with_dynamic_provider_options``)
    so a plugin installed after server start still shows up.
    """
    return [*_TERMINAL_BACKENDS, *_plugin_terminal_backend_rows()]


def _terminal_backend_names() -> set:
    """Valid ``terminal.backend`` values, including plugin backends."""
    return {row["name"] for row in _terminal_backend_rows()}


def _terminal_cfg_value(terminal_cfg: dict, key: str, env_var: str) -> str:
    """Read a terminal.* setting from config.yaml, falling back to its env var."""
    value = terminal_cfg.get(key)
    if value is not None and str(value).strip():
        return str(value).strip()
    try:
        from hermes_cli.config import get_env_value

        return (get_env_value(env_var) or "").strip()
    except Exception:
        return ""


def _probe_docker_backend() -> tuple:
    if not shutil.which("docker"):
        return (
            "needs_setup",
            "Docker CLI not found — install Docker Desktop or docker-ce.",
        )
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=2,
        )
        if proc.returncode == 0:
            return ("ready", "")
        return (
            "needs_setup",
            "Docker daemon not reachable — start Docker and retry.",
        )
    except subprocess.TimeoutExpired:
        return ("needs_setup", "Docker daemon not responding (timed out).")
    except Exception as exc:
        return ("unavailable", f"Docker probe failed: {exc}")


def _probe_singularity_backend() -> tuple:
    if shutil.which("singularity") or shutil.which("apptainer"):
        return ("ready", "")
    return (
        "needs_setup",
        "Neither singularity nor apptainer found on PATH.",
    )


def _probe_ssh_backend(terminal_cfg: dict) -> tuple:
    host = _terminal_cfg_value(terminal_cfg, "ssh_host", "TERMINAL_SSH_HOST")
    user = _terminal_cfg_value(terminal_cfg, "ssh_user", "TERMINAL_SSH_USER")
    missing = []
    if not host:
        missing.append("terminal.ssh_host")
    if not user:
        missing.append("terminal.ssh_user")
    if missing:
        return (
            "needs_setup",
            f"Set {' and '.join(missing)} in config.yaml (or the matching TERMINAL_SSH_* env vars).",
        )
    return ("ready", f"{user}@{host}")


def _probe_modal_backend() -> tuple:
    try:
        from tools.tool_backend_helpers import has_direct_modal_credentials

        if has_direct_modal_credentials():
            return ("ready", "")
    except Exception:
        pass
    try:
        from hermes_cli.config import get_env_value

        if get_env_value("MODAL_TOKEN_ID") and get_env_value("MODAL_TOKEN_SECRET"):
            return ("ready", "")
    except Exception:
        pass
    return (
        "needs_setup",
        "Modal credentials not found — set MODAL_TOKEN_ID and MODAL_TOKEN_SECRET (or run `modal setup`).",
    )


def _probe_daytona_backend() -> tuple:
    try:
        from hermes_cli.config import get_env_value

        if get_env_value("DAYTONA_API_KEY"):
            return ("ready", "")
    except Exception:
        pass
    return ("needs_setup", "Set DAYTONA_API_KEY to use the Daytona backend.")


def _probe_terminal_backend(name: str, terminal_cfg: dict) -> tuple:
    """Return ``(status, detail)`` for one backend. Never raises."""
    try:
        if name == "local":
            return ("ready", "")
        if name == "docker":
            return _probe_docker_backend()
        if name == "singularity":
            return _probe_singularity_backend()
        if name == "ssh":
            return _probe_ssh_backend(terminal_cfg)
        if name == "modal":
            return _probe_modal_backend()
        if name == "daytona":
            return _probe_daytona_backend()
        try:
            from agent.terminal_env_registry import get_provider

            provider = get_provider(name)
            if provider is not None:
                return provider.probe()
        except Exception:
            pass
        return ("unavailable", f"Unknown backend: {name}")
    except Exception as exc:  # pragma: no cover — belt-and-braces guard
        return ("unavailable", f"Probe failed: {exc}")






# ---------------------------------------------------------------------------
# Computer Use (cua-driver) — cross-platform readiness + macOS permission grant
#
# cua-driver runs on macOS, Windows, and Linux. The desktop card reflects
# per-OS readiness: on macOS the Accessibility + Screen Recording TCC grants
# (which attach to cua-driver's OWN identity, com.trycua.driver — not Hermes,
# so no app entitlement is involved); elsewhere, driver health from
# `cua-driver doctor`. The grant flow is macOS-only (no TCC toggles to request
# on Windows/Linux).
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# Raw YAML config endpoint
# ---------------------------------------------------------------------------


@app.get("/api/config/raw")
async def get_config_raw(profile: Optional[str] = None):
    """Raw config.yaml text plus its resolved path.

    ``path`` is resolved inside ``_profile_scope`` so the Config page header
    shows the file the switched profile actually reads/writes — /api/status's
    ``config_path`` is machine-global and always reports the dashboard
    process's own profile, which is wrong under the global profile switcher.
    """
    def _run():
        with _profile_scope(profile):
            path = get_config_path()
        if not path.exists():
            return {"yaml": "", "path": str(path)}
        return {"yaml": path.read_text(encoding="utf-8"), "path": str(path)}

    return await asyncio.to_thread(_run)


@app.put("/api/config/raw")
async def update_config_raw(body: RawConfigUpdate, profile: Optional[str] = None):
    def _run():
        parsed = yaml.safe_load(body.yaml_text)
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=400, detail="YAML must be a mapping")
        approvals_mode_changed = False
        with _profile_scope(body.profile or profile):
            # Full-document replacement: the editor owns the whole file; do not
            # merge omitted sections back from disk (#62723).
            approvals_mode_changed = _approval_mode_of(parsed) != _approval_mode_of(read_raw_config())
            save_config(parsed, merge_existing=False)
        # Same indicator refresh as the schema-driven save above.
        if approvals_mode_changed and not _is_other_profile(body.profile or profile):
            _broadcast_gateway_session_info()
        return {"ok": True}

    try:
        return await asyncio.to_thread(_run)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}")


# ---------------------------------------------------------------------------
# Token / cost analytics endpoint
# ---------------------------------------------------------------------------


def _aux_usage_rows(db, cutoff: float) -> List[Dict[str, Any]]:
    """Per-(model, task) auxiliary usage within the window (issue #23270).

    Reads the task-dimension rows (task != '') that record_auxiliary_usage
    writes into session_model_usage. Returns [] when the table predates the
    task column (older DB opened read-only by newer code).
    """
    try:
        cur = db._conn.execute("""
            SELECT u.model,
                   u.task,
                   u.billing_provider,
                   SUM(u.input_tokens) as input_tokens,
                   SUM(u.output_tokens) as output_tokens,
                   SUM(u.cache_read_tokens) as cache_read_tokens,
                   SUM(u.reasoning_tokens) as reasoning_tokens,
                   COALESCE(SUM(u.estimated_cost_usd), 0) as estimated_cost,
                   COUNT(DISTINCT u.session_id) as sessions,
                   SUM(COALESCE(u.api_call_count, 0)) as api_calls,
                   MAX(u.last_seen) as last_used_at
            FROM session_model_usage u
            JOIN sessions s ON s.id = u.session_id
            WHERE s.started_at > ? AND u.task != ''
            GROUP BY u.model, u.task, u.billing_provider
            ORDER BY SUM(u.input_tokens) + SUM(u.output_tokens) DESC
        """, (cutoff,))
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        # Table predates the task column (older DB opened by newer code) —
        # aux breakdown is simply unavailable.
        return []


def _merge_aux_into_by_model(
    by_model: List[Dict[str, Any]], aux_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Fold aux usage rows into the sessions-derived per-model list.

    Aux usage lives only in session_model_usage (never in the sessions
    counters), so adding it here cannot double-count. Models that ONLY
    appear via aux calls (e.g. a dedicated vision model) get their own
    entry — previously they were entirely invisible.
    """
    if not aux_rows:
        return by_model
    merged: Dict[str, Dict[str, Any]] = {}
    for row in by_model:
        merged[row.get("model") or "unknown"] = row
    for aux in aux_rows:
        model = aux.get("model") or "unknown"
        target = merged.get(model)
        if target is None:
            target = {
                "model": model,
                "input_tokens": 0,
                "output_tokens": 0,
                "estimated_cost": 0,
                "sessions": 0,
                "api_calls": 0,
            }
            merged[model] = target
        target["input_tokens"] = (target.get("input_tokens") or 0) + (aux.get("input_tokens") or 0)
        target["output_tokens"] = (target.get("output_tokens") or 0) + (aux.get("output_tokens") or 0)
        target["estimated_cost"] = (target.get("estimated_cost") or 0) + (aux.get("estimated_cost") or 0)
        target["api_calls"] = (target.get("api_calls") or 0) + (aux.get("api_calls") or 0)
        tasks = target.setdefault("aux_tasks", [])
        tasks.append({
            "task": aux.get("task") or "",
            "input_tokens": aux.get("input_tokens") or 0,
            "output_tokens": aux.get("output_tokens") or 0,
            "estimated_cost": aux.get("estimated_cost") or 0,
            "api_calls": aux.get("api_calls") or 0,
        })
    result = list(merged.values())
    result.sort(
        key=lambda r: (r.get("input_tokens") or 0) + (r.get("output_tokens") or 0),
        reverse=True,
    )
    return result


def _aux_task_summary(aux_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate aux usage rows across models into a per-task summary."""
    by_task: Dict[str, Dict[str, Any]] = {}
    for aux in aux_rows:
        task = aux.get("task") or ""
        d = by_task.setdefault(task, {
            "task": task,
            "input_tokens": 0,
            "output_tokens": 0,
            "estimated_cost": 0,
            "api_calls": 0,
            "models": [],
        })
        d["input_tokens"] += aux.get("input_tokens") or 0
        d["output_tokens"] += aux.get("output_tokens") or 0
        d["estimated_cost"] += aux.get("estimated_cost") or 0
        d["api_calls"] += aux.get("api_calls") or 0
        model = aux.get("model") or "unknown"
        if model not in d["models"]:
            d["models"].append(model)
    result = list(by_task.values())
    result.sort(
        key=lambda r: (r.get("input_tokens") or 0) + (r.get("output_tokens") or 0),
        reverse=True,
    )
    return result


def _get_usage_analytics(days: int = 30, profile: Optional[str] = None):
    from agent.insights import InsightsEngine

    db = _open_session_db_for_profile(profile, read_only=True)
    try:
        cutoff = time.time() - (days * 86400)
        cur = db._conn.execute("""
            SELECT date(started_at, 'unixepoch') as day,
                   SUM(input_tokens) as input_tokens,
                   SUM(output_tokens) as output_tokens,
                   SUM(cache_read_tokens) as cache_read_tokens,
                   SUM(reasoning_tokens) as reasoning_tokens,
                   COALESCE(SUM(estimated_cost_usd), 0) as estimated_cost,
                   COALESCE(SUM(actual_cost_usd), 0) as actual_cost,
                   COUNT(*) as sessions,
                   SUM(COALESCE(api_call_count, 0)) as api_calls
            FROM sessions WHERE started_at > ?
            GROUP BY day ORDER BY day
        """, (cutoff,))
        daily = [dict(r) for r in cur.fetchall()]

        cur2 = db._conn.execute("""
            SELECT model,
                   SUM(input_tokens) as input_tokens,
                   SUM(output_tokens) as output_tokens,
                   COALESCE(SUM(estimated_cost_usd), 0) as estimated_cost,
                   COUNT(*) as sessions,
                   SUM(COALESCE(api_call_count, 0)) as api_calls
            FROM sessions WHERE started_at > ? AND model IS NOT NULL
            GROUP BY model ORDER BY SUM(input_tokens) + SUM(output_tokens) DESC
        """, (cutoff,))
        by_model = [dict(r) for r in cur2.fetchall()]

        # Fold in auxiliary usage (vision, compression, title_generation, ...)
        # recorded per (model, task) in session_model_usage. Aux calls never
        # touch the sessions counters, so this is add-only — no double count.
        # Without it the models list shows only the main agent model even when
        # aux models are actively burning tokens (issue #23270).
        aux_rows = _aux_usage_rows(db, cutoff)
        by_model = _merge_aux_into_by_model(by_model, aux_rows)

        cur3 = db._conn.execute("""
            SELECT SUM(input_tokens) as total_input,
                   SUM(output_tokens) as total_output,
                   SUM(cache_read_tokens) as total_cache_read,
                   SUM(reasoning_tokens) as total_reasoning,
                   COALESCE(SUM(estimated_cost_usd), 0) as total_estimated_cost,
                   COALESCE(SUM(actual_cost_usd), 0) as total_actual_cost,
                   COUNT(*) as total_sessions,
                   SUM(COALESCE(api_call_count, 0)) as total_api_calls
            FROM sessions WHERE started_at > ?
        """, (cutoff,))
        totals = dict(cur3.fetchone())
        usage = InsightsEngine(db).get_usage_breakdown(days=days)

        return {
            "daily": daily,
            "by_model": by_model,
            # Aux-task summary across models (vision, compression, ...). Lets
            # the dashboard answer "what is compression costing me" directly.
            "by_task": _aux_task_summary(aux_rows),
            "totals": totals,
            "period_days": days,
            "skills": usage["skills"],
            # Per-tool-name call counts (already computed by InsightsEngine);
            # the desktop Capabilities page aggregates these per toolset.
            "tools": usage["tools"],
        }
    finally:
        db.close()


@app.get("/api/analytics/usage")
async def get_usage_analytics(
    days: int = Query(30, ge=1, le=365),
    profile: Optional[str] = None,
):
    """``days`` is clamped to 1-365 (idea from #74778): huge or non-positive
    values would force expensive full-history SQL and InsightsEngine work, or
    produce empty/inverted time windows. The UI only offers 7/30/90-day
    presets."""
    return await asyncio.to_thread(_get_usage_analytics, days, profile)


def _get_models_analytics(days: int = 30, profile: Optional[str] = None):
    """Rich per-model analytics for the Models dashboard page.

    Returns token/cost/session breakdown per model plus capability metadata
    from models.dev (context window, vision, tools, reasoning, etc.).
    """
    db = _open_session_db_for_profile(profile, read_only=True)
    try:
        cutoff = time.time() - (days * 86400)

        cur = db._conn.execute("""
            SELECT model,
                   billing_provider,
                   SUM(input_tokens) as input_tokens,
                   SUM(output_tokens) as output_tokens,
                   SUM(cache_read_tokens) as cache_read_tokens,
                   SUM(reasoning_tokens) as reasoning_tokens,
                   COALESCE(SUM(estimated_cost_usd), 0) as estimated_cost,
                   COALESCE(SUM(actual_cost_usd), 0) as actual_cost,
                   COUNT(*) as sessions,
                   SUM(COALESCE(api_call_count, 0)) as api_calls,
                   SUM(tool_call_count) as tool_calls,
                   MAX(started_at) as last_used_at,
                   AVG(input_tokens + output_tokens) as avg_tokens_per_session
            FROM sessions WHERE started_at > ? AND model IS NOT NULL AND model != ''
            GROUP BY model, billing_provider
            ORDER BY SUM(input_tokens) + SUM(output_tokens) DESC
        """, (cutoff,))
        raw_rows = [dict(r) for r in cur.fetchall()]

        # Add auxiliary usage as (model, provider) rows so aux-only models
        # (dedicated vision/compression models) appear on the Models page
        # instead of being invisible (issue #23270). Keyed by
        # model+billing_provider to match the GROUP BY above.
        for aux in _aux_usage_rows(db, cutoff):
            raw_rows.append({
                "model": aux.get("model") or "unknown",
                "billing_provider": aux.get("billing_provider") or "",
                "input_tokens": aux.get("input_tokens") or 0,
                "output_tokens": aux.get("output_tokens") or 0,
                "cache_read_tokens": aux.get("cache_read_tokens") or 0,
                "reasoning_tokens": aux.get("reasoning_tokens") or 0,
                "estimated_cost": aux.get("estimated_cost") or 0,
                "actual_cost": 0,
                "sessions": aux.get("sessions") or 0,
                "api_calls": aux.get("api_calls") or 0,
                "tool_calls": 0,
                "last_used_at": aux.get("last_used_at"),
                "avg_tokens_per_session": 0,
                "aux_task": aux.get("task") or "",
            })

        # Combine all rows for the same model name, regardless of provider, base URL,
        # or task. Sum all accounting fields, take the max last_used_at, and choose
        # the provider with the greatest combined token volume, preferring non-empty on a tie.
        rows_by_model: Dict[str, List[Dict[str, Any]]] = {}
        for row in raw_rows:
            rows_by_model.setdefault(row.get("model") or "", []).append(row)

        rows: List[Dict[str, Any]] = []
        for model_name, model_rows in rows_by_model.items():
            if not model_rows:
                continue
            merged = {
                "model": model_name,
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "reasoning_tokens": 0,
                "estimated_cost": 0.0,
                "actual_cost": 0.0,
                "sessions": 0,
                "api_calls": 0,
                "tool_calls": 0,
                "last_used_at": 0,
            }
            best_provider = ""
            best_provider_tokens = -1

            for r in model_rows:
                merged["input_tokens"] += r.get("input_tokens") or 0
                merged["output_tokens"] += r.get("output_tokens") or 0
                merged["cache_read_tokens"] += r.get("cache_read_tokens") or 0
                merged["reasoning_tokens"] += r.get("reasoning_tokens") or 0
                merged["estimated_cost"] += r.get("estimated_cost") or 0.0
                merged["actual_cost"] += r.get("actual_cost") or 0.0
                merged["sessions"] += r.get("sessions") or 0
                merged["api_calls"] += r.get("api_calls") or 0
                merged["tool_calls"] += r.get("tool_calls") or 0

                lu = r.get("last_used_at") or 0
                if lu > merged["last_used_at"]:
                    merged["last_used_at"] = lu

                p = r.get("billing_provider") or ""
                t = (r.get("input_tokens") or 0) + (r.get("output_tokens") or 0)
                if t > best_provider_tokens:
                    best_provider = p
                    best_provider_tokens = t
                elif t == best_provider_tokens and p and not best_provider:
                    best_provider = p

            merged["billing_provider"] = best_provider
            total_tokens = merged["input_tokens"] + merged["output_tokens"]
            merged["avg_tokens_per_session"] = total_tokens / merged["sessions"] if merged["sessions"] else 0
            rows.append(merged)

        rows.sort(
            key=lambda r: (r.get("input_tokens") or 0) + (r.get("output_tokens") or 0),
            reverse=True,
        )

        models = []
        for row in rows:
            provider = row.get("billing_provider") or ""
            model_name = row["model"]
            caps = {}
            try:
                from agent.models_dev import get_model_capabilities
                mc = get_model_capabilities(provider=provider, model=model_name)
                if mc is not None:
                    caps = {
                        "supports_tools": mc.supports_tools,
                        "supports_vision": mc.supports_vision,
                        "supports_reasoning": mc.supports_reasoning,
                        "context_window": mc.context_window,
                        "max_output_tokens": mc.max_output_tokens,
                        "model_family": mc.model_family,
                    }
            except Exception:
                pass

            models.append({
                "model": model_name,
                "provider": provider,
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "cache_read_tokens": row["cache_read_tokens"],
                "reasoning_tokens": row["reasoning_tokens"],
                "estimated_cost": row["estimated_cost"],
                "actual_cost": row["actual_cost"],
                "sessions": row["sessions"],
                "api_calls": row["api_calls"],
                "tool_calls": row["tool_calls"],
                "last_used_at": row["last_used_at"],
                "avg_tokens_per_session": row["avg_tokens_per_session"],
                "capabilities": caps,
            })

        totals_cur = db._conn.execute("""
            SELECT COUNT(DISTINCT model) as distinct_models,
                   SUM(input_tokens) as total_input,
                   SUM(output_tokens) as total_output,
                   SUM(cache_read_tokens) as total_cache_read,
                   SUM(reasoning_tokens) as total_reasoning,
                   COALESCE(SUM(estimated_cost_usd), 0) as total_estimated_cost,
                   COALESCE(SUM(actual_cost_usd), 0) as total_actual_cost,
                   COUNT(*) as total_sessions,
                   SUM(COALESCE(api_call_count, 0)) as total_api_calls
            FROM sessions WHERE started_at > ? AND model IS NOT NULL AND model != ''
        """, (cutoff,))
        totals = dict(totals_cur.fetchone())

        return {
            "models": models,
            "totals": totals,
            "period_days": days,
        }
    finally:
        db.close()


@app.get("/api/analytics/models")
async def get_models_analytics(
    days: int = Query(30, ge=1, le=365),
    profile: Optional[str] = None,
):
    # ``days`` clamped to 1-365 (idea from #74778) — see get_usage_analytics.
    """Return model analytics without blocking the serving event loop."""
    return await asyncio.to_thread(_get_models_analytics, days, profile)


# ---------------------------------------------------------------------------
# /api/pty — PTY-over-WebSocket bridge for the dashboard "Chat" tab.
#
# The endpoint spawns the same ``hermes --tui`` binary the CLI uses, behind
# a POSIX pseudo-terminal, and forwards bytes + resize escapes across a
# WebSocket.  The browser renders the ANSI through xterm.js (see
# web/src/pages/ChatPage.tsx).
#
# Auth: ``?token=<session_token>`` query param (browsers can't set
# Authorization on the WS upgrade).  Same ephemeral ``_SESSION_TOKEN`` as
# REST.  Localhost-only — we defensively reject non-loopback clients even
# though uvicorn binds to 127.0.0.1.
# ---------------------------------------------------------------------------

# PTY bridge: POSIX uses pty_bridge (fcntl/termios/ptyprocess); native Windows
# uses win_pty_bridge (pywinpty/ConPTY, already a declared dependency).  Both
# expose the same public surface — spawn/read/write/resize/close/is_available —
# so the /api/pty WebSocket handler needs no platform guards.
if sys.platform.startswith("win"):
    try:
        from hermes_cli.win_pty_bridge import WinPtyBridge as PtyBridge, PtyUnavailableError
        _PTY_BRIDGE_AVAILABLE = True
    except ImportError:  # pragma: no cover - pywinpty missing
        PtyBridge = None  # type: ignore[assignment]
        _PTY_BRIDGE_AVAILABLE = False

        class PtyUnavailableError(RuntimeError):  # type: ignore[no-redef]
            """Stub when win_pty_bridge cannot be imported."""
            pass
else:
    try:
        from hermes_cli.pty_bridge import PtyBridge, PtyUnavailableError
        _PTY_BRIDGE_AVAILABLE = True
    except ImportError:  # pragma: no cover - dev env without ptyprocess
        PtyBridge = None  # type: ignore[assignment]
        _PTY_BRIDGE_AVAILABLE = False

        class PtyUnavailableError(RuntimeError):  # type: ignore[no-redef]
            """Stub on platforms where pty_bridge can't be imported."""
            pass

_RESIZE_RE = re.compile(rb"\x1b\[RESIZE:(\d+);(\d+)\]")
_PTY_READ_CHUNK_TIMEOUT = 0.2
# Back-off delay between idle PTY reads so a quiet terminal does not spin
# the event loop.  A positive sleep lets other coroutines run and keeps
# dashboard idle CPU low (#42627).
_PTY_IDLE_BACKOFF = 0.05

# Keep-alive PTY sessions: a terminal connecting with ``?attach=<token>`` is
# bound to a process that survives disconnect/refresh and is reattachable.
from hermes_cli.pty_session import PtySessionRegistry, RegistryFull, run_reaper  # noqa: E402

PTY_REGISTRY = PtySessionRegistry(
    ttl=30 * 60,
    max_sessions=16,
    buffer_cap=1 * 1024 * 1024,
    read_timeout=_PTY_READ_CHUNK_TIMEOUT,
)


async def _legacy_pump(ws: "WebSocket", bridge) -> None:
    """Original 1:1 socket<->PTY pump: stream until disconnect, then close the
    bridge. Used when no ``?attach=`` token is supplied (keep-alive opt-in).

    Behavior is identical to the pre-keep-alive ``pty_ws`` body, including the
    #54028 half-open-socket protection (reader EOF → close the WS so the
    writer's ``ws.receive()`` unparks) and the #53227 ``to_thread`` offloads
    for the blocking ``bridge.close()``.
    """
    loop = asyncio.get_running_loop()

    # --- reader task: PTY master → WebSocket ----------------------------
    async def pump_pty_to_ws() -> None:
        try:
            while True:
                chunk = await loop.run_in_executor(
                    None, bridge.read, _PTY_READ_CHUNK_TIMEOUT
                )
                if chunk is None:  # EOF
                    return
                if not chunk:  # no data this tick; yield control and retry
                    await asyncio.sleep(_PTY_IDLE_BACKOFF)
                    continue
                try:
                    await ws.send_bytes(chunk)
                except Exception:
                    return
        finally:
            # The child has exited (EOF) or the send side broke.  Close the
            # WebSocket so the writer loop's ``ws.receive()`` returns instead
            # of blocking forever — otherwise, when the browser's socket is
            # half-open (no FIN delivered, common on macOS/launchd) the
            # handler never reaches its ``finally`` and the PTY's fds leak.
            # With dashboard auto-reconnect (#52962) every dropped socket then
            # stacks a fresh PTY on top of the orphaned one, exhausting fds.
            #
            # Reap the bridge here too (close() is idempotent): on child EOF the
            # writer loop's ``finally`` is the usual closer, but if the handler
            # task is cancelled the instant we close the WS, that ``finally``
            # can be skipped, leaking the PTY. Closing from the EOF path makes
            # the reap independent of that cancellation race (#54028).
            try:
                await asyncio.to_thread(bridge.close)
            except Exception:
                pass
            try:
                await ws.close()
            except Exception:
                pass

    reader_task = asyncio.create_task(pump_pty_to_ws())

    # --- writer loop: WebSocket → PTY master ----------------------------
    try:
        while True:
            try:
                msg = await ws.receive()
            except RuntimeError:
                # Raised when ws.receive() is called after the socket is
                # already disconnected (e.g. closed by the reader task above).
                break
            if msg.get("type") == "websocket.disconnect":
                break
            raw = msg.get("bytes")
            if raw is None:
                text = msg.get("text")
                raw = text.encode("utf-8") if isinstance(text, str) else b""
            if not raw:
                continue
            # Resize escape is consumed locally, never written to the PTY.
            match = _RESIZE_RE.match(raw)
            if match and match.end() == len(raw):
                bridge.resize(cols=int(match.group(1)), rows=int(match.group(2)))
                continue
            bridge.write(raw)
    except WebSocketDisconnect:
        pass
    finally:
        reader_task.cancel()
        try:
            await reader_task
        except (asyncio.CancelledError, Exception):
            pass
        await asyncio.to_thread(bridge.close)


_VALID_CHANNEL_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
# Starlette's TestClient reports the peer as "testclient"; treat it as
# loopback so tests don't need to rewrite request scope.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", "testclient"})


def _ws_client_reason(ws: "WebSocket") -> Optional[str]:
    """Return a rejection reason for the client IP, or None when allowed.

    Reasons are short machine-parseable tokens logged on the rejection path
    so a "WS keeps closing" report can be diagnosed from agent.log without a
    repro. ``None`` means the peer IP passed this gate.

    See :func:`_ws_client_is_allowed` for the full policy rationale.
    """
    if getattr(app.state, "auth_required", False):
        return None
    bound_host = (getattr(app.state, "bound_host", "") or "").strip().lower()
    if bound_host and bound_host not in _LOOPBACK_HOSTS:
        return None
    client_host = ws.client.host if ws.client else ""
    if not client_host:
        # Fail-closed: a loopback-bound dashboard with auth disabled must
        # not accept a WebSocket with no identifiable peer. ASGI servers
        # behind a misconfigured proxy or unix socket can deliver
        # ws.client == None or "" — treating that as "allowed" would let
        # an unidentified peer reach a loopback-only surface.
        return f"missing_or_empty_peer bound={bound_host or '?'}"
    if client_host in _LOOPBACK_HOSTS:
        return None
    return f"peer_not_loopback peer={client_host} bound={bound_host or '?'}"


def _ws_client_is_allowed(ws: "WebSocket") -> bool:
    """Check if the WebSocket client IP is acceptable.

    Loopback bind: only loopback clients allowed — the legacy
    ``?token=<_SESSION_TOKEN>`` path is the only auth we have, so we
    don't want LAN hosts guessing tokens.

    Explicit non-loopback bind (``--host 0.0.0.0``, ``--host ::``, or a
    specific address such as a Tailscale/LAN IP, always with
    ``--insecure``): allow any peer. The operator explicitly opted into
    non-loopback exposure, so the loopback-only peer restriction does not
    apply. DNS-rebinding is still blocked by the Host/Origin guard in
    :func:`_ws_host_origin_is_allowed`, which mirrors the HTTP layer and
    requires the Host header to match the bound interface — the same
    defence ``_is_accepted_host`` applies to non-loopback HTTP requests.

    Gated mode: any peer is allowed — uvicorn's ``proxy_headers=True``
    (enabled when the OAuth gate is active so cookies can pick up
    ``X-Forwarded-Proto``) rewrites ``ws.client.host`` to the
    X-Forwarded-For value, which is the real internet client IP. The
    OAuth gate + single-use ``?ticket=`` is the auth at that point; the
    Host/Origin guard in :func:`_ws_host_origin_is_allowed` is what
    blocks DNS-rebinding here, not the peer IP.
    """
    if getattr(app.state, "auth_required", False):
        return True
    # Any explicit non-loopback bind (0.0.0.0, ::, or a specific LAN /
    # Tailscale address) means the operator opted into non-loopback
    # access via --insecure.  The loopback-only peer gate only applies to
    # an actual loopback bind; otherwise the WS handshake is rejected even
    # though same-bind HTTP requests pass _is_accepted_host.
    bound_host = (getattr(app.state, "bound_host", "") or "").strip().lower()
    if bound_host and bound_host not in _LOOPBACK_HOSTS:
        return True
    client_host = ws.client.host if ws.client else ""
    if not client_host:
        # Fail-closed: see _ws_client_reason for rationale. An empty
        # client_host on a loopback-bound dashboard with auth disabled
        # must be rejected, not accepted as a default-allow.
        return False
    return client_host in _LOOPBACK_HOSTS


def _ws_host_origin_reason(ws: "WebSocket") -> Optional[str]:
    """Return a Host/Origin rejection reason, or None when allowed.

    Mirrors :func:`_ws_host_origin_is_allowed` but yields a short
    machine-parseable token (``host_mismatch …`` / ``origin_mismatch …``)
    on rejection so the close path can log *why* the upgrade was refused.
    """
    bound_host = getattr(app.state, "bound_host", None)
    if not bound_host:
        return None

    trusted_public_hosts = getattr(
        app.state, "trusted_public_hosts", frozenset()
    )

    host_header = ws.headers.get("host", "")
    if not _is_accepted_host(
        host_header, bound_host, trusted_public_hosts
    ):
        return f"host_mismatch host={host_header or '?'} bound={bound_host}"

    origin = ws.headers.get("origin", "")
    if not origin:
        return None

    parsed = urllib.parse.urlparse(origin)
    if parsed.scheme not in {"http", "https"}:
        # Non-web origin (packaged Electron: file://, null, app://). The
        # upstream credential check is the real auth boundary; trust it.
        # See _ws_host_origin_is_allowed for the full rationale.
        return None

    if not parsed.netloc:
        return f"origin_mismatch origin={origin} bound={bound_host}"

    if not _is_accepted_host(
        parsed.netloc, bound_host, trusted_public_hosts
    ):
        return f"origin_mismatch origin={origin} bound={bound_host}"
    return None


def _ws_host_origin_is_allowed(ws: "WebSocket") -> bool:
    """Apply the dashboard Host/Origin guard to WebSocket upgrades.

    FastAPI HTTP middleware does not run for WebSocket routes, so the
    DNS-rebinding Host check used for normal dashboard HTTP requests must be
    repeated here before accepting the upgrade.  Browsers also send an Origin
    header on WebSocket handshakes; when present, require it to target the
    same bound dashboard host.
    """
    return _ws_host_origin_reason(ws) is None


def _ws_request_reason(ws: "WebSocket") -> Optional[str]:
    """First Host/Origin or peer-IP rejection reason, or None when allowed."""
    return _ws_host_origin_reason(ws) or _ws_client_reason(ws)


def _ws_request_is_allowed(ws: "WebSocket") -> bool:
    """Return True when the WebSocket upgrade matches dashboard boundaries."""
    return _ws_host_origin_is_allowed(ws) and _ws_client_is_allowed(ws)


def _ws_auth_mode() -> str:
    """Short label for the active WS auth mode — logged on every connection."""
    if getattr(app.state, "auth_required", False):
        return "gated"
    bound_host = (getattr(app.state, "bound_host", "") or "").strip().lower()
    if bound_host and bound_host not in _LOOPBACK_HOSTS:
        return "insecure"
    return "loopback"


_GATEWAY_WS_PROTOCOL = "hermes-gateway-v1"
_GATEWAY_WS_TICKET_PROTOCOL_PREFIX = "hermes-gateway-ticket."


def _gateway_ws_ticket_from_subprotocol(ws: "WebSocket") -> tuple[str, str]:
    """Return ``(ticket, reason)`` from an unambiguous gateway protocol set."""
    raw = str(ws.headers.get("sec-websocket-protocol", "") or "")
    protocols = [value.strip() for value in raw.split(",") if value.strip()]
    ticket_protocols = [
        value for value in protocols
        if value.startswith(_GATEWAY_WS_TICKET_PROTOCOL_PREFIX)
    ]
    if not ticket_protocols:
        return "", "none"
    if _GATEWAY_WS_PROTOCOL not in protocols or len(ticket_protocols) != 1:
        return "", "invalid"
    ticket = ticket_protocols[0][len(_GATEWAY_WS_TICKET_PROTOCOL_PREFIX):]
    return (ticket, "ok") if ticket else ("", "invalid")


def _ws_auth_reason(ws: "WebSocket") -> tuple[Optional[str], str]:
    """Validate WS-upgrade auth; return ``(reason, credential)``.

    ``reason`` is None when the credential is accepted, else a short
    machine-parseable token explaining the rejection (``no_credential``,
    ``token_mismatch``, ``ticket_invalid``, ``internal_invalid``).
    ``credential`` names which credential type was presented (``ticket``,
    ``internal``, ``token``, or ``none``) so the accepted path can log *how*
    a peer authed, not just that it did.

    Loopback / ``--insecure``: legacy ``?token=<_SESSION_TOKEN>`` query
    parameter, constant-time compared.

    Gated (public bind, no ``--insecure``): one of two credentials —

    * ``?ticket=<single-use>`` — a browser-minted, single-use, 30s-TTL ticket
      consumed against the dashboard-auth ticket store. This is what the SPA
      (and native clients) use.
    * ``?internal=<process-credential>`` — the process-lifetime internal
      credential, used only by WS clients the server spawns itself (the
      embedded-TUI PTY child attaching to ``/api/ws`` and ``/api/pub``). It
      is multi-use and never expires so the child can reconnect, and is never
      injected into the SPA — see ``dashboard_auth.ws_tickets`` for the
      threat model.

    The legacy ``?token=`` path is unconditionally rejected in gated mode
    (the SPA bundle isn't carrying the token any longer, and a leaked
    ``_SESSION_TOKEN`` must not grant WS access once the gate is engaged).

    Audit-logs the rejection so operators can debug "WS keeps closing"
    issues from the log.
    """
    auth_required = bool(getattr(app.state, "auth_required", False))
    if auth_required:
        # Lazy import — keeps this function importable in test harnesses
        # that don't bring in the dashboard_auth layer.
        from hermes_cli.dashboard_auth.audit import AuditEvent, audit_log
        from hermes_cli.dashboard_auth.ws_tickets import (
            TicketInvalid,
            consume_internal_credential,
            consume_ticket,
        )

        # Server-spawned children (PTY child → /api/ws, /api/pub) present the
        # multi-use internal credential rather than a single-use ticket, so
        # they survive reconnects and slow cold boots.
        internal = ws.query_params.get("internal", "")
        if internal:
            try:
                info = consume_internal_credential(internal)
                # Stamp the server-minted identity onto the WS object so the
                # connection (and any transport built from it) can never be
                # impersonated by RPC params. Internal peers are marked
                # ``server-internal`` and are excluded from privileged
                # controller registration downstream.
                ws._hermes_auth_identity = {
                    "user_id": info.get("user_id"),
                    "provider": info.get("provider"),
                }
                return None, "internal"
            except TicketInvalid as exc:
                audit_log(
                    AuditEvent.WS_TICKET_REJECTED,
                    reason=f"internal: {exc}",
                    ip=(ws.client.host if ws.client else ""),
                    path=ws.url.path,
                )
                return "internal_invalid", "internal"

        protocol_ticket, protocol_reason = _gateway_ws_ticket_from_subprotocol(ws)
        if protocol_reason == "invalid":
            return "ticket_invalid", "ticket-subprotocol"
        ticket = protocol_ticket or ws.query_params.get("ticket", "")
        if not ticket:
            return "no_credential", "none"

        try:
            info = consume_ticket(ticket)
            # The ticket binds a server-minted {user_id, provider}; stamp it
            # onto the WS object so ``gateway_ws`` can hand it to the gateway
            # transport, where it is the sole identity authority for
            # browser-controller registration. A client can never supply or
            # spoof this value through RPC params. Only the two identity
            # fields are carried — bookkeeping (e.g. ``minted_at``) is not
            # part of the identity contract.
            ws._hermes_auth_identity = {
                "user_id": info.get("user_id"),
                "provider": info.get("provider"),
            }
            if protocol_ticket:
                # Select only the stable public protocol during accept. The
                # ticket-bearing protocol is a credential and must never be
                # reflected back to the browser or retained after admission.
                ws._hermes_ws_subprotocol = _GATEWAY_WS_PROTOCOL
                return None, "ticket-subprotocol"
            return None, "ticket"
        except TicketInvalid as exc:
            audit_log(
                AuditEvent.WS_TICKET_REJECTED,
                reason=str(exc),
                ip=(ws.client.host if ws.client else ""),
                path=ws.url.path,
            )
            return "ticket_invalid", "ticket"

    token = ws.query_params.get("token", "")
    if not token:
        return "no_credential", "none"
    if hmac.compare_digest(token.encode(), _SESSION_TOKEN.encode()):
        return None, "token"
    return "token_mismatch", "token"


def _ws_auth_ok(ws: "WebSocket") -> bool:
    """True when the WS-upgrade credential is accepted. See _ws_auth_reason."""
    return _ws_auth_reason(ws)[0] is None

# Per-channel subscriber registry used by /api/pub (PTY-side gateway → dashboard)
# and /api/events (dashboard → browser sidebar).  Keyed by an opaque channel id
# the chat tab generates on mount; entries auto-evict when the last subscriber
# drops AND the publisher has disconnected.
# (Channel state and the chat-argv lock are initialised in _lifespan on app
# startup — see _get_event_state / _get_chat_argv_lock above.)


def _resolve_chat_argv(
    resume: Optional[str] = None,
    sidecar_url: Optional[str] = None,
    profile: Optional[str] = None,
    active_session_file: Optional[str] = None,
) -> tuple[list[str], Optional[str], Optional[dict]]:
    """Resolve the argv + cwd + env for the chat PTY.

    Default: whatever ``hermes --tui`` would run.  Tests monkeypatch this
    function to inject a tiny fake command (``cat``, ``sh -c 'printf …'``)
    so nothing has to build Node or the TUI bundle.

    Session resume is propagated via the ``HERMES_TUI_RESUME`` env var —
    matching what ``hermes_cli.main._launch_tui`` does for the CLI path.
    Appending ``--resume <id>`` to argv doesn't work because ``ui-tui`` does
    not parse its argv.

    ``HERMES_TUI_GATEWAY_URL`` is injected so the PTY child can attach to
    this process's in-memory ``tui_gateway`` instance instead of spawning
    its own Python gateway subprocess.

    `sidecar_url` (when set) is forwarded as ``HERMES_TUI_SIDECAR_URL`` so
    the spawned ``tui_gateway.entry`` can mirror dispatcher emits to the
    dashboard's ``/api/pub`` endpoint (see :func:`pub_ws`).

    `active_session_file` (when set) is forwarded as
    ``HERMES_TUI_ACTIVE_SESSION_FILE``. The TUI writes the current session id
    there whenever it creates/resumes/switches sessions, giving the dashboard a
    small cross-process breadcrumb for reconnecting after an unexpected browser
    WebSocket close.

    `profile` (when set) scopes the ENTIRE chat to that profile by pointing
    ``HERMES_HOME`` at the profile dir in the child env. Every spawned
    process (the TUI and the ``tui_gateway.entry`` it launches) resolves
    ``get_hermes_home()`` from that env var at its own import, so the child
    binds the profile's config, skills, memory, and state.db from the start
    — the same propagation ``hermes -p <name>`` performs. The in-process
    ``HERMES_TUI_GATEWAY_URL`` attach is SKIPPED for scoped chats: the
    dashboard's in-memory gateway runs under the dashboard's own profile,
    so a profile-scoped chat must spawn its own gateway subprocess.
    """
    from hermes_cli.main import PROJECT_ROOT, _apply_tui_python_env, _make_tui_argv

    profile_dir: Optional[Path] = None
    requested = (profile or "").strip()
    if requested and requested.lower() != "current":
        profile_dir = _resolve_profile_dir(requested)

    argv, cwd = _make_tui_argv(PROJECT_ROOT / "ui-tui", tui_dev=False)
    # Hermes TUI child: build via the single spawn-env factory (profile-home
    # contract applied; secrets kept — the spawned agent needs provider creds).
    # An explicit profile scope still overrides HERMES_HOME before config is
    # bridged into the child environment.
    from tools.environments.local import build_subprocess_env
    env = build_subprocess_env(scrub_secrets=False, inherit_profile_home=True)
    if profile_dir is not None:
        env["HERMES_HOME"] = str(profile_dir)
    try:
        from hermes_cli.config import (
            apply_terminal_config_to_env,
            read_raw_config,
            terminal_config_owned_env_vars,
        )

        if profile_dir is not None:
            # The dashboard process already bridged its own terminal config
            # into os.environ at startup. Remove only keys explicitly owned by
            # that launch profile before applying the selected profile. Values
            # exported by the operator for keys omitted from the launch profile
            # remain valid fallbacks, matching apply_terminal_config_to_env().
            raw_launch_terminal = read_raw_config().get("terminal")
            for env_var in terminal_config_owned_env_vars(raw_launch_terminal):
                env.pop(env_var, None)
            with _config_profile_scope(requested):
                apply_terminal_config_to_env(env=env)
        else:
            apply_terminal_config_to_env(env=env)
    except Exception:
        _log.warning("Failed to apply terminal config bridge for dashboard chat", exc_info=True)
    _apply_tui_python_env(env)
    env.setdefault("NODE_ENV", "production")
    # Browser-embedded chat should prefer stable wheel-based scrollback over
    # native terminal mouse tracking. When mouse tracking is enabled, wheel
    # events are consumed by the TUI and forwarded as terminal input, which
    # makes browser-side transcript scrolling feel broken. Keep the terminal
    # build unchanged for native CLI usage; only disable mouse tracking for
    # the dashboard PTY path.
    env.setdefault("HERMES_TUI_DISABLE_MOUSE", "1")
    env.setdefault("HERMES_TUI_INLINE", "1")
    # The dashboard terminal is xterm.js, which always renders 24-bit RGB.
    # But chalk inside the TUI child decides its color depth from the
    # SERVER process env — and hosted/cloud deploys run the dashboard under
    # a process manager (container init, systemd) with no COLORTERM, so
    # chalk downgrades every hex color to the xterm 256 palette. The skin's
    # bronze border #CD7F32 snaps to palette 173 (#D7875F, salmon-red) and
    # the banner reads red/yellow instead of gold. Local launches dodge
    # this only because the operator's interactive terminal leaks
    # COLORTERM=truecolor into os.environ. Backfill it for the PTY child;
    # setdefault so an explicit operator value still wins.
    env.setdefault("COLORTERM", "truecolor")
    env["HERMES_TUI_DASHBOARD"] = "1"

    if resume:
        _resume_db = _open_session_db_for_profile(
            requested if profile_dir is not None else None,
            read_only=True,
        )
        try:
            latest_resume, _latest_path = _session_latest_descendant(resume, _resume_db)
        finally:
            _resume_db.close()
        if latest_resume:
            resume = latest_resume
        env["HERMES_TUI_RESUME"] = resume

    if sidecar_url:
        env["HERMES_TUI_SIDECAR_URL"] = sidecar_url

    if active_session_file:
        env["HERMES_TUI_ACTIVE_SESSION_FILE"] = active_session_file

    # Profile-scoped chats must NOT attach to the dashboard's in-memory
    # gateway — it runs under the dashboard's own profile. Without the
    # attach URL, gatewayClient spawns its own `tui_gateway.entry`, which
    # inherits the profile HERMES_HOME set above.
    if profile_dir is None:
        if gateway_ws_url := _build_gateway_ws_url():
            env["HERMES_TUI_GATEWAY_URL"] = gateway_ws_url

    return list(argv), str(cwd) if cwd else None, env


# Hosts that mean "listen on every interface" — the server should bind to
# them, but an in-container client must NOT dial them: dialing 0.0.0.0
# resolves to "any local interface", which on most platforms routes through
# the kernel's wildcard stack and behind a forward proxy (HTTPS_PROXY with
# a NO_PROXY that doesn't list 0.0.0.0) gets MITM'd into a failed handshake
# (issue #58993).  The fix is to use a loopback address for the client
# netloc while leaving the bind host alone.
_WILDCARD_HOSTS = frozenset({"0.0.0.0", "::"})


def _resolve_client_ws_host() -> Optional[str]:
    """Return the host the in-container WS client should dial.

    Resolution order:

    1. Explicit ``HERMES_DASHBOARD_WS_HOST`` env var — wins always. Operators
       running the dashboard behind a forward proxy can pin a routable host
       (e.g. ``127.0.0.1``, the container's internal IP, or a sidecar DNS
       name) and bypass auto-detection entirely.
    2. The configured bind host — if it's a wildcard (``0.0.0.0`` / ``::``),
       substitute ``127.0.0.1`` since both the dashboard and its TUI child
       run in the same container.
    3. Any other bind host (loopback or LAN IP) — preserved verbatim.
    """
    explicit = os.environ.get("HERMES_DASHBOARD_WS_HOST", "").strip()
    if explicit:
        return explicit

    host = getattr(app.state, "bound_host", None)
    if not host:
        return None

    if host in _WILDCARD_HOSTS:
        return "127.0.0.1"

    return host


def _build_gateway_ws_url() -> Optional[str]:
    """ws:// URL the PTY child should attach to for JSON-RPC gateway traffic.

    Loopback / ``--insecure``: ``?token=<_SESSION_TOKEN>``.

    Gated mode: the legacy token path is rejected by ``_ws_auth_ok``, so the
    server-spawned PTY child authenticates with the process-lifetime internal
    credential (``?internal=``). It must NOT use a single-use browser ticket:
    the child reads this URL once at startup and reuses it on every reconnect,
    and a 30s-TTL ticket can expire before a slow cold boot even dials.
    """
    host = _resolve_client_ws_host()
    port = getattr(app.state, "bound_port", None)

    if not host or not port:
        return None

    netloc = (
        f"[{host}]:{port}"
        if ":" in host and not host.startswith("[")
        else f"{host}:{port}"
    )

    if getattr(app.state, "auth_required", False):
        from hermes_cli.dashboard_auth.ws_tickets import internal_ws_credential

        qs = urllib.parse.urlencode({"internal": internal_ws_credential()})
    else:
        qs = urllib.parse.urlencode({"token": _SESSION_TOKEN})

    return f"ws://{netloc}/api/ws?{qs}"


async def _resolve_chat_argv_async(
    resume: Optional[str] = None,
    sidecar_url: Optional[str] = None,
    profile: Optional[str] = None,
    active_session_file: Optional[str] = None,
) -> tuple[list[str], Optional[str], Optional[dict]]:
    """Resolve chat argv without blocking the dashboard event loop.

    ``_resolve_chat_argv`` may run ``npm install`` / ``npm run build`` through
    ``_make_tui_argv``.  Keep that synchronous work off the WebSocket event
    loop so reverse proxies and existing dashboard connections can continue
    to exchange keepalives while the TUI launch command is prepared.  The
    async lock preserves the previous one-build-at-a-time behavior when
    multiple browser tabs connect at once without occupying worker threads
    while queued connections wait.
    """
    kwargs = {
        "resume": resume,
        "sidecar_url": sidecar_url,
        "profile": profile,
    }
    if active_session_file is not None:
        kwargs["active_session_file"] = active_session_file

    async with _get_chat_argv_lock(app):
        return await asyncio.to_thread(
            _resolve_chat_argv,
            **kwargs,
        )


def _build_sidecar_url(channel: str) -> Optional[str]:
    """ws:// URL the PTY child should publish events to, or None when unbound.

    Loopback / ``--insecure``: uses ``?token=<_SESSION_TOKEN>``.

    Gated mode: authenticates with the process-lifetime internal credential
    (``?internal=``), the same one ``_build_gateway_ws_url`` uses. The PTY
    child is a server-spawned process we trust; the credential is multi-use
    and never expires, so the child can reconnect ``/api/pub`` without a new
    URL. (This previously minted a single-use 30s ticket, which meant the
    child could not reconnect and could miss the window on a slow cold boot.)
    Connections authenticated this way are recorded under the
    ``server-internal`` identity in the audit log.
    """
    host = _resolve_client_ws_host()
    port = getattr(app.state, "bound_port", None)

    if not host or not port:
        return None

    netloc = f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"

    if getattr(app.state, "auth_required", False):
        # Gated mode — use the internal credential so the WS upgrade survives
        # _ws_auth_ok and the child can reconnect.
        from hermes_cli.dashboard_auth.ws_tickets import internal_ws_credential

        qs = urllib.parse.urlencode(
            {"internal": internal_ws_credential(), "channel": channel}
        )
    else:
        qs = urllib.parse.urlencode({"token": _SESSION_TOKEN, "channel": channel})

    return f"ws://{netloc}/api/pub?{qs}"


async def _broadcast_event(app: Any, channel: str, payload: str) -> None:
    """Fan out one publisher frame to every subscriber on `channel`."""
    event_channels, event_lock = _get_event_state(app)
    async with event_lock:
        subs = list(event_channels.get(channel, ()))

    for sub in subs:
        try:
            await sub.send_text(payload)
        except Exception:
            # Subscriber went away mid-send; the /api/events finally clause
            # will remove it from the registry on its next iteration.
            _log.warning("broadcast send failed for subscriber on %s", channel, exc_info=True)


def _channel_or_close_code(ws: WebSocket) -> Optional[str]:
    """Return the channel id from the query string or None if invalid."""
    channel = ws.query_params.get("channel", "")

    return channel if _VALID_CHANNEL_RE.match(channel) else None


def _active_session_file_for_channel(app: "FastAPI", channel: str) -> Path:
    """Return the per-channel file where a dashboard TUI writes its active sid."""
    files = _get_pty_active_session_files(app)
    existing = files.get(channel)
    if existing is not None:
        return existing

    fd, raw_path = tempfile.mkstemp(prefix="hermes-pty-active-", suffix=".json")
    os.close(fd)
    path = Path(raw_path)
    files[channel] = path
    return path


def _read_active_session_file(path: Path) -> Optional[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    session_id = str(data.get("session_id") or "").strip()
    return session_id or None


def _forget_active_session_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _ws_close_reason(text: str) -> str:
    """Clamp a WS close reason to the protocol's 123-byte UTF-8 limit.

    RFC 6455 caps the close-frame reason at 123 bytes; uvicorn raises if a
    longer string is passed. Our reasons embed an attacker-controlled origin,
    so truncate defensively rather than crash the close handler.
    """
    encoded = text.encode("utf-8", "replace")
    if len(encoded) <= 123:
        return text
    return encoded[:120].decode("utf-8", "ignore") + "..."


# ---------------------------------------------------------------------------
# /api/console — safe Hermes Console command WebSocket.
#
# Unlike /api/pty, this endpoint never spawns a PTY, shell, or full Hermes CLI
# subprocess. It runs the curated console engine in-process and exchanges
# structured JSON frames with the dashboard xterm overlay.
# ---------------------------------------------------------------------------

_CONSOLE_PROMPT = "hermes> "
_CONSOLE_COMMAND_TIMEOUT_SECONDS = 60.0
_CONSOLE_OUTPUT_LIMIT = 50000

# Console commands run in a worker thread. On a timeout, asyncio.wait_for cancels
# the *awaitable*, but Python threads aren't preemptible, so a genuinely stuck
# worker keeps running to completion. To keep that from exhausting the shared
# default thread pool (asyncio.to_thread), we run console commands on a small
# dedicated, bounded pool: a leaked worker is capped, and concurrent console
# execution is bounded to a fixed number of threads regardless of reconnects.
_CONSOLE_EXECUTOR_MAX_WORKERS = 4
_console_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
_console_executor_lock = threading.Lock()


def _get_console_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Lazily create the bounded console worker pool (once per process)."""
    global _console_executor
    if _console_executor is None:
        with _console_executor_lock:
            if _console_executor is None:
                _console_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_CONSOLE_EXECUTOR_MAX_WORKERS,
                    thread_name_prefix="hermes-console",
                )
                # Ensure the pool is torn down on interpreter exit. Don't wait on
                # in-flight workers: a stuck 60s console command must not block
                # shutdown (cancel_futures drops anything not yet started).
                atexit.register(
                    lambda: _console_executor
                    and _console_executor.shutdown(wait=False, cancel_futures=True)
                )
    return _console_executor


def _console_profile_from_ws(ws: WebSocket) -> Optional[str]:
    profile = (ws.query_params.get("profile") or "").strip()
    return profile or None


def _execute_console_line(
    engine: Any,
    line: str,
    *,
    confirmed: bool,
    profile: Optional[str],
) -> Any:
    # _profile_scope swaps process-global skill module paths; keep it inside
    # the worker thread and never hold it across awaits.
    with _profile_scope(profile):
        return engine.execute(line, confirmed=confirmed)


async def _console_send(
    ws: WebSocket,
    send_lock: asyncio.Lock,
    payload: Dict[str, Any],
) -> None:
    async with send_lock:
        await ws.send_json(payload)


async def _console_send_result(
    ws: WebSocket,
    send_lock: asyncio.Lock,
    result: Any,
    *,
    command_id: int,
) -> None:
    command = result.command or ""
    status = result.status
    if status == "ok":
        if result.output:
            await _console_send(
                ws,
                send_lock,
                {
                    "type": "output",
                    "id": command_id,
                    "stream": "stdout",
                    "data": result.output,
                    "command": command,
                },
            )
        await _console_send(
            ws,
            send_lock,
            {
                "type": "complete",
                "id": command_id,
                "status": "ok",
                "command": command,
                "prompt": _CONSOLE_PROMPT,
            },
        )
        return

    if status == "error":
        await _console_send(
            ws,
            send_lock,
            {
                "type": "error",
                "id": command_id,
                "message": result.output or "Command failed.",
                "command": command,
            },
        )
        await _console_send(
            ws,
            send_lock,
            {
                "type": "complete",
                "id": command_id,
                "status": "error",
                "command": command,
                "prompt": _CONSOLE_PROMPT,
            },
        )
        return

    if status == "confirm_required":
        await _console_send(
            ws,
            send_lock,
            {
                "type": "confirm_required",
                "id": command_id,
                "command": command,
                "message": result.confirmation_message or f"Run `{command}`?",
                "prompt": _CONSOLE_PROMPT,
            },
        )
        await _console_send(
            ws,
            send_lock,
            {
                "type": "complete",
                "id": command_id,
                "status": "confirm_required",
                "command": command,
                "prompt": _CONSOLE_PROMPT,
            },
        )
        return

    if status == "clear":
        await _console_send(ws, send_lock, {"type": "clear", "id": command_id})
        await _console_send(
            ws,
            send_lock,
            {
                "type": "complete",
                "id": command_id,
                "status": "clear",
                "command": command,
                "prompt": _CONSOLE_PROMPT,
            },
        )
        return

    if status == "exit":
        await _console_send(
            ws,
            send_lock,
            {
                "type": "complete",
                "id": command_id,
                "status": "exit",
                "command": command,
                "prompt": "",
            },
        )
        return

    await _console_send(
        ws,
        send_lock,
        {
            "type": "error",
            "id": command_id,
            "message": f"Unknown console result status: {status}",
            "command": command,
        },
    )


def _console_json_payload(msg: Any) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    raw: str | bytes | None = msg.get("text")
    if raw is None:
        raw = msg.get("bytes")
    if raw is None:
        return None, None
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None, "Console frames must be UTF-8 JSON."
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, "Console frames must be JSON objects."
    if not isinstance(payload, dict):
        return None, "Console frames must be JSON objects."
    return payload, None


@app.websocket("/api/console")
async def console_ws(ws: WebSocket) -> None:
    peer = ws.client.host if ws.client else "?"

    if not _DASHBOARD_EMBEDDED_CHAT_ENABLED:
        _log.info("console refused: embedded chat disabled peer=%s", peer)
        await ws.close(code=4404, reason="embedded chat disabled")
        return

    auth_reason, cred = _ws_auth_reason(ws)
    mode = _ws_auth_mode()
    if auth_reason is not None:
        _log.warning(
            "console auth rejected reason=%s mode=%s cred=%s peer=%s",
            auth_reason, mode, cred, peer,
        )
        await ws.close(code=4401, reason=_ws_close_reason(f"auth: {auth_reason}"))
        return

    host_origin_reason = _ws_host_origin_reason(ws)
    if host_origin_reason is not None:
        _log.warning("console refused: %s peer=%s", host_origin_reason, peer)
        await ws.close(code=4403, reason=_ws_close_reason(host_origin_reason))
        return

    client_reason = _ws_client_reason(ws)
    if client_reason is not None:
        _log.warning("console refused: %s", client_reason)
        await ws.close(code=4408, reason=_ws_close_reason(client_reason))
        return

    await ws.accept()

    profile = _console_profile_from_ws(ws)
    send_lock = asyncio.Lock()

    try:
        from hermes_cli.console_engine import HermesConsoleEngine

        engine = HermesConsoleEngine(output_limit=_CONSOLE_OUTPUT_LIMIT)
        if profile and profile.lower() != "current":
            _resolve_profile_dir(profile)
    except HTTPException as exc:
        await _console_send(
            ws,
            send_lock,
            {
                "type": "error",
                "message": str(exc.detail),
                "prompt": "",
            },
        )
        await ws.close(code=4400, reason=_ws_close_reason(str(exc.detail)))
        return
    except Exception as exc:
        _log.exception("console failed to initialize")
        await _console_send(
            ws,
            send_lock,
            {
                "type": "error",
                "message": f"Console unavailable: {exc}",
                "prompt": "",
            },
        )
        await ws.close(code=1011)
        return

    _log.info(
        "console accepted peer=%s mode=%s cred=%s profile=%s",
        peer,
        mode,
        cred,
        profile or "current",
    )
    await _console_send(
        ws,
        send_lock,
        {
            "type": "ready",
            "profile": profile or "current",
            "prompt": _CONSOLE_PROMPT,
        },
    )

    active_task: asyncio.Task | None = None
    pending_confirmation: Optional[str] = None
    command_generation = 0

    async def run_command(line: str, *, confirmed: bool, command_id: int) -> None:
        nonlocal active_task, pending_confirmation, command_generation
        try:
            loop = asyncio.get_running_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    _get_console_executor(),
                    functools.partial(
                        _execute_console_line,
                        engine,
                        line,
                        confirmed=confirmed,
                        profile=profile,
                    ),
                ),
                timeout=_CONSOLE_COMMAND_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if command_id == command_generation:
                pending_confirmation = None
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "error",
                        "id": command_id,
                        "message": (
                            "Command timed out. Hermes Console returned to the prompt."
                        ),
                        "command": line,
                    },
                )
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "complete",
                        "id": command_id,
                        "status": "timeout",
                        "command": line,
                        "prompt": _CONSOLE_PROMPT,
                    },
                )
        except Exception as exc:
            if command_id == command_generation:
                pending_confirmation = None
                _log.exception("console command failed")
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "error",
                        "id": command_id,
                        "message": str(exc) or exc.__class__.__name__,
                        "command": line,
                    },
                )
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "complete",
                        "id": command_id,
                        "status": "error",
                        "command": line,
                        "prompt": _CONSOLE_PROMPT,
                    },
                )
        else:
            if command_id != command_generation:
                return
            pending_confirmation = (
                result.command if result.status == "confirm_required" else None
            )
            await _console_send_result(
                ws,
                send_lock,
                result,
                command_id=command_id,
            )
            if result.status == "exit":
                await ws.close(code=1000)
        finally:
            if command_id == command_generation:
                active_task = None

    async def start_command(line: str, *, confirmed: bool = False) -> None:
        nonlocal active_task, command_generation
        command_generation += 1
        command_id = command_generation
        active_task = asyncio.create_task(
            run_command(line, confirmed=confirmed, command_id=command_id)
        )

    try:
        while True:
            try:
                msg = await ws.receive()
            except RuntimeError:
                break
            msg_type = msg.get("type")
            if msg_type == "websocket.disconnect":
                break

            payload, error = _console_json_payload(msg)
            if error:
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "error",
                        "message": error,
                        "prompt": _CONSOLE_PROMPT,
                    },
                )
                continue
            if payload is None:
                continue

            frame_type = str(payload.get("type") or "").strip().lower()
            if frame_type == "ping":
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "pong",
                        "prompt": _CONSOLE_PROMPT,
                    },
                )
                continue

            if frame_type == "cancel":
                if active_task and not active_task.done():
                    command_generation += 1
                    active_task.cancel()
                    active_task = None
                    pending_confirmation = None
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "complete",
                            "status": "cancelled",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                elif pending_confirmation:
                    pending_confirmation = None
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "complete",
                            "status": "cancelled",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                else:
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "complete",
                            "status": "idle",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                continue

            if active_task and not active_task.done():
                await _console_send(
                    ws,
                    send_lock,
                    {
                        "type": "error",
                        "message": "A console command is already running.",
                        "prompt": _CONSOLE_PROMPT,
                    },
                )
                continue

            if frame_type == "confirm":
                command = str(payload.get("command") or pending_confirmation or "").strip()
                if not pending_confirmation:
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "error",
                            "message": "No command is waiting for confirmation.",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                    continue
                if command != pending_confirmation:
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "error",
                            "message": "Confirmation does not match the pending command.",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                    continue
                pending_confirmation = None
                await start_command(command, confirmed=True)
                continue

            if frame_type in {"input", "command"}:
                line = str(payload.get("line") or payload.get("command") or "").strip()
                if not line:
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "complete",
                            "status": "ok",
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                    continue
                if pending_confirmation:
                    await _console_send(
                        ws,
                        send_lock,
                        {
                            "type": "error",
                            "message": (
                                "Confirm or cancel the pending command before "
                                "running another one."
                            ),
                            "prompt": _CONSOLE_PROMPT,
                        },
                    )
                    continue
                await start_command(line)
                continue

            await _console_send(
                ws,
                send_lock,
                {
                    "type": "error",
                    "message": f"Unsupported console frame: {frame_type or '?'}",
                    "prompt": _CONSOLE_PROMPT,
                },
            )
    except WebSocketDisconnect:
        pass
    finally:
        if active_task and not active_task.done():
            active_task.cancel()
            try:
                await active_task
            except (asyncio.CancelledError, Exception):
                pass


@app.websocket("/api/pty")
async def pty_ws(ws: WebSocket) -> None:
    peer = ws.client.host if ws.client else "?"

    if not _DASHBOARD_EMBEDDED_CHAT_ENABLED:
        _log.info("pty refused: embedded chat disabled peer=%s", peer)
        await ws.close(code=4404, reason="embedded chat disabled")
        return

    # --- auth + host/origin/peer check (before accept so we can close
    #     cleanly AND tell the client WHY via the close code + reason).
    #     Each gate maps to a distinct close code so the log and the
    #     browser banner agree on the cause:
    #       4401 bad credential   4403 host/origin mismatch
    #       4408 peer not allowed  4404 chat disabled
    auth_reason, cred = _ws_auth_reason(ws)
    mode = _ws_auth_mode()
    if auth_reason is not None:
        _log.warning(
            "pty auth rejected reason=%s mode=%s cred=%s peer=%s",
            auth_reason, mode, cred, peer,
        )
        await ws.close(code=4401, reason=_ws_close_reason(f"auth: {auth_reason}"))
        return

    host_origin_reason = _ws_host_origin_reason(ws)
    if host_origin_reason is not None:
        _log.warning("pty refused: %s peer=%s", host_origin_reason, peer)
        await ws.close(code=4403, reason=_ws_close_reason(host_origin_reason))
        return

    client_reason = _ws_client_reason(ws)
    if client_reason is not None:
        _log.warning("pty refused: %s", client_reason)
        await ws.close(code=4408, reason=_ws_close_reason(client_reason))
        return

    await ws.accept()
    _log.info("pty accepted peer=%s mode=%s cred=%s", peer, mode, cred)

    # On native Windows, the POSIX PTY bridge can't be imported.  Tell the
    # client and close cleanly rather than pretending the feature works.
    if not _PTY_BRIDGE_AVAILABLE:
        await ws.send_text(
            "\r\n\x1b[31mChat unavailable: the embedded terminal requires a "
            "POSIX PTY, which native Windows Python doesn't provide.\x1b[0m\r\n"
            "\x1b[33mInstall Hermes inside WSL2 to use the dashboard's /chat "
            "tab — the rest of the dashboard works here.\x1b[0m\r\n"
        )
        await ws.close(code=1011)
        return

    # --- spawn PTY ------------------------------------------------------
    raw_resume = ws.query_params.get("resume") or None
    resume = raw_resume
    profile = ws.query_params.get("profile") or None
    channel = _channel_or_close_code(ws)
    sidecar_url = _build_sidecar_url(channel) if channel else None
    force_fresh = (ws.query_params.get("fresh") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    active_session_file: Optional[Path] = None

    if channel:
        active_session_file = _active_session_file_for_channel(ws.app, channel)
        if force_fresh:
            resume = None
            _forget_active_session_file(active_session_file)
        elif not resume:
            resume = _read_active_session_file(active_session_file)
            if resume:
                # The client only knows to pin the viewport to the bottom
                # when it requested `?resume=`. Tell it a replay is coming
                # anyway so the implicit active-session fallback gets the
                # same follow-scroll treatment as an explicit resume (#93518).
                await ws.send_json({"type": "resume", "id": resume})

    resolve_kwargs = {
        "resume": resume,
        "sidecar_url": sidecar_url,
        "profile": profile,
    }
    if active_session_file is not None:
        resolve_kwargs["active_session_file"] = str(active_session_file)

    try:
        argv, cwd, env = await _resolve_chat_argv_async(**resolve_kwargs)
    except HTTPException as exc:
        # Unknown/invalid profile from _resolve_profile_dir.
        await ws.send_text(f"\r\n\x1b[31mChat unavailable: {exc.detail}\x1b[0m\r\n")
        await ws.close(code=1011)
        return
    except SystemExit as exc:
        # _make_tui_argv calls sys.exit(1) when node/npm is missing.
        await ws.send_text(f"\r\n\x1b[31mChat unavailable: {exc}\x1b[0m\r\n")
        await ws.close(code=1011)
        return


    attach_token = ws.query_params.get("attach") or None
    registry_resume = raw_resume
    if raw_resume and env:
        registry_resume = env.get("HERMES_TUI_RESUME") or raw_resume
    if attach_token is not None and (registry_resume or profile):
        # Key explicit resumes on their canonical target, never the active-session fallback.
        attach_token = f"{attach_token}\0{profile or ''}\0{registry_resume or ''}"

    def _spawn():
        return PtyBridge.spawn(argv, cwd=cwd, env=env)

    if attach_token is None:
        # Legacy path: 1:1 socket<->PTY, killed on disconnect (unchanged).
        try:
            bridge = _spawn()
        except PtyUnavailableError as exc:
            await ws.send_text(f"\r\n\x1b[31mChat unavailable: {exc}\x1b[0m\r\n")
            await ws.close(code=1011)
            return
        except (FileNotFoundError, OSError) as exc:
            await ws.send_text(f"\r\n\x1b[31mChat failed to start: {exc}\x1b[0m\r\n")
            await ws.close(code=1011)
            return
        await _legacy_pump(ws, bridge)
        return

    # Keep-alive path: the PTY outlives this socket; reattach by token.
    try:
        session, _created = await PTY_REGISTRY.attach_or_spawn(
            attach_token, spawn=_spawn
        )
    except PtyUnavailableError as exc:
        await ws.send_text(f"\r\n\x1b[31mChat unavailable: {exc}\x1b[0m\r\n")
        await ws.close(code=1011)
        return
    except (FileNotFoundError, OSError, RegistryFull) as exc:
        await ws.send_text(f"\r\n\x1b[31mChat unavailable: {exc}\x1b[0m\r\n")
        await ws.close(code=1011)
        return

    # A fresh xterm cannot reliably reconstruct the TUI from an arbitrary
    # bounded tail of alternate-screen, differential ANSI output. Reused PTYs
    # emit a complete frame after replay so reconnects never reopen blank.
    await session.attach(ws, force_redraw=not _created)

    # --- writer loop: WebSocket → PTY master ----------------------------
    # No reader task here: the session's drain task (spawned once per PTY,
    # inside the registry) forwards PTY output to whichever socket is
    # attached and rings-buffers it while detached.  On child EOF the drain
    # closes the attached socket with 4410, which unparks ``ws.receive()``
    # below — same half-open-socket protection the legacy pump has (#54028).
    try:
        while True:
            try:
                msg = await ws.receive()
            except RuntimeError:
                # ws.receive() after the socket is already disconnected
                # (e.g. closed by the drain task on process exit).
                break
            if msg.get("type") == "websocket.disconnect":
                break
            raw = msg.get("bytes")
            if raw is None:
                text = msg.get("text")
                raw = text.encode("utf-8") if isinstance(text, str) else b""
            if not raw:
                continue

            # Resize escape is consumed locally, never written to the PTY.
            match = _RESIZE_RE.match(raw)
            if match and match.end() == len(raw):
                session.bridge.resize(cols=int(match.group(1)), rows=int(match.group(2)))
                continue

            session.bridge.write(raw)
    except WebSocketDisconnect:
        pass
    finally:
        # Detach only — the PTY keeps running for a reattach; the registry
        # reaper closes it after the TTL (or immediately on process exit).
        PTY_REGISTRY.detach(attach_token, ws)


# ---------------------------------------------------------------------------
# /api/ws — JSON-RPC WebSocket sidecar for the dashboard "Chat" tab.
#
# Drives the same `tui_gateway.dispatch` surface Ink uses over stdio, so the
# dashboard can render structured metadata (model badge, tool-call sidebar,
# slash launcher, session info) alongside the xterm.js terminal that PTY
# already paints. Both transports bind to the same session id when one is
# active, so a tool.start emitted by the agent fans out to both sinks.
# ---------------------------------------------------------------------------


@app.websocket("/api/ws")
async def gateway_ws(ws: WebSocket) -> None:
    if not _DASHBOARD_EMBEDDED_CHAT_ENABLED:
        await ws.close(code=4403)
        return

    if not _ws_auth_ok(ws):
        await ws.close(code=4401)
        return

    if not _ws_request_is_allowed(ws):
        await ws.close(code=4403)
        return

    from tui_gateway.ws import handle_ws

    # The authenticated identity (ticket / internal credential) was stamped
    # onto the WS object by _ws_auth_reason; carry it into the gateway
    # transport where it becomes the identity authority for privileged RPCs
    # (browser.controller.register). None on the legacy token path.
    await handle_ws(
        ws,
        auth_identity=getattr(ws, "_hermes_auth_identity", None),
        subprotocol=getattr(ws, "_hermes_ws_subprotocol", None),
    )


# ---------------------------------------------------------------------------
# /api/pub + /api/events — chat-tab event broadcast.
#
# The PTY-side ``tui_gateway.entry`` opens /api/pub at startup (driven by
# HERMES_TUI_SIDECAR_URL set in /api/pty's PTY env) and writes every
# dispatcher emit through it.  The dashboard fans those frames out to any
# subscriber that opened /api/events on the same channel id.  This is what
# gives the React sidebar its tool-call feed without breaking the PTY
# child's stdio handshake with Ink.
# ---------------------------------------------------------------------------


@app.websocket("/api/pub")
async def pub_ws(ws: WebSocket) -> None:
    if not _DASHBOARD_EMBEDDED_CHAT_ENABLED:
        await ws.close(code=4403)
        return

    if not _ws_auth_ok(ws):
        await ws.close(code=4401)
        return

    if not _ws_request_is_allowed(ws):
        await ws.close(code=4403)
        return

    channel = _channel_or_close_code(ws)
    if not channel:
        await ws.close(code=4400)
        return

    await ws.accept()

    try:
        while True:
            await _broadcast_event(ws.app, channel, await ws.receive_text())
    except WebSocketDisconnect:
        pass


@app.websocket("/api/events")
async def events_ws(ws: WebSocket) -> None:
    if not _DASHBOARD_EMBEDDED_CHAT_ENABLED:
        await ws.close(code=4403)
        return

    if not _ws_auth_ok(ws):
        await ws.close(code=4401)
        return

    if not _ws_request_is_allowed(ws):
        await ws.close(code=4403)
        return

    channel = _channel_or_close_code(ws)
    if not channel:
        await ws.close(code=4400)
        return

    await ws.accept()

    event_channels, event_lock = _get_event_state(ws.app)
    async with event_lock:
        event_channels.setdefault(channel, set()).add(ws)

    try:
        while True:
            # Subscribers don't speak — the receive() just blocks until
            # disconnect so the connection stays open as long as the
            # browser holds it.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with event_lock:
            subs = event_channels.get(channel)

            if subs is not None:
                subs.discard(ws)

                if not subs:
                    event_channels.pop(channel, None)


def _normalise_prefix(raw: Optional[str]) -> str:
    """Normalise an X-Forwarded-Prefix header value.

    Thin re-export of :func:`hermes_cli.dashboard_auth.prefix.normalise_prefix`
    — the single source of truth lives in the dashboard_auth package so
    the gate middleware, the OAuth routes, the cookie helpers, and the
    SPA mount all agree on validation rules.
    """
    from hermes_cli.dashboard_auth.prefix import normalise_prefix
    return normalise_prefix(raw)


def _render_active_theme_bootstrap_css() -> str:
    """Critical-CSS shim for the active user theme.

    Returns a ``<style>`` block with the ``:root`` CSS variables that
    ``ThemeProvider.applyTheme()`` installs once the
    ``/api/dashboard/themes`` round-trip completes.  The goal is to
    eliminate the green flash where the first paint shows the bundle's
    default Hermes Teal canvas before the SPA flips the configured user
    theme into place.

    Built-in themes return an empty string — their full definitions live
    in ``web/src/themes/presets.ts`` and are applied by the bundle
    before paint, so no shim is needed for them.
    """
    try:
        config = load_config()
        active = cfg_get(config, "dashboard", "theme", default="default")
        if not active or not isinstance(active, str):
            return ""
        # Built-in: the bundle already owns the definition, no flash.
        if any(b["name"] == active for b in _BUILTIN_DASHBOARD_THEMES):
            return ""
        for theme in _discover_user_themes():
            if theme.get("name") != active:
                continue
            palette = theme.get("palette") or {}
            bg = palette.get("background") or {}
            mg = palette.get("midground") or {}
            bg_hex = bg.get("hex", "#0a0a0a") if isinstance(bg, dict) else "#0a0a0a"
            mg_hex = mg.get("hex", "#e5e5e5") if isinstance(mg, dict) else "#e5e5e5"
            typo = theme.get("typography") or {}
            font_sans = typo.get("fontSans") or _THEME_DEFAULT_TYPOGRAPHY["fontSans"]
            base_size = typo.get("baseSize") or _THEME_DEFAULT_TYPOGRAPHY["baseSize"]
            # Defensive ``</style>`` escape — current values are well-known
            # hex/font strings, but this keeps the helper safe if it is
            # later extended to ship user-authored CSS literals.
            def _esc(s: str) -> str:
                return str(s).replace("</", "<\\/")
            # Variable names MUST match what the bundle actually consumes:
            #   - ``--background-base`` / ``--midground-base`` come from
            #     ``layerVars()`` in ``web/src/themes/context.tsx``.
            #   - ``--theme-font-sans`` / ``--theme-base-size`` come from
            #     ``typographyVars()`` there, and ``index.css`` applies them
            #     via ``html{font-family:var(--theme-font-sans);
            #     font-size:var(--theme-base-size)}``.
            # The ``html,body`` canvas rule references the SAME variables
            # instead of literal values so runtime theme switches stay
            # live: ``applyTheme()`` writes these vars as inline styles on
            # ``documentElement``, which outrank this stylesheet block in
            # the cascade — the rule below re-resolves automatically and
            # never goes stale when the user picks a different theme.
            return (
                '<style id="hermes-theme-bootstrap">'
                ":root{"
                f"--background-base:{_esc(bg_hex)};"
                f"--midground-base:{_esc(mg_hex)};"
                f"--theme-font-sans:{_esc(font_sans)};"
                f"--theme-base-size:{_esc(base_size)};"
                "}"
                "html,body{background-color:var(--background-base);"
                "color:var(--midground-base);"
                "font-family:var(--theme-font-sans);"
                "font-size:var(--theme-base-size);}"
                "</style>"
            )
        return ""
    except Exception:
        _log.debug("theme bootstrap render failed", exc_info=True)
        return ""


# Hashed bundle assets (``/assets/<name>-<contenthash>.<ext>``) are immutable
# by construction: any content change produces a new filename, and the entry
# point (index.html) is served ``no-store`` so it always references the
# current hashes. A year-long immutable cache lets browsers skip even the
# revalidation round-trip on every dashboard load.
_IMMUTABLE_ASSET_CACHE_CONTROL = "public, max-age=31536000, immutable"


def mount_spa(application: FastAPI):
    """Mount the built SPA. Falls back to index.html for client-side routing.

    The session token is injected into index.html via a ``<script>`` tag so
    the SPA can authenticate against protected API endpoints without a
    separate (unauthenticated) token-dispensing endpoint.

    When served behind a path-prefix reverse proxy (e.g.
    ``mission-control.tilos.com/hermes/*`` -> local Caddy -> :9119), the
    proxy injects ``X-Forwarded-Prefix: /hermes`` on every request. We
    rewrite the served ``index.html`` so absolute asset URLs (``/assets/...``)
    and the SPA's runtime ``__HERMES_BASE_PATH__`` honour that prefix
    without rebuilding the bundle.
    """
    # `hermes serve` is the headless backend: it must NEVER serve the browser
    # SPA, even if a dist is lying around from a prior `dashboard`/build. Take
    # the no-frontend path so only the JSON-RPC/WS/API surface is reachable.
    _headless = os.environ.get("HERMES_SERVE_HEADLESS") == "1"
    if _headless:
        _msg = (
            "Headless backend (hermes serve): web UI disabled — use "
            "`hermes dashboard` for the browser UI."
        )

        @application.get("/{full_path:path}")
        async def no_frontend(full_path: str):
            # Desktop token handshake (#94227): the Electron shell boots by
            # fetching `/` and extracting ``window.__HERMES_SESSION_TOKEN__``
            # for /api/ws auth (apps/desktop/electron/dashboard-token.ts).
            # When headless serve 404'd every path, a renderer whose spawn
            # token no longer matched the backend's live token (e.g. after
            # `hermes update` replaced the backend) had no way to adopt the
            # served token — the WS handshake failed and the window
            # white-screened (#95575). Serve a minimal token-only page at the
            # exact root, but ONLY when the dashboard auth gate is off: on a
            # gated (non-loopback/remote) serve the token must never be
            # readable without auth, so the 404 JSON stays.
            gated = bool(getattr(application.state, "auth_required", False))
            if full_path == "" and not gated:
                token_js = json.dumps(_SESSION_TOKEN)
                return HTMLResponse(
                    "<!doctype html><html><head><script>"
                    f"window.__HERMES_SESSION_TOKEN__={token_js};"
                    "window.__HERMES_AUTH_REQUIRED__=false;"
                    "</script></head><body>"
                    "Headless backend (hermes serve): web UI disabled — use "
                    "`hermes dashboard` for the browser UI."
                    "</body></html>",
                    headers={
                        "Cache-Control": "no-store, no-cache, must-revalidate"
                    },
                )
            return JSONResponse({"error": _msg}, status_code=404)
        return

    # A missing WEB_DIST is deliberately NOT a mount-time terminal state
    # (#82614): a long-lived `hermes dashboard --skip-build` process that
    # survives a `git pull` (or starts before the first build) used to
    # install a permanent no_frontend catch-all here and could never
    # recover — every route answered 404 "Frontend not built" until the
    # process was restarted, even after `npm run build` completed. The SPA
    # routes below all cope with a missing dist per-request (`_serve_index`
    # returns the same 404 JSON when index.html is unreadable; the asset
    # mounts use check_dir=False and 404 on missing files), so mounting
    # them unconditionally makes the dashboard recover the moment a build
    # appears on disk — no restart needed.

    _index_path = WEB_DIST / "index.html"

    def _serve_index(prefix: str = ""):
        """Return index.html with the session token + base-path injected.

        ``prefix`` is the normalised ``X-Forwarded-Prefix`` (e.g. ``/hermes``)
        or empty string when served at root.

        When the OAuth auth gate is active (``app.state.auth_required``),
        the legacy ``_SESSION_TOKEN`` is NOT injected — the SPA reads
        identity from ``/api/auth/me`` over cookie auth instead.  The
        ``__HERMES_AUTH_REQUIRED__`` flag lets the SPA pick the right
        auth scheme for /api/pty and /api/ws (ticket vs token).
        """
        try:
            html = _index_path.read_text(encoding="utf-8")
        except OSError:
            # The dist dir existed at mount time but index.html is missing or
            # unreadable now (partial build, wiped dist, permissions). Without
            # this guard every request raises FileNotFoundError (500). Return
            # the same JSON 404 payload mount_spa uses for a fully-missing
            # dist so clients get a clear, consistent signal.
            return JSONResponse(
                {"error": "Frontend not built. Run: cd web && npm run build"},
                status_code=404,
            )
        chat_js = "true" if _DASHBOARD_EMBEDDED_CHAT_ENABLED else "false"
        gated = bool(getattr(app.state, "auth_required", False))
        gated_js = "true" if gated else "false"
        if gated:
            bootstrap_script = (
                f"<script>"
                f"window.__HERMES_DASHBOARD_EMBEDDED_CHAT__={chat_js};"
                f'window.__HERMES_BASE_PATH__="{prefix}";'
                f"window.__HERMES_AUTH_REQUIRED__={gated_js};"
                f"</script>"
            )
        else:
            bootstrap_script = (
                f'<script>window.__HERMES_SESSION_TOKEN__="{_SESSION_TOKEN}";'
                f"window.__HERMES_DASHBOARD_EMBEDDED_CHAT__={chat_js};"
                f'window.__HERMES_BASE_PATH__="{prefix}";'
                f"window.__HERMES_AUTH_REQUIRED__={gated_js};"
                f"</script>"
            )
        if prefix:
            # Rewrite absolute asset URLs baked into the Vite build so the
            # browser fetches them through the same proxy prefix.
            html = html.replace('href="/assets/', f'href="{prefix}/assets/')
            html = html.replace('src="/assets/', f'src="{prefix}/assets/')
            html = html.replace('href="/favicon.ico"', f'href="{prefix}/favicon.ico"')
            html = html.replace('href="/fonts/', f'href="{prefix}/fonts/')
            html = html.replace('href="/ds-assets/', f'href="{prefix}/ds-assets/')
            html = html.replace('src="/ds-assets/', f'src="{prefix}/ds-assets/')
        # Theme flash mitigation: when the active theme is a user theme
        # (``HERMES_HOME/dashboard-themes/<name>.yaml``), inject a minimal
        # critical-CSS block so the first paint uses the target palette.
        # Without this the SPA paints the default Hermes Teal canvas, then
        # ``ThemeProvider`` flips the CSS variables once
        # ``/api/dashboard/themes`` resolves.  Built-in themes are already
        # in the bundle's ``presets.ts`` so no shim is needed for them.
        theme_bootstrap = _render_active_theme_bootstrap_css()
        if theme_bootstrap:
            html = html.replace("</head>", f"{theme_bootstrap}</head>", 1)
        html = html.replace("</head>", f"{bootstrap_script}</head>", 1)
        return HTMLResponse(
            html,
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    # When served behind a path-prefix proxy, the built CSS contains
    # absolute ``url(/fonts/...)`` and ``url(/ds-assets/...)`` references.
    # Browsers resolve those against the document origin, which means
    # under ``/hermes`` they'd hit ``mission-control.tilos.com/fonts/...``
    # (the MC Pages app), not the Hermes backend. Intercept CSS asset
    # requests BEFORE the StaticFiles mount and rewrite the absolute paths
    # when a prefix is in play.
    @application.get("/assets/{filename}.css")
    async def serve_css(filename: str, request: Request):
        css_path = WEB_DIST / "assets" / f"{filename}.css"
        if not css_path.is_file() or not css_path.resolve().is_relative_to(
            WEB_DIST.resolve()
        ):
            return JSONResponse({"error": "not found"}, status_code=404)
        prefix = _normalise_prefix(request.headers.get("x-forwarded-prefix"))
        css = css_path.read_text(encoding="utf-8")
        if prefix:
            for asset_dir in ("/fonts/", "/fonts-terminal/", "/ds-assets/", "/assets/"):
                css = css.replace(f"url({asset_dir}", f"url({prefix}{asset_dir}")
                css = css.replace(f"url(\"{asset_dir}", f"url(\"{prefix}{asset_dir}")
                css = css.replace(f"url('{asset_dir}", f"url('{prefix}{asset_dir}")
        return Response(
            content=css,
            media_type="text/css",
            headers={"Cache-Control": _IMMUTABLE_ASSET_CACHE_CONTROL},
        )

    class _ImmutableAssetFiles(StaticFiles):
        """StaticFiles that marks hashed bundle assets immutable.

        Everything under ``/assets/`` carries a Vite content hash in its
        filename, so a given URL's bytes can never change — a rebuild
        produces a NEW filename referenced by a fresh (``no-store``)
        index.html. Without this header every dashboard load re-validated
        each chunk; with it the browser serves reloads straight from its
        HTTP cache.
        """

        async def get_response(self, path: str, scope):
            response = await super().get_response(path, scope)
            if response.status_code == 200:
                response.headers["Cache-Control"] = _IMMUTABLE_ASSET_CACHE_CONTROL
            return response

    application.mount(
        "/assets",
        # check_dir=False: the dist (and its assets/ dir) may not exist yet —
        # the whole point of the dynamic recheck (#82614). StaticFiles then
        # 404s per-request until a build appears instead of raising at mount.
        _ImmutableAssetFiles(directory=WEB_DIST / "assets", check_dir=False),
        name="assets",
    )

    @application.get("/{full_path:path}")
    async def serve_spa(full_path: str, request: Request):
        prefix = _normalise_prefix(request.headers.get("x-forwarded-prefix"))
        # An unmatched /api/* path is a missing/renamed endpoint, NOT a
        # client-side route. Falling through to index.html here returns
        # `<!doctype html>` with status 200, which makes JSON clients (the
        # desktop app's fetchJson, dashboard fetch wrappers) blow up with an
        # opaque `SyntaxError: Unexpected token '<'`. Return a real 404 JSON
        # so the caller sees a clear "no such endpoint" instead.
        if full_path == "api" or full_path.startswith("api/"):
            return JSONResponse(
                {"detail": f"No such API endpoint: /{full_path}"},
                status_code=404,
            )
        file_path = WEB_DIST / full_path
        # Prevent path traversal via url-encoded sequences (%2e%2e/)
        if (
            full_path
            and file_path.resolve().is_relative_to(WEB_DIST.resolve())
            and file_path.exists()
            and file_path.is_file()
        ):
            return FileResponse(file_path)
        return _serve_index(prefix)


# ---------------------------------------------------------------------------
# Dashboard theme endpoints
# ---------------------------------------------------------------------------

# Built-in dashboard themes — label + description only.  The actual color
# definitions live in the frontend (web/src/themes/presets.ts).
_BUILTIN_DASHBOARD_THEMES = [
    {"name": "default",       "label": "Hermes Teal",         "description": "Classic dark teal — the canonical Hermes look"},
    {"name": "default-large", "label": "Hermes Teal (Large)", "description": "Hermes Teal with bigger fonts and roomier spacing"},
    {"name": "nous-blue",     "label": "Nous Blue",           "description": "Light mode — vivid Nous-blue accents on cream canvas"},
    {"name": "midnight",      "label": "Midnight",            "description": "Deep blue-violet with cool accents"},
    {"name": "ember",     "label": "Ember",          "description": "Warm crimson and bronze — forge vibes"},
    {"name": "mono",      "label": "Mono",           "description": "Clean grayscale — minimal and focused"},
    {"name": "cyberpunk", "label": "Cyberpunk",      "description": "Neon green on black — matrix terminal"},
    {"name": "rose",      "label": "Rosé",           "description": "Soft pink and warm ivory — easy on the eyes"},
]


def _parse_theme_layer(value: Any, default_hex: str, default_alpha: float = 1.0) -> Optional[Dict[str, Any]]:
    """Normalise a theme layer spec from YAML into `{hex, alpha}` form.

    Accepts shorthand (a bare hex string) or full dict form.  Returns
    ``None`` on garbage input so the caller can fall back to a built-in
    default rather than blowing up.
    """
    if value is None:
        return {"hex": default_hex, "alpha": default_alpha}
    if isinstance(value, str):
        return {"hex": value, "alpha": default_alpha}
    if isinstance(value, dict):
        hex_val = value.get("hex", default_hex)
        alpha_val = value.get("alpha", default_alpha)
        if not isinstance(hex_val, str):
            return None
        try:
            alpha_f = float(alpha_val)
        except (TypeError, ValueError):
            alpha_f = default_alpha
        return {"hex": hex_val, "alpha": max(0.0, min(1.0, alpha_f))}
    return None


_THEME_DEFAULT_TYPOGRAPHY: Dict[str, str] = {
    "fontSans": 'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
    "fontMono": 'ui-monospace, "SF Mono", "Cascadia Mono", Menlo, Consolas, monospace',
    "baseSize": "15px",
    "lineHeight": "1.55",
    "letterSpacing": "0",
}

_THEME_DEFAULT_LAYOUT: Dict[str, str] = {
    "radius": "0.5rem",
    "density": "comfortable",
}

_THEME_OVERRIDE_KEYS = {
    "card", "cardForeground", "popover", "popoverForeground",
    "primary", "primaryForeground", "secondary", "secondaryForeground",
    "muted", "mutedForeground", "accent", "accentForeground",
    "destructive", "destructiveForeground", "success", "warning",
    "border", "input", "ring",
}

# Well-known named asset slots themes can populate.  Any other keys under
# ``assets.custom`` are exposed as ``--theme-asset-custom-<key>`` CSS vars
# for plugin/shell use.
_THEME_NAMED_ASSET_KEYS = {"bg", "hero", "logo", "crest", "sidebar", "header"}

# Component-style buckets themes can override.  The value under each bucket
# is a mapping from camelCase property name to CSS string; each pair emits
# ``--component-<bucket>-<kebab-property>`` on :root.  The frontend's shell
# components (Card, App header, Backdrop, etc.) consume these vars so themes
# can restyle chrome (clip-path, border-image, segmented progress, etc.)
# without shipping their own CSS.
_THEME_COMPONENT_BUCKETS = {
    "card", "header", "footer", "sidebar", "tab",
    "progress", "badge", "backdrop", "page",
}

_THEME_LAYOUT_VARIANTS = {"standard", "cockpit", "tiled"}

# Cap on customCSS length so a malformed/oversized theme YAML can't blow up
# the response payload or the <style> tag.  32 KiB is plenty for every
# practical reskin (the Strike Freedom demo is ~2 KiB).
_THEME_CUSTOM_CSS_MAX = 32 * 1024


def _normalise_theme_definition(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalise a user theme YAML into the wire format `ThemeProvider`
    expects.  Returns ``None`` if the theme is unusable.

    Accepts both the full schema (palette/typography/layout) and a loose
    form with bare hex strings, so hand-written YAMLs stay friendly.
    """
    if not isinstance(data, dict):
        return None
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        return None

    # Palette
    palette_src = data.get("palette", {}) if isinstance(data.get("palette"), dict) else {}
    # Allow top-level `colors.background` as a shorthand too.
    colors_src = data.get("colors", {}) if isinstance(data.get("colors"), dict) else {}

    def _layer(key: str, default_hex: str, default_alpha: float = 1.0) -> Dict[str, Any]:
        spec = palette_src.get(key, colors_src.get(key))
        parsed = _parse_theme_layer(spec, default_hex, default_alpha)
        return parsed if parsed is not None else {"hex": default_hex, "alpha": default_alpha}

    palette = {
        "background": _layer("background", "#041c1c", 1.0),
        "midground": _layer("midground", "#ffe6cb", 1.0),
        "foreground": _layer("foreground", "#ffffff", 0.0),
        "warmGlow": palette_src.get("warmGlow") or data.get("warmGlow") or "rgba(255, 189, 56, 0.35)",
        "noiseOpacity": 1.0,
    }
    raw_noise = palette_src.get("noiseOpacity", data.get("noiseOpacity"))
    try:
        palette["noiseOpacity"] = float(raw_noise) if raw_noise is not None else 1.0
    except (TypeError, ValueError):
        palette["noiseOpacity"] = 1.0

    # Typography
    typo_src = data.get("typography", {}) if isinstance(data.get("typography"), dict) else {}
    typography = dict(_THEME_DEFAULT_TYPOGRAPHY)
    for key in ("fontSans", "fontMono", "fontDisplay", "fontUrl", "baseSize", "lineHeight", "letterSpacing"):
        val = typo_src.get(key)
        if isinstance(val, str) and val.strip():
            typography[key] = val

    # Layout
    layout_src = data.get("layout", {}) if isinstance(data.get("layout"), dict) else {}
    layout = dict(_THEME_DEFAULT_LAYOUT)
    radius = layout_src.get("radius")
    if isinstance(radius, str) and radius.strip():
        layout["radius"] = radius
    density = layout_src.get("density")
    if isinstance(density, str) and density in {"compact", "comfortable", "spacious"}:
        layout["density"] = density

    # Color overrides — keep only valid keys with string values.
    overrides_src = data.get("colorOverrides", {})
    color_overrides: Dict[str, str] = {}
    if isinstance(overrides_src, dict):
        for key, val in overrides_src.items():
            if key in _THEME_OVERRIDE_KEYS and isinstance(val, str) and val.strip():
                color_overrides[key] = val

    # Assets — named slots + arbitrary user-defined keys.  Values must be
    # strings (URLs or CSS ``url(...)``/``linear-gradient(...)`` expressions).
    # We don't fetch remote assets here; the frontend just injects them as
    # CSS vars.  Empty values are dropped so a theme can explicitly clear a
    # slot by setting ``hero: ""``.
    assets_out: Dict[str, Any] = {}
    assets_src = data.get("assets", {}) if isinstance(data.get("assets"), dict) else {}
    for key in _THEME_NAMED_ASSET_KEYS:
        val = assets_src.get(key)
        if isinstance(val, str) and val.strip():
            assets_out[key] = val
    custom_assets_src = assets_src.get("custom")
    if isinstance(custom_assets_src, dict):
        custom_assets: Dict[str, str] = {}
        for key, val in custom_assets_src.items():
            if (
                isinstance(key, str)
                and key.replace("-", "").replace("_", "").isalnum()
                and isinstance(val, str)
                and val.strip()
            ):
                custom_assets[key] = val
        if custom_assets:
            assets_out["custom"] = custom_assets

    # Custom CSS — raw CSS text the frontend injects as a scoped <style>
    # tag on theme apply.  Clipped to _THEME_CUSTOM_CSS_MAX to keep the
    # payload bounded.  We intentionally do NOT parse/sanitise the CSS
    # here — the dashboard is localhost-only and themes are user-authored
    # YAML in ~/.hermes/, same trust level as the config file itself.
    custom_css_val = data.get("customCSS")
    custom_css: Optional[str] = None
    if isinstance(custom_css_val, str) and custom_css_val.strip():
        custom_css = custom_css_val[:_THEME_CUSTOM_CSS_MAX]

    # Component style overrides — per-bucket dicts of camelCase CSS
    # property -> CSS string.  The frontend converts these into CSS vars
    # that shell components (Card, App header, Backdrop) consume.
    component_styles_src = data.get("componentStyles", {})
    component_styles: Dict[str, Dict[str, str]] = {}
    if isinstance(component_styles_src, dict):
        for bucket, props in component_styles_src.items():
            if bucket not in _THEME_COMPONENT_BUCKETS or not isinstance(props, dict):
                continue
            clean: Dict[str, str] = {}
            for prop, value in props.items():
                if (
                    isinstance(prop, str)
                    and prop.replace("-", "").replace("_", "").isalnum()
                    and isinstance(value, (str, int, float))
                    and str(value).strip()
                ):
                    clean[prop] = str(value)
            if clean:
                component_styles[bucket] = clean

    layout_variant_src = data.get("layoutVariant")
    layout_variant = (
        layout_variant_src
        if isinstance(layout_variant_src, str) and layout_variant_src in _THEME_LAYOUT_VARIANTS
        else "standard"
    )

    result: Dict[str, Any] = {
        "name": name,
        "label": data.get("label") or name,
        "description": data.get("description", ""),
        "palette": palette,
        "typography": typography,
        "layout": layout,
        "layoutVariant": layout_variant,
    }
    if color_overrides:
        result["colorOverrides"] = color_overrides
    if assets_out:
        result["assets"] = assets_out
    if custom_css is not None:
        result["customCSS"] = custom_css
    if component_styles:
        result["componentStyles"] = component_styles
    return result


def _discover_user_themes() -> list:
    """Scan ~/.hermes/dashboard-themes/*.yaml for user-created themes.

    Returns a list of fully-normalised theme definitions ready to ship
    to the frontend, so the client can apply them without a secondary
    round-trip or a built-in stub.

    Uses the dashboard process launch home, not ``get_hermes_home()``, so a
    transient profile override from embedded chat does not hide themes that
    live under the server's own ``HERMES_HOME``.
    """
    themes_dir = get_process_hermes_home() / "dashboard-themes"
    if not themes_dir.is_dir():
        return []
    result = []
    for f in sorted(themes_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        normalised = _normalise_theme_definition(data)
        if normalised is not None:
            result.append(normalised)
    return result


@app.get("/api/dashboard/themes")
async def get_dashboard_themes():
    """Return available themes and the currently active one.

    Built-in entries ship name/label/description only (the frontend owns
    their full definitions in `web/src/themes/presets.ts`).  User themes
    from `~/.hermes/dashboard-themes/*.yaml` ship with their full
    normalised definition under `definition`, so the client can apply
    them without a stub.
    """
    def _run():
        config = load_config()
        active = cfg_get(config, "dashboard", "theme", default="default")
        user_themes = _discover_user_themes()
        seen = set()
        themes = []
        for t in _BUILTIN_DASHBOARD_THEMES:
            seen.add(t["name"])
            themes.append(t)
        for t in user_themes:
            if t["name"] in seen:
                continue
            themes.append({
                "name": t["name"],
                "label": t["label"],
                "description": t["description"],
                "definition": t,
            })
            seen.add(t["name"])
        return {"themes": themes, "active": active}

    return await asyncio.to_thread(_run)


@app.put("/api/dashboard/theme")
async def set_dashboard_theme(body: ThemeSetBody):
    """Set the active dashboard theme (persists to config.yaml)."""
    def _run():
        with _CONFIG_MUTATION_LOCK:
            config = load_config()
            if "dashboard" not in config:
                config["dashboard"] = {}
            config["dashboard"]["theme"] = body.name
            save_config(config)
        return {"ok": True, "theme": body.name}

    return await asyncio.to_thread(_run)


# Curated font-override ids. Kept in sync with FONT_CHOICES in
# web/src/themes/fonts.ts — the frontend owns the stacks + webfont URLs;
# the backend only needs the id allow-list so it can reject anything not
# in the vetted catalog (the font's webfont URL is injected as a <link>,
# so we never accept an arbitrary user-supplied id/URL here).
_FONT_DEFAULT_ID = "theme"
_FONT_CHOICES = frozenset({
    "system-sans", "system-serif", "system-mono",
    "inter", "ibm-plex-sans", "work-sans", "atkinson-hyperlegible", "dm-sans",
    "spectral", "fraunces", "source-serif",
    "jetbrains-mono", "ibm-plex-mono", "space-mono",
})


@app.get("/api/dashboard/font")
async def get_dashboard_font():
    """Return the active font override (``"theme"`` = use the theme's font)."""
    def _run():
        config = load_config()
        font = cfg_get(config, "dashboard", "font", default=_FONT_DEFAULT_ID)
        if font not in _FONT_CHOICES:
            font = _FONT_DEFAULT_ID
        return {"font": font}

    return await asyncio.to_thread(_run)


@app.put("/api/dashboard/font")
async def set_dashboard_font(body: FontSetBody):
    """Set the dashboard font override (persists to config.yaml).

    Accepts any id in the curated catalog, or ``"theme"`` to clear the
    override and fall back to the active theme's own font. Unknown ids are
    coerced to ``"theme"`` rather than 400'd so a stale client can't wedge
    the picker.
    """
    font = body.font if body.font in _FONT_CHOICES else _FONT_DEFAULT_ID

    def _run():
        with _CONFIG_MUTATION_LOCK:
            config = load_config()
            if "dashboard" not in config:
                config["dashboard"] = {}
            config["dashboard"]["font"] = font
            save_config(config)
        return {"ok": True, "font": font}

    return await asyncio.to_thread(_run)


# ---------------------------------------------------------------------------
# Dashboard plugin system
# ---------------------------------------------------------------------------

def _safe_plugin_api_relpath(api_field: Any, *, dashboard_dir: Path) -> Optional[str]:
    """Validate the manifest's ``api`` field for the plugin loader.

    The web server later imports this file as a Python module via
    ``importlib.util.spec_from_file_location`` (arbitrary code
    execution by design — that's how plugins extend the backend).
    Pre-#29156 the field was used as-is, which meant:

    * An absolute path swallowed the plugin's dashboard directory
      entirely — ``Path('safe/dashboard') / '/tmp/evil.py'`` resolves
      to ``/tmp/evil.py``, so any attacker-controlled manifest could
      point the import at any Python file on disk (GHSA-5qr3-c538-wm9j).
    * A ``../..`` traversal could climb out of the plugin into
      neighbouring directories on the search path.

    Return the original string when the resolved path stays under
    ``dashboard_dir``; return ``None`` (with a warning logged at the
    call site) otherwise so the plugin still loads its static JS/CSS
    but its backend ``api`` is rejected.
    """
    if not isinstance(api_field, str) or not api_field.strip():
        return None
    candidate = Path(api_field)
    if candidate.is_absolute():
        return None
    try:
        resolved = (dashboard_dir / candidate).resolve()
        base = dashboard_dir.resolve()
    except (OSError, RuntimeError):
        return None
    try:
        resolved.relative_to(base)
    except ValueError:
        return None
    return api_field


def _discover_dashboard_plugins() -> list:
    """Scan plugins/*/dashboard/manifest.json for dashboard extensions.

    Checks three plugin sources (same as hermes_cli.plugins):
    1. User plugins:    ~/.hermes/plugins/<name>/dashboard/manifest.json
    2. Bundled plugins: <repo>/plugins/<name>/dashboard/manifest.json  (memory/, etc.)
    3. Project plugins: ./.hermes/plugins/  (only if HERMES_ENABLE_PROJECT_PLUGINS)
    """
    plugins = []
    seen_names: set = set()

    from hermes_cli.plugins import get_bundled_plugins_dir
    bundled_root = get_bundled_plugins_dir()
    # User dashboard plugins are a dashboard-owned asset (same category as
    # theme YAML): resolve them from the process launch home so they don't
    # vanish when a request is scoped to another profile via a context-local
    # HERMES_HOME override (e.g. embedded /chat under --open-profile).
    #
    # #87197: when the process itself is profile-scoped (``--profile <name>``
    # sets ``HERMES_HOME=<root>/profiles/<name>``), the launch home is the
    # profile directory, which has no ``plugins/`` — user plugins are
    # installed in the hermes root (``~/.hermes/plugins``). Scan the default
    # root as well (``get_default_hermes_root()`` unwraps
    # ``<root>/profiles/<name>`` → ``<root>`` and returns a custom
    # ``HERMES_HOME`` unchanged when it *is* the root), mirroring how
    # ``hermes_cli.plugins`` resolves plugin install locations. The
    # ``seen_names`` dedupe below keeps profile-local plugins (if any)
    # authoritative over same-named root plugins.
    from hermes_constants import get_default_hermes_root

    user_plugin_roots = [get_process_hermes_home() / "plugins"]
    root_plugins = get_default_hermes_root() / "plugins"
    if root_plugins.resolve(strict=False) != user_plugin_roots[0].resolve(strict=False):
        user_plugin_roots.append(root_plugins)
    search_dirs = [(d, "user") for d in user_plugin_roots]
    search_dirs += [
        (bundled_root / "memory", "bundled"),
        (bundled_root, "bundled"),
    ]
    # GHSA-5qr3-c538-wm9j (#29156): the previous ``os.environ.get(...)``
    # check treated *any* non-empty string as truthy, so ``=0``, ``=false``,
    # and ``=no`` — all of which the agent loader and operators correctly
    # read as "disabled" — silently *enabled* the untrusted project source
    # in the web server.  Combined with the absolute-path RCE primitive on
    # the manifest's ``api`` field (now patched below), this turned the
    # opt-in into a sticky always-on switch.  Use the shared truthy
    # semantics (``1`` / ``true`` / ``yes`` / ``on``) so the gate matches
    # ``hermes_cli/plugins.py`` and the documented user contract.
    if env_var_enabled("HERMES_ENABLE_PROJECT_PLUGINS"):
        search_dirs.append((Path.cwd() / ".hermes" / "plugins", "project"))

    for plugins_root, source in search_dirs:
        if not plugins_root.is_dir():
            continue
        with os.scandir(plugins_root) as scan:
            children = sorted((Path(e.path) for e in scan), key=lambda p: p.name)
        for child in children:
            if not child.is_dir():
                continue
            manifest_file = child / "dashboard" / "manifest.json"
            if not manifest_file.exists():
                continue
            try:
                data = json.loads(manifest_file.read_text(encoding="utf-8"))
                name = data.get("name", child.name)
                if name in seen_names:
                    continue
                seen_names.add(name)
                # Tab options: ``path`` + ``position`` for a new tab, optional
                # ``override`` to replace a built-in route, and ``hidden`` to
                # register the plugin component/slots without adding a tab
                # (useful for slot-only plugins like a header-crest injector).
                raw_tab = data.get("tab", {}) if isinstance(data.get("tab"), dict) else {}
                tab_info = {
                    "path": raw_tab.get("path", f"/{name}"),
                    "position": raw_tab.get("position", "end"),
                }
                override_path = raw_tab.get("override")
                if isinstance(override_path, str) and override_path.startswith("/"):
                    tab_info["override"] = override_path
                if bool(raw_tab.get("hidden")):
                    tab_info["hidden"] = True
                # Slots: list of named slot locations this plugin populates.
                # The frontend exposes ``registerSlot(pluginName, slotName, Component)``
                # on window; plugins with non-empty slots call it from their JS bundle.
                slots_src = data.get("slots")
                slots: List[str] = []
                if isinstance(slots_src, list):
                    slots = [s for s in slots_src if isinstance(s, str) and s]
                # Validate ``api`` at discovery time so the value cached
                # on the plugin entry is already safe to feed into the
                # importer.  An attacker-controlled manifest can name
                # any absolute path or ``..`` traversal here — the
                # web server then imports that file as a Python module
                # (RCE, GHSA-5qr3-c538-wm9j).
                raw_api = data.get("api")
                dashboard_dir = child / "dashboard"
                safe_api = _safe_plugin_api_relpath(raw_api, dashboard_dir=dashboard_dir)
                if raw_api and safe_api is None:
                    _log.warning(
                        "Plugin %s: refusing unsafe api path %r (must be a "
                        "relative file inside the plugin's dashboard/ "
                        "directory); backend routes from this plugin will "
                        "not be mounted",
                        name, raw_api,
                    )
                plugins.append({
                    "name": name,
                    "label": data.get("label", name),
                    "description": data.get("description", ""),
                    "icon": data.get("icon", "Puzzle"),
                    "version": data.get("version", "0.0.0"),
                    "tab": tab_info,
                    "slots": slots,
                    "entry": data.get("entry", "dist/index.js"),
                    "css": data.get("css"),
                    "has_api": bool(safe_api),
                    "source": source,
                    "_dir": str(dashboard_dir),
                    "_api_file": safe_api,
                })
            except Exception as exc:
                _log.warning("Bad dashboard plugin manifest %s: %s", manifest_file, exc)
                continue
    return plugins


# Cache discovered plugins per-process (refresh on explicit re-scan).
_dashboard_plugins_cache: Optional[list] = None


def _get_dashboard_plugins(force_rescan: bool = False) -> list:
    global _dashboard_plugins_cache
    stale = _dashboard_plugins_cache is None or force_rescan or any(
        not Path(p["_dir"]).is_dir() for p in _dashboard_plugins_cache
    )
    if stale:
        _dashboard_plugins_cache = _discover_dashboard_plugins()
    return _dashboard_plugins_cache


# Router mounting. ORDER IS ROUTE-MATCHING ORDER: literal paths must land before
# templated siblings (e.g. /api/sessions/bulk-delete before /api/sessions/{id}).
from hermes_cli.web_routers import (  # noqa: E402
    files as _files_routes,
    git as _git_routes,
    local_models as _local_models_routes,
    status as _status_routes,
    actions as _actions_routes,
    audio as _audio_routes,
    sessions as _sessions_routes,
    profiles as _profiles_routes,
    memory_providers as _memory_providers_routes,
    config_env as _config_env_routes,
    models as _models_routes,
    messaging as _messaging_routes,
    oauth as _oauth_routes,
    cron as _cron_routes,
    mcp as _mcp_routes,
    ops as _ops_routes,
    skills as _skills_routes,
    tools as _tools_routes,
    analytics as _analytics_routes,
    chat_ws as _chat_ws_routes,
    dashboard_ui as _dashboard_ui_routes,
)

app.include_router(_files_routes.router)
app.include_router(_git_routes.router)
app.include_router(_local_models_routes.router)
app.include_router(_status_routes.router)
app.include_router(_actions_routes.router)
app.include_router(_audio_routes.router)
app.include_router(_actions_routes.status_router)
app.include_router(_sessions_routes.list_router)
app.include_router(_profiles_routes.sessions_router)
app.include_router(_sessions_routes.search_router)
app.include_router(_memory_providers_routes.router)
app.include_router(_config_env_routes.config_router)
app.include_router(_models_routes.router)
app.include_router(_config_env_routes.router)
app.include_router(_messaging_routes.router)
app.include_router(_oauth_routes.router)
app.include_router(_sessions_routes.manage_router)
app.include_router(_status_routes.logs_router)
app.include_router(_cron_routes.router)
app.include_router(_mcp_routes.router)
app.include_router(_ops_routes.router)
app.include_router(_skills_routes.hub_router)
app.include_router(_profiles_routes.router)
app.include_router(_skills_routes.router)
app.include_router(_tools_routes.router)
app.include_router(_analytics_routes.router)
app.include_router(_chat_ws_routes.router)
app.include_router(_dashboard_ui_routes.router)

# Plugin API routes and the dashboard auth routes (/login, /auth/*, /api/auth/*)
# mount before the SPA catch-all so /{full_path:path} doesn't swallow them. Auth
# routes are always mounted — the gate middleware decides enforcement.
_mount_plugin_api_routes()
from hermes_cli.dashboard_auth.routes import router as _dashboard_auth_router  # noqa: E402

app.include_router(_dashboard_auth_router)
mount_spa(app)


def _no_auth_provider_message(host: str) -> str:
    """Actionable SystemExit text for a gated bind with no registered auth provider.

    Names the exact trigger: on a loopback bind the ONLY trigger is
    dashboard.public_url, so print the offending URL and the remove-it exit.
    Bundled providers expose ``LAST_SKIP_REASON`` so an installed-but-
    unconfigured provider is not reported as merely "no providers".
    """
    skip_reasons: list[str] = []
    try:
        from plugins.dashboard_auth import nous as _nous_plugin

        if _nous_plugin.LAST_SKIP_REASON:
            skip_reasons.append(f"  • nous: {_nous_plugin.LAST_SKIP_REASON}")
    except Exception:
        pass

    if host in _LOOPBACK_HOST_VALUES:
        public_url = ""
        try:
            from hermes_cli.dashboard_auth.prefix import resolve_public_url

            public_url = resolve_public_url()
        except Exception:
            pass
        gate_reason = (
            f"dashboard.public_url is set to "
            f"{public_url or '<a non-loopback URL>'} — an "
            f"operator-declared external URL engages the auth gate "
            f"even on a loopback bind"
        )
        fix_hint = (
            "If this dashboard should be LOCAL-ONLY (no reverse "
            "proxy), remove dashboard.public_url from config.yaml "
            "(and unset HERMES_DASHBOARD_PUBLIC_URL) to restore the "
            "unauthenticated loopback mode.\n"
        )
    else:
        gate_reason = f"the auth gate engages on non-loopback binds ({host})"
        fix_hint = ""

    fix_hint += (
        "Configure an auth provider before exposing the dashboard:\n"
        "  • Password: set dashboard.basic_auth.username + "
        "password_hash in config.yaml\n"
        "    (hash with: python -c \"from "
        "plugins.dashboard_auth.basic import hash_password; "
        "print(hash_password('your-password'))\")\n"
        "  • OAuth: run `hermes dashboard register` (Nous Portal) or "
        "install a DashboardAuthProvider plugin.\n"
        "There is no unauthenticated public-dashboard option. For "
        "local-only use, bind 127.0.0.1 and leave dashboard.public_url "
        "unset; a configured external public URL requires auth even "
        "when a local reverse proxy reaches a loopback backend."
    )
    # Credentials exist but the bundled provider is disabled (#54489). Basic
    # auth needs a username AND a credential; a half-configured block is silent.
    try:
        from hermes_cli.config import load_config as _load_cfg
        from hermes_cli.plugins_cmd import _BASIC_AUTH_PLUGIN_KEYS

        cfg = _load_cfg()
        ba = (cfg.get("dashboard") or {}).get("basic_auth") or {}
        disabled = (cfg.get("plugins") or {}).get("disabled") or []
        has_creds = bool(ba.get("username")) and bool(ba.get("password_hash") or ba.get("password"))
        if has_creds and (set(disabled) & _BASIC_AUTH_PLUGIN_KEYS):
            fix_hint = (
                "The 'basic' dashboard-auth plugin is in "
                "plugins.disabled but dashboard.basic_auth is "
                "configured.\n"
                "Remove 'basic' from plugins.disabled (or run "
                "`hermes plugins enable basic`), then restart the "
                "dashboard.\n\n"
            ) + fix_hint
    except Exception:
        pass
    msg = (
        f"Refusing to bind dashboard to {host} — {gate_reason}, "
        f"but no auth providers are registered.\n\n"
    )
    if skip_reasons:
        msg += "Bundled providers reported these issues:\n" + "\n".join(skip_reasons) + "\n\n"
    return msg + fix_hint


def _configure_auth_gate(
    host: str,
    allow_public: bool,
    ssh_session_token: Optional[str],
    ssh_owner_nonce: Optional[str],
) -> None:
    """Resolve the trusted public hosts + auth-gate flag onto ``app.state``.

    Fails closed (``SystemExit`` with an actionable message) when the gate
    engages but no dashboard auth provider is registered.
    """
    # dashboard.public_url is also the exact Host/Origin trust declaration for
    # reverse-proxy deployments; resolved once so middleware never reloads
    # config. A non-loopback public hostname engages the gate even on a loopback
    # backend, else the SPA's local session token becomes remotely reachable.
    app.state.trusted_public_hosts = _dashboard_public_hosts()
    # auth_required drives middleware, SPA-token injection, WS auth, the
    # startup refusal, the gate-on banner and uvicorn proxy_headers.
    if _desktop_loopback_auth_exempt(host, ssh_session_token, ssh_owner_nonce):
        # public_url describes the operator's PUBLIC deployment, not this
        # Desktop-owned loopback backend (#96490), which authenticates with the
        # per-spawn session token the ticket-only gate would refuse.
        app.state.auth_required = should_require_auth(host)
        _log.info(
            "Desktop-owned loopback backend: dashboard.public_url does not "
            "engage the ticket gate for this process; the public deployment "
            "keeps its own gate.",
        )
    else:
        app.state.auth_required = should_require_dashboard_auth(host, app.state.trusted_public_hosts)

    # ``--insecure`` no longer disables the gate (June 2026 hermes-0day
    # hardening); warn that it is a no-op rather than silently ignore it.
    if allow_public and host not in _LOOPBACK_HOST_VALUES:
        _log.warning(
            "--insecure no longer bypasses dashboard authentication. A "
            "non-loopback bind (%s) now ALWAYS requires an auth provider "
            "(OAuth or the bundled password provider). Configure one — see "
            "below — or bind to 127.0.0.1 and reach it over an SSH tunnel / "
            "Tailscale.", host,
        )

    if app.state.auth_required:
        # No escape hatch serves a gated dashboard without a provider.
        from hermes_cli.dashboard_auth import list_providers
        if not list_providers():
            raise SystemExit(_no_auth_provider_message(host))
        _log.info(
            "Dashboard binding to %s with auth gate enabled. Providers: %s",
            host,
            ", ".join(p.name for p in list_providers()),
        )


def _build_uvicorn_server(host: str, port: int, *, ssh_isolated: bool = False):
    """Build the uvicorn ``Config`` + ``Server`` for this bind (reads ``app.state.auth_required``).

    uvicorn.Server is driven directly (not uvicorn.run) so startup is split from
    the main loop: after startup() the socket is bound and held by uvicorn, so the
    OS-assigned port can be read with no pre-bind-then-close TOCTOU. Explicit
    taken ports are caught by the #93608 preflight probe; uvicorn's own bind
    error stays the fallback for races.
    """
    import uvicorn

    # WS keepalive ping runs ON the agent event loop; a GIL-holding worker call
    # can starve it for minutes, so uvicorn misses the pong and drops a healthy
    # local socket (#53773/#48445/#50005). The ping only detects half-open
    # connections (proxy 524, dropped tunnels), impossible on loopback where a
    # dead client sends a real FIN/RST -> WebSocketDisconnect. So: no ping on
    # loopback; non-loopback sits behind a Cloudflare Tunnel (~100s idle) and
    # keeps a config-driven cadence (dashboard.ws_ping_interval/_timeout,
    # #79635) defaulting to 20/20.
    _is_loopback = host in _LOOPBACK_HOST_VALUES
    try:
        _dash_cfg = load_config().get("dashboard") or {}
    except Exception:
        _dash_cfg = {}

    def _ws_ping_setting(key: str, default: float = 20.0) -> float:
        try:
            return float(_dash_cfg.get(key, default))
        except (TypeError, ValueError):
            return default

    # A Desktop-owned SSH-isolated backend is loopback on the SERVER, but the client sits at the far
    # end of a tunnel: the local socket stays healthy while the laptop sleeps, so only a slow WS ping
    # notices the half-open tunnel (#101626). Its client count is tracked at the ASGI boundary so
    # the idle watchdog can retire the backend once nothing is connected.
    served_app = app
    ping_interval, ping_timeout = (None, None) if _is_loopback else (
        _ws_ping_setting("ws_ping_interval"), _ws_ping_setting("ws_ping_timeout"))
    if ssh_isolated:
        from hermes_cli.web_server_idle_exit import (
            TUNNEL_WS_PING_INTERVAL_S, TUNNEL_WS_PING_TIMEOUT_S, IdleClientTracker, wrap_asgi_with_ws_tracking)
        app.state.ssh_isolated_clients = IdleClientTracker()
        served_app = wrap_asgi_with_ws_tracking(app, app.state.ssh_isolated_clients)
        ping_interval, ping_timeout = TUNNEL_WS_PING_INTERVAL_S, TUNNEL_WS_PING_TIMEOUT_S

    config = uvicorn.Config(
        served_app, host=host, port=port, log_level="warning",
        # Off by default so _ws_client_is_allowed sees the real peer, not
        # X-Forwarded-For. Gated mode runs behind a TLS terminator and needs
        # X-Forwarded-Proto for cookie Secure flags.
        proxy_headers=bool(app.state.auth_required),
        # Loopback-only unless the operator trusts a bounded upstream proxy, so
        # spoofed X-Forwarded-* from arbitrary callers is never honoured.
        forwarded_allow_ips=_dashboard_forwarded_allow_ips(_dash_cfg),
        ws_ping_interval=ping_interval,
        ws_ping_timeout=ping_timeout,
        ws_max_size=_DESKTOP_ATTACHMENT_WS_MAX_BYTES,
    )
    return config, uvicorn.Server(config)


def _best_effort(what: str, fn) -> None:
    """Run a best-effort startup step; any failure (import included) is a debug line."""
    try:
        fn()
    except Exception as exc:
        _log.debug("%s skipped: %s", what, exc)


def _on_server_started(
    server,
    *,
    host: str,
    port: int,
    headless: bool,
    open_browser: bool,
    initial_profile: str,
    start_mcp_discovery_after_bind: bool,
) -> None:
    """Post-bind arming on the serving loop right after ``server.startup()``.

    Reap prior corpses, parent-death watchdog, process identity, READY
    announcement, browser open, deferred MCP discovery, loop-noise filter,
    loop heartbeat.
    """
    # Clear corpses from a previous unclean Desktop exit (crash/SIGKILL/update
    # handoff leaves an orphaned backend + its MCP subtree) before stacking a
    # new tree (EMFILE / missing tabs). The watchdog only protects *this*
    # process going forward.
    def _reap_desktop_serves() -> None:
        from hermes_cli.dashboard_procs import _reap_orphaned_desktop_local_serves

        _reap_orphaned_desktop_local_serves()

    def _reap_mcp_helpers() -> None:
        from hermes_cli.process_identity import reap_orphaned_mcp_helpers

        reap_orphaned_mcp_helpers()

    if os.getenv("HERMES_DESKTOP") == "1":
        _best_effort("orphan desktop-local serve reap", _reap_desktop_serves)
    # Same sweep for stdio MCP helpers (#61514): positive identity only (spawn
    # ledger + spawner provably dead); anything alive or unprovable is untouched.
    _best_effort("orphan MCP helper reap", _reap_mcp_helpers)

    # No-op for standalone `hermes serve` (no HERMES_PARENT_PID).
    _start_parent_death_watchdog()
    # SSH-isolated backends are detached from any parent on purpose (#91668); their liveness signal
    # is "does a client still hold a WebSocket" (#101626).
    if getattr(app.state, "ssh_isolated_clients", None) is not None:
        from hermes_cli.web_server_idle_exit import DEFAULT_IDLE_GRACE_S, start_idle_watchdog
        try:
            grace = float((load_config().get("dashboard") or {}).get("ssh_isolated_idle_grace_s", DEFAULT_IDLE_GRACE_S))
        except (TypeError, ValueError):
            grace = DEFAULT_IDLE_GRACE_S
        start_idle_watchdog(server, app.state.ssh_isolated_clients, grace_s=grace)

    actual_port = _read_bound_port(server, fallback=port)
    app.state.bound_port = actual_port

    # Positive process identity in the machine spawn ledger (+ Windows
    # kill-on-close job). Registered AFTER the bind so the entry carries the
    # ACTUAL port — what lets `hermes update` relaunch a manually-started serve
    # on its real endpoint (#63206).
    def _register_identity() -> None:
        from hermes_cli.process_identity import attach_self_to_kill_on_close_job, register_self

        register_self(
            "serve" if headless else "dashboard",
            detail={"host": host, "port": actual_port, "profile": initial_profile or ""},
        )
        attach_self_to_kill_on_close_job()

    _best_effort("process-identity registration", _register_identity)

    _write_dashboard_ready_file(actual_port)
    # Port-discovery sentinel parsed by the Desktop spawn (matches either
    # token). Written to fd 1: tui_gateway.server redirects sys.stdout to
    # stderr at import, and the Desktop watches child.stdout (#96282).
    ready_token = "HERMES_BACKEND_READY" if headless else "HERMES_DASHBOARD_READY"
    _write_machine_sentinel_line(f"{ready_token} port={actual_port}")
    if headless:
        # Auth-gated JSON-RPC/WS only — announce the bind, not a URL. flush:
        # a piped stdout otherwise surfaces this minutes after the sentinel.
        print(f"  Hermes backend listening on {host}:{actual_port}", flush=True)
    else:
        print(f"  Hermes Web UI → http://{host}:{actual_port}")
    _maybe_open_browser(host, actual_port, open_browser, initial_profile)

    if start_mcp_discovery_after_bind:
        # Desktop `serve`: the ~350ms `mcp` SDK import holds the GIL while the
        # renderer does its WS handshake + first hydration reads, so arm it one
        # second later when the shell is painted and idle. An agent build inside
        # that second fires the deferred start itself (wait_for_mcp_discovery).
        try:
            from hermes_cli.mcp_startup import defer_background_mcp_discovery

            defer_background_mcp_discovery(
                logger=_log,
                thread_name="dashboard-mcp-discovery",
                delay=_DESKTOP_MCP_DISCOVERY_DELAY_S,
            )
        except Exception:
            _log.debug("Deferred MCP discovery arm failed", exc_info=True)

    # Collapse the peer-hangup teardown flood (#50005): 50+ identical WinError
    # 10054 tracebacks per Desktop disconnect become one debug line.
    def _install_noise_filter() -> None:
        from tui_gateway.loop_noise import install_loop_noise_filter

        install_loop_noise_filter(asyncio.get_running_loop())

    _best_effort("loop noise filter install", _install_noise_filter)

    # Loop heartbeat watchdog (CF-1): a 2s call_later tick whose drift equals
    # any GIL stall, so a stalled-loop WS drop is diagnosable from the log.
    # call_later (not a task) dies with the loop — nothing to cancel.
    _hb_interval = 2.0
    _hb_stall_threshold = 5.0
    _hb_loop = asyncio.get_running_loop()

    def _loop_heartbeat(expected: float) -> None:
        now = _hb_loop.time()
        drift = now - expected
        if drift > _hb_stall_threshold:
            _log.warning("event loop stalled %.1fs (GIL pressure suspected)", drift)
        _hb_loop.call_later(_hb_interval, _loop_heartbeat, now + _hb_interval)

    _hb_loop.call_later(_hb_interval, _loop_heartbeat, _hb_loop.time() + _hb_interval)


def _run_serve(serve, config, host: str, port: int) -> None:
    """Drive ``serve()`` on the loop uvicorn expects.

    POSIX keeps ``asyncio.run`` (already a SelectorEventLoop / uvloop). On
    Windows ``asyncio.run`` defaults to a ProactorEventLoop, on which uvicorn
    binds a socket that never accepts (#50641), so mirror uvicorn's own runner +
    loop factory there (hand-installed selector policy for uvicorn < 0.36).
    Ctrl+C -> clean return; probe-to-bind port race -> sentinel + exit code.
    """
    runner = asyncio.run
    runner_kwargs: dict = {}
    if sys.platform == "win32":
        # Resolved FIRST; the serve call is outside this try so genuine
        # serve-time errors (port in use) propagate instead of double-running.
        try:
            from uvicorn._compat import asyncio_run as runner

            runner_kwargs = {"loop_factory": config.get_loop_factory()}
        except Exception:
            runner = asyncio.run
            runner_kwargs = {}
            try:
                asyncio.set_event_loop_policy(
                    asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]
                )
            except Exception:
                pass

    # ``capture_signals()`` re-raises the captured signal after graceful
    # shutdown; console Ctrl+C lands as KeyboardInterrupt = clean exit.
    # (Re-raised SIGTERM/SIGBREAK keep their terminate disposition.)
    try:
        runner(serve(), **runner_kwargs)
    except KeyboardInterrupt:
        return
    except SystemExit as exc:
        # Probe-to-bind race (#93608): uvicorn's bind_socket() exits 1 — re-check
        # and translate a confirmed conflict into the sentinel + distinct code.
        if exc.code == 1 and _port_bind_conflict(host, port):
            _report_port_in_use(host, port)
            raise SystemExit(PORT_IN_USE_EXIT_CODE) from None
        raise


def start_server(
    host: str = "127.0.0.1",
    port: int = 9119,
    open_browser: bool = True,
    allow_public: bool = False,
    initial_profile: str = "",
    headless: bool = False,
    ssh_session_token: Optional[str] = None,
    ssh_owner_nonce: Optional[str] = None,
    start_mcp_discovery_after_bind: bool = False,
):
    """Start the web UI server.

    ``initial_profile`` is appended to the auto-opened URL as ``?profile=<name>``
    (profile alias ``<profile> dashboard``). ``headless`` is the ``serve`` path:
    JSON-RPC/WS backend, no UI build, no SPA mount (``HERMES_SERVE_HEADLESS``).
    ``ssh_session_token``/``ssh_owner_nonce`` are process-local Desktop SSH
    bootstrap state, never persisted or exported to children.
    ``start_mcp_discovery_after_bind`` (Desktop ``serve``) defers MCP discovery
    until the ready sentinel is written so its SDK import can't hold the GIL
    against the pre-bind path.
    """
    _apply_ssh_session_token(ssh_session_token or "")
    _apply_ssh_owner_nonce(ssh_owner_nonce)

    # Dashboard-mode starts don't route through main.py's `serve` path, which
    # applies the same RLIMIT_NOFILE floor (policy in resource_limits, #81547).
    from hermes_cli.resource_limits import apply_nofile_soft_limit

    apply_nofile_soft_limit()

    import uvicorn  # noqa: F401 — fail fast (before any side effects) when the dashboard extra is missing

    try:
        from hermes_cli.nous_auth_keepalive import start_nous_auth_keepalive

        start_nous_auth_keepalive()
    except Exception as exc:
        _log.debug("Nous auth keepalive did not start: %s", exc)

    _configure_auth_gate(host, allow_public, ssh_session_token, ssh_owner_nonce)

    # host_header_middleware validates Host against this (DNS rebinding,
    # GHSA-ppp5-vxwm-4cf7).
    app.state.bound_host = host

    config, server = _build_uvicorn_server(host, port, ssh_isolated=bool(ssh_session_token))

    # Flush-on-kill guard (#94724): chaining SIGTERM/SIGINT handlers persist
    # in-memory transcripts to state.db before shutdown. Installed BEFORE
    # uvicorn's capture_signals() so uvicorn re-raises into them as the
    # "original" handlers — kills outside the serve window are covered too.
    try:
        from tui_gateway.server import install_exit_flush_signal_handlers

        install_exit_flush_signal_handlers()
    except Exception as exc:
        _log.debug("exit-flush signal handlers not installed: %s", exc)

    # #93608: uvicorn's bind_socket() would exit 1 with a bare ERROR line,
    # indistinguishable from "backend broken". Probe first so a conflict
    # surfaces as the BACKEND_PORT_IN_USE sentinel + distinct exit code.
    # ``--port 0`` is skipped by the probe.
    if _port_bind_conflict(host, port):
        _report_port_in_use(host, port)
        raise SystemExit(PORT_IN_USE_EXIT_CODE)

    async def _serve():
        # startup split from main_loop so the bound (ephemeral) port is readable.
        if not config.loaded:
            config.load()
        server.lifespan = config.lifespan_class(config)
        with server.capture_signals():
            await server.startup()
            if server.should_exit:
                return

            _on_server_started(
                server,
                host=host,
                port=port,
                headless=headless,
                open_browser=open_browser,
                initial_profile=initial_profile,
                start_mcp_discovery_after_bind=start_mcp_discovery_after_bind,
            )

            await server.main_loop()
            if server.started:
                await server.shutdown()

    _run_serve(_serve, config, host, port)


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
from typing import List  # noqa: F401,E402
from typing import Literal  # noqa: F401,E402
import atexit  # noqa: F401,E402
import base64  # noqa: F401,E402
import binascii  # noqa: F401,E402
import concurrent.futures  # noqa: F401,E402
import contextlib  # noqa: F401,E402
from contextlib import contextmanager  # noqa: F401,E402
from dataclasses import dataclass  # noqa: F401,E402
from datetime import datetime  # noqa: F401,E402
import functools  # noqa: F401,E402
import hashlib  # noqa: F401,E402
import importlib.util  # noqa: F401,E402
import inspect  # noqa: F401,E402
import ipaddress  # noqa: F401,E402
import json  # noqa: F401,E402
import math  # noqa: F401,E402
import mimetypes  # noqa: F401,E402
import queue  # noqa: F401,E402
import shlex  # noqa: F401,E402
import shutil  # noqa: F401,E402
import stat  # noqa: F401,E402
import tempfile  # noqa: F401,E402
from datetime import timezone  # noqa: F401,E402
import yaml  # noqa: F401,E402
import zipfile  # noqa: F401,E402


_PLUGIN_COMPAT_LAZY = {
    'AudioTranscriptionRequest': ('hermes_cli.web_models', 'AudioTranscriptionRequest'),
    'AutomationBlueprintInstantiate': ('hermes_cli.web_models', 'AutomationBlueprintInstantiate'),
    'BackupRequest': ('hermes_cli.web_models', 'BackupRequest'),
    'BulkDeleteSessions': ('hermes_cli.web_models', 'BulkDeleteSessions'),
    'CONFIG_SCHEMA': ('hermes_cli.web_server_config', 'CONFIG_SCHEMA'),
    'ChatImageUpload': ('hermes_cli.web_models', 'ChatImageUpload'),
    'ConfigUpdate': ('hermes_cli.web_models', 'ConfigUpdate'),
    'CredentialPoolAdd': ('hermes_cli.web_models', 'CredentialPoolAdd'),
    'CronJobCreate': ('hermes_cli.web_models', 'CronJobCreate'),
    'CronJobUpdate': ('hermes_cli.web_models', 'CronJobUpdate'),
    'CuratorPause': ('hermes_cli.web_models', 'CuratorPause'),
    'CustomEndpointUpdate': ('hermes_cli.web_models', 'CustomEndpointUpdate'),
    'DEFAULT_CONFIG': ('hermes_cli.config', 'DEFAULT_CONFIG'),
    'DebugShareRequest': ('hermes_cli.web_models', 'DebugShareRequest'),
    'EnvVarDelete': ('hermes_cli.web_models', 'EnvVarDelete'),
    'EnvVarReveal': ('hermes_cli.web_models', 'EnvVarReveal'),
    'EnvVarUpdate': ('hermes_cli.web_models', 'EnvVarUpdate'),
    'FontSetBody': ('hermes_cli.web_models', 'FontSetBody'),
    'FsWriteText': ('hermes_cli.web_models', 'FsWriteText'),
    'GitBranchSwitchBody': ('hermes_cli.web_models', 'GitBranchSwitchBody'),
    'GitCommitBody': ('hermes_cli.web_models', 'GitCommitBody'),
    'GitFileBody': ('hermes_cli.web_models', 'GitFileBody'),
    'GitPathBody': ('hermes_cli.web_models', 'GitPathBody'),
    'GitWorktreeAddBody': ('hermes_cli.web_models', 'GitWorktreeAddBody'),
    'GitWorktreeRemoveBody': ('hermes_cli.web_models', 'GitWorktreeRemoveBody'),
    'HookCreate': ('hermes_cli.web_models', 'HookCreate'),
    'HookDelete': ('hermes_cli.web_models', 'HookDelete'),
    'ImportRequest': ('hermes_cli.web_models', 'ImportRequest'),
    'LearningNodeEdit': ('hermes_cli.web_models', 'LearningNodeEdit'),
    'LearningNodeRef': ('hermes_cli.web_models', 'LearningNodeRef'),
    'MCPCatalogInstall': ('hermes_cli.web_models', 'MCPCatalogInstall'),
    'MCPEnabledToggle': ('hermes_cli.web_models', 'MCPEnabledToggle'),
    'MCPServerCreate': ('hermes_cli.web_models', 'MCPServerCreate'),
    'MCPServersReplace': ('hermes_cli.web_models', 'MCPServersReplace'),
    'ManagedDirectoryCreate': ('hermes_cli.web_models', 'ManagedDirectoryCreate'),
    'ManagedFileDelete': ('hermes_cli.web_models', 'ManagedFileDelete'),
    'ManagedFileUpload': ('hermes_cli.web_models', 'ManagedFileUpload'),
    'ManagedFilesPolicy': ('hermes_cli.web_server_files', 'ManagedFilesPolicy'),
    'MemoryProviderConfigUpdate': ('hermes_cli.web_models', 'MemoryProviderConfigUpdate'),
    'MemoryProviderSelect': ('hermes_cli.web_models', 'MemoryProviderSelect'),
    'MemoryProviderSetupRequest': ('hermes_cli.web_models', 'MemoryProviderSetupRequest'),
    'MemoryReset': ('hermes_cli.web_models', 'MemoryReset'),
    'MessagingPlatformUpdate': ('hermes_cli.web_models', 'MessagingPlatformUpdate'),
    'MoaConfigPayload': ('hermes_cli.web_models', 'MoaConfigPayload'),
    'MoaModelSlot': ('hermes_cli.web_models', 'MoaModelSlot'),
    'MoaPresetPayload': ('hermes_cli.web_models', 'MoaPresetPayload'),
    'ModelAssignment': ('hermes_cli.web_models', 'ModelAssignment'),
    'OAuthSubmitBody': ('hermes_cli.web_models', 'OAuthSubmitBody'),
    'OPTIONAL_ENV_VARS': ('hermes_cli.config', 'OPTIONAL_ENV_VARS'),
    'PairingApprove': ('hermes_cli.web_models', 'PairingApprove'),
    'PairingRevoke': ('hermes_cli.web_models', 'PairingRevoke'),
    'ProfileActiveUpdate': ('hermes_cli.web_models', 'ProfileActiveUpdate'),
    'ProfileCreate': ('hermes_cli.web_models', 'ProfileCreate'),
    'ProfileDescribeAuto': ('hermes_cli.web_models', 'ProfileDescribeAuto'),
    'ProfileDescriptionUpdate': ('hermes_cli.web_models', 'ProfileDescriptionUpdate'),
    'ProfileModelUpdate': ('hermes_cli.web_models', 'ProfileModelUpdate'),
    'ProfileRename': ('hermes_cli.web_models', 'ProfileRename'),
    'ProfileSoulUpdate': ('hermes_cli.web_models', 'ProfileSoulUpdate'),
    'ProviderConfigSchema': ('plugins.memory.config_schema', 'ProviderConfigSchema'),
    'ProviderField': ('plugins.memory.config_schema', 'ProviderField'),
    'PtyBridge': ('hermes_cli.pty_bridge', 'PtyBridge'),
    'PtySessionRegistry': ('hermes_cli.pty_session', 'PtySessionRegistry'),
    'PtyUnavailableError': ('hermes_cli.pty_bridge', 'PtyUnavailableError'),
    'RawConfigUpdate': ('hermes_cli.web_models', 'RawConfigUpdate'),
    'RegistryFull': ('hermes_cli.pty_session', 'RegistryFull'),
    'STORAGE_HONCHO_HOST_BLOCK': ('plugins.memory.config_schema', 'STORAGE_HONCHO_HOST_BLOCK'),
    'SessionImport': ('hermes_cli.web_models', 'SessionImport'),
    'SessionPrune': ('hermes_cli.web_models', 'SessionPrune'),
    'SessionRename': ('hermes_cli.web_models', 'SessionRename'),
    'SkillContentUpdate': ('hermes_cli.web_models', 'SkillContentUpdate'),
    'SkillCreate': ('hermes_cli.web_models', 'SkillCreate'),
    'SkillInstallRequest': ('hermes_cli.web_models', 'SkillInstallRequest'),
    'SkillToggle': ('hermes_cli.web_models', 'SkillToggle'),
    'SkillUninstallRequest': ('hermes_cli.web_models', 'SkillUninstallRequest'),
    'SkillsUpdateRequest': ('hermes_cli.web_models', 'SkillsUpdateRequest'),
    'TTSLeaseRequest': ('hermes_cli.web_models', 'TTSLeaseRequest'),
    'TTSSpeakRequest': ('hermes_cli.web_models', 'TTSSpeakRequest'),
    'TelegramOnboardingApply': ('hermes_cli.web_models', 'TelegramOnboardingApply'),
    'TelegramOnboardingStart': ('hermes_cli.web_models', 'TelegramOnboardingStart'),
    'TerminalBackendSelect': ('hermes_cli.web_models', 'TerminalBackendSelect'),
    'ThemeSetBody': ('hermes_cli.web_models', 'ThemeSetBody'),
    'ToolsetEnvUpdate': ('hermes_cli.web_models', 'ToolsetEnvUpdate'),
    'ToolsetModelSelect': ('hermes_cli.web_models', 'ToolsetModelSelect'),
    'ToolsetPostSetup': ('hermes_cli.web_models', 'ToolsetPostSetup'),
    'ToolsetProviderSelect': ('hermes_cli.web_models', 'ToolsetProviderSelect'),
    'ToolsetToggle': ('hermes_cli.web_models', 'ToolsetToggle'),
    'WebhookCreate': ('hermes_cli.web_models', 'WebhookCreate'),
    'WebhookEnabledToggle': ('hermes_cli.web_models', 'WebhookEnabledToggle'),
    'WhatsAppOnboardingApply': ('hermes_cli.web_models', 'WhatsAppOnboardingApply'),
    'WhatsAppOnboardingStart': ('hermes_cli.web_models', 'WhatsAppOnboardingStart'),
    'activate_custom_endpoint': ('hermes_cli.web_routers.config_env', 'activate_custom_endpoint'),
    'add_credential_pool_entry': ('hermes_cli.web_routers.ops', 'add_credential_pool_entry'),
    'add_mcp_server': ('hermes_cli.web_routers.mcp', 'add_mcp_server'),
    'apply_telegram_onboarding': ('hermes_cli.web_routers.messaging', 'apply_telegram_onboarding'),
    'apply_whatsapp_onboarding': ('hermes_cli.web_routers.messaging', 'apply_whatsapp_onboarding'),
    'approve_pairing': ('hermes_cli.web_routers.ops', 'approve_pairing'),
    'auth_mcp_server': ('hermes_cli.web_routers.mcp', 'auth_mcp_server'),
    'build_cron_model_impact': ('hermes_cli.config', 'build_cron_model_impact'),
    'bulk_delete_sessions_endpoint': ('hermes_cli.web_routers.sessions', 'bulk_delete_sessions_endpoint'),
    'cancel_oauth_session': ('hermes_cli.web_routers.oauth', 'cancel_oauth_session'),
    'cancel_telegram_onboarding': ('hermes_cli.web_routers.messaging', 'cancel_telegram_onboarding'),
    'cancel_whatsapp_onboarding': ('hermes_cli.web_routers.messaging', 'cancel_whatsapp_onboarding'),
    'cfg_get': ('hermes_cli.config', 'cfg_get'),
    'check_config_version': ('hermes_cli.config', 'check_config_version'),
    'check_hermes_update': ('hermes_cli.web_routers.actions', 'check_hermes_update'),
    'clear_model_endpoint_credentials': ('hermes_cli.config', 'clear_model_endpoint_credentials'),
    'clear_pending_pairing': ('hermes_cli.web_routers.ops', 'clear_pending_pairing'),
    'coerce_provider_id': ('hermes_cli.config', 'coerce_provider_id'),
    'console_ws': ('hermes_cli.web_routers.chat_ws', 'console_ws'),
    'count_empty_sessions_endpoint': ('hermes_cli.web_routers.sessions', 'count_empty_sessions_endpoint'),
    'create_cron_job': ('hermes_cli.web_routers.cron', 'create_cron_job'),
    'create_hook': ('hermes_cli.web_routers.ops', 'create_hook'),
    'create_managed_directory': ('hermes_cli.web_routers.files', 'create_managed_directory'),
    'create_profile_endpoint': ('hermes_cli.web_routers.profiles', 'create_profile_endpoint'),
    'create_skill': ('hermes_cli.web_routers.skills', 'create_skill'),
    'create_webhook': ('hermes_cli.web_routers.ops', 'create_webhook'),
    'cron_fire_webhook': ('hermes_cli.web_routers.cron', 'cron_fire_webhook'),
    'custom_endpoint_key_env': ('hermes_cli.config', 'custom_endpoint_key_env'),
    'delete_agent_plugin': ('hermes_cli.web_routers.dashboard_ui', 'delete_agent_plugin'),
    'delete_cron_job': ('hermes_cli.web_routers.cron', 'delete_cron_job'),
    'delete_custom_endpoint': ('hermes_cli.web_routers.config_env', 'delete_custom_endpoint'),
    'delete_empty_sessions_endpoint': ('hermes_cli.web_routers.sessions', 'delete_empty_sessions_endpoint'),
    'delete_hook': ('hermes_cli.web_routers.ops', 'delete_hook'),
    'delete_learning_node': ('hermes_cli.web_routers.status', 'delete_learning_node'),
    'delete_managed_file': ('hermes_cli.web_routers.files', 'delete_managed_file'),
    'delete_profile_endpoint': ('hermes_cli.web_routers.profiles', 'delete_profile_endpoint'),
    'delete_session_endpoint': ('hermes_cli.web_routers.sessions', 'delete_session_endpoint'),
    'delete_webhook': ('hermes_cli.web_routers.ops', 'delete_webhook'),
    'derive_gateway_busy': ('gateway.status', 'derive_gateway_busy'),
    'derive_gateway_drainable': ('gateway.status', 'derive_gateway_drainable'),
    'describe_profile_auto_endpoint': ('hermes_cli.web_routers.profiles', 'describe_profile_auto_endpoint'),
    'detect_install_method': ('hermes_cli.config', 'detect_install_method'),
    'disconnect_oauth_provider': ('hermes_cli.web_routers.oauth', 'disconnect_oauth_provider'),
    'download_dashboard_backup': ('hermes_cli.web_routers.ops', 'download_dashboard_backup'),
    'download_managed_file': ('hermes_cli.web_routers.files', 'download_managed_file'),
    'enable_webhooks': ('hermes_cli.web_routers.ops', 'enable_webhooks'),
    'env_var_enabled': ('utils', 'env_var_enabled'),
    'events_ws': ('hermes_cli.web_routers.chat_ws', 'events_ws'),
    'export_session_endpoint': ('hermes_cli.web_routers.sessions', 'export_session_endpoint'),
    'find_provider_entry': ('hermes_cli.config', 'find_provider_entry'),
    'format_docker_update_message': ('hermes_cli.config', 'format_docker_update_message'),
    'fs_default_cwd': ('hermes_cli.web_routers.files', 'fs_default_cwd'),
    'fs_download': ('hermes_cli.web_routers.files', 'fs_download'),
    'fs_git_root': ('hermes_cli.web_routers.files', 'fs_git_root'),
    'fs_list': ('hermes_cli.web_routers.files', 'fs_list'),
    'fs_read_data_url': ('hermes_cli.web_routers.files', 'fs_read_data_url'),
    'fs_read_text': ('hermes_cli.web_routers.files', 'fs_read_text'),
    'fs_write_text': ('hermes_cli.web_routers.files', 'fs_write_text'),
    'gateway_drain': ('hermes_cli.web_routers.actions', 'gateway_drain'),
    'gateway_ws': ('hermes_cli.web_routers.chat_ws', 'gateway_ws'),
    'get_action_status': ('hermes_cli.web_routers.actions', 'get_action_status'),
    'get_active_profile_endpoint': ('hermes_cli.web_routers.profiles', 'get_active_profile_endpoint'),
    'get_auxiliary_models': ('hermes_cli.web_routers.models', 'get_auxiliary_models'),
    'get_client_voice_config': ('hermes_cli.web_routers.audio', 'get_client_voice_config'),
    'get_computer_use_status': ('hermes_cli.web_routers.tools', 'get_computer_use_status'),
    'get_config': ('hermes_cli.web_routers.config_env', 'get_config'),
    'get_config_path': ('hermes_cli.config', 'get_config_path'),
    'get_config_raw': ('hermes_cli.web_routers.analytics', 'get_config_raw'),
    'get_cron_delivery_targets': ('hermes_cli.web_routers.cron', 'get_cron_delivery_targets'),
    'get_cron_job': ('hermes_cli.web_routers.cron', 'get_cron_job'),
    'get_curator_status': ('hermes_cli.web_routers.status', 'get_curator_status'),
    'get_dashboard_font': ('hermes_cli.web_routers.dashboard_ui', 'get_dashboard_font'),
    'get_dashboard_plugins': ('hermes_cli.web_routers.dashboard_ui', 'get_dashboard_plugins'),
    'get_dashboard_themes': ('hermes_cli.web_routers.dashboard_ui', 'get_dashboard_themes'),
    'get_defaults': ('hermes_cli.web_routers.config_env', 'get_defaults'),
    'get_egress_status': ('hermes_cli.web_routers.config_env', 'get_egress_status'),
    'get_elevenlabs_voices': ('hermes_cli.web_routers.audio', 'get_elevenlabs_voices'),
    'get_env_path': ('hermes_cli.config', 'get_env_path'),
    'get_env_vars': ('hermes_cli.web_routers.config_env', 'get_env_vars'),
    'get_health': ('hermes_cli.web_routers.status', 'get_health'),
    'get_hermes_home': ('hermes_cli.config', 'get_hermes_home'),
    'get_learning_graph': ('hermes_cli.web_routers.status', 'get_learning_graph'),
    'get_learning_node': ('hermes_cli.web_routers.status', 'get_learning_node'),
    'get_logs': ('hermes_cli.web_routers.status', 'get_logs'),
    'get_media': ('hermes_cli.web_routers.files', 'get_media'),
    'get_memory_provider_config': ('hermes_cli.web_routers.memory_providers', 'get_memory_provider_config'),
    'get_memory_status': ('hermes_cli.web_routers.ops', 'get_memory_status'),
    'get_messaging_platforms': ('hermes_cli.web_routers.messaging', 'get_messaging_platforms'),
    'get_moa_models': ('hermes_cli.web_routers.models', 'get_moa_models'),
    'get_model_info': ('hermes_cli.web_routers.models', 'get_model_info'),
    'get_model_options': ('hermes_cli.web_routers.models', 'get_model_options'),
    'get_models_analytics': ('hermes_cli.web_routers.analytics', 'get_models_analytics'),
    'get_plugins_hub': ('hermes_cli.web_routers.dashboard_ui', 'get_plugins_hub'),
    'get_portal_status': ('hermes_cli.web_routers.status', 'get_portal_status'),
    'get_process_hermes_home': ('hermes_cli.config', 'get_process_hermes_home'),
    'get_profile_setup_command': ('hermes_cli.web_routers.profiles', 'get_profile_setup_command'),
    'get_profile_soul': ('hermes_cli.web_routers.profiles', 'get_profile_soul'),
    'get_profiles_sessions': ('hermes_cli.web_routers.profiles', 'get_profiles_sessions'),
    'get_profiles_sessions_sidebar': ('hermes_cli.web_routers.profiles', 'get_profiles_sessions_sidebar'),
    'get_provider_config_schema': ('plugins.memory.config_schema', 'get_provider_config_schema'),
    'get_recommended_default_model': ('hermes_cli.web_routers.models', 'get_recommended_default_model'),
    'get_running_pid': ('gateway.status', 'get_running_pid'),
    'get_running_pid_cached': ('gateway.status', 'get_running_pid_cached'),
    'get_runtime_status_running_pid': ('gateway.status', 'get_runtime_status_running_pid'),
    'get_schema': ('hermes_cli.web_routers.config_env', 'get_schema'),
    'get_session_detail': ('hermes_cli.web_routers.sessions', 'get_session_detail'),
    'get_session_latest_descendant': ('hermes_cli.web_routers.sessions', 'get_session_latest_descendant'),
    'get_session_messages': ('hermes_cli.web_routers.sessions', 'get_session_messages'),
    'get_session_stats': ('hermes_cli.web_routers.sessions', 'get_session_stats'),
    'get_sessions': ('hermes_cli.web_routers.sessions', 'get_sessions'),
    'get_skill_content': ('hermes_cli.web_routers.skills', 'get_skill_content'),
    'get_skills': ('hermes_cli.web_routers.skills', 'get_skills'),
    'get_ssh_ownership': ('hermes_cli.web_routers.status', 'get_ssh_ownership'),
    'get_status': ('hermes_cli.web_routers.status', 'get_status'),
    'get_system_stats': ('hermes_cli.web_routers.status', 'get_system_stats'),
    'get_telegram_onboarding_status': ('hermes_cli.web_routers.messaging', 'get_telegram_onboarding_status'),
    'get_terminal_backends': ('hermes_cli.web_routers.tools', 'get_terminal_backends'),
    'get_toolset_config': ('hermes_cli.web_routers.tools', 'get_toolset_config'),
    'get_toolset_models': ('hermes_cli.web_routers.tools', 'get_toolset_models'),
    'get_toolsets': ('hermes_cli.web_routers.tools', 'get_toolsets'),
    'get_update_receipt': ('hermes_cli.web_routers.actions', 'get_update_receipt'),
    'get_usage_analytics': ('hermes_cli.web_routers.analytics', 'get_usage_analytics'),
    'get_whatsapp_onboarding_status': ('hermes_cli.web_routers.messaging', 'get_whatsapp_onboarding_status'),
    'git_base_branches_route': ('hermes_cli.web_routers.git', 'git_base_branches_route'),
    'git_branch_switch_route': ('hermes_cli.web_routers.git', 'git_branch_switch_route'),
    'git_branches_route': ('hermes_cli.web_routers.git', 'git_branches_route'),
    'git_commit_context_route': ('hermes_cli.web_routers.git', 'git_commit_context_route'),
    'git_commit_route': ('hermes_cli.web_routers.git', 'git_commit_route'),
    'git_create_pr_route': ('hermes_cli.web_routers.git', 'git_create_pr_route'),
    'git_file_diff_route': ('hermes_cli.web_routers.git', 'git_file_diff_route'),
    'git_push_route': ('hermes_cli.web_routers.git', 'git_push_route'),
    'git_rev_parse_route': ('hermes_cli.web_routers.git', 'git_rev_parse_route'),
    'git_revert_route': ('hermes_cli.web_routers.git', 'git_revert_route'),
    'git_review_diff_route': ('hermes_cli.web_routers.git', 'git_review_diff_route'),
    'git_review_list_route': ('hermes_cli.web_routers.git', 'git_review_list_route'),
    'git_ship_info_route': ('hermes_cli.web_routers.git', 'git_ship_info_route'),
    'git_stage_route': ('hermes_cli.web_routers.git', 'git_stage_route'),
    'git_status_route': ('hermes_cli.web_routers.git', 'git_status_route'),
    'git_unstage_route': ('hermes_cli.web_routers.git', 'git_unstage_route'),
    'git_worktree_add_route': ('hermes_cli.web_routers.git', 'git_worktree_add_route'),
    'git_worktree_remove_route': ('hermes_cli.web_routers.git', 'git_worktree_remove_route'),
    'git_worktrees_route': ('hermes_cli.web_routers.git', 'git_worktrees_route'),
    'grant_computer_use_permissions': ('hermes_cli.web_routers.tools', 'grant_computer_use_permissions'),
    'import_sessions_endpoint': ('hermes_cli.web_routers.sessions', 'import_sessions_endpoint'),
    'install_mcp_catalog_entry': ('hermes_cli.web_routers.mcp', 'install_mcp_catalog_entry'),
    'install_skill_hub': ('hermes_cli.web_routers.skills', 'install_skill_hub'),
    'instantiate_blueprint': ('hermes_cli.web_routers.cron', 'instantiate_blueprint'),
    'is_nix_install_method': ('hermes_cli.config', 'is_nix_install_method'),
    'list_checkpoints': ('hermes_cli.web_routers.ops', 'list_checkpoints'),
    'list_credential_pool': ('hermes_cli.web_routers.ops', 'list_credential_pool'),
    'list_cron_blueprints': ('hermes_cli.web_routers.cron', 'list_cron_blueprints'),
    'list_cron_job_runs': ('hermes_cli.web_routers.cron', 'list_cron_job_runs'),
    'list_cron_jobs': ('hermes_cli.web_routers.cron', 'list_cron_jobs'),
    'list_custom_endpoints': ('hermes_cli.web_routers.config_env', 'list_custom_endpoints'),
    'list_hooks': ('hermes_cli.web_routers.ops', 'list_hooks'),
    'list_managed_files': ('hermes_cli.web_routers.files', 'list_managed_files'),
    'list_mcp_catalog': ('hermes_cli.web_routers.mcp', 'list_mcp_catalog'),
    'list_mcp_servers': ('hermes_cli.web_routers.mcp', 'list_mcp_servers'),
    'list_oauth_providers': ('hermes_cli.web_routers.oauth', 'list_oauth_providers'),
    'list_pairing': ('hermes_cli.web_routers.ops', 'list_pairing'),
    'list_profiles_endpoint': ('hermes_cli.web_routers.profiles', 'list_profiles_endpoint'),
    'list_skills_hub_sources': ('hermes_cli.web_routers.skills', 'list_skills_hub_sources'),
    'list_webhooks': ('hermes_cli.web_routers.ops', 'list_webhooks'),
    'load_env': ('hermes_cli.config', 'load_env'),
    'mcp_oauth_callback': ('hermes_cli.web_routers.mcp', 'mcp_oauth_callback'),
    'mcp_oauth_flow_status': ('hermes_cli.web_routers.mcp', 'mcp_oauth_flow_status'),
    'normalize_updated_at': ('gateway.status', 'normalize_updated_at'),
    'open_profile_terminal_endpoint': ('hermes_cli.web_routers.profiles', 'open_profile_terminal_endpoint'),
    'parse_active_agents': ('gateway.status', 'parse_active_agents'),
    'pause_cron_job': ('hermes_cli.web_routers.cron', 'pause_cron_job'),
    'poll_oauth_session': ('hermes_cli.web_routers.oauth', 'poll_oauth_session'),
    'post_agent_plugin_disable': ('hermes_cli.web_routers.dashboard_ui', 'post_agent_plugin_disable'),
    'post_agent_plugin_enable': ('hermes_cli.web_routers.dashboard_ui', 'post_agent_plugin_enable'),
    'post_agent_plugin_install': ('hermes_cli.web_routers.dashboard_ui', 'post_agent_plugin_install'),
    'post_agent_plugin_update': ('hermes_cli.web_routers.dashboard_ui', 'post_agent_plugin_update'),
    'post_plugin_visibility': ('hermes_cli.web_routers.dashboard_ui', 'post_plugin_visibility'),
    'preview_skill_hub': ('hermes_cli.web_routers.skills', 'preview_skill_hub'),
    'prune_checkpoints': ('hermes_cli.web_routers.ops', 'prune_checkpoints'),
    'prune_sessions_endpoint': ('hermes_cli.web_routers.sessions', 'prune_sessions_endpoint'),
    'pty_ws': ('hermes_cli.web_routers.chat_ws', 'pty_ws'),
    'pub_ws': ('hermes_cli.web_routers.chat_ws', 'pub_ws'),
    'put_plugin_providers': ('hermes_cli.web_routers.dashboard_ui', 'put_plugin_providers'),
    'read_managed_file': ('hermes_cli.web_routers.files', 'read_managed_file'),
    'read_raw_config': ('hermes_cli.config', 'read_raw_config'),
    'read_runtime_status': ('gateway.status', 'read_runtime_status'),
    'recommended_update_command_for_method': ('hermes_cli.config', 'recommended_update_command_for_method'),
    'redact_key': ('hermes_cli.config', 'redact_key'),
    'remove_credential_pool_entry': ('hermes_cli.web_routers.ops', 'remove_credential_pool_entry'),
    'remove_env_value': ('hermes_cli.config', 'remove_env_value'),
    'remove_env_var': ('hermes_cli.web_routers.config_env', 'remove_env_var'),
    'remove_mcp_server': ('hermes_cli.web_routers.mcp', 'remove_mcp_server'),
    'rename_profile_endpoint': ('hermes_cli.web_routers.profiles', 'rename_profile_endpoint'),
    'rename_session_endpoint': ('hermes_cli.web_routers.sessions', 'rename_session_endpoint'),
    'replace_mcp_servers': ('hermes_cli.web_routers.mcp', 'replace_mcp_servers'),
    'rescan_dashboard_plugins': ('hermes_cli.web_routers.dashboard_ui', 'rescan_dashboard_plugins'),
    'reset_memory': ('hermes_cli.web_routers.ops', 'reset_memory'),
    'resolve_cron_model_drift_defaults': ('hermes_cli.config', 'resolve_cron_model_drift_defaults'),
    'resolve_gateway_liveness': ('gateway.status', 'resolve_gateway_liveness'),
    'restart_gateway': ('hermes_cli.web_routers.actions', 'restart_gateway'),
    'resume_cron_job': ('hermes_cli.web_routers.cron', 'resume_cron_job'),
    'reveal_env_var': ('hermes_cli.web_routers.config_env', 'reveal_env_var'),
    'revoke_pairing': ('hermes_cli.web_routers.ops', 'revoke_pairing'),
    'run_backup': ('hermes_cli.web_routers.ops', 'run_backup'),
    'run_config_migrate': ('hermes_cli.web_routers.status', 'run_config_migrate'),
    'run_curator': ('hermes_cli.web_routers.status', 'run_curator'),
    'run_debug_share_endpoint': ('hermes_cli.web_routers.status', 'run_debug_share_endpoint'),
    'run_doctor': ('hermes_cli.doctor', 'run_doctor'),
    'run_dump': ('hermes_cli.dump', 'run_dump'),
    'run_import': ('hermes_cli.web_routers.ops', 'run_import'),
    'run_import_upload': ('hermes_cli.web_routers.ops', 'run_import_upload'),
    'run_prompt_size': ('hermes_cli.web_routers.status', 'run_prompt_size'),
    'run_security_audit': ('hermes_cli.web_routers.ops', 'run_security_audit'),
    'run_toolset_post_setup': ('hermes_cli.web_routers.tools', 'run_toolset_post_setup'),
    'save_config': ('hermes_cli.config', 'save_config'),
    'save_env_value': ('hermes_cli.config', 'save_env_value'),
    'save_toolset_env': ('hermes_cli.web_routers.tools', 'save_toolset_env'),
    'scan_skill_hub': ('hermes_cli.web_routers.skills', 'scan_skill_hub'),
    'search_sessions': ('hermes_cli.web_routers.sessions', 'search_sessions'),
    'search_skills_hub': ('hermes_cli.web_routers.skills', 'search_skills_hub'),
    'select_terminal_backend': ('hermes_cli.web_routers.tools', 'select_terminal_backend'),
    'select_toolset_model': ('hermes_cli.web_routers.tools', 'select_toolset_model'),
    'select_toolset_provider': ('hermes_cli.web_routers.tools', 'select_toolset_provider'),
    'serve_plugin_asset': ('hermes_cli.web_routers.dashboard_ui', 'serve_plugin_asset'),
    'set_active_profile_endpoint': ('hermes_cli.web_routers.profiles', 'set_active_profile_endpoint'),
    'set_curator_paused': ('hermes_cli.web_routers.status', 'set_curator_paused'),
    'set_dashboard_font': ('hermes_cli.web_routers.dashboard_ui', 'set_dashboard_font'),
    'set_dashboard_theme': ('hermes_cli.web_routers.dashboard_ui', 'set_dashboard_theme'),
    'set_env_var': ('hermes_cli.web_routers.config_env', 'set_env_var'),
    'set_mcp_server_enabled': ('hermes_cli.web_routers.mcp', 'set_mcp_server_enabled'),
    'set_memory_provider': ('hermes_cli.web_routers.ops', 'set_memory_provider'),
    'set_moa_models': ('hermes_cli.web_routers.models', 'set_moa_models'),
    'set_model_assignment': ('hermes_cli.web_routers.models', 'set_model_assignment'),
    'set_webhook_enabled': ('hermes_cli.web_routers.ops', 'set_webhook_enabled'),
    'setup_memory_provider': ('hermes_cli.web_routers.memory_providers', 'setup_memory_provider'),
    'speak_stream_ws': ('hermes_cli.web_routers.audio', 'speak_stream_ws'),
    'speak_text': ('hermes_cli.web_routers.audio', 'speak_text'),
    'start_gateway': ('hermes_cli.web_routers.ops', 'start_gateway'),
    'start_oauth_login': ('hermes_cli.web_routers.oauth', 'start_oauth_login'),
    'start_telegram_onboarding': ('hermes_cli.web_routers.messaging', 'start_telegram_onboarding'),
    'start_whatsapp_onboarding': ('hermes_cli.web_routers.messaging', 'start_whatsapp_onboarding'),
    'stop_gateway': ('hermes_cli.web_routers.ops', 'stop_gateway'),
    'stream_managed_file': ('hermes_cli.web_routers.files', 'stream_managed_file'),
    'submit_oauth_code': ('hermes_cli.web_routers.oauth', 'submit_oauth_code'),
    'test_mcp_server': ('hermes_cli.web_routers.mcp', 'test_mcp_server'),
    'test_messaging_platform': ('hermes_cli.web_routers.messaging', 'test_messaging_platform'),
    'toggle_skill': ('hermes_cli.web_routers.skills', 'toggle_skill'),
    'toggle_toolset': ('hermes_cli.web_routers.tools', 'toggle_toolset'),
    'transcribe_audio_upload': ('hermes_cli.web_routers.audio', 'transcribe_audio_upload'),
    'trigger_cron_job': ('hermes_cli.web_routers.cron', 'trigger_cron_job'),
    'tts_lease': ('hermes_cli.web_routers.audio', 'tts_lease'),
    'uninstall_skill_hub': ('hermes_cli.web_routers.skills', 'uninstall_skill_hub'),
    'update_config': ('hermes_cli.web_routers.config_env', 'update_config'),
    'update_config_raw': ('hermes_cli.web_routers.analytics', 'update_config_raw'),
    'update_cron_job': ('hermes_cli.web_routers.cron', 'update_cron_job'),
    'update_hermes': ('hermes_cli.web_routers.actions', 'update_hermes'),
    'update_learning_node': ('hermes_cli.web_routers.status', 'update_learning_node'),
    'update_memory_provider_config': ('hermes_cli.web_routers.memory_providers', 'update_memory_provider_config'),
    'update_messaging_platform': ('hermes_cli.web_routers.messaging', 'update_messaging_platform'),
    'update_profile_description_endpoint': ('hermes_cli.web_routers.profiles', 'update_profile_description_endpoint'),
    'update_profile_model_endpoint': ('hermes_cli.web_routers.profiles', 'update_profile_model_endpoint'),
    'update_profile_soul': ('hermes_cli.web_routers.profiles', 'update_profile_soul'),
    'update_skill_content': ('hermes_cli.web_routers.skills', 'update_skill_content'),
    'update_skills_hub': ('hermes_cli.web_routers.skills', 'update_skills_hub'),
    'upload_chat_image': ('hermes_cli.web_routers.files', 'upload_chat_image'),
    'upload_managed_file': ('hermes_cli.web_routers.files', 'upload_managed_file'),
    'upload_managed_file_stream': ('hermes_cli.web_routers.files', 'upload_managed_file_stream'),
    'upsert_custom_endpoint': ('hermes_cli.web_routers.config_env', 'upsert_custom_endpoint'),
    'validate_custom_endpoint': ('hermes_cli.web_routers.config_env', 'validate_custom_endpoint'),
    'validate_provider_credential': ('hermes_cli.web_routers.config_env', 'validate_provider_credential'),
    'windows_detach_flags': ('hermes_cli._subprocess_compat', 'windows_detach_flags'),
    'windows_hide_flags': ('hermes_cli._subprocess_compat', 'windows_hide_flags'),
    'write_platform_config_field': ('hermes_cli.config', 'write_platform_config_field'),
}


def __getattr__(name):  # PEP 562 — lazy so no import cycles
    target = _PLUGIN_COMPAT_LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    from hermes_cli.plugin_compat import warn_once
    warn_once(__name__, name, *target)
    return getattr(importlib.import_module(target[0]), target[1])
# ---- END PLUGIN-COMPAT ----
