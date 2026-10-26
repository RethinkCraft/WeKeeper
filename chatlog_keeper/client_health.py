"""Read-only evidence of the native Linux WeChat client's session UI.

An open session window is not proof of server connectivity. This probe never
clicks, logs in, scans memory, reads chat text, or makes an outbound request.
X11 inspection runs in a bounded child so Xlib connection errors cannot take
down the collector.
"""
from __future__ import annotations

import argparse
import ctypes
import ctypes.util
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from chatlog_keeper.core import _linux

_VERIFIED_BUILD = "2749535f299ba67feb8ef80cab898bb0531fa8f376b16f8a72db706811c84132"
_TITLES = {"weixin", "wechat", "微信"}
_CLASSES = {"weixin", "wechat", "com.tencent.wechat"}
_LOCAL_DISPLAY = re.compile(r"^(?:unix)?:[0-9]+(?:\.[0-9]+)?$")
_HELPERS = ("--type=", "crashpad", "renderer", "gpu-process", "zygote")
_MAX_OUTPUT = 65536


def _result(state, reason, *, running=None, display=None, network_state="unknown", **evidence):
    return {"state": state, "reason": reason, "checked_at": time.time(),
            "process_running": running, "display_available": display,
            "network_state": network_state, "evidence": evidence}


def _network_state():
    """Read NetworkManager's cached state, never request a connectivity check.

    ``available`` means a locally connected network, not a successful WeChat
    server request. An unsupported/unmanaged state remains unknown.
    """
    try:
        value = subprocess.run(["nmcli", "-t", "-f", "STATE", "general"],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, check=False,
            timeout=2, env={**os.environ, "LC_ALL": "C"})
        if value.returncode != 0:
            return "unknown"
        state = value.stdout.strip().casefold()
        if state in {"asleep", "disconnected"}:
            return "unavailable"
        if state == "connected" or state.startswith("connected ("):
            return "available"
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def _discover_processes():
    """Distinguish a complete absence check from denied process inspection."""
    expected = {str(path.resolve()) for path in _linux.official_wechat_executables()}
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return False, []
    complete, found = True, []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            comm = (entry / "comm").read_text().strip()
            try:
                executable = os.readlink(entry / "exe")
            except PermissionError:
                if comm.casefold() in {"wechat", "weixin"}:
                    complete = False
                continue
            if (executable not in expected
                    and comm.casefold() not in {"wechat", "weixin"}):
                continue
            with (entry / "cmdline").open("rb") as handle:
                command = handle.read(16384).replace(b"\0", b" ").decode(errors="replace")
            if any(marker in command.casefold() for marker in _HELPERS):
                continue
            found.append({"pid": int(entry.name), "executable": executable,
                          "official_path": executable in expected})
        except FileNotFoundError:
            continue  # A process may finish while /proc is being enumerated.
        except PermissionError:
            complete = False
        except OSError:
            complete = False
    return complete, found


@lru_cache(maxsize=8)
def _hash_build(path, identity):
    del identity  # Part of the cache key; invalidate after an in-place upgrade.
    try:
        with Path(path).open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest() == _VERIFIED_BUILD
    except OSError:
        return False


def _verified_build(path):
    try:
        value = Path(path).stat()
        return _hash_build(str(path), (value.st_dev, value.st_ino, value.st_size,
                                      value.st_mtime_ns))
    except OSError:
        return False


def _account_binding(account_id):
    """Only certify the selected account when its local scope is unambiguous."""
    if account_id is None:
        return "not_requested"
    accounts = set()
    try:
        for root in _linux.wechat_data_roots():
            if not root.exists():
                continue
            for child in root.iterdir():
                directory = child / "db_storage" / "message"
                if child.is_dir() and directory.is_dir() and any(directory.glob("message_*.db")):
                    accounts.add(child.resolve())
    except OSError:
        return "unverified"
    if len(accounts) == 1 and next(iter(accounts)).name == str(account_id):
        return "single_account_scope"
    return "ambiguous" if len(accounts) > 1 else "unverified"


def _classify_windows(windows):
    """Classify only our verified build's own, visible, normal top-level UI."""
    recognized = []
    for item in windows:
        classes = {part.casefold() for part in item.get("classes", [])}
        if not (classes & _CLASSES):
            continue
        if item.get("title", "").strip().casefold() not in _TITLES:
            continue
        recognized.append(item)
    def login_layout(window):
        width, height = window.get("width", 0), window.get("height", 0)
        scale = height / 380
        # Qt may report pixel dimensions scaled by the display's DPI factor.
        # Recognize the reviewed portrait login shape before the main-window
        # minimum, including small integer rounding at fractional scaling.
        return 0.75 <= scale <= 4 and abs(width - 280 * scale) <= 2
    if any(w.get("map_state") == 2 and login_layout(w) for w in recognized):
        return "login_required", "visible_login_window"
    sessions = [w for w in recognized
                if 600 <= w.get("width", 0) <= 10000 and 450 <= w.get("height", 0) <= 10000
                and w.get("width", 0) >= w.get("height", 0)
                and not w.get("transient_for") and w.get("normal", True)]
    if any(w.get("map_state") == 2 for w in sessions):
        return "session_present", "visible_session_window"
    if any(w.get("map_state") in {0, 1} and w.get("wm_state") == 3
           and w.get("hidden") is True for w in sessions):
        return "session_present", "minimized_session_window"
    return "unknown", "no_verified_visible_session_window"


def _inspect_windows(pid):
    """Call the isolated X11 reader, returning only bounded parsed metadata."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "chatlog_keeper.client_health", "--x11-probe", str(pid)],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=4,
            check=False, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        if result.returncode != 0 or len(result.stdout) > _MAX_OUTPUT:
            return {"display_available": False, "reason": "display_inspection_failed"}
        value = json.loads(result.stdout)
        if not isinstance(value, dict) or not isinstance(value.get("windows", []), list):
            raise ValueError("invalid helper result")
        return value
    except subprocess.TimeoutExpired:
        return {"display_available": False, "reason": "display_inspection_timeout"}
    except (OSError, ValueError, TypeError):
        return {"display_available": False, "reason": "display_inspection_failed"}


def probe_client(account_id=None):
    """Return conservative session evidence, independently of archive liveness.

    ``session_present`` certifies an observed client session UI only. It does not
    claim that WeChat's servers are reachable or that a quiet group is offline.
    """
    if not sys.platform.startswith("linux"):
        return _result("unknown", "linux_required")
    try:
        complete, processes = _discover_processes()
        if not processes:
            return _result("not_running" if complete else "unknown",
                           "client_process_absent" if complete else "process_inspection_unavailable",
                           running=False if complete else None)
        if not complete:
            return _result("unknown", "process_inspection_incomplete", running=True)
        if len(processes) != 1:
            return _result("unknown", "multiple_client_processes", running=True,
                           client_process_count=len(processes))
        process = processes[0]
        if not process["official_path"] or not _verified_build(process["executable"]):
            return _result("unknown", "unverified_client_build", running=True)
        network = _network_state()
        inspection = _inspect_windows(process["pid"])
        if not inspection.get("display_available"):
            return _result("network_unavailable" if network == "unavailable" else "unknown",
                           "local_network_disconnected" if network == "unavailable" else
                           inspection.get("reason", "display_inspection_failed"),
                           running=True, display=False, network_state=network, verified_build=True)
        state, reason = _classify_windows(inspection.get("windows", []))
        binding = _account_binding(account_id)
        if state == "session_present" and binding in {"ambiguous", "unverified"}:
            state, reason = "unknown", "selected_account_unverified"
        if state != "login_required" and network == "unavailable":
            state, reason = "network_unavailable", "local_network_disconnected"
        return _result(state, reason, running=True, display=True, verified_build=True,
                       network_state=network,
                       account_binding=binding,
                       own_visible_windows=sum(w.get("map_state") == 2
                                               for w in inspection.get("windows", [])),
                       server_connectivity="not_probed",
                       session_window_minimized=reason == "minimized_session_window")
    except Exception:
        return _result("unknown", "client_inspection_failed")


def _x11_probe(pid):
    """Read metadata from this official process's display without UI actions."""
    try:
        with Path("/proc", str(pid), "environ").open("rb") as handle:
            raw = handle.read(1024 * 1024)
        environment = dict(item.split(b"=", 1) for item in raw.split(b"\0") if b"=" in item)
        display = environment.get(b"DISPLAY", b"").decode(errors="strict")
        if not _LOCAL_DISPLAY.fullmatch(display):
            return {"display_available": False, "reason": "local_client_display_unavailable"}
        # No generic desktop-session fallback: another Xwayland session can have
        # a valid but unrelated authority cookie.
        for key in ("DISPLAY", "XAUTHORITY"):
            if key.encode() in environment:
                os.environ[key] = environment[key.encode()].decode(errors="strict")
            else:
                os.environ.pop(key, None)
        library = ctypes.util.find_library("X11")
        if not library:
            return {"display_available": False, "reason": "x11_library_unavailable"}
        x = ctypes.CDLL(library)
        D, W, U = ctypes.c_void_p, ctypes.c_ulong, ctypes.c_ulong
        class Attributes(ctypes.Structure):
            _fields_ = [(n, ctypes.c_int) for n in ("x", "y", "width", "height", "border", "depth")] + [
                ("visual", D), ("root", W), ("window_class", ctypes.c_int),
                ("bit_gravity", ctypes.c_int), ("win_gravity", ctypes.c_int),
                ("backing_store", ctypes.c_int), ("backing_planes", U), ("backing_pixel", U),
                ("save_under", ctypes.c_int), ("colormap", U), ("map_installed", ctypes.c_int),
                ("map_state", ctypes.c_int), ("all_event_masks", ctypes.c_long),
                ("your_event_mask", ctypes.c_long), ("do_not_propagate_mask", ctypes.c_long),
                ("override_redirect", ctypes.c_int), ("screen", D)]
        handler_type = ctypes.CFUNCTYPE(ctypes.c_int, D, D)
        # A disappearing window is a normal race; Xlib's default handler would
        # otherwise terminate this helper (the parent still remains isolated).
        error_handler = handler_type(lambda _d, _event: 0)
        x.XSetErrorHandler.argtypes = [handler_type]
        x.XSetErrorHandler(error_handler)
        x.XOpenDisplay.argtypes = [ctypes.c_char_p]; x.XOpenDisplay.restype = D
        x.XDefaultRootWindow.argtypes = [D]; x.XDefaultRootWindow.restype = W
        x.XInternAtom.argtypes = [D, ctypes.c_char_p, ctypes.c_int]; x.XInternAtom.restype = U
        x.XGetWindowProperty.argtypes = [D, W, U, ctypes.c_long, ctypes.c_long, ctypes.c_int, U,
                                        ctypes.POINTER(U), ctypes.POINTER(ctypes.c_int),
                                        ctypes.POINTER(U), ctypes.POINTER(U), ctypes.POINTER(D)]
        x.XGetWindowAttributes.argtypes = [D, W, ctypes.POINTER(Attributes)]
        x.XFree.argtypes = [D]; x.XCloseDisplay.argtypes = [D]
        connection = x.XOpenDisplay(display.encode())
        if not connection:
            return {"display_available": False, "reason": "display_access_unavailable"}
        try:
            def prop(window, name, length=128):
                actual, fmt, count, remaining, pointer = U(), ctypes.c_int(), U(), U(), D()
                status = x.XGetWindowProperty(connection, window,
                    x.XInternAtom(connection, name.encode(), 0), 0, length, 0, 0,
                    ctypes.byref(actual), ctypes.byref(fmt), ctypes.byref(count),
                    ctypes.byref(remaining), ctypes.byref(pointer))
                if status or not pointer:
                    return None
                try:
                    if fmt.value == 32:
                        return list(ctypes.cast(pointer, ctypes.POINTER(U))[:count.value])
                    if fmt.value == 8:
                        return ctypes.string_at(pointer, count.value)
                    return None
                finally:
                    x.XFree(pointer)
            clients = prop(x.XDefaultRootWindow(connection), "_NET_CLIENT_LIST", 4096)
            if not isinstance(clients, list):
                return {"display_available": True, "windows": [], "reason": "window_inventory_unavailable"}
            windows = []
            normal_type = x.XInternAtom(connection, b"_NET_WM_WINDOW_TYPE_NORMAL", 0)
            hidden_state = x.XInternAtom(connection, b"_NET_WM_STATE_HIDDEN", 0)
            for window in clients[:4096]:
                owner = prop(window, "_NET_WM_PID")
                if not owner or owner[0] != pid:
                    continue
                attributes = Attributes()
                if not x.XGetWindowAttributes(connection, window, ctypes.byref(attributes)):
                    continue
                title = prop(window, "_NET_WM_NAME") or prop(window, "WM_NAME") or b""
                classes = prop(window, "WM_CLASS") or b""
                if not isinstance(title, bytes) or not isinstance(classes, bytes):
                    continue
                types = prop(window, "_NET_WM_WINDOW_TYPE")
                transient = prop(window, "WM_TRANSIENT_FOR")
                wm_state = prop(window, "WM_STATE")
                states = prop(window, "_NET_WM_STATE")
                windows.append({"width": attributes.width, "height": attributes.height,
                    "map_state": attributes.map_state,
                    "title": title.decode(errors="replace")[:128],
                    "classes": [c.decode(errors="replace")[:128] for c in classes.split(b"\0") if c],
                    "transient_for": transient[0] if transient else None,
                    "wm_state": wm_state[0] if wm_state else None,
                    "hidden": bool(states and hidden_state in states),
                    "normal": not types or normal_type in types})
                if len(windows) >= 64:
                    break
            return {"display_available": True, "windows": windows}
        finally:
            x.XCloseDisplay(connection)
    except (OSError, ValueError, UnicodeError):
        return {"display_available": False, "reason": "display_inspection_unavailable"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read-only WeChat client session probe")
    parser.add_argument("--x11-probe", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.x11_probe is not None:
        print(json.dumps(_x11_probe(args.x11_probe)))
    else:
        print(json.dumps(probe_client()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
