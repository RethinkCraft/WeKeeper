"""Narrow accessibility bridge for the verified Linux WeChat window.

Only explicit, allowlisted navigation/transcription operations are exposed.
Only the verified history-member filter accepts text, never the composer.
There is no keypress, file-selection, login or send operation.
Executed with the system Python for its installed AT-SPI bindings.
"""
from __future__ import annotations

import argparse
import ctypes as C
import ctypes.util
import json
import os
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class NativeUnavailable(RuntimeError):
    pass


def context():
    from chatlog_keeper import client_health
    complete, processes = client_health._discover_processes()
    if not complete or len(processes) != 1:
        raise NativeUnavailable('native_client_not_unique')
    process = processes[0]
    if not process['official_path'] or not client_health._verified_build(process['executable']):
        raise NativeUnavailable('native_client_build_not_verified')
    inspection = client_health._inspect_windows(process['pid'])
    state, _ = client_health._classify_windows(inspection.get('windows', []))
    if state != 'session_present':
        raise NativeUnavailable('native_login_required' if state == 'login_required' else 'native_session_unavailable')
    for part in (Path('/proc') / str(process['pid']) / 'environ').read_bytes().split(b'\0'):
        key, sep, value = part.partition(b'=')
        if sep and key in {b'DBUS_SESSION_BUS_ADDRESS', b'DISPLAY', b'XAUTHORITY', b'XDG_RUNTIME_DIR'}:
            os.environ[key.decode()] = value.decode()
    import gi
    gi.require_version('Atspi', '2.0')
    from gi.repository import Atspi
    desktop = Atspi.get_desktop(0)
    matches = []
    for index in range(min(desktop.get_child_count(), 100)):
        app = desktop.get_child_at_index(index)
        if app.get_process_id() == process['pid']:
            matches.append(app)
    if len(matches) != 1:
        raise NativeUnavailable('native_accessibility_unavailable')
    matches[0].set_cache_mask(Atspi.Cache.NONE)
    return process, Atspi, matches[0]


def walk(root, maximum=8000, depth_limit=30):
    # Qt updates the AT-SPI tree asynchronously after navigation. Dispatch its
    # events before observing the next state, without a second GUI main loop.
    from gi.repository import GLib
    event_context = GLib.MainContext.default()
    for _ in range(200):
        if not event_context.pending():
            break
        event_context.iteration(False)
    queue, count = [(root, (), 0)], 0
    while queue and count < maximum:
        node, path, depth = queue.pop(0)
        count += 1
        try:
            yield node, path
            if depth < depth_limit:
                for index in range(min(node.get_child_count(), 200)):
                    queue.append((node.get_child_at_index(index), path + (index,), depth + 1))
        except Exception:
            continue


def showing(node, Atspi):
    return node.get_state_set().contains(Atspi.StateType.SHOWING)


def snapshot(root, Atspi, maximum=250):
    items = []
    for node, path in walk(root):
        if not showing(node, Atspi):
            continue
        role, name = node.get_role_name(), node.get_name() or ''
        # Do not inspect the message composer or other editable text values.
        if role in {'entry', 'password text', 'text'} and 'EditableText' in node.get_interfaces():
            continue
        if not name and role not in {'list', 'list item', 'document frame', 'menu', 'menu item', 'text'}:
            continue
        item = {'path': list(path), 'role': role, 'name': name[:500], 'children': node.get_child_count()}
        try:
            item['description'] = (node.get_description() or '')[:500]
            item['attributes'] = [v for v in node.get_attributes() if any(v.startswith(k) for k in ('id:', 'class:', 'level:', 'placeholder-text:'))]
        except Exception:
            pass
        items.append(item)
        if len(items) >= maximum:
            break
    return items


def _click(process, node, Atspi, button=1, *, voice_bubble=False):
    if button not in {1, 3, 4, 5} or not showing(node, Atspi):
        raise NativeUnavailable('native_control_not_visible')
    if 'EditableText' in node.get_interfaces():
        raise NativeUnavailable('native_editable_control_forbidden')
    rectangle = node.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
    if not 1 <= rectangle.width <= 10000 or not 1 <= rectangle.height <= 10000:
        raise NativeUnavailable('native_control_geometry_invalid')
    ancestor = node
    while ancestor is not None and ancestor.get_role_name() != 'frame':
        parent = ancestor.get_parent()
        if parent is not None and parent.get_role_name() == 'application':
            break  # Qt popup windows expose a top-level filler, not a frame.
        ancestor = parent
    if ancestor is None:
        raise NativeUnavailable('native_control_without_verified_frame')
    frame = ancestor.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
    left, top = max(rectangle.x, frame.x), max(rectangle.y, frame.y)
    right = min(rectangle.x + rectangle.width, frame.x + frame.width)
    bottom = min(rectangle.y + rectangle.height, frame.y + frame.height)
    if right <= left or bottom <= top:
        raise NativeUnavailable('native_control_clipped')
    gx, gy = (left + right) // 2, (top + bottom) // 2
    x = C.CDLL(C.util.find_library('X11'))
    D, U, W = C.c_void_p, C.c_ulong, C.c_ulong
    x.XOpenDisplay.argtypes = [C.c_char_p]; x.XOpenDisplay.restype = D
    x.XDefaultRootWindow.argtypes = [D]; x.XDefaultRootWindow.restype = W
    x.XInternAtom.argtypes = [D, C.c_char_p, C.c_int]; x.XInternAtom.restype = U
    x.XGetWindowProperty.argtypes = [D, W, U, C.c_long, C.c_long, C.c_int, U, C.POINTER(U), C.POINTER(C.c_int), C.POINTER(U), C.POINTER(U), C.POINTER(C.c_void_p)]
    x.XQueryTree.argtypes = [D, W, C.POINTER(W), C.POINTER(W), C.POINTER(C.POINTER(W)), C.POINTER(C.c_uint)]
    x.XTranslateCoordinates.argtypes = [D, W, W, C.c_int, C.c_int, C.POINTER(C.c_int), C.POINTER(C.c_int), C.POINTER(W)]
    x.XGetGeometry.argtypes = [D, W, C.POINTER(W), C.POINTER(C.c_int), C.POINTER(C.c_int), C.POINTER(C.c_uint), C.POINTER(C.c_uint), C.POINTER(C.c_uint), C.POINTER(C.c_uint)]
    x.XFree.argtypes = [C.c_void_p]; x.XCloseDisplay.argtypes = [D]; x.XFlush.argtypes = [D]
    display = x.XOpenDisplay(os.environ['DISPLAY'].encode())
    if not display:
        raise NativeUnavailable('native_display_unavailable')
    try:
        root = x.XDefaultRootWindow(display)
        def prop(window, name):
            actual, fmt, count, remaining, pointer = U(), C.c_int(), U(), U(), C.c_void_p()
            status = x.XGetWindowProperty(display, window, x.XInternAtom(display, name.encode(), 0), 0, 65536, 0, 0,
                C.byref(actual), C.byref(fmt), C.byref(count), C.byref(remaining), C.byref(pointer))
            if status or not pointer:
                return []
            try:
                return list(C.cast(pointer, C.POINTER(U))[:count.value]) if fmt.value == 32 else []
            finally:
                x.XFree(pointer)
        windows = set(prop(root, '_NET_CLIENT_LIST'))
        parent, returned_root, children, count = W(), W(), C.POINTER(W)(), C.c_uint()
        if x.XQueryTree(display, root, C.byref(returned_root), C.byref(parent), C.byref(children), C.byref(count)):
            try:
                windows.update(children[:min(count.value, 4000)])
            finally:
                if children:
                    x.XFree(children)
        candidates = []
        for window in windows:
            owner = prop(window, '_NET_WM_PID')
            if not owner or owner[0] != process['pid']:
                continue
            rw, px, py, width, height, border, depth = W(), C.c_int(), C.c_int(), C.c_uint(), C.c_uint(), C.c_uint(), C.c_uint()
            if not x.XGetGeometry(display, window, C.byref(rw), C.byref(px), C.byref(py), C.byref(width), C.byref(height), C.byref(border), C.byref(depth)):
                continue
            lx, ly, child = C.c_int(), C.c_int(), W()
            x.XTranslateCoordinates(display, root, window, gx, gy, C.byref(lx), C.byref(ly), C.byref(child))
            # Match this accessible frame, rather than any overlapping window
            # owned by the same process (e.g. the chat behind its history).
            origin_x, origin_y, unused = C.c_int(), C.c_int(), W()
            x.XTranslateCoordinates(display, window, root, 0, 0, C.byref(origin_x), C.byref(origin_y), C.byref(unused))
            if (abs(origin_x.value - frame.x) <= 2 and abs(origin_y.value - frame.y) <= 2
                    and abs(width.value - frame.width) <= 2 and abs(height.value - frame.height) <= 2
                    and 0 <= lx.value < width.value and 0 <= ly.value < height.value):
                candidates.append((width.value * height.value, window, lx.value, ly.value))
        if not candidates:
            raise NativeUnavailable('native_control_outside_verified_window')
        _, window, lx, ly = min(candidates)
        if voice_bubble:
            if button != 3 or not re.fullmatch(r'Audio\d+"sec(?: \d{2}:\d{2}|Unplayed)?', node.get_name() or ''):
                raise NativeUnavailable('native_voice_control_changed')
            # This build exposes each history row as one accessible leaf. Find
            # its visible voice bubble by its reviewed native background color;
            # no text recognition or guessed fixed screen coordinate is used.
            class XI(C.Structure):
                _fields_ = [('width', C.c_int), ('height', C.c_int), ('xoffset', C.c_int), ('format', C.c_int), ('data', C.c_void_p), ('byte_order', C.c_int), ('bitmap_unit', C.c_int), ('bitmap_bit_order', C.c_int), ('bitmap_pad', C.c_int), ('depth', C.c_int), ('bytes_per_line', C.c_int), ('bits_per_pixel', C.c_int), ('red_mask', U), ('green_mask', U), ('blue_mask', U)]
            x.XGetImage.argtypes = [D, W, C.c_int, C.c_int, C.c_uint, C.c_uint, U, C.c_int]
            x.XGetImage.restype = C.POINTER(XI)
            x.XDestroyImage.argtypes = [C.POINTER(XI)]
            rx, ry = rectangle.x - frame.x, rectangle.y - frame.y
            capture_width = min(rectangle.width, frame.width-rx)
            capture_height = min(rectangle.height, frame.height-ry)
            if (rx < 0 or ry < 0 or rectangle.width-capture_width > 2
                    or rectangle.height-capture_height > 2 or rectangle.width > 2000 or rectangle.height > 300):
                raise NativeUnavailable('native_voice_control_clipped')
            pixels = x.XGetImage(display, window, rx, ry, capture_width, capture_height, U(-1).value, 2)
            if not pixels:
                raise NativeUnavailable('native_voice_bubble_unavailable')
            try:
                pic = pixels.contents
                if pic.bits_per_pixel != 32 or pic.byte_order != 0 or pic.red_mask != 0xff0000:
                    raise NativeUnavailable('native_voice_pixel_format_changed')
                raw = C.string_at(pic.data, pic.bytes_per_line * pic.height)
                points = [(cx, cy) for cy in range(pic.height // 3, pic.height)
                          for cx in range(pic.width // 2)
                          if raw[cy * pic.bytes_per_line + cx * 4:cy * pic.bytes_per_line + cx * 4 + 3] == b'\xf0\xee\xee']
                if len(points) < 700:
                    raise NativeUnavailable('native_voice_bubble_not_unique')
                xs, ys = zip(*points)
                x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
                if not (40 <= x0 < x1 and 50 <= x1-x0 <= 400 and 25 <= y1-y0 <= 65):
                    raise NativeUnavailable('native_voice_bubble_shape_changed')
                lx, ly = rx + (x0+x1)//2, ry + (y0+y1)//2
                gx, gy = frame.x + lx, frame.y + ly
            finally:
                x.XDestroyImage(pixels)
        x.XRaiseWindow.argtypes = [D, W]
        x.XSetInputFocus.argtypes = [D, W, C.c_int, U]
        if node.get_role_name() != 'menu item':
            x.XRaiseWindow(display, window)
            x.XSetInputFocus(display, window, 2, 0)
            x.XFlush(display)
            time.sleep(0.15)
        class Button(C.Structure):
            _fields_ = [('type', C.c_int), ('serial', U), ('send_event', C.c_int), ('display', D), ('window', W), ('root', W), ('subwindow', W), ('time', U), ('x', C.c_int), ('y', C.c_int), ('x_root', C.c_int), ('y_root', C.c_int), ('state', C.c_uint), ('button', C.c_uint), ('same_screen', C.c_int)]
        class Event(C.Union):
            _fields_ = [('button', Button), ('pad', C.c_long * 24)]
        x.XSendEvent.argtypes = [D, W, C.c_int, C.c_long, C.POINTER(Event)]
        state = {1: 256, 3: 1024, 4: 2048, 5: 4096}[button]
        for event_type, event_state in ((4, 0), (5, state)):
            event = Event(); event.button = Button(event_type, 0, 1, display, window, root, 0, 0, lx, ly, gx, gy, event_state, button, 1)
            x.XSendEvent(display, window, 1, 4 if event_type == 4 else 8, C.byref(event))
        x.XFlush(display)
    finally:
        x.XCloseDisplay(display)


def select_chat(process, root, Atspi, name):
    main = [n for n, _ in walk(root) if n.get_role_name() == 'frame' and n.get_name() == 'Weixin']
    if len(main) == 1 and any(n.get_role_name() == 'label' and n.get_name() == name and showing(n, Atspi)
                              for n, _ in walk(main[0])):
        return  # Clicking the already selected chat toggles it closed.
    found = []
    for node, path in walk(root):
        if node.get_role_name() != 'list' or node.get_name() != 'Chats':
            continue
        for index in range(min(node.get_child_count(), 200)):
            item = node.get_child_at_index(index)
            if showing(item, Atspi) and (item.get_name() or '').split('\n', 1)[0] == name:
                found.append(item)
    if len(found) != 1:
        raise NativeUnavailable('native_chat_not_unique_or_visible')
    _click(process, found[0], Atspi)


def open_history(process, root, Atspi):
    matches = [node for node, _ in walk(root) if node.get_role_name() == 'button'
               and node.get_name() in {'Chat History', '聊天记录'} and showing(node, Atspi)]
    if len(matches) != 1:
        raise NativeUnavailable('native_history_control_not_unique')
    _click(process, matches[0], Atspi)


def _one(root, Atspi, role, name):
    nodes = [n for n, _ in walk(root) if n.get_role_name() == role
             and n.get_name() == name and showing(n, Atspi)]
    if len(nodes) != 1:
        raise NativeUnavailable('native_target_not_unique')
    return nodes[0]


def _visible_bounds(node, Atspi):
    bounds = None
    while node is not None and node.get_role_name() != 'application':
        rect = node.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
        if rect.width > 0 and rect.height > 0:
            current = (rect.x, rect.y, rect.x + rect.width, rect.y + rect.height)
            bounds = current if bounds is None else (max(bounds[0], current[0]), max(bounds[1], current[1]),
                                                      min(bounds[2], current[2]), min(bounds[3], current[3]))
        node = node.get_parent()
    return bounds


def _member_filter(process, root, frame, Atspi, name):
    tab = _one(frame, Atspi, 'page tab', 'Group Members')
    for _ in range(3):
        area = _visible_bounds(tab, Atspi)
        if area and area[2] - area[0] > 30:
            break
        bar = _one(frame, Atspi, 'page tab list', 'Message Type')
        rect = bar.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
        arrows = []
        for node, _ in walk(frame):
            if node.get_role_name() != 'button' or node.get_name() or not showing(node, Atspi):
                continue
            box = node.get_component_iface().get_extents(Atspi.CoordType.SCREEN)
            if abs(box.y - rect.y) <= 2 and 0 < box.width < 30 and box.x > rect.x:
                # Parent/child button aliases have the same extents.
                if not any(box.x == previous[0] for previous in arrows):
                    arrows.append((box.x, node))
        if len(arrows) != 1:
            raise NativeUnavailable('native_history_filter_clipped')
        _click(process, arrows[0][1], Atspi)
        time.sleep(0.2)
    area = _visible_bounds(tab, Atspi)
    if not area or area[2] - area[0] <= 30:
        raise NativeUnavailable('native_history_filter_clipped')
    _click(process, tab, Atspi)
    time.sleep(0.2)
    popups = []
    for node, path in walk(root):
        if len(path) != 1 or node.get_role_name() != 'filler' or not showing(node, Atspi):
            continue
        fields = [n for n, _ in walk(node) if n.get_role_name() == 'text'
                  and n.get_name() == 'Search' and 'EditableText' in n.get_interfaces() and showing(n, Atspi)]
        lists = [n for n, _ in walk(node) if n.get_role_name() == 'list' and not n.get_name() and showing(n, Atspi)]
        if len(fields) == 1 and len(lists) == 1:
            popups.append((node, fields[0]))
    if len(popups) != 1:
        raise NativeUnavailable('native_member_filter_changed')
    popup, field = popups[0]
    # This exact detached history-member popup has been verified on this build.
    # It cannot send a message, and no key event is generated.
    if not field.get_editable_text_iface().set_text_contents(name):
        raise NativeUnavailable('native_member_filter_unavailable')
    time.sleep(0.3)
    item = _one(popup, Atspi, 'list item', name)
    _click(process, item, Atspi)
    time.sleep(0.3)
    _one(frame, Atspi, 'label', 'Group Members')


def _load_target(job):
    """Bind an internal job to an unambiguous source signature before UI use."""
    import sqlite3
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from chatlog_keeper import client_health
    from chatlog_keeper.core._path_resolver import data_dir
    state = Path(job.get('state_dir') or data_dir()).resolve()
    if not (state / 'archive' / 'archive.sqlite').is_file():
        raise NativeUnavailable('native_archive_unavailable')
    connection = sqlite3.connect((state / 'archive' / 'archive.sqlite').as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute('SELECT * FROM messages WHERE message_ref=?', (job.get('message_ref'),)).fetchone()
        keys = ('account_id', 'group_id', 'server_id', 'create_time', 'sender_id')
        if row is None or row['msg_type'] != 34 or any(row[key] != job.get(key) for key in keys):
            raise NativeUnavailable('native_archive_identity_mismatch')
        duration = json.loads(row['attachment'] or '{}').get('voice_length_ms')
        if type(duration) is not int or duration != job.get('voice_length_ms') or duration <= 0:
            raise NativeUnavailable('native_voice_duration_unverified')
        if client_health._account_binding(row['account_id']) != 'single_account_scope':
            raise NativeUnavailable('native_account_not_unique')
        if job.get('build') != client_health._VERIFIED_BUILD:
            raise NativeUnavailable('native_client_build_not_verified')
        groups = connection.execute('SELECT group_ref,name FROM groups WHERE account_id=? AND name=?',
                                    (row['account_id'], job.get('group_name'))).fetchall()
        if len(groups) != 1 or groups[0]['group_ref'] != row['group_ref']:
            raise NativeUnavailable('native_group_not_unique')
        members = [json.loads(r[0]) for r in connection.execute('SELECT payload FROM group_members WHERE group_ref=?', (row['group_ref'],))]
        member = [m for m in members if m.get('sender_id') == row['sender_id']]
        if len(member) != 1 or not member[0].get('display_name'):
            raise NativeUnavailable('native_sender_not_in_directory')
        name = member[0]['display_name']
        if sum(m.get('display_name') == name for m in members) != 1:
            raise NativeUnavailable('native_sender_name_not_unique')
        created = datetime.fromtimestamp(row['create_time'], ZoneInfo('Asia/Shanghai'))
        # Reviewed UI renders same-day history audio with HH:MM. Other date
        # layouts are deliberately unsupported until separately accepted.
        if created.date() != datetime.now(ZoneInfo('Asia/Shanghai')).date():
            raise NativeUnavailable('native_history_date_not_verified')
        candidates = []
        for other in connection.execute('SELECT message_ref,attachment FROM messages WHERE account_id=? AND group_id=? AND msg_type=34 AND create_time>=? AND create_time<?',
                                        (row['account_id'], row['group_id'], row['create_time']//60*60, row['create_time']//60*60+60)):
            length = json.loads(other['attachment'] or '{}').get('voice_length_ms')
            if type(length) is int:
                candidates.append((other['message_ref'], length))
        return {'state_dir': state, 'sender_display_name': name, 'minute': created.strftime('%H:%M'),
                'duration': duration, 'candidates': candidates}
    finally:
        connection.close()


def _match_audio(nodes, target, message_ref, *, history):
    found = []
    pattern = r'Audio(\d+)"sec ' + re.escape(target['minute']) if history else r'Audio(\d+)"sec(.*)'
    anchor = None
    for node in nodes:
        name = node.get_name() or ''
        if re.fullmatch(r'\d{2}:\d{2}', name):
            anchor = name
        match = re.fullmatch(pattern, name)
        if not match or (not history and anchor != target['minute']):
            continue
        seconds = int(match.group(1))
        if abs(seconds - target['duration']/1000) >= 1:
            continue
        candidates = [ref for ref, ms in target['candidates'] if abs(seconds-ms/1000) < 1]
        if candidates != [message_ref]:
            raise NativeUnavailable('native_voice_signature_ambiguous')
        found.append((node, '' if history else match.group(2)))
    if len(found) != 1:
        raise NativeUnavailable('native_voice_not_unique_or_visible')
    return found[0]


def _main_voice(root, Atspi, target, job):
    main = _one(root, Atspi, 'frame', 'Weixin')
    _one(main, Atspi, 'label', job['group_name'])
    messages = _one(main, Atspi, 'list', 'Messages')
    nodes = [messages.get_child_at_index(i) for i in range(min(messages.get_child_count(), 200))]
    return _match_audio(nodes, target, job['message_ref'], history=False)


def _cached_voice(job):
    """Prove the client still has the audio before any native conversion.

    The GI Python has no database crypto dependency. Its short-lived companion
    uses the collector's interpreter and cached keys, without acquiring keys or
    downloading anything. Both run outside the collector's polling thread.
    """
    import subprocess
    interpreter = job.get('source_python')
    if not isinstance(interpreter, str) or not Path(interpreter).is_absolute():
        raise NativeUnavailable('native_source_probe_unavailable')
    try:
        probe_job = {**job, 'parent_pid': os.getpid()}
        result = subprocess.run([interpreter, '-B', '-m', 'chatlog_keeper.native_cache_probe'],
            input=json.dumps(probe_job).encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=12, check=False)
        if result.returncode != 0 or len(result.stdout) > 16384:
            raise NativeUnavailable('native_source_probe_failed')
        proof = json.loads(result.stdout)
        if not isinstance(proof, dict) or proof.get('status') != 'cached':
            raise NativeUnavailable('native_source_audio_not_cached')
        identity = proof.get('proof')
        if not isinstance(identity, dict) or any(identity.get(key) != job.get(key)
                for key in ('message_ref', 'account_id', 'group_id', 'server_id', 'create_time')):
            raise NativeUnavailable('native_source_identity_mismatch')
    except (OSError, ValueError, subprocess.SubprocessError):
        raise NativeUnavailable('native_source_probe_failed') from None


def transcribe(job):
    import fcntl
    import signal
    from chatlog_keeper import client_health
    # Enforce both a child-owned deadline and parent death before interacting.
    parent = os.getppid()
    expected_parent = job.get('parent_pid', parent)
    libc = C.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0 or os.getppid() != expected_parent:
        raise NativeUnavailable('native_worker_parent_changed')
    def expired(*_):
        raise NativeUnavailable('native_transcription_timeout')
    signal.signal(signal.SIGALRM, expired)
    signal.alarm(90)
    target = _load_target(job)
    _cached_voice(job)
    lock = os.open(target['state_dir'] / 'native-ui.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    owned_frame = None
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise NativeUnavailable('native_ui_busy')
        process, Atspi, root = context()
        for frame, _ in walk(root):
            if frame.get_role_name() == 'frame' and (frame.get_name() or '').startswith('Chat history of'):
                raise NativeUnavailable('native_existing_history_window')
        select_chat(process, root, Atspi, job['group_name'])
        time.sleep(0.3)
        main = _one(root, Atspi, 'frame', 'Weixin')
        _one(main, Atspi, 'label', job['group_name'])
        open_history(process, root, Atspi)
        time.sleep(0.3)
        frames = [n for n, _ in walk(root) if n.get_role_name() == 'frame'
                  and re.fullmatch(r'Chat history of "' + re.escape(job['group_name']) + r'"\(\d+\)', n.get_name() or '') and showing(n, Atspi)]
        if len(frames) != 1:
            raise NativeUnavailable('native_history_group_changed')
        frame = frames[0]
        owned_frame = frame
        _member_filter(process, root, frame, Atspi, target['sender_display_name'])
        history = _one(frame, Atspi, 'list', 'Chat History')
        nodes = [history.get_child_at_index(i) for i in range(min(history.get_child_count(), 200))]
        voice, _ = _match_audio(nodes, target, job['message_ref'], history=True)
        _click(process, voice, Atspi, 3, voice_bubble=True)
        time.sleep(0.15)
        _click(process, _one(root, Atspi, 'menu item', 'Find in Chat'), Atspi)
        time.sleep(0.3)
        _click(process, _one(frame, Atspi, 'button', 'Close'), Atspi)
        time.sleep(0.2)
        voice, suffix = _main_voice(root, Atspi, target, job)
        requested = False
        if not suffix or suffix == 'Unplayed':
            _click(process, voice, Atspi, 3, voice_bubble=True)
            time.sleep(0.15)
            _click(process, _one(root, Atspi, 'menu item', 'Audio to Text'), Atspi)
            requested = True
        deadline = time.monotonic() + 75
        while True:
            current, _, _ = context()
            if current['pid'] != process['pid'] or client_health._account_binding(job['account_id']) != 'single_account_scope':
                raise NativeUnavailable('native_account_or_process_changed')
            _, suffix = _main_voice(root, Atspi, target, job)
            if suffix and suffix != 'Unplayed':
                if suffix.strip().casefold() in {'conversion failed', 'unable to convert', '转文字失败'}:
                    raise NativeUnavailable('native_transcript_unavailable')
                if suffix.strip().casefold() in {'converting...', 'converting', '转换中'}:
                    if time.monotonic() >= deadline:
                        raise NativeUnavailable('native_transcription_timeout')
                    time.sleep(0.3)
                    continue
                if len(suffix) > 50000:
                    raise NativeUnavailable('native_transcript_too_large')
                proof = {key: job[key] for key in ('account_id', 'group_id', 'server_id', 'create_time', 'sender_id', 'voice_length_ms', 'build')}
                return {'status': 'succeeded', 'text': suffix, 'proof': proof,
                        'provenance': {'source': 'wechat_native_accessibility', 'version': 1, 'build': job['build'],
                            'located_by': ['single_account_scope', 'cached_source_audio', 'exact_group', 'unique_group_member', 'minute_duration_unique',
                                           'find_in_chat', 'native_audio_to_text' if requested else 'existing_native_text', 'same_accessible_voice_row']}}
            if time.monotonic() >= deadline:
                raise NativeUnavailable('native_transcription_timeout')
            time.sleep(0.3)
    finally:
        if owned_frame is not None:
            try:
                if showing(owned_frame, Atspi):
                    _click(process, _one(owned_frame, Atspi, 'button', 'Close'), Atspi)
            except Exception:
                pass  # Never act on a different window to clean up a task.
        os.close(lock)
        signal.alarm(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inspect', action='store_true')
    parser.add_argument('--select-chat')
    parser.add_argument('--open-history', action='store_true')
    parser.add_argument('--transcribe', action='store_true')
    args = parser.parse_args()
    try:
        if args.transcribe:
            value = sys.stdin.buffer.read(16385)
            if len(value) > 16384:
                raise NativeUnavailable('native_job_too_large')
            job = json.loads(value)
            if not isinstance(job, dict):
                raise NativeUnavailable('native_job_invalid')
            print(json.dumps(transcribe(job), ensure_ascii=False))
            return
        process, Atspi, root = context()
        if args.select_chat:
            select_chat(process, root, Atspi, args.select_chat)
            time.sleep(0.4)
        if args.open_history:
            open_history(process, root, Atspi)
            time.sleep(0.5)
        print(json.dumps({'status': 'ready', 'controls': snapshot(root, Atspi)}, ensure_ascii=False))
    except NativeUnavailable as exc:
        reason = str(exc)
        status = ('needs_login' if reason == 'native_login_required' else
                  'timeout' if reason == 'native_transcription_timeout' else
                  'ambiguous' if any(v in reason for v in ('not_unique', 'ambiguous', 'changed', 'mismatch')) else 'unsupported')
        print(json.dumps({'status': status if args.transcribe else 'unavailable', 'reason': reason}))
    except Exception:
        print(json.dumps({'status': 'failed' if args.transcribe else 'unavailable', 'reason': 'native_accessibility_operation_failed'}))


if __name__ == '__main__':
    main()
