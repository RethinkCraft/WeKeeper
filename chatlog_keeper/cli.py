"""Native Linux WeChat: keys, groups, bounded messages and local images."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import io
import json
import logging
import os
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

from chatlog_keeper import wechat_db
from chatlog_keeper.core._linux import official_wechat_executables, process_pids
from chatlog_keeper.core._secrets import private_binary_writer

ZONE = ZoneInfo("Asia/Shanghai")


class ReaderError(RuntimeError):
    """A short error code without credentials or message content."""


def account_dir(data_root=None, account=None):
    root = Path(data_root).expanduser() if data_root else wechat_db.find_weixin_data_root()
    if root is None:
        raise ReaderError("wechat_data_not_found")
    matches = [p for p in wechat_db.find_wxid_dirs(root) if account is None or p.name == account]
    if not matches:
        raise ReaderError("account_not_found")
    if len(matches) != 1:
        raise ReaderError("multiple_accounts_use_account_option")
    return Path(root), matches[0]


@contextmanager
def reader_for(args):
    root, account = account_dir(args.data_root, args.account)
    reader = wechat_db.WeChatDBReader(root, account.name, allow_live_key_extract=False)
    try:
        if not reader.initialize() or not reader.enc_keys:
            raise ReaderError("cached_database_key_unavailable_run_extract_key")
        if len(reader.enc_keys) != len(wechat_db.find_msg_databases(account)):
            raise ReaderError("some_message_databases_remain_locked")
        yield reader
    finally:
        wechat_db._decrypt_cache_clear(retry_failed=False)


def group_rows(reader):
    return [{"id": native_id, "name": display}
            for native_id, display in sorted(reader._load_contacts()._wxid_to_display.items())
            if native_id.endswith("@chatroom")]


def select_group(reader, name):
    matches = [g["id"] for g in group_rows(reader) if name in (g["id"], g["name"])]
    if len(matches) != 1:
        raise ReaderError("group_not_found" if not matches else "group_name_ambiguous_use_id")
    return matches[0]


def message_dict(message):
    # Never serialize the XML attachment credential as ordinary metadata.
    attachment = {k: v for k, v in (message.attachment_meta or {}).items()
                  if k in {"kind", "filename", "file_md5", "md5", "length", "total_bytes",
                           "fileext", "voice_length_ms", "encryver", "width", "height"}}
    text = re.sub(r'''(\baeskey\s*=\s*["'])[^"']*''', r'\1[redacted]',
                  message.content, flags=re.IGNORECASE)
    text = re.sub(r'(<aeskey>).*?(</aeskey>)', r'\1[redacted]\2',
                  text, flags=re.IGNORECASE | re.DOTALL)
    return {"id": message.server_id, "time": message.timestamp.isoformat(),
            "type": message.msg_type, "sender": message.sender,
            "sender_name": message.sender_display_name, "group": message.chat_name,
            "text": text, "attachment": attachment}


def probe(args):
    root = Path(args.data_root).expanduser() if args.data_root else wechat_db.find_weixin_data_root()
    accounts = []
    for account in wechat_db.find_wxid_dirs(root) if root else []:
        databases = wechat_db.find_msg_databases(account)
        key = wechat_db.load_cached_wechat_key_for_account(account.name)
        verified = 0
        if key:
            for database in databases:
                page = wechat_db._read_stable_page1(database)
                verified += bool(page and wechat_db._verify_key_v4(key, page))
        accounts.append({"id": account.name, "databases": len(databases), "unlocked": verified})
    return {"client_found": bool(official_wechat_executables()),
            "client_running": bool(process_pids(("wechat", "WeChat"))), "accounts": accounts}


def extract_key(args):
    from chatlog_keeper.active_key import extract_wechat_key_active
    from chatlog_keeper.linux_key import last_error
    _, account = account_dir(args.data_root, args.account)
    databases = wechat_db.find_msg_databases(account)
    if not databases:
        raise ReaderError("message_database_missing_login_first")
    key = extract_wechat_key_active(db_path=str(databases[0]), timeout=args.timeout)
    if key is None:
        raise ReaderError(last_error() or "database_key_not_captured")
    if not wechat_db.save_cached_wechat_key_for_account(key, account.name, databases[0]):
        raise ReaderError("database_key_verification_or_save_failed")
    return {"ok": True, "database_key_verified": True, "key_saved": True}


def messages(args):
    now = datetime.now(ZONE)
    with reader_for(args) as reader:
        page = reader.read_conversation_page(
            conversation_id=select_group(reader, args.group),
            since_ts=int((now - timedelta(minutes=args.minutes)).timestamp()),
            until_ts=int(now.timestamp()), page_size=args.limit,
        )
        return {"messages": [message_dict(m) for m in page.messages],
                "has_more": page.has_more, "scanned_rows": page.scanned_rows}


def image_key(args):
    from chatlog_keeper.image_key import discover_uin
    root, account = account_dir(args.data_root, args.account)
    result = discover_uin(root, expected_account=account.name)
    if not result.get("found"):
        raise ReaderError(result.get("error", "image_key_discovery_failed"))
    return result


def image(args):
    from PIL import Image
    from chatlog_keeper.wechat_image import decode_image
    _, account = account_dir(args.data_root, args.account)
    raw = decode_image(Path(args.path).expanduser(), account_dir=account)
    if not raw:
        raise ReaderError("image_decode_failed_check_image_key_and_cache")
    with Image.open(io.BytesIO(raw)) as opened:
        width, height = opened.size
        fmt = opened.format
        opened.verify()
    with Image.open(io.BytesIO(raw)) as opened:
        opened.load()
    destination = Path(args.out).expanduser()
    with private_binary_writer(destination) as handle:
        handle.write(raw)
    return {"ok": True, "format": fmt, "width": width, "height": height,
            "output": str(destination)}


def collect(args):
    from chatlog_keeper.archive import ArchiveStore
    from chatlog_keeper.archive_media import MediaArchiver
    from chatlog_keeper.client_health import probe_client
    from chatlog_keeper.collector import Collector
    from chatlog_keeper.health_alerts import HealthAlerts
    state = Path(args.state_dir).expanduser() if args.state_dir else None
    if state is not None:
        os.environ["CHATLOG_KEEPER_DATA_DIR"] = str(state)

    def archive_media(collector, summary):
        if collector.account_dir is not None:
            summary["media"] = MediaArchiver(collector.store, collector.data_root,
                                             collector.account_dir).process_pending(limit=20, time_budget=10.0)

    with ArchiveStore(state) as store:
        alerts = HealthAlerts(store)

        def monitor_client(collector):
            # Source databases remain readable after logout. Never use a
            # successful archive read as proof that WeChat is connected.
            try:
                health = probe_client(account_id=collector.account_id)
            except Exception:
                health = {"state": "unknown", "reason": "client_probe_failed",
                          "checked_at": datetime.now(ZONE).timestamp(),
                          "process_running": None, "display_available": None}
            try:
                effective = collector.store.get_live_health(client_health=health)
                health["reception"] = {key: effective.get(key) for key in (
                    "state", "reason", "message_reception_confirmed", "needs_attention",
                    "last_recent_message_received_at", "seconds_without_new_records",
                )}
                health["notifications"] = alerts.observe(effective)
            except Exception as exc:
                health["notifications"] = {"status": "failed",
                                            "error": type(exc).__name__}
            return health

        selected_account = args.account or store.get_status().get("collector", {}).get("account_id")
        collector = Collector(store, data_root=args.data_root, account=selected_account,
                              poll_interval=args.interval, page_size=args.page_size,
                              after_cycle=archive_media, health_monitor=monitor_client)
        try:
            if args.once:
                return collector.collect_once()
            collector.run()
            return {"ok": True, "stopped": True}
        finally:
            collector.close()


def bounded_int(low, high):
    def parse(value):
        result = int(value)
        if not low <= result <= high:
            raise argparse.ArgumentTypeError(f"must be {low}..{high}")
        return result
    return parse


def parser():
    p = argparse.ArgumentParser(description="Native Linux WeChat local reader")
    commands = p.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("probe", "check client, accounts and cached keys"),
        ("extract-key", "launch WeChat to capture and verify its database key"),
        ("groups", "list group IDs and names"),
        ("messages", "read a bounded page from one selected group"),
        ("image-key", "initialize image-key metadata for the verified Linux build"),
        ("image", "decode one local cached image"),
        ("collect", "continuously archive all groups using cached keys only"),
        ("mcp", "serve archive tools and gated native voice requests over stdio"),
    ]:
        c = commands.add_parser(name, help=help_text)
        c.add_argument("--data-root", help="xwechat_files directory")
        c.add_argument("--account", help="account directory ID if multiple exist")
        if name == "extract-key":
            c.add_argument("--timeout", type=bounded_int(10, 600), default=180)
        elif name == "messages":
            c.add_argument("--group", required=True, help="exact group name or native ID")
            c.add_argument("--minutes", type=bounded_int(1, 1440), default=60)
            c.add_argument("--limit", type=bounded_int(1, 1000), default=100)
        elif name == "image":
            c.add_argument("path", help="one .dat file cached by WeChat")
            c.add_argument("--out", required=True)
        elif name == "collect":
            c.add_argument("--state-dir", help="private archive and key directory")
            c.add_argument("--interval", type=bounded_int(1, 3600), default=30)
            c.add_argument("--page-size", type=bounded_int(1, 1000), default=500)
            c.add_argument("--once", action="store_true", help="run one resumable collection cycle")
        elif name == "mcp":
            c.add_argument("--state-dir", help="existing private archive directory")
    return p


def main(argv=None):
    os.umask(0o077)
    args = parser().parse_args(argv)
    if not sys.platform.startswith("linux"):
        print(json.dumps({"ok": False, "error": "linux_required"}))
        return 2
    logging.basicConfig(level=logging.ERROR)
    try:
        if args.command == "mcp":
            from chatlog_keeper.mcp_server import run
            run(args.state_dir)
            return 0
        elif args.command == "groups":
            with reader_for(args) as reader:
                result = {"groups": group_rows(reader)}
        else:
            result = {"probe": probe, "extract-key": extract_key, "messages": messages,
                      "image-key": image_key, "image": image, "collect": collect}[args.command](args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2 if args.command == "collect" and result.get("errors") else 0
    except ReaderError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        if args.command == "mcp":
            print("mcp_start_failed: " + type(exc).__name__, file=sys.stderr)
            return 2
        code = getattr(exc, "code", None)
        if code is None and type(exc).__name__ == "CollectorError":
            code = str(exc)
        if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,80}", code):
            print(json.dumps({"ok": False, "error": code}))
            return 2
        print(json.dumps({"ok": False, "error": "operation_failed", "type": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
