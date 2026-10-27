"""Strict text-only export of explicitly selected groups from one read snapshot."""
from __future__ import annotations

from datetime import datetime
import json
from zoneinfo import ZoneInfo

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.mcp_server import _public
from .avatars import sender_reference

ZONE = ZoneInfo("Asia/Shanghai")


def list_local_groups(archive_dir):
    with ArchiveStore(archive_dir, readonly=True) as store:
        return [dict(row) for row in store.conn.execute("SELECT group_ref,name FROM groups ORDER BY name,group_ref")]


def projections(archive_dir, selected_groups, salt):
    if not selected_groups:
        raise ValueError("explicit_group_allowlist_required")
    if len(selected_groups) > 100 or len(set(selected_groups)) != len(selected_groups):
        raise ValueError("invalid_group_allowlist")
    with ArchiveStore(archive_dir, readonly=True) as store:
        with store._lock, store._read_transaction():
            refs = store._group_scope(selected_groups)
            coverage = store._coverage(refs)
            collector = coverage.get("collector", {})
            coverages = {c["group_id"]: c for c in coverage["groups"]}
            result = []
            for ref in refs:
                group = store.conn.execute("SELECT group_ref,name FROM groups WHERE group_ref=?", (ref,)).fetchone()
                cov = coverages[ref]
                safe_coverage = {
                    "local_backfill_complete": bool(cov["local_backfill_complete"]),
                    "source_available": not bool(cov["source_issues"]),
                    "sources_readable": collector.get("sources_readable"),
                    "sources_total": collector.get("sources_total"),
                    "backfill_streams_pending": cov["backfill_streams_pending"],
                    "gaps": sorted(set(str(i.get("error") or "source_unavailable") for i in cov["source_issues"])),
                }
                buckets = {}
                for row in store.conn.execute("SELECT * FROM messages WHERE group_ref=? ORDER BY create_time,message_ref", (ref,)):
                    day = datetime.fromtimestamp(row["create_time"], ZONE).date().isoformat()
                    sender = row["sender_id"]
                    opaque = sender_reference(salt, row["account_id"], sender)
                    texts = [row["text"]]
                    # Cached native text/card text remains evidence of the same message.
                    for derived in store.conn.execute("SELECT source_kind,text FROM p2_text_search WHERE message_ref=? ORDER BY source_kind,source_ref", (row["message_ref"],)):
                        if derived["text"] and derived["text"] not in texts:
                            label = "已有语音转字" if derived["source_kind"] == "transcript" else "卡片文字"
                            texts.append(f"[{label}] {derived['text']}")
                    reply = None
                    if row["reply"]:
                        raw = json.loads(row["reply"])
                        sid = str(raw.get("server_id") or "")
                        target = store.conn.execute("SELECT message_ref FROM messages WHERE account_id=? AND group_ref=? AND server_id=?", (row["account_id"], ref, sid)).fetchone() if sid else None
                        reply = {"target_ref": target[0] if target else None, "text": str(raw.get("text") or "")}
                    buckets.setdefault(day, []).append(_public({
                        "message_ref": row["message_ref"], "create_time": row["create_time"],
                        "sender_id": opaque, "sender_name": row["sender_name"],
                        "msg_type": row["msg_type"], "text": "\n".join(t for t in texts if t), "reply": reply,
                    }))
                # Empty groups still appear; a later deletion emits an empty old partition.
                if not buckets:
                    buckets[datetime.now(ZONE).date().isoformat()] = []
                for day, messages in buckets.items():
                    result.append({"group": dict(group), "day": day, "messages": messages, "coverage": safe_coverage})
            return result


def heartbeat(archive_dir, source_id, summary=None):
    import time
    try:
        with ArchiveStore(archive_dir, readonly=True) as store:
            status = store.get_status()
        c = status.get("collector", {})
        health = status.get("health", {})
        return {"source_id": source_id, "sent_at": time.time(),
                "health": {"state": health.get("state", "unknown"), "reason": health.get("reason", "unknown")},
                "collector": {"running": bool(c.get("running")), "last_cycle_at": c.get("last_cycle_at"), "last_error": c.get("last_error")},
                "last_message_at": status.get("latest_message_time"),
                "summary": summary or {"state": "not_configured", "last_success_at": None, "error": None}}
    except Exception:
        return {"source_id": source_id, "sent_at": time.time(),
                "health": {"state": "source_unavailable", "reason": "archive_unavailable"},
                "collector": {"running": False, "last_cycle_at": None, "last_error": "archive_unavailable"},
                "last_message_at": None,
                "summary": summary or {"state": "not_configured", "last_success_at": None, "error": None}}
