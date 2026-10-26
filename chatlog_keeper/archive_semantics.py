"""Versioned, reparseable group semantics. No source or network access here."""
from __future__ import annotations

import hashlib
import time

SEMANTIC_SCHEMA = """
CREATE TABLE IF NOT EXISTS forwarded(
 forward_ref TEXT PRIMARY KEY,message_ref TEXT NOT NULL,path TEXT NOT NULL,
 parent_path TEXT NOT NULL,ordinal INTEGER NOT NULL,text TEXT NOT NULL,
 sender_id TEXT NOT NULL,payload TEXT NOT NULL,UNIQUE(message_ref,path));
CREATE INDEX IF NOT EXISTS forwarded_parent ON forwarded(message_ref,parent_path,ordinal);
CREATE TABLE IF NOT EXISTS forward_aliases(alias_ref TEXT PRIMARY KEY,forward_ref TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_directories(
 group_ref TEXT PRIMARY KEY,owner_id TEXT,coverage TEXT NOT NULL,
 fingerprint TEXT NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS group_members(
 group_ref TEXT NOT NULL,sender_id TEXT NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(group_ref,sender_id));
CREATE TABLE IF NOT EXISTS directory_history(
 observation_id INTEGER PRIMARY KEY,group_ref TEXT NOT NULL,observed_at REAL NOT NULL,
 fingerprint TEXT NOT NULL,payload TEXT NOT NULL);
"""


def _api():
    # Resolve lazily: ArchiveStore inherits this mixin.
    from chatlog_keeper.archive import ArchiveError, _json, _load, _ref, _safe, _limit
    return ArchiveError, _json, _load, _ref, _safe, _limit


class SemanticsMixin:
    def semantics_status(self, refs=None, begin=None, end=None):
        from chatlog_keeper.message_model import PARSER_VERSION
        clause, args = "1=1", []
        if refs:
            clause = "m.group_ref IN (" + ",".join("?" for _ in refs) + ")"
            args.extend(refs)
        if begin is not None:
            clause += " AND m.create_time>=? AND m.create_time<=?"
            args.extend((begin, end))
        return {"parser_version": PARSER_VERSION,
                "reparse_pending": self.conn.execute("SELECT COUNT(*) FROM messages m WHERE " + clause + " AND parser_version<?", (*args, PARSER_VERSION)).fetchone()[0],
                "missing_mention_metadata": self.conn.execute("SELECT COUNT(*) FROM messages m WHERE " + clause + " AND COALESCE(json_extract(semantic,'$.mentions_status'),'missing_metadata')='missing_metadata'", args).fetchone()[0],
                "mention_metadata_errors": self.conn.execute("SELECT COUNT(*) FROM messages m WHERE " + clause + " AND json_extract(semantic,'$.mentions_status')='parse_error'", args).fetchone()[0],
                "metadata_backfill_pending": self.conn.execute("SELECT COUNT(*) FROM origins o JOIN messages m USING(message_ref) JOIN sources s ON s.source_ref=o.source_ref AND s.generation=o.generation WHERE " + clause + " AND source_metadata_checked=0 AND s.available=1", args).fetchone()[0],
                "unavailable_metadata_origins": self.conn.execute("SELECT COUNT(*) FROM origins o JOIN messages m USING(message_ref) JOIN sources s ON s.source_ref=o.source_ref WHERE " + clause + " AND source_metadata_checked=0 AND (s.generation<>o.generation OR s.available=0)", args).fetchone()[0],
                "metadata_source_conflicts": self.conn.execute("SELECT COUNT(*) FROM origins o JOIN messages m USING(message_ref) WHERE " + clause + " AND source_metadata_checked=-1", args).fetchone()[0]}

    def semantic_stats(self, refs, begin, end):
        where = "m.group_ref IN (" + ",".join("?" for _ in refs) + ") AND m.create_time>=? AND m.create_time<=?"
        args = (*refs, begin, end)
        forwarded = self.conn.execute("SELECT COUNT(*),COUNT(DISTINCT f.message_ref),COUNT(DISTINCT CASE WHEN f.sender_id<>'' THEN f.sender_id ELSE NULLIF(json_extract(f.payload,'$.sender_name'),'') END) FROM forwarded f JOIN messages m USING(message_ref) WHERE " + where, args).fetchone()
        mentions = self.conn.execute("SELECT COUNT(*),COALESCE(SUM(json_extract(semantic,'$.mention_all')=1),0) FROM messages m WHERE " + where + " AND json_extract(semantic,'$.mentions_status')='verified' AND json_array_length(mentions)>0", args).fetchone()
        return {"forwarded_items": forwarded[0], "forwarded_parent_messages": forwarded[1],
                "embedded_author_labels": forwarded[2], "messages_with_mentions": mentions[0],
                "messages_mentioning_all": mentions[1],
                "events_by_type": {r[0]: r[1] for r in self.conn.execute("SELECT COALESCE(json_extract(event,'$.event_type'),'unknown'),COUNT(*) FROM messages m WHERE " + where + " AND event IS NOT NULL GROUP BY 1", args)},
                "semantics": self.semantics_status(refs, begin, end)}

    def _migrate_semantics(self):
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(messages)")}
        if "semantic" not in columns:
            self.conn.execute("ALTER TABLE messages ADD COLUMN semantic TEXT NOT NULL DEFAULT '{}'")
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(origins)")}
        for column, spec in (("source_compression", "INTEGER"),
                             ("source_metadata_checked", "INTEGER NOT NULL DEFAULT 0")):
            if column not in columns:
                self.conn.execute(f"ALTER TABLE origins ADD COLUMN {column} {spec}")
        # executescript commits implicitly. Keep the version upgrade and tables atomic.
        for statement in SEMANTIC_SCHEMA.split(";"):
            if statement.strip():
                self.conn.execute(statement)

    def _store_semantics(self, message_ref, parsed, existing=True):
        from chatlog_keeper.message_model import PARSER_VERSION
        _, encode, load, ref, safe, _ = _api()
        semantic = {k: parsed.get(k, default) for k, default in (
            ("mentions_status", "missing_metadata"), ("mention_all", False),
            ("forwarded_status", "not_applicable"))}
        old = self.conn.execute("SELECT semantic FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
        old_children = {r["path"]: dict(r) for r in self.conn.execute(
            "SELECT * FROM forwarded WHERE message_ref=?", (message_ref,))}
        new_rows = []

        def walk(children, parent=""):
            for ordinal, child in enumerate(children, 1):
                path = parent + ("." if parent else "") + str(ordinal)
                payload = safe({k: v for k, v in child.items() if k != "children"})
                payload["child_count"] = len(child.get("children") or [])
                previous = old_children.get(path)
                child_ref = previous["forward_ref"] if previous else ref("fwd_", message_ref, path)
                new_rows.append((child_ref, message_ref, path, parent, ordinal,
                                 payload.get("text") or "", payload.get("sender_id") or "", encode(payload)))
                walk(child.get("children") or [], path)

        walk(parsed.get("forwarded") or [])
        changed = (old and old[0] != encode(semantic)) or {
            p: r["payload"] for p, r in old_children.items()
        } != {r[2]: r[7] for r in new_rows}
        if existing and changed:
            self._bump_revision()
        self.conn.execute("UPDATE messages SET semantic=?,parser_version=? WHERE message_ref=?",
                          (encode(semantic), PARSER_VERSION, message_ref))
        self.conn.execute("DELETE FROM forwarded WHERE message_ref=?", (message_ref,))
        self.conn.executemany("INSERT INTO forwarded VALUES(?,?,?,?,?,?,?,?)", new_rows)
        self._seed_file_media(message_ref, parsed)
        self._store_p2_semantics(message_ref, parsed)

    def _merge_forwarded(self, old_ref, canonical):
        for row in self.conn.execute("SELECT * FROM forwarded WHERE message_ref=?", (old_ref,)).fetchall():
            target = self.conn.execute("SELECT forward_ref FROM forwarded WHERE message_ref=? AND path=?",
                                       (canonical, row["path"])).fetchone()
            if target:
                self.conn.execute("UPDATE media SET forward_ref=? WHERE forward_ref=?", (target[0], row["forward_ref"]))
                self.conn.execute("INSERT OR REPLACE INTO forward_aliases VALUES(?,?)",
                                  (row["forward_ref"], target[0]))
                self.conn.execute("UPDATE forward_aliases SET forward_ref=? WHERE forward_ref=?",
                                  (target[0], row["forward_ref"]))
                self.conn.execute("DELETE FROM forwarded WHERE forward_ref=?", (row["forward_ref"],))
            else:
                self.conn.execute("UPDATE forwarded SET message_ref=? WHERE forward_ref=?", (canonical, row["forward_ref"]))

    def _public_semantics(self, row):
        _, _, load, _, _, _ = _api()
        return {**load(row["semantic"], {}), "parser_version": row["parser_version"],
                "forwarded_count": self.conn.execute("SELECT COUNT(*) FROM forwarded WHERE message_ref=?", (row["message_ref"],)).fetchone()[0],
                "forwarded_direct_count": self.conn.execute("SELECT COUNT(*) FROM forwarded WHERE message_ref=? AND parent_path=''", (row["message_ref"],)).fetchone()[0]}

    def _public_identities(self, row):
        _, _, load, _, safe, _ = _api()

        def identity(sender_id):
            member = self.conn.execute("SELECT payload FROM group_members WHERE group_ref=? AND sender_id=?", (row["group_ref"], sender_id)).fetchone()
            return {"sender_id": sender_id, "directory_status": "observed" if member else "not_in_current_directory",
                    **(load(member[0], {}) if member else {})}

        return safe({"sender_identity": identity(row["sender_id"]),
                     "mention_targets": [{"sender_id": target, "kind": "all"} if target == "notify@all" else identity(target)
                                         for target in load(row["mentions"], [])]})

    def _public_event(self, row):
        _, _, load, _, safe, _ = _api()
        event = load(row["event"])
        if event and event.get("target_server_id"):
            if event.get("target_group_id") and event["target_group_id"] != row["group_id"]:
                return safe({**event, "target_ref": None, "target_status": "group_mismatch"})
            target = self.conn.execute("SELECT message_ref FROM messages WHERE account_id=? AND group_id=? AND server_id=?",
                                       (row["account_id"], row["group_id"], str(event["target_server_id"]))).fetchone()
            event = {**event, "target_ref": target[0] if target else None,
                     "target_status": "archived" if target else "not_archived"}
        return safe(event)

    def needs_reparse(self):
        from chatlog_keeper.message_model import PARSER_VERSION
        return bool(self.conn.execute("SELECT 1 FROM messages WHERE parser_version<? LIMIT 1", (PARSER_VERSION,)).fetchone())

    def reparse_batch(self, limit=500):
        from chatlog_keeper.message_model import parse_record, PARSER_VERSION
        _, encode, _, _, _, check = _api()
        self._write()
        check(limit)
        with self._lock, self.conn:
            rows = self.conn.execute("SELECT * FROM messages WHERE parser_version<? ORDER BY archive_id LIMIT ?", (PARSER_VERSION, limit)).fetchall()
            for row in rows:
                origin = self.conn.execute("SELECT * FROM origins WHERE message_ref=? ORDER BY (message_source IS NOT NULL) DESC,last_seen DESC LIMIT 1", (row["message_ref"],)).fetchone()
                if origin is None:
                    parsed = {"parse_status": "missing_origin", "msg_type": row["msg_type"], "text": row["text"]}
                else:
                    content = origin["raw_content"]
                    if origin["raw_is_text"] and content is not None:
                        content = bytes(content).decode("utf-8")
                    parsed = parse_record({"local_type": origin["raw_type"], "sender_id": row["sender_id"], "group_id": row["group_id"],
                                           "message_content": content, "message_source": origin["message_source"],
                                           "source_compression": origin["source_compression"],
                                           "packed_info_data": origin["packed_info_data"]})
                self.conn.execute("UPDATE messages SET msg_type=?,sub_type=?,text=?,parse_status=?,reply=?,attachment=?,mentions=?,event=? WHERE message_ref=?",
                                  (parsed.get("msg_type", row["msg_type"]), parsed.get("sub_type"), parsed.get("text", ""),
                                   parsed.get("parse_status", "parse_error"), encode(parsed.get("reply")) if parsed.get("reply") else None,
                                   encode(parsed.get("attachment")) if parsed.get("attachment") else None, encode(parsed.get("mentions", [])),
                                   encode(parsed.get("event")) if parsed.get("event") else None, row["message_ref"]))
                self._store_semantics(row["message_ref"], parsed)
            if rows:
                self._bump_revision()
            pending = self.conn.execute("SELECT COUNT(*) FROM messages WHERE parser_version<?", (PARSER_VERSION,)).fetchone()[0]
        return {"reparsed": len(rows), "pending": pending}

    def metadata_backfill_needed(self, source_ref, generation):
        return bool(self.conn.execute("SELECT 1 FROM origins WHERE source_ref=? AND generation=? AND source_metadata_checked=0 LIMIT 1", (source_ref, generation)).fetchone())

    def metadata_backfill_rows(self, source_ref, generation, table_name, limit=500):
        _api()[-1](limit)
        return [r[0] for r in self.conn.execute("SELECT row_id FROM origins WHERE source_ref=? AND generation=? AND table_name=? AND source_metadata_checked=0 ORDER BY row_id LIMIT ?", (source_ref, generation, table_name, limit))]

    def mark_metadata_checked(self, source_ref, generation, table_name, row_ids):
        self._write()
        with self._lock, self.conn:
            self.conn.executemany("UPDATE origins SET source_metadata_checked=1 WHERE source_ref=? AND generation=? AND table_name=? AND row_id=?", [(source_ref, generation, table_name, row) for row in row_ids])

    def backfill_source_metadata(self, source_ref, generation, table_name, rows):
        from chatlog_keeper.archive import _blob
        from chatlog_keeper.message_model import source_row_hash
        self._write()
        changed = 0
        with self._lock, self.conn:
            for raw in rows:
                origin = self.conn.execute("SELECT * FROM origins WHERE source_ref=? AND generation=? AND table_name=? AND row_id=?", (source_ref, generation, table_name, raw["row_id"])).fetchone()
                if origin is None:
                    continue
                # Never attach data from a recycled source row to historical messages.
                compatible = (str(origin["raw_server_id"] or "") == str(raw.get("server_id") or "")
                              and origin["raw_create_time"] == int(raw.get("create_time") or 0)
                              and origin["raw_type"] == raw.get("local_type")
                              and str(origin["raw_sender"]) == str(raw.get("real_sender_id", raw.get("sender_id", "")))
                              and origin["raw_content"] == _blob(raw.get("message_content"))
                              and origin["packed_info_data"] == _blob(raw.get("packed_info_data")))
                if compatible:
                    digest = source_row_hash(raw)
                    self.conn.execute("UPDATE origins SET message_source=?,source_compression=?,source_metadata_checked=1,payload_hash=? WHERE source_ref=? AND generation=? AND table_name=? AND row_id=?",
                                      (_blob(raw.get("message_source")), raw.get("source_compression"), digest, source_ref, generation, table_name, raw["row_id"]))
                    self.conn.execute("UPDATE streams SET boundary_hash=? WHERE source_ref=? AND generation=? AND table_name=? AND boundary_rowid=?", (digest, source_ref, generation, table_name, raw["row_id"]))
                    self.conn.execute("UPDATE messages SET parser_version=1 WHERE message_ref=?", (origin["message_ref"],))
                    changed += 1
                else:
                    self.conn.execute("UPDATE origins SET source_metadata_checked=-1 WHERE source_ref=? AND generation=? AND table_name=? AND row_id=?", (source_ref, generation, table_name, raw["row_id"]))
        return changed

    def _forward_row(self, reference):
        for _ in range(16):
            alias = self.conn.execute("SELECT forward_ref FROM forward_aliases WHERE alias_ref=?", (reference,)).fetchone()
            if not alias:
                break
            reference = alias[0]
        return self.conn.execute("SELECT * FROM forwarded WHERE forward_ref=?", (reference,)).fetchone()

    def _public_forward(self, row):
        _, _, load, _, safe, _ = _api()
        payload = load(row["payload"], {})
        media = self._forward_media_matches(row, payload)
        return safe({**payload, "forward_ref": row["forward_ref"], "message_ref": row["message_ref"],
                     "path": row["path"], "media": media,
                     "media_status": "archived" if media else "unresolved" if payload.get("attachment") else "not_applicable"})

    def _forward_media_matches(self, row, payload):
        attachment = payload.get("attachment") or {}
        digest = attachment.get("file_md5") or attachment.get("md5")
        if not digest or len(str(digest)) != 32:
            return []
        return [dict(r) for r in self.conn.execute(
            "SELECT x.media_ref,x.kind,x.status,x.tier FROM media x JOIN messages m ON m.message_ref=? WHERE x.account_id=m.account_id AND x.group_id=m.group_id AND lower(x.file_key)=lower(?) AND x.kind=? AND x.status='ready'",
            (row["message_ref"], digest, attachment.get("kind", "image")))]

    def forward_media_reference(self, reference):
        error, _, load, _, _, _ = _api()
        row = self._forward_row(reference)
        media = self._forward_media_matches(row, load(row["payload"], {})) if row else []
        if not media:
            raise error("forwarded_media_not_archived")
        return media[0]["media_ref"]

    def read_forwarded_messages(self, reference, cursor=None, limit=100):
        error, _, _, _, _, check = _api()
        check(limit)
        with self._lock, self._read_transaction():
            if reference.startswith("fwd_"):
                item = self._forward_row(reference)
                if item is None:
                    raise error("forwarded_item_not_found")
                message_ref, path = item["message_ref"], item["path"]
            else:
                item = None
                message_ref, path = self._resolve_ref(reference), ""
            parent = self.conn.execute("SELECT * FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
            if parent is None:
                raise error("message_not_found")
            after, revision = 0, self._revision()
            if cursor:
                decoded = self._cursor(cursor)
                if decoded.get("kind") != "forwarded" or decoded.get("ref") != message_ref or decoded.get("path") != path:
                    raise error("cursor_scope_changed")
                if decoded.get("revision") != revision:
                    raise error("cursor_snapshot_changed")
                after = decoded.get("after")
                if type(after) is not int or after < 0:
                    raise error("invalid_cursor")
            rows = self.conn.execute("SELECT * FROM forwarded WHERE message_ref=? AND parent_path=? AND ordinal>? ORDER BY ordinal LIMIT ?", (message_ref, path, after, limit + 1)).fetchall()
            page, more = rows[:limit], len(rows) > limit
            return {"message_ref": message_ref, "item": self._public_forward(item) if item else None,
                    "forwarded_status": self._public_semantics(parent).get("forwarded_status"),
                    "children": [self._public_forward(r) for r in page], "has_more": more,
                    "next_cursor": self._encode_cursor({"kind": "forwarded", "ref": message_ref, "path": path, "revision": revision, "after": page[-1]["ordinal"]}) if more else None,
                    "coverage": self._coverage([parent["group_ref"]])}

    def update_group_directory(self, account_id, groups):
        _, encode, _, ref, safe, _ = _api()
        self._write()
        self.upsert_groups(account_id, groups)
        with self._lock, self.conn:
            for group in groups:
                native = group.get("group_id") or group.get("id")
                group_ref = ref("group_", account_id, native)
                # A partial read must not erase a previously observed member roster.
                coverage = group.get("coverage") or {"status": "unknown"}
                members = sorted((safe(m) for m in group.get("members", []) if m.get("sender_id")), key=lambda m: m["sender_id"])
                if coverage.get("group_nickname_status") not in {None, "parsed"}:
                    from chatlog_keeper.archive import _load
                    previous_members = {r[0]: _load(r[1], {}) for r in self.conn.execute("SELECT sender_id,payload FROM group_members WHERE group_ref=?", (group_ref,))}
                    for member in members:
                        previous_member = previous_members.get(member["sender_id"], {})
                        if not member.get("group_nickname") and previous_member.get("group_nickname"):
                            member["group_nickname"] = previous_member["group_nickname"]
                            member["group_nickname_status"] = "stale_last_verified"
                            member["display_name"] = member.get("remark") or member["group_nickname"] or member.get("nickname") or member["sender_id"]
                snapshot = {"owner_id": group.get("owner_id"), "coverage": coverage, "members": members}
                fingerprint = hashlib.sha256(encode(snapshot).encode()).hexdigest()
                previous = self.conn.execute("SELECT fingerprint FROM group_directories WHERE group_ref=?", (group_ref,)).fetchone()
                if previous and previous[0] == fingerprint:
                    self.conn.execute("UPDATE group_directories SET updated_at=?,coverage=?,owner_id=? WHERE group_ref=?", (time.time(), encode(coverage), group.get("owner_id"), group_ref))
                    continue
                now = time.time()
                if coverage.get("status") not in {"complete", "locally_complete", "available"} and previous:
                    self.conn.execute("UPDATE group_directories SET coverage=?,updated_at=? WHERE group_ref=?", (encode(coverage), now, group_ref))
                    continue
                self.conn.execute("INSERT OR REPLACE INTO group_directories VALUES(?,?,?,?,?)", (group_ref, group.get("owner_id"), encode(coverage), fingerprint, now))
                self.conn.execute("DELETE FROM group_members WHERE group_ref=?", (group_ref,))
                self.conn.executemany("INSERT INTO group_members VALUES(?,?,?)", [(group_ref, m["sender_id"], encode(m)) for m in members])
                self.conn.execute("INSERT INTO directory_history(group_ref,observed_at,fingerprint,payload) VALUES(?,?,?,?)", (group_ref, now, fingerprint, encode(snapshot)))

    def list_group_members(self, group_id, cursor=None, limit=100):
        error, _, load, _, safe, check = _api()
        check(limit)
        with self._lock, self._read_transaction():
            group_ref = self._group_scope([group_id])[0]
            directory = self.conn.execute("SELECT * FROM group_directories WHERE group_ref=?", (group_ref,)).fetchone()
            after = ""
            if cursor:
                decoded = self._cursor(cursor)
                if decoded.get("kind") != "members" or decoded.get("group") != group_ref:
                    raise error("cursor_scope_changed")
                if not directory or decoded.get("fingerprint") != directory["fingerprint"]:
                    raise error("cursor_snapshot_changed")
                after = decoded.get("after")
                if not isinstance(after, str):
                    raise error("invalid_cursor")
            rows = self.conn.execute("SELECT * FROM group_members WHERE group_ref=? AND sender_id>? ORDER BY sender_id LIMIT ?", (group_ref, after, limit + 1)).fetchall()
            page, more = rows[:limit], len(rows) > limit
            return safe({"group_id": group_ref, "members": [load(r["payload"]) for r in page],
                         "member_count": self.conn.execute("SELECT COUNT(*) FROM group_members WHERE group_ref=?", (group_ref,)).fetchone()[0],
                         "owner_id": directory["owner_id"] if directory else None,
                         "directory_updated_at": directory["updated_at"] if directory else None,
                         "directory_coverage": load(directory["coverage"]) if directory else {"status": "not_collected"},
                         "history_scope": "observed local snapshots, not complete historical membership",
                         "has_more": more, "next_cursor": self._encode_cursor({"kind": "members", "group": group_ref, "fingerprint": directory["fingerprint"], "after": page[-1]["sender_id"]}) if more else None,
                         "coverage": self._coverage([group_ref])})
