"""Local P2 records: card components, profiles, voice assets and native jobs.

No client interaction or network access belongs in this module. Query results
are source data; native transcription is explicitly queued only after its
separate real-client acceptance gate has been enabled.
"""
from __future__ import annotations

import hashlib
import os
import re
import stat
import time

P2_SCHEMA = """
CREATE TABLE IF NOT EXISTS card_components(
 item_ref TEXT PRIMARY KEY,message_ref TEXT NOT NULL,path TEXT NOT NULL,
 parent_path TEXT NOT NULL,ordinal INTEGER NOT NULL,text TEXT NOT NULL,payload TEXT NOT NULL,
 UNIQUE(message_ref,path));
CREATE INDEX IF NOT EXISTS card_components_parent ON card_components(message_ref,parent_path,ordinal);
CREATE TABLE IF NOT EXISTS component_aliases(alias_ref TEXT PRIMARY KEY,item_ref TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS profiles(
 account_id TEXT NOT NULL,subject_id TEXT NOT NULL,payload TEXT NOT NULL,
 avatar_ref TEXT,revision TEXT NOT NULL,updated_at REAL NOT NULL,
 PRIMARY KEY(account_id,subject_id));
CREATE TABLE IF NOT EXISTS profile_versions(
 version_ref TEXT PRIMARY KEY,account_id TEXT NOT NULL,subject_id TEXT NOT NULL,
 payload TEXT NOT NULL,avatar_ref TEXT,observed_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS avatar_assets(
 avatar_ref TEXT PRIMARY KEY,account_id TEXT NOT NULL,subject_id TEXT NOT NULL,
 sha256 TEXT NOT NULL,payload TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS voice_assets(
 asset_ref TEXT PRIMARY KEY,message_ref TEXT NOT NULL,sha256 TEXT NOT NULL,
 blob TEXT NOT NULL,source TEXT NOT NULL,created_at REAL NOT NULL,
 UNIQUE(message_ref,sha256));
CREATE TABLE IF NOT EXISTS voice_current(message_ref TEXT PRIMARY KEY,asset_ref TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_transcripts(
 transcript_ref TEXT PRIMARY KEY,message_ref TEXT NOT NULL,text TEXT NOT NULL,
 provenance TEXT NOT NULL,created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS native_transcripts_message ON native_transcripts(message_ref,created_at);
CREATE TABLE IF NOT EXISTS native_transcript_current(message_ref TEXT PRIMARY KEY,transcript_ref TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_voice_jobs(
 job_id TEXT PRIMARY KEY,message_ref TEXT NOT NULL,status TEXT NOT NULL,
 reason TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL,
 lease_until REAL NOT NULL DEFAULT 0,worker_id TEXT,details TEXT NOT NULL DEFAULT '{}');
CREATE UNIQUE INDEX IF NOT EXISTS native_voice_jobs_active ON native_voice_jobs(message_ref)
 WHERE status IN ('queued','running');
CREATE VIRTUAL TABLE IF NOT EXISTS p2_text_search USING fts5(
 text,message_ref UNINDEXED,source_kind UNINDEXED,source_ref UNINDEXED,tokenize='trigram');
"""


def _api():
    from chatlog_keeper.archive import ArchiveError, _json, _load, _ref, _safe, _limit
    return ArchiveError, _json, _load, _ref, _safe, _limit


class P2Mixin:
    def _migrate_p2(self):
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(messages)")}
        if "card" not in columns:
            self.conn.execute("ALTER TABLE messages ADD COLUMN card TEXT")
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(media)")}
        if "component_ref" not in columns:
            self.conn.execute("ALTER TABLE media ADD COLUMN component_ref TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS media_component ON media(component_ref)")
        for statement in P2_SCHEMA.split(";"):
            if statement.strip():
                self.conn.execute(statement)

    def _content_revision(self):
        row = self.conn.execute("SELECT value FROM meta WHERE key='p2_content_revision'").fetchone()
        return int(row[0]) if row else 0

    def _bump_content_revision(self):
        self.conn.execute("INSERT OR REPLACE INTO meta VALUES('p2_content_revision',?)", (str(self._content_revision() + 1),))

    def _store_p2_semantics(self, message_ref, parsed):
        _, encode, _, ref, safe, _ = _api()
        card = safe(parsed.get("card"))
        old = self.conn.execute("SELECT card FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
        encoded_card = encode(card) if card else None
        previous = {r["path"]: dict(r) for r in self.conn.execute("SELECT * FROM card_components WHERE message_ref=?", (message_ref,))}
        rows = []

        def walk(children, parent=""):
            for ordinal, child in enumerate(children, 1):
                path = parent + ("." if parent else "") + str(ordinal)
                payload = safe({k: v for k, v in child.items() if k != "children"})
                payload["child_count"] = len(child.get("children") or [])
                item_ref = previous[path]["item_ref"] if path in previous else ref("item_", message_ref, path)
                rows.append((item_ref, message_ref, path, parent, ordinal, payload.get("text") or "", encode(payload)))
                walk(child.get("children") or [], path)

        walk(parsed.get("card_components") or [])
        if (old and old[0] != encoded_card) or {p: r["payload"] for p, r in previous.items()} != {r[2]: r[6] for r in rows}:
            self._bump_content_revision()
        self.conn.execute("UPDATE messages SET card=? WHERE message_ref=?", (encoded_card, message_ref))
        self.conn.execute("DELETE FROM card_components WHERE message_ref=?", (message_ref,))
        self.conn.executemany("INSERT INTO card_components VALUES(?,?,?,?,?,?,?)", rows)
        excluded = {"kind", "status", "parse_status", "coordinate_system", "evidence", "media_status", "cache_status"}

        def strings(value):
            if isinstance(value, dict):
                return [text for key, child in value.items() if key not in excluded for text in strings(child)]
            if isinstance(value, list):
                return [text for child in value for text in strings(child)]
            return [value] if isinstance(value, str) and value else []

        self.conn.execute("DELETE FROM p2_text_search WHERE message_ref=? AND source_kind='card'", (message_ref,))
        text = "\n".join(strings(card))
        if text:
            self.conn.execute("INSERT INTO p2_text_search VALUES(?,?,?,?)", (text, message_ref, "card", message_ref))
        self.conn.executemany("INSERT INTO p2_text_search VALUES(?,?,?,?)", [(r[5], message_ref, "card", r[0]) for r in rows if r[5]])
        self._seed_component_media(message_ref)
        self._seed_additional_media(message_ref, parsed)

    def _seed_additional_media(self, message_ref, parsed):
        _, encode, load, ref, _, _ = _api()
        message = self.conn.execute("SELECT * FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
        entries = []
        top = parsed.get("attachment") or {}
        if top.get("kind") == "sticker":
            entries.append((None, top))
        for item in self.conn.execute("SELECT forward_ref,payload FROM forwarded WHERE message_ref=?", (message_ref,)):
            attachment = load(item["payload"], {}).get("attachment") or {}
            if attachment.get("kind") in {"image", "sticker", "voice"}:
                entries.append((item["forward_ref"], attachment))
        for forward_ref, attachment in entries:
            kind = attachment["kind"]
            key = str(attachment.get("file_md5") or attachment.get("md5") or "primary")
            media_ref = ref("media_", forward_ref or message_ref, kind, key)
            self.conn.execute("INSERT INTO media(media_ref,message_ref,account_id,group_id,create_time,kind,file_key,attachment,updated_at,forward_ref) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(media_ref) DO UPDATE SET attachment=excluded.attachment,updated_at=excluded.updated_at,forward_ref=excluded.forward_ref",
                              (media_ref, message_ref, message["account_id"], message["group_id"], message["create_time"], kind, key, encode(attachment), time.time(), forward_ref))

    def _seed_component_media(self, message_ref):
        _, encode, load, ref, _, _ = _api()
        message = self.conn.execute("SELECT * FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
        for item in self.conn.execute("SELECT item_ref,payload FROM card_components WHERE message_ref=?", (message_ref,)):
            attachment = load(item["payload"], {}).get("attachment") or {}
            kind = attachment.get("kind")
            if kind not in {"file", "image", "sticker", "voice"}:
                continue
            key = str(attachment.get("file_md5") or attachment.get("md5") or attachment.get("filename") or "primary")
            media_ref = ref("media_", item["item_ref"], kind, key)
            self.conn.execute("INSERT INTO media(media_ref,message_ref,account_id,group_id,create_time,kind,file_key,attachment,updated_at,component_ref) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(media_ref) DO UPDATE SET attachment=excluded.attachment,updated_at=excluded.updated_at,component_ref=excluded.component_ref",
                              (media_ref, message_ref, message["account_id"], message["group_id"], message["create_time"], kind, key, encode(attachment), time.time(), item["item_ref"]))

    def _component_row(self, reference):
        error, *_ = _api()
        for _ in range(16):
            alias = self.conn.execute("SELECT item_ref FROM component_aliases WHERE alias_ref=?", (reference,)).fetchone()
            if alias is None:
                return self.conn.execute("SELECT * FROM card_components WHERE item_ref=?", (reference,)).fetchone()
            reference = alias[0]
        raise error("invalid_component_alias")

    def component_media_records(self, reference):
        error, _, load, *_ = _api()
        item = self._component_row(reference)
        if item is None:
            raise error("card_component_not_found")
        attachment = load(item["payload"], {}).get("attachment") or {}
        kind = attachment.get("kind")
        if not kind:
            return []
        key = str(attachment.get("file_md5") or attachment.get("md5") or attachment.get("filename") or "primary")
        return [self._media_record(r) for r in self.conn.execute(
            "SELECT * FROM media WHERE component_ref=? AND kind=? AND file_key=? ORDER BY status='ready' DESC,media_ref",
            (item["item_ref"], kind, key))]

    def component_media_reference(self, reference, kind=None):
        error, *_ = _api()
        records = self.component_media_records(reference)
        if kind:
            records = [r for r in records if r["kind"] == kind]
        if not records:
            raise error("component_media_not_found")
        return records[0]["media_ref"]

    def _public_component(self, row):
        _, _, load, _, safe, _ = _api()
        records = self.component_media_records(row["item_ref"])
        media = [{k: r.get(k) for k in ("media_ref", "kind", "status", "reason", "mime", "width", "height", "tier")} for r in records]
        payload = load(row["payload"], {})
        return safe({**payload, "item_ref": row["item_ref"], "message_ref": row["message_ref"], "path": row["path"],
                     "media": media, "media_status": "archived" if any(r["status"] == "ready" for r in records) else "unresolved" if payload.get("attachment") else "not_applicable"})

    def read_card(self, reference, cursor=None, limit=100):
        error, _, load, _, safe, check = _api()
        check(limit)
        if not isinstance(reference, str) or not re.fullmatch(r"(?:msg|item)_[A-Za-z0-9_-]{1,200}", reference):
            raise error("invalid_card_reference")
        with self._lock, self._read_transaction():
            item = self._component_row(reference) if reference.startswith("item_") else None
            if reference.startswith("item_") and item is None:
                raise error("card_component_not_found")
            message_ref, path = (item["message_ref"], item["path"]) if item else (self._resolve_ref(reference), "")
            message = self.conn.execute("SELECT * FROM messages WHERE message_ref=?", (message_ref,)).fetchone()
            if message is None:
                raise error("message_not_found")
            card = load(message["card"])
            if card is None and item is None:
                raise error("card_not_found")
            after = 0
            revision = _api()[3]("cardrev_", message["card"],
                                 [(r[0], r[1]) for r in self.conn.execute("SELECT path,payload FROM card_components WHERE message_ref=? ORDER BY path", (message_ref,))])
            if cursor:
                decoded = self._cursor(cursor)
                if decoded.get("kind") != "card" or decoded.get("ref") != message_ref or decoded.get("path") != path:
                    raise error("cursor_scope_changed")
                if decoded.get("revision") != revision:
                    raise error("cursor_snapshot_changed")
                after = decoded.get("after")
                if type(after) is not int or after < 0:
                    raise error("invalid_cursor")
            rows = self.conn.execute("SELECT * FROM card_components WHERE message_ref=? AND parent_path=? AND ordinal>? ORDER BY ordinal LIMIT ?", (message_ref, path, after, limit + 1)).fetchall()
            page, more = rows[:limit], len(rows) > limit
            return safe({"message_ref": message_ref, "group_id": message["group_ref"], "card": card,
                         "item": self._public_component(item) if item else None,
                         "components": [self._public_component(r) for r in page], "has_more": more,
                         "next_cursor": self._encode_cursor({"kind": "card", "ref": message_ref, "path": path, "revision": revision, "after": page[-1]["ordinal"]}) if more else None,
                         "coverage": self._coverage([message["group_ref"]])})

    def _merge_p2(self, old_ref, canonical):
        for row in self.conn.execute("SELECT * FROM card_components WHERE message_ref=?", (old_ref,)).fetchall():
            other = self.conn.execute("SELECT item_ref FROM card_components WHERE message_ref=? AND path=?", (canonical, row["path"])).fetchone()
            if other:
                self.conn.execute("UPDATE media SET component_ref=? WHERE component_ref=?", (other[0], row["item_ref"]))
                self.conn.execute("UPDATE component_aliases SET item_ref=? WHERE item_ref=?", (other[0], row["item_ref"]))
                self.conn.execute("INSERT OR REPLACE INTO component_aliases VALUES(?,?)", (row["item_ref"], other[0]))
                self.conn.execute("DELETE FROM card_components WHERE item_ref=?", (row["item_ref"],))
            else:
                self.conn.execute("UPDATE card_components SET message_ref=? WHERE item_ref=?", (canonical, row["item_ref"]))
        self.conn.execute("UPDATE voice_assets SET message_ref=? WHERE message_ref=? AND sha256 NOT IN(SELECT sha256 FROM voice_assets WHERE message_ref=?)", (canonical, old_ref, canonical))
        # Assets with the same digest still remain on disk and historical IDs
        # remain in the table; only the canonical current pointer is selected.
        voice = self.conn.execute("SELECT asset_ref FROM voice_current WHERE message_ref=?", (old_ref,)).fetchone()
        if voice:
            self.conn.execute("INSERT OR IGNORE INTO voice_current VALUES(?,?)", (canonical, voice[0]))
        self.conn.execute("DELETE FROM voice_current WHERE message_ref=?", (old_ref,))
        self.conn.execute("UPDATE native_transcripts SET message_ref=? WHERE message_ref=?", (canonical, old_ref))
        transcript = self.conn.execute("SELECT transcript_ref FROM native_transcript_current WHERE message_ref=?", (old_ref,)).fetchone()
        if transcript:
            self.conn.execute("INSERT OR IGNORE INTO native_transcript_current VALUES(?,?)", (canonical, transcript[0]))
        self.conn.execute("DELETE FROM native_transcript_current WHERE message_ref=?", (old_ref,))
        active = self.conn.execute("SELECT 1 FROM native_voice_jobs WHERE message_ref=? AND status IN('queued','running')", (canonical,)).fetchone()
        if active:
            self.conn.execute("UPDATE native_voice_jobs SET status='cancelled',reason='message_merged',updated_at=?,lease_until=0 WHERE message_ref=? AND status IN('queued','running')", (time.time(), old_ref))
        self.conn.execute("UPDATE native_voice_jobs SET message_ref=? WHERE message_ref=?", (canonical, old_ref))
        self.conn.execute("DELETE FROM p2_text_search WHERE message_ref=?", (old_ref,))
        current = self.conn.execute("SELECT t.text,t.transcript_ref FROM native_transcript_current c JOIN native_transcripts t USING(transcript_ref) WHERE c.message_ref=?", (canonical,)).fetchone()
        if current:
            self.conn.execute("DELETE FROM p2_text_search WHERE message_ref=? AND source_kind='transcript'", (canonical,))
            self.conn.execute("INSERT INTO p2_text_search VALUES(?,?,?,?)", (current["text"], canonical, "transcript", current["transcript_ref"]))
        self._bump_content_revision()

    def upsert_profiles(self, account_id, profiles):
        _, encode, _, ref, safe, _ = _api()
        self._write()
        counts = {"profiles": 0, "avatars": 0}
        with self._lock, self.conn:
            for incoming in profiles:
                subject = str(incoming.get("subject_id") or "")
                if not subject:
                    continue
                payload = safe(incoming.get("profile") or {})
                avatar = incoming.get("avatar")
                avatar_ref = None
                if avatar:
                    digest = str(avatar.get("sha256") or "")
                    if not re.fullmatch(r"[0-9a-f]{64}", digest):
                        raise ValueError("invalid_avatar_digest")
                    from chatlog_keeper.archive_media import _blob_path
                    _blob_path(self.media_dir, avatar.get("relative_path"))
                    avatar_ref = ref("avatar_", account_id, subject, digest)
                    self.conn.execute("INSERT OR IGNORE INTO avatar_assets VALUES(?,?,?,?,?,?)", (avatar_ref, account_id, subject, digest, encode(avatar), time.time()))
                    counts["avatars"] += 1
                old = self.conn.execute("SELECT * FROM profiles WHERE account_id=? AND subject_id=?", (account_id, subject)).fetchone()
                if avatar is None and old:
                    avatar_ref = old["avatar_ref"]
                revision = ref("profile_", account_id, subject, payload, avatar_ref)
                now = time.time()
                self.conn.execute("INSERT INTO profiles VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,subject_id) DO UPDATE SET payload=excluded.payload,avatar_ref=excluded.avatar_ref,revision=excluded.revision,updated_at=excluded.updated_at", (account_id, subject, encode(payload), avatar_ref, revision, now))
                self.conn.execute("INSERT OR IGNORE INTO profile_versions VALUES(?,?,?,?,?,?)", (revision, account_id, subject, encode(payload), avatar_ref, now))
                counts["profiles"] += 1
        return counts

    def get_profile(self, group_id, sender_id=None, include_avatar=False):
        error, _, load, _, safe, _ = _api()
        if type(include_avatar) is not bool or (sender_id is not None and (not isinstance(sender_id, str) or not sender_id)):
            raise error("invalid_profile_query")
        with self._lock, self._read_transaction():
            group_ref = self._group_scope([group_id])[0]
            group = self.conn.execute("SELECT * FROM groups WHERE group_ref=?", (group_ref,)).fetchone()
            subject = sender_id or group["group_id"]
            member = self.conn.execute("SELECT payload FROM group_members WHERE group_ref=? AND sender_id=?", (group_ref, subject)).fetchone() if sender_id else None
            observed = self.conn.execute("SELECT 1 FROM messages WHERE group_ref=? AND sender_id=? LIMIT 1", (group_ref, subject)).fetchone() if sender_id else None
            if sender_id and not member and not observed:
                raise error("member_not_observed_in_group")
            profile = self.conn.execute("SELECT * FROM profiles WHERE account_id=? AND subject_id=?", (group["account_id"], subject)).fetchone()
            result = {"group_id": group_ref, "subject_id": subject, "kind": "member" if sender_id else "group",
                      "profile_status": "archived" if profile else "directory_only" if member or not sender_id else "not_cached",
                      "profile": {**(load(profile["payload"], {}) if profile else {}), **(load(member[0], {}) if member else {})},
                      "profile_updated_at": profile["updated_at"] if profile else None,
                      "avatar": {"status": "not_cached", "avatar_ref": None}, "include_avatar": include_avatar,
                      "coverage": self._coverage([group_ref])}
            if not sender_id:
                result["profile"].setdefault("display_name", group["name"])
            if profile and profile["avatar_ref"]:
                asset = self.conn.execute("SELECT * FROM avatar_assets WHERE avatar_ref=?", (profile["avatar_ref"],)).fetchone()
                if asset:
                    info = load(asset["payload"], {})
                    result["avatar"] = {"status": "archived", "avatar_ref": asset["avatar_ref"], "sha256": asset["sha256"],
                                        **{k: info.get(k) for k in ("mime", "width", "height", "observed_at")}, "quality": "local_cache"}
            return safe(result)

    def get_profile_avatar(self, avatar_ref):
        error, _, load, _, _, _ = _api()
        with self._lock, self._read_transaction():
            asset = self.conn.execute("SELECT * FROM avatar_assets WHERE avatar_ref=?", (avatar_ref,)).fetchone()
            if asset is None:
                raise error("avatar_not_archived")
            metadata = load(asset["payload"], {})
            from chatlog_keeper.archive_media import _read_blob
            data = _read_blob(self.media_dir, metadata["relative_path"], asset["sha256"], 32 * 1024 * 1024)
            return {"data": data, "mime_type": metadata.get("mime") or "image/jpeg",
                    "metadata": {"avatar_ref": avatar_ref, "sha256": asset["sha256"], **{k: metadata.get(k) for k in ("mime", "width", "height")}}}

    def upsert_voice(self, account_id, group_id, server_id, blob, source):
        _, encode, _, ref, _, _ = _api()
        self._write()
        with self._lock, self.conn:
            message = self.conn.execute("SELECT * FROM messages WHERE account_id=? AND (group_id=? OR group_ref=?) AND server_id=? AND msg_type=34", (account_id, group_id, group_id, str(server_id))).fetchone()
            if message is None:
                return {"matched": False, "message_ref": None, "media_ref": None, "status": "unmatched"}
            from chatlog_keeper.archive_media import _blob_path
            _blob_path(self.media_dir, blob.get("raw_path"))
            digest = str(blob.get("raw_sha256") or "")
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid_voice_digest")
            message_ref = message["message_ref"]
            asset_ref = ref("voiceasset_", message_ref, digest)
            now = time.time()
            self.conn.execute("INSERT OR IGNORE INTO voice_assets VALUES(?,?,?,?,?,?)", (asset_ref, message_ref, digest, encode(blob), encode(source), now))
            self.conn.execute("INSERT OR REPLACE INTO voice_current VALUES(?,?)", (message_ref, asset_ref))
            media = self.conn.execute("SELECT media_ref FROM media WHERE message_ref=? AND kind='voice' AND forward_ref IS NULL AND component_ref IS NULL ORDER BY media_ref LIMIT 1", (message_ref,)).fetchone()
            media_ref = media[0] if media else ref("media_", message_ref, "voice", "primary")
            attachment = {"kind": "voice"}
            self.conn.execute("INSERT OR IGNORE INTO media(media_ref,message_ref,account_id,group_id,create_time,kind,file_key,attachment,updated_at) VALUES(?,?,?,?,?,?,?,?,?)", (media_ref, message_ref, account_id, message["group_id"], message["create_time"], "voice", "primary", encode(attachment), now))
            self.conn.execute("UPDATE media SET status='ready',reason=NULL,raw_path=?,raw_sha256=?,mime=?,codec=?,extras=?,updated_at=? WHERE media_ref=?", (blob["raw_path"], digest, blob.get("mime", "audio/silk"), blob.get("codec", "silk_v3"), encode({"raw_size": blob.get("raw_size"), "source_status": "authenticated_media_database", "voice_asset_ref": asset_ref}), now, media_ref))
            return {"matched": True, "message_ref": message_ref, "media_ref": media_ref, "status": "ready"}

    def _voice_message(self, reference):
        error, *_ = _api()
        if not isinstance(reference, str) or not re.fullmatch(r"(?:msg|media|fwd|item)_[A-Za-z0-9_-]{1,200}", reference):
            raise error("invalid_voice_reference")
        if reference.startswith("msg_"):
            row = self.conn.execute("SELECT * FROM messages WHERE message_ref=?", (self._resolve_ref(reference),)).fetchone()
        elif reference.startswith("media_"):
            row = self.conn.execute("SELECT m.* FROM media x JOIN messages m USING(message_ref) WHERE x.media_ref=? AND x.kind='voice' AND x.component_ref IS NULL AND x.forward_ref IS NULL", (reference,)).fetchone()
        else:
            raise error("embedded_voice_not_supported")
        if row is None:
            raise error("voice_message_not_found")
        if row["msg_type"] != 34:
            raise error("not_voice_message")
        return row

    def get_voice(self, reference):
        _, _, load, _, safe, _ = _api()
        with self._lock, self._read_transaction():
            row = self._voice_message(reference)
            asset = self.conn.execute("SELECT a.* FROM voice_current c JOIN voice_assets a USING(asset_ref) WHERE c.message_ref=?", (row["message_ref"],)).fetchone()
            transcript = self.conn.execute("SELECT t.* FROM native_transcript_current c JOIN native_transcripts t USING(transcript_ref) WHERE c.message_ref=?", (row["message_ref"],)).fetchone()
            job = self.conn.execute("SELECT job_id,status,reason,created_at,updated_at FROM native_voice_jobs WHERE message_ref=? ORDER BY created_at DESC LIMIT 1", (row["message_ref"],)).fetchone()
            attachment = load(row["attachment"], {})
            blob = load(asset["blob"], {}) if asset else {}
            audio_status, audio_reason = self._verify_voice_asset(asset)
            return safe({"message_ref": row["message_ref"], "group_id": row["group_ref"], "timestamp": row["create_time"],
                         "sender_id": row["sender_id"], "sender_name": row["sender_name"],
                         "voice_length_ms": attachment.get("voice_length_ms"),
                         "status": "ready" if audio_status == "archived" else audio_status,
                         "audio_status": audio_status, "audio_reason": audio_reason,
                         "audio": {"asset_ref": asset["asset_ref"], "sha256": asset["sha256"],
                                   "size_bytes": blob.get("raw_size"), "codec": blob.get("codec", "silk_v3"),
                                   "mime": blob.get("mime", "audio/silk")} if asset else None,
                         "source": load(asset["source"], {}) if asset else None,
                         "native_text": transcript["text"] if transcript else None,
                         "transcription_status": "ready" if transcript else job["status"] if job else "not_requested",
                         "transcript": {"transcript_ref": transcript["transcript_ref"], "text": transcript["text"],
                                        "provenance": load(transcript["provenance"], {}), "created_at": transcript["created_at"]} if transcript else None,
                         "task": dict(job) if job else None,
                         "native_transcription_capability": self.get_meta("native_transcription_capability", {"enabled": False, "reason": "native_transcription_not_verified"}),
                         "coverage": self._coverage([row["group_ref"]])})

    def _verify_voice_asset(self, asset):
        """Verify an owned regular raw file with bounded streaming reads."""
        if asset is None:
            return "not_archived", "voice_not_cached"
        from chatlog_keeper.archive_media import _blob_path, MediaUnavailable
        blob = _api()[2](asset["blob"], {})
        try:
            path = _blob_path(self.media_dir, blob.get("raw_path"))
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as handle:
                before = os.fstat(handle.fileno())
                if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid():
                    return "corrupt", "unsafe_archive_blob"
                if before.st_size > 8 * 1024 * 1024:
                    return "corrupt", "voice_blob_exceeds_limit"
                digest, size = hashlib.sha256(), 0
                while True:
                    chunk = handle.read(65536)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 8 * 1024 * 1024:
                        return "corrupt", "voice_blob_exceeds_limit"
                    digest.update(chunk)
                after = os.fstat(handle.fileno())
                current = path.lstat()
                signature = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
                if signature(before) != signature(after) or signature(after) != signature(current):
                    return "corrupt", "archive_blob_changed"
                if (size != blob.get("raw_size") or digest.hexdigest() != asset["sha256"]
                        or digest.hexdigest() != path.name.split(".", 1)[0]):
                    return "corrupt", "archive_blob_corrupt"
                return "archived", None
        except MediaUnavailable:
            return "corrupt", "unsafe_archive_blob"
        except FileNotFoundError:
            return "missing", "archive_blob_missing"
        except OSError:
            return "corrupt", "archive_blob_unreadable"

    def save_native_transcript(self, message_ref, text, provenance, job_id=None):
        error, encode, _, ref, safe, _ = _api()
        self._write()
        if not isinstance(text, str) or not text.strip() or len(text) > 1_000_000:
            raise error("invalid_native_transcript")
        with self._lock, self.conn:
            message = self._voice_message(message_ref)
            message_ref = message["message_ref"]
            proof = safe(provenance or {})
            transcript_ref = ref("transcript_", message_ref, text, proof)
            self.conn.execute("INSERT OR IGNORE INTO native_transcripts VALUES(?,?,?,?,?)", (transcript_ref, message_ref, text, encode(proof), time.time()))
            previous = self.conn.execute("SELECT transcript_ref FROM native_transcript_current WHERE message_ref=?", (message_ref,)).fetchone()
            self.conn.execute("INSERT OR REPLACE INTO native_transcript_current VALUES(?,?)", (message_ref, transcript_ref))
            self.conn.execute("DELETE FROM p2_text_search WHERE message_ref=? AND source_kind='transcript'", (message_ref,))
            self.conn.execute("INSERT INTO p2_text_search VALUES(?,?,?,?)", (text, message_ref, "transcript", transcript_ref))
            if not previous or previous[0] != transcript_ref:
                self._bump_content_revision()
            if job_id:
                self.conn.execute("UPDATE native_voice_jobs SET status='succeeded',reason=NULL,lease_until=0,updated_at=? WHERE job_id=? AND message_ref=?", (time.time(), job_id, message_ref))
            return {"message_ref": message_ref, "transcript_ref": transcript_ref, "status": "ready"}

    def request_voice_transcription(self, message_ref):
        _, _, _, ref, _, _ = _api()
        self._write()
        with self._lock, self.conn:
            if not self.conn.in_transaction:
                self.conn.execute("BEGIN IMMEDIATE")
            row = self._voice_message(message_ref)
            message_ref = row["message_ref"]
            transcript = self.conn.execute("SELECT transcript_ref FROM native_transcript_current WHERE message_ref=?", (message_ref,)).fetchone()
            if transcript:
                return {"message_ref": message_ref, "status": "ready", "transcript_ref": transcript[0], "reused": True}
            capability = self.get_meta("native_transcription_capability", {"enabled": False, "reason": "native_transcription_not_verified"})
            if capability.get("enabled") is not True or capability.get("verified") is not True:
                return {"message_ref": message_ref, "status": "unavailable", "reason": capability.get("reason") or "native_transcription_not_verified", "capability": capability}
            asset = self.conn.execute("SELECT a.* FROM voice_current c JOIN voice_assets a USING(asset_ref) WHERE c.message_ref=?", (message_ref,)).fetchone()
            audio_status, audio_reason = self._verify_voice_asset(asset)
            if audio_status != "archived":
                return {"message_ref": message_ref, "status": "not_archived", "audio_status": audio_status, "reason": audio_reason}
            active = self.conn.execute("SELECT job_id,status FROM native_voice_jobs WHERE message_ref=? AND status IN('queued','running')", (message_ref,)).fetchone()
            if active:
                return {"message_ref": message_ref, "job_id": active["job_id"], "status": active["status"], "reused": True}
            now = time.time()
            job_id = ref("nativejob_", message_ref, now)
            self.conn.execute("INSERT INTO native_voice_jobs(job_id,message_ref,status,created_at,updated_at) VALUES(?,?,'queued',?,?)", (job_id, message_ref, now, now))
            return {"message_ref": message_ref, "job_id": job_id, "status": "queued", "reused": False}

    def claim_voice_transcription_job(self, worker_id=None, lease_seconds=100):
        _, _, load, _, _, _ = _api()
        self._write()
        if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool) or not 1 <= lease_seconds <= 120:
            raise ValueError("invalid_native_job_lease")
        with self._lock, self.conn:
            now = time.time()
            self.conn.execute("UPDATE native_voice_jobs SET status='timeout',reason='native_worker_lease_expired',lease_until=0,updated_at=? WHERE status='running' AND lease_until<=?", (now, now))
            capability = self.get_meta("native_transcription_capability", {})
            if capability.get("enabled") is not True or capability.get("verified") is not True:
                return None
            if self.conn.execute("SELECT 1 FROM native_voice_jobs WHERE status='running' LIMIT 1").fetchone():
                return None
            row = self.conn.execute("SELECT j.*,m.account_id,m.group_id,m.group_ref,m.server_id,m.create_time,m.sender_id,m.sender_name,m.attachment,g.name group_name FROM native_voice_jobs j JOIN messages m USING(message_ref) JOIN groups g USING(group_ref) WHERE j.status='queued' ORDER BY j.created_at LIMIT 1").fetchone()
            if row is None:
                return None
            asset = self.conn.execute("SELECT a.* FROM voice_current c JOIN voice_assets a USING(asset_ref) WHERE c.message_ref=?", (row["message_ref"],)).fetchone()
            audio_status, audio_reason = self._verify_voice_asset(asset)
            if audio_status != "archived":
                self.conn.execute("UPDATE native_voice_jobs SET status='failed',reason=?,updated_at=? WHERE job_id=?", (audio_reason, now, row["job_id"]))
                return None
            self.conn.execute("UPDATE native_voice_jobs SET status='running',lease_until=?,worker_id=?,updated_at=? WHERE job_id=?", (now + lease_seconds, worker_id or "local_native_worker", now, row["job_id"]))
            result = dict(row)
            result.update(status="running", lease_until=now + lease_seconds, voice_length_ms=load(row["attachment"], {}).get("voice_length_ms"))
            result.pop("attachment", None)
            return result

    def update_voice_transcription_job(self, job_id, status, reason=None, **fields):
        error, encode, load, _, safe, _ = _api()
        self._write()
        if status not in {"queued", "running", "succeeded", "failed", "timeout", "needs_login", "unsupported", "ambiguous", "cancelled"}:
            raise error("invalid_native_job_status")
        with self._lock, self.conn:
            row = self.conn.execute("SELECT details FROM native_voice_jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise error("native_job_not_found")
            details = {**load(row[0], {}), **safe(fields)}
            self.conn.execute("UPDATE native_voice_jobs SET status=?,reason=?,details=?,lease_until=CASE WHEN ?='running' THEN lease_until ELSE 0 END,updated_at=? WHERE job_id=?", (status, reason, encode(details), status, time.time(), job_id))

    def p2_status(self):
        return {"cards": self.conn.execute("SELECT COUNT(*) FROM messages WHERE card IS NOT NULL").fetchone()[0],
                "card_components": self.conn.execute("SELECT COUNT(*) FROM card_components").fetchone()[0],
                "profiles": self.conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0],
                "avatars": self.conn.execute("SELECT COUNT(*) FROM avatar_assets").fetchone()[0],
                "voices_archived": self.conn.execute("SELECT COUNT(*) FROM voice_current").fetchone()[0],
                "native_transcripts": self.conn.execute("SELECT COUNT(*) FROM native_transcript_current").fetchone()[0],
                "native_jobs": {r[0]: r[1] for r in self.conn.execute("SELECT status,COUNT(*) FROM native_voice_jobs GROUP BY status")},
                "native_transcription_capability": self.get_meta("native_transcription_capability", {"enabled": False, "reason": "native_transcription_not_verified"})}
