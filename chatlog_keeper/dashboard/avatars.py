"""Export only bounded, re-encoded cached member avatars, never media paths."""
from __future__ import annotations

import base64
import hashlib
import hmac
import io

from PIL import Image, ImageOps

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.archive_media import MediaUnavailable


def sender_reference(salt, account_id, sender_id):
    if not sender_id:
        return ""
    return "sender_" + hmac.new(bytes.fromhex(salt), (account_id + "\0" + sender_id).encode(), hashlib.sha256).hexdigest()


def avatar_projections(archive_dir, group_refs, salt, source_id):
    """Identity joins happen locally; no native identifiers leave this function."""
    if not group_refs:
        return []
    result = []
    with ArchiveStore(archive_dir, readonly=True) as store:
        with store._lock, store._read_transaction():
            refs = store._group_scope(group_refs)
            for group in refs:
                members = store.conn.execute("""SELECT DISTINCT m.account_id,m.sender_id,p.avatar_ref,p.updated_at
                    FROM messages m JOIN profiles p ON p.account_id=m.account_id AND p.subject_id=m.sender_id
                    WHERE m.group_ref=? AND m.sender_id<>'' AND p.avatar_ref IS NOT NULL""", (group,)).fetchall()
                for row in members:
                    try:
                        asset = store.get_profile_avatar(row["avatar_ref"])
                        with Image.open(io.BytesIO(asset["data"])) as original:
                            if original.width * original.height > 16_000_000:
                                continue
                            image = ImageOps.exif_transpose(original).convert("RGB")
                            image.thumbnail((128, 128))
                            output = io.BytesIO()
                            image.save(output, format="JPEG", quality=85, optimize=True)
                        body = output.getvalue()
                        if len(body) > 128 * 1024:
                            continue
                    except (OSError, ValueError, MediaUnavailable, Image.DecompressionBombError):
                        continue
                    name = store.conn.execute("""SELECT sender_name FROM messages
                        WHERE group_ref=? AND account_id=? AND sender_id=?
                        ORDER BY create_time DESC,message_ref DESC LIMIT 1""", (group, row["account_id"], row["sender_id"])).fetchone()
                    result.append({"source_id": source_id, "group_ref": group,
                                   "sender_id": sender_reference(salt, row["account_id"], row["sender_id"]),
                                   "avatar_hash": hashlib.sha256(body).hexdigest(), "mime_type": "image/jpeg",
                                   "content_base64": base64.b64encode(body).decode("ascii"),
                                   "observed_at": row["updated_at"], "display_name": name[0] if name else ""})
    return result
