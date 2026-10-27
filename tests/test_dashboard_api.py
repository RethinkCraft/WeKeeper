from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from chatlog_keeper.dashboard.api import create_app
from chatlog_keeper.dashboard.protocol import content_hash

TOKEN = "machine-token-longer-than-thirty-two-characters"


@pytest.fixture
def app(tmp_path):
    value = create_app(tmp_path, TOKEN)
    value.state.store.create_user("admin", "long-admin-password")
    value.state.store.create_user("viewer", "long-viewer-password", "viewer")
    return value


@pytest.fixture
def client(app):
    with TestClient(app, base_url="https://testserver") as value:
        yield value


def login(client, username="admin"):
    response = client.post("/api/auth/login", json={"username": username, "password": f"long-{username}-password"}, headers={"Origin": "https://testserver"})
    assert response.status_code == 200
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return response


def part(text="hello"):
    day = "2026-10-26"
    data = {"group": {"group_ref": "group_a", "name": "Group"}, "day": day, "messages": [{"message_ref": "msg_a", "create_time": int(datetime(2026, 10, 26, 10, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()), "sender_id": "opaque_a", "sender_name": "A", "msg_type": 1, "text": text, "reply": None}], "coverage": {"local_backfill_complete": True}}
    return {"source_id": "source_a", "group_ref": "group_a", "day": day, "revision": 1, "content_hash": content_hash(data), "initial": True, "data": data}


def test_authentication_machine_separation_csrf_and_roles(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/api/overview").status_code == 401
    assert client.post("/v1/ingest/partitions", json=part()).status_code == 401
    response = login(client, "viewer")
    cookie = response.headers["set-cookie"]
    assert "HttpOnly" in cookie and "Secure" in cookie and "SameSite=strict" in cookie
    assert client.get("/api/users").status_code == 403
    assert client.post("/api/rules", json={"name": "x", "keyword": "x"}).status_code == 403
    assert client.post("/v1/ingest/partitions", json=part()).status_code == 401
    login(client)
    csrf = client.headers.pop("X-CSRF-Token")
    assert client.post("/api/rules", json={"name": "x", "keyword": "x"}).status_code == 403
    client.headers["X-CSRF-Token"] = csrf
    assert client.post("/api/rules", json={"name": "x", "keyword": "x"}, headers={"Origin": "https://evil.invalid"}).status_code == 403
    assert client.post("/api/rules", json={"name": "x", "keyword": "x"}).status_code == 200


def test_partition_ingest_unknown_fields_bad_hash_and_exact_replay(client):
    headers = {"Authorization": "Bearer " + TOKEN}
    value = part("<script>alert(1)</script>")
    first = client.post("/v1/ingest/partitions", json=value, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["applied"]
    assert not client.post("/v1/ingest/partitions", json=value, headers=headers).json()["applied"]
    value["data"]["messages"][0]["raw_payload"] = "private"
    assert client.post("/v1/ingest/partitions", json=value, headers=headers).status_code == 422
    del value["data"]["messages"][0]["raw_payload"]
    value["content_hash"] = "0" * 64
    assert client.post("/v1/ingest/partitions", json=value, headers=headers).status_code == 400
    login(client)
    response = client.get("/api/groups/group_a/messages", params={"since": "2026-10-26T00:00:00+08:00", "until": "2026-10-27T00:00:00+08:00"})
    assert response.json()["messages"][0]["text"] == "<script>alert(1)</script>"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"] == "no-store"


def test_login_origin_and_disabled_session(client, app):
    assert client.post("/api/auth/login", json={"username": "admin", "password": "long-admin-password"}, headers={"Origin": "https://evil.invalid"}).status_code == 403
    login(client, "viewer")
    viewer = next(u for u in app.state.store.list_users() if u["username"] == "viewer")
    app.state.store.update_user(viewer["id"], {"disabled": True})
    assert client.get("/api/auth/me").status_code == 401


def test_logout_and_user_password_policy(client):
    login(client)
    assert client.post("/api/users", json={"username": "new", "password": "short", "role": "viewer"}).status_code == 422
    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/auth/me").status_code == 401


def test_protocol_rejects_date_mismatch_duplicate_refs_and_oversize(client):
    headers = {"Authorization": "Bearer " + TOKEN}
    bad = part()
    bad["data"]["messages"] *= 2
    bad["content_hash"] = content_hash(bad["data"])
    assert client.post("/v1/ingest/partitions", json=bad, headers=headers).status_code == 422
    bad = part()
    bad["day"] = "2026-10-25"
    assert client.post("/v1/ingest/partitions", json=bad, headers=headers).status_code == 422
    assert client.post("/v1/ingest/partitions", content=b"x", headers={**headers, "Content-Length": str(17 * 1024 * 1024)}).status_code == 413


def test_configured_public_origin_behind_reverse_proxy(tmp_path, monkeypatch):
    monkeypatch.setenv("WEKEEPER_PUBLIC_ORIGIN", "https://203.0.113.10:8443")
    app = create_app(tmp_path, TOKEN)
    app.state.store.create_user("admin", "long-admin-password")
    with TestClient(app, base_url="http://api:8000") as client:
        good = client.post("/api/auth/login", json={"username": "admin", "password": "long-admin-password"}, headers={"Origin": "https://203.0.113.10:8443"})
        assert good.status_code == 200
        bad = client.post("/api/auth/login", json={"username": "admin", "password": "long-admin-password"}, headers={"Origin": "http://api:8000"})
        assert bad.status_code == 403


def test_validation_errors_do_not_echo_forbidden_values(client):
    bad = part()
    bad["data"]["messages"][0]["raw_payload"] = "sensitive-content-do-not-echo"
    response = client.post("/v1/ingest/partitions", json=bad, headers={"Authorization": "Bearer " + TOKEN})
    assert response.status_code == 422
    assert "sensitive-content-do-not-echo" not in response.text


def test_sse_invalidation_contains_revision_only_and_rechecks_auth(tmp_path):
    import asyncio
    from starlette.requests import Request

    app = create_app(tmp_path, TOKEN)
    store = app.state.store
    user = store.create_user("admin", "long-admin-password")
    auth = store.login("admin", "long-admin-password")
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/events")

    async def run():
        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}
        request = Request({"type": "http", "method": "GET", "path": "/api/events", "scheme": "https", "server": ("testserver", 443), "headers": [(b"cookie", ("wekeeper_session=" + auth["token"]).encode())]}, receive)
        response = await endpoint(request, store.session(auth["token"]))
        event = await anext(response.body_iterator)
        assert "event: refresh" in event and '"revision"' in event
        assert "username" not in event and "message" not in event
        store.update_user(user["id"], {"password": "changed-admin-password"})
        assert "event: auth_required" in await anext(response.body_iterator)
        await response.body_iterator.aclose()
    try:
        asyncio.run(run())
    finally:
        store.close()
