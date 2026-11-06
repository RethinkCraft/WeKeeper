"""Expired and near-expiry subscription authorization on the real bot path.

Every request uses MockTransport and a generated test signing key. No live
credentials, network requests, local services or WeChat messages are involved.
"""
import json
import time
from urllib.parse import parse_qs

import httpx
import pytest

from chatlog_keeper.dashboard.siwc import DIRECT_SCOPE, ISSUER, RESOURCE, SIWCClient, SIWCError
from test_siwc import claims_token, seed, signing_key, stream


def bot_request(client, events, run_id="auth_refresh_case"):
    return client.bot_chat(
        [{"role": "user", "content": "在吗"}], instructions="简短接话。",
        tools=[], tool_handler=lambda *_: pytest.fail("no business tool is authorized by this test"),
        run_metadata={"run_id": run_id, "profile_id": "gpt", "group_ref": "group_test", "bot_request_id": run_id},
        on_event=events.append,
    )


def completed_response():
    return httpx.Response(200, text=stream(
        {"type": "response.output_text.delta", "delta": "在呢。"},
        {"type": "response.completed", "response": {"status": "completed"}},
    ))


def authorization_events(events):
    return [event for event in events if event.get("stage") == "authorization"]


def test_expired_access_rotates_validates_and_persists_before_model(tmp_path, signing_key):
    key, jwk = signing_key
    calls, events = [], []
    granted_scopes = [DIRECT_SCOPE, "offline_access"]

    def remote(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/oauth/token"):
            fields = parse_qs(request.content.decode())
            assert fields == {"grant_type": ["refresh_token"], "client_id": ["issued-client"],
                              "refresh_token": ["test-refresh"], "resource": [RESOURCE]}
            return httpx.Response(200, json={"access_token": "renewed-test-access", "refresh_token": "rotated-test-refresh",
                "expires_in": 3600, "scope": " ".join(granted_scopes), "id_token": claims_token(key)})
        if request.url.path == "/.well-known/openid-configuration":
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/.well-known/jwks.json"})
        if request.url.path == "/.well-known/jwks.json":
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"keys": [jwk]})
        if request.url.path == "/v1/responses":
            assert request.headers["authorization"] == "Bearer renewed-test-access"
            assert json.loads(request.content)["model"] == "test-model"
            assert set(calls[:-1]) == {"/api/accounts/oauth/token", "/.well-known/openid-configuration", "/.well-known/jwks.json"}
            # Rotation must be durable before the model request is attempted.
            persisted = client._read()["accounts"]["account"]
            assert persisted["refresh_token"] == "rotated-test-refresh"
            assert persisted["scopes"] == granted_scopes
            return completed_response()
        pytest.fail("unexpected endpoint")

    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http)
    seed(client, expired=True, scopes=granted_scopes)
    original_host = client._read()["host_id"]
    try:
        assert bot_request(client, events)["output_text"] == "在呢。"
        saved = client._read()
        account = saved["accounts"]["account"]
        assert saved["host_id"] == original_host
        assert account["subject"] == "subject" and account["client_id"] == "issued-client"
        assert account["refresh_token"] == "rotated-test-refresh"
        assert account["expires_at"] > time.time() + 3500
        assert client.path.stat().st_mode & 0o777 == 0o600
        auth = authorization_events(events)
        assert any(event["type"] == "timing" and event["seconds"] >= 0 for event in auth)
        assert not any(event["type"] == "failure" for event in auth)
        assert next(i for i, event in enumerate(events) if event.get("stage") == "authorization") < next(
            i for i, event in enumerate(events) if event.get("type") == "model")
        encoded_events = json.dumps(events)
        assert "renewed-test-access" not in encoded_events and "rotated-test-refresh" not in encoded_events
    finally:
        client.close(); http.close()


@pytest.mark.parametrize("failure_at", ["token", "discovery", "jwks"])
def test_refresh_transport_failure_names_authorization_and_never_calls_model(tmp_path, signing_key, failure_at):
    key, jwk = signing_key
    calls, events = [], []

    def remote(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/oauth/token"):
            if failure_at == "token":
                raise httpx.ConnectError("synthetic auth outage", request=request)
            return httpx.Response(200, json={"access_token": "renewed-test-access", "refresh_token": "rotated-test-refresh",
                "expires_in": 3600, "scope": DIRECT_SCOPE, "id_token": claims_token(key)})
        if request.url.path.endswith("openid-configuration"):
            if failure_at == "discovery":
                raise httpx.ConnectError("synthetic discovery outage", request=request)
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/keys"})
        if request.url.path == "/keys":
            if failure_at == "jwks":
                raise httpx.ConnectError("synthetic key outage", request=request)
            return httpx.Response(200, json={"keys": [jwk]})
        pytest.fail("the model must never receive an expired or unvalidated access token")

    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http)
    seed(client, expired=True)
    try:
        with pytest.raises(SIWCError, match="siwc_network_error"):
            bot_request(client, events, "auth_failure_" + failure_at)
        assert "/v1/responses" not in calls
        auth = authorization_events(events)
        assert any(event["type"] == "timing" and event["seconds"] >= 0 for event in auth)
        assert any(event["type"] == "failure" and event["error"] == "siwc_network_error" for event in auth)
        assert not any(event.get("type") == "model" for event in events)
        assert client._read()["accounts"]["account"].get("refresh_token")
    finally:
        client.close(); http.close()


def test_maintenance_refresh_failure_keeps_valid_access_and_never_uses_it_after_expiry(tmp_path):
    now = [time.time()]
    calls = []

    def remote(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/oauth/token"):
            raise httpx.ConnectError("temporary auth outage", request=request)
        assert request.url.path == "/v1/responses"
        assert request.headers["authorization"] == "Bearer test-access"
        return completed_response()

    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http, clock=lambda: now[0])
    seed(client)
    with client._locked():
        saved = client._read(); saved["accounts"]["account"]["expires_at"] = now[0] + 120; client._write(saved)
    try:
        assert client._access(refresh_before=180)[0] == "test-access"
        assert bot_request(client, [], "valid_after_maintenance_failure")["output_text"] == "在呢。"
        assert calls.count("/api/accounts/oauth/token") == 1
        assert client._read()["accounts"]["account"]["refresh_token"] == "test-refresh"
        now[0] += 121
        with pytest.raises(SIWCError, match="siwc_network_error"):
            client._access(refresh_before=180)
        assert calls.count("/v1/responses") == 1
    finally:
        client.close(); http.close()


def test_request_near_expiry_can_use_still_valid_access_after_transient_refresh_failure(tmp_path):
    calls, events = [], []
    http = httpx.Client(transport=httpx.MockTransport(lambda request: remote(request)))
    client = SIWCClient(tmp_path, http_client=http)

    def remote(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/oauth/token"):
            raise httpx.ConnectError("temporary auth outage", request=request)
        assert request.headers["authorization"] == "Bearer test-access"
        return completed_response()

    seed(client)
    with client._locked():
        saved = client._read(); saved["accounts"]["account"]["expires_at"] = time.time() + 30; client._write(saved)
    try:
        assert bot_request(client, events)["output_text"] == "在呢。"
        assert calls == ["/api/accounts/oauth/token", "/v1/responses"]
    finally:
        client.close(); http.close()


@pytest.mark.parametrize("expired", [False, True])
def test_maintenance_refresh_respects_provider_earliest_refresh_time(tmp_path, expired):
    now = time.time()
    http = httpx.Client(transport=httpx.MockTransport(lambda _: pytest.fail("refresh is not permitted yet")))
    client = SIWCClient(tmp_path, http_client=http, clock=lambda: now)
    seed(client)
    with client._locked():
        saved = client._read(); account = saved["accounts"]["account"]
        account.update(expires_at=now - 1 if expired else now + 120, earliest_refresh_at=now + 90)
        client._write(saved)
    try:
        if expired:
            with pytest.raises(SIWCError, match="siwc_refresh_not_yet_available"):
                client._access(refresh_before=180)
        else:
            assert client._access(refresh_before=180)[0] == "test-access"
    finally:
        client.close(); http.close()


def test_refresh_cannot_call_model_after_direct_permission_is_removed(tmp_path):
    calls, events = [], []

    def remote(request):
        calls.append(request.url.path)
        assert request.url.path.endswith("/oauth/token")
        return httpx.Response(200, json={"access_token": "reduced-test-access", "refresh_token": "rotated-test-refresh",
            "expires_in": 3600, "scope": "openid offline_access"})

    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http)
    seed(client, expired=True)
    try:
        with pytest.raises(SIWCError, match="siwc_plan_permission_required"):
            bot_request(client, events)
        assert calls == ["/api/accounts/oauth/token"]
        assert DIRECT_SCOPE not in client._read()["accounts"]["account"]["scopes"]
        assert any(event.get("stage") == "authorization" and event["type"] == "failure" for event in events)
    finally:
        client.close(); http.close()


def test_maintenance_preloads_verification_before_consuming_rotating_grant(tmp_path, signing_key):
    key, jwk = signing_key
    calls = []
    def remote(request):
        calls.append(request.url.path)
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/keys"})
        if request.url.path == "/keys":
            return httpx.Response(200, json={"keys": [jwk]})
        assert request.url.path.endswith("/oauth/token")
        assert calls[:2] == ["/.well-known/openid-configuration", "/keys"]
        return httpx.Response(200, json={"access_token": "maintained-test-access", "refresh_token": "maintained-test-refresh",
            "expires_in": 3600, "scope": DIRECT_SCOPE, "id_token": claims_token(key)})
    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http)
    seed(client)
    with client._locked():
        saved = client._read(); saved["accounts"]["account"]["expires_at"] = time.time() + 120; client._write(saved)
    try:
        assert client.maintain_access()[0] == "maintained-test-access"
        assert calls == ["/.well-known/openid-configuration", "/keys", "/api/accounts/oauth/token"]
        assert client._read()["accounts"]["account"]["refresh_token"] == "maintained-test-refresh"
    finally:
        client.close(); http.close()


def test_unavailable_metadata_does_not_consume_rotating_grant_in_maintenance(tmp_path):
    calls = []
    def remote(request):
        calls.append(request.url.path)
        assert request.url.path.endswith("openid-configuration")
        raise httpx.ConnectError("public metadata temporarily unavailable", request=request)
    http = httpx.Client(transport=httpx.MockTransport(remote))
    client = SIWCClient(tmp_path, http_client=http)
    seed(client)
    with client._locked():
        saved = client._read(); saved["accounts"]["account"]["expires_at"] = time.time() + 120; client._write(saved)
    try:
        with pytest.raises(SIWCError, match="siwc_network_error"):
            client.maintain_access()
        assert calls == ["/.well-known/openid-configuration"]
        assert client._access()[0] == "test-access"  # Still valid, outside the request refresh window.
        assert client._read()["accounts"]["account"]["refresh_token"] == "test-refresh"
    finally:
        client.close(); http.close()
