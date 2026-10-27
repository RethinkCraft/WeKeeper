"""Offline checks for the short-lived, fixed-target authorization relay."""
from email.message import Message
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import time
from urllib.parse import urlencode

import pytest

MODULE = Path(__file__).parents[1] / "scripts" / "remote_summary_auth.py"
spec = importlib.util.spec_from_file_location("wekeeper_remote_summary_auth", MODULE)
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)

PREFIX = "/wekeeper-auth-abcdefghijklmnopqrstuvwxyz1234"
ORIGIN = "https://192.0.2.1"
CALLBACK = "http://127.0.0.1:1455/auth/callback?code=fake-code&state=fake-state&client_id=issued-client"


class Response:
    def __init__(self, status=200, location=None):
        self.status = status
        self.headers = Message()
        if location is not None:
            self.headers["Location"] = location
        self.closed = False

    def getcode(self):
        return self.status

    def close(self):
        self.closed = True


class Opener:
    def __init__(self, response=None):
        self.response = response or Response()
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append(request)
        assert timeout == 5
        return self.response


def handler(path=None, *, body=None, origin=ORIGIN, csrf="csrf-token", ttl=60, content_type="application/x-www-form-urlencoded"):
    value = relay.RelayHandler.__new__(relay.RelayHandler)
    value.server = SimpleNamespace(prefix=PREFIX, public_origin=ORIGIN, callback_port=1455,
                                   csrf="csrf-token", deadline=time.monotonic() + ttl,
                                   opener=Opener(), completed=False)
    value.path = path or PREFIX + "/"
    value.headers = Message()
    if origin is not None:
        value.headers["Origin"] = origin
    if body is None:
        body = urlencode({"csrf": csrf, "callback_url": CALLBACK}).encode()
    value.headers["Content-Length"] = str(len(body))
    value.headers["Content-Type"] = content_type
    value.rfile, value.wfile = io.BytesIO(body), io.BytesIO()
    value.statuses, value.response_headers = [], {}
    value.send_response = lambda code: value.statuses.append(code)
    value.send_header = lambda name, content: value.response_headers.update({name: content})
    value.end_headers = lambda: None
    return value


def test_random_prefix_and_https_origin_settings():
    assert relay.normalize_prefix(PREFIX + "/") == PREFIX
    assert relay.normalize_origin(ORIGIN + "/") == ORIGIN
    for bad in ("/guessable", "/" + "x" * 129, "/../../" + "x" * 32, "/" + "x" * 24 + "?code=bad"):
        with pytest.raises(relay.RelayError):
            relay.normalize_prefix(bad)
    for bad in ("http://example.com", "https://user@example.com", ORIGIN + "/private", ORIGIN + "?x=1"):
        with pytest.raises(relay.RelayError):
            relay.normalize_origin(bad)


@pytest.mark.parametrize("bad", [
    CALLBACK.replace("http://", "https://"), CALLBACK.replace("127.0.0.1", "localhost"),
    CALLBACK.replace("127.0.0.1", "evil.example"), CALLBACK.replace(":1455", ":80"),
    CALLBACK.replace("127.0.0.1:1455", "user@127.0.0.1:1455"),
    CALLBACK.replace("/auth/callback", "/other"), CALLBACK + "#fragment",
    CALLBACK + "&state=another", CALLBACK + "&code=another", CALLBACK + "&error=access_denied",
    CALLBACK.replace("state=fake-state", "state="), CALLBACK.replace("code=fake-code", "code="),
    CALLBACK + "\nHeader: value", "http://127.0.0.1:1455/auth/callback?state=s",
])
def test_callback_validation_rejects_ssrf_and_ambiguous_parameters(bad):
    opener = Opener()
    with pytest.raises(relay.RelayError):
        relay.submit_callback(opener, 1455, bad)
    assert not opener.requests


def test_callback_only_forwards_query_to_fixed_loopback_host():
    opener = Opener()
    relay.submit_callback(opener, 1455, CALLBACK)
    request = opener.requests[0]
    assert request.full_url == CALLBACK
    assert request.get_header("Host") == "127.0.0.1:1455"
    assert request.get_method() == "GET"
    assert opener.response.closed


@pytest.mark.parametrize("target", [
    "https://evil.example/api/accounts/authorize", "http://auth.openai.com/api/accounts/authorize",
    "https://auth.openai.com:8443/api/accounts/authorize", "https://auth.openai.com/other",
    "https://auth.openai.com@evil.example/api/accounts/authorize", "https://user@auth.openai.com/api/accounts/authorize",
    "https://auth.openai.com/api/accounts/authorize#fragment",
])
def test_start_never_redirects_to_untrusted_authorization_url(target):
    opener = Opener(Response(302, target))
    with pytest.raises(relay.RelayError):
        relay.authorization_location(opener, 1455)
    assert opener.requests[0].full_url == "http://127.0.0.1:1455/login"
    assert opener.response.closed


def test_page_security_headers_and_no_callback_echo():
    request = handler()
    request.do_GET()
    assert request.statuses == [200]
    body = request.wfile.getvalue().decode()
    assert "fake-code" not in body and "fake-state" not in body
    assert "autocomplete=\"off\"" in body
    assert request.response_headers["Referrer-Policy"] == "same-origin"
    assert "no-store" in request.response_headers["Cache-Control"]
    assert "frame-ancestors 'none'" in request.response_headers["Content-Security-Policy"]
    assert "form-action 'self'" in request.response_headers["Content-Security-Policy"]


def test_start_only_issues_validated_redirect():
    request = handler(PREFIX + "/start")
    location = "https://auth.openai.com/api/accounts/authorize?client_id=dynamic_agent_client&state=opaque"
    request.server.opener = Opener(Response(302, location))
    request.do_GET()
    assert request.statuses == [302]
    assert request.response_headers["Location"] == location
    assert not request.wfile.getvalue()


@pytest.mark.parametrize("origin,csrf", [("https://evil.example", "csrf-token"), ("null", "csrf-token"), (None, "csrf-token"), (ORIGIN, "wrong"), (ORIGIN, "中文")])
def test_complete_requires_same_origin_and_csrf(origin, csrf):
    request = handler(PREFIX + "/complete", origin=origin, csrf=csrf)
    request.do_POST()
    assert request.statuses == [403]
    assert not request.server.opener.requests
    assert not request.server.completed


def test_complete_success_is_safe_and_closes_one_time_relay():
    request = handler(PREFIX + "/complete")
    request.do_POST()
    assert request.statuses == [200]
    assert request.server.completed
    assert len(request.server.opener.requests) == 1
    body = request.wfile.getvalue().decode()
    assert "回调已安全提交" in body
    assert "fake-code" not in body and "fake-state" not in body and "issued-client" not in body


@pytest.mark.parametrize("body,status", [
    (b"x" * (relay.MAX_BODY + 1), 413),
    (b"csrf=csrf-token&csrf=duplicate&callback_url=x", 400),
    (b"csrf=csrf-token&callback_url=x&extra=y", 400),
])
def test_body_limits_and_duplicate_form_parameters(body, status):
    request = handler(PREFIX + "/complete", body=body)
    request.do_POST()
    assert request.statuses == [status]
    assert not request.server.opener.requests


def test_expired_or_wrong_prefix_cannot_reach_local_callback():
    expired = handler(PREFIX + "/complete", ttl=-1)
    expired.do_POST()
    assert expired.statuses == [410]
    assert not expired.server.opener.requests
    wrong = handler("/another-prefix/complete")
    wrong.do_POST()
    assert wrong.statuses == [404]
    assert not wrong.server.opener.requests


def test_invalid_callback_error_page_never_echoes_query():
    request = handler(PREFIX + "/complete")
    request.server.opener = Opener(Response(400))
    request.do_POST()
    assert request.statuses == [400]
    assert not request.server.completed
    assert "fake-code" not in request.wfile.getvalue().decode()
