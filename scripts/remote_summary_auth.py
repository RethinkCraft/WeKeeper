#!/usr/bin/env python3
"""Short-lived relay to an already-running, loopback-only WeKeeper OAuth login.

Deploy only behind the owner's trusted HTTPS origin and a private SSH tunnel.
This helper never exchanges authorization codes or handles credential tokens.
"""
from __future__ import annotations

import argparse
import hmac
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import re
import secrets
import signal
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

MAX_BODY = 32768
MAX_CALLBACK = 16384


class RelayError(ValueError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def local_opener():
    # Environment proxy settings must never move a loopback callback elsewhere.
    return build_opener(ProxyHandler({}), NoRedirect())


def normalize_origin(value):
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise RelayError("invalid_public_origin") from None
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment or port not in (None, 443, 8443)
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)):
        raise RelayError("invalid_public_origin")
    return "https://" + parsed.netloc


def normalize_prefix(value):
    value = value.rstrip("/")
    if not re.fullmatch(r"/[A-Za-z0-9_-]{24,128}", value):
        raise RelayError("random_prefix_minimum_24_characters")
    return value


def validate_callback(value, callback_port):
    if not isinstance(value, str):
        raise RelayError("invalid_callback")
    value = value.strip()
    if not 1 <= len(value) <= MAX_CALLBACK or any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise RelayError("invalid_callback")
    try:
        parsed = urlsplit(value)
        port = parsed.port
        fields = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True,
                          encoding="utf-8", errors="strict", max_num_fields=12)
    except (ValueError, UnicodeError):
        raise RelayError("invalid_callback") from None
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or port != callback_port
            or parsed.netloc != f"127.0.0.1:{callback_port}"
            or parsed.username is not None or parsed.password is not None
            or parsed.path != "/auth/callback" or parsed.fragment
            or not fields.get("state", [""])[0]
            or any(len(values) != 1 for values in fields.values())
            or bool(fields.get("code", [""])[0]) == bool(fields.get("error", [""])[0])
            or any(any(ord(char) < 32 or ord(char) == 127 for char in item)
                   for values in fields.values() for item in values)):
        raise RelayError("invalid_callback")
    # Only the query is forwarded; a user-supplied authority is never requested.
    return parsed.query


def _get(opener, url, callback_port):
    request = Request(url, method="GET", headers={"Host": f"127.0.0.1:{callback_port}",
                      "User-Agent": "WeKeeper-local-authorization-relay", "Connection": "close"})
    try:
        return opener.open(request, timeout=5)
    except HTTPError as response:
        return response
    except (URLError, OSError, TimeoutError):
        raise RelayError("local_login_unavailable") from None


def authorization_location(opener, callback_port):
    response = _get(opener, f"http://127.0.0.1:{callback_port}/login", callback_port)
    try:
        location = response.headers.get("Location", "")
        if response.getcode() != 302 or len(location) > 65536 or any(ord(char) < 32 or ord(char) == 127 for char in location):
            raise RelayError("invalid_authorization_redirect")
        try:
            parsed = urlsplit(location)
            port = parsed.port
        except ValueError:
            raise RelayError("invalid_authorization_redirect") from None
        if (parsed.scheme != "https" or parsed.hostname != "auth.openai.com" or port not in (None, 443)
                or parsed.username is not None or parsed.password is not None or parsed.fragment
                or parsed.path != "/api/accounts/authorize"):
            raise RelayError("invalid_authorization_redirect")
        return location
    finally:
        response.close()


def submit_callback(opener, callback_port, value):
    query = validate_callback(value, callback_port)
    response = _get(opener, f"http://127.0.0.1:{callback_port}/auth/callback?{query}", callback_port)
    try:
        if response.getcode() != 200:
            raise RelayError("callback_not_accepted")
    finally:
        response.close()


def page(prefix, csrf, *, callback_port=1455, message="", failed=False, completed=False):
    status = f'<div class="notice {"error" if failed else "success"}">{escape(message)}</div>' if message else ""
    content = "" if completed else f"""
      <ol><li><strong>打开官方授权页面</strong><p>点击下方按钮，在 OpenAI 官方页面完成登录并授权 WeKeeper 使用你的订阅额度。</p>
        <a class="button" href="{escape(prefix, quote=True)}/start" target="_blank" rel="noopener noreferrer">开始授权 ↗</a></li>
        <li><strong>复制回调地址</strong><p>授权后，浏览器会跳转到 <code>127.0.0.1:{escape(str(callback_port))}</code>。如果页面打不开，请复制地址栏中的完整网址，并回到此页提交。</p></li>
        <li><strong>把回调交给本机 WeKeeper</strong><form method="post" action="{escape(prefix, quote=True)}/complete">
          <input type="hidden" name="csrf" value="{escape(csrf, quote=True)}">
          <label for="callback">完整回调网址</label>
          <textarea id="callback" name="callback_url" rows="4" maxlength="{MAX_CALLBACK}" required autocomplete="off" spellcheck="false" placeholder="http://127.0.0.1:{escape(str(callback_port))}/auth/callback?…"></textarea>
          <button type="submit">安全提交回调</button></form></li></ol>
      <p class="footnote">此页面只在本次授权期间开放。回调仅转交给你本机正在运行的 WeKeeper 登录流程；本页不保存回调或登录凭据。</p>"""
    return ("""<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
      <title>WeKeeper · 本机摘要授权</title><style>
      *{box-sizing:border-box}body{margin:0;background:#f5f6f3;color:#172b35;font:16px/1.65 system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
      main{max-width:700px;margin:6vh auto;padding:36px;background:white;border:1px solid #e1e6e1;border-radius:18px;box-shadow:0 12px 40px #243a3510}
      .brand{font-size:14px;font-weight:700;letter-spacing:.12em;color:#25705c}h1{font-size:28px;line-height:1.3;margin:14px 0 10px}.intro{color:#53636c}
      ol{padding-left:25px}li{padding:12px 0 16px 8px}li p{margin:7px 0 14px;color:#53636c}code{background:#f0f3ef;padding:2px 5px;border-radius:4px}
      .button,button{display:inline-block;background:#246f5b;color:white;border:0;border-radius:8px;padding:11px 20px;text-decoration:none;font:inherit;cursor:pointer}
      form{margin-top:12px}label{display:block;font-size:14px;font-weight:600}textarea{display:block;width:100%;margin:7px 0 14px;border:1px solid #bccbc3;border-radius:8px;padding:12px;font:14px/1.5 monospace;resize:vertical}
      .notice{padding:14px 18px;border-radius:8px;margin-top:22px}.error{background:#fff0ed;color:#933a27}.success{background:#edf8f0;color:#24624b}.footnote{font-size:13px;color:#748079;border-top:1px solid #e6eae5;padding-top:20px}
      @media(max-width:740px){main{margin:20px 14px;padding:25px 20px}h1{font-size:25px}}
      </style><main><div class="brand">WEKEEPER / CONNECT</div><h1>连接本机自动摘要</h1>
      <p class="intro">通过你的 ChatGPT 账号授权，让本机定时生成群聊摘要。</p>""" + status + content + "</main></html>").encode("utf-8")


class RelayHandler(BaseHTTPRequestHandler):
    server_version = "WeKeeper"
    sys_version = ""

    def log_message(self, *_):
        pass

    def _headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        # no-referrer makes native no-cors form POSTs serialize Origin as null.
        # same-origin keeps the strict Origin check usable while withholding
        # this temporary capability path from the external OAuth destination.
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")

    def _html(self, code=200, *, message="", failed=False, completed=False):
        body = page(self.server.prefix, self.server.csrf, callback_port=self.server.callback_port,
                    message=message, failed=failed, completed=completed)
        self.send_response(code)
        self._headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _valid_path(self):
        if time.monotonic() >= self.server.deadline:
            self._html(410, message="授权中转页面已到期，请重新发起授权。", failed=True, completed=True)
            return None
        try:
            parsed = urlsplit(self.path)
        except ValueError:
            self._html(404, message="页面不可用。", failed=True, completed=True)
            return None
        if parsed.query or parsed.fragment or parsed.scheme or parsed.netloc:
            self._html(404, message="页面不可用。", failed=True, completed=True)
            return None
        if parsed.path not in {self.server.prefix + "/", self.server.prefix + "/start", self.server.prefix + "/complete"}:
            self._html(404, message="页面不可用。", failed=True, completed=True)
            return None
        return parsed.path

    def do_GET(self):
        path = self._valid_path()
        if path is None:
            return
        if path == self.server.prefix + "/":
            return self._html()
        if path != self.server.prefix + "/start":
            return self._html(405, message="请使用页面中的提交表单。", failed=True)
        try:
            location = authorization_location(self.server.opener, self.server.callback_port)
        except RelayError:
            return self._html(503, message="本机授权流程暂时不可用，请等待重新启动后再试。", failed=True)
        self.send_response(302)
        self._headers()
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        path = self._valid_path()
        if path is None:
            return
        if path != self.server.prefix + "/complete":
            return self._html(405, message="请使用页面中的授权按钮。", failed=True)
        if self.headers.get("Origin") != self.server.public_origin:
            return self._html(403, message="提交来源校验失败，请从原授权页面提交。", failed=True)
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,8}", lengths[0]):
            return self._html(411, message="提交内容无效。", failed=True)
        length = int(lengths[0])
        if length > MAX_BODY:
            return self._html(413, message="回调网址过长，请重新复制完整地址。", failed=True)
        if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/x-www-form-urlencoded":
            return self._html(415, message="请使用页面中的提交表单。", failed=True)
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise RelayError("incomplete_body")
            form = parse_qs(raw.decode("utf-8"), keep_blank_values=True, strict_parsing=True,
                            encoding="utf-8", errors="strict", max_num_fields=4)
            if set(form) != {"csrf", "callback_url"} or any(len(values) != 1 for values in form.values()):
                raise RelayError("invalid_form")
            if not hmac.compare_digest(form["csrf"][0].encode("utf-8"), self.server.csrf.encode("utf-8")):
                return self._html(403, message="页面校验已失效，请刷新本页后重试。", failed=True)
            submit_callback(self.server.opener, self.server.callback_port, form["callback_url"][0])
        except (RelayError, UnicodeError, ValueError, OSError):
            return self._html(400, message="回调校验或提交失败，请确认已复制授权后地址栏中的完整网址。", failed=True)
        self._html(message="回调已安全提交，正在验证授权。请等待 WeKeeper 的完成提示。", completed=True)
        self.server.completed = True


class RelayServer(HTTPServer):
    allow_reuse_address = False

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address

    def handle_error(self, request, client_address):
        # Never print a traceback containing request paths or callback values.
        print("relay_request_failed", file=sys.stderr, flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Temporary HTTPS/SSH relay to a local WeKeeper OAuth callback")
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--public-origin", required=True)
    parser.add_argument("--port", type=int, default=1456)
    parser.add_argument("--callback-port", type=int, default=1455)
    parser.add_argument("--ttl", type=int, default=900)
    args = parser.parse_args(argv)
    try:
        prefix = normalize_prefix(args.prefix)
        origin = normalize_origin(args.public_origin)
        if not 1 <= args.port <= 65535 or not 1 <= args.callback_port <= 65535 or not 30 <= args.ttl <= 3600:
            raise RelayError("invalid_relay_settings")
        with RelayServer(("127.0.0.1", args.port), RelayHandler) as server:
            server.prefix, server.public_origin = prefix, origin
            server.callback_port = args.callback_port
            server.csrf = secrets.token_urlsafe(32)
            server.opener = local_opener()
            server.deadline = time.monotonic() + args.ttl
            server.completed = False
            server.timeout = 0.5
            stopped = [False]
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: stopped.__setitem__(0, True))
            print(json.dumps({"listening": f"127.0.0.1:{args.port}", "authorization_page": origin + prefix + "/", "ttl_seconds": args.ttl}), flush=True)
            while not stopped[0] and not server.completed and time.monotonic() < server.deadline:
                server.handle_request()
            print("authorization_callback_submitted" if server.completed else "authorization_relay_closed", flush=True)
            return 0 if server.completed else 2
    except (RelayError, OSError):
        print("authorization_relay_unavailable", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
