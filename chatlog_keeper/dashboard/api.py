"""Authenticated FastAPI transport for the independent dashboard read model."""
import asyncio
from contextlib import asynccontextmanager
import hmac
import json
import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.types import ASGIApp, Receive, Scope, Send

from .protocol import (Heartbeat, Partition, Summary, AvatarUpload, ConversationCreate, TurnCreate, JobClaim, JobEvents, JobResult, UsageBatch, QuotaSnapshot)
from .store import Conflict, DashboardStore, LoginThrottled

COOKIE = "wekeeper_session"
MAX_BODY = 16 * 1024 * 1024


class BodyLimit:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or scope.get("method") not in {"POST", "PUT", "PATCH", "DELETE"}:
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers", []))
        try:
            declared = int(headers.get(b"content-length", b"0"))
        except ValueError:
            declared = MAX_BODY + 1
        if declared > MAX_BODY:
            return await JSONResponse({"detail": "request_too_large"}, status_code=413)(scope, receive, send)
        chunks, total = [], 0
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                return
            chunk = event.get("body", b"")
            total += len(chunk)
            if total > MAX_BODY:
                return await JSONResponse({"detail": "request_too_large"}, status_code=413)(scope, receive, send)
            chunks.append(chunk)
            if not event.get("more_body", False):
                break
        body, delivered = b"".join(chunks), False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()
        await self.app(scope, replay, send)


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Login(Input):
    username: str = Field(min_length=1, max_length=80)
    password: str = Field(min_length=1, max_length=1024)


class UserCreate(Input):
    username: str = Field(min_length=1, max_length=80)
    password: str = Field(min_length=12, max_length=1024)
    role: Literal["admin", "viewer"] = "viewer"


class UserUpdate(Input):
    password: str | None = Field(default=None, min_length=12, max_length=1024)
    role: Literal["admin", "viewer"] | None = None
    disabled: bool | None = None


class RuleCreate(Input):
    name: str = Field(min_length=1, max_length=200)
    keyword: str = Field(min_length=1, max_length=200)
    group_ref: str | None = Field(default=None, max_length=256)
    enabled: bool = True


class RuleUpdate(Input):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    keyword: str | None = Field(default=None, min_length=1, max_length=200)
    group_ref: str | None = Field(default=None, max_length=256)
    enabled: bool | None = None


def create_app(data_dir=None, machine_token=None, static_dir=None, secure_cookies=True):
    root = Path(data_dir or os.environ.get("WEKEEPER_DASHBOARD_DATA_DIR", os.environ.get("WEKEEPER_DATA_DIR", "~/.local/share/wekeeper-dashboard"))).expanduser()
    if machine_token is None:
        token_file = os.environ.get("WEKEEPER_MACHINE_TOKEN_FILE")
        machine_token = Path(token_file).read_text().strip() if token_file else os.environ.get("WEKEEPER_MACHINE_TOKEN")
    if not isinstance(machine_token, str) or len(machine_token) < 32:
        raise ValueError("machine_token_must_have_at_least_32_characters")
    public_origin = os.environ.get("WEKEEPER_PUBLIC_ORIGIN")
    if public_origin:
        parsed_origin = urlsplit(public_origin)
        if parsed_origin.scheme not in {"https", "http"} or not parsed_origin.netloc or parsed_origin.path not in {"", "/"} or parsed_origin.username or parsed_origin.password:
            raise ValueError("invalid_public_origin")
        public_origin = (parsed_origin.scheme, parsed_origin.netloc.lower())
    store = DashboardStore(root)

    @asynccontextmanager
    async def lifespan(app):
        async def monitor():
            while True:
                await asyncio.sleep(15)
                await asyncio.to_thread(store.check_stale_sources)
                await asyncio.to_thread(store.expire_chat_jobs)
        task = asyncio.create_task(monitor())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            store.close()

    app = FastAPI(title="WeKeeper Dashboard", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.store = store
    app.add_middleware(BodyLimit)

    @app.middleware("http")
    async def response_security(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        if request.url.path.startswith(("/api", "/v1")):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request, exc):
        # Pydantic's default error embeds submitted values, including prohibited fields.
        return JSONResponse({"detail": "invalid_request", "errors": [{"loc": list(e["loc"]), "type": e["type"]} for e in exc.errors()]}, status_code=422)

    @app.exception_handler(Conflict)
    async def conflict_handler(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(ValueError)
    async def value_error_handler(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(KeyError)
    async def not_found_handler(request, exc):
        return JSONResponse({"detail": exc.args[0] if exc.args else "not_found"}, status_code=404)

    def valid_origin(request: Request):
        origin = request.headers.get("origin")
        if request.headers.get("sec-fetch-site") == "cross-site":
            raise HTTPException(403, "cross_origin_write_denied")
        if origin:
            expected = public_origin or (request.url.scheme, request.url.netloc.lower())
            supplied = urlsplit(origin)
            if (supplied.scheme, supplied.netloc.lower()) != expected or supplied.path not in {"", "/"}:
                raise HTTPException(403, "cross_origin_write_denied")

    def session(request: Request):
        result = store.session(request.cookies.get(COOKIE))
        if not result:
            raise HTTPException(401, "authentication_required")
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            valid_origin(request)
            token = request.headers.get("x-csrf-token", "")
            if not hmac.compare_digest(token, result["csrf_token"]):
                raise HTTPException(403, "csrf_token_required")
        return result

    def admin(auth=Depends(session)):
        if auth["user"]["role"] != "admin":
            raise HTTPException(403, "admin_required")
        return auth

    def machine(request: Request):
        token = request.headers.get("authorization", "")
        if not hmac.compare_digest(token, "Bearer " + machine_token):
            raise HTTPException(401, "machine_authentication_required")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/api/auth/login")
    def login(body: Login, request: Request, response: Response):
        valid_origin(request)
        try:
            auth = store.login(body.username, body.password, request.client.host if request.client else "")
        except LoginThrottled:
            raise HTTPException(429, "too_many_login_attempts") from None
        if not auth:
            raise HTTPException(401, "invalid_credentials")
        response.set_cookie(COOKIE, auth["token"], httponly=True, secure=secure_cookies, samesite="strict", max_age=7 * 86400, path="/")
        return {"user": auth["user"], "csrf_token": auth["csrf_token"]}

    @app.get("/api/auth/me")
    def me(auth=Depends(session)):
        return auth

    @app.post("/api/auth/logout")
    def logout(request: Request, response: Response, auth=Depends(session)):
        store.logout(request.cookies.get(COOKIE, ""))
        response.delete_cookie(COOKIE, path="/", secure=secure_cookies, httponly=True, samesite="strict")
        return {"ok": True}

    @app.post("/v1/ingest/partitions", dependencies=[Depends(machine)])
    async def partitions(body: Partition, request: Request):
        raw = await request.json()
        # Hash the exact sender projection, not default-enriched Pydantic output.
        return store.ingest_partition({**body.model_dump(mode="json"), "data": raw["data"]})

    @app.post("/v1/ingest/heartbeat", dependencies=[Depends(machine)])
    def heartbeat(body: Heartbeat):
        return store.ingest_heartbeat(body.model_dump(mode="json"))

    @app.post("/v1/ingest/summaries", dependencies=[Depends(machine)])
    def summaries_ingest(body: Summary):
        return store.ingest_summary(body.model_dump(mode="json", exclude_none=True))

    @app.get("/api/overview")
    def overview(since: str | None = None, until: str | None = None, auth=Depends(session)):
        return store.overview(auth["user"]["id"], since, until)

    @app.get("/api/groups")
    def groups(since: str | None = None, until: str | None = None, auth=Depends(session)):
        return {"groups": store.groups(since, until)}

    @app.get("/api/groups/{group_ref}/stats")
    def stats(group_ref: str, since: str | None = None, until: str | None = None, auth=Depends(session)):
        return store.group_stats(group_ref, since, until)

    @app.get("/api/groups/{group_ref}/messages")
    def messages(group_ref: str, since: str | None = None, until: str | None = None, q: str | None = None, limit: int = 50, before: str | None = None, auth=Depends(session)):
        return store.messages(group_ref, since, until, q, limit, before)

    @app.get("/api/groups/{group_ref}/summaries")
    def group_summaries(group_ref: str, auth=Depends(session)):
        return {"summaries": store.summaries(group_ref)}

    @app.get("/api/messages/{message_ref}")
    def context(message_ref: str, auth=Depends(session)):
        return store.message_context(message_ref)

    @app.get("/api/alerts")
    def alerts(state: str = "all", auth=Depends(session)):
        return {"alerts": store.alerts(auth["user"]["id"], state)}

    @app.post("/api/alerts/{alert_id}/read")
    def read_alert(alert_id: int, auth=Depends(session)):
        return store.mark_alert(alert_id, auth["user"]["id"])

    @app.post("/api/alerts/{alert_id}/ack")
    def acknowledge_alert(alert_id: int, auth=Depends(session)):
        return store.mark_alert(alert_id, auth["user"]["id"], acknowledge=True)

    @app.get("/api/rules")
    def rules(auth=Depends(admin)):
        return {"rules": store.list_rules()}

    @app.post("/api/rules")
    def create_rule(body: RuleCreate, auth=Depends(admin)):
        return store.save_rule(body.model_dump())

    @app.patch("/api/rules/{rule_id}")
    def update_rule(rule_id: int, body: RuleUpdate, auth=Depends(admin)):
        return store.save_rule(body.model_dump(exclude_unset=True), rule_id)

    @app.delete("/api/rules/{rule_id}")
    def delete_rule(rule_id: int, auth=Depends(admin)):
        store.delete_rule(rule_id)
        return {"ok": True}

    @app.get("/api/users")
    def users(auth=Depends(admin)):
        return {"users": store.list_users()}

    @app.post("/api/users")
    def create_user(body: UserCreate, auth=Depends(admin)):
        return store.create_user(body.username, body.password, body.role)

    @app.patch("/api/users/{user_id}")
    def update_user(user_id: int, body: UserUpdate, auth=Depends(admin)):
        return store.update_user(user_id, body.model_dump(exclude_none=True))

    @app.get("/api/events")
    async def events(request: Request, auth=Depends(session)):
        token = request.cookies.get(COOKIE)
        try:
            previous = int(request.headers.get("last-event-id", "-1"))
        except ValueError:
            previous = -1

        async def stream():
            nonlocal previous
            keepalive = 0
            while not await request.is_disconnected():
                if not store.session(token):
                    yield "event: auth_required\ndata: {}\n\n"
                    break
                revision = store.revision()
                if previous != revision:
                    previous = revision
                    yield f"id: {revision}\nevent: refresh\ndata: {json.dumps({'revision': revision})}\n\n"
                    keepalive = 0
                elif keepalive >= 15:
                    yield ": keepalive\n\n"
                    keepalive = 0
                await asyncio.sleep(1)
                keepalive += 1
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-store", "X-Accel-Buffering": "no", "Connection": "keep-alive"})

    @app.post("/v1/ingest/avatars", dependencies=[Depends(machine)])
    def avatars_ingest(body: AvatarUpload):
        return store.ingest_avatar(body.model_dump(mode="json"))

    @app.get("/api/groups/{group_ref}/senders/{sender_id}/avatar")
    def avatar(group_ref: str, sender_id: str, expected_hash: str | None = Query(default=None, alias="hash"), auth=Depends(session)):
        value = store.avatar(group_ref, sender_id, expected_hash)
        return Response(value["data"], media_type=value["mime_type"], headers={"ETag": '\"' + value["avatar_hash"] + '\"', "Cache-Control": "private, no-store"})

    @app.get("/api/chat/conversations")
    def conversations(auth=Depends(session)):
        return {"conversations": store.conversations(auth["user"]["id"])}

    @app.post("/api/chat/conversations")
    def create_conversation(body: ConversationCreate, auth=Depends(session)):
        return {"conversation": store.create_conversation(auth["user"]["id"], body.model_dump(mode="json"))}

    @app.get("/api/chat/conversations/{conversation_id}")
    def conversation(conversation_id: str, auth=Depends(session)):
        return store.conversation(conversation_id, auth["user"]["id"])

    @app.post("/api/chat/conversations/{conversation_id}/turns")
    def create_turn(conversation_id: str, body: TurnCreate, auth=Depends(session)):
        return {"turn": store.create_turn(conversation_id, auth["user"]["id"], body.model_dump(mode="json", exclude_none=True))}

    @app.get("/api/chat/turns/{turn_id}")
    def get_turn(turn_id: str, auth=Depends(session)):
        return {"turn": store.chat_turn(turn_id, auth["user"]["id"])}

    @app.post("/api/chat/turns/{turn_id}/cancel")
    def cancel_turn(turn_id: str, auth=Depends(session)):
        return {"turn": store.cancel_turn(turn_id, auth["user"]["id"])}

    @app.post("/v1/agent/jobs/claim", dependencies=[Depends(machine)])
    def claim_job(body: JobClaim):
        return store.claim_job(body.model_dump(mode="json"))

    @app.post("/v1/agent/jobs/{turn_id}/events", dependencies=[Depends(machine)])
    def job_events(turn_id: str, body: JobEvents):
        return store.append_job_events(turn_id, body.model_dump(mode="json", exclude_none=True))

    @app.post("/v1/agent/jobs/{turn_id}/result", dependencies=[Depends(machine)])
    def job_result(turn_id: str, body: JobResult):
        return store.restore_job_result(turn_id, body.model_dump(mode="json"))

    @app.get("/api/chat/turns/{turn_id}/events")
    async def chat_stream(turn_id: str, request: Request, after: int = Query(default=0, ge=0), auth=Depends(session)):
        user_id = auth["user"]["id"]
        store.chat_turn(turn_id, user_id)  # Check ownership before opening SSE.
        token = request.cookies.get(COOKIE)
        try:
            after = max(after, int(request.headers.get("last-event-id", "0")))
        except ValueError:
            raise HTTPException(400, "invalid_event_cursor") from None

        async def stream():
            cursor, quiet, previous_status = after, 0, None
            while not await request.is_disconnected():
                if not store.session(token):
                    yield "event: auth_required\ndata: {}\n\n"
                    break
                events = store.chat_events(turn_id, user_id, cursor)
                for event in events:
                    cursor = event["seq"]
                    yield f"id: {cursor}\nevent: {event['type']}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                turn = store.chat_turn(turn_id, user_id)
                status = (turn["status"], turn["cancel_requested"])
                if status != previous_status:
                    previous_status = status
                    data = {"status": turn["status"], "cancel_requested": turn["cancel_requested"], "last_seq": turn["last_seq"]}
                    yield "event: status\ndata: " + json.dumps(data) + "\n\n"
                if turn["status"] in {"completed", "failed", "interrupted", "cancelled"} and cursor >= turn["last_seq"]:
                    break
                if events:
                    quiet = 0
                elif quiet >= 15:
                    yield ": keepalive\n\n"
                    quiet = 0
                await asyncio.sleep(0.5)
                quiet += 0.5
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-store", "X-Accel-Buffering": "no"})

    @app.post("/v1/ingest/usage", dependencies=[Depends(machine)])
    def usage_ingest(body: UsageBatch):
        return store.ingest_usage(body.model_dump(mode="json"))

    @app.post("/v1/ingest/quota", dependencies=[Depends(machine)])
    def quota_ingest(body: QuotaSnapshot):
        return store.ingest_quota(body.model_dump(mode="json"))

    @app.get("/api/usage")
    def usage(since: str | None = None, until: str | None = None, auth=Depends(session)):
        return store.usage(auth["user"]["id"], auth["user"]["role"] == "admin", since, until)

    static_path = Path(static_dir or os.environ.get("WEKEEPER_DASHBOARD_STATIC_DIR", os.environ.get("WEKEEPER_STATIC_DIR", Path(__file__).with_name("static"))))
    if static_path.is_dir():
        app.mount("/", StaticFiles(directory=static_path, html=True), name="dashboard")
    return app
