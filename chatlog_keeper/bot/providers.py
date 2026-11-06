"""SIWC and API-key transports for the business-only agent loop.

No provider may add hosted tools, follow message URLs, or choose another
provider as a fallback. Tool names and schemas come from the local MCP host.
"""
from __future__ import annotations

import json
import hashlib
from datetime import date
import os
from pathlib import Path
import re
import time
import uuid
from urllib.parse import urlsplit

import httpx

from chatlog_keeper.dashboard.siwc import SIWCClient
from chatlog_keeper.dashboard.usage import UsageLedger

from .model_http_trace import begin_trace

BUSINESS_TOOLS = frozenset({"list_offer_items", "query_offer_status", "report_offer_status"})


def profile_signature(profile):
    value = {key: profile.get(key) for key in ("kind", "model", "base_url", "credential_ref", "reasoning_effort")}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class ProviderError(RuntimeError):
    pass


def validate_tools(tools):
    names = [tool.get("name") for tool in tools]
    if not names or len(names) != len(set(names)) or not set(names).issubset(BUSINESS_TOOLS):
        raise ProviderError("bot_unapproved_tools")


def _read_gate(metadata, tools):
    """Host policy stays private; it is never passed to the usage schema."""
    required = metadata.pop("_required_read_tool", None)
    items = metadata.pop("_required_read_items", ())
    day = metadata.pop("_required_read_day", None)
    default_day = metadata.pop("_required_read_default_day", None)
    if required is not None and (required != "query_offer_status"
            or required not in {tool["name"] for tool in tools}):
        raise ProviderError("bot_invalid_required_read_tool")
    if not isinstance(items, (list, tuple)) or any(not isinstance(item, str) for item in items):
        raise ProviderError("bot_invalid_required_read_tool")
    if required and (not _iso_day(day) or not _iso_day(default_day)):
        raise ProviderError("bot_invalid_required_read_date")
    return required, tuple(items), day, default_day


def _report_gate(metadata, tools):
    required = metadata.pop("_required_report", None)
    if required is None:
        return None
    if (not isinstance(required, dict) or set(required) != {"event_id", "day", "items"}
            or not isinstance(required["event_id"], str) or not required["event_id"]
            or not _iso_day(required["day"]) or not isinstance(required["items"], list)
            or not required["items"] or "report_offer_status" not in {tool["name"] for tool in tools}
            or any(not isinstance(item, dict) or set(item) != {"item_id", "status"}
                   or not isinstance(item["item_id"], str) or not item["item_id"]
                   or item["status"] not in {"available", "unavailable", "unknown"}
                   for item in required["items"])
            or len({item["item_id"] for item in required["items"]}) != len(required["items"])):
        raise ProviderError("bot_invalid_required_report")
    return required


def _verified_final_callback(metadata):
    # An in-process host capability, never model input or accounting metadata.
    callback = metadata.pop("_verified_tool_final", None)
    if callback is not None and not callable(callback):
        raise ProviderError("bot_invalid_verified_final_callback")
    return callback


def report_tool_succeeded(name, output, required, seen_items=None):
    """Only a successful receipt bound to this request can complete its write."""
    if not required or name != "report_offer_status":
        return False
    seen = seen_items if seen_items is not None else set()
    expected = {item["item_id"]: item["status"] for item in required["items"]}
    if (not isinstance(output, dict) or output.get("ok") is not True or output.get("recorded") is not True
            or output.get("event_id") != required["event_id"] or output.get("day") != required["day"]
            or output.get("item_id") not in expected
            or output.get("status") != expected[output["item_id"]]):
        return False
    seen.add(output["item_id"])
    return set(expected).issubset(seen)


def _iso_day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def read_tool_succeeded(name, output, required, items=(), seen_items=None, *,
                        arguments=None, expected_day=None, default_day=None):
    if not required or name != required:
        return False
    seen = seen_items if seen_items is not None else set()
    returned = output.get("items") if isinstance(output, dict) else None
    supplied_days = ([arguments[key] for key in ("date", "day") if key in arguments]
                     if isinstance(arguments, dict) else [])
    if (not _iso_day(expected_day) or not _iso_day(default_day) or not isinstance(arguments, dict)
            or any(not _iso_day(day) or day != expected_day for day in supplied_days)
            or not supplied_days and default_day != expected_day
            or not isinstance(output, dict) or output.get("ok") is not True or output.get("day") != expected_day
            or not isinstance(returned, list) or not returned
            or any(not isinstance(item, dict) or item.get("day") != expected_day for item in returned)):
        # A later failed/wrong-day read must revoke earlier coverage, too.
        seen.clear()
        return False
    seen.update(item["item_id"] for item in returned if isinstance(item, dict)
                and isinstance(item.get("item_id"), str))
    return set(items).issubset(seen)


def _usage(value):
    if not isinstance(value, dict):
        return None
    details = value.get("prompt_tokens_details") or {}
    output = value.get("completion_tokens_details") or {}
    details = details if isinstance(details, dict) else {}
    output = output if isinstance(output, dict) else {}
    return {"input_tokens": value.get("prompt_tokens"),
            "output_tokens": value.get("completion_tokens"), "total_tokens": value.get("total_tokens"),
            "cached_input_tokens": value.get("prompt_cache_hit_tokens", details.get("cached_tokens")),
            "reasoning_output_tokens": output.get("reasoning_tokens")}


def _model_identifier(value, credential):
    """Only a bounded model ID may enter diagnostics, never response text."""
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}", value)
            or "://" in value or value.casefold().startswith(("sk-", "sk_", "bearer", "gsk_", "hf_", "aiza"))
            or credential and credential in value):
        return None
    return value


def credential_path(state_dir, reference):
    if not isinstance(reference, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", reference):
        raise ProviderError("bot_invalid_credential_reference")
    root = Path(state_dir).expanduser() / "bot" / "secrets"
    path = root / (reference + ".key")
    if root.is_symlink() or root.parent.is_symlink() or path.is_symlink():
        raise ProviderError("bot_unsafe_credential_path")
    return path


def save_credential(state_dir, reference, value):
    path = credential_path(state_dir, reference)
    if not isinstance(value, str) or not value.strip() or "\n" in value.strip():
        raise ProviderError("bot_invalid_credential")
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.parent.chmod(0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(value.strip())
        stream.flush()
        os.fsync(stream.fileno())


def read_credential(state_dir, reference):
    path = credential_path(state_dir, reference)
    try:
        if path.stat().st_mode & 0o077:
            raise ProviderError("bot_credential_permissions")
        value = path.read_text().strip()
    except OSError:
        raise ProviderError("bot_credential_missing") from None
    if not value:
        raise ProviderError("bot_credential_missing")
    return value


class ChatGPTProvider:
    def __init__(self, dashboard_dir, profile, *, client=None, usage_dir=None, http_client=None):
        self.profile = profile
        self.client = client or SIWCClient(dashboard_dir, http_client=http_client)
        self.owned = client is None
        if usage_dir is not None:
            if self.client.usage_ledger is not None:
                raise ProviderError("bot_usage_ledger_already_configured")
            self.client.usage_ledger = UsageLedger(usage_dir)

    def complete(self, messages, instructions, tools, handler, metadata):
        if tools:
            validate_tools(tools)
        metadata = dict(metadata)
        timeout = min(60, max(.1, float(metadata.pop("_timeout", 60))))
        on_event = metadata.pop("_on_event", None)
        required_read, required_items, required_day, default_day = _read_gate(metadata, tools)
        required_report = _report_gate(metadata, tools)
        # SIWC retains its existing bounded tool/final loop. Do not forward a
        # local callable to its run metadata or silently change that strategy.
        _verified_final_callback(metadata)
        read_satisfied = False
        report_satisfied, report_items = False, set()
        read_items = set()
        def checked_handler(name, arguments):
            nonlocal read_satisfied, report_satisfied
            if name not in {tool["name"] for tool in tools}:
                raise ProviderError("bot_unapproved_tool_call")
            output = handler(name, arguments)
            if name == required_read:
                read_satisfied = read_tool_succeeded(name, output, required_read, required_items, read_items,
                    arguments=arguments, expected_day=required_day, default_day=default_day)
            if required_report and name == "report_offer_status":
                report_satisfied = report_tool_succeeded(name, output, required_report, report_items)
            return output
        functions = [{"type": "function", "name": t["name"], "description": t.get("description", ""), "strict": False,
                      "parameters": t["parameters"]} for t in tools]
        result = self.client.bot_chat(messages, instructions=instructions,
            tools=[{"type": "namespace", "name": "business", "description": "仅本轮绑定业务的首单查询与获授权报备。", "tools": functions}] if tools else [],
            tool_handler=checked_handler, model=self.profile.get("model") or None,
            run_metadata=metadata, timeout=timeout, on_event=on_event,
            reasoning_effort=self.profile.get("reasoning_effort", "low"))
        # SIWC owns its existing bounded loop and authorization. Until it
        # supports this host gate, never accept an ungrounded business final.
        if required_read and not read_satisfied:
            raise ProviderError("bot_required_read_not_satisfied")
        if required_report and not report_satisfied:
            raise ProviderError("bot_required_report_not_satisfied")
        return result["output_text"]

    def close(self):
        if self.owned:
            self.client.close()


class CompatibleProvider:
    def __init__(self, state_dir, dashboard_dir, profile, *, client=None, ledger=None, clock=time.monotonic, on_close=None):
        self.profile, self.clock = profile, clock
        defaults = {"deepseek": "https://api.deepseek.com", "openai": "https://api.openai.com/v1"}
        base = str(profile.get("base_url") or defaults.get(profile.get("kind"), "")).rstrip("/")
        parsed = urlsplit(base)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ProviderError("bot_invalid_provider_endpoint")
        self.url = base + "/chat/completions"
        self.key = read_credential(state_dir, profile.get("credential_ref"))
        self.http = client or httpx.Client(timeout=60, follow_redirects=False)
        self.owned = client is None
        self.ledger = ledger or UsageLedger(dashboard_dir)
        self.own_ledger = ledger is None
        self._on_close, self._closed = on_close, False

    def complete(self, messages, instructions, tools, handler, metadata):
        if tools:
            validate_tools(tools)
        metadata = dict(metadata)
        timeout = min(60, max(.1, float(metadata.pop("_timeout", 60))))
        on_event = metadata.pop("_on_event", None)
        # Optional request-local buffer: generation does not wait on FULL commits.
        ledger = metadata.pop("_defer_usage_ledger", None) or self.ledger
        required_read, required_items, required_day, default_day = _read_gate(metadata, tools)
        required_report = _report_gate(metadata, tools)
        verified_tool_final = _verified_final_callback(metadata)
        if not self.profile.get("tools_verified"):
            raise ProviderError("bot_provider_tools_not_verified")
        history = [{"role": "system", "content": instructions}, *messages]
        deadline, tool_count, read_satisfied = self.clock() + timeout, 0, False
        report_satisfied, report_items = False, set()
        read_items = set()
        usage_started = self.clock()
        try:
            run_id = ledger.start_run(kind="bot_chat", model=self.profile["model"],
                **{k: v for k, v in metadata.items() if k not in {"kind", "model"}})
        finally:
            if on_event:
                on_event({"type": "timing", "stage": "usage_start", "seconds": self.clock() - usage_started})
        error, pending_call, failed_http_trace = None, None, None
        def record_pending():
            nonlocal pending_call
            if pending_call is None:
                return
            usage_started = self.clock()
            try:
                ledger.record_call(run_id, **pending_call)
                pending_call = None
            finally:
                if on_event:
                    on_event({"type": "timing", "stage": "usage_record_call", "seconds": self.clock() - usage_started})
        try:
            for turn in range(4):
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise ProviderError("bot_provider_timeout")
                pending_read = bool(required_read and not read_satisfied)
                pending_report = bool(required_report and not report_satisfied)
                required_tool = required_read if pending_read else "report_offer_status" if pending_report else None
                round_tools = [tool for tool in tools if (not required_tool or tool["name"] == required_tool)
                               and not (report_satisfied and tool["name"] == "report_offer_status")]
                body = {"model": self.profile["model"], "messages": history, "stream": False}
                if round_tools:
                    body["tools"] = [{"type": "function", "function": tool} for tool in round_tools]
                if required_tool:
                    # DeepSeek Chat Completions named tool_choice, also used by
                    # compatible providers. The host still checks actual calls.
                    body["tool_choice"] = {"type": "function", "function": {"name": required_tool}}
                if self.profile.get("kind") == "deepseek":
                    body.update(max_tokens=512, thinking={"type": "disabled"})
                elif self.profile.get("kind") == "openai":
                    body["max_completion_tokens"] = 512
                else:
                    body["max_tokens"] = 512
                try:
                    http_trace = begin_trace(body, turn + 1)
                except BaseException:
                    http_trace = None
                call_trace, returned = None, False
                call_started = self.clock()
                try:
                    if http_trace is None:
                        response = self.http.post(self.url, headers={"Authorization": "Bearer " + self.key},
                                                  json=body, timeout=max(.1, remaining))
                    else:
                        response = self.http.post(self.url, headers={"Authorization": "Bearer " + self.key},
                            json=body, timeout=max(.1, remaining), extensions={"trace": http_trace.callback})
                    returned = True
                finally:
                    if http_trace is not None:
                        try:
                            call_trace = http_trace.finish(returned=returned)
                        except BaseException:
                            pass
                        if not returned:
                            failed_http_trace = call_trace
                    if on_event:
                        elapsed = max(0, self.clock() - call_started)
                        on_event({"type": "timing", "stage": "model_call", "seconds": elapsed})
                        on_event({"type": "timing", "stage": "model_call_" + str(turn + 1), "seconds": elapsed})
                # A returned response is an actual call even if its envelope is
                # malformed. Keep unknown usage unknown, then finish atomically.
                pending_call = {"call_id": str(uuid.uuid4()), "response_id": None,
                    "request_id": response.headers.get("x-request-id"), "usage": None,
                    "status": "failed" if response.status_code >= 400 else "completed"}
                if call_trace is not None:
                    pending_call["model_http_trace"] = call_trace
                if len(response.content) > 1024 * 1024:
                    raise ProviderError("bot_provider_output_limit")
                try:
                    data = response.json()
                except ValueError:
                    if response.status_code < 400:
                        raise
                    data = None
                if isinstance(data, dict):
                    identifier = data.get("id")
                    pending_call.update(response_id=identifier if isinstance(identifier, str) else None,
                                        usage=_usage(data.get("usage")))
                if response.status_code >= 400:
                    category = {401: "auth", 403: "auth", 429: "rate_limit"}.get(response.status_code, "http")
                    raise ProviderError("bot_provider_" + category)
                if not isinstance(data, dict):
                    raise ProviderError("bot_provider_invalid_response")
                message = data["choices"][0]["message"]
                if on_event:
                    model = (_model_identifier(data.get("model"), self.key)
                             or _model_identifier(self.profile.get("model"), self.key))
                    if model:
                        on_event({"type": "model", "model": model})
                calls = message.get("tool_calls") or []
                if not calls:
                    if required_tool:
                        if turn == 3:
                            raise ProviderError("bot_required_read_not_satisfied" if pending_read
                                                else "bot_required_report_not_satisfied")
                        # Discard the ungrounded final, retaining this request's
                        # tool outputs and budget. Never restart or replay writes.
                        record_pending()
                        repair = ("当前请求必须先成功调用query_offer_status查询" + required_day +
                            "，再依据本次工具结果回答。历史回复和其他日期结果不能代替本次查询。" if pending_read else
                            "当前消息已明确要求报备。必须先调用report_offer_status，以当前消息原文作为证据，"
                            "取得本次保存成功的结果后再回答；尚未执行工具不能自行声称成功或失败。待报备项目与状态：" +
                            json.dumps([item for item in required_report["items"]
                                        if item["item_id"] not in report_items], ensure_ascii=False))
                        history.append({"role": "system", "content": repair})
                        continue
                    text = message.get("content")
                    if not isinstance(text, str) or not text.strip():
                        raise ProviderError("bot_provider_empty_response")
                    if self.clock() >= deadline:
                        raise ProviderError("bot_provider_timeout")
                    return text.strip()
                if turn == 3:
                    raise ProviderError("bot_tool_round_limit")
                # Reject the whole batch before any side effect if a provider
                # ignores the query-only tool choice or advertises another name.
                if any(call["function"]["name"] not in {tool["name"] for tool in round_tools} for call in calls):
                    raise ProviderError("bot_unapproved_tool_call")
                history.append({"role": "assistant", "content": message.get("content"), "tool_calls": calls})
                for call in calls:
                    tool_count += 1
                    function = call["function"]
                    if tool_count > 9 or function["name"] not in BUSINESS_TOOLS:
                        raise ProviderError("bot_unapproved_tool_call")
                    arguments = function["arguments"]
                    if not isinstance(arguments, str) or len(arguments) > 16000:
                        raise ProviderError("bot_invalid_tool_arguments")
                    arguments = json.loads(arguments)
                    if not isinstance(arguments, dict):
                        raise ProviderError("bot_invalid_tool_arguments")
                    if self.clock() >= deadline:
                        raise ProviderError("bot_provider_timeout")
                    # Intermediate responses are durable before any business
                    # side effect; only the terminal response joins finish_run.
                    record_pending()
                    if self.clock() >= deadline:
                        raise ProviderError("bot_provider_timeout")
                    output = handler(function["name"], arguments)
                    if function["name"] == required_read:
                        read_satisfied = read_tool_succeeded(function["name"], output, required_read, required_items, read_items,
                            arguments=arguments, expected_day=required_day, default_day=default_day)
                    if required_report and function["name"] == "report_offer_status":
                        report_satisfied = report_tool_succeeded(function["name"], output, required_report, report_items)
                    history.append({"role": "tool", "tool_call_id": call["id"],
                                    "content": json.dumps(output, ensure_ascii=False)})
                    if len(json.dumps(history, ensure_ascii=False)) > 80000:
                        raise ProviderError("bot_context_limit")
                # Finish only after the entire advertised batch has executed.
                # The host separately proves complete current facts/receipts;
                # a provider gate alone is not enough to invent a final. Mixed
                # read/write work and optional tools retain the original loop.
                if (verified_tool_final is not None and bool(required_read) != bool(required_report)
                        and (not required_read or read_satisfied)
                        and (not required_report or report_satisfied)):
                    if self.clock() >= deadline:
                        raise ProviderError("bot_provider_timeout")
                    final = verified_tool_final()
                    if final is not None:
                        if not isinstance(final, str) or not final.strip() or len(final) > 4000:
                            raise ProviderError("bot_invalid_verified_final")
                        if self.clock() >= deadline:
                            raise ProviderError("bot_provider_timeout")
                        return final
        except (httpx.TimeoutException, TimeoutError):
            error = "bot_provider_timeout"
            raise ProviderError(error) from None
        except ProviderError as exc:
            error = str(exc)
            raise
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError):
            error = "bot_provider_invalid_response"
            raise ProviderError(error) from None
        except Exception:
            error = "bot_provider_execution_failed"
            raise ProviderError(error) from None
        finally:
            finish = getattr(ledger, "finish_with_call", None)
            if pending_call is not None and callable(finish):
                usage_started = self.clock()
                try:
                    finish(run_id, call=pending_call, status="failed" if error else "completed", error_code=error)
                    pending_call = None
                finally:
                    if on_event:
                        on_event({"type": "timing", "stage": "usage_finish_with_call", "seconds": self.clock() - usage_started})
            else:
                # Preserve the existing injectable-ledger interface as well.
                record_pending()
                usage_started = self.clock()
                try:
                    if failed_http_trace is None:
                        ledger.finish_run(run_id, status="failed" if error else "completed", error_code=error)
                    else:
                        ledger.finish_run(run_id, status="failed" if error else "completed", error_code=error,
                                          model_http_trace=failed_http_trace)
                finally:
                    if on_event:
                        on_event({"type": "timing", "stage": "usage_finish", "seconds": self.clock() - usage_started})

    def close(self):
        if getattr(self, "_closed", False):
            return
        self._closed = True
        try:
            if self.owned:
                self.http.close()
            if self.own_ledger:
                self.ledger.close()
        finally:
            callback = getattr(self, "_on_close", None)
            if callback is not None:
                callback()
