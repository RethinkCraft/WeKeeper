"""Explicit local configuration, credentials and robot worker entry points."""
from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path
import uuid


def parser():
    p = argparse.ArgumentParser(prog="wekeeper-bot")
    p.add_argument("--state-dir", default=os.environ.get("CHATLOG_KEEPER_DATA_DIR", str(Path.home() / ".local/share/wekeeper")))
    p.add_argument("--dashboard-dir", help="Existing local dashboard/SIWC state")
    p.add_argument("--usage-dir", help="Optional isolated bot usage ledger; authentication still uses dashboard-dir")
    subs = p.add_subparsers(dest="command", required=True)
    subs.add_parser("status")
    init = subs.add_parser("configure")
    init.add_argument("file", help="Local JSON configuration, never an API key")
    bind = subs.add_parser("bind")
    bind.add_argument("--bot-wxid", required=True, help="Verified native WeChat identity, not a display name")
    key = subs.add_parser("secret-set")
    key.add_argument("reference", help="Protected local credential reference")
    verify = subs.add_parser("verify-provider")
    verify.add_argument("profile_id")
    from .runtime_cli import add_runtime_options
    for command in ("check", "run"):
        add_runtime_options(subs.add_parser(command), shared=True)
    bootstrap = subs.add_parser("cloud-bootstrap", help="Align cloud configuration from current local state; no model calls")
    bootstrap.add_argument("--archive-state", type=Path, required=True)
    bootstrap.add_argument("--alignment-id", required=True, help="Stable idempotency identifier for this alignment")
    mcp = subs.add_parser("mcp")
    mcp.add_argument("--group-ref", required=True, help="Read-only business scope for an external MCP client")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    from .store import BotStore
    from .errors import safe_error
    if args.command in {"check", "run"}:
        if args.activate and args.command != "run":
            parser().error("--activate requires run")
        from .runtime_cli import run_command
        return run_command(args)
    store = BotStore(args.state_dir)
    dashboard = Path(args.dashboard_dir or Path(args.state_dir) / "dashboard")
    usage_dir = Path(args.usage_dir or dashboard)
    try:
        if args.command == "configure":
            value = json.loads(Path(args.file).read_text())
            revision = store.configure(value)
            print(json.dumps({"configured": True, "revision": revision}))
        elif args.command == "bind":
            if not args.bot_wxid.strip() or len(args.bot_wxid) > 256:
                raise ValueError("bot_identity_invalid")
            store.set("bot_wxid", args.bot_wxid)
            print(json.dumps({"identity_configured": True}))
        elif args.command == "secret-set":
            from .providers import save_credential
            save_credential(args.state_dir, args.reference, getpass.getpass("Provider API key (hidden): "))
            print(json.dumps({"credential_configured": True, "reference": args.reference}))
        elif args.command == "verify-provider":
            from .providers import ChatGPTProvider, CompatibleProvider, profile_signature
            config = store.config()
            profile = next((row for row in config["providers"] if row["id"] == args.profile_id), None)
            if not profile:
                raise ValueError("bot_provider_not_configured")
            group = next((row["group_ref"] for row in config["groups"] if row["provider_id"] == profile["id"]), None)
            if not group:
                raise ValueError("bot_provider_group_binding_required")
            called = []
            provider = ChatGPTProvider(dashboard, profile, usage_dir=usage_dir) if profile["kind"] == "siwc" else CompatibleProvider(
                args.state_dir, usage_dir, {**profile, "tools_verified": True})
            try:
                provider.complete([{"role": "user", "content": "请调用list_offer_items一次，再简短回复验证完成。"}],
                    "本轮是工具能力自检，先调用提供的只读工具，再回复；不要编造工具结果。",
                    [{"name": "list_offer_items", "description": "只读工具自检", "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}],
                    lambda name, arguments: called.append((name, arguments)) or {"ok": True, "items": []},
                    {"run_id": "bot_verify_" + uuid.uuid4().hex, "provider": profile["kind"],
                     "profile_id": profile["id"], "group_ref": group, "bot_request_id": "verify_" + uuid.uuid4().hex})
            finally:
                provider.close()
            if not called:
                raise ValueError("bot_provider_tool_validation_failed")
            store.set("verified_profile_" + profile["id"], profile_signature(profile))
            print(json.dumps({"tools_verified": True, "profile_id": profile["id"]}))
        elif args.command == "status":
            from .protocol_runtime import _read_db
            deliveries = {}
            outbox = store.get("protocol_outbox_file")
            if outbox:
                deliveries = dict(_read_db(Path(outbox), "SELECT state,COUNT(*) FROM replies GROUP BY state"))
            queued = store.conn.execute("""SELECT COUNT(*) FROM requests r JOIN reply_outbox o USING(event_id)
                WHERE o.kind='final' AND o.status='awaiting_generation'
                AND json_extract(r.event,'$.transport')='native_protocol'""").fetchone()[0]
            print(json.dumps({"revision": store.config()["revision"], "enabled": store.config()["enabled"],
                "groups": len(store.config()["groups"]), "identity_configured": bool(store.get("bot_wxid")),
                "runtime": store.get("runtime", {}), "delivery_counts": deliveries,
                "pending_replies": queued + deliveries.get("queued", 0) + deliveries.get("submitting", 0),
                "last_error": store.get("last_model_error"), "last_latency": store.get("last_latency"),
                "latency_metrics": store.get("latency_metrics", {}), "active": store.get("active"),
                "last_request": store.get("last_request")}, ensure_ascii=False))
        elif args.command == "mcp":
            from .mcp import create_server
            server = create_server(args.state_dir, group_ref=args.group_ref)
            try:
                server.run(transport="stdio")
            finally:
                server.bot_store.close()
        elif args.command == "cloud-bootstrap":
            from .control import ControlClient
            control = ControlClient(store=store, state_dir=args.state_dir, archive_state=args.archive_state,
                                    dashboard_dir=dashboard)
            try:
                print(json.dumps(control.bootstrap(args.alignment_id)))
            finally:
                control.close()
        return 0
    except Exception as exc:
        print(json.dumps({"error": safe_error(exc)}))
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
