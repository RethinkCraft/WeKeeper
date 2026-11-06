#!/usr/bin/env python3
"""Explicit Direct opt-in for the shared calibrated runtime benchmark.

The unmodified engine owns scheduling, transport timing and business oracles.
This adapter observes provider construction and verifies local execution using
the copied request DB and actual usage DB. It never opens a WeChat connection.
"""
from __future__ import annotations

import argparse
from contextvars import ContextVar
from copy import deepcopy
import hashlib
import importlib
import importlib.util
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import quote


BASE_SHA = '542d3bcdc368e5b71cc0d9f85e56ac56fe652722c0774491ca5fbcbaabcfe544'
PHASE_SHA = '6f0695a32b530b6b4325aaefd09f5ef8407e05023f681ff05ce7c744e9e80d3f'
PHASE_PATH = Path(__file__).with_name("benchmark_protocol_runtime_phased.py")
BASE_PATH = Path(__file__).with_name("benchmark_protocol_runtime.py")
ROUTE = "direct_native_plan_v1"
TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens")
REQUEST_FIELDS = ("event_id", "message_ref", "group_ref", "sender_id", "text", "occurred_at",
                  "business_id", "profile_id", "persona_id", "persona_version")


def checked_phase_engine():
    # Check the actual files before executing either wrapper or base engine.
    for path, expected in ((BASE_PATH, BASE_SHA), (PHASE_PATH, PHASE_SHA)):
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError("benchmark_adapter_engine_changed")
    spec = importlib.util.spec_from_file_location("_direct_phase_engine", PHASE_PATH)
    phased = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(phased)
    return phased


class DirectAdapter:
    def __init__(self, *, enabled=False):
        if type(enabled) is not bool:
            raise ValueError("invalid_direct_benchmark_option")
        self.enabled = enabled
        self.request_context = ContextVar("benchmark_provider_request", default=None)
        self.lock = threading.Lock()
        self.resources = None
        self.provider_constructions = []
        self.generated_requests = set()
        self.local_checks = {}
        self.plan_validator = None

    def metadata(self):
        with self.lock:
            return {"direct_native_plan_enabled": self.enabled,
                "adapter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "base_engine_sha256": BASE_SHA, "phase_engine_sha256": PHASE_SHA,
                "adapter_is_production_source": False,
                "phase_calibration_is_production_optimization": False,
                "provider_constructions": deepcopy(self.provider_constructions),
                "local_execution_checks": deepcopy(self.local_checks)}

    def wrap_components(self, components):
        wrapped = dict(components)
        resources_class, generator_factory, worker_class = (
            components[name] for name in ("ProviderResources", "ReplyGenerator", "ProtocolBusinessWorker"))
        adapter = self

        class ObservedResources(resources_class):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                if adapter.resources is not None:
                    raise RuntimeError("benchmark_multiple_resource_owners")
                adapter.resources = self

        def observed_generator(*args, **kwargs):
            original_provider = kwargs["provider_factory"]

            def observed_provider(profile):
                # Count constructor attempts even if construction raises. The
                # ContextVar propagates through asyncio.to_thread in generate.
                with adapter.lock:
                    adapter.provider_constructions.append({
                        "request_id": adapter.request_context.get(),
                        "profile_id": profile.get("id"), "at": time.monotonic()})
                return original_provider(profile)

            kwargs["provider_factory"] = observed_provider
            if adapter.enabled:
                if adapter.resources is None:
                    raise RuntimeError("benchmark_resource_owner_missing")
                kwargs["local_execution_factory"] = adapter.resources.local_business_execution
            return generator_factory(*args, **kwargs)

        class ObservedWorker(worker_class):
            def _generate(self, request, nearby, progress):
                identifier = request["event_id"]
                with adapter.lock:
                    adapter.generated_requests.add(identifier)
                token = adapter.request_context.set(identifier)
                try:
                    return super()._generate(request, nearby, progress)
                finally:
                    adapter.request_context.reset(token)

        wrapped.update(ProviderResources=ObservedResources, ReplyGenerator=observed_generator,
                       ProtocolBusinessWorker=ObservedWorker)
        return wrapped

    def local_route_errors(self, evidence, injected, frozen):
        errors = []
        identifier = injected["request_id"]
        if not self.enabled:
            errors.append("direct_route_without_opt_in")
        if (evidence.get("execution_route") != ROUTE or evidence.get("model_invoked") is not False
                or type(evidence.get("model_calls")) is not int or evidence["model_calls"] != 0
                or "usage_run_id" not in evidence or evidence["usage_run_id"] is not None):
            errors.append("invalid_local_execution_evidence")
        with self.lock:
            bound = identifier in self.generated_requests
            constructors = deepcopy(self.provider_constructions)
        if not bound or any(row["request_id"] is None for row in constructors):
            errors.append("provider_observation_not_bound")
        count = sum(row["request_id"] == identifier for row in constructors)
        if count:
            errors.append("local_route_constructed_model_provider")
        if self.resources is None or self.plan_validator is None:
            return [*errors, "local_execution_resources_missing"]

        store = self.resources.store
        request = store.request(identifier)
        claimed = evidence.get("request") or {}
        binding = store.binding(request["group_ref"]) if request else None
        if (not request or any(claimed.get(key) != request.get(key) for key in REQUEST_FIELDS)
                or claimed.get("source_event") != request["event"]
                or request["event"].get("native_source") != frozen["source"]):
            errors.append("local_request_evidence_mismatch")
        if (not binding or not binding.get("enabled") or request["status"] == "cancelled"
                or binding.get("business") != request["event"].get("native_business")
                or binding.get("profile") != request["profile"] or binding.get("persona") != request["persona"]
                or binding.get("business_id") != request["business_id"]):
            errors.append("local_request_binding_mismatch")
        try:
            plan = self.plan_validator(request or {}, (binding or {}).get("business") or {})
            if plan is None or plan != evidence.get("direct_plan"):
                errors.append("persisted_direct_plan_mismatch")
        except (TypeError, ValueError, KeyError):
            errors.append("persisted_direct_plan_mismatch")

        execution_id = evidence.get("execution_run_id")
        if not isinstance(execution_id, str) or not re.fullmatch(r"local_[0-9a-f]{32}", execution_id):
            errors.append("invalid_local_execution_run_id")
        else:
            try:
                path = Path(self.resources.usage_dir) / "usage.sqlite"
                uri = "file:" + quote(str(path.resolve()), safe="/") + "?mode=ro"
                with sqlite3.connect(uri, uri=True) as ledger:
                    ledger.row_factory = sqlite3.Row
                    run = ledger.execute("SELECT * FROM usage_runs WHERE run_id=?", (execution_id,)).fetchone()
                    calls = ledger.execute("SELECT count(*) FROM usage_calls WHERE run_id=?", (execution_id,)).fetchone()[0]
                    siblings = ledger.execute("SELECT run_id FROM usage_runs WHERE json_extract(metadata,'$.bot_request_id')=?",
                                              (identifier,)).fetchall()
                if not run:
                    errors.append("local_execution_run_missing")
                else:
                    metadata = json.loads(run["metadata"])
                    usage = json.loads(run["usage"]) if run["usage"] else None
                    times = (run["started_at"], run["completed_at"])
                    if not isinstance(metadata, dict):
                        metadata = {}
                    if (run["kind"] != "local_business" or run["model"] != "local-business"
                            or run["status"] != "completed" or run["user_id"] is not None
                            or run["turn_id"] is not None or run["error_code"] is not None
                            or any(type(value) not in (int, float) or not math.isfinite(value) for value in times)
                            or times[1] < times[0]
                            or metadata.get("provider") != "local-business" or metadata.get("model_invoked") is not False
                            or metadata.get("execution_route") != ROUTE
                            or metadata.get("bot_request_id") != identifier
                            or metadata.get("group_ref") != (request or {}).get("group_ref")
                            or metadata.get("profile_id") != (request or {}).get("profile_id")):
                        errors.append("local_execution_run_mismatch")
                    if (not isinstance(usage, dict) or set(usage) != set(TOKEN_FIELDS)
                            or any(type(usage[key]) is not int or usage[key] != 0 for key in TOKEN_FIELDS)):
                        errors.append("local_execution_tokens_not_zero")
                    if calls != 0:
                        errors.append("local_execution_has_model_calls")
                    if len(siblings) != 1 or siblings[0]["run_id"] != execution_id:
                        errors.append("local_request_has_extra_usage_runs")
            except (sqlite3.Error, OSError, ValueError, TypeError):
                errors.append("local_execution_ledger_unreadable")
        with self.lock:
            self.local_checks[identifier] = {"passed": not errors, "errors": list(errors),
                "provider_constructions": count, "execution_run_id": execution_id}
        return errors

    def inspect_case(self, original, *args, **kwargs):
        errors = list(original(*args, **kwargs))
        # Engine passes positional fixture arguments and keyword item metadata.
        frozen = kwargs.get("frozen", args[2] if len(args) > 2 else None)
        injected = kwargs.get("injected", args[1] if len(args) > 1 else None)
        if not frozen:
            return errors
        evidence = frozen["evidence"]
        route = evidence.get("execution_route")
        if route == ROUTE:
            local_errors = self.local_route_errors(evidence, injected, frozen)
            if not local_errors:
                # The original combined check also tests event_id. Our request
                # DB check above independently verified it, so only its usage
                # requirement is replaced. Every other oracle error survives.
                errors = [error for error in errors if error != "current_request_evidence_missing"]
            errors.extend(local_errors)
        elif route is not None or "execution_run_id" in evidence or "direct_plan" in evidence:
            errors.append("unknown_local_execution_route")
        return errors


def configure_engine(phased, adapter):
    engine = phased.engine
    original_load, original_inspect, original_json, original_transport = (
        getattr(engine, name) for name in ("load_components", "inspect_case", "private_json", "transport_configuration"))

    def load(code_root):
        components, build = original_load(code_root)
        if adapter.enabled:
            module = importlib.import_module("chatlog_keeper.bot.direct_plan")
            if not Path(module.__file__).resolve().is_relative_to(Path(code_root).resolve()):
                raise ValueError("benchmark_direct_plan_code_root_mismatch")
            adapter.plan_validator = module.saved_native_plan
        build["benchmark_adapter"] = adapter.metadata()
        return adapter.wrap_components(components), build

    def transport(*args, **kwargs):
        result = original_transport(*args, **kwargs)
        result["provenance"]["direct_adapter"] = adapter.metadata()
        return result

    def private_json(path, value):
        if Path(path).name in {"manifest.json", "report.json"}:
            value["benchmark_adapter"] = adapter.metadata()
        return original_json(path, value)

    engine.load_components = load
    engine.inspect_case = lambda *args, **kwargs: adapter.inspect_case(original_inspect, *args, **kwargs)
    engine.private_json, engine.transport_configuration = private_json, transport
    return engine


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--phase-calibration", type=Path, required=True)
    parser.add_argument("--direct-native-plan", action="store_true")
    args, remaining = parser.parse_known_args(argv)
    phased = checked_phase_engine()
    phased.configure_engine(args.phase_calibration)
    engine = configure_engine(phased, DirectAdapter(enabled=args.direct_native_plan))
    return engine.main(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
