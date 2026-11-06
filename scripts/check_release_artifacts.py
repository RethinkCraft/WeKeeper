"""Check distributable contents before publishing, without extracting private files."""
from __future__ import annotations

import argparse
import hashlib
import json
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath
import tarfile
import zipfile


FORBIDDEN_PARTS = {
    ".git", ".venv", "venv", "node_modules", "__pycache__", ".pytest_cache",
    ".mypy_cache", ".ruff_cache", "private", "state", "data", "secrets",
    "runtime", "dist", "build", "archive", "results", "downloads", "wechat",
}
FORBIDDEN_SUFFIXES = (
    ".pyc", ".pyo", ".key", ".pem", ".p12", ".pfx", ".db", ".db-wal",
    ".db-shm", ".sqlite", ".sqlite-wal", ".sqlite-shm", ".dat", ".log",
    ".tsbuildinfo", ".tgz", ".tar", ".tar.gz", ".bak",
)
FORBIDDEN_PATTERNS = ("*.db*", "*.sqlite*")
SOURCE_REQUIRED = {
    "CHANGELOG.md", ".dockerignore", "pyproject.toml", "README.md", "README_zh.md",
    "docs/dashboard.md", "deploy/dashboard/Dockerfile", "deploy/dashboard/Dockerfile.prebuilt",
    "deploy/dashboard/requirements.lock", "deploy/dashboard/compose.prebuilt.yaml",
    "deploy/wekeeper-avatar-backfill.service.example", "scripts/check_release_artifacts.py",
    "frontend/package.json", "frontend/package-lock.json", "frontend/vite.config.ts",
    "frontend/src/App.tsx", "frontend/tests/range-chat.test.mjs",
    "tests/test_dashboard_api.py", "tests/test_avatar_backfill.py",
    "docs/bot.md", "docs/protocol-gateway.md", "docs/upgrading.md",
    "deploy/protocol-gateway/upstream.lock.json",
    "deploy/protocol-gateway/LICENSE",
    "deploy/protocol-gateway/wekeeper-protocol-business.service.example",
    "deploy/protocol-gateway/wekeeper-archive-bridge.service.example",
    "scripts/setup_protocol_gateway.py", "scripts/run_protocol_bot.py",
    "scripts/run_archive_bridge.py",
}
WHEEL_REQUIRED = {
    "chatlog_keeper/__init__.py", "chatlog_keeper/mcp_server.py",
    "chatlog_keeper/dashboard/cli.py", "chatlog_keeper/dashboard/api.py",
    "chatlog_keeper/dashboard/agent.py",
    "chatlog_keeper/dashboard/avatar_backfill.py",
    "chatlog_keeper/dashboard/inference_state.py", "chatlog_keeper/dashboard/maintenance.py",
    "chatlog_keeper/dashboard/usage_sync.py",
    "chatlog_keeper/bot/cli.py", "chatlog_keeper/bot/mcp.py",
    "chatlog_keeper/bot/runtime_cli.py", "chatlog_keeper/bot/control.py",
    "chatlog_keeper/bot/protocol_runtime.py", "chatlog_keeper/bot/protocol_native_inbox.py",
    "chatlog_keeper/bot/protocol_outbox.py", "chatlog_keeper/bot/archive_bridge.py",
    "chatlog_keeper/bot/memory_host.py", "chatlog_keeper/bot/deferred_usage.py",
}
RETIRED_FILES = {
    "chatlog_keeper/dashboard/summaries.py", "chatlog_keeper/bot/worker.py",
    "chatlog_keeper/bot/host.py",
    "chatlog_keeper/bot/pilot.py", "chatlog_keeper/bot/native_quote.py",
    "chatlog_keeper/bot/native_quote_ui.py", "chatlog_keeper/bot/native_acceptance.py",
    "deploy/wekeeper-summary.service.example", "deploy/wekeeper-bot.service.example",
    "scripts/archive_bridge_schema4_write.py",
}


def check(artifact: Path) -> None:
    if artifact.name.endswith(".tar.gz"):
        with tarfile.open(artifact, "r:gz") as archive:
            members = archive.getmembers()
            if any(member.issym() or member.islnk() for member in members):
                raise ValueError(f"{artifact.name}: symbolic/hard links are not allowed")
            paths = [PurePosixPath(member.name) for member in members]
            contents = {str(PurePosixPath(*PurePosixPath(member.name).parts[1:])):
                        archive.extractfile(member).read()
                        for member in members if member.isfile() and (
                            member.name.endswith("/upstream.lock.json") or member.name.endswith(".patch"))}
        roots = {path.parts[0] for path in paths if path.parts}
        if len(roots) != 1:
            raise ValueError(f"{artifact.name}: expected a single source directory")
        if any(path.is_absolute() or ".." in path.parts for path in paths):
            raise ValueError(f"{artifact.name}: unsafe archive path")
        names = {str(PurePosixPath(*path.parts[1:])) for path in paths if len(path.parts) > 1}
        required = SOURCE_REQUIRED | WHEEL_REQUIRED
        lock_path = "deploy/protocol-gateway/upstream.lock.json"
        if lock_path in contents:
            lock = json.loads(contents[lock_path])
            for patch in lock["patches"]:
                relative = PurePosixPath(patch["file"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError(f"{artifact.name}: unsafe patch path")
                name = str(PurePosixPath(lock_path).parent / relative)
                if name not in contents or hashlib.sha256(contents[name]).hexdigest() != patch["sha256"]:
                    raise ValueError(f"{artifact.name}: missing or corrupt locked patch: {name}")
    elif artifact.suffix == ".whl":
        with zipfile.ZipFile(artifact) as archive:
            names = set(archive.namelist())
            if any((member.external_attr >> 16) & 0o170000 == 0o120000 for member in archive.infolist()):
                raise ValueError(f"{artifact.name}: symbolic links are not allowed")
        required = WHEEL_REQUIRED
        if not any(name.endswith(".dist-info/entry_points.txt") for name in names):
            raise ValueError(f"{artifact.name}: missing console entry points")
    else:
        raise ValueError(f"unsupported release artifact: {artifact.name}")
    missing = sorted(required - names)
    forbidden = []
    for name in sorted(names):
        path = PurePosixPath(name)
        if (name in RETIRED_FILES or name.startswith("chatlog_keeper/benchmark/")
                or path.is_absolute() or ".." in path.parts or FORBIDDEN_PARTS.intersection(path.parts)
                or path.name.startswith(".env") or name.endswith(FORBIDDEN_SUFFIXES)
                or any(fnmatch(path.name, pattern) for pattern in FORBIDDEN_PATTERNS)):
            forbidden.append(name)
    if missing or forbidden:
        raise ValueError(f"{artifact.name}: missing={missing}, forbidden={forbidden}")
    print(f"{artifact.name}: {len(names)} members checked; required files present; no private/build artifacts")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path, nargs="+")
    args = parser.parse_args()
    for artifact in args.artifacts:
        check(artifact)


if __name__ == "__main__":
    main()
