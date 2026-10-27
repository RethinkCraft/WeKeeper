"""Check distributable contents before publishing, without extracting private files."""
from __future__ import annotations

import argparse
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
    ".tsbuildinfo", ".tgz", ".tar", ".tar.gz",
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
}
WHEEL_REQUIRED = {
    "chatlog_keeper/__init__.py", "chatlog_keeper/mcp_server.py",
    "chatlog_keeper/dashboard/cli.py", "chatlog_keeper/dashboard/api.py",
    "chatlog_keeper/dashboard/agent.py", "chatlog_keeper/dashboard/summaries.py",
    "chatlog_keeper/dashboard/avatar_backfill.py",
}


def check(artifact: Path) -> None:
    if artifact.name.endswith(".tar.gz"):
        with tarfile.open(artifact, "r:gz") as archive:
            members = archive.getmembers()
            if any(member.issym() or member.islnk() for member in members):
                raise ValueError(f"{artifact.name}: symbolic/hard links are not allowed")
            paths = [PurePosixPath(member.name) for member in members]
        roots = {path.parts[0] for path in paths if path.parts}
        if len(roots) != 1:
            raise ValueError(f"{artifact.name}: expected a single source directory")
        if any(path.is_absolute() or ".." in path.parts for path in paths):
            raise ValueError(f"{artifact.name}: unsafe archive path")
        names = {str(PurePosixPath(*path.parts[1:])) for path in paths if len(path.parts) > 1}
        required = SOURCE_REQUIRED | WHEEL_REQUIRED
    elif artifact.suffix == ".whl":
        with zipfile.ZipFile(artifact) as archive:
            names = set(archive.namelist())
        required = WHEEL_REQUIRED
        if not any(name.endswith(".dist-info/entry_points.txt") for name in names):
            raise ValueError(f"{artifact.name}: missing console entry points")
    else:
        raise ValueError(f"unsupported release artifact: {artifact.name}")
    missing = sorted(required - names)
    forbidden = []
    for name in sorted(names):
        path = PurePosixPath(name)
        if (path.is_absolute() or ".." in path.parts or FORBIDDEN_PARTS.intersection(path.parts)
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
