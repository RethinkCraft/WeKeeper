#!/usr/bin/env python3
"""Reproduce the pinned native gateway build. Never logs in or sends messages."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "deploy/protocol-gateway/upstream.lock.json"


def run(*command, cwd=None, capture=False, environment=None):
    return subprocess.run(command, cwd=cwd, env=environment, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def verify_source_tree(source, commit, patch_paths):
    """Compare every source blob with HEAD plus only the locked patches."""
    with tempfile.TemporaryDirectory(prefix="wekeeper-source-check-") as directory:
        environment = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}

        def git(*args):
            return subprocess.check_output(["git", *args], cwd=source, env=environment)

        git("read-tree", commit)
        for patch in patch_paths:
            git("apply", "--cached", str(patch))
        expected = {}
        algorithm = git("rev-parse", "--show-object-format").decode().strip()
        for entry in git("ls-files", "--stage", "-z").split(b"\0"):
            if not entry:
                continue
            metadata, name = entry.split(b"\t", 1)
            mode, object_id, stage = metadata.decode().split()
            if stage != "0" or mode not in {"100644", "100755", "120000"}:
                raise RuntimeError("unsupported_gateway_source_entry")
            relative = os.fsdecode(name)
            path = source / relative
            if mode == "120000":
                if not path.is_symlink():
                    raise RuntimeError("gateway_source_mismatch: " + relative)
                data = os.fsencode(os.readlink(path))
            else:
                if not path.is_file() or path.is_symlink():
                    raise RuntimeError("gateway_source_mismatch: " + relative)
                if bool(path.stat().st_mode & 0o111) != (mode == "100755"):
                    raise RuntimeError("gateway_source_mode_mismatch: " + relative)
                data = path.read_bytes()
            actual = hashlib.new(algorithm, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            if actual != object_id:
                raise RuntimeError("gateway_source_mismatch: " + relative)
            expected[relative] = object_id
        # Ignore rules are local mutable configuration, not a trust boundary.
        # Hidden .cargo/config.toml or build.rs files can change the build.
        actual_names = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "-z"], cwd=source)
        for raw in actual_names.split(b"\0"):
            if not raw:
                continue
            relative = os.fsdecode(raw)
            if relative.startswith("packages/native-client-rust/target/"):
                continue  # Cargo's output/cache directory, never an input checkout.
            if relative not in expected and (source / relative).exists():
                raise RuntimeError("gateway_unlocked_source_file: " + relative)
        return git("write-tree").decode().strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=ROOT / "private/protocol-gateway-rebuild")
    parser.add_argument("--build", action="store_true", help="Build a release executable using the pinned Rust toolchain")
    parser.add_argument("--check", action="store_true", help="Verify an existing source tree without modifying it")
    parser.add_argument("--verify-lock", action="store_true", help="Verify packaged patch checksums without a source checkout or network")
    args = parser.parse_args()
    if args.verify_lock and (args.check or args.build):
        parser.error("--verify-lock cannot be combined with --check or --build")
    lock = json.loads(LOCK.read_text())
    patch_paths = []
    for patch in lock["patches"]:
        path = (LOCK.parent / patch["file"]).resolve()
        if not path.is_relative_to(LOCK.parent.resolve()) or digest(path) != patch["sha256"]:
            raise RuntimeError("gateway_patch_checksum_mismatch")
        patch_paths.append(path)
    if args.verify_lock:
        print(json.dumps({"locked_patch_count": len(patch_paths), "commit": lock["commit"],
                          "rust_toolchain": lock["rust_toolchain"]}))
        return
    work = args.work_dir.expanduser().absolute()
    source = work / "source"
    if not source.exists():
        if args.check:
            raise RuntimeError("gateway_source_missing")
        work.mkdir(mode=0o700, parents=True, exist_ok=True)
        run("git", "clone", "--filter=blob:none", "--no-checkout", lock["repository"], str(source))
        run("git", "checkout", "--detach", lock["commit"], cwd=source)
    if run("git", "rev-parse", "HEAD", cwd=source, capture=True).strip() != lock["commit"]:
        raise RuntimeError("gateway_source_commit_mismatch")
    try:
        # This first constructs the entire ordered patch chain in an isolated
        # index. A broken later patch cannot leave a partly modified checkout.
        source_tree = verify_source_tree(source, lock["commit"], patch_paths)
    except RuntimeError:
        if args.check:
            raise
        # Only a pristine input tree may be changed; never repair over edits.
        verify_source_tree(source, lock["commit"], [])
        for patch in patch_paths:
            run("git", "apply", str(patch), cwd=source)
        source_tree = verify_source_tree(source, lock["commit"], patch_paths)
    # The recorded digest describes the final locked tree. A reviewed patch may
    # itself change Cargo.lock; pristine input is protected by the full upstream
    # tree/blob verification above, and no unlocked edit is repaired over.
    cargo_lock = source / "packages/native-client-rust/Cargo.lock"
    if digest(cargo_lock) != lock["cargo_lock_sha256"]:
        raise RuntimeError("gateway_dependency_lock_changed")
    run("git", "diff", "--check", cwd=source)
    result = {"commit": lock["commit"], "cargo_lock_sha256": lock["cargo_lock_sha256"],
              "source_tree": source_tree,
              "patches": lock["patches"], "built": False,
              "native_quote_live_validated": False, "phone_receipt_verified": False}
    if args.build:
        cargo = shutil.which("cargo")
        if not cargo:
            raise RuntimeError("cargo_missing_install_pinned_toolchain_first")
        manifest = source / "packages/native-client-rust/Cargo.toml"
        build_environment = {**os.environ,
            "WEKEEPER_GATEWAY_UPSTREAM_COMMIT": lock["commit"],
            "WEKEEPER_GATEWAY_SOURCE_TREE": source_tree}
        run(cargo, "+" + lock["rust_toolchain"], "build", "--release", "--locked",
            "--manifest-path", str(manifest), "-p", "wx-native", cwd=source,
            environment=build_environment)
        if verify_source_tree(source, lock["commit"], patch_paths) != source_tree:
            raise RuntimeError("gateway_source_changed_during_build")
        executable = source / "packages/native-client-rust/target/release/wx"
        result.update(built=True, executable=str(executable), executable_sha256=digest(executable))
    if not args.check:
        destination = work / "build-manifest.json"
        temporary = work / "build-manifest.json.tmp"
        temporary.write_text(json.dumps(result, indent=2) + "\n")
        temporary.chmod(0o600)
        os.replace(temporary, destination)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
