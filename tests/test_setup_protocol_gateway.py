"""Exercise setup against tiny real Git repositories, never the live gateway."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/setup_protocol_gateway.py"


def git(source, *args):
    return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c",
        "user.email=fixture@example.invalid", *args], cwd=source, text=True, stderr=subprocess.PIPE).strip()


@pytest.fixture
def checkout(tmp_path, monkeypatch, request):
    # Neither a developer's global ignores nor an inherited alternate index
    # may affect this isolated fixture. Tests add local ignores deliberately.
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.delenv("GIT_INDEX_FILE", raising=False)
    work = tmp_path / "work"
    source = work / "source"
    source.mkdir(parents=True)
    git(source, "init", "--quiet")
    (source / "src").mkdir()
    library = source / "src/lib.rs"
    library.write_text("pub const VALUE: u32 = 0;\n")
    dependent = getattr(request, "param", None) == "dependent"
    if not dependent:
        for version in (2, 3):
            (source / f"src/part_{version}.rs").write_text("pub const VALUE: u32 = 0;\n")
    (source / "README.md").write_text("Synthetic fixture; no network or account data.\n")
    (source / ".gitignore").write_text("target/\n")
    cargo = source / "packages/native-client-rust/Cargo.lock"
    cargo.parent.mkdir(parents=True)
    cargo.write_text("# Synthetic dependency lock; no build is attempted.\n")
    git(source, "add", ".")
    git(source, "commit", "--quiet", "-m", "fixture base")
    commit = git(source, "rev-parse", "HEAD")
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    patches = []
    # Use real Git diffs. One dedicated case overlaps predecessor content;
    # ordinary mutation tests use independent patches to isolate their defect.
    for version in (1, 2, 3):
        target = library if dependent or version == 1 else source / f"src/part_{version}.rs"
        target.write_text(f"pub const VALUE: u32 = {version};\n")
        patch = deployment / f"000{version}.patch"
        patch.write_text(git(source, "diff", "--binary") + "\n")
        patches.append({"file": patch.name, "sha256": hashlib.sha256(patch.read_bytes()).hexdigest()})
        git(source, "add", ".")
        git(source, "commit", "--quiet", "-m", f"fixture patch {version}")
    git(source, "reset", "--hard", commit)
    lock = deployment / "upstream.lock.json"
    lock.write_text(json.dumps({"repository": "https://must-not-be-contacted.invalid/fixture.git",
        "commit": commit, "cargo_lock_sha256": hashlib.sha256(cargo.read_bytes()).hexdigest(),
        "rust_toolchain": "unused-fixture", "patches": patches}))
    spec = importlib.util.spec_from_file_location("fixture_gateway_setup", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "LOCK", lock)

    def invoke(*args):
        monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--work-dir", str(work), *args])
        module.main()

    return source, work, deployment, lock, invoke


def test_fresh_checkout_applies_locked_patches_and_check_reproduces_tree(checkout, capsys):
    source, work, _, _, invoke = checkout
    with pytest.raises(RuntimeError, match="gateway_(patches_not_applied|source_mismatch)"):
        invoke("--check")
    invoke()
    first = json.loads(capsys.readouterr().out)
    assert (source / "src/lib.rs").read_text() == "pub const VALUE: u32 = 1;\n"
    assert first["built"] is False
    assert first["native_quote_live_validated"] is first["phone_receipt_verified"] is False
    assert json.loads((work / "build-manifest.json").read_text())["source_tree"] == first["source_tree"]
    invoke("--check")
    checked = json.loads(capsys.readouterr().out)
    assert checked["source_tree"] == first["source_tree"]
    git(source, "add", ".")
    assert git(source, "write-tree") == checked["source_tree"]


@pytest.mark.parametrize("staged", [False, True])
def test_additional_source_is_rejected_whether_staged_or_untracked(checkout, staged):
    source, _, _, _, invoke = checkout
    invoke()
    path = source / "src/unlocked.rs"
    path.write_text("pub fn unlocked() {}\n")
    if staged:
        git(source, "add", "src/unlocked.rs")
    with pytest.raises(RuntimeError, match="gateway_unlocked_source_file"):
        invoke("--check")


def test_unrelated_tracked_source_edit_cannot_hide_behind_applied_patch_state(checkout):
    source, _, _, _, invoke = checkout
    invoke()
    (source / "README.md").write_text("Not part of any locked patch.\n")
    with pytest.raises(RuntimeError, match="gateway_source_mismatch"):
        invoke("--check")


def test_file_mode_is_part_of_the_frozen_source(checkout):
    source, _, _, _, invoke = checkout
    invoke()
    (source / "README.md").chmod(0o755)
    with pytest.raises(RuntimeError, match="gateway_source_mode_mismatch"):
        invoke("--check")


def test_local_git_ignore_cannot_hide_extra_cargo_build_configuration(checkout):
    source, _, _, _, invoke = checkout
    invoke()
    path = source / "packages/native-client-rust/.cargo/config.toml"
    path.parent.mkdir()
    path.write_text('[build]\nrustflags = ["--cfg", "unlocked_fixture"]\n')
    (source / ".git/info/exclude").write_text("packages/native-client-rust/.cargo/config.toml\n")
    assert git(source, "check-ignore", "packages/native-client-rust/.cargo/config.toml")
    with pytest.raises(RuntimeError, match="gateway_unlocked_source_file"):
        invoke("--check")


def test_modified_locked_patch_is_rejected_before_changing_checkout(checkout):
    source, _, deployment, _, invoke = checkout
    original = (source / "src/lib.rs").read_text()
    patch = deployment / "0002.patch"
    patch.write_text(patch.read_text() + "\n# not part of lock\n")
    with pytest.raises(RuntimeError, match="gateway_patch_checksum_mismatch"):
        invoke()
    assert (source / "src/lib.rs").read_text() == original


def test_dependency_lock_change_is_rejected_before_patch_application(checkout):
    source, _, _, _, invoke = checkout
    (source / "packages/native-client-rust/Cargo.lock").write_text("different dependencies\n")
    with pytest.raises(RuntimeError, match="gateway_(dependency_lock_changed|source_mismatch)"):
        invoke()
    assert (source / "src/lib.rs").read_text() == "pub const VALUE: u32 = 0;\n"


@pytest.mark.parametrize("checkout", ["dependent"], indirect=True)
def test_overlapping_dependent_patches_apply_in_order_and_recheck(checkout, capsys):
    source, _, _, _, invoke = checkout
    invoke()
    first = json.loads(capsys.readouterr().out)
    assert (source / "src/lib.rs").read_text() == "pub const VALUE: u32 = 3;\n"
    invoke("--check")
    assert json.loads(capsys.readouterr().out)["source_tree"] == first["source_tree"]


@pytest.mark.parametrize("checkout", ["dependent"], indirect=True)
def test_reordered_dependent_patches_cannot_be_treated_as_applied(checkout):
    source, _, _, lock, invoke = checkout
    original = (source / "src/lib.rs").read_text()
    metadata = json.loads(lock.read_text())
    metadata["patches"].reverse()
    lock.write_text(json.dumps(metadata))
    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        invoke()
    assert (source / "src/lib.rs").read_text() == original


def dependency_edge_patch(checkout):
    source, _, deployment, lock, _ = checkout
    cargo = source / "packages/native-client-rust/Cargo.lock"
    original = cargo.read_bytes()
    final = original + b"# One locked synthetic dependency edge.\n"
    cargo.write_bytes(final)
    patch = deployment / "0004-dependency-edge.patch"
    patch.write_text(git(source, "diff", "--binary") + "\n")
    cargo.write_bytes(original)
    metadata = json.loads(lock.read_text())
    metadata["patches"].append({"file": patch.name, "sha256": hashlib.sha256(patch.read_bytes()).hexdigest()})
    metadata["cargo_lock_sha256"] = hashlib.sha256(final).hexdigest()
    lock.write_text(json.dumps(metadata))
    return cargo, original, final, patch


def test_locked_dependency_edge_patch_precedes_final_lock_check(checkout, capsys):
    source, work, _, _, invoke = checkout
    cargo, _, final, _ = dependency_edge_patch(checkout)
    invoke()
    first = json.loads(capsys.readouterr().out)
    assert cargo.read_bytes() == final
    assert first["cargo_lock_sha256"] == hashlib.sha256(final).hexdigest()
    invoke("--check")
    checked = json.loads(capsys.readouterr().out)
    assert checked["source_tree"] == first["source_tree"]
    assert json.loads((work / "build-manifest.json").read_text())["source_tree"] == first["source_tree"]
    assert (source / "src/lib.rs").read_text() == "pub const VALUE: u32 = 1;\n"


@pytest.mark.parametrize("already_patched", [False, True])
def test_invalid_final_dependency_pin_is_rejected(checkout, already_patched, capsys):
    _, work, _, lock, invoke = checkout
    dependency_edge_patch(checkout)
    if already_patched:
        invoke()
        capsys.readouterr()
    manifest = work / "build-manifest.json"
    previous = manifest.read_bytes() if manifest.exists() else None
    metadata = json.loads(lock.read_text())
    metadata["cargo_lock_sha256"] = "0" * 64
    lock.write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="gateway_dependency_lock_changed"):
        invoke(*(["--check"] if already_patched else []))
    assert (manifest.read_bytes() if manifest.exists() else None) == previous


def test_missing_dependency_patch_fails_before_checkout_changes(checkout):
    source, work, _, _, invoke = checkout
    cargo, original, _, patch = dependency_edge_patch(checkout)
    patch.unlink()
    with pytest.raises(OSError):
        invoke()
    assert cargo.read_bytes() == original
    assert (source / "src/lib.rs").read_text() == "pub const VALUE: u32 = 0;\n"
    assert not (work / "build-manifest.json").exists()


def test_omitted_dependency_patch_cannot_match_the_final_lock_pin(checkout):
    _, work, _, lock, invoke = checkout
    dependency_edge_patch(checkout)
    metadata = json.loads(lock.read_text())
    metadata["patches"].pop()
    lock.write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="gateway_dependency_lock_changed"):
        invoke()
    assert not (work / "build-manifest.json").exists()


@pytest.mark.parametrize("check_only", [False, True])
def test_patched_dependency_tampering_is_not_repaired(checkout, check_only, capsys):
    _, work, _, _, invoke = checkout
    cargo, _, _, _ = dependency_edge_patch(checkout)
    invoke()
    capsys.readouterr()
    previous_manifest = (work / "build-manifest.json").read_bytes()
    tampered = b"# Unlocked dependency change.\n"
    cargo.write_bytes(tampered)
    with pytest.raises(RuntimeError, match="gateway_(source_mismatch|dependency_lock_changed)"):
        invoke(*(["--check"] if check_only else []))
    assert cargo.read_bytes() == tampered
    assert (work / "build-manifest.json").read_bytes() == previous_manifest
