"""Release regressions: source builds need the full locked gateway patch chain."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import zipfile

import pytest


spec = importlib.util.spec_from_file_location(
    "release_artifacts", Path(__file__).resolve().parents[1] / "scripts/check_release_artifacts.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def source_archive(tmp_path, *, include_patch=True, patch_bytes=b"reviewed gateway patch\n"):
    expected = b"reviewed gateway patch\n"
    files = {name: b"fixture\n" for name in release.SOURCE_REQUIRED | release.WHEEL_REQUIRED}
    files["deploy/protocol-gateway/upstream.lock.json"] = json.dumps({"patches": [{
        "file": "patches/0001.patch", "sha256": hashlib.sha256(expected).hexdigest()}]}).encode()
    if include_patch:
        files["deploy/protocol-gateway/patches/0001.patch"] = patch_bytes
    path = tmp_path / "source.tar.gz"
    with tarfile.open(path, "w:gz") as archive:
        for name, data in files.items():
            item = tarfile.TarInfo("wekeeper/" + name)
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
    return path


def test_source_requires_matching_locked_patches(tmp_path):
    release.check(source_archive(tmp_path))
    with pytest.raises(ValueError, match="missing or corrupt locked patch"):
        release.check(source_archive(tmp_path, include_patch=False))
    with pytest.raises(ValueError, match="missing or corrupt locked patch"):
        release.check(source_archive(tmp_path, patch_bytes=b"different patch"))


@pytest.mark.parametrize("unwanted", [
    "chatlog_keeper/dashboard/summaries.py",
    "chatlog_keeper/bot/worker.py",
    "chatlog_keeper/benchmark/source.py",
    "deploy/gateway.service.bak",
    "private/deploy/token",
])
def test_wheel_rejects_retired_or_private_files(tmp_path, unwanted):
    path = tmp_path / "release.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name in release.WHEEL_REQUIRED:
            archive.writestr(name, "fixture\n")
        archive.writestr("wekeeper.dist-info/entry_points.txt", "[console_scripts]\n")
        archive.writestr(unwanted, "must not ship\n")
    with pytest.raises(ValueError, match="forbidden"):
        release.check(path)
