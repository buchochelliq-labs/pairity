import io
import json
from types import SimpleNamespace

import pytest


def test_local_bundle_checksums_completion_and_path_safety(tmp_path):
    from pairity.artifacts import LocalArtifactStore

    store = LocalArtifactStore(tmp_path)
    ref = store.publish("run-1", {"counts.json": b'{"changed":2}', "reproduce.sql": b"SELECT 1;"})
    assert store.download(ref)["reproduce.sql"] == b"SELECT 1;"
    manifest = json.loads((tmp_path / ref.bundle_id / "manifest.json").read_text())
    assert manifest["run_id"] == "run-1"
    (tmp_path / ref.bundle_id / "counts.json").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum|size"):
        store.download(ref)
    with pytest.raises(ValueError):
        store.publish("run-1", {"../bad.sql": b"x"})


class Files:
    def __init__(self):
        self.data = {}
        self.fail = False

    def create_directory(self, directory_path):
        pass

    def upload(self, file_path, contents, *, overwrite):
        assert overwrite is False
        if self.fail and file_path.endswith("manifest.json"):
            raise PermissionError("denied")
        if file_path in self.data:
            raise FileExistsError(file_path)
        self.data[file_path] = contents.read()

    def download(self, file_path):
        return SimpleNamespace(contents=io.BytesIO(self.data[file_path]))


def test_volume_publication_uses_manifest_last_and_verified_download():
    from pairity.artifacts import VolumeArtifactStore

    files = Files()
    store = VolumeArtifactStore("/Volumes/catalog/schema/evidence", SimpleNamespace(files=files))
    ref = store.publish("run", {"summary.png": b"image", "steps.md": b"steps"})
    assert list(files.data)[-1].endswith("manifest.json")
    assert ref.uri.startswith("/Volumes/catalog/schema/evidence/")
    assert store.download(ref)["steps.md"] == b"steps"
    files.fail = True
    with pytest.raises(PermissionError):
        store.publish("run", {"summary.png": b"image"})


def test_volume_rejects_non_volume_and_traversal_roots():
    from pairity.artifacts import VolumeArtifactStore

    for root in ("/tmp/artifacts", "/Volumes/c/s/v/../other", "/Volumes/c/s"):
        with pytest.raises(ValueError):
            VolumeArtifactStore(root, None)


def test_execution_registry_reuses_first_manifest_without_repinning():
    from databricks.sdk.errors import AlreadyExists, NotFound

    from pairity.artifacts import VolumeArtifactStore, VolumeExecutionRegistry

    class SDKFiles(Files):
        def download(self, file_path):
            if file_path not in self.data:
                raise NotFound(file_path)
            return super().download(file_path)

        def upload(self, file_path, contents, *, overwrite):
            if file_path in self.data:
                raise AlreadyExists(file_path)
            return super().upload(file_path, contents, overwrite=overwrite)

    files = SDKFiles()
    store = VolumeArtifactStore("/Volumes/c/s/v", SimpleNamespace(files=files))
    registry = VolumeExecutionRegistry(store)
    calls = []

    def publish():
        calls.append(1)
        return store.publish("job:1", {"execution.json": b'{"version":7}'})

    first = registry.get_or_create("job:1", publish)
    assert registry.get_or_create("job:1", publish) == first
    assert calls == [1]
