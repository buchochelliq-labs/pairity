"""Immutable evidence bundles in UC Volumes, with a local development adapter."""

import hashlib
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_BUNDLE_BYTES = 128 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _name(name):
    if (
        not isinstance(name, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", name)
        or name == "manifest.json"
    ):
        raise ValueError("Artifact names must be portable filenames; manifest.json is reserved")


@dataclass(frozen=True)
class ArtifactBundle:
    bundle_id: str
    uri: str
    sha256: str


class BundleStore:
    api_version = 1

    def publish(self, run_id, files):
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id is required")
        if not files or len(files) > 128:
            raise ValueError("Provide between 1 and 128 artifact files")
        for name, data in files.items():
            _name(name)
            if not isinstance(data, bytes) or len(data) > MAX_FILE_BYTES:
                raise ValueError("Each artifact must be bytes of at most 32 MiB")
        if sum(map(len, files.values())) > MAX_BUNDLE_BYTES:
            raise ValueError("Artifact bundle exceeds 128 MiB")
        bundle_id = uuid4().hex
        manifest = {
            "artifact_schema_version": 1,
            "bundle_id": bundle_id,
            "run_id": run_id,
            "files": {
                name: {"size": len(data), "sha256": _digest(data)}
                for name, data in sorted(files.items())
            },
        }
        for name, data in files.items():
            self._write(bundle_id, name, data)
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        # No atomic rename assumption: only a complete manifest makes a bundle published.
        self._write(bundle_id, "manifest.json", encoded)
        ref = ArtifactBundle(bundle_id, self._path(bundle_id, "manifest.json"), _digest(encoded))
        self.download(ref)  # Verify publication before returning a usable reference.
        return ref

    def download(self, ref):
        if not re.fullmatch(r"[0-9a-f]{32}", ref.bundle_id) or ref.uri != self._path(
            ref.bundle_id, "manifest.json"
        ):
            raise ValueError("Artifact reference is outside this store")
        encoded = self._read(ref.bundle_id, "manifest.json", MAX_MANIFEST_BYTES)
        if _digest(encoded) != ref.sha256:
            raise ValueError("Artifact manifest checksum mismatch")
        manifest = json.loads(encoded)
        if (
            manifest.get("artifact_schema_version") != 1
            or manifest.get("bundle_id") != ref.bundle_id
        ):
            raise ValueError("Invalid artifact manifest")
        entries = manifest.get("files")
        if not isinstance(entries, dict) or not 1 <= len(entries) <= 128:
            raise ValueError("Invalid artifact file list")
        result = {}
        total = 0
        for name, entry in entries.items():
            _name(name)
            size = entry.get("size")
            if type(size) is not int or not 0 <= size <= MAX_FILE_BYTES:
                raise ValueError("Invalid artifact size")
            total += size
            if total > MAX_BUNDLE_BYTES:
                raise ValueError("Artifact bundle exceeds 128 MiB")
            data = self._read(ref.bundle_id, name, size)
            if len(data) != size or _digest(data) != entry.get("sha256"):
                raise ValueError("Artifact size or checksum mismatch")
            result[name] = data
        return result


class LocalArtifactStore(BundleStore):
    """Local development only; hosted deployments use VolumeArtifactStore."""

    def __init__(self, root):
        self.root = Path(root).resolve()

    def _path(self, bundle_id, name):
        return str(self.root / bundle_id / name)

    def _write(self, bundle_id, name, data):
        path = Path(self._path(bundle_id, name))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(data)

    def _read(self, bundle_id, name, limit):
        with Path(self._path(bundle_id, name)).open("rb") as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError("Artifact size exceeds manifest or configured limit")
        return data


class VolumeArtifactStore(BundleStore):
    """UC Files API adapter; create separate instances to use multiple volumes."""

    def __init__(self, root, workspace_client):
        if not isinstance(root, str):
            raise ValueError("A UC Volume root is required")
        parts = root.rstrip("/").split("/")
        if (
            len(parts) < 5
            or parts[:2] != ["", "Volumes"]
            or any(not p or p in {".", ".."} or "\\" in p or "\x00" in p for p in parts[2:])
        ):
            raise ValueError("Use /Volumes/catalog/schema/volume[/prefix]")
        self.root = root.rstrip("/")
        self.files = workspace_client.files

    def _path(self, bundle_id, name):
        return f"{self.root}/{bundle_id}/{name}"

    def _write(self, bundle_id, name, data):
        self.files.create_directory(f"{self.root}/{bundle_id}")
        self.files.upload(self._path(bundle_id, name), io.BytesIO(data), overwrite=False)

    def _read(self, bundle_id, name, limit):
        response = self.files.download(self._path(bundle_id, name))
        if response.contents is None:
            raise OSError("Artifact download returned no content stream")
        with response.contents as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError("Artifact size exceeds manifest or configured limit")
        return data
