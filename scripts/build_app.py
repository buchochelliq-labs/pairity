"""Assemble the app from the same core source and freshly built frontend."""

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build():
    ui = ROOT / "app/ui/dist"
    if not (ui / "index.html").is_file():
        raise ValueError("Build the UI first: cd app/ui && npm ci && npm run build")
    static = ROOT / "src/pairity/web/static"
    if static.exists():
        shutil.rmtree(static)
    shutil.copytree(ui, static)
    target = ROOT / "app/build"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir()
    shutil.copytree(
        ROOT / "src/pairity",
        target / "pairity",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copyfile(ROOT / "app/requirements.txt", target / "requirements.txt")
    print("App assembled in app/build; web assets included in core wheel build")


if __name__ == "__main__":
    build()
