from types import SimpleNamespace

import pytest

from pairity.config_storage import RevisionConflict
from pairity.serialization import dump_config


def suite():
    from pairity.config import SuiteConfig

    return SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "daily",
            "pairs": [
                {
                    "id": "one",
                    "baseline": "a.b.left",
                    "candidate": "a.b.right",
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
            ],
        }
    )


def test_revision_preview_selection_and_submission(tmp_path):
    from pairity.workbench import FileSuiteCatalog, Workbench

    submitted = []
    submitter = SimpleNamespace(
        submit=lambda c, request_id: (
            submitted.append(c)
            or __import__("pairity.submission", fromlist=["RunHandle"]).RunHandle("test", "42")
        )
    )
    service = Workbench(FileSuiteCatalog(tmp_path), submitter=submitter)
    config = suite()
    saved = service.save("daily", dump_config(config, "yaml"), "yaml", 0)
    assert saved["revision"] == 1
    assert service.list_suites()[0]["suite"] == "daily"
    preview = service.preview("daily", dump_config(config, "json"), "json", 1)
    assert preview["preview"]["unchanged"] == ["one"]
    with pytest.raises(RevisionConflict):
        service.save("daily", dump_config(config, "json"), "json", 0)
    service.submit("daily", revision=1, request_id="request-1", pair_ids=["one"])
    assert submitted[0].pairs[0].id == "one"
    assert service.get("daily")["revision"] == 1


def test_missing_keys_block_launch_but_allow_editing(tmp_path):
    from pairity.workbench import FileSuiteCatalog, Workbench

    service = Workbench(FileSuiteCatalog(tmp_path))
    data = suite().model_dump(exclude_none=True)
    del data["pairs"][0]["keys"]
    import json

    text = json.dumps(data)
    assert service.validate(text, "json")["effective"][0]["needs_keys"]
    service.save("daily", text, "json", 0)
    with pytest.raises(ValueError, match="keys"):
        service.submit("daily", revision=1, request_id="request-1")


def test_submission_returns_snapshot_pair_indexes_for_progress(tmp_path):
    from pairity.submission import RunHandle
    from pairity.workbench import FileSuiteCatalog, Workbench

    service = Workbench(
        FileSuiteCatalog(tmp_path),
        submitter=SimpleNamespace(
            submit=lambda config, request_id: RunHandle("databricks-jobs", "42")
        ),
    )
    config = suite()
    config.pairs.append(config.pairs[0].model_copy(update={"id": "two", "enabled": False}))
    config.pairs.append(config.pairs[0].model_copy(update={"id": "three"}))
    service.save("daily", dump_config(config, "json"), "json", 0)
    response = service.submit("daily", revision=1, request_id="r", pair_ids=["three", "two"])
    assert response["pairs"] == [
        {"pair_index": 0, "pair_id": "two", "enabled": False},
        {"pair_index": 1, "pair_id": "three", "enabled": True},
    ]
    assert response["suite"] == "daily" and response["revision"] == 1
    assert response["result_run_id"] == "job:42"


def test_catalog_paths_confined_and_bulk_requires_selection(tmp_path):
    from pairity.workbench import FileSuiteCatalog, Workbench

    service = Workbench(FileSuiteCatalog(tmp_path))
    config = suite().model_copy(update={"suite": "../outside"})
    service.save(config.suite, dump_config(config, "json"), "json", 0)
    assert len(list(tmp_path.iterdir())) == 1
    assert service.get(config.suite)["config"]["suite"] == "../outside"
    with pytest.raises(ValueError, match="selection"):
        service.patch(config.suite, {"enabled": False}, expected_revision=1)
