from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from pairity.bulk import patch_pairs, preview_update, upsert_pairs
from pairity.config import SuiteConfig
from pairity.config_storage import FileConfigStore, RevisionConflict


def suite():
    return SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "release",
            "pairs": [
                {"id": "a", "baseline": "prod.s.a", "candidate": "dev.s.a", "tags": ["sales"]},
                {"id": "b", "baseline": "prod.s.b", "candidate": "dev.s.b", "tags": ["sales"]},
            ],
        }
    )


def test_bulk_patch_changes_all_matches_without_mutating_original():
    before = suite()
    after = patch_pairs(before, {"compare": {"exclude_columns": ["audit"]}}, tags=["sales"])
    assert all(p.compare.exclude_columns == ["audit"] for p in after.pairs)
    assert all(p.compare.exclude_columns is None for p in before.pairs)
    assert preview_update(before, after) == {
        "added": [],
        "changed": ["a", "b"],
        "unchanged": [],
        "removed": [],
    }


@pytest.mark.parametrize(
    "patch,selector",
    [
        ({"typo": True}, {"ids": ["a"]}),
        ({"enabled": False}, {"ids": ["missing"]}),
        ({"enabled": False}, {"tags": ["typo"]}),
        ({"enabled": False}, {}),
        ({"id": "renamed"}, {"ids": ["a"]}),
    ],
)
def test_invalid_bulk_update_does_not_change_original(patch, selector):
    before = suite()
    original = before.model_dump()
    with pytest.raises(ValueError):
        patch_pairs(before, patch, **selector)
    assert before.model_dump() == original


def test_upsert_preserves_omitted_pairs_and_rejects_duplicate_input_ids():
    before = suite()
    incoming = [{"id": "a", "baseline": "prod.s.a", "candidate": "dev.s.new_a"}]
    after = upsert_pairs(before, incoming)
    assert [p.id for p in after.pairs] == ["a", "b"]
    assert after.pairs[0].candidate == "dev.s.new_a"
    with pytest.raises(ValueError, match="duplicate"):
        upsert_pairs(before, incoming * 2)


def test_revision_conflicts_and_historical_export(tmp_path):
    store = FileConfigStore(tmp_path)
    first = store.save(suite(), expected_revision=0)
    second_config = patch_pairs(first.config, {"enabled": False}, ids=["a"])
    second = store.save(second_config, expected_revision=1)
    with pytest.raises(RevisionConflict):
        store.save(suite(), expected_revision=1)
    assert second.revision == 2
    assert store.load().config.pairs[0].enabled is False
    assert store.load(revision=1).config.pairs[0].enabled is True


def test_concurrent_writers_cannot_both_commit_same_expected_revision(tmp_path):
    FileConfigStore(tmp_path).save(suite(), expected_revision=0)
    barrier = Barrier(2)

    def write():
        barrier.wait(timeout=5)
        try:
            return FileConfigStore(tmp_path).save(suite(), expected_revision=1).revision
        except RevisionConflict:
            return "conflict"

    with ThreadPoolExecutor(2) as pool:
        outcomes = list(pool.map(lambda _: write(), range(2)))
    assert sorted(map(str, outcomes)) == ["2", "conflict"]
    assert FileConfigStore(tmp_path).load().revision == 2
