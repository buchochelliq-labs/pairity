import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from pairity.config import SuiteConfig
from pairity.delta_storage import DeltaResultStore
from pairity.results import PairResult, RunResult

pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(
        not os.environ.get("PAIRITY_TEST_DELTA"), reason="Set PAIRITY_TEST_DELTA=1 for Delta"
    ),
]

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)


def run(suite, run_id, days_ago, changed=0):
    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": suite,
            "pairs": [
                {
                    "id": "orders",
                    "baseline": "prod.s.orders",
                    "candidate": "dev.s.orders",
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
            ],
        }
    )
    finished = (NOW - timedelta(days=days_ago)).isoformat()
    pair = PairResult(
        "orders",
        "failed" if changed else "passed",
        counts={
            "baseline_rows": 10,
            "candidate_rows": 10,
            "matched_rows": 10,
            "changed_rows": changed,
            "missing_rows": 0,
            "extra_rows": 0,
        },
        column_changes={"amount": changed},
    )
    return RunResult(
        run_id,
        suite,
        config.fingerprint,
        config.model_dump(exclude_none=True),
        [pair],
        finished,
        finished,
    )


@pytest.fixture
def store(spark):
    name = f"spark_catalog.default.pairity_results_{uuid4().hex[:8]}"
    store = DeltaResultStore(spark, name)
    store.initialize()
    for suite, run_id, days, changed in [
        ("sales", "s-old-1", 200, 1),
        ("sales", "s-old-2", 120, 0),
        ("sales", "s-new", 5, 2),
        ("hr", "h-old", 300, 0),
    ]:
        store.write(run(suite, run_id, days, changed))
    yield store
    for view in ("_runs", "_pairs"):
        spark.sql(f"DROP VIEW IF EXISTS {store.table[:-1]}{view}`")
    spark.sql(f"DROP TABLE IF EXISTS {store.table}")


def run_ids(store):
    return sorted(
        r.run_id for r in store.spark.table(store.table).where("record_type = 'report'").collect()
    )


def test_plan_previews_old_runs_and_keeps_newest_per_suite(store):
    plan = store.retention_plan(older_than_days=90, now=NOW)
    # h-old is the only (newest) hr run, so the default keep_latest_per_suite=1 keeps it.
    assert [r["run_id"] for r in plan["runs"]] == ["s-old-1", "s-old-2"]
    assert plan["truncated"] is False
    assert run_ids(store) == ["h-old", "s-new", "s-old-1", "s-old-2"]  # preview deletes nothing
    assert [
        r["run_id"]
        for r in store.retention_plan(older_than_days=90, keep_latest_per_suite=0, now=NOW)["runs"]
    ] == ["h-old", "s-old-1", "s-old-2"]
    assert [
        r["run_id"]
        for r in store.retention_plan(older_than_days=150, suite="sales", now=NOW)["runs"]
    ] == ["s-old-1"]
    capped = store.retention_plan(older_than_days=90, keep_latest_per_suite=0, max_runs=2, now=NOW)
    assert len(capped["runs"]) == 2 and capped["truncated"] is True


def test_apply_removes_whole_runs_and_rechecks_eligibility(store):
    plan = store.retention_plan(older_than_days=90, now=NOW)
    # A later run in the same suite does not protect older runs; deleting the newest one
    # concurrently would make s-old-2 the newest, so it must then be kept.
    store.spark.sql(f"DELETE FROM {store.table} WHERE run_id = 's-new'")
    outcome = store.apply_retention(plan)
    assert outcome == {"deleted": ["s-old-1"], "kept": ["s-old-2"]}
    assert run_ids(store) == ["h-old", "s-old-2"]
    with pytest.raises(FileNotFoundError):
        store.load("s-old-1")
    assert store.apply_retention(plan) == {"deleted": [], "kept": ["s-old-1", "s-old-2"]}
    other = DeltaResultStore(store.spark, store.name + "_other")
    with pytest.raises(ValueError, match="different result table"):
        other.apply_retention(plan)


def test_views_decode_run_history_and_pair_outcomes(store):
    views = store.create_views()
    runs = {
        r.run_id: r
        for r in store.spark.sql(
            f"SELECT * FROM {views['runs_view']} ORDER BY finished_at"
        ).collect()
    }
    assert set(runs) == {"h-old", "s-new", "s-old-1", "s-old-2"}
    assert runs["s-new"].status == "failed" and runs["s-new"].pair_count == 1
    assert runs["s-old-2"].differences_retained is False
    assert runs["s-new"].finished_at.replace(tzinfo=None) is not None
    trend = store.spark.sql(
        f"""SELECT run_id, changed_rows, column_changes['amount'] AS amount_changes
FROM {views["pairs_view"]} WHERE suite = 'sales' AND pair_id = 'orders'
ORDER BY finished_at"""
    ).collect()
    assert [(r.run_id, r.changed_rows, r.amount_changes) for r in trend] == [
        ("s-old-1", 1, 1),
        ("s-old-2", 0, 0),
        ("s-new", 2, 2),
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"older_than_days": 0},
        {"older_than_days": 1.5},
        {"older_than_days": 30, "keep_latest_per_suite": -1},
        {"older_than_days": 30, "max_runs": 0},
        {"older_than_days": 30, "now": datetime(2026, 1, 1)},
    ],
)
def test_retention_arguments_are_validated(spark, kwargs):
    # Validation happens before any table access.
    store = DeltaResultStore(spark, "spark_catalog.default.pairity_unused")
    with pytest.raises(ValueError):
        store.retention_plan(**kwargs)
