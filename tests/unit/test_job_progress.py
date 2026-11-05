import json
from types import SimpleNamespace

import pytest
from databricks.sdk.service.jobs import Run

from pairity.submission import DatabricksJobSubmitter, RunHandle


def state(lifecycle="RUNNING", result=None):
    return {
        "life_cycle_state": lifecycle,
        "result_state": result,
        "state_message": "private execution output",
    }


def task(key, lifecycle="RUNNING", result=None, **extra):
    return {"task_key": key, "run_id": 20, "state": state(lifecycle, result), **extra}


def parent(**extra):
    return {"job_id": 7, "run_id": 42, "start_time": 1000, "state": state(), **extra}


def iteration(run_id, index, lifecycle="RUNNING", result=None, attempt=0):
    return task(
        "compare_pair",
        lifecycle,
        result,
        run_id=run_id,
        attempt_number=attempt,
        resolved_values={
            "notebook_task": {
                "base_parameters": {"pair_index": str(index), "manifest": "private config"}
            }
        },
    )


class Jobs:
    def __init__(self, root, pages=()):
        self.root, self.pages, self.calls = root, iter(pages), []

    def get_run(self, **kwargs):
        self.calls.append(kwargs)
        value = self.root if kwargs["run_id"] == 42 else next(self.pages)
        if isinstance(value, Exception):
            raise value
        return Run.from_dict(value)


def status(root, pages=()):
    jobs = Jobs(root, pages)
    result = DatabricksJobSubmitter(SimpleNamespace(jobs=jobs), 7, None).status(
        RunHandle("databricks-jobs", "42")
    )
    return result, jobs.calls


def comparisons(**stats):
    return task(
        "comparisons",
        for_each_task={"inputs": "private input", "task": {}, "stats": {"task_run_stats": stats}},
    )


def test_progress_uses_latest_retry_counts_and_preserves_attempts():
    root = parent(
        tasks=[
            task("prepare", "TERMINATED", "SUCCESS"),
            comparisons(
                total_iterations=4,
                completed_iterations=2,
                succeeded_iterations=1,
                failed_iterations=1,
                active_iterations=2,
                scheduled_iterations=4,
            ),
        ]
    )
    child = {
        "job_id": 7,
        "job_run_id": 42,
        "run_id": 20,
        "iterations": [
            iteration(100, 0, "TERMINATED", "FAILED"),
            iteration(101, 0, "TERMINATED", "SUCCESS", attempt=1),
            iteration(102, 1, "TERMINATED", "FAILED"),
            iteration(103, 2),
            iteration(104, 3),
        ],
    }
    result, calls = status(root, [child])
    progress = result["progress"]
    assert progress["stage"] == "comparing"
    assert progress["counts"]["completed"] == 2  # not three completed attempts
    assert progress["counts"]["total"] == 4
    assert len(progress["attempts"]) == 5
    assert progress["attempts"][1]["pair_index"] == 0
    assert progress["attempts"][1]["status"] == "completed"
    assert result["result_run_id"] == "job:42"
    assert result["start_time"] == 1000
    assert "private" not in json.dumps(result)
    assert calls[1] == {"run_id": 20, "include_resolved_values": True}


def test_iteration_pages_are_followed_and_unknown_indexes_stay_unknown():
    first = {
        "job_id": 7,
        "run_id": 20,
        "iterations": [iteration(100, 3)],
        "next_page_token": "next",
    }
    second = {"job_id": 7, "run_id": 20, "iterations": [task("compare_pair", run_id=101)]}
    result, calls = status(parent(tasks=[comparisons()]), [first, second])
    assert result["progress"]["attempts"][1]["pair_index"] is None
    assert calls[2]["page_token"] == "next"
    assert result["progress"]["counts"] is None


def test_progress_requests_are_bounded_and_truncation_is_explicit():
    pages = [
        {"job_id": 7, "run_id": 20, "iterations": [], "next_page_token": str(i)} for i in range(3)
    ]
    result, calls = status(parent(tasks=[comparisons(total_iterations=500)]), pages)
    assert len(calls) == 4
    assert not result["progress"]["details_complete"]
    assert result["progress"]["warnings"]
    assert result["progress"]["counts"]["total"] == 500


@pytest.mark.parametrize("child", [PermissionError("secret"), {"job_id": 99, "run_id": 20}])
def test_child_failure_does_not_hide_parent_state_or_leak_data(child):
    result, _ = status(parent(tasks=[comparisons()]), [child])
    assert result["status"] == "running"
    assert not result["progress"]["details_complete"]
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize(
    "root,stage",
    [
        (parent(state=state("QUEUED")), "queued"),
        (parent(tasks=[task("prepare")]), "preparing"),
        (parent(tasks=[task("aggregate")]), "saving-results"),
        (parent(state=state("TERMINATING")), "cancelling"),
        (parent(state=state("TERMINATED", "FAILED")), "finished"),
    ],
)
def test_stages_follow_actual_job_state(root, stage):
    result, _ = status(root)
    assert result["progress"]["stage"] == stage
    assert result["done"] is (stage == "finished")


def test_inconsistent_counts_do_not_generate_progress():
    result, _ = status(
        parent(tasks=[comparisons(total_iterations=2, completed_iterations=3)]),
        [{"job_id": 7, "run_id": 20, "iterations": []}],
    )
    assert result["progress"]["counts"] is None
    assert result["progress"]["warnings"]


def test_parent_job_authorization_boundary_is_preserved():
    with pytest.raises(ValueError, match="different Job"):
        status(parent(job_id=8))
