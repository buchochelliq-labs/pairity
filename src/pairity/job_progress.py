"""Bounded Jobs metadata for UI/CLI progress; never exposes task inputs or output."""

TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def state_values(run):
    state = getattr(run, "state", None)
    lifecycle = getattr(getattr(state, "life_cycle_state", None), "value", "UNKNOWN")
    result = getattr(getattr(state, "result_state", None), "value", None)
    return lifecycle, result


def _task(task):
    lifecycle, result = state_values(task)
    if result:
        status = {"SUCCESS": "completed", "CANCELED": "cancelled"}.get(result, "failed")
    else:
        status = {
            "RUNNING": "running",
            "PENDING": "queued",
            "QUEUED": "queued",
            "BLOCKED": "queued",
            "WAITING_FOR_RETRY": "retrying",
            "TERMINATING": "cancelling",
            "SKIPPED": "skipped",
            "INTERNAL_ERROR": "error",
            "TERMINATED": "error",
        }.get(lifecycle, "unknown")
    return {
        "task_key": task.task_key,
        "run_id": getattr(task, "run_id", None),
        "status": status,
        "lifecycle": lifecycle,
        "result_state": result,
        "url": getattr(task, "run_page_url", None),
        "attempt": getattr(task, "attempt_number", None),
    }


def _counts(task):
    stats = getattr(
        getattr(getattr(task, "for_each_task", None), "stats", None), "task_run_stats", None
    )
    if stats is None:
        return None
    fields = {
        "total": "total_iterations",
        "completed": "completed_iterations",
        "active": "active_iterations",
        "failed": "failed_iterations",
        "succeeded": "succeeded_iterations",
        "scheduled": "scheduled_iterations",
    }
    values = {key: getattr(stats, name, None) for key, name in fields.items()}
    if all(value is None for value in values.values()):
        return None
    if any(
        value is not None and (type(value) is not int or value < 0) for value in values.values()
    ):
        raise ValueError("Invalid iteration counts")
    total = values["total"]
    if total is not None and any(value is not None and value > total for value in values.values()):
        raise ValueError("Inconsistent iteration counts")
    return values


def _pair_index(task):
    resolved = getattr(getattr(task, "resolved_values", None), "notebook_task", None)
    value = (getattr(resolved, "base_parameters", None) or {}).get("pair_index")
    # Index belongs to the submitted snapshot, not the position in the Jobs response.
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 9:
        return int(value)
    return None


def job_progress(client, run, job_id):
    tasks = list(getattr(run, "tasks", None) or [])[:100]
    lifecycle, result = state_values(run)
    done = lifecycle in TERMINAL or result is not None
    stage = "running"
    if done:
        stage = "finished"
    elif lifecycle == "TERMINATING":
        stage = "cancelling"
    elif lifecycle in {"PENDING", "QUEUED", "BLOCKED"}:
        stage = "queued"
    else:
        for key, label in [
            ("aggregate", "saving-results"),
            ("comparisons", "comparing"),
            ("prepare", "preparing"),
        ]:
            selected = next((t for t in tasks if t.task_key == key), None)
            if selected and state_values(selected)[0] in {
                "RUNNING",
                "TERMINATED",
                "WAITING_FOR_RETRY",
            }:
                stage = label
                break
    progress = {
        "stage": stage,
        "tasks": [_task(task) for task in tasks],
        "counts": None,
        "attempts": [],
        "details_complete": not bool(getattr(run, "next_page_token", None)),
        "warnings": [],
    }
    if not progress["details_complete"]:
        progress["warnings"].append("Additional Job tasks are available in Databricks.")
    comparison = next((t for t in tasks if t.task_key == "comparisons"), None)
    if comparison is None:
        return progress
    try:
        progress["counts"] = _counts(comparison)
    except ValueError:
        progress["warnings"].append(
            "Comparison counts are temporarily inconsistent; waiting for an update."
        )
    comparison_run = getattr(comparison, "run_id", None)
    if not comparison_run:
        return progress
    page_token, seen_tokens, seen_runs = None, set(), set()
    try:
        for _ in range(3):
            kwargs = {"run_id": comparison_run, "include_resolved_values": True}
            if page_token:
                kwargs["page_token"] = page_token
            page = client.jobs.get_run(**kwargs)
            if (
                page.job_id != job_id
                or page.run_id != comparison_run
                or (getattr(page, "job_run_id", None) not in {None, getattr(run, "run_id", None)})
            ):
                raise ValueError("Unexpected task identity")
            iterations = list(page.iterations or [])
            for task in iterations[:100]:
                if task.run_id is None or task.run_id in seen_runs:
                    continue
                seen_runs.add(task.run_id)
                progress["attempts"].append(_task(task) | {"pair_index": _pair_index(task)})
            page_token = page.next_page_token
            if len(iterations) > 100:
                raise ValueError("Unexpected page size")
            if not page_token:
                return progress
            if page_token in seen_tokens:
                raise ValueError("Repeated page cursor")
            seen_tokens.add(page_token)
        progress["warnings"].append(
            "Showing at most 300 comparison attempts; open Databricks for all attempts."
        )
    except Exception:
        # Raw SDK/task messages may contain source values, SQL or credentials.
        progress["warnings"].append(
            "Some comparison details are unavailable; overall Job status is still current."
        )
    progress["details_complete"] = False
    return progress
