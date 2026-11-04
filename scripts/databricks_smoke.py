"""Run deployed smoke Jobs and require both Job states and verified Delta outcomes."""

import argparse
import json
from datetime import timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed


def run(client, job_id, parameters, expected, records):
    waiter = client.jobs.run_now(job_id=job_id, job_parameters=parameters)
    run_id = waiter.run_id
    records.append({"job_id": job_id, "run_id": run_id, "expected": expected})
    try:
        waiter.result(timeout=timedelta(minutes=20))
    except TimeoutError:
        client.jobs.cancel_run(run_id=run_id)
        raise
    except OperationFailed:
        # Regression failure is expected only if the final verifier certifies its evidence.
        pass
    result = client.jobs.get_run(run_id=run_id)
    state = result.state.result_state.value if result.state and result.state.result_state else None
    records[-1].update(result=state, url=result.run_page_url)
    if state != expected:
        raise RuntimeError(f"Job {job_id} ended {state}; expected {expected}")
    return run_id


def run_metadata(run):
    """Export timing/identity only, excluding parameters, source paths and notebook output."""
    raw = run.as_dict()
    fields = ("run_id", "task_key", "start_time", "end_time", "attempt_number")

    def task_metadata(task):
        item = {key: task[key] for key in fields if key in task}
        item["result_state"] = task.get("state", {}).get("result_state")
        return item

    return {
        **task_metadata(raw),
        "tasks": [task_metadata(t) for t in raw.get("tasks", [])],
        "iterations": [task_metadata(t) for t in raw.get("iterations", [])],
        "iteration_count": len(raw.get("iterations", [])),
    }


def inspect_runs(client, run_ids, output):
    """Read existing runs without deploying or executing Jobs."""
    reports = []
    try:
        for run_id in run_ids:
            report = {"requested_run_id": run_id, "foreach": []}
            reports.append(report)
            parent = client.jobs.get_run(run_id=run_id)
            report["parent"] = run_metadata(parent)
            for task in parent.tasks or []:
                if task.task_key != "comparisons" or task.run_id is None:
                    continue
                child = client.jobs.get_run(run_id=task.run_id)
                report["foreach"].append(
                    {
                        "requested_run_id": task.run_id,
                        **run_metadata(child),
                    }
                )
    finally:
        content = json.dumps(reports, indent=2) + "\n"
        (output / "run-inspection.json").write_text(content)
        print(content)


def overlap(client, run_id):
    parent = client.jobs.get_run(run_id=run_id)
    tasks = [t for t in (parent.tasks or []) if t.task_key == "comparisons"]
    if len(tasks) != 1 or tasks[0].run_id is None:
        raise ValueError("Missing For each task run")
    iterations = client.jobs.get_run(run_id=tasks[0].run_id).iterations or []
    # Jobs returns every attempt, including retries, for each logical iteration.
    if any(t.attempt_number is None or t.attempt_number < 0 for t in iterations):
        raise ValueError("Missing or invalid iteration attempt number")
    initial = [t for t in iterations if t.attempt_number == 0]
    if len(initial) != 4:
        raise ValueError(
            f"Expected four pair iterations for Job run {run_id}, "
            f"For each run {tasks[0].run_id}; API returned {len(initial)} initial "
            f"attempts across {len(iterations)} total attempts"
        )
    events = []
    for task in iterations:
        if not task.start_time or not task.end_time or task.end_time <= task.start_time:
            raise ValueError("Incomplete pair task timing")
        events.extend([(task.start_time, 1), (task.end_time, -1)])
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    if peak != 2:
        raise ValueError(f"Expected peak pair-task overlap 2; observed {peak}")
    return {
        "run_id": run_id,
        "peak_pair_tasks": peak,
        "pair_iterations": len(initial),
        "attempt_count": len(iterations),
        "iterations": [
            {
                "run_id": t.run_id,
                "attempt_number": t.attempt_number,
                "start": t.start_time,
                "end": t.end_time,
            }
            for t in iterations
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary")
    parser.add_argument("--inspect-runs", help="Comma-separated existing run IDs; read-only")
    parser.add_argument("--test-id")
    parser.add_argument("--artifact-root")
    parser.add_argument("--output", default="smoke-output")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    if args.inspect_runs is not None:
        values = args.inspect_runs.split(",")
        if not 1 <= len(values) <= 10 or any(
            not v.strip().isascii() or not v.strip().isdigit() or int(v) <= 0 for v in values
        ):
            parser.error("--inspect-runs requires 1..10 positive run IDs separated by commas")
        inspect_runs(WorkspaceClient(), [int(v) for v in values], output)
        return
    if not all((args.summary, args.test_id, args.artifact_root)):
        parser.error("--summary, --test-id and --artifact-root are required for smoke execution")
    jobs = json.loads(Path(args.summary).read_text())["resources"]["jobs"]
    client = WorkspaceClient()
    records = []
    directory = args.artifact_root.rstrip("/") + "/smoke/" + args.test_id
    try:
        run(client, int(jobs["smoke_setup"]["id"]), {"test_id": args.test_id}, "SUCCESS", records)
        ids = {}
        for variant in ("identical", "regression"):
            ids[variant] = run(
                client,
                int(jobs["regression"]["id"]),
                {"config_path": directory + "/tuva-" + variant + ".yaml"},
                "SUCCESS" if variant == "identical" else "FAILED",
                records,
            )
        timings = [overlap(client, run_id) for run_id in ids.values()]
        (output / "parallel-task-timing.json").write_text(json.dumps(timings, indent=2))
        run(
            client,
            int(jobs["smoke_verify"]["id"]),
            {"test_id": args.test_id, **{k + "_run_id": str(v) for k, v in ids.items()}},
            "SUCCESS",
            records,
        )
        response = client.files.download(directory + "/verified-results.json")
        if response.contents is None:
            raise RuntimeError("No verified report artifact")
        with response.contents as stream:
            data = stream.read(2 * 1024 * 1024 + 1)
        if len(data) > 2 * 1024 * 1024:
            raise ValueError("Smoke report unexpectedly exceeds 2 MiB")
        (output / "verified-results.json").write_bytes(data)
    finally:
        (output / "job-runs.json").write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    main()
