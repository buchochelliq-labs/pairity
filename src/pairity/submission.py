"""Submission plugins keep orchestrators independent of the comparison engine."""

import re
from dataclasses import dataclass

from pairity.config import SuiteConfig


@dataclass(frozen=True)
class RunHandle:
    backend: str
    run_id: str


def _request_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", value):
        raise ValueError("request_id must be 1..64 portable characters")


class DirectSubmitter:
    """Synchronous Python/notebook submission through any configured engine.

    Handles and results are process-local. Durable history belongs to the engine sink.
    """

    api_version = 1

    def __init__(self, engine):
        self.engine = engine
        self._results = {}
        self._configs = {}

    def submit(self, config: SuiteConfig, *, request_id: str):
        _request_id(request_id)
        if request_id in self._configs and self._configs[request_id] != config.fingerprint:
            raise ValueError("Request ID reused with a different configuration")
        if request_id not in self._results:
            self._results[request_id] = self.engine.run(config, run_id=request_id)
            self._configs[request_id] = config.fingerprint
        return RunHandle("direct", request_id)

    def result(self, handle):
        if handle.backend != "direct":
            raise ValueError("Handle belongs to another submission backend")
        return self._results[handle.run_id]

    def status(self, handle):
        return {"status": self.result(handle).status, "run_id": handle.run_id}

    def cancel(self, handle):
        self.result(handle)
        raise ValueError("Synchronous execution has already completed")


class SubmissionStepFailed(ValueError):
    """A named submission step failed; the message says whether a run may exist."""


def _detail(exc):
    text = " ".join(str(exc).split())
    return f"{type(exc).__name__}: {text[:400]}" if text else type(exc).__name__


def _step(message, call):
    try:
        return call()
    except SubmissionStepFailed:
        raise
    except Exception as exc:
        raise SubmissionStepFailed(f"{message} Details: {_detail(exc)}") from exc


class DatabricksJobSubmitter:
    """Submit an existing deployed Job through an injected SDK client.

    config_publisher must publish an immutable JSON/YAML config and return its path.
    External schedulers can use this adapter without hosting Spark themselves.
    """

    api_version = 1

    def __init__(self, client, job_id, config_publisher):
        if type(job_id) is not int or job_id <= 0:
            raise ValueError("job_id must be a positive integer")
        self.client, self.job_id, self.config_publisher = client, job_id, config_publisher

    def submit(self, config: SuiteConfig, *, request_id: str):
        _request_id(request_id)
        config = SuiteConfig.model_validate(config.model_dump(exclude_none=True))
        path = _step(
            "Publishing the suite snapshot to the artifact Volume failed. The submitting "
            "identity needs USE CATALOG, USE SCHEMA, READ VOLUME and WRITE VOLUME on the "
            "artifact Volume. No Job was started.",
            lambda: self.config_publisher(config),
        )
        response = _step(
            f"Starting Job {self.job_id} failed. The submitting identity needs CAN_MANAGE_RUN "
            "on the Job. No run was confirmed, but one may still have started if only the "
            "response was lost: check Databricks Jobs before submitting again.",
            lambda: self.client.jobs.run_now(
                job_id=self.job_id,
                idempotency_token=request_id,
                job_parameters={"config_path": path, "config_hash": config.fingerprint},
            ),
        )
        if response.run_id is None:
            raise ValueError("Jobs API returned no run ID")
        handle = RunHandle("databricks-jobs", str(response.run_id))
        run = _step(
            f"Job run {response.run_id} was started, but reading it back failed. Check the "
            f"run in Databricks Jobs before submitting again.",
            lambda: self._run(handle),
        )
        parameters = {p.name: p.value for p in (run.job_parameters or [])}
        if parameters.get("config_hash") != config.fingerprint:
            raise ValueError("Submission token belongs to a different configuration")
        return handle

    def _id(self, handle):
        if handle.backend != "databricks-jobs" or not handle.run_id.isdigit():
            raise ValueError("Invalid Databricks Jobs handle")
        return int(handle.run_id)

    def _run(self, handle):
        run = self.client.jobs.get_run(run_id=self._id(handle))
        if run.job_id != self.job_id:
            raise ValueError("Run belongs to a different Job")
        return run

    def status(self, handle):
        from pairity.job_progress import TERMINAL, job_progress, state_values

        run = self._run(handle)
        lifecycle, result = state_values(run)
        status = {"SUCCESS": "passed", "CANCELED": "cancelled"}.get(result)
        if status is None:
            status = (
                "failed"
                if result
                else (
                    "error"
                    if lifecycle in {"INTERNAL_ERROR", "SKIPPED", "TERMINATED"}
                    else "running"
                )
            )
        return {
            "status": status,
            "run_id": handle.run_id,
            "lifecycle": lifecycle,
            "result_state": result,
            "url": run.run_page_url,
            "done": lifecycle in TERMINAL or result is not None,
            "start_time": getattr(run, "start_time", None),
            "end_time": getattr(run, "end_time", None),
            "result_run_id": f"job:{handle.run_id}",
            "progress": job_progress(self.client, run, self.job_id),
        }

    def cancel(self, handle):
        self._run(handle)
        self.client.jobs.cancel_run(run_id=self._id(handle))


class VolumeConfigPublisher:
    """Reuse the verified artifact store to publish immutable suite snapshots."""

    def __init__(self, artifact_store):
        self.store = artifact_store

    def __call__(self, config):
        from pairity.serialization import dump_config

        ref = self.store.publish(
            config.fingerprint, {"suite.json": dump_config(config, "json").encode()}
        )
        return ref.uri.rsplit("/", 1)[0] + "/suite.json"
