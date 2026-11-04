"""Bounded status monitoring shared by terminal and other clients."""

import time


def watch_run(
    submitter,
    handle,
    *,
    timeout=1200,
    interval=5,
    emit=lambda state: None,
    sleep=time.sleep,
    clock=time.monotonic,
):
    if timeout <= 0 or interval <= 0:
        raise ValueError("Timeout and poll interval must be positive")
    deadline = clock() + timeout
    previous = None
    while clock() < deadline:
        state = submitter.status(handle)
        if state != previous:
            emit(state)
            previous = state.copy()
        if state["status"] in {"passed", "failed", "error", "cancelled", "blocked", "skipped"}:
            return state
        sleep(min(interval, max(0, deadline - clock())))
    raise TimeoutError(
        "Status wait expired; the Job was not cancelled. Use job-status or job-cancel."
    )
