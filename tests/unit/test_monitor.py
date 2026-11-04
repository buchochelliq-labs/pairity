from types import SimpleNamespace

import pytest


def test_watch_emits_transitions_and_keeps_terminal_failure():
    from pairity.monitor import watch_run

    states = iter([{"status": "running"}, {"status": "running"}, {"status": "failed"}])
    seen = []
    result = watch_run(
        SimpleNamespace(status=lambda _: next(states)),
        None,
        timeout=10,
        interval=1,
        sleep=lambda _: None,
        emit=seen.append,
    )
    assert result["status"] == "failed"
    assert len(seen) == 2


def test_watch_timeout_does_not_cancel_job():
    from pairity.monitor import watch_run

    clock = iter([0, 0, 2, 2])
    with pytest.raises(TimeoutError):
        watch_run(
            SimpleNamespace(status=lambda _: {"status": "running"}),
            None,
            timeout=1,
            interval=1,
            sleep=lambda _: None,
            clock=lambda: next(clock),
        )
