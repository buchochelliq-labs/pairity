from io import StringIO

from rs_rich.console import Console
from rs_rich.tui import run

from pairity.terminal import create_report_app, render_report


def report():
    return {
        "result_schema_version": 1,
        "suite": "release",
        "status": "blocked",
        "pairs": [
            {
                "pair_id": "[bold]orders",
                "status": "passed",
                "reason": None,
                "counts": {
                    "matched_rows": 12,
                    "changed_rows": 0,
                    "missing_rows": 0,
                    "extra_rows": 0,
                },
            },
            {
                "pair_id": "lines",
                "status": "blocked",
                "counts": None,
                "reason": "Supply explicit PKs",
            },
        ],
    }


def test_rs_rich_report_renders_counts_and_literal_user_text():
    stream = StringIO()
    render_report(report(), Console(file=stream, width=100, color_system=None))
    output = stream.getvalue()
    assert "[bold]orders" in output
    assert "12" in output
    assert "blocked" in output
    assert "Supply explicit PKs" in output


def test_intuituive_report_supports_navigation_and_quit():
    rendered = run(create_report_app(report()), "down q", width=100, height=18)
    screen = "\n".join(rendered.screen)
    assert rendered.finished
    assert "[bold]orders" in screen
    assert "Supply explicit PKs" in screen
