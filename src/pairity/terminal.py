"""Human CLI output uses rs-rich; interactive views use its intuiTUIve binding."""

from rs_rich.console import Console
from rs_rich.markup import escape
from rs_rich.table import Table
from rs_rich.text import Text


def _safe(value):
    return "".join(
        f"\\x{ord(c):02x}" if ord(c) < 32 or 127 <= ord(c) < 160 else c for c in str(value)
    )


def _rows(report):
    if report.get("result_schema_version") != 1 or not isinstance(report.get("pairs"), list):
        raise ValueError("Expected a Pairity result_schema_version 1 run report")
    rows = []
    for pair in report["pairs"]:
        if pair.get("status") not in {"passed", "failed", "blocked", "error", "skipped"}:
            raise ValueError("Invalid status in run report")
        counts = pair.get("counts") or {}
        rows.append(
            [_safe(pair["pair_id"]), pair["status"]]
            + [
                _safe(counts.get(key, "—"))
                for key in ("matched_rows", "changed_rows", "missing_rows", "extra_rows")
            ]
        )
    return rows


def _normalization_details(pair):
    lines = []
    for cast in pair.get("casts", []):
        lines.append(
            f"{cast['baseline']} -> {cast['candidate']}: {cast['mode']} compare_as "
            f"{cast['compare_as']} (invalid rows: baseline={cast['invalid_baseline_rows']}, "
            f"candidate={cast['invalid_candidate_rows']})"
        )
    for side, names in pair.get("ignored_columns", {}).items():
        if names:
            lines.append(f"Ignored {side} columns: {', '.join(names)}")
    return lines


def render_report(report, console=None):
    console = console if console is not None else Console()
    if isinstance(report.get("pairs"), list):
        table = Table(
            "Pair",
            "Status",
            "Matched",
            "Changed",
            "Missing",
            "Extra",
            title=Text(f"Pairity · {_safe(report['suite'])}"),
        )
        for row in _rows(report):
            table.add_row(*[Text(value) for value in row])
        console.print(table)
        for pair in report["pairs"]:
            if pair.get("reason"):
                console.print(Text(f"{_safe(pair['pair_id'])}: {_safe(pair['reason'])}"))
            for detail in _normalization_details(pair):
                console.print(Text(f"{_safe(pair['pair_id'])}: {_safe(detail)}"))
        console.print(Text(f"Suite status: {_safe(report['status'])}"))
    elif "preview" in report:
        table = Table("Pair", "Change", title="Pairity · configuration update")
        for change, names in report["preview"].items():
            for name in names:
                table.add_row(Text(_safe(name)), Text(change))
        console.print(table)
        console.print(
            Text(
                f"{'Applied' if report['applied'] else 'Preview'} · "
                f"revision {report['revision']} · "
                f"suite settings changed: {report['suite_settings_changed']}"
            )
        )
    else:
        table = Table("Setting", "Value", title="Pairity · configuration")
        for key, value in report.items():
            table.add_row(Text(_safe(key)), Text(_safe(value)))
        console.print(table)


def render_error(error):
    Console(stderr=True).print(
        Text(f"Pairity: {type(error).__name__}: {_safe(error)}", style="red")
    )


def create_report_app(report):
    from rs_rich.tui import App, Column, Size, column, label, signal, table, text

    rows = [[escape(value) for value in row] for row in _rows(report)]
    if not rows:
        raise ValueError("The run report contains no pairs to inspect")

    def build():
        selected = signal(0)

        def detail():
            pair = report["pairs"][selected.get()]
            lines = [pair.get("reason") or f"{pair['pair_id']}: {pair['status']}"]
            lines += _normalization_details(pair)
            return "\n".join(escape(_safe(line)) for line in lines)

        return column(
            [
                label(f"[bold magenta]Pairity[/] · {escape(_safe(report['suite']))}").auto(),
                table(
                    [
                        Column(name, Size.Auto)
                        for name in ("Pair", "Status", "Matched", "Changed", "Missing", "Extra")
                    ],
                    rows,
                    selected,
                ),
                text(detail).auto(),
                label("[dim]↑/↓ select a pair · q quit[/]").auto(),
            ]
        ).on_key("q", lambda context: context.quit())

    return App(build)
