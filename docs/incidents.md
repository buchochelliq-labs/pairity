# Offline incident evidence

Install `pairity-data[artifacts]` for PNG rendering. `IncidentExporter` consumes the
persisted `RunResult.to_dict()` report (the `report` field of a Delta run envelope).
It never reads current tables, discovers current primary keys, or needs credentials.

```python
from pairity.incidents import IncidentExporter

exporter = IncidentExporter()
files = exporter.export(envelope['report'], pair_ids=['orders'])
archive = exporter.zip_bytes(files)
# Publish each bytes value with your configured ArtifactStore, or save the archive.
```

The bundle contains a ticket draft, deterministic PNG summary, complete HTML/text
summary, selected result JSON, expanded/resolved JSON and YAML configuration,
reproduction Python and SQL, and a versioned manifest. The manifest records each
payload's SHA-256 and byte count; it does not hash itself. Original run configuration
hash and selected resolved configuration hash are distinct. Filenames are fixed,
ZIP timestamps fixed, and pair IDs never become filesystem paths. PNG bytes are
repeatable within the same Pillow/font version; cross-version identity is not promised.

Pair selection removes other outcomes, pair definitions and unused templates. Source
templates are expanded, settings resolved, and recorded key mappings copied. Blocked,
error and skipped outcomes have unavailable counts, never invented zeros. Images are
bounded to 180 lines; complete details remain in the text/HTML/JSON files.

## Reproduction capabilities and limits

Every table leaf is matched to recorded input metadata in traversal order and must
have a nonnegative integer version and `snapshot_pinned=true`. Explicit configured
versions must agree. Missing, extra, mismatched or conflicting leaves block the
reproduction scripts; they never fall back to the latest version. Error outcomes may
lack these metadata and still produce useful incident evidence.

Python reuses Pairity's actual comparison backend, expanded union/dedup source
resolver, per-column casts, null policies, mappings, tolerances, and recorded keys.
Its reader refuses unpinned reads and does not query current Delta history. Run it
on an authorized Classic Spark/Delta environment with the original engine/runtime
versions and retained Delta files. Runtime versions, session timezone, ANSI and
collation settings were not recorded by the report schema, so the export cannot
promise byte-for-byte or semantic equivalence across runtime changes. The manifest's
`reproduction.ready` means pins/keys are available, not that the rerun was executed.

`reproduce.sql` contains pinned source reads, raw key null/duplicate checks and, where it can be rendered faithfully, **exact comparison SQL** for each pair. The comparison queries come from the recorded outcome, not from a generic full outer join:

- the resolved keys and value-column mappings, including renamed columns;
- the recorded casts, applied with `try_cast` to the same targets;
- the effective null policy for each column;
- each column's recorded comparator: float tolerances, exact decimal/integral tolerances (the same floored literal the engine uses), and nested policies (structs by field name, maps by content, arrays ordered or as multisets).

Each pair gets a counts query (`equal_rows`, `changed_rows`, `missing_rows`, `extra_rows` and a `column:<name>` count per value column) and a query listing the missing (baseline-only), extra (candidate-only) and changed rows with their changed columns. Source reads pin `VERSION AS OF` and apply recorded `where` filters, so the queries keep reproducing the recorded counts after the tables change, as long as those Delta versions are retained. `manifest.json` records `reproduction.exact_comparison_sql` per pair, and the summary lists which pairs have it.

Exact SQL is marked UNSUPPORTED, rather than approximated, when:
- a source is a union (persisted reports lack the column order needed to reproduce `unionByName`);
- a column uses a custom comparator;
- the report predates recorded comparator types;
- no row comparison ran;
- the compared columns were not recorded.

Use `reproduce.py` for those pairs. Raw key checks run before casts and can differ from validation after configured casts. The renderer is tested for equivalence against the engine on fixtures covering scalars, missing/extra rows, per-column null policies, float NaN/infinity/relative tolerances, exact decimal and integral tolerances, nested structs/maps/unordered arrays, casts and renamed composite keys, and end to end on Delta tables after later commits.

## Optional samples

Samples are aggregate-only by default, even if supplied. To attach already captured
rows, explicitly opt in:

```python
exporter = IncidentExporter(
    include_samples=True, max_sample_rows=50, max_sample_bytes=100_000,
    mask_columns=['email', 'account_number'],
)
files = exporter.export(report, pair_ids=['orders'], samples={'orders': captured_rows})
```

Rows must be JSON dictionaries. The row cap applies across the entire selected
bundle; a serialized byte limit rejects oversized samples. Column masking is recursive
and case-insensitive. Common credential field names and common credential patterns in
text are redacted throughout evidence; arbitrary unlabeled secrets cannot be detected
reliably. Keep credentials out of persisted reports and inspect evidence before sharing.
No samples are queried or collected by the exporter, and attaching samples does not
make them a complete difference dataset.
