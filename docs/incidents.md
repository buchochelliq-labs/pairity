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

SQL is explicitly **diagnostic**, not an equivalent comparison query. It includes
pinned table reads, supported dedup reads, and raw key null/duplicate checks. Key checks
precede casts and can differ from validation after configured casts. For composed
unions, the export includes individually pinned leaf reads but marks union SQL
unsupported: persisted reports lack column order needed to safely reproduce the
engine's `unionByName` semantics. Full comparison SQL remains unsupported for all
shapes. Use Python for comparison semantics; do not interpret these SQL snippets as
certification of row equality.

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
