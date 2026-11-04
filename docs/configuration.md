# Configuration

The `.json`, `.yaml` and `.yml` extensions select codecs for the same schema. Use `pairity schema` to print the schema or [the checked-in schema](../schema/pairity.schema.json) for editor validation. Examples in both formats describe the same suite.

## Primary keys

Explicit ordered `keys` mappings take precedence and never require metadata access. Otherwise, the Unity Catalog provider reads declared primary-key columns. Discovered keys are matched by column name, even when their declaration order differs. Differently named, absent or ambiguous PKs require explicit mappings. Metadata permission errors remain errors, not an empty successful lookup.

Every supported row comparison checks both input snapshots for null and duplicate keys before joining. This is necessary because Databricks primary-key declarations are informational. Empty explicit key lists are invalid. There is no all-columns or sampled-key fallback.

## Settings

| Setting | Meaning |
|---|---|
| `schema_version` | Integer `1` |
| `suite` | Suite name |
| `execution.max_parallel_pairs` | Integer 1–64; default 4; CLI override is recorded in the run configuration |
| `pairs[].id` | Stable, unique identifier used for updates and results |
| `baseline`, `candidate` | Three-part `catalog.schema.table` names |
| `keys` | Optional nonempty list of `{baseline, candidate}` PK mappings |
| `columns` | Explicit value-column mappings; remaining columns match by name |
| `baseline_version`, `candidate_version` | Optional Delta versions; otherwise each side is pinned at read time |
| `compare` | Overrides for the suite's `defaults` |
| `enabled`, `tags` | Execution selection and bulk-update grouping |

Initial table identifiers support letters, digits, underscores and hyphens within each part, starting with a letter/underscore. Literal column names can contain dots or spaces and are quoted in Spark expressions. Case-colliding/duplicate columns are rejected rather than ambiguously matched.

Defaults and overrides support strict schema comparison, `exclude_columns`, `column_rules`, `allow_key_only`, and maximum changed/missing/extra row counts. Zero row thresholds mean exact regression equality. A value equal to a configured maximum passes. `missing` means present only in the baseline; `extra` means present only in the candidate.

In this version, strict schema comparison covers column names and Spark logical data types after explicit mappings/exclusions. Field order, nullability declarations and arbitrary schema metadata are not compared; actual key nulls are always validated.

Overrides replace scalar/list values. `column_rules` merges by column name, with an overriding column rule replacing that column's complete policy. Omitting an override inherits it; an explicit null override is invalid. An empty exclusion list clears inherited exclusions. Rules address baseline column names. Excluding a key, naming an absent exclusion or applying a rule to an un-compared column blocks execution.

Float/double values pass when they are exactly null-safe equal, or both finite and:

`abs(baseline - candidate) <= max(absolute_tolerance, relative_tolerance * max(abs(baseline), abs(candidate)))`

NaN equals NaN; equal infinities compare equal; finite and infinite values differ. Null equals null and differs from non-null. Nonzero tolerances on other types currently block rather than cast away precision. If no value columns remain, explicitly enable `allow_key_only` to compare key membership alone.

## JSON/YAML interchange

Both formats reject duplicate mapping keys, unknown fields, non-finite numbers, invalid types and unsupported versions. YAML plain scalars follow JSON-like rules: lowercase `true`/`false`/`null`, JSON decimal numbers, and otherwise strings. Thus `on`, `off`, unquoted dates and `01` stay strings. YAML object tags and aliases cannot introduce executable objects or recursive structures. YAML anchors/aliases are not supported; use suite defaults instead.

Conversion preserves semantic values; comments, whitespace and original key order are not a round-trip contract. Configuration fingerprints are computed over normalized, sorted JSON, independent of the input format.

## Bulk updates and local storage

`import` accepts a complete suite in either format. The imported suite-level defaults and execution settings become authoritative. Supplied pairs replace/add by ID; omitted pairs are retained. `patch` accepts a mapping of pair fields and targets explicit IDs or tags (any selected tag matches). Patches recursively merge objects and replace lists/scalars. IDs cannot be patched. Unknown selectors or invalid values reject the entire operation.

Both commands preview changes by default. `--apply` writes only after whole-suite validation; `--expected-revision` prevents overwriting a newer revision. Preview output explicitly indicates suite-level setting changes. Old numbered revisions remain exportable.

`FileConfigStore` owns one suite per local directory and publishes revisions with atomic replacement under an exclusive writer lock. It is not suitable as distributed locking on object storage or UC Volumes. A process killed while holding the lock may leave `.write.lock`; remove it only after confirming that no writer remains. This fail-closed behavior avoids automatically stealing a live writer's lock.

Source table data is never stored in these configuration files. Run JSON reports store config, keys, input versions and exact summary counts. Distributed row differences are available through the library and should be written only to an explicitly selected result destination.
