# Sources, templates and comparison rules

Each side of a pair accepts a three-part table name, a structured table source, a union, a deduplicated source, or a named template. [The YAML example](../examples/templates.yaml) and [equivalent JSON](../examples/templates.json) exercise the complete configuration.

## Reusable source templates

Templates live in the suite's `templates` mapping. They declare named source parameters and compose the same source operations available directly on a pair:

```yaml
templates:
  merge_unique:
    parameters: [first, second]
    source:
      kind: deduplicate
      source:
        kind: union
        sources:
          - {kind: parameter, name: first}
          - {kind: parameter, name: second}
pairs:
  - id: orders
    baseline:
      kind: template
      name: merge_unique
      inputs:
        first: prod.sales.orders
        second: {kind: table, table: prod.sales.archive, version: 12}
    candidate: dev.sales.orders
    keys:
      - {baseline: order_id, candidate: order_id}
```

`union` preserves rows and aligns columns by name. Its inputs must have identical named column types; it does not invoke Spark's silent union type coercion or fill missing columns. `deduplicate` removes identical full rows. It does not choose an arbitrary survivor among conflicting rows with the same key: those still fail PK preflight. Each leaf table is read at a recorded Delta version, with repeated identical table/version references reused within a pair. Derived-source reports list all leaf snapshots.

Template inputs are typed source objects, not string substitution or executable Python. Templates can compose other templates; unknown references, missing/extra inputs, unbound parameters and cycles are rejected. Nesting is limited to 32 levels. Derived sources require explicit PK mappings; structured table sources retain metadata discovery. An explicit table version belongs inside that table source.

The `SourceResolver` protocol separates source operations from table I/O and comparison. `SparkSourceResolver` is the built-in implementation and uses public DataFrame APIs. Additional transforms and raw SQL sources are not implemented in this version.

## Column selection

`exclude_columns` removes named columns from comparison. `column_policy: common` compares the columns present on both sides after exclusions and explicit mappings; `strict` (the default) reports added/missing columns as schema changes. Reports list ignored columns separately for each side. Excluding a nonexistent column or a PK is an error. Explicitly mapped columns must exist even under `common`. A comparison with no value columns requires `allow_key_only: true`.

## Implicit and explicit casts

```yaml
defaults:
  implicit_casts: true       # default; set false to disable across the suite
  null_safe: true
  column_rules:
    external_code:
      implicit_casts: false # retain strict type comparison for this column
    amount:
      compare_as: decimal(20,2)
    description:
      null_safe: false
pairs:
  - id: orders
    baseline: prod.sales.orders
    candidate: dev.sales.orders
    compare:
      implicit_casts: false # optional pair override
      column_rules:
        quantity:
          implicit_casts: true # re-enable only this column
```

`compare_as` always takes precedence over implicit detection, even when `implicit_casts` is false. Directives use baseline column names, including when a candidate column is mapped to another name. Pair column rules inherit individual fields from suite column rules; omitting a field (or using null inside a column rule) retains the inherited value. Unspecified tolerances resolve to zero. Lists such as `exclude_columns` are replaced by pair overrides.

Automatic alignment handles numeric strings against numeric columns, integral widening, decimal alignment within Spark's 38-digit limit, and numeric/floating pairs. It selects a target and validates the entire input before comparing rows; it does not infer equivalence from samples. Numeric strings use plain signed decimal notation. Whitespace, exponent notation and formatting such as thousands separators are not normalized automatically. String-to-floating validation is conservatively bounded to values representable in `decimal(38,18)` without nonzero fractional digits beyond scale 18, then checks the cast's canonical decimal value. Unsupported or ambiguous type combinations remain schema differences; supply an explicit target or normalize upstream.

Supported explicit targets are `string`, `boolean`, `byte`, `short`, `int`, `long`, `float`, `double`, `decimal(p,s)`, `date`, `timestamp`, and `timestamp_ntz`. Numeric narrowing, overflow, lost numeric precision and malformed values block comparison. A non-null value converted to null can never become a passing null/null comparison. Explicit nonnumeric casts follow Spark's target semantics (for example, converting a timestamp to a date discards its time); time-zone-sensitive casts use the session time zone. Casts affect the comparison frames only; source tables are unchanged.

The JSON report and rs-rich/intuiTUIve views expose selected casts, original types (JSON), whether they were implicit or explicit, and invalid-row counts. PK casts also run before null/uniqueness validation: keys `"01"` and `"1"` cannot collapse to one numeric key and pass.

## Null handling

`null_safe: true` treats two null values as equal, and a null versus a non-null as different. With `false`, any comparison involving a null counts as different, including null versus null. This avoids SQL's unknown/null predicate silently suppressing a difference. The option can be set for the suite, a pair, or an individual value column. PKs must always be non-null and unique regardless of this option. NaN continues to use Spark's equality semantics; it is not treated as null.

## Complex types and comparators

Every compared value column is assigned one comparator before any row is joined. The selection and its policy are recorded in the pair result's `comparators` list (and shown by `pairity inspect`/CLI reports) whenever the column does not use plain exact equality.

| Column type and rule | Comparator | Semantics |
|---|---|---|
| Scalar, no tolerance | `exact` | Null-safe equality per `null_safe` (not recorded) |
| `float`/`double` with a tolerance | `float_tolerance` | Formula in [configuration](configuration.md) |
| Integral/decimal with `absolute_tolerance` | `decimal_tolerance` | `abs(baseline - candidate) <= tolerance` in exact decimal arithmetic |
| `struct`, `array`, `map` | `nested` | Structs by field name, maps by content, arrays by `array_order` |
| `comparator: <name>` | Registered plugin | Supplied by the application |

```yaml
defaults:
  column_rules:
    line_items:
      array_order: unordered   # default ordered; applies to arrays at every depth
    amount:                    # decimal(18,2)
      absolute_tolerance: 0.01
```

**Structs** compare by declared field name, recursively. Field order does not matter; field names and types must match after alignment, otherwise the column is reported as a type change. Nullability flags inside nested types are not compared, consistent with top-level columns.

**Maps** compare by key/value content, independent of storage order.

**Arrays** compare element by element by default. `array_order: unordered` compares them as multisets: `[1, 1, 2]` differs from `[1, 2, 2]`. The setting applies to every array inside the column, including arrays in struct fields and map values.

**Nested nulls and floats.** `null_safe` governs the top-level value. A null field or element inside a present value equals a null in the same position, so a single null field cannot make an entire struct unknown. Nested floats use Spark's value equality: NaN equals NaN and `-0.0` equals `0.0`. Tolerances inside nested values block; implement a custom comparator for them. `compare_as` on a nested column blocks instead of stringifying it, and implicit casts never apply to nested columns.

**Decimal and integral tolerances** are computed in decimal arithmetic: `decimal(p,s) - decimal(p,s)` is exactly representable as `decimal(p+1,s)` for `p <= 37`. Integral columns use their decimal precision (`long` is `decimal(20,0)`). Because values at scale `s` differ in multiples of `10^-s`, the configured tolerance is floored to the column scale, so `0.015` on `decimal(12,2)` behaves as `0.01`. The configured float is read using its shortest round-trip decimal form (`0.1` is exactly `0.1`). Tolerances on `decimal(38,s)` and `relative_tolerance` on integral/decimal columns block. Use `compare_as: double` to opt into floating relative tolerance explicitly.

**Timestamps.** `timestamp` values compare as instants, independent of the session time zone. `timestamp_ntz` compares wall-clock values. A `timestamp`/`timestamp_ntz` pair is a type change unless `compare_as` selects one target explicitly; that cast uses the session time zone. Dates and timestamps are not truncated or normalized implicitly.

### Custom comparators

A comparator implements `pairity.contracts.ColumnComparator`: `problems(data_type, rule)` returns reasons a combination is unsupported, `equal(...)` returns a Spark boolean expression, and `policy(...)` returns the recorded description. Register trusted factories in application code. The configuration only names them; it never imports code:

```python
from pairity.plugins import PluginRegistry
from pairity.spark import SparkComparisonBackend

comparators = PluginRegistry()
comparators.register("case_insensitive", CaseInsensitiveComparator)
backend = SparkComparisonBackend(comparators)
```

Built-in names (`exact`, `float_tolerance`, `decimal_tolerance`, `nested`) cannot be replaced. An unknown name, a failing validation or any reported problem blocks the pair, and the message lists every affected column. A null result from `equal` counts as changed, never as equal. Orchestration, results and storage are unchanged by custom comparators. Incident `reproduce.py` scripts use the built-in registry, so a suite that names a custom comparator blocks there until the same plugin is supplied.
