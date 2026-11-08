"""Render a recorded pair comparison as Spark SQL with the engine's exact semantics.

The renderer works from what a run recorded: resolved keys, value-column mappings,
cast decisions, effective null policies and the comparator selected for each column
with its policy and data type. It mirrors ``SparkComparisonBackend`` expression by
expression (casts via ``try_cast``, null-safe or strict equality, float and exact
decimal tolerances, nested canonicalization). Anything it cannot render faithfully,
such as a custom comparator, raises :class:`UnsupportedSql` instead of approximating.
"""

import json
import re
from decimal import ROUND_FLOOR, Decimal, localcontext

INTEGRAL_DIGITS = {"byte": 3, "short": 5, "integer": 10, "long": 20}
KINDS = ("equal", "changed", "missing", "extra")


class UnsupportedSql(ValueError):
    """The recorded comparison cannot be expressed faithfully in SQL."""


def ident(name):
    return "`" + name.replace("`", "``") + "`"


def string_literal(value):
    # Spark string literals treat backslash as an escape character.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _complex(data_type):
    return isinstance(data_type, dict) and data_type.get("type") in {"struct", "array", "map"}


def _canonical(expr, data_type, order, depth=0):
    """SQL mirror of spark_comparators.canonical_value."""
    if depth > 32:
        raise UnsupportedSql("Nested type exceeds 32 levels")
    if not _complex(data_type):
        return expr
    variable = f"pairity_e{depth}"
    kind = data_type["type"]
    if kind == "struct":
        fields = sorted(data_type["fields"], key=lambda f: f["name"])
        parts = ", ".join(
            f"{string_literal(f['name'])}, "
            + _canonical(f"{expr}.{ident(f['name'])}", f["type"], order, depth + 1)
            for f in fields
        )
        return f"CASE WHEN {expr} IS NULL THEN NULL ELSE named_struct({parts}) END"
    if kind == "array":
        element = data_type["elementType"]
        if _complex(element):
            inner = _canonical(variable, element, order, depth + 1)
            expr = f"transform({expr}, {variable} -> {inner})"
        return f"array_sort({expr})" if order == "unordered" else expr
    key = _canonical(f"{variable}.key", data_type["keyType"], order, depth + 1)
    value = _canonical(f"{variable}.value", data_type["valueType"], order, depth + 1)
    return (
        f"array_sort(transform(map_entries({expr}), {variable} -> "
        f"named_struct('key', {key}, 'value', {value})))"
    )


def _exact(left, right, null_safe):
    return f"({left} <=> {right})" if null_safe else f"coalesce({left} = {right}, false)"


def _tolerant(left, right, null_safe, within):
    both = f"{left} IS NOT NULL AND {right} IS NOT NULL"
    return f"({_exact(left, right, null_safe)} OR coalesce({both} AND {within}, false))"


def _float_equal(left, right, policy, null_safe):
    absolute = float(policy.get("absolute_tolerance") or 0)
    relative = float(policy.get("relative_tolerance") or 0)
    if not (absolute or relative):
        return _exact(left, right, null_safe)
    lhs, rhs = f"CAST({left} AS DOUBLE)", f"CAST({right} AS DOUBLE)"
    infinity = "CAST('Infinity' AS DOUBLE)"
    finite = (
        f"NOT isnan({lhs}) AND NOT isnan({rhs}) "
        f"AND abs({lhs}) != {infinity} AND abs({rhs}) != {infinity}"
    )
    distance = f"abs({lhs} - {rhs})"
    within = f"{distance} <= CAST({absolute!r} AS DOUBLE)"
    if relative:
        scale = f"greatest(abs({lhs}), abs({rhs}))"
        scaled = (
            f"CASE WHEN {distance} = {infinity} THEN abs({lhs} / {scale} - {rhs} / {scale}) "
            f"ELSE {distance} / {scale} END"
        )
        within = f"({within} OR {scaled} <= CAST({relative!r} AS DOUBLE))"
    return _tolerant(lhs, rhs, null_safe, f"({finite} AND {within})")


def _decimal_shape(data_type):
    if isinstance(data_type, str):
        if data_type in INTEGRAL_DIGITS:
            return INTEGRAL_DIGITS[data_type], 0
        match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", data_type)
        if match:
            return int(match.group(1)), int(match.group(2))
    raise UnsupportedSql(f"decimal_tolerance on unexpected type {data_type}")


def tolerance_literal(value, scale):
    """The same floored, capped decimal literal the engine uses."""
    with localcontext() as context:
        context.prec = 80
        quantum = Decimal(1).scaleb(-scale)
        ceiling = Decimal(10) ** (38 - scale) - quantum
        configured = Decimal(repr(float(value)))
        literal = (
            ceiling if configured >= ceiling else configured.quantize(quantum, rounding=ROUND_FLOOR)
        )
    return f"CAST('{literal}' AS DECIMAL(38,{scale}))"


def _decimal_equal(left, right, data_type, policy, null_safe):
    tolerance = policy.get("absolute_tolerance") or 0
    if not tolerance:
        return _exact(left, right, null_safe)
    precision, scale = _decimal_shape(data_type)
    target = f"DECIMAL({precision},{scale})"
    distance = f"abs(CAST({left} AS {target}) - CAST({right} AS {target}))"
    return _tolerant(left, right, null_safe, f"{distance} <= {tolerance_literal(tolerance, scale)}")


def column_equal(left, right, record, null_safe):
    """SQL for one column's equality, never null, from its recorded comparator."""
    if record is None:
        return _exact(left, right, null_safe)
    name, policy = record["comparator"], record.get("policy", {})
    if name == "float_tolerance":
        return _float_equal(left, right, policy, null_safe)
    if "type_json" not in record and name in {"decimal_tolerance", "nested"}:
        raise UnsupportedSql(f"{record['column']}: report predates recorded comparator types")
    data_type = json.loads(record["type_json"]) if "type_json" in record else None
    if name == "decimal_tolerance":
        return _decimal_equal(left, right, data_type, policy, null_safe)
    if name == "nested":
        order = policy.get("array_order", "ordered")
        return _exact(
            _canonical(left, data_type, order), _canonical(right, data_type, order), null_safe
        )
    raise UnsupportedSql(f"{record['column']}: custom comparator {name} has no SQL rendering")


def _side_select(source, side, keys, columns, casts):
    """Project one side to its compared columns, applying recorded casts."""
    by_column = {cast[side]: cast["compare_as"] for cast in casts}
    names = [getattr(k, side) for k in keys] + [getattr(c, side) for c in columns]
    projected = []
    for name in names:
        target = by_column.get(name)
        expr = ident(name)
        projected.append(f"try_cast({expr} AS {target}) AS {expr}" if target else expr)
    return f"SELECT {', '.join(projected)} FROM ({source}) AS pairity_source"


def comparison_sql(baseline_sql, candidate_sql, *, keys, columns, settings, casts, comparators):
    """Return ``(counts_sql, differences_sql)`` reproducing one recorded comparison.

    ``keys`` and ``columns`` are ColumnMapping-like objects in comparison order;
    ``settings`` is the effective ComparisonSettings; ``casts`` and ``comparators`` are
    the pair result's recorded lists.
    """
    if not keys:
        raise UnsupportedSql("No recorded primary keys")
    records = {item["column"]: item for item in comparators}
    changed = []
    for mapping in columns:
        rule = settings.column_rules.get(mapping.baseline)
        null_safe = settings.null_safe if rule is None or rule.null_safe is None else rule.null_safe
        equal = column_equal(
            f"b.pairity_row.{ident(mapping.baseline)}",
            f"c.pairity_row.{ident(mapping.candidate)}",
            records.get(mapping.baseline),
            null_safe,
        )
        changed.append((mapping.baseline, f"(matched AND NOT coalesce({equal}, false))"))
    join = " AND ".join(
        f"b.pairity_row.{ident(k.baseline)} = c.pairity_row.{ident(k.candidate)}" for k in keys
    )
    flags = "".join(f",\n    {expr} AS {ident('changed:' + name)}" for name, expr in changed)
    any_changed = " OR ".join(ident("changed:" + name) for name, _ in changed) or "false"
    changed_list = ", ".join(
        f"CASE WHEN {ident('changed:' + name)} THEN {string_literal(name)} END"
        for name, _ in changed
    )
    changed_array = f"filter(array({changed_list}), x -> x IS NOT NULL)" if changed else "array()"
    base = f"""WITH pairity_baseline AS (
  SELECT true AS pairity_present, struct(*) AS pairity_row
  FROM ({_side_select(baseline_sql, "baseline", keys, columns, casts)}) AS s
), pairity_candidate AS (
  SELECT true AS pairity_present, struct(*) AS pairity_row
  FROM ({_side_select(candidate_sql, "candidate", keys, columns, casts)}) AS s
), pairity_joined AS (
  SELECT
    b.pairity_row AS baseline,
    c.pairity_row AS candidate,
    b.pairity_present IS NOT NULL AS has_baseline,
    c.pairity_present IS NOT NULL AS has_candidate,
    b.pairity_present IS NOT NULL AND c.pairity_present IS NOT NULL AS matched{flags}
  FROM pairity_baseline b FULL OUTER JOIN pairity_candidate c ON {join}
), pairity_classified AS (
  SELECT *,
    CASE WHEN NOT has_baseline THEN 'extra'
         WHEN NOT has_candidate THEN 'missing'
         WHEN {any_changed} THEN 'changed'
         ELSE 'equal' END AS kind,
    {changed_array} AS changed_columns
  FROM pairity_joined
)"""
    counts = ",\n  ".join(
        [f"count_if(kind = '{kind}') AS {kind}_rows" for kind in KINDS]
        + [
            f"count_if({ident('changed:' + name)}) AS {ident('column:' + name)}"
            for name, _ in changed
        ]
    )
    counts_sql = f"{base}\nSELECT\n  {counts}\nFROM pairity_classified"
    differences_sql = (
        f"{base}\nSELECT kind, baseline, candidate, changed_columns\n"
        "FROM pairity_classified WHERE kind != 'equal'"
    )
    return counts_sql, differences_sql
