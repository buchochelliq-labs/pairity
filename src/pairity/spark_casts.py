"""Conservative automatic type alignment with full-input cast validation."""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pairity.type_alignment import implicit_target_name

INTEGRAL = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
FLOATING = (T.FloatType, T.DoubleType)
NUMERIC = (*INTEGRAL, *FLOATING, T.DecimalType)


def quote(name):
    return "`" + name.replace("`", "``") + "`"


def implicit_target(a, b):
    return implicit_target_name(a.simpleString(), b.simpleString())


def _cast(frame, name, target):
    dtype = frame.schema[name].dataType
    value = F.col(quote(name))
    expression = f"try_cast({quote(name)} as {target})"
    converted = F.expr(expression)
    converted_type = frame.select(converted.alias("_cast")).schema[0].dataType
    invalid = value.isNotNull() & converted.isNull()
    if isinstance(dtype, NUMERIC):
        # Detect narrowing, fractional truncation and numeric precision loss.
        back = F.expr(f"try_cast({expression} as {dtype.simpleString()})")
        invalid = invalid | (value.isNotNull() & ~value.eqNullSafe(back))
    elif isinstance(dtype, T.StringType) and isinstance(converted_type, NUMERIC):
        scale = converted_type.scale if isinstance(converted_type, T.DecimalType) else 0
        if isinstance(converted_type, FLOATING):
            # Compare canonical decimal values, with a bounded exact input domain.
            # Inputs beyond this domain require an upstream normalization policy.
            scale = 18
            original = F.expr(f"try_cast({quote(name)} as decimal(38,18))")
            back = F.expr(f"try_cast(cast({expression} as string) as decimal(38,18))")
            invalid = invalid | (
                value.isNotNull() & (original.isNull() | ~original.eqNullSafe(back))
            )
        pattern = rf"^[+-]?[0-9]+(?:\.[0-9]{{0,{scale}}}0*)?$"
        invalid = invalid | (value.isNotNull() & ~value.rlike(pattern))
    invalid_count = frame.where(F.coalesce(invalid, F.lit(False))).count()
    return frame.withColumn(name, converted), invalid_count


def normalize_frames(left, right, mappings, settings):
    decisions = []
    for mapping in mappings:
        a = left.schema[mapping.baseline].dataType
        b = right.schema[mapping.candidate].dataType
        rule = settings.column_rules.get(mapping.baseline)
        implicit = (
            settings.implicit_casts
            if rule is None or rule.implicit_casts is None
            else rule.implicit_casts
        )
        target = rule.compare_as if rule else None
        mode = "explicit" if target else "implicit"
        if not target and implicit:
            target = implicit_target(a, b)
        if target is None:
            continue
        left, invalid_left = _cast(left, mapping.baseline, target)
        right, invalid_right = _cast(right, mapping.candidate, target)
        decisions.append(
            {
                "baseline": mapping.baseline,
                "candidate": mapping.candidate,
                "baseline_type": a.simpleString(),
                "candidate_type": b.simpleString(),
                "compare_as": target,
                "mode": mode,
                "invalid_baseline_rows": invalid_left,
                "invalid_candidate_rows": invalid_right,
            }
        )
    return left, right, decisions
