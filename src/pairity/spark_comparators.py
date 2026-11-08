"""Built-in Spark column comparators and the registry that selects them.

Every comparator implements :class:`pairity.contracts.ColumnComparator`. Built-ins are
selected by data type; a column rule's ``comparator`` names a trusted, application-
registered plugin instead. Configuration strings never become imports.
"""

from decimal import ROUND_FLOOR, Decimal, localcontext

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pairity.plugins import PluginRegistry

INTEGRAL = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
FLOATING = (T.FloatType, T.DoubleType)
COMPLEX = (T.StructType, T.ArrayType, T.MapType)
SCALAR = (
    T.BooleanType,
    *INTEGRAL,
    *FLOATING,
    T.DecimalType,
    T.StringType,
    T.BinaryType,
    T.DateType,
    T.TimestampType,
    T.TimestampNTZType,
    T.NullType,
)
INTEGRAL_DECIMAL = {T.ByteType: 3, T.ShortType: 5, T.IntegerType: 10, T.LongType: 20}
BUILT_IN = ("exact", "float_tolerance", "decimal_tolerance", "nested")


def _has_tolerance(rule):
    return bool(rule and (rule.absolute_tolerance or rule.relative_tolerance))


def _scalar_rule_problems(rule):
    if rule and rule.array_order:
        return ["array_order applies only to array, map or struct columns"]
    return []


def _exact(left, right, null_safe):
    return left.eqNullSafe(right) if null_safe else F.coalesce(left == right, F.lit(False))


def _tolerant(left, right, null_safe, within):
    """Exact equality, or both values non-null and within the tolerance; never null."""
    both = left.isNotNull() & right.isNotNull()
    return _exact(left, right, null_safe) | F.coalesce(both & within, F.lit(False))


class ExactComparator:
    api_version = 1

    def problems(self, data_type, rule):
        if not isinstance(data_type, SCALAR):
            return [f"exact comparison does not support {data_type.simpleString()}"]
        if _has_tolerance(rule):
            return [f"tolerances are not supported for {data_type.simpleString()}"]
        return _scalar_rule_problems(rule)

    def equal(self, left, right, data_type, rule, null_safe):
        return _exact(left, right, null_safe)

    def policy(self, data_type, rule):
        return {}


class FloatToleranceComparator(ExactComparator):
    """``abs(a-b) <= max(abs_tol, rel_tol * max(abs(a), abs(b)))`` for finite values."""

    def problems(self, data_type, rule):
        if not isinstance(data_type, FLOATING):
            return [f"float_tolerance requires float or double, not {data_type.simpleString()}"]
        return _scalar_rule_problems(rule)

    def equal(self, left, right, data_type, rule, null_safe):
        if not _has_tolerance(rule):
            return _exact(left, right, null_safe)
        # FloatType subtraction may overflow even when the result fits in a double.
        left, right = left.cast("double"), right.cast("double")
        finite = (
            ~F.isnan(left)
            & ~F.isnan(right)
            & (F.abs(left) != float("inf"))
            & (F.abs(right) != float("inf"))
        )
        distance = F.abs(left - right)
        within = distance <= F.lit(rule.absolute_tolerance or 0)
        if rule.relative_tolerance:
            scale = F.greatest(F.abs(left), F.abs(right))
            # Opposite extreme doubles can overflow subtraction. Normalizing first
            # bounds that distance by two and avoids an infinity <= infinity false pass.
            scaled_distance = F.when(
                distance == float("inf"), F.abs(left / scale - right / scale)
            ).otherwise(distance / scale)
            within = within | (scaled_distance <= F.lit(rule.relative_tolerance))
        return _tolerant(left, right, null_safe, finite & within)

    def policy(self, data_type, rule):
        return {
            "absolute_tolerance": (rule and rule.absolute_tolerance) or 0,
            "relative_tolerance": (rule and rule.relative_tolerance) or 0,
        }


def _decimal_shape(data_type):
    if isinstance(data_type, T.DecimalType):
        return data_type.precision, data_type.scale
    for kind, digits in INTEGRAL_DECIMAL.items():
        if isinstance(data_type, kind):
            return digits, 0
    return None


def _tolerance_literal(value, scale):
    """Floor the tolerance to the column scale.

    Values at scale ``s`` differ by whole multiples of ``10**-s``, so
    ``distance <= tolerance`` exactly when ``distance <= floor_s(tolerance)``.
    ``repr`` gives the shortest decimal that round-trips the configured float.
    """
    with localcontext() as context:
        # The default 28-digit context would round 38-digit values.
        context.prec = 80
        quantum = Decimal(1).scaleb(-scale)
        ceiling = Decimal(10) ** (38 - scale) - quantum
        configured = Decimal(repr(value))
        # Any distance fits decimal(38,s); a larger tolerance is equivalent to the ceiling.
        literal = (
            ceiling if configured >= ceiling else configured.quantize(quantum, rounding=ROUND_FLOOR)
        )
    return F.lit(literal).cast(T.DecimalType(38, scale))


class DecimalToleranceComparator(ExactComparator):
    """Exact decimal arithmetic for integral and decimal absolute tolerances."""

    def problems(self, data_type, rule):
        shape = _decimal_shape(data_type)
        if shape is None:
            return [
                f"decimal_tolerance requires an integral or decimal type, "
                f"not {data_type.simpleString()}"
            ]
        if rule and rule.relative_tolerance:
            return [
                "relative_tolerance is not precision-safe for integral/decimal columns; "
                "use absolute_tolerance or compare_as double explicitly"
            ]
        if shape[0] > 37:
            return [
                f"absolute_tolerance on {data_type.simpleString()} would lose precision; "
                "use precision 37 or lower"
            ]
        return _scalar_rule_problems(rule)

    def equal(self, left, right, data_type, rule, null_safe):
        if not _has_tolerance(rule):
            return _exact(left, right, null_safe)
        precision, scale = _decimal_shape(data_type)
        target = T.DecimalType(precision, scale)
        # decimal(p,s) - decimal(p,s) is exactly decimal(p+1,s) for p <= 37: no rounding.
        distance = F.abs(left.cast(target) - right.cast(target))
        within = distance <= _tolerance_literal(rule.absolute_tolerance, scale)
        return _tolerant(left, right, null_safe, within)

    def policy(self, data_type, rule):
        return {
            "absolute_tolerance": (rule and rule.absolute_tolerance) or 0,
            "arithmetic": "exact_decimal",
        }


def canonical_type(data_type):
    """Type identity under nested policies: struct fields by name, nullability ignored."""
    if isinstance(data_type, T.StructType):
        return T.StructType(
            [
                T.StructField(f.name, canonical_type(f.dataType), True)
                for f in sorted(data_type.fields, key=lambda f: f.name)
            ]
        )
    if isinstance(data_type, T.ArrayType):
        return T.ArrayType(canonical_type(data_type.elementType), True)
    if isinstance(data_type, T.MapType):
        return T.MapType(canonical_type(data_type.keyType), canonical_type(data_type.valueType))
    return data_type


def _contains_complex(data_type):
    return isinstance(data_type, COMPLEX)


def canonical_value(column, data_type, array_order):
    """Rewrite a nested value into an order-independent, orderable representation.

    Structs are rebuilt with fields sorted by name. Maps become arrays of
    ``struct<key, value>`` sorted by key, so storage order never matters. Arrays keep
    their order unless ``array_order`` is ``unordered``, which compares them as sorted
    multisets (duplicates are significant).
    """
    if isinstance(data_type, T.StructType):
        rebuilt = F.struct(
            *[
                canonical_value(column.getField(f.name), f.dataType, array_order).alias(f.name)
                for f in sorted(data_type.fields, key=lambda f: f.name)
            ]
        )
        return F.when(column.isNull(), F.lit(None)).otherwise(rebuilt)
    if isinstance(data_type, T.ArrayType):
        element = data_type.elementType
        if _contains_complex(element):
            column = F.transform(column, lambda item: canonical_value(item, element, array_order))
        return F.array_sort(column) if array_order == "unordered" else column
    if isinstance(data_type, T.MapType):
        key_type, value_type = data_type.keyType, data_type.valueType
        entries = F.transform(
            F.map_entries(column),
            lambda entry: F.struct(
                canonical_value(entry["key"], key_type, array_order).alias("key"),
                canonical_value(entry["value"], value_type, array_order).alias("value"),
            ),
        )
        # Map keys are unique, so ordering entries by (key, value) orders them by key.
        return F.array_sort(entries)
    return column


class NestedComparator(ExactComparator):
    """Structs by declared field name, maps by content, arrays by configured order."""

    def problems(self, data_type, rule):
        if not isinstance(data_type, COMPLEX):
            return [f"nested comparison requires struct, array or map, not {data_type}"]
        if _has_tolerance(rule):
            return [
                "tolerances are not supported inside nested values; "
                "register a custom comparator for nested tolerance policies"
            ]
        return []

    def equal(self, left, right, data_type, rule, null_safe):
        order = (rule.array_order if rule else None) or "ordered"
        return _exact(
            canonical_value(left, data_type, order),
            canonical_value(right, data_type, order),
            null_safe,
        )

    def policy(self, data_type, rule):
        return {
            "struct_fields": "by_name",
            "map_entries": "by_key",
            "array_order": (rule.array_order if rule else None) or "ordered",
            "nested_nulls": "equal",
        }


class ComparatorRegistry:
    """Built-in comparators plus trusted application-registered extensions."""

    def __init__(self, custom: PluginRegistry | None = None):
        self.custom = custom or PluginRegistry()
        if shadowed := [name for name in BUILT_IN if name in self.custom]:
            raise ValueError(f"Custom comparators cannot replace built-ins: {shadowed}")
        self._built_in = {
            "exact": ExactComparator(),
            "float_tolerance": FloatToleranceComparator(),
            "decimal_tolerance": DecimalToleranceComparator(),
            "nested": NestedComparator(),
        }

    def default_name(self, data_type, rule):
        if isinstance(data_type, COMPLEX):
            return "nested"
        if _has_tolerance(rule) and isinstance(data_type, FLOATING):
            return "float_tolerance"
        if _has_tolerance(rule) and _decimal_shape(data_type):
            return "decimal_tolerance"
        return "exact"

    def resolve(self, data_type, rule):
        """Return ``(name, comparator, problems)`` for one column."""
        name = (rule.comparator if rule else None) or self.default_name(data_type, rule)
        if name in self._built_in:
            comparator = self._built_in[name]
        else:
            try:
                comparator = self.custom.create(name)
            except ValueError as exc:
                return name, None, [str(exc)]
        try:
            problems = list(comparator.problems(data_type, rule))
        except Exception as exc:  # A faulty plugin must block, not pass.
            problems = [f"comparator {name} failed validation: {exc}"]
        return name, comparator, problems
