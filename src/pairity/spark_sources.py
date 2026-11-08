"""Source operations over pinned leaf frames, using public DataFrame APIs."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import reduce
from operator import and_
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pairity.config import (
    DeduplicateSource,
    SqlSource,
    TableSource,
    UnionSource,
    parse_instant,
)
from pairity.results import BlockedComparison, TableInput
from pairity.sql_guard import check_query, function_calls, query_hash, render

INTEGRAL = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)


def assert_builtin_functions(catalog, names):
    """Block calls that resolve to persistent catalog functions.

    A persistent (for example Unity Catalog SQL) function can read tables that are not
    declared inputs. Builtins and session-temporary functions registered by trusted
    application code are allowed. Names that do not resolve are left to query analysis.
    """
    for name in names:
        try:
            function = catalog.getFunction(name)
        except Exception:
            continue  # Keywords such as IN, or genuinely unknown names that analysis rejects.
        location = (function.catalog, tuple(function.namespace or ()))
        if location not in {(None, ()), ("system", ("builtin",))}:
            raise BlockedComparison(
                f"SQL source calls persistent function {name} "
                f"({'.'.join(filter(None, [function.catalog, *location[1]]))}); "
                "only builtin functions can be used"
            )


def _sql_frame(source, children):
    """Run a guarded read-only query whose only relations are the pinned input frames."""
    check_query(source.query)  # Defense in depth: never trust an unvalidated model copy.
    for child in children.values():
        if child.frame.isStreaming:
            raise BlockedComparison("SQL source inputs must be batch snapshots")
    session = next(iter(children.values())).frame.sparkSession
    assert_builtin_functions(session.catalog, function_calls(source.query))
    token = uuid4().hex
    views = {}
    for position, (name, child) in enumerate(children.items()):
        # Session-scoped, unique and never dropped: Spark Connect re-analyzes the plan on
        # every action, so the views must outlive this call.
        view = f"pairity_sql_{token}_{position}"
        child.frame.createOrReplaceTempView(view)
        views[name] = f"`{view}`"
    try:
        return session.sql(render(source.query, views), args=dict(source.parameters) or None)
    except Exception as exc:
        raise BlockedComparison(f"SQL source failed analysis: {type(exc).__name__}: {exc}") from exc


class SparkSourceResolver:
    api_version = 1

    def read(self, source, reader, cache):
        if isinstance(source, str):
            source = TableSource(table=source)
        if isinstance(source, TableSource):
            return read_table(source, reader, cache)
        own = []
        if isinstance(source, DeduplicateSource):
            child = self.read(source.source, reader, cache)
            frame = child.frame.distinct()
            children = [child]
        elif isinstance(source, SqlSource):
            named = {name: self.read(node, reader, cache) for name, node in source.inputs.items()}
            frame = _sql_frame(source, named)
            children = list(named.values())
            own = [
                {"query_sha256": query_hash(source.query), "parameters": dict(source.parameters)}
            ]
        elif isinstance(source, UnionSource):
            children = [self.read(node, reader, cache) for node in source.sources]
            schemas = []
            for child in children:
                fields = child.frame.schema.fields
                if child.frame.isStreaming or len({f.name.casefold() for f in fields}) != len(
                    fields
                ):
                    raise BlockedComparison("Union requires batch inputs with unambiguous columns")
                schemas.append({f.name: f.dataType for f in fields})
            if any(schema != schemas[0] for schema in schemas[1:]):
                raise BlockedComparison(
                    "Union inputs must have identical named column types; normalize upstream"
                )
            frame = children[0].frame
            for child in children[1:]:
                frame = frame.unionByName(child.frame)
        else:
            raise BlockedComparison("Source must be expanded before materialization")
        return TableInput(
            frame,
            source.kind,
            snapshot_pinned=all(c.snapshot_pinned for c in children),
            leaves=[leaf for child in children for leaf in _leaves(child)],
            details={"queries": queries} if (queries := _queries(children) + own) else {},
        )


def _queries(children):
    return [query for child in children for query in child.details.get("queries", [])]


def read_table(source, reader, cache):
    """Read one pinned table snapshot (cached per table/version/as_of), then filter it."""
    # Same key shape as ComparisonRunner, so filtered and unfiltered reads share a snapshot.
    key = (source.table, source.version) + ((source.as_of,) if source.as_of else ())
    if key not in cache:
        if source.as_of is None:
            cache[key] = reader.read(source.table, source.version)
        else:
            try:
                cache[key] = reader.read(source.table, source.version, as_of=source.as_of)
            except TypeError as exc:
                raise BlockedComparison("This table reader does not support as_of") from exc
    base = cache[key]
    if not source.where:
        return base
    return TableInput(
        apply_filters(base.frame, source.where, source.table),
        base.table,
        base.version,
        snapshot_pinned=base.snapshot_pinned,
        details={
            **base.details,
            "filters": [f.model_dump(exclude_none=True) for f in source.where],
        },
    )


def _filter_literal(value, dtype):
    """Validate a configured value against the column type before building a literal."""
    if isinstance(dtype, T.BooleanType):
        if type(value) is bool:
            return F.lit(value)
    elif isinstance(dtype, INTEGRAL):
        bits = {T.ByteType: 8, T.ShortType: 16, T.IntegerType: 32, T.LongType: 64}[type(dtype)]
        if type(value) is int and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            return F.lit(value).cast(dtype)
    elif isinstance(dtype, T.DecimalType):
        # Floats are rejected: a binary float cannot state an exact decimal boundary.
        if type(value) in {int, str}:
            try:
                number = Decimal(str(value))
            except InvalidOperation:
                number = None
            if number is not None and number.is_finite():
                scaled = number.quantize(Decimal(1).scaleb(-dtype.scale))
                digits = len(scaled.as_tuple().digits)
                if scaled == number and digits <= dtype.precision:
                    return F.lit(scaled).cast(dtype)
    elif isinstance(dtype, (T.FloatType, T.DoubleType)):
        if type(value) in {int, float}:
            return F.lit(float(value)).cast(dtype)
    elif isinstance(dtype, T.StringType):
        if type(value) is str:
            return F.lit(value)
    elif isinstance(dtype, T.DateType):
        if type(value) is str:
            try:
                return F.lit(date.fromisoformat(value))
            except ValueError:
                pass
    elif isinstance(dtype, T.TimestampType):
        if type(value) is str:
            try:
                return F.lit(parse_instant(value))
            except ValueError:
                pass
    elif isinstance(dtype, T.TimestampNTZType):
        if type(value) is str:
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                parsed = None
            if parsed is not None and parsed.tzinfo is None:
                return F.lit(parsed.isoformat()).cast(dtype)
    return None


def apply_filters(frame, filters, table):
    """AND together typed predicates; any invalid filter blocks and lists every problem."""
    types = {field.name: field.dataType for field in frame.schema}
    predicates, problems = [], []
    for item in filters:
        dtype = types.get(item.column)
        if dtype is None:
            problems.append(f"{item.column}: column does not exist")
            continue
        column = F.col("`" + item.column.replace("`", "``") + "`")
        if item.op == "is_null":
            predicates.append(column.isNull())
            continue
        if item.op == "is_not_null":
            predicates.append(column.isNotNull())
            continue
        values = item.value if isinstance(item.value, list) else [item.value]
        literals = [_filter_literal(value, dtype) for value in values]
        if any(literal is None for literal in literals):
            problems.append(
                f"{item.column}: {values} is not a valid {dtype.simpleString()} filter value"
            )
            continue
        if item.op in {"in", "not_in"}:
            predicate = column.isin(*literals)
            predicates.append(~predicate if item.op == "not_in" else predicate)
        elif item.op == "between":
            predicates.append(column.between(*literals))
        else:
            operators = {
                "=": column.__eq__,
                "!=": column.__ne__,
                "<": column.__lt__,
                "<=": column.__le__,
                ">": column.__gt__,
                ">=": column.__ge__,
            }
            predicates.append(operators[item.op](literals[0]))
    if problems:
        raise BlockedComparison(f"Invalid filter on {table}: " + "; ".join(problems))
    # A null predicate result excludes the row, matching SQL WHERE semantics.
    return frame.where(reduce(and_, predicates))


def _leaves(child):
    if child.leaves:
        return child.leaves
    keep = {"table", "version", "snapshot_pinned", "as_of", "filters"}
    return [{k: v for k, v in child.metadata().items() if k in keep}]
