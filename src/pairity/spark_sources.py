"""Source operations over pinned leaf frames, using public DataFrame APIs."""

from uuid import uuid4

from pairity.config import DeduplicateSource, SqlSource, TableSource, UnionSource
from pairity.results import BlockedComparison, TableInput
from pairity.sql_guard import check_query, function_calls, query_hash, render


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
            key = (source.table, source.version)
            if key not in cache:
                cache[key] = reader.read(*key)
            return cache[key]
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


def _leaves(child):
    if child.leaves:
        return child.leaves
    return [
        {k: v for k, v in child.metadata().items() if k in {"table", "version", "snapshot_pinned"}}
    ]
