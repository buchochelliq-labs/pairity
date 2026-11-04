"""Source operations over pinned leaf frames, using public DataFrame APIs."""

from pairity.config import DeduplicateSource, TableSource, UnionSource
from pairity.results import BlockedComparison, TableInput


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
        if isinstance(source, DeduplicateSource):
            child = self.read(source.source, reader, cache)
            frame = child.frame.distinct()
            children = [child]
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
            leaves=[leaf for child in children for leaf in (child.leaves or [child.metadata()])],
        )
