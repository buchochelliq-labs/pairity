"""Pure, bounded source expansion. Templates substitute typed sources, never code."""

from pairity.config import (
    DeduplicateSource,
    ParameterSource,
    SqlSource,
    TableSource,
    TemplateSource,
    UnionSource,
    ValueRef,
)


def _value(value, values):
    if isinstance(value, ValueRef):
        if values is None or value.name not in values:
            raise ValueError(f"Unbound template value: {value.name}")
        return values[value.name]
    return value


def expand_source(source, templates, *, bindings=None, values=None, stack=(), depth=0):
    if depth > 32:
        raise ValueError("Source/template nesting exceeds 32 levels")
    if isinstance(source, str):
        return TableSource(table=source)
    if isinstance(source, TableSource):
        return source.model_copy(deep=True)
    if isinstance(source, ParameterSource):
        if bindings is None or source.name not in bindings:
            raise ValueError(f"Unbound source parameter: {source.name}")
        return bindings[source.name].model_copy(deep=True)
    if isinstance(source, TemplateSource):
        if source.name not in templates:
            raise ValueError(f"Unknown source template: {source.name}")
        if source.name in stack:
            raise ValueError(f"Cyclic source template: {source.name}")
        template = templates[source.name]
        if set(source.inputs) != set(template.parameters):
            raise ValueError(f"Template {source.name} requires exactly {template.parameters}")
        if set(source.values) != set(template.values):
            raise ValueError(f"Template {source.name} requires exactly values {template.values}")
        inputs = {
            name: expand_source(
                value, templates, bindings=bindings, values=values, stack=stack, depth=depth + 1
            )
            for name, value in source.inputs.items()
        }
        bound = {name: _value(value, values) for name, value in source.values.items()}
        return expand_source(
            template.source,
            templates,
            bindings=inputs,
            values=bound,
            stack=(*stack, source.name),
            depth=depth + 1,
        )
    kwargs = dict(bindings=bindings, values=values, stack=stack, depth=depth + 1)
    if isinstance(source, UnionSource):
        return UnionSource(
            kind="union",
            sources=[expand_source(child, templates, **kwargs) for child in source.sources],
        )
    if isinstance(source, DeduplicateSource):
        return DeduplicateSource(
            kind="deduplicate", source=expand_source(source.source, templates, **kwargs)
        )
    if isinstance(source, SqlSource):
        return SqlSource(
            kind="sql",
            query=source.query,
            inputs={
                name: expand_source(child, templates, **kwargs)
                for name, child in source.inputs.items()
            },
            parameters={name: _value(v, values) for name, v in source.parameters.items()},
        )
    raise ValueError("Unknown source kind")


def child_sources(source):
    """Direct children of an expanded source, in the order their leaves are recorded."""
    if isinstance(source, UnionSource):
        return list(source.sources)
    if isinstance(source, DeduplicateSource):
        return [source.source]
    if isinstance(source, SqlSource):
        return list(source.inputs.values())
    if isinstance(source, (str, TableSource)):
        return []
    raise ValueError("Source must be expanded before traversal")


def replace_children(source, children):
    """Return a copy of an expanded source with its children replaced in order."""
    children = list(children)
    if isinstance(source, UnionSource):
        return source.model_copy(update={"sources": children})
    if isinstance(source, DeduplicateSource):
        (child,) = children
        return source.model_copy(update={"source": child})
    if isinstance(source, SqlSource):
        return source.model_copy(update={"inputs": dict(zip(source.inputs, children, strict=True))})
    return source


def leaf_tables(source):
    """Every TableSource leaf of an expanded source, depth first, in recorded order."""
    if isinstance(source, str):
        return [TableSource(table=source)]
    if isinstance(source, TableSource):
        return [source]
    return [leaf for child in child_sources(source) for leaf in leaf_tables(child)]
