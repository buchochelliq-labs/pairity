"""Pure, bounded source expansion. Templates substitute typed sources, never code."""

from pairity.config import (
    DeduplicateSource,
    ParameterSource,
    TableSource,
    TemplateSource,
    UnionSource,
)


def expand_source(source, templates, *, bindings=None, stack=(), depth=0):
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
        inputs = {
            name: expand_source(value, templates, bindings=bindings, stack=stack, depth=depth + 1)
            for name, value in source.inputs.items()
        }
        return expand_source(
            template.source,
            templates,
            bindings=inputs,
            stack=(*stack, source.name),
            depth=depth + 1,
        )
    kwargs = dict(bindings=bindings, stack=stack, depth=depth + 1)
    if isinstance(source, UnionSource):
        return UnionSource(
            kind="union",
            sources=[expand_source(child, templates, **kwargs) for child in source.sources],
        )
    if isinstance(source, DeduplicateSource):
        return DeduplicateSource(
            kind="deduplicate", source=expand_source(source.source, templates, **kwargs)
        )
    raise ValueError("Unknown source kind")
