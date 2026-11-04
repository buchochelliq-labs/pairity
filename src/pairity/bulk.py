"""Pure bulk transforms: validate the whole new suite before any storage write."""

from copy import deepcopy

from pairity.config import PairConfig, SuiteConfig


def _merge(before: dict, patch: dict) -> dict:
    result = deepcopy(before)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def select_pairs(config: SuiteConfig, ids=None, tags=None) -> list[PairConfig]:
    if ids is not None and tags is not None:
        raise ValueError("Select pairs by IDs or tags, not both")
    if ids is not None:
        known = {p.id for p in config.pairs}
        if not ids or len(ids) != len(set(ids)) or set(ids) - known:
            raise ValueError("Pair selection contains empty, duplicate or unknown IDs")
        return [p for p in config.pairs if p.id in ids]
    if tags is not None:
        known = {tag for p in config.pairs for tag in p.tags}
        if not tags or set(tags) - known:
            raise ValueError("Tag selection contains empty or unknown tags")
        return [p for p in config.pairs if set(tags).intersection(p.tags)]
    return list(config.pairs)


def patch_pairs(config: SuiteConfig, patch: dict, *, ids=None, tags=None) -> SuiteConfig:
    if ids is None and tags is None:
        raise ValueError("Bulk patches require an explicit ID or tag selection")
    if not isinstance(patch, dict) or not patch or "id" in patch:
        raise ValueError("Provide a nonempty patch; pair IDs cannot be changed")
    selected = {p.id for p in select_pairs(config, ids, tags)}
    data = config.model_dump(exclude_none=True)
    data["pairs"] = [_merge(p, patch) if p["id"] in selected else p for p in data["pairs"]]
    return SuiteConfig.model_validate(data)


def upsert_pairs(config: SuiteConfig, pairs: list[dict]) -> SuiteConfig:
    incoming = [PairConfig.model_validate(p) for p in pairs]
    if not incoming:
        raise ValueError("Upsert requires at least one pair")
    ids = [p.id for p in incoming]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate pair IDs in upsert")
    merged = {p.id: p.model_dump(exclude_none=True) for p in config.pairs}
    merged.update({p.id: p.model_dump(exclude_none=True) for p in incoming})
    data = config.model_dump(exclude_none=True) | {"pairs": list(merged.values())}
    return SuiteConfig.model_validate(data)


def preview_update(before: SuiteConfig, after: SuiteConfig) -> dict[str, list[str]]:
    old = {p.id: p for p in before.pairs}
    new = {p.id: p for p in after.pairs}
    return {
        "added": [k for k in new if k not in old],
        "changed": [k for k in new if k in old and new[k] != old[k]],
        "unchanged": [k for k in new if k in old and new[k] == old[k]],
        "removed": [k for k in old if k not in new],
    }
