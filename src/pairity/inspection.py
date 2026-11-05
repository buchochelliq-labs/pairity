"""Metadata-only pair preflight; data checks remain the engine's responsibility."""

from pairity.config import PairConfig, TableSource, resolve_settings
from pairity.metadata import MissingKeys, resolve_keys
from pairity.type_alignment import canonical_type, implicit_target_name, scalar_type


class _DeclaredKeys:
    api_version = 1

    def __init__(self, descriptions):
        self.keys = {d["table"]: d["primary_key"] for d in descriptions if d}

    def primary_key(self, table):
        return self.keys.get(table)


def inspect_pair(config, pair_id, discovery):
    pair = next((p for p in config.pairs if p.id == pair_id), None)
    if pair is None:
        raise ValueError("Pair ID does not exist in this configuration")
    settings = resolve_settings(config, pair)
    issues, casts = [], []
    result = {
        "pair_id": pair_id,
        "baseline": None,
        "candidate": None,
        "key_origin": "explicit" if pair.keys else "declared",
        "settings": settings.model_dump(),
        "keys": [],
        "issues": issues,
        "casts": casts,
        "warnings": [
            "Metadata checks do not prove key uniqueness, absence of nulls or lossless casts. "
            "The comparison job checks the data before comparing rows.",
            "This check uses current table metadata; "
            "pinned snapshot schemas are checked by the job.",
        ],
    }
    for side in ("baseline", "candidate"):
        source = getattr(pair, side)
        table = (
            source
            if isinstance(source, str)
            else (source.table if isinstance(source, TableSource) else None)
        )
        if table is None:
            issues.append(f"{side}: use the source editor and explicit keys for composed sources")
            continue
        try:
            result[side] = discovery.describe(table)
        except ValueError as exc:
            issues.append(f"{side}: {exc}")
        except Exception:
            issues.append(
                f"{side}: metadata could not be read; check the table and SELECT permissions"
            )
    if result["baseline"] and result["candidate"]:
        try:
            keys = resolve_keys(pair, _DeclaredKeys([result["baseline"], result["candidate"]]))
        except MissingKeys as exc:
            keys = []
            issues.append(str(exc))
        result["keys"] = [key.model_dump() for key in keys]
        if keys:
            try:
                # Reuse the shared cross-key/value-mapping constraint before an
                # inferred key suggestion can be applied to the configuration.
                PairConfig.model_validate(
                    pair.model_dump(exclude_none=True) | {"keys": result["keys"]}
                )
            except ValueError:
                issues.append(
                    "Primary keys overlap configured value-column mappings; remove the "
                    "overlapping entries from columns in the source editor before applying keys"
                )
        left = {c["name"]: c["type"] for c in result["baseline"]["columns"]}
        right = {c["name"]: c["type"] for c in result["candidate"]["columns"]}
        for key in keys:
            label = f"{key.baseline} → {key.candidate}"
            if key.baseline not in left or key.candidate not in right:
                issues.append(f"{label}: primary key column is missing")
                continue
            if {key.baseline, key.candidate} & set(settings.exclude_columns):
                issues.append(f"{label}: primary key columns cannot be excluded")
            a, b = left[key.baseline], right[key.candidate]
            if not scalar_type(a) or not scalar_type(b):
                issues.append(f"{label}: unsupported primary key type ({a}, {b})")
                continue
            rule = settings.column_rules.get(key.baseline)
            implicit = (
                settings.implicit_casts
                if rule is None or rule.implicit_casts is None
                else (rule.implicit_casts)
            )
            explicit = rule.compare_as if rule else None
            target = explicit or (implicit_target_name(a, b) if implicit else None)
            if target:
                casts.append(
                    {
                        **key.model_dump(),
                        "baseline_type": a,
                        "candidate_type": b,
                        "compare_as": target,
                        "mode": "explicit" if explicit else "implicit",
                    }
                )
            elif canonical_type(a) != canonical_type(b):
                issues.append(
                    f"{label}: incompatible key types ({a}, {b}); configure compare_as "
                    "or enable a supported implicit cast"
                )
    result["valid"] = not issues
    return result
