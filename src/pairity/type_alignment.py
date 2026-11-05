"""Spark-free scalar type policy shared by execution and metadata preflight."""

import re

INTEGRAL_DIGITS = {"byte": 3, "short": 5, "int": 10, "long": 20}
FLOATING = {"float", "double"}
ALIASES = {"tinyint": "byte", "smallint": "short", "integer": "int", "bigint": "long"}


def canonical_type(name: str) -> str:
    name = re.sub(r"\s+", "", name.lower())
    return ALIASES.get(name, name)


def _decimal_shape(name):
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", name)
    if match:
        precision, scale = map(int, match.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return precision - scale, scale
    if name in INTEGRAL_DIGITS:
        return INTEGRAL_DIGITS[name], 0
    return None


def scalar_type(name: str) -> bool:
    name = canonical_type(name)
    return bool(_decimal_shape(name)) or name in {
        *FLOATING,
        "string",
        "boolean",
        "binary",
        "date",
        "timestamp",
        "timestamp_ntz",
        "void",
    }


def implicit_target_name(left: str, right: str) -> str | None:
    a, b = canonical_type(left), canonical_type(right)
    if a == b:
        return None
    da, db = _decimal_shape(a), _decimal_shape(b)
    if a == "void":
        return (
            re.sub(r"\s+", "", right.lower())
            if db or b in {*FLOATING, "string", "boolean", "date"}
            else None
        )
    if b == "void":
        return implicit_target_name(right, left)
    if a == "string":
        if b in INTEGRAL_DIGITS:
            return "decimal(38,0)"
        if db:
            return f"decimal(38,{db[1]})"
        return "double" if b in FLOATING else None
    if b == "string":
        return implicit_target_name(right, left)
    if (da or a in FLOATING) and (db or b in FLOATING) and (a in FLOATING or b in FLOATING):
        return "double"
    if da and db:
        if a in INTEGRAL_DIGITS and b in INTEGRAL_DIGITS:
            return "long"
        scale = max(da[1], db[1])
        precision = max(da[0], db[0]) + scale
        return f"decimal({precision},{scale})" if precision <= 38 else None
    return None
