"""One validated configuration model shared by every frontend and codec."""

import hashlib
import json
import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Name = Annotated[str, Field(min_length=1, pattern=r"^[^\x00-\x1f]+$")]
TableName = Annotated[str, Field(pattern=r"^[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*$")]
NonNegative = Annotated[int, Field(ge=0)]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class TableSource(Model):
    kind: Literal["table"] = "table"
    table: TableName
    version: NonNegative | None = None


class UnionSource(Model):
    kind: Literal["union"]
    sources: list["SourceSpec"] = Field(min_length=2)


class DeduplicateSource(Model):
    kind: Literal["deduplicate"]
    source: "SourceSpec"


class TemplateSource(Model):
    kind: Literal["template"]
    name: Name
    inputs: dict[Name, "SourceSpec"] = Field(default_factory=dict)
    values: dict[str, "SqlScalar | ValueRef"] = Field(default_factory=dict)


class ParameterSource(Model):
    kind: Literal["parameter"]
    name: Name


SqlIdentifier = Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")]
SqlScalar = str | bool | int | Annotated[float, Field(allow_inf_nan=False)]


class ValueRef(Model):
    """A template value parameter; bound by the referencing template source."""

    kind: Literal["value"]
    name: SqlIdentifier


class SqlSource(Model):
    """A read-only query over declared inputs; see pairity.sql_guard for the subset."""

    kind: Literal["sql"]
    query: Annotated[str, Field(min_length=1, max_length=20_000)]
    inputs: dict[SqlIdentifier, "SourceSpec"] = Field(min_length=1)
    parameters: dict[SqlIdentifier, SqlScalar | ValueRef] = Field(default_factory=dict)

    @model_validator(mode="after")
    def query_is_read_only_and_bound(self):
        from pairity.sql_guard import check_parameter_name, check_query

        checked = check_query(self.query)
        if set(checked.inputs) != set(self.inputs):
            raise ValueError(
                f"SQL placeholders {sorted(checked.inputs)} must match inputs {sorted(self.inputs)}"
            )
        if set(checked.parameters) != set(self.parameters):
            raise ValueError(
                f"SQL parameter markers {sorted(checked.parameters)} must match "
                f"parameters {sorted(self.parameters)}"
            )
        for name in self.parameters:
            check_parameter_name(name)
        return self


Source = Annotated[
    TableSource | UnionSource | DeduplicateSource | TemplateSource | ParameterSource | SqlSource,
    Field(discriminator="kind"),
]
SourceSpec = TableName | Source
for _model in (UnionSource, DeduplicateSource, TemplateSource, SqlSource):
    _model.model_rebuild()


class SourceTemplate(Model):
    parameters: list[Name] = Field(default_factory=list)
    values: list[SqlIdentifier] = Field(default_factory=list)
    source: SourceSpec

    @model_validator(mode="after")
    def unique_parameters(self):
        if len(self.parameters) != len(set(self.parameters)):
            raise ValueError("Template parameters must be unique")
        if len(self.values) != len(set(self.values)):
            raise ValueError("Template values must be unique")
        return self


CompareType = Annotated[
    str,
    Field(
        pattern=r"^(string|boolean|byte|short|int|long|float|double|date|timestamp|timestamp_ntz|decimal\([0-9]+,[0-9]+\))$"
    ),
]


class ColumnMapping(Model):
    baseline: Name
    candidate: Name


class ColumnRule(Model):
    compare_as: CompareType | None = None
    implicit_casts: bool | None = None
    null_safe: bool | None = None
    comparator: Name | None = None
    array_order: Literal["ordered", "unordered"] | None = None

    @field_validator("compare_as")
    @classmethod
    def valid_decimal(cls, value):
        if value and value.startswith("decimal"):
            precision, scale = map(int, re.findall(r"[0-9]+", value))
            if not 1 <= precision <= 38 or not 0 <= scale <= precision:
                raise ValueError(
                    "decimal requires 1 <= precision <= 38 and 0 <= scale <= precision"
                )
        return value

    absolute_tolerance: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    relative_tolerance: float | None = Field(default=None, ge=0, allow_inf_nan=False)


class ComparisonSettings(Model):
    column_policy: Literal["strict", "common"] = "strict"
    implicit_casts: bool = True
    null_safe: bool = True
    schema_policy: Literal["strict"] = "strict"
    exclude_columns: list[Name] = Field(default_factory=list)
    column_rules: dict[Name, ColumnRule] = Field(default_factory=dict)
    max_changed_rows: NonNegative = 0
    max_missing_rows: NonNegative = 0
    max_extra_rows: NonNegative = 0
    allow_key_only: bool = False


class ComparisonOverrides(Model):
    column_policy: Literal["strict", "common"] | None = None
    implicit_casts: bool | None = None
    null_safe: bool | None = None
    schema_policy: Literal["strict"] | None = None
    exclude_columns: list[Name] | None = None
    column_rules: dict[Name, ColumnRule] | None = None
    max_changed_rows: NonNegative | None = None
    max_missing_rows: NonNegative | None = None
    max_extra_rows: NonNegative | None = None
    allow_key_only: bool | None = None

    @model_validator(mode="before")
    @classmethod
    def no_explicit_nulls(cls, value):
        if isinstance(value, dict) and any(v is None for v in value.values()):
            raise ValueError("Omit an override to inherit it; explicit null is not a setting")
        return value


def validate_mapping(mappings: list[ColumnMapping]) -> None:
    for side in ("baseline", "candidate"):
        names = [getattr(m, side) for m in mappings]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate {side} columns in mapping")


class PairConfig(Model):
    id: Name
    baseline: SourceSpec
    candidate: SourceSpec
    keys: list[ColumnMapping] | None = Field(default=None, min_length=1)
    columns: list[ColumnMapping] = Field(default_factory=list)
    compare: ComparisonOverrides = Field(default_factory=ComparisonOverrides)
    tags: list[Name] = Field(default_factory=list)
    enabled: bool = True
    baseline_version: NonNegative | None = None
    candidate_version: NonNegative | None = None

    @model_validator(mode="after")
    def mappings_are_unique(self):
        for side in ("baseline", "candidate"):
            if (
                not isinstance(getattr(self, side), str)
                and getattr(self, f"{side}_version") is not None
            ):
                raise ValueError("Specify version inside a table source, not on a composed source")
        validate_mapping((self.keys or []) + self.columns)
        return self


class ExecutionConfig(Model):
    max_parallel_pairs: int = Field(default=4, ge=1, le=64)


class SuiteConfig(Model):
    schema_version: Literal[1]
    suite: Name
    templates: dict[Name, SourceTemplate] = Field(default_factory=dict)
    defaults: ComparisonSettings = Field(default_factory=ComparisonSettings)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    pairs: list[PairConfig] = Field(min_length=1)

    @field_validator("schema_version", mode="before")
    @classmethod
    def integer_version(cls, value):
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value

    @model_validator(mode="after")
    def unique_pair_ids(self):
        ids = [p.id for p in self.pairs]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate pair IDs")
        from pairity.sources import expand_source

        for name, template in self.templates.items():
            expand_source(
                TemplateSource(
                    kind="template",
                    name=name,
                    inputs={p: "pairity.validation.placeholder" for p in template.parameters},
                    values={v: "placeholder" for v in template.values},
                ),
                self.templates,
            )
        for pair in self.pairs:
            expand_source(pair.baseline, self.templates)
            expand_source(pair.candidate, self.templates)
        return self

    @property
    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.model_dump(exclude_none=True),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


def resolve_settings(suite: SuiteConfig, pair: PairConfig) -> ComparisonSettings:
    defaults = suite.defaults.model_dump(exclude_none=True)
    overrides = pair.compare.model_dump(exclude_none=True)
    rules = defaults["column_rules"].copy()
    for name, patch in overrides.pop("column_rules", {}).items():
        rules[name] = rules.get(name, {}) | patch
    return ComparisonSettings.model_validate(defaults | overrides | {"column_rules": rules})
