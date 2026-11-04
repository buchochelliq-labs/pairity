"""One validated configuration model shared by every frontend and codec."""

import hashlib
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Name = Annotated[str, Field(min_length=1, pattern=r"^[^\x00-\x1f]+$")]
TableName = Annotated[str, Field(pattern=r"^[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*$")]
NonNegative = Annotated[int, Field(ge=0)]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, validate_default=True)


class ColumnMapping(Model):
    baseline: Name
    candidate: Name


class ColumnRule(Model):
    absolute_tolerance: float = Field(default=0, ge=0, allow_inf_nan=False)
    relative_tolerance: float = Field(default=0, ge=0, allow_inf_nan=False)


class ComparisonSettings(Model):
    schema_policy: Literal["strict"] = "strict"
    exclude_columns: list[Name] = Field(default_factory=list)
    column_rules: dict[Name, ColumnRule] = Field(default_factory=dict)
    max_changed_rows: NonNegative = 0
    max_missing_rows: NonNegative = 0
    max_extra_rows: NonNegative = 0
    allow_key_only: bool = False


class ComparisonOverrides(Model):
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
    baseline: TableName
    candidate: TableName
    keys: list[ColumnMapping] | None = Field(default=None, min_length=1)
    columns: list[ColumnMapping] = Field(default_factory=list)
    compare: ComparisonOverrides = Field(default_factory=ComparisonOverrides)
    tags: list[Name] = Field(default_factory=list)
    enabled: bool = True
    baseline_version: NonNegative | None = None
    candidate_version: NonNegative | None = None

    @model_validator(mode="after")
    def mappings_are_unique(self):
        validate_mapping((self.keys or []) + self.columns)
        return self


class ExecutionConfig(Model):
    max_parallel_pairs: int = Field(default=4, ge=1, le=64)


class SuiteConfig(Model):
    schema_version: Literal[1]
    suite: Name
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
    defaults = suite.defaults.model_dump()
    overrides = pair.compare.model_dump(exclude_none=True)
    rules = defaults["column_rules"] | overrides.pop("column_rules", {})
    return ComparisonSettings.model_validate(defaults | overrides | {"column_rules": rules})
