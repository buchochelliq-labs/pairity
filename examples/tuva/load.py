"""Load only the checked-in synthetic fixture into new Delta tables."""

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from pairity.config import SuiteConfig
from pairity.metadata import quote_table

ROOT = Path(__file__).parent


def frames(spark, root=ROOT):
    root = Path(root)
    schemas = json.loads((root / "schemas.json").read_text())
    checksums = json.loads((root / "checksums.json").read_text())
    result = {}
    for variant in ("baseline", "changed"):
        for table, columns in schemas.items():
            relative = f"{variant}/{table}.jsonl"
            raw = (root / relative).read_bytes()
            if hashlib.sha256(raw).hexdigest() != checksums[relative]:
                raise ValueError(f"Fixture checksum mismatch: {relative}")
            records = [json.loads(line) for line in raw.splitlines()]
            for row in records:
                if set(row) != set(columns):
                    raise ValueError(f"Fixture schema mismatch: {relative}")
                for name, dtype in columns.items():
                    if row[name] is None:
                        continue
                    if dtype == "date":
                        row[name] = date.fromisoformat(row[name])
                    elif dtype.startswith("decimal("):
                        row[name] = Decimal(row[name])
            ddl = ", ".join(f"`{name}` {dtype}" for name, dtype in columns.items())
            result[(variant, table)] = spark.createDataFrame(records, ddl)
    return result


def load_fixture(spark, catalog, schema, *, root=ROOT):
    """Requires an existing sandbox schema. Never overwrite existing tables."""
    root = Path(root)
    data = frames(spark, root)
    targets = {key: f"{catalog}.{schema}.tuva_{key[0]}_{key[1]}" for key in data}
    # Validate every identifier and refuse pre-existing targets before writing any table.
    quoted = {key: quote_table(name) for key, name in targets.items()}
    existing = [name for key, name in targets.items() if spark.catalog.tableExists(quoted[key])]
    if existing:
        raise ValueError(f"Fixture tables already exist; use a fresh sandbox schema: {existing}")
    for key, frame in data.items():
        frame.write.format("delta").mode("errorifexists").saveAsTable(quoted[key])
    configs = {}
    for name in ("tuva-regression", "tuva-identical"):
        document = json.loads((root / f"{name}.json").read_text())
        for pair in document["pairs"]:
            table = pair["id"]
            pair["baseline"] = targets[("baseline", table)]
            pair["candidate"] = targets[
                ("baseline" if name == "tuva-identical" else "changed", table)
            ]
        configs[name] = SuiteConfig.model_validate(document)
    return configs
