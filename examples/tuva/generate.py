"""Deterministic synthetic Tuva-shaped fixtures; no upstream code is executed."""

import copy
import csv
import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parent
COMMON = {
    "person_id": "string",
    "member_id": "string",
    "payer": "string",
    "plan": "string",
    "data_source": "string",
}
MONTH = {"member_month_id": "string", **COMMON, "year_month": "string"}
SCHEMAS = {
    "core__member_month": MONTH,
    "core__medical_claim": {
        "medical_claim_id": "string",
        "claim_id": "string",
        "claim_line_number": "int",
        **COMMON,
        "claim_start_date": "date",
        "paid_date": "date",
        "paid_amount": "decimal(18,2)",
        "allowed_amount": "decimal(18,2)",
    },
    "core__pharmacy_claim": {
        "pharmacy_claim_id": "string",
        "claim_id": "string",
        "claim_line_number": "int",
        **COMMON,
        "dispensing_date": "date",
        "paid_date": "date",
        "paid_amount": "decimal(18,2)",
        "allowed_amount": "decimal(18,2)",
    },
    "core__cost": {
        **MONTH,
        "total_paid": "decimal(18,2)",
        "medical_paid": "decimal(18,2)",
        "pharmacy_paid": "decimal(18,2)",
        "total_allowed": "decimal(18,2)",
    },
}
KEYS = {
    "core__member_month": "member_month_id",
    "core__cost": "member_month_id",
    "core__medical_claim": "medical_claim_id",
    "core__pharmacy_claim": "pharmacy_claim_id",
}
EXPECTED = {
    "core__member_month": (104, 102, 102, 0, 2, 0),
    "core__medical_claim": (100, 100, 99, 3, 1, 1),
    "core__pharmacy_claim": (100, 100, 100, 0, 0, 0),
    "core__cost": (104, 104, 104, 3, 0, 0),
}
COUNT_NAMES = (
    "baseline_rows",
    "candidate_rows",
    "matched_rows",
    "changed_rows",
    "missing_rows",
    "extra_rows",
)


def datasets():
    baseline = {table: [] for table in SCHEMAS}
    for month in range(1, 5):
        for person in range(1, 27):
            identity = {
                "person_id": f"SYN-P{person:03}",
                "member_id": f"SYN-M{person:03}",
                "payer": "synthetic_commercial",
                "plan": "synthetic_standard",
                "data_source": "medicare_lds_demo" if person == 26 else "synthetic_tuva",
            }
            # A plan change leaves person_id stable but changes member_id.
            if person == 1 and month >= 3:
                identity.update(member_id="SYN-M101", plan="synthetic_plus")
            mid = f"MM-{month:02}-{person:03}"
            mm = {"member_month_id": mid, **identity, "year_month": f"2026{month:02}"}
            baseline["core__member_month"].append(mm)
            medical, pharmacy = (0, 0) if person == 25 else (100 + person, 20 + person)
            baseline["core__cost"].append(
                {
                    **mm,
                    "medical_paid": f"{medical}.00",
                    "pharmacy_paid": f"{pharmacy}.00",
                    "total_paid": f"{medical + pharmacy}.00",
                    "total_allowed": f"{medical + pharmacy + (20 if medical else 0)}.00",
                }
            )
            if person == 25:  # Eligible without claims: still in PMPM denominator.
                continue
            for kind, amount, day in [("medical", medical, 10), ("pharmacy", pharmacy, 11)]:
                cid = f"{kind.upper()}-{month:02}-{person:03}"
                date_column = "claim_start_date" if kind == "medical" else "dispensing_date"
                baseline[f"core__{kind}_claim"].append(
                    {
                        f"{kind}_claim_id": cid,
                        "claim_id": cid,
                        "claim_line_number": 1,
                        **identity,
                        date_column: f"2026-{month:02}-{day}",
                        "paid_date": f"2026-{month:02}-28",
                        "paid_amount": f"{amount}.00",
                        "allowed_amount": f"{amount + 10}.00",
                    }
                )
    changed = copy.deepcopy(baseline)
    changed["core__member_month"] = [
        r
        for r in changed["core__member_month"]
        if r["member_month_id"] not in {"MM-04-024", "MM-04-025"}
    ]
    claims = changed["core__medical_claim"]
    claims[0]["paid_amount"] = "126.00"
    claims[0]["allowed_amount"] = "136.00"
    claims[2]["paid_date"] = None
    claims[3]["claim_start_date"] = "2026-01-12"
    changed["core__medical_claim"] = [
        r for r in claims if r["medical_claim_id"] != "MEDICAL-01-002"
    ]
    extra = copy.deepcopy(claims[4])
    extra.update(
        medical_claim_id="MEDICAL-EXTRA-005",
        claim_id="MEDICAL-EXTRA-005",
        paid_amount="75.00",
        allowed_amount="85.00",
    )
    changed["core__medical_claim"].append(extra)
    costs = changed["core__cost"]
    costs[0].update(medical_paid="126.00", total_paid="147.00", total_allowed="167.00")
    costs[1].update(medical_paid="0.00", total_paid="22.00", total_allowed="32.00")
    costs[4].update(medical_paid="180.00", total_paid="205.00", total_allowed="235.00")
    return baseline, changed


def suite(catalog="workspace", schema="pairity_test", *, identical=False):
    candidate_variant = "baseline" if identical else "changed"
    return {
        "schema_version": 1,
        "suite": "tuva-identical" if identical else "tuva-regression",
        "execution": {"max_parallel_pairs": 2},
        "pairs": [
            {
                "id": table,
                "baseline": f"{catalog}.{schema}.tuva_baseline_{table}",
                "candidate": f"{catalog}.{schema}.tuva_{candidate_variant}_{table}",
                "keys": [{"baseline": KEYS[table], "candidate": KEYS[table]}],
            }
            for table in SCHEMAS
        ],
    }


def generate(root=ROOT):
    root = Path(root)
    for name, data in zip(("baseline", "changed"), datasets(), strict=True):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        for table, rows in data.items():
            (directory / f"{table}.jsonl").write_text(
                "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows)
            )
            with (directory / f"{table}.csv").open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=SCHEMAS[table], lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
    (root / "schemas.json").write_text(json.dumps(SCHEMAS, indent=2) + "\n")
    (root / "expected.json").write_text(
        json.dumps(
            {
                table: dict(zip(COUNT_NAMES, counts, strict=True))
                for table, counts in EXPECTED.items()
            },
            indent=2,
        )
        + "\n"
    )
    for identical in (False, True):
        config = suite(identical=identical)
        for ext in ("json", "yaml"):
            text = (
                json.dumps(config, indent=2)
                if ext == "json"
                else yaml.safe_dump(config, sort_keys=False)
            )
            (root / f"{config['suite']}.{ext}").write_text(text.rstrip() + "\n")
    checksums = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.glob("*/*"))
        if p.suffix in {".jsonl", ".csv"}
    }
    (root / "checksums.json").write_text(json.dumps(checksums, indent=2) + "\n")


if __name__ == "__main__":
    generate()
