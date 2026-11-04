# Tuva-shaped regression datasets

Two deterministic synthetic datasets, each with four table files, based on the
Tuva models referenced by Open Data Suite Ontology. `baseline/` is the unchanged
reference; `changed/` is a copy with explicit mutations. Each has JSON Lines for
typed loading and CSV for inspection. No patient records are included.

ODS Ontology's Tuva fixture contains a compiled dbt manifest and a fictional
team's ontology; it does not populate tables. These are **new synthetic rows using
a subset of Tuva columns**, not an unchanged upstream data extract, complete Tuva
schema, or output of running Tuva/dbt. Monetary values use decimal(18,2), dates use
Spark DATE, and keys are explicit. Loader schemas are in `schemas.json`.

## Provenance

- ODS Ontology: https://github.com/buchochelliq-labs/open-data-suite-ontology/tree/main/fixtures/tuva
- ODS fixture tree inspected: `c936f65ec1f7e3451ddf7125e92c5a985d774ccc`.
- Its `build.sh` pins Tuva commit `9ab0d1c3e4a920f5acebdd5f9a2f456fb20fe7e2`.
- Column/grain reference: https://github.com/tuva-health/tuva/blob/9ab0d1c3e4a920f5acebdd5f9a2f456fb20fe7e2/models/core/core_models.yml
- Tuva and ODS Ontology are Apache-2.0 projects. No upstream executable code or
  model descriptions are bundled or executed by this fixture generator.

The synthetic scenario follows the ontology's fictional Riverbend definitions:
paid medical plus pharmacy amounts for PMPM, person_id distinct from member_id,
coverage without claims, and exclusion of `medicare_lds_demo` from reports.
The raw regression comparison intentionally includes demo rows on both sides.
Dates are fixed to January–April 2026; this fixture does not implement a rolling
claims-runout report or the full Tuva transformation pipeline.

## Known differences

| Table | Baseline | Changed | Matched keys | Changed rows | Removed | Added |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| core__member_month | 104 | 102 | 102 | 0 | 2 | 0 |
| core__medical_claim | 100 | 100 | 99 | 3 | 1 | 1 |
| core__pharmacy_claim | 100 | 100 | 100 | 0 | 0 | 0 |
| core__cost | 104 | 104 | 104 | 3 | 0 | 0 |

The baseline covers 25 commercial people plus one demo person in each of four
months. Person SYN-P025 has coverage but no claims. SYN-P001 changes plan/member
ID in March without changing person_id.

Changes are deterministic:

- Remove `MM-04-024` and `MM-04-025`: exactly 8% of the 25 commercial April member
  months, modelling ontology issue RB-204. Cost rows are retained deliberately,
  exposing the denominator mismatch.
- `MEDICAL-01-001`: paid 101 → 126 and allowed 111 → 136.
- `MEDICAL-01-003`: paid_date becomes null.
- `MEDICAL-01-004`: claim_start_date changes from January 10 to January 12.
- Remove `MEDICAL-01-002`; add `MEDICAL-EXTRA-005` (paid 75, allowed 85).
- Adjust corresponding January cost amounts for persons 001, 002 and 005.
- Pharmacy claims remain identical as a positive control.

`expected.json` is the machine-readable oracle. These counts describe physical
row differences; a domain-level PMPM report is outside this fixture's scope.

## Regenerate and load

```bash
python examples/tuva/generate.py
```

Run on a Databricks notebook with Pairity installed and this folder uploaded
alongside it (the deployment bundle sync includes the example files):

```python
import sys
from pairity.serialization import dump_config

fixture_root = "/Workspace/Shared/pairity/examples/tuva"  # Actual deployed path
sys.path.insert(0, fixture_root)
from load import load_fixture

configs = load_fixture(spark, "workspace", "pairity_test", root=fixture_root)
# Save each returned config in an existing UC Volume for the serverless Job.
for name, config in configs.items():
    with open(f"/Volumes/workspace/pairity_test/artifacts/{name}.yaml", "w") as out:
        out.write(dump_config(config, "yaml"))
```

Pre-create the sandbox schema and managed Volume; see
[required Databricks access](../../docs/databricks-test-access.md). The loader
creates eight Delta tables named `tuva_baseline_core__...` / `tuva_changed_core__...`
and refuses to overwrite any existing target. A mid-load infrastructure failure
can leave a partial fixture: use a fresh dedicated schema or explicitly remove
only those fixture tables before retrying. It never creates catalogs, grants
permissions or deletes data.

Submit `tuva-identical.yaml` for four passing self-comparisons, then
`tuva-regression.yaml` for three failures and one pass. A failed regression Job is
expected, but the test harness must also verify all four saved pair outcomes and
the exact counts above. Both JSON and YAML configurations are included. The
loader returns configs with the actual chosen catalog/schema, rather than the
example defaults.
