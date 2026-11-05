# Unity Catalog pair discovery

Spec: https://github.com/buchochelliq-labs/pairity/issues/50 (parent #10).

Goal: let an operator find two Delta tables, inspect declared keys, configure
renamed/composite key mappings, and validate metadata from the existing editor.

Architecture: a versioned discovery adapter uses caller-scoped SQL information
schema queries. The Spark-free workbench facade validates key mappings against
the same pure type-alignment policy used by Spark. Thin HTTP endpoints and a
React component feed the existing JSON/YAML draft, preview and revision flow.

Global constraints: preserve the verified deployment, literal Lakebase endpoint,
source templates, manual editing, config schema and caller authorization. Do not
scan source rows in the app. UC constraints do not establish data uniqueness.
Discovery is bounded and paginated; table and column names are never SQL values
interpolated from user input. Catalog identifiers follow the existing schema.

## Task 1: Discovery and shared type policy

Files: `discovery.py`, `type_alignment.py`, `contracts.py`, `spark_casts.py`, unit tests.
Interfaces: CatalogDiscovery v1 supplies catalogs/schemas/tables pages and table
descriptions; pure type names supply scalar support and implicit cast decisions.

1. Add tests for pagination, bound search values, invalid identifiers, metadata
   visibility/readability, missing tables, declared keys and type alignment.
2. Run tests; expected: missing implementation fails.
3. Implement SQL adapter and extract the existing implicit cast policy unchanged.
4. Run focused tests; expected: all pass. Commit.

## Task 2: Facade and API validation

Files: `workbench.py`, `web/app.py`, `web/runtime.py`, unit tests.
Interfaces: discovery is request-scoped and optional. Inspect receives the full
suite document and pair ID to resolve inherited settings and explicit keys.

1. Add tests for missing/renamed/composite keys, unsupported types, inherited and
   column-specific cast policy, inaccessible tables, and authenticated routes.
2. Run tests; expected: new tests fail.
3. Implement metadata inspection and inject discovery using the caller SQL query.
4. Run focused tests; expected: all pass. Commit.

## Task 3: Guided UX

Files: `app/ui/src/CatalogPairEditor.tsx`, component tests, `main.tsx`, CSS/docs.
Interfaces: selecting sources and applying keys updates the existing draft
atomically and preserves unrelated settings; inspection uses the full document.

1. Add component tests for browsing, empty/error states, pagination, composite
   mappings, config preservation and stale async responses.
2. Run tests; expected: new tests fail.
3. Implement catalog/schema/table selectors and metadata validation with explicit
   application of key suggestions. Derived sources keep the manual source editor.
4. Run UI tests/build and Python suite; expected: all pass. Commit.

## Task 4: Qualification and review

Update deployment evidence and user documentation. Review the complete branch,
fix substantive findings with regression tests, and publish a reviewable PR.
Keep #50 open for live Databricks and permission qualification.

Review focus: request-scoped identity, SQL identifier/value boundaries, stale
responses when a pair changes, advanced source preservation, no silent metadata
truncation, consistency between metadata casts and engine casts, and row-level
checks remaining authoritative at execution time.
