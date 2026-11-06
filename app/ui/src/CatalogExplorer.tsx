import React, { useEffect, useRef, useState } from "react";
import { api, Pair } from "./api";
import { useMetadataPage } from "./useMetadataPage";

type Side = "baseline" | "candidate";
type Scope = { catalog?: string; schema?: string };
type Table = { catalog: string; schema: string; name: string; full_name: string };
export type TableDescription = {
  table: string; primary_key: string[];
  columns: { name: string; type: string; nullable: boolean }[];
};
export const sourceTable = (source: any): string | null =>
  typeof source === "string" ? source : source?.kind === "table" ? source.table : null;
const isTableSource = (source: any): boolean => typeof source === "string" || source?.kind === "table";

function PageControls({ page, noun }: { page: ReturnType<typeof useMetadataPage>; noun: string }) {
  return <>
    {page.loading && <p className="muted" role="status">Loading {noun}…</p>}
    {page.error && <div className="catalog-error" role="alert"><p>{page.error}</p><button onClick={page.retry}>Retry {noun}</button></div>}
    {page.next && !page.error && <button disabled={page.loading} onClick={page.more}>Load more {noun}</button>}
  </>;
}

function Schemas({ catalog, scope, onScope }: { catalog: string; scope: Scope; onScope: (scope: Scope) => void }) {
  const page = useMetadataPage(`/discovery/schemas?${new URLSearchParams({ catalog })}`);
  return <ul className="catalog-schemas">
    {page.items.map(item => <li key={item.name}>
      <button aria-label={`Schema ${catalog}.${item.name}`} aria-current={scope.catalog === catalog && scope.schema === item.name ? "location" : undefined}
        onClick={() => onScope({ catalog, schema: item.name })}><span aria-hidden="true">▤</span> {item.name}</button>
    </li>)}
    <li><PageControls page={page} noun={`schemas in ${catalog}`} /></li>
    {!page.loading && !page.error && !page.items.length && <li className="muted">No visible schemas.</li>}
  </ul>;
}

function TableDetails({ table, side, canSelect, onSelect }: {
  table: string; side: Side; canSelect: boolean; onSelect: (side: Side, table: string) => void;
}) {
  const [info, setInfo] = useState<TableDescription | null>(null);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    let stopped = false;
    const controller = new AbortController();
    setInfo(null); setError("");
    api<TableDescription>(`/discovery/table?${new URLSearchParams({ table })}`, undefined, "GET", controller.signal)
      .then(value => { if (!stopped) setInfo(value); })
      .catch(e => { if (!stopped) setError(e.message); });
    return () => { stopped = true; controller.abort(); };
  }, [table, retry]);
  return <section className="table-inspector" aria-label="Table details">
    <p className="eyebrow">TABLE DETAILS</p><h3>{table}</h3>
    {!info && !error && <p role="status">Checking table access and loading columns…</p>}
    {error && <div role="alert"><p>{error}</p><button onClick={() => setRetry(r => r + 1)}>Retry table details</button></div>}
    {info && <>
      <p><strong>Declared primary key:</strong> {info.primary_key.join(", ") || "None declared"}</p>
      <p className="muted">Metadata only, not a uniqueness check. No data rows are loaded.</p>
      <button disabled={!canSelect} onClick={() => onSelect(side, table)}>Use as {side}</button>
      <div className="table-columns"><table aria-label="Table columns"><thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead>
        <tbody>{info.columns.map(column => <tr key={column.name}><td>{column.name}{info.primary_key.includes(column.name) && <span className="column-key"> PK</span>}</td>
          <td>{column.type}</td><td>{column.nullable ? "Yes" : "No"}</td></tr>)}</tbody></table></div>
    </>}
  </section>;
}

export function CatalogExplorer({ pair, onSelect }: { pair: Pair; onSelect: (side: Side, table: string) => void }) {
  const [scope, setScope] = useState<Scope>({});
  const [expanded, setExpanded] = useState<string[]>([]);
  const [catalogInput, setCatalogInput] = useState("");
  const [catalogSearch, setCatalogSearch] = useState("");
  const [input, setInput] = useState("");
  const [search, setSearch] = useState("");
  const [side, setSide] = useState<Side>(isTableSource(pair.baseline) ? "baseline" : "candidate");
  const [inspected, setInspected] = useState<string | null>(null);
  const resultsHeading = useRef<HTMLHeadingElement>(null);
  useEffect(() => { const timer = setTimeout(() => setSearch(input.trim()), 350); return () => clearTimeout(timer); }, [input]);
  useEffect(() => { const timer = setTimeout(() => setCatalogSearch(catalogInput.trim()), 350); return () => clearTimeout(timer); }, [catalogInput]);
  const catalogs = useMetadataPage(`/discovery/catalogs?${new URLSearchParams({ search: catalogSearch })}`);
  const params = new URLSearchParams({ search, ...(scope.catalog ? { catalog: scope.catalog } : {}), ...(scope.schema ? { schema: scope.schema } : {}) });
  const tables = useMetadataPage<Table>(`/discovery/search?${params}`);
  const canSelect = isTableSource(pair[side]);

  function choose(nextSide: Side, table: string) {
    if (!isTableSource(pair[nextSide])) return;
    onSelect(nextSide, table);
    if (nextSide === "baseline" && isTableSource(pair.candidate)) {
      setSide("candidate"); setScope({});
      const name = table.split(".").at(-1)!;
      setInput(name); setSearch(name);
      resultsHeading.current?.focus();
    }
  }
  function pickSide(value: Side) {
    setSide(value);
    if (value === "candidate") {
      const name = sourceTable(pair.baseline)?.split(".").at(-1) || "";
      setInput(name); setSearch(name); setScope({});
    }
  }
  return <div className="catalog-explorer">
    <div className="comparison-selection">
      {(["baseline", "candidate"] as const).map(value => {
        const name = sourceTable(pair[value]);
        const editable = isTableSource(pair[value]);
        const title = value === "baseline" ? "Baseline" : "Candidate";
        const version = pair[`${value}_version`] ?? (typeof pair[value] === "object" ? pair[value]?.version : undefined);
        return <section key={value} className={`selection-card ${side === value ? "selecting" : ""}`} aria-label={`${title} selection`}>
          <div className="selection-heading"><span>{title}</span><button disabled={!editable} aria-pressed={side === value} onClick={() => pickSide(value)}>Choose {value}</button></div>
          {name ? <><strong>{name}</strong><small>{version == null ? "Current version" : `Pinned version ${version}`}</small></>
            : editable ? <p>No table selected</p> : <p>{title} uses a composed source. Configure its template and keys in the source editor.</p>}
        </section>;
      })}
    </div>
    <div className="explorer-layout">
      <div className="catalog-navigation" role="navigation" aria-label="Catalog navigation">
        <p className="eyebrow">CATALOGS</p>
        <label className="sr-only" htmlFor="catalog-filter">Find catalogs</label>
        <input id="catalog-filter" type="search" placeholder="Find a catalog…" value={catalogInput} maxLength={256} onChange={e => setCatalogInput(e.target.value)} />
        <button className="all-catalogs" aria-current={!scope.catalog ? "location" : undefined} onClick={() => setScope({})}>All catalogs</button>
        <ul className="catalog-tree">{catalogs.items.map(catalog => <li key={catalog.name}>
          <button aria-label={`Catalog ${catalog.name}`} aria-expanded={expanded.includes(catalog.name)}
            aria-current={scope.catalog === catalog.name && !scope.schema ? "location" : undefined}
            onClick={() => { setScope({ catalog: catalog.name }); setExpanded(previous => previous.includes(catalog.name) ? previous.filter(n => n !== catalog.name) : [...previous, catalog.name]); }}>
            <span aria-hidden="true">{expanded.includes(catalog.name) ? "▾" : "▸"}</span> {catalog.name}
          </button>
          {expanded.includes(catalog.name) && <Schemas catalog={catalog.name} scope={scope} onScope={setScope} />}
        </li>)}</ul>
        <PageControls page={catalogs} noun="catalogs" />
        {!catalogs.loading && !catalogs.error && !catalogs.items.length && <p className="muted">No matching catalogs.</p>}
      </div>
      <div className="catalog-results">
        <div className="catalog-results-heading"><div><p className="eyebrow">{scope.catalog ? `${scope.catalog}${scope.schema ? ` / ${scope.schema}` : " / all schemas"}` : "ALL ACCESSIBLE CATALOGS"}</p>
          <h3 ref={resultsHeading} tabIndex={-1}>Choose a {side} table</h3></div><span className="badge">Delta tables</span></div>
        <form className="catalog-search" onSubmit={e => { e.preventDefault(); setSearch(input.trim()); }}>
          <label className="sr-only" htmlFor="table-search">Search tables</label>
          <input autoFocus id="table-search" type="search" placeholder="Search table or qualified name…" value={input} maxLength={256} onChange={e => setInput(e.target.value)} />
          <button type="submit">Search</button>
          {input && <button type="button" onClick={() => { setInput(""); setSearch(""); }}>Clear search</button>}
        </form>
        {side === "candidate" && <p className="muted">Same-name results are suggestions. Choose the candidate explicitly; the baseline stays selected.</p>}
        <div className="catalog-table-wrap"><table aria-label="Matching tables"><thead><tr><th>Table</th><th>Catalog / schema</th><th>Compare</th></tr></thead>
          <tbody>{tables.items.map(table => <tr key={table.full_name} className={inspected === table.full_name ? "inspected" : ""}>
            <td><button className="table-name" aria-label={`Inspect ${table.full_name}`} onClick={() => setInspected(table.full_name)}>{table.name}</button></td>
            <td><span>{table.catalog}</span><small>{table.schema}</small></td>
            <td><button disabled={!canSelect} aria-label={side === "baseline" ? `Compare ${table.full_name} with…` : `Use ${table.full_name} as candidate`}
              onClick={() => choose(side, table.full_name)}>{side === "baseline" ? "Compare with…" : "Use as candidate"}</button></td>
          </tr>)}</tbody></table></div>
        <PageControls page={tables} noun="tables" />
        {!tables.loading && !tables.error && !tables.items.length && <div className="catalog-empty"><strong>No matching Delta tables in this scope.</strong><p>Try a different name or choose All catalogs. Only visible Delta tables are listed.</p></div>}
        {inspected ? <TableDetails key={inspected} table={inspected} side={side} canSelect={canSelect} onSelect={choose} />
          : <p className="catalog-details-hint">Select a table name to inspect columns and declared keys.</p>}
      </div>
    </div>
  </div>;
}
