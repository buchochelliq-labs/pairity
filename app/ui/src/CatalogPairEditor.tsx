import React, { useEffect, useRef, useState } from "react";
import { api, Config, Pair, pretty } from "./api";

type Key = { baseline: string; candidate: string };
type Page = { items: { name: string }[]; next_after: string | null };
type Description = {
  table: string;
  primary_key: string[];
  columns: { name: string; type: string; nullable: boolean }[];
};
type Inspection = {
  valid: boolean;
  keys: Key[];
  key_origin: string;
  baseline: Description | null;
  candidate: Description | null;
  issues: string[];
  warnings: string[];
  casts: (Key & { compare_as: string; mode: string })[];
  settings: { implicit_casts: boolean; null_safe: boolean };
};

function MetadataSelect({ label, path, value, onChange }: {
  label: string; path: string; value: string; onChange: (value: string) => void;
}) {
  const [input, setInput] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState<Page>({ items: [], next_after: null });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const generation = useRef(0);

  async function load(after = "") {
    const request = ++generation.current;
    setBusy(true); setError("");
    try {
      const params = new URLSearchParams({ search, after });
      const next = await api<Page>(`${path}${path.includes("?") ? "&" : "?"}${params}`);
      if (request !== generation.current) return;
      setPage(previous => ({ ...next, items: after ? [...previous.items, ...next.items] : next.items }));
    } catch (e) {
      if (request === generation.current) setError((e as Error).message);
    } finally {
      if (request === generation.current) setBusy(false);
    }
  }
  useEffect(() => {
    setPage({ items: [], next_after: null });
    void load();
    return () => { generation.current++; };
  }, [path, search]);

  return <div className="catalog-select">
    <label>Search {label}<input value={input} maxLength={256} onChange={e => setInput(e.target.value)} /></label>
    <button onClick={() => setSearch(input)}>Search {label}</button>
    <label>{label}<select value={value} disabled={busy} onChange={e => onChange(e.target.value)}>
      <option value="">Choose…</option>
      {value && !page.items.some(i => i.name === value) && <option value={value}>{value}</option>}
      {page.items.map(item => <option key={item.name} value={item.name}>{item.name}</option>)}
    </select></label>
    {busy && <p role="status">Loading {label}…</p>}
    {!busy && !error && page.items.length === 0 && <p>No accessible matches for {label}.</p>}
    {error && <p role="alert">{error}</p>}
    {error && <button onClick={() => void load()}>Retry {label}</button>}
    {page.next_after && !error && <button disabled={busy} onClick={() => void load(page.next_after!)}>More {label}</button>}
  </div>;
}

function TablePicker({ side, source, onSelect }: {
  side: "baseline" | "candidate"; source: Pair["baseline"]; onSelect: (table: string) => void;
}) {
  const title = side === "baseline" ? "Baseline" : "Candidate";
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  if (typeof source !== "string" && source?.kind !== "table") {
    return <p>{title} uses a composed source. Keep its template and explicit keys in the source editor.</p>;
  }
  return <section className="catalog-source" aria-label={`${title} discovery`}>
    <h4>{title}</h4>
    <p className="catalog-name">Current: {typeof source === "string" ? source : source.table}</p>
    <MetadataSelect label={`${title} catalog`} path="/discovery/catalogs" value={catalog}
      onChange={v => { setCatalog(v); setSchema(""); setTable(""); }} />
    {catalog && <MetadataSelect key={catalog} label={`${title} schema`}
      path={`/discovery/schemas?${new URLSearchParams({ catalog })}`} value={schema}
      onChange={v => { setSchema(v); setTable(""); }} />}
    {catalog && schema && <MetadataSelect key={`${catalog}.${schema}`} label={`${title} table selection`}
      path={`/discovery/tables?${new URLSearchParams({ catalog, schema })}`} value={table} onChange={setTable} />}
    {table && <p className="catalog-name">Selected: {catalog}.{schema}.{table}</p>}
    <button disabled={!catalog || !schema || !table}
      onClick={() => onSelect(`${catalog}.${schema}.${table}`)}>Use selected {side} table</button>
    <p className="muted">Uses the current version. Existing key mappings are retained for validation.</p>
  </section>;
}

export function CatalogPairEditor({ config, pairIndex, onChange }: {
  config: Config; pairIndex: number; onChange: (pair: Pair) => void;
}) {
  const pair = config.pairs[pairIndex];
  const [open, setOpen] = useState(false);
  const [inspection, setInspection] = useState<Inspection | null>(null);
  const [draft, setDraft] = useState<Key[]>([]);
  const [checkedKeys, setCheckedKeys] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const stamp = pretty(config);
  const currentStamp = useRef(stamp);
  currentStamp.current = stamp;
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setInspection(null); setError(""); setBusy(false); setCheckedKeys("");
  }, [stamp, pairIndex, open]);
  useEffect(() => () => { generation.current++; }, []);

  async function inspect(useDraft = false) {
    const request = ++generation.current;
    const original = stamp;
    const content = useDraft ? pretty({ ...config, pairs: config.pairs.map((p, i) =>
      i === pairIndex ? { ...p, keys: draft } : p) }) : stamp;
    setBusy(true); setError(""); setCheckedKeys("");
    try {
      const result = await api<Inspection>("/discovery/inspect", { content, format: "json", pair_id: pair.id });
      if (request !== generation.current || currentStamp.current !== original) return;
      setInspection(result); setDraft(result.keys); setCheckedKeys(pretty(result.keys));
    } catch (e) {
      if (request === generation.current && currentStamp.current === original) setError((e as Error).message);
    } finally {
      if (request === generation.current) setBusy(false);
    }
  }
  const valid = !!inspection?.valid && checkedKeys === pretty(draft) && !busy && !error;
  const canValidate = draft.length > 0 && draft.every(k => k.baseline && k.candidate);
  const replaceSource = (side: "baseline" | "candidate", table: string) => {
    const changed = { ...pair, [side]: table };
    delete changed[`${side}_version`];
    onChange(changed);
  };
  return <div className="catalog-editor">
    <button aria-expanded={open} onClick={() => setOpen(v => !v)}>{open ? "Close catalog browser" : "Browse Unity Catalog"}</button>
    {open && <div>
      <p>Find accessible Delta tables, then inspect their columns and primary keys.</p>
      <div className="form-grid">
        <TablePicker key={`baseline-${pretty(pair.baseline)}`} side="baseline" source={pair.baseline} onSelect={t => replaceSource("baseline", t)} />
        <TablePicker key={`candidate-${pretty(pair.candidate)}`} side="candidate" source={pair.candidate} onSelect={t => replaceSource("candidate", t)} />
      </div>
      <button disabled={busy} onClick={() => void inspect()}>Inspect tables and keys</button>
      {busy && <p role="status">Checking table access and key metadata…</p>}
      {error && <p role="alert">{error}</p>}
      {inspection && <section aria-label="Key mapping validation">
        <p role="status">{valid ? "Metadata checks passed" : "Review the key mappings and validate before applying."}</p>
        {inspection.issues.map((issue, i) => <p role="alert" key={i}>{issue}</p>)}
        <p>Key source: {inspection.key_origin}. Implicit casts: {inspection.settings.implicit_casts ? "on" : "off"}; null-safe comparison: {inspection.settings.null_safe ? "on" : "off"}.</p>
        {(["baseline", "candidate"] as const).map(side => inspection[side] && <p key={side}>
          {side === "baseline" ? "Baseline" : "Candidate"} declared primary key: {inspection[side]!.primary_key.join(", ") || "none"}
        </p>)}
        {draft.map((key, index) => <div className="key-mapping" key={index}>
          {(["baseline", "candidate"] as const).map(side => <label key={side}>
            {side === "baseline" ? "Baseline" : "Candidate"} key {index + 1}
            <select disabled={busy} value={key[side]} onChange={e => setDraft(previous => previous.map((k, i) =>
              i === index ? { ...k, [side]: e.target.value } : k))}>
              <option value="">Choose column…</option>
              {key[side] && !inspection[side]?.columns.some(c => c.name === key[side]) && <option value={key[side]}>{key[side]} (missing)</option>}
              {inspection[side]?.columns.map(c => <option key={c.name} value={c.name}>{c.name} · {c.type}{c.nullable ? " · nullable" : ""}</option>)}
            </select>
          </label>)}
          <button disabled={busy} onClick={() => setDraft(previous => previous.filter((_, i) => i !== index))}>Remove key {index + 1}</button>
        </div>)}
        <div className="actions wrap">
          <button disabled={busy || !inspection.baseline || !inspection.candidate} onClick={() => setDraft(previous => [...previous, { baseline: "", candidate: "" }])}>Add key column</button>
          <button disabled={busy || !canValidate} onClick={() => void inspect(true)}>Validate key mapping</button>
          <button disabled={!valid || !canValidate} onClick={() => onChange({ ...pair, keys: draft })}>Apply key mappings</button>
        </div>
        {valid && inspection.casts.map((cast, i) => <p key={i}>{cast.baseline} → {cast.candidate}: compare as {cast.compare_as} ({cast.mode})</p>)}
        {inspection.warnings.map((warning, i) => <p className="muted" key={i}>{warning}</p>)}
      </section>}
    </div>}
  </div>;
}
