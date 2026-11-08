import React, { useEffect, useRef, useState } from "react";
import { api, Config, Pair, pretty } from "./api";
import { CatalogExplorer, TableDescription } from "./CatalogExplorer";

type Key = { baseline: string; candidate: string };
type Description = TableDescription;
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

export function CatalogPairEditor({ config, pairIndex, onChange }: {
  config: Config; pairIndex: number; onChange: (pair: Pair) => void;
}) {
  const pair = config.pairs[pairIndex];
  const [open, setOpen] = useState(false);
  const dialog = useRef<HTMLDialogElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    const node = dialog.current;
    if (!open || !node) return;
    if (typeof node.showModal === "function") node.showModal();
    else node.setAttribute("open", "");
    return () => {
      if (node.open && typeof node.close === "function") node.close();
      else node.removeAttribute("open");
      trigger.current?.focus();
    };
  }, [open]);
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
    <button ref={trigger} aria-expanded={open} onClick={() => setOpen(v => !v)}>{open ? "Close catalog browser" : "Browse Unity Catalog"}</button>
    {open && <dialog ref={dialog} className="catalog-dialog" aria-label={`Catalog Explorer for ${pair.id}`}
      onCancel={e => { e.preventDefault(); setOpen(false); }}>
      <div className="catalog-dialog-heading"><div><p className="eyebrow">PAIRITY / UNITY CATALOG</p>
        <h2>Catalog Explorer</h2><p>Build comparison <strong>{pair.id}</strong> from tables you can access.</p></div>
        <button onClick={() => setOpen(false)}>Done</button></div>
      <CatalogExplorer pair={pair} onSelect={replaceSource} />
      <div className="catalog-key-section"><div className="section-heading"><div><h3>Primary keys & comparison readiness</h3>
        <p className="muted">Inspect both selected sources, then confirm the columns that identify a record.</p></div>
        <button className="primary" disabled={busy} onClick={() => void inspect()}>Inspect tables and keys</button></div>
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
      </div>
      <p className="catalog-editor-note">Selections update the configuration editor. Close this view and use Save &amp; run to start analysis.</p>
    </dialog>}
  </div>;
}
