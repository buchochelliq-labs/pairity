import React, { useState, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { parse, stringify } from "yaml";
import { api, encoded, pretty, Config, Pair } from "./api";
import "./style.css";
const initial: Config = {
  schema_version: 1,
  suite: "new-suite",
  pairs: [
    {
      id: "customers",
      baseline: "catalog.baseline.customers",
      candidate: "catalog.candidate.customers",
      keys: [{ baseline: "id", candidate: "id" }],
      tags: [],
      enabled: true,
    },
  ],
};
function Badge({ value }: { value: string }) {
  return <span className={`badge ${value}`}>{value}</span>;
}
function Json({ value }: { value: unknown }) {
  return <pre>{pretty(value)}</pre>;
}
export function App() {
  const [view, setView] = useState("Overview"),
    [session, setSession] = useState<any>({}),
    [suites, setSuites] = useState<any[]>([]),
    [runs, setRuns] = useState<any[]>([]),
    [historyAvailable, setHistoryAvailable] = useState(false),
    [suitesAvailable, setSuitesAvailable] = useState(false),
    [unavailableCount, setUnavailableCount] = useState(0),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [busy, setBusy] = useState(false),
    [query, setQuery] = useState("");
  const [text, setText] = useState(pretty(initial)),
    [format, setFormat] = useState<"json" | "yaml">("json"),
    [revision, setRevision] = useState<number | null>(null),
    [savedName, setSavedName] = useState(""),
    [savedConfig, setSavedConfig] = useState(""),
    [preview, setPreview] = useState<any>(null),
    [effective, setEffective] = useState<any[]>([]),
    [selected, setSelected] = useState<string[]>([]),
    [patch, setPatch] = useState('{"enabled": false}'),
    [patchPreview, setPatchPreview] = useState<any>(null),
    [tags, setTags] = useState(""),
    [detail, setDetail] = useState<any>(null),
    [job, setJob] = useState<any>(null);
  let config: Config | null = null;
  try {
    const parsed = format === "yaml" ? parse(text) : JSON.parse(text);
    if (
      parsed &&
      typeof parsed.suite === "string" &&
      Array.isArray(parsed.pairs) &&
      parsed.pairs.every(
        (p: any) =>
          p &&
          typeof p.id === "string" &&
          (!p.tags || Array.isArray(p.tags)) &&
          (!p.keys ||
            (Array.isArray(p.keys) &&
              p.keys.every(
                (k: any) =>
                  k &&
                  typeof k.baseline === "string" &&
                  typeof k.candidate === "string",
              ))),
      )
    )
      config = parsed;
  } catch {
    /* Preserve invalid editor input. */
  }
  const act = async (fn: () => Promise<void>) => {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await fn();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  const refresh = async () => {
    const [s, r] = await Promise.allSettled([
      api("/suites"),
      api("/runs?limit=50"),
    ]);
    setSuitesAvailable(s.status === "fulfilled");
    setHistoryAvailable(r.status === "fulfilled");
    if (s.status === "fulfilled") setSuites(s.value.items);
    if (r.status === "fulfilled") {
      setRuns(r.value.items);
      setUnavailableCount(r.value.unavailable_count ?? 0);
    } else {
      setRuns([]);
      setUnavailableCount(0);
    }
    const failures = [s, r].filter((result) => result.status === "rejected");
    if (failures.length)
      throw new Error(
        failures
          .map(
            (result) =>
              (result as PromiseRejectedResult).reason?.message ||
              "Workspace data unavailable",
          )
          .join("; "),
      );
  };
  useEffect(() => {
    void act(async () => {
      setSession(await api("/session"));
      await refresh();
    });
  }, []);
  useEffect(() => {
    if (!job?.run_id || job.done) return;
    let stopped = false;
    const poll = async () => {
      try {
        if (job.backend === "databricks-jobs") {
          const state = await api(`/jobs/${encoded(String(job.run_id))}`);
          if (stopped) return;
          setJob((j: any) => ({ ...j, ...state, run_id: j.run_id }));
          if (
            ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"].includes(
              state.lifecycle,
            ) ||
            [
              "passed",
              "failed",
              "blocked",
              "error",
              "cancelled",
              "canceled",
            ].includes(state.status)
          ) {
            setJob((j: any) => ({ ...j, done: true }));
            await refresh();
          }
        } else {
          const result = await api(`/runs/${encoded(String(job.run_id))}`);
          if (!stopped) {
            setDetail(result);
            setJob((j: any) => ({ ...j, done: true, status: result.status }));
            await refresh();
          }
        }
      } catch (e) {
        if (!stopped) setError((e as Error).message);
      }
    };
    void poll();
    const timer = setInterval(poll, 3000);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [job?.run_id, job?.done]);
  function edit(value: string) {
    setText(value);
    setPreview(null);
    setEffective([]);
    setPatchPreview(null);
  }
  function write(c: Config) {
    edit(format === "yaml" ? stringify(c) : pretty(c));
  }
  async function load(name: string) {
    const d = await api(`/suites/${encoded(name)}`);
    setFormat("json");
    setText(pretty(d.config));
    setSavedConfig(pretty(d.config));
    setSavedName(name);
    setRevision(d.revision);
    setTags("");
    setSelected(
      d.config.pairs
        .filter((p: Pair) => p.enabled !== false)
        .map((p: Pair) => p.id),
    );
    setPreview(null);
    setEffective([]);
    setPatchPreview(null);
    setView("Configurations");
  }
  const patchSignature = pretty({ patch, selected, tags, revision, savedName });
  const dirty = !config || pretty(config) !== savedConfig;
  const suitePath = `/suites/${encoded(savedName)}`;
  const tagList = () =>
    tags
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
  function updatePair(i: number, key: string, value: unknown) {
    if (config)
      write({
        ...config,
        pairs: config.pairs.map((p, j) =>
          j === i ? { ...p, [key]: value } : p,
        ),
      });
  }
  const open = (r: any) =>
    void act(async () => {
      setDetail(await api(`/runs/${encoded(r.run_id)}`));
      setView("Run history");
    });
  return (
    <div className="shell">
      <aside>
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setView("Overview");
          }}
        >
          <span className="logo">↔</span> pairity
          <span className="beta">WORKBENCH</span>
        </a>
        <p className="nav-label">WORKSPACE</p>
        <nav>
          {["Overview", "Configurations", "Run history"].map((v, i) => (
            <button
              key={v}
              className={view === v ? "active" : ""}
              onClick={() => setView(v)}
            >
              <span aria-hidden="true">{["◫", "≋", "◷"][i]}</span>
              {v}
            </button>
          ))}
        </nav>
        <div className="aside-bottom">
          <span className="live-dot" /> {session.mode || "Connecting"}
          <small>
            {typeof session.user === "string"
              ? session.user
              : session.user?.name ||
                session.user?.email ||
                "Authenticated workspace"}
          </small>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <p className="eyebrow">PAIRITY / WORKSPACE</p>
            <h1>{view}</h1>
          </div>
          <button disabled={busy} onClick={() => void act(refresh)}>
            ↻ Refresh
          </button>
        </header>
        {error && (
          <div role="alert" className="alert">
            {error}
          </div>
        )}
        {notice && (
          <div role="status" className="notice">
            {notice}
          </div>
        )}
        {unavailableCount > 0 && (
          <div role="status" className="notice">
            {unavailableCount} run report
            {unavailableCount === 1 ? " is" : "s are"} unavailable because
            source data is inaccessible or deleted. Counts include accessible
            reports only.
          </div>
        )}
        {busy && (
          <div role="status" className="loading">
            Working…
          </div>
        )}
        {view === "Overview" && (
          <>
            <section className="welcome">
              <div>
                <p className="eyebrow">CONFIDENCE IN EVERY CHANGE</p>
                <h2>Your data. Side by side.</h2>
                <p>
                  Configure comparisons, launch runs, and investigate the
                  differences.
                </p>
              </div>
              <button
                className="primary"
                onClick={() => setView("Configurations")}
              >
                Open configurations →
              </button>
            </section>
            <div className="stats">
              {[
                ["Configurations", suitesAvailable ? suites.length : "—"],
                ["Recent runs", historyAvailable ? runs.length : "—"],
                [
                  "Passed",
                  historyAvailable
                    ? runs.filter((r) => r.status === "passed").length
                    : "—",
                ],
                [
                  "Needs attention",
                  historyAvailable
                    ? runs.filter((r) =>
                        ["failed", "blocked", "error"].includes(r.status),
                      ).length
                    : "—",
                ],
              ].map(([label, value]) => (
                <div className="stat" key={label}>
                  <span>{label}</span>
                  <strong>{value}</strong>
                  <small>Current workspace · recent 50 runs</small>
                </div>
              ))}
            </div>
            <section className="panel">
              <div className="section-heading">
                <h2>Recent outcomes</h2>
                <span className="muted">Newest first</span>
              </div>
              <div className="trend" aria-label="Recent run outcomes">
                {runs.slice(0, 30).map((r) => (
                  <button
                    key={r.run_id}
                    className={`trend-item ${r.status}`}
                    title={`${r.suite}: ${r.status}`}
                    aria-label={`${r.suite}: ${r.status}`}
                    onClick={() => open(r)}
                  />
                ))}
                {!runs.length && (
                  <p className="empty">
                    {historyAvailable
                      ? "No accessible runs to show."
                      : "Run history is unavailable."}
                  </p>
                )}
              </div>
              <div className="legend">
                {["passed", "failed", "blocked", "error", "skipped"].map(
                  (s) => (
                    <Badge key={s} value={s} />
                  ),
                )}
              </div>
            </section>
            <section className="panel">
              <h2>Failure history</h2>
              <RunList
                unavailable={!historyAvailable}
                items={runs
                  .filter((r) =>
                    ["failed", "blocked", "error"].includes(r.status),
                  )
                  .slice(0, 8)}
                open={open}
              />
            </section>
          </>
        )}
        {view === "Configurations" && (
          <div className="config-layout">
            <section className="panel suite-list">
              <div className="section-heading">
                <h2>Suites</h2>
                <button
                  aria-label="New suite"
                  onClick={() => {
                    setSavedName("");
                    setRevision(null);
                    setFormat("json");
                    edit(pretty(initial));
                    setSelected([]);
                    setTags("");
                  }}
                >
                  ＋ New
                </button>
              </div>
              <label>
                Search suites
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Find a configuration"
                />
              </label>
              {suites
                .filter((s) =>
                  s.suite.toLowerCase().includes(query.toLowerCase()),
                )
                .map((s) => (
                  <button
                    className={`suite ${s.suite === savedName ? "current" : ""}`}
                    key={s.suite}
                    onClick={() => void act(() => load(s.suite))}
                  >
                    <strong>{s.suite}</strong>
                    <small>
                      Revision {s.revision} ·{" "}
                      {Array.isArray(s.pairs) ? s.pairs.length : s.pairs} pairs
                    </small>
                  </button>
                ))}
              {!suites.length && (
                <p className="empty">
                  {suitesAvailable
                    ? "No saved suites yet. Import a file or create your first comparison."
                    : "Saved suites are unavailable."}
                </p>
              )}
            </section>
            <div>
              <section className="panel">
                <div className="section-heading">
                  <div>
                    <h2>Configuration editor</h2>
                    <p className="muted">
                      {revision === null
                        ? "Unsaved configuration"
                        : `${savedName} · revision ${revision}`}
                    </p>
                  </div>
                  <div className="actions">
                    <button
                      disabled={!config}
                      onClick={() => {
                        if (config) {
                          write({ ...config, suite: `${config.suite}-copy` });
                          setSavedName("");
                          setRevision(null);
                          setSelected([]);
                          setTags("");
                        }
                      }}
                    >
                      Clone
                    </button>
                    {savedName && (
                      <button onClick={() => void act(() => load(savedName))}>
                        Reload saved
                      </button>
                    )}
                  </div>
                </div>
                <div className="actions wrap">
                  <label className="file-button">
                    Import JSON / YAML
                    <input
                      type="file"
                      accept=".json,.yaml,.yml"
                      onChange={(e) => {
                        const file = e.target.files?.[0];
                        if (file)
                          void act(async () => {
                            setFormat(
                              file.name.endsWith(".json") ? "json" : "yaml",
                            );
                            edit(await file.text());
                            setSavedName("");
                            setRevision(null);
                            setSelected([]);
                            setTags("");
                          });
                      }}
                    />
                  </label>
                  <label>
                    Format
                    <select
                      value={format}
                      onChange={(e) => {
                        const next = e.target.value as "json" | "yaml";
                        setFormat(next);
                        if (config)
                          edit(
                            next === "json"
                              ? pretty(config)
                              : stringify(config),
                          );
                      }}
                    >
                      <option value="json">JSON</option>
                      <option value="yaml">YAML</option>
                    </select>
                  </label>
                  {savedName &&
                    (["json", "yaml"] as const).map((f) => (
                      <a
                        className="button"
                        href={`/api/v1/exports/${encoded(savedName)}?format=${f}&revision=${revision}`}
                        key={f}
                      >
                        Export {f.toUpperCase()}
                      </a>
                    ))}
                </div>
                <label>
                  Suite name
                  <input
                    value={config?.suite || ""}
                    disabled={!config}
                    onChange={(e) =>
                      config && write({ ...config, suite: e.target.value })
                    }
                  />
                </label>
                <details open>
                  <summary>Guided pair editor</summary>
                  <p className="muted">
                    Use the source editor below for templates, composed sources,
                    column mappings, and advanced settings.
                  </p>
                  {config?.pairs?.map((p, i) => (
                    <fieldset key={i}>
                      <legend>Pair {i + 1}</legend>
                      <div className="form-grid">
                        <label>
                          Pair ID
                          <input
                            value={p.id}
                            onChange={(e) =>
                              updatePair(i, "id", e.target.value)
                            }
                          />
                        </label>
                        <label>
                          Tags (comma separated)
                          <input
                            key={pretty(p.tags)}
                            defaultValue={(p.tags || []).join(", ")}
                            onBlur={(e) =>
                              updatePair(
                                i,
                                "tags",
                                e.target.value
                                  .split(",")
                                  .map((t) => t.trim())
                                  .filter(Boolean),
                              )
                            }
                          />
                        </label>
                        <label>
                          Baseline table
                          <input
                            value={
                              typeof p.baseline === "string"
                                ? p.baseline
                                : "Composed source — use source editor"
                            }
                            disabled={typeof p.baseline !== "string"}
                            onChange={(e) =>
                              updatePair(i, "baseline", e.target.value)
                            }
                          />
                        </label>
                        <label>
                          Candidate table
                          <input
                            value={
                              typeof p.candidate === "string"
                                ? p.candidate
                                : "Composed source — use source editor"
                            }
                            disabled={typeof p.candidate !== "string"}
                            onChange={(e) =>
                              updatePair(i, "candidate", e.target.value)
                            }
                          />
                        </label>
                        <label>
                          Keys (baseline:candidate, …)
                          <input
                            key={pretty(p.keys)}
                            defaultValue={(p.keys || [])
                              .map((k) =>
                                k.baseline === k.candidate
                                  ? k.baseline
                                  : `${k.baseline}:${k.candidate}`,
                              )
                              .join(", ")}
                            onBlur={(e) =>
                              updatePair(
                                i,
                                "keys",
                                e.target.value.trim()
                                  ? e.target.value.split(",").map((k) => {
                                      const [baseline, candidate] = k
                                        .trim()
                                        .split(":");
                                      return {
                                        baseline,
                                        candidate: candidate || baseline,
                                      };
                                    })
                                  : null,
                              )
                            }
                          />
                        </label>
                        <label className="check">
                          <input
                            type="checkbox"
                            checked={p.enabled !== false}
                            onChange={(e) =>
                              updatePair(i, "enabled", e.target.checked)
                            }
                          />{" "}
                          Enabled
                        </label>
                      </div>
                      <button
                        disabled={config!.pairs.length < 2}
                        onClick={() =>
                          config &&
                          write({
                            ...config,
                            pairs: config.pairs.filter((_, j) => i !== j),
                          })
                        }
                      >
                        Remove pair
                      </button>
                    </fieldset>
                  ))}
                  <button
                    disabled={!config}
                    onClick={() =>
                      config &&
                      write({
                        ...config,
                        pairs: [
                          ...config.pairs,
                          {
                            ...initial.pairs[0],
                            id: `pair-${config.pairs.length + 1}`,
                          },
                        ],
                      })
                    }
                  >
                    ＋ Add pair
                  </button>
                </details>
                <label>
                  Configuration source
                  <textarea
                    className="code"
                    rows={16}
                    value={text}
                    spellCheck={false}
                    onChange={(e) => edit(e.target.value)}
                  />
                </label>
                <div className="actions wrap">
                  <button
                    disabled={busy}
                    onClick={() =>
                      void act(async () => {
                        const d = await api("/validate", {
                          content: text,
                          format,
                        });
                        setEffective(d.effective);
                        setNotice(
                          `Valid configuration · ${d.config_hash.slice(0, 12)}`,
                        );
                      })
                    }
                  >
                    Validate
                  </button>
                  <button
                    disabled={busy}
                    onClick={() =>
                      void act(async () => {
                        const d = await api("/preview", {
                          suite: config?.suite || savedName,
                          content: text,
                          format,
                          expected_revision: revision ?? 0,
                        });
                        setPreview({
                          ...d,
                          source: text,
                          format,
                          expectedRevision: revision,
                        });
                        setEffective(d.effective);
                      })
                    }
                  >
                    Preview changes
                  </button>
                  <button
                    className="primary"
                    disabled={
                      busy ||
                      !preview ||
                      !config ||
                      preview.source !== text ||
                      preview.format !== format ||
                      preview.expectedRevision !== revision
                    }
                    onClick={() =>
                      void act(async () => {
                        const d = await api(
                          `/suites/${encoded(config!.suite)}`,
                          {
                            content: text,
                            format,
                            expected_revision: revision ?? 0,
                          },
                          "PUT",
                        );
                        setSavedName(d.config.suite);
                        setSavedConfig(pretty(d.config));
                        setText(
                          format === "yaml"
                            ? stringify(d.config)
                            : pretty(d.config),
                        );
                        setRevision(d.revision);
                        setPreview(null);
                        setNotice(`Saved revision ${d.revision}`);
                        await refresh();
                      })
                    }
                  >
                    Apply & save
                  </button>
                </div>
                {preview && (
                  <div className="review">
                    <h3>Review changes</h3>
                    <Json
                      value={{
                        ...preview.preview,
                        suite_settings_changed: preview.suite_settings_changed,
                        config_hash: preview.config_hash,
                      }}
                    />
                  </div>
                )}
                {effective.length > 0 && (
                  <details open>
                    <summary>Effective settings & key readiness</summary>
                    {effective.map((e) => (
                      <div key={e.pair_id}>
                        <h3>
                          {e.pair_id}{" "}
                          {e.needs_keys && <Badge value="needs keys" />}
                        </h3>
                        <Json value={{ keys: e.keys, settings: e.settings }} />
                      </div>
                    ))}
                  </details>
                )}
              </section>
              <section className="panel">
                <h2>Run & bulk actions</h2>
                <p className="muted">
                  Actions use saved revision {revision ?? "—"}. Save editor
                  changes before running.
                </p>
                <div className="pair-selection">
                  {config?.pairs?.map((p) => (
                    <label className="check" key={p.id}>
                      <input
                        type="checkbox"
                        checked={selected.includes(p.id)}
                        onChange={(e) => {
                          setTags("");
                          setSelected(
                            e.target.checked
                              ? [...selected, p.id]
                              : selected.filter((id) => id !== p.id),
                          );
                          setPatchPreview(null);
                        }}
                      />
                      {p.id}
                    </label>
                  ))}
                </div>
                <label>
                  Filter tags (comma separated)
                  <input
                    value={tags}
                    onChange={(e) => {
                      setTags(e.target.value);
                      setSelected([]);
                      setPatchPreview(null);
                    }}
                  />
                </label>
                <button
                  className="primary"
                  disabled={busy || revision === null || !savedName || dirty}
                  onClick={() =>
                    void act(async () => {
                      const d = await api(`${suitePath}/runs`, {
                        revision,
                        request_id: crypto.randomUUID(),
                        ...(selected.length ? { pair_ids: selected } : {}),
                        ...(tagList().length ? { tags: tagList() } : {}),
                      });
                      setJob(d);
                      setNotice(`Run submitted: ${d.run_id}`);
                      setView("Run history");
                      await refresh();
                    })
                  }
                >
                  ▷ Run{" "}
                  {selected.length
                    ? "selected pairs"
                    : tagList().length
                      ? "tagged pairs"
                      : "saved suite"}
                </button>
                <details>
                  <summary>Bulk patch saved pairs</summary>
                  <p className="muted">
                    Select pairs or enter tags to choose the patch targets.
                    Preview the exact patch before applying.
                  </p>
                  <label>
                    Pair patch (JSON)
                    <textarea
                      className="code"
                      value={patch}
                      onChange={(e) => {
                        setPatch(e.target.value);
                        setPatchPreview(null);
                      }}
                    />
                  </label>
                  <div className="actions">
                    <button
                      disabled={
                        busy ||
                        revision === null ||
                        dirty ||
                        (!selected.length && !tagList().length)
                      }
                      onClick={() =>
                        void act(async () =>
                          setPatchPreview({
                            ...(await api(`${suitePath}/patch`, {
                              patch: JSON.parse(patch),
                              ...(selected.length
                                ? { pair_ids: selected }
                                : { tags: tagList() }),
                              expected_revision: revision ?? 0,
                              apply: false,
                            })),
                            signature: patchSignature,
                          }),
                        )
                      }
                    >
                      Preview patch
                    </button>
                    <button
                      disabled={
                        busy ||
                        !patchPreview ||
                        dirty ||
                        patchPreview.signature !== patchSignature
                      }
                      onClick={() =>
                        void act(async () => {
                          await api(`${suitePath}/patch`, {
                            patch: JSON.parse(patch),
                            ...(selected.length
                              ? { pair_ids: selected }
                              : { tags: tagList() }),
                            expected_revision: revision ?? 0,
                            apply: true,
                          });
                          await load(savedName);
                          await refresh();
                          setNotice("Patch applied");
                        })
                      }
                    >
                      Apply patch
                    </button>
                  </div>
                  {patchPreview && <Json value={patchPreview.preview} />}
                </details>
              </section>
            </div>
          </div>
        )}
        {view === "Run history" && (
          <>
            {job && (
              <section className="panel">
                <div className="section-heading">
                  <h2>Run progress</h2>
                  <Badge value={job.status || "submitted"} />
                </div>
                <p>
                  Run {job.run_id} · {job.backend}{" "}
                  {job.lifecycle && `· ${job.lifecycle}`}
                </p>
                {job.url && /^https:\/\//.test(job.url) && (
                  <a href={job.url} target="_blank" rel="noreferrer">
                    Open Databricks job ↗
                  </a>
                )}
                {job.backend === "databricks-jobs" && !job.done && (
                  <button
                    onClick={() =>
                      void act(async () => {
                        await api(
                          `/jobs/${encoded(String(job.run_id))}/cancel`,
                          {},
                        );
                        setNotice("Cancellation requested");
                      })
                    }
                  >
                    Cancel run
                  </button>
                )}
              </section>
            )}
            <section className="panel">
              <div className="section-heading">
                <h2>Comparison runs</h2>
                <span className="muted">Last 50</span>
              </div>
              <RunList
                items={runs}
                open={open}
                unavailable={!historyAvailable}
              />
            </section>
            {detail && (
              <section className="panel">
                <div className="section-heading">
                  <div>
                    <h2>{detail.suite}</h2>
                    <p className="muted">
                      {detail.run_id} · {detail.finished_at}
                    </p>
                  </div>
                  <Badge value={detail.status} />
                </div>
                <p className="hash">Configuration hash: {detail.config_hash}</p>
                <a
                  className="button"
                  href={`/api/v1/runs/${encoded(detail.run_id)}/incident`}
                >
                  Download full incident bundle
                </a>
                {detail.pairs?.map((p: any) => (
                  <article className="result" key={p.pair_id}>
                    <div className="section-heading">
                      <h3>{p.pair_id}</h3>
                      <Badge value={p.status} />
                    </div>
                    {p.reason && <p>{p.reason}</p>}
                    {p.counts && !["blocked", "error"].includes(p.status) ? (
                      <div className="counts">
                        {Object.entries(p.counts).map(([k, v]) => (
                          <div key={k}>
                            <strong>{String(v)}</strong>
                            <span>{k.replaceAll("_", " ")}</span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="unavailable">
                        Row counts unavailable for this comparison.
                      </p>
                    )}
                    <details>
                      <summary>Schema, casts & ignored columns</summary>
                      <Json
                        value={{
                          schema_differences: p.schema_differences,
                          casts: p.casts,
                          ignored_columns: p.ignored_columns,
                          column_changes: p.column_changes,
                        }}
                      />
                    </details>
                    <details>
                      <summary>Inputs, keys & effective settings</summary>
                      <Json
                        value={{
                          inputs: p.inputs,
                          keys: p.keys,
                          effective_settings: p.effective_settings,
                        }}
                      />
                    </details>
                    <a
                      href={`/api/v1/runs/${encoded(detail.run_id)}/incident?pair_id=${encoded(p.pair_id)}`}
                    >
                      Download pair incident bundle ↓
                    </a>
                  </article>
                ))}
              </section>
            )}
          </>
        )}
        <footer>
          Pairity workbench{" "}
          <span>
            Configuration-driven comparison · Powered by your execution backend
          </span>
        </footer>
      </main>
    </div>
  );
}
function RunList({
  items,
  open,
  unavailable = false,
}: {
  items: any[];
  open: (r: any) => void;
  unavailable?: boolean;
}) {
  return items.length ? (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Suite / run</th>
            <th>Status</th>
            <th>Pairs</th>
            <th>Finished</th>
            <th>
              <span className="sr-only">Action</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.run_id}>
              <td>
                <strong>{r.suite}</strong>
                <small>{r.run_id}</small>
              </td>
              <td>
                <Badge value={r.status} />
              </td>
              <td>
                {Array.isArray(r.pairs) ? r.pairs.length : (r.pairs ?? "—")}
              </td>
              <td>
                {r.finished_at
                  ? new Date(r.finished_at).toLocaleString()
                  : "In progress"}
              </td>
              <td>
                <button onClick={() => open(r)}>Review →</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ) : (
    <p className="empty">
      {unavailable
        ? "Run history is unavailable."
        : "No accessible runs to show."}
    </p>
  );
}
if (document.getElementById("root"))
  createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
