import React, { useEffect, useRef, useState } from "react";
import { api, encoded } from "./api";

type Task = { task_key: string; run_id?: number; status: string; attempt?: number; pair_index?: number | null; url?: string };
type Progress = {
  stage: string;
  counts: { total?: number | null; completed?: number | null; active?: number | null; failed?: number | null } | null;
  tasks: Task[];
  attempts: Task[];
  warnings: string[];
};
export type AnalysisRun = {
  backend?: string; run_id?: string; result_run_id?: string; status: string;
  lifecycle?: string; done?: boolean; url?: string; suite?: string; revision?: number;
  submitted_at: number; updated_at?: number; observed_end?: number; start_time?: number; end_time?: number;
  pairs?: { pair_index: number; pair_id: string; enabled: boolean }[];
  progress?: Progress;
};
const terminal = (run: any) => run.done || ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"].includes(run.lifecycle)
  || ["passed", "failed", "blocked", "error", "cancelled", "canceled", "skipped"].includes(run.status);

export function useAnalysisRun({ onReport, onFinished }: {
  onReport: (report: any) => void; onFinished: () => void;
}) {
  const [run, setRun] = useState<AnalysisRun | null>(null);
  const [open, setOpen] = useState(false);
  const [submissionError, setSubmissionError] = useState("");
  const [monitorError, setMonitorError] = useState("");
  const [cancelError, setCancelError] = useState("");
  const [reportError, setReportError] = useState("");
  const [cancelRequested, setCancelRequested] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [loadingReport, setLoadingReport] = useState(false);
  const [polling, setPolling] = useState(false);
  const retryPoll = useRef<() => void>(() => {});
  const current = useRef(run); current.current = run;
  const callbacks = useRef({ onReport, onFinished }); callbacks.current = { onReport, onFinished };
  const mounted = useRef(true);
  const launch = useRef(false);
  const cancellation = useRef(false);
  const generation = useRef(0);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; generation.current++; }; }, []);

  async function start(path: string, body: unknown) {
    if (launch.current || (current.current && !current.current.done)) return;
    launch.current = true;
    const request = ++generation.current;
    const submitted_at = Date.now();
    setRun({ status: "submitting", submitted_at, done: false }); setOpen(true);
    setSubmissionError(""); setMonitorError(""); setCancelError(""); setReportError(""); setLoadingReport(false);
    setCancelRequested(false); setCancelling(false); cancellation.current = false;
    try {
      const handle = await api(path, body);
      if (!handle.run_id || !handle.backend) throw new Error("Submission returned no run handle");
      if (mounted.current && request === generation.current) setRun({ ...handle, status: "submitted", submitted_at, done: false });
    } catch (e) {
      if (mounted.current && request === generation.current) {
        setSubmissionError(`${(e as Error).message}. Check Databricks Jobs before submitting again if the outcome is uncertain.`);
        setRun({ status: "submission-error", submitted_at, done: true, observed_end: Date.now() });
      }
    } finally { launch.current = false; }
  }

  useEffect(() => {
    if (!run?.run_id || run.done) return;
    let stopped = false;
    let inFlight = false;
    let timer: ReturnType<typeof setTimeout>;
    const controller = new AbortController();
    const runId = run.run_id;
    const backend = run.backend;
    async function poll() {
      if (stopped || inFlight) return;
      inFlight = true; setPolling(true);
      let done = false;
      try {
        const value = await api(backend === "databricks-jobs" ? `/jobs/${encoded(runId)}` : `/runs/${encoded(runId)}`,
          undefined, "GET", controller.signal);
        if (stopped) return;
        done = backend !== "databricks-jobs" || terminal(value);
        setMonitorError("");
        setRun(previous => previous?.run_id === runId ? {
          ...previous, ...(backend === "databricks-jobs" ? value : { status: value.status }),
          run_id: runId, done, updated_at: Date.now(), ...(done ? { observed_end: Date.now() } : {}),
        } : previous);
        if (done) {
          if (backend !== "databricks-jobs") callbacks.current.onReport(value);
          callbacks.current.onFinished();
        }
      } catch (e) {
        if (!stopped) setMonitorError(`Monitoring interrupted: ${(e as Error).message}. The Job may still be running.`);
      } finally {
        inFlight = false;
        if (!stopped) setPolling(false);
        // Schedule only after the previous request completes, never overlapping SQL/API waits.
        if (!stopped && !done) timer = setTimeout(() => void poll(), 3000);
      }
    }
    retryPoll.current = () => {
      if (inFlight || stopped) return;
      clearTimeout(timer);
      void poll();
    };
    void poll();
    return () => {
      stopped = true; controller.abort(); clearTimeout(timer);
      retryPoll.current = () => {};
    };
  }, [run?.run_id, run?.backend, run?.done]);

  async function cancel() {
    const active = current.current;
    if (!active?.run_id || active.done || cancellation.current || cancelRequested) return;
    const request = generation.current;
    const isCurrent = () => mounted.current && generation.current === request && current.current?.run_id === active.run_id;
    cancellation.current = true; setCancelling(true); setCancelError("");
    try {
      await api(`/jobs/${encoded(active.run_id)}/cancel`, {});
      if (isCurrent()) setCancelRequested(true);
    } catch (e) {
      if (isCurrent()) { setCancelError((e as Error).message); cancellation.current = false; }
    } finally { if (isCurrent()) setCancelling(false); }
  }
  async function viewResults() {
    const active = current.current;
    if (!active?.run_id || !active.done || loadingReport) return;
    const resultId = active.result_run_id || (active.backend === "databricks-jobs" ? `job:${active.run_id}` : active.run_id);
    const request = generation.current;
    const isCurrent = () => mounted.current && generation.current === request && current.current?.run_id === active.run_id;
    setLoadingReport(true); setReportError("");
    try {
      const value = await api(`/runs/${encoded(resultId)}`);
      if (isCurrent()) {
        callbacks.current.onReport(value); setOpen(false);
      }
    } catch (e) {
      if (isCurrent()) setReportError(`Results could not be loaded: ${(e as Error).message}. The report may not have been saved or access may be unavailable.`);
    } finally { if (isCurrent()) setLoadingReport(false); }
  }
  return {
    run, open, start, show: () => setOpen(true), hide: () => setOpen(false),
    submissionError, monitorError, cancelError, reportError, cancelRequested, cancelling,
    loadingReport, cancel, viewResults, polling, retryStatus: () => retryPoll.current(),
    active: !!run && !run.done,
  };
}

const stages: Record<string, string> = {
  queued: "Queued", preparing: "Preparing comparisons", comparing: "Comparing tables",
  "saving-results": "Saving results", cancelling: "Cancelling", finished: "Analysis finished", running: "Analysis running",
};
const link = (url?: string) => url && /^https:\/\//.test(url);
function duration(start: number, end: number) {
  const seconds = Math.max(0, Math.floor((end - start) / 1000));
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

export function RunProgressDialog({ analysis }: { analysis: ReturnType<typeof useAnalysisRun> }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [now, setNow] = useState(Date.now());
  const { run, open } = analysis;
  useEffect(() => {
    const node = dialog.current;
    if (!open || !node) return;
    if (typeof node.showModal === "function") node.showModal();
    else node.setAttribute("open", "");
    return () => {
      if (node.open && typeof node.close === "function") node.close();
      else node.removeAttribute("open");
    };
  }, [open]);
  useEffect(() => {
    if (!open || run?.done) return;
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [open, run?.done]);
  if (!open || !run) return null;
  const counts = run.progress?.counts;
  const determinate = typeof counts?.total === "number" && counts.total > 0 && typeof counts.completed === "number";
  const stage = run.status === "submitting" ? "Submitting analysis" : run.done ? "Analysis finished" :
    stages[run.progress?.stage || "running"] || "Analysis running";
  return <dialog ref={dialog} className="run-dialog" aria-labelledby="analysis-progress-title"
    onCancel={e => { e.preventDefault(); analysis.hide(); }}>
    <div className="section-heading">
      <h2 id="analysis-progress-title">Analysis progress</h2>
      <button autoFocus onClick={analysis.hide}>{run.done ? "Close" : "Minimize"}</button>
    </div>
    <div className="section-heading"><strong>{stage}</strong><span className={`badge ${run.status}`}>{run.status}</span></div>
    <p>{run.suite || "Suite"}{run.revision ? ` · revision ${run.revision}` : ""}{run.run_id ? ` · Run ${run.run_id}` : ""}</p>
    <p>Elapsed: {duration(run.start_time || run.submitted_at, run.end_time || run.observed_end || now)}</p>
    {run.updated_at && !run.done && <p className="muted">Last status update: {duration(run.updated_at, now)} ago</p>}
    {analysis.submissionError && <p role="alert">{analysis.submissionError}</p>}
    {!analysis.submissionError && <>
      {(!run.done || determinate) && <progress aria-label="Comparison tasks completed" max={determinate ? counts!.total! : 1}
        value={determinate ? counts!.completed! : undefined} />}
      <p>{determinate ? `${counts!.completed} of ${counts!.total} comparison tasks complete` : run.done ? "Comparison progress unavailable for this run" : "Waiting for comparison progress"}</p>
      {counts && <p>Active: {counts.active ?? "—"} · Failed tasks: {counts.failed ?? "—"}</p>}
      <p className="muted">Progress counts completed tasks, including their latest retries. The final report contains regression outcomes.</p>
    </>}
    {analysis.monitorError && <div role="alert"><p>{analysis.monitorError}</p><button disabled={analysis.polling} onClick={analysis.retryStatus}>Retry status</button></div>}
    {run.progress?.warnings.map((warning, i) => <p className="muted" key={i}>{warning}</p>)}
    {!!run.progress?.tasks.length && <ul className="run-stages">{run.progress.tasks.map(task =>
      <li key={task.task_key}><strong>{({ prepare: "Prepare", comparisons: "Compare", aggregate: "Save results" } as Record<string, string>)[task.task_key] || task.task_key}</strong>: {task.status}</li>)}</ul>}
    {!!run.progress?.attempts.length && <div className="run-attempts"><table>
      <caption>Comparison attempts</caption><thead><tr><th>Pair</th><th>Attempt</th><th>Task status</th></tr></thead>
      <tbody>{run.progress.attempts.map(task => {
        const pair = run.pairs?.find(p => p.pair_index === task.pair_index);
        return <tr key={task.run_id}><td>{pair?.pair_id || (task.pair_index == null ? "Unmapped attempt" : `Pair ${task.pair_index + 1}`)}{pair?.enabled === false ? " (disabled)" : ""}</td>
          <td>{task.attempt == null ? "—" : task.attempt + 1}</td><td>{link(task.url) ? <a href={task.url} target="_blank" rel="noreferrer">{task.status} ↗</a> : task.status}</td></tr>;
      })}</tbody></table></div>}
    {analysis.cancelRequested && !run.done && <p role="status">Cancellation requested; waiting for Databricks.</p>}
    {analysis.cancelError && <p role="alert">Cancellation failed: {analysis.cancelError}</p>}
    {analysis.reportError && <p role="alert">{analysis.reportError}</p>}
    <div className="actions wrap">
      {link(run.url) && <a href={run.url} target="_blank" rel="noreferrer">Open Databricks job ↗</a>}
      {run.backend === "databricks-jobs" && !run.done && <button disabled={analysis.cancelling || analysis.cancelRequested}
        onClick={() => void analysis.cancel()}>{analysis.cancelRequested ? "Cancellation requested" : analysis.cancelling ? "Requesting cancellation…" : "Cancel run"}</button>}
      {run.done && run.run_id && <button disabled={analysis.loadingReport} onClick={() => void analysis.viewResults()}>{analysis.loadingReport ? "Loading results…" : "View results"}</button>}
    </div>
    {!run.done && <p className="muted">Minimizing keeps monitoring in this tab. Closing the browser does not cancel the Job.</p>}
  </dialog>;
}
