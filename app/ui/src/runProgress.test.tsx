import React from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { RunProgressDialog, useAnalysisRun } from "./RunProgress";

const respond = (data: any, status = 200) => ({ ok: status < 400, status, json: async () => data }) as Response;
const handle = { backend: "databricks-jobs", run_id: "123", result_run_id: "job:123", suite: "daily", revision: 2,
  pairs: [{ pair_index: 0, pair_id: "customers", enabled: true }, { pair_index: 1, pair_id: "orders", enabled: true }] };
let state: any;
let failPoll: boolean;
let failReport: boolean;
let report: ReturnType<typeof vi.fn>;
let finished: ReturnType<typeof vi.fn>;

function Harness() {
  const analysis = useAnalysisRun({ onReport: report, onFinished: finished });
  return <>
    <button onClick={() => void analysis.start("/suites/daily/runs", { request_id: "stable" })}>Start analysis</button>
    {analysis.run && !analysis.open && <button onClick={analysis.show}>Show progress</button>}
    <RunProgressDialog analysis={analysis} />
  </>;
}

beforeEach(() => {
  vi.useFakeTimers();
  report = vi.fn(); finished = vi.fn(); failPoll = false; failReport = false;
  state = { status: "running", lifecycle: "RUNNING", done: false, progress: {
    stage: "comparing", counts: { total: 2, completed: 1, active: 1, failed: 0 }, tasks: [], warnings: [],
    attempts: [{ run_id: 12, pair_index: 1, status: "running", attempt: 0 }],
  } };
  vi.stubGlobal("fetch", vi.fn(async (url: any) => {
    if (String(url).endsWith("/cancel")) return respond({ cancel_requested: "123" });
    if (String(url).endsWith("/suites/daily/runs")) return respond(handle);
    if (String(url).includes("/runs/job%3A123")) return failReport
      ? respond({ detail: "Report unavailable" }, 404) : respond({ run_id: "job:123", status: "failed" });
    return failPoll ? respond({ detail: "Status unavailable" }, 503) : respond(state);
  }));
  Object.defineProperty(HTMLDialogElement.prototype, "showModal", { configurable: true, value() { this.setAttribute("open", ""); } });
  Object.defineProperty(HTMLDialogElement.prototype, "close", { configurable: true, value() { this.removeAttribute("open"); } });
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); });
async function click(name: string) { await act(async () => { fireEvent.click(screen.getByRole("button", { name })); }); }
async function tick(ms = 3000) { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); }
const calls = (suffix: string) => vi.mocked(fetch).mock.calls.filter(([url]) => String(url).endsWith(suffix));

it("opens immediately while submitting and prevents duplicate launches", async () => {
  let resolve: (value: Response) => void = () => {};
  vi.mocked(fetch).mockImplementationOnce(() => new Promise(r => { resolve = r; }));
  render(<Harness />);
  await click("Start analysis"); await click("Start analysis");
  expect(screen.getByRole("dialog", { name: "Analysis progress" })).toBeTruthy();
  expect(screen.getByText("Submitting analysis")).toBeTruthy();
  expect(calls("/suites/daily/runs")).toHaveLength(1);
  await act(async () => { resolve(respond(handle)); });
  expect(screen.getByText("1 of 2 comparison tasks complete")).toBeTruthy();
  expect(screen.getByText("orders")).toBeTruthy();
});

it("continues polling when minimized and reopens without resubmission", async () => {
  render(<Harness />); await click("Start analysis"); await click("Minimize");
  expect(screen.queryByRole("dialog")).toBeNull();
  state = { ...state, progress: { ...state.progress, stage: "saving-results", counts: { total: 2, completed: 2 } } };
  await tick(); await click("Show progress");
  expect(screen.getByText("Saving results")).toBeTruthy();
  expect(screen.getByRole("button", { name: "Cancel run" })).toBeTruthy();
  expect(calls("/suites/daily/runs")).toHaveLength(1);
});

it("requests cancellation once and monitors until terminal", async () => {
  render(<Harness />); await click("Start analysis"); await click("Cancel run");
  expect(screen.getByText("Cancellation requested; waiting for Databricks.")).toBeTruthy();
  expect((screen.getByRole("button", { name: "Cancellation requested" }) as HTMLButtonElement).disabled).toBe(true);
  await tick();
  expect(calls("/jobs/123")).toHaveLength(2);
  state = { ...state, status: "cancelled", lifecycle: "TERMINATED", done: true };
  await tick(); await tick();
  expect(screen.queryByRole("button", { name: "Cancel run" })).toBeNull();
  expect(calls("/cancel")).toHaveLength(1);
  expect(finished).toHaveBeenCalledTimes(1);
});

it("does not overlap slow status requests", async () => {
  let resolve: (value: Response) => void = () => {};
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/jobs/123")
    ? new Promise(r => { resolve = r; }) : original(url, init));
  render(<Harness />); await click("Start analysis"); await tick(15000);
  expect(calls("/jobs/123")).toHaveLength(1);
  await act(async () => { resolve(respond(state)); }); await tick();
  expect(calls("/jobs/123")).toHaveLength(2);
});

it("shows monitor errors without treating the run as failed or resubmitting", async () => {
  render(<Harness />); await click("Start analysis"); failPoll = true; await tick();
  expect(screen.getByText(/Status unavailable/)).toBeTruthy();
  expect(screen.getByText("1 of 2 comparison tasks complete")).toBeTruthy();
  failPoll = false; await click("Retry status");
  expect(screen.queryByText(/Status unavailable/)).toBeNull();
  expect(calls("/suites/daily/runs")).toHaveLength(1);
});

it("loads the actual persisted Job report and allows retry if it is not available", async () => {
  state = { ...state, status: "failed", lifecycle: "TERMINATED", done: true };
  render(<Harness />); await click("Start analysis"); failReport = true; await click("View results");
  expect(screen.getByText(/Report unavailable/)).toBeTruthy();
  expect(report).not.toHaveBeenCalled();
  failReport = false; await click("View results");
  expect(report).toHaveBeenCalledWith({ run_id: "job:123", status: "failed" });
  expect(calls("/runs/job%3A123")).toHaveLength(2);
});

it("leaves unknown progress indeterminate and does not expose fake percentages", async () => {
  state = { status: "running", lifecycle: "QUEUED", progress: { stage: "queued", counts: null, tasks: [], attempts: [], warnings: [] } };
  render(<Harness />); await click("Start analysis");
  expect(screen.getByRole("progressbar").hasAttribute("value")).toBe(false);
  expect(screen.getByText("Waiting for comparison progress")).toBeTruthy();
});

it("shows submission failure without polling or silently retrying", async () => {
  vi.mocked(fetch).mockResolvedValueOnce(respond({ detail: "Submission unavailable" }, 500));
  render(<Harness />); await click("Start analysis"); await tick(15000);
  expect(screen.getByText(/Submission unavailable/)).toBeTruthy();
  expect(calls("/jobs/123")).toHaveLength(0);
  expect(calls("/suites/daily/runs")).toHaveLength(1);
});

it("ignores an in-flight response after unmount", async () => {
  let resolve: (value: Response) => void = () => {};
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/jobs/123")
    ? new Promise(r => { resolve = r; }) : original(url, init));
  const view = render(<Harness />); await click("Start analysis"); view.unmount();
  await act(async () => { resolve(respond({ status: "passed", done: true })); });
  expect(finished).not.toHaveBeenCalled();
});

it("keeps monitoring after a denied cancellation and permits an explicit retry", async () => {
  const original = vi.mocked(fetch).getMockImplementation()!;
  let denied = true;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/cancel") && denied
    ? Promise.resolve(respond({ detail: "Permission denied" }, 403)) : original(url, init));
  render(<Harness />); await click("Start analysis"); await click("Cancel run");
  expect(screen.getByText("Cancellation failed: Permission denied")).toBeTruthy();
  expect(screen.queryByText("Cancellation requested; waiting for Databricks.")).toBeNull();
  await tick();
  denied = false; await click("Cancel run");
  expect(screen.getByText("Cancellation requested; waiting for Databricks.")).toBeTruthy();
  expect(calls("/suites/daily/runs")).toHaveLength(1);
});

it("treats the dialog Escape action as minimize, never as Job cancellation", async () => {
  render(<Harness />); await click("Start analysis");
  await act(async () => { fireEvent(screen.getByRole("dialog"), new Event("cancel", { bubbles: false, cancelable: true })); });
  expect(screen.queryByRole("dialog")).toBeNull();
  await tick();
  expect(calls("/jobs/123")).toHaveLength(2);
  expect(calls("/cancel")).toHaveLength(0);
});

it("coalesces manual retries while an automatic status retry is still pending", async () => {
  failPoll = true;
  render(<Harness />); await click("Start analysis");
  const original = vi.mocked(fetch).getMockImplementation()!;
  let resolve: (value: Response) => void = () => {};
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/jobs/123")
    ? new Promise(r => { resolve = r; }) : original(url, init));
  await tick();
  const retry = screen.getByRole("button", { name: "Retry status" }) as HTMLButtonElement;
  expect(retry.disabled).toBe(true);
  await click("Retry status"); await click("Retry status");
  expect(calls("/jobs/123")).toHaveLength(2);
  failPoll = false;
  await act(async () => { resolve(respond(state)); });
  expect(screen.queryByRole("button", { name: "Retry status" })).toBeNull();
});

it("does not show waiting progress after a run ends before comparisons start", async () => {
  state = { status: "error", lifecycle: "INTERNAL_ERROR", done: true,
    progress: { stage: "finished", counts: null, tasks: [], attempts: [], warnings: [] } };
  render(<Harness />); await click("Start analysis");
  expect(screen.queryByRole("progressbar")).toBeNull();
  expect(screen.getByText("Comparison progress unavailable for this run")).toBeTruthy();
});

it("does not let a late cancellation failure change a newer run's cancellation", async () => {
  let submissions = 0;
  let oldCancel: (value: Response) => void = () => {};
  vi.mocked(fetch).mockImplementation(async (url: any) => {
    const path = String(url);
    if (path.endsWith("/suites/daily/runs")) return respond({ ...handle, run_id: ++submissions === 1 ? "123" : "456" });
    if (path.endsWith("/jobs/123/cancel")) return new Promise(resolve => { oldCancel = resolve; });
    if (path.endsWith("/jobs/456/cancel")) return new Promise(() => {});
    return respond(state);
  });
  render(<Harness />); await click("Start analysis"); await click("Cancel run");
  state = { ...state, done: true, status: "passed", lifecycle: "TERMINATED" };
  await tick();
  state = { ...state, done: false, status: "running", lifecycle: "RUNNING" };
  await click("Start analysis"); await click("Cancel run");
  await act(async () => { oldCancel(respond({ detail: "Old request denied" }, 403)); });
  expect(screen.queryByText(/Old request denied/)).toBeNull();
  expect((screen.getByRole("button", { name: "Requesting cancellation…" }) as HTMLButtonElement).disabled).toBe(true);
  expect(calls("/jobs/456/cancel")).toHaveLength(1);
});
