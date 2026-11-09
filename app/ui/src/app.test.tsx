import React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  render,
  screen,
  fireEvent,
  waitFor,
  cleanup,
} from "@testing-library/react";
import { App } from "./main";
import { parse } from "yaml";
const cfg = {
  schema_version: 1,
  suite: "test",
  pairs: [
    {
      id: "customers",
      baseline: "a.b.c",
      candidate: "a.b.d",
      keys: [{ baseline: "id", candidate: "id" }],
      enabled: true,
      tags: [],
    },
  ],
};
let saved: any = null;
let calls: any[] = [];
let conflict = false;
let jobState: any = null;
let historyFailure = false;
let suiteFailure = false;
let unavailableCount = 0;
let savedRevision = 0;
let saveError = "";
beforeEach(() => {
  saved = null;
  calls = [];
  conflict = false;
  jobState = null;
  historyFailure = false;
  suiteFailure = false;
  unavailableCount = 0;
  savedRevision = 0;
  saveError = "";
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init: any) => {
      const path = url.replace("/api/v1", "");
      const body = init.body ? JSON.parse(init.body) : null;
      calls.push({ path, body, init });
      let data: any = {};
      let status = 200;
      if (path === "/session") data = { user: "local", mode: "local" };
      else if (path === "/suites") {
        if (suiteFailure) {
          status = 503;
          data = { detail: "Suites unavailable" };
        } else
          data = {
            items: saved ? [{ suite: "test", revision: 1, pairs: 1 }] : [],
          };
      } else if (path.startsWith("/runs?")) {
        if (historyFailure) {
          status = 503;
          data = { detail: "History unavailable" };
        } else data = { items: [], unavailable_count: unavailableCount };
      } else if (path === "/preview" || path === "/validate")
        data = {
          config:
            body.format === "yaml"
              ? parse(body.content)
              : JSON.parse(body.content),
          config_hash: "abc123",
          effective: [
            {
              pair_id: "customers",
              settings: { null_safe: true },
              keys: [{ baseline: "id", candidate: "id" }],
              needs_keys: false,
            },
          ],
          preview: { added: ["customers"] },
        };
      else if (path === "/suites/test" && init.method === "PUT") {
        if (conflict) {
          status = 409;
          data = { detail: "conflict" };
        } else if (saveError) {
          status = 422;
          data = { detail: saveError };
        } else {
          saved =
            body.format === "yaml"
              ? parse(body.content)
              : JSON.parse(body.content);
          savedRevision = body.expected_revision + 1;
          data = { config: saved, revision: savedRevision, config_hash: "abc123" };
        }
      } else if (path === "/suites/test" && init.method === "GET") {
        data = { config: saved, revision: 1, config_hash: "abc123" };
      } else if (path === "/suites/test/patch") {
        if (
          (!body.pair_ids?.length && !body.tags?.length) ||
          (body.pair_ids?.length && body.tags?.length)
        ) {
          status = 422;
          data = { detail: "Select exactly one target mode" };
        } else
          data = {
            preview: { changed: ["customers"] },
            revision: 1,
            config_hash: "abc123",
            applied: body.apply,
          };
      } else if (path === "/suites/test/runs") {
        if (body.pair_ids?.length && body.tags?.length) {
          status = 422;
          data = { detail: "Select exactly one target mode" };
        } else
          data = jobState
            ? { backend: "databricks-jobs", run_id: "123" }
            : { backend: "local", run_id: "run-1" };
      } else if (path === "/jobs/123") data = jobState;
      else if (path === "/runs/run-1")
        data = {
          suite: "test",
          run_id: "run-1",
          status: "blocked",
          config_hash: "abc123",
          pairs: [
            {
              pair_id: "customers",
              status: "blocked",
              counts: null,
              reason: "Missing key metadata",
              casts: [],
              ignored_columns: {},
              schema_differences: [],
            },
          ],
        };
      return { ok: status < 400, status, json: async () => data };
    }),
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
async function editor() {
  render(<App />);
  await waitFor(() => expect(screen.queryByText("Working…")).toBeNull());
  fireEvent.click(screen.getByRole("button", { name: "Configurations" }));
  fireEvent.change(screen.getByLabelText("Configuration source"), {
    target: { value: JSON.stringify(cfg) },
  });
}
describe("workbench workflow", () => {
  it.each(["json", "yaml"])("saves and runs a new %s configuration without a separate preview", async (format) => {
    await editor();
    if (format === "yaml")
      fireEvent.change(screen.getByLabelText("Format"), { target: { value: format } });
    const button = screen.getByRole("button", { name: /Save & run/ }) as HTMLButtonElement;
    expect(button.disabled).toBe(false);
    fireEvent.click(button);
    await screen.findByText("Row counts unavailable for this comparison.");
    const writes = calls.filter(c => c.init.method === "PUT" || c.path.endsWith("/runs"));
    expect(writes.map(c => c.path)).toEqual(["/suites/test", "/suites/test/runs"]);
    expect(writes[0].body).toMatchObject({ format, expected_revision: 0 });
    expect(writes[1].body.revision).toBe(1);
    expect(saved.pairs[0].baseline).toBe("a.b.c");
  });
  it("refuses to run the starter suite's example tables without saving or submitting", async () => {
    render(<App />);
    await waitFor(() => expect(screen.queryByText("Working…")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "Configurations" }));
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    const status = await screen.findByText(/Unable to start analysis: replace the example tables/);
    expect(status.textContent).toContain("catalog.baseline.customers and catalog.candidate.customers");
    expect(calls.filter(c => c.init.method === "PUT" || c.path.endsWith("/runs"))).toHaveLength(0);
  });
  it("runs a selected real pair even when an unselected pair still uses example tables", async () => {
    render(<App />);
    await waitFor(() => expect(screen.queryByText("Working…")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "Configurations" }));
    const mixed = {
      ...cfg,
      pairs: [
        ...cfg.pairs,
        {
          id: "starter",
          baseline: "catalog.baseline.customers",
          candidate: "catalog.candidate.customers",
          keys: [{ baseline: "id", candidate: "id" }],
          tags: [],
          enabled: true,
        },
      ],
    };
    fireEvent.change(screen.getByLabelText("Configuration source"), {
      target: { value: JSON.stringify(mixed) },
    });
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByText(/Unable to start analysis: replace the example tables/);
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
    fireEvent.click(screen.getByLabelText(cfg.pairs[0].id));
    fireEvent.click(screen.getByRole("button", { name: /Save & run selected pairs/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    const request = calls.find(c => c.path.endsWith("/runs"));
    expect(request.body.pair_ids).toEqual([cfg.pairs[0].id]);
  });
  it("saves changed YAML before running the newly returned revision and selected tags", async () => {
    saved = cfg;
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
    await screen.findByText("test · revision 1");
    fireEvent.change(screen.getByLabelText("Format"), { target: { value: "yaml" } });
    fireEvent.change(screen.getByLabelText("Candidate table"), { target: { value: "a.b.updated" } });
    fireEvent.change(screen.getByLabelText("Filter tags (comma separated)"), { target: { value: "daily" } });
    fireEvent.click(screen.getByRole("button", { name: /Save & run tagged pairs/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(saved.pairs[0].candidate).toBe("a.b.updated");
    const request = calls.find(c => c.path === "/suites/test/runs");
    expect(request.body).toMatchObject({ revision: 2, tags: ["daily"] });
    expect(request.body).not.toHaveProperty("pair_ids");
    expect(calls.find(c => c.init.method === "PUT").body.expected_revision).toBe(1);
  });
  it.each(["validation", "conflict"])("does not run when saving fails with a %s error", async (failure) => {
    await editor();
    if (failure === "conflict") conflict = true;
    else saveError = "Candidate source is required";
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByRole("alert");
    expect(screen.getByRole("alert").textContent).toContain(
      failure === "conflict" ? "changed since you opened" : "Candidate source is required",
    );
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
    expect(saved).toBeNull();
    const retry = screen.getByRole("button", { name: /Save & run/ }) as HTMLButtonElement;
    expect(retry.disabled).toBe(failure === "conflict");
    expect(document.getElementById(retry.getAttribute("aria-describedby")!)!.textContent).toContain(
      failure === "conflict" ? "changed since you opened" : "Candidate source is required",
    );
  });
  it("recovers an imported suite conflict by reviewing the latest revision without losing edits", async () => {
    saved = cfg;
    await editor();
    fireEvent.change(screen.getByLabelText("Candidate table"), { target: { value: "a.b.my_edit" } });
    conflict = true;
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByRole("alert");
    conflict = false;
    fireEvent.click(screen.getByRole("button", { name: "Review latest revision" }));
    await screen.findByText("Review changes");
    expect((screen.getByLabelText("Candidate table") as HTMLInputElement).value).toBe("a.b.my_edit");
    expect(calls.find(c => c.path === "/preview").body.expected_revision).toBe(1);
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(saved.pairs[0].candidate).toBe("a.b.my_edit");
    expect(calls.filter(c => c.init.method === "PUT").map(c => c.body.expected_revision)).toEqual([0, 1]);
    expect(calls.find(c => c.path.endsWith("/runs")).body.revision).toBe(2);
  });
  it("does not advance the editor revision when another change invalidates conflict recovery", async () => {
    saved = cfg;
    await editor();
    conflict = true;
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByRole("alert");
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/preview")
      ? Promise.resolve({ ok: false, status: 409, json: async () => ({ detail: "changed again" }) } as Response)
      : original(url, init));
    fireEvent.click(screen.getByRole("button", { name: "Review latest revision" }));
    await screen.findByRole("alert");
    expect(screen.getByText("Unsaved configuration")).toBeTruthy();
    expect(screen.queryByText("Review changes")).toBeNull();
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
  });
  it("reopens active progress from the run action without submitting another job", async () => {
    saved = cfg;
    jobState = { status: "running", lifecycle: "RUNNING", run_id: "123", done: false };
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
    await screen.findByText("test · revision 1");
    fireEvent.click(screen.getByRole("button", { name: /Run selected pairs/ }));
    await screen.findByText("Analysis running");
    fireEvent.click(screen.getByRole("button", { name: "Minimize" }));
    const button = screen.getByRole("button", { name: "View analysis progress" }) as HTMLButtonElement;
    expect(button.disabled).toBe(false);
    fireEvent.click(button);
    expect(screen.getByRole("dialog", { name: "Analysis progress" })).toBeTruthy();
    expect(calls.filter(c => c.path === "/suites/test/runs")).toHaveLength(1);
    expect(calls.filter(c => c.init.method === "PUT")).toHaveLength(0);
  });
  it("waits for the save, locks edits and submits once despite repeated clicks", async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    let releaseSave!: () => void;
    const saving = new Promise<void>(resolve => { releaseSave = resolve; });
    vi.mocked(fetch).mockImplementation(async (url: any, init: any) => {
      if (init.method === "PUT") await saving;
      return original(url, init);
    });
    await editor();
    const button = screen.getByRole("button", { name: /Save & run/ });
    fireEvent.click(button);
    await screen.findByText("Saving before run…");
    fireEvent.click(button);
    expect(screen.getByLabelText("Candidate table").matches(":disabled")).toBe(true);
    expect(screen.getByRole("button", { name: "New suite" }).matches(":disabled")).toBe(true);
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
    releaseSave();
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(calls.filter(c => c.init.method === "PUT")).toHaveLength(1);
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(1);
  });
  it("explains malformed source next to the disabled action and still allows validation", async () => {
    await editor();
    fireEvent.change(screen.getByLabelText("Configuration source"), { target: { value: "{" } });
    const button = screen.getByRole("button", { name: /Save & run/ }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    const description = document.getElementById(button.getAttribute("aria-describedby")!)!;
    expect(description.textContent).toContain("Use Validate");
    expect(screen.getByRole("button", { name: "Validate" }).matches(":disabled")).toBe(false);
    expect(calls.filter(c => c.path.endsWith("/runs"))).toHaveLength(0);
  });
  it("retains the saved revision after a submission error so retrying does not save again", async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((url: any, init: any) => {
      if (String(url).endsWith("/suites/test/runs"))
        return Promise.resolve({ ok: false, status: 403, json: async () => ({ detail: "Run permission required" }) } as Response);
      return original(url, init);
    });
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByText(/Run permission required/);
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    const button = screen.getByRole("button", { name: /Run saved suite/ }) as HTMLButtonElement;
    expect(button.disabled).toBe(false);
    expect(document.getElementById(button.getAttribute("aria-describedby")!)!.textContent).toContain("Run permission required");
    vi.mocked(fetch).mockImplementation(original);
    fireEvent.click(button);
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(calls.filter(c => c.init.method === "PUT")).toHaveLength(1);
    expect(calls.find(c => c.path.endsWith("/runs")).body.revision).toBe(1);
  });
  it("keeps editing locked when a history request finishes during a pending save", async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    let releaseSave!: () => void;
    const saving = new Promise<void>(resolve => { releaseSave = resolve; });
    vi.mocked(fetch).mockImplementation(async (url: any, init: any) => {
      if (String(url).includes("/runs?"))
        return { ok: true, json: async () => ({ items: [{ suite: "test", run_id: "run-1", status: "passed", pairs: 1 }] }) } as Response;
      if (init.method === "PUT") await saving;
      return original(url, init);
    });
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /Save & run/ }));
    await screen.findByText("Saving before run…");
    try {
      fireEvent.click(screen.getByRole("button", { name: "Run history" }));
      fireEvent.click(screen.getByRole("button", { name: "Review →" }));
      await screen.findByText("Row counts unavailable for this comparison.");
      fireEvent.click(screen.getByRole("button", { name: "Configurations" }));
      expect(screen.getByLabelText("Configuration source").matches(":disabled")).toBe(true);
      expect(screen.getByRole("button", { name: "New suite" }).matches(":disabled")).toBe(true);
    } finally { releaseSave(); }
    await waitFor(() => expect(calls.filter(c => c.path === "/suites/test/runs")).toHaveLength(1));
    await screen.findByText("Row counts unavailable for this comparison.");
  });
  it("creates a renamed suite at revision zero and runs that destination", async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((url: any, init: any) => {
      const path = String(url).replace("/api/v1", "");
      if (path.startsWith("/suites/renamed")) {
        const body = JSON.parse(init.body);
        calls.push({ path, body, init });
        const data = init.method === "PUT"
          ? { config: JSON.parse(body.content), revision: 1, config_hash: "renamed-hash" }
          : { backend: "local", run_id: "run-1" };
        return Promise.resolve({ ok: true, json: async () => data } as Response);
      }
      return original(url, init);
    });
    saved = cfg;
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
    await screen.findByText("test · revision 1");
    fireEvent.change(screen.getByLabelText("Suite name"), { target: { value: "renamed" } });
    fireEvent.click(screen.getByRole("button", { name: /Save & run selected pairs/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(calls.find(c => c.path === "/suites/renamed").body.expected_revision).toBe(0);
    expect(calls.find(c => c.path === "/suites/renamed/runs").body).toMatchObject({ revision: 1, pair_ids: ["customers"] });
  });
  it("applies catalog keys to YAML and invalidates an existing preview", async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((url: any, init: any) => {
      if (String(url).includes("/discovery/")) {
        const describe = (table: string) => ({ table, primary_key: ["version"],
          columns: [{ name: "version", type: "INT", nullable: false }] });
        const data = String(url).endsWith("/inspect") ? {
          valid: true, key_origin: "declared", keys: [{ baseline: "version", candidate: "version" }],
          baseline: describe("a.b.c"), candidate: describe("a.b.d"),
          issues: [], casts: [], warnings: [], settings: { implicit_casts: true, null_safe: true },
        } : { items: [], next_after: null };
        return Promise.resolve({ ok: true, json: async () => data } as Response);
      }
      return original(url, init);
    });
    await editor();
    fireEvent.change(screen.getByLabelText("Format"), { target: { value: "yaml" } });
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText("Review changes");
    fireEvent.click(screen.getByRole("button", { name: "Browse Unity Catalog" }));
    fireEvent.click(screen.getByRole("button", { name: "Inspect tables and keys" }));
    await screen.findByText("Metadata checks passed");
    fireEvent.click(screen.getByRole("button", { name: "Apply key mappings" }));
    const text = (screen.getByLabelText("Configuration source") as HTMLTextAreaElement).value;
    expect(parse(text).pairs[0].keys).toEqual([{ baseline: "version", candidate: "version" }]);
    expect((screen.getByLabelText("Format") as HTMLSelectElement).value).toBe("yaml");
    expect((screen.getByRole("button", { name: "Apply & save" }) as HTMLButtonElement).disabled).toBe(true);
  });
  it("imports configuration text, edits, previews, saves, runs a snapshot and reviews unavailable counts", async () => {
    await editor();
    fireEvent.change(screen.getByLabelText("Candidate table"), {
      target: { value: "a.b.updated" },
    });
    expect(
      (
        screen.getByRole("button", {
          name: "Apply & save",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText("Review changes");
    fireEvent.click(screen.getByRole("button", { name: "Apply & save" }));
    await screen.findByText("Saved revision 1");
    fireEvent.click(screen.getByRole("button", { name: /Run saved suite/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    expect(screen.getByText("Missing key metadata")).toBeTruthy();
    const run = calls.find((c) => c.path === "/suites/test/runs");
    expect(run.body.revision).toBe(1);
    expect(run.body.request_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(run.init.headers["X-Pairity-Request"]).toBe("1");
    expect(saved.pairs[0].candidate).toBe("a.b.updated");
    expect(
      screen.getByText("Download pair incident bundle ↓").getAttribute("href"),
    ).toContain("pair_id=customers");
  });
  it("invalidates preview on edits and never overwrites a revision conflict", async () => {
    await editor();
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText("Review changes");
    fireEvent.change(screen.getByLabelText("Candidate table"), {
      target: { value: "a.b.updated" },
    });
    expect(
      (
        screen.getByRole("button", {
          name: "Apply & save",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText("Review changes");
    conflict = true;
    fireEvent.click(screen.getByRole("button", { name: "Apply & save" }));
    await screen.findByRole("alert");
    expect(screen.getByRole("alert").textContent).toContain(
      "changed since you opened",
    );
    expect(saved).toBeNull();
    expect(calls.filter((c) => c.init.method === "PUT")).toHaveLength(1);
  });
  it("preserves invalid source for server validation without crashing", async () => {
    await editor();
    fireEvent.change(screen.getByLabelText("Configuration source"), {
      target: { value: '{"pairs":[null]}' },
    });
    expect(
      (screen.getByLabelText("Configuration source") as HTMLTextAreaElement)
        .value,
    ).toBe('{"pairs":[null]}');
    expect(
      (
        screen.getByRole("button", {
          name: "Apply & save",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
  });
  it("imports a YAML file and previews creation at revision zero", async () => {
    await editor();
    const content =
      "schema_version: 1\nsuite: test\npairs:\n  - id: customers\n    baseline: a.b.c\n    candidate: a.b.d\n";
    const file = new File([content], "suite.yaml", {
      type: "application/yaml",
    });
    Object.defineProperty(file, "text", { value: async () => content });
    fireEvent.change(screen.getByLabelText("Import JSON / YAML"), {
      target: { files: [file] },
    });
    await waitFor(() =>
      expect((screen.getByLabelText("Format") as HTMLSelectElement).value).toBe(
        "yaml",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));
    await screen.findByText("Review changes");
    const request = calls.find((c) => c.path === "/preview");
    expect(request.body.format).toBe("yaml");
    expect(request.body.expected_revision).toBe(0);
    expect(request.body.content).toBe(content);
    fireEvent.click(screen.getByRole("button", { name: "Apply & save" }));
    await screen.findByText("Saved revision 1");
    fireEvent.change(screen.getByLabelText("Candidate table"), {
      target: { value: "a.b.unsaved" },
    });
    expect(
      (
        screen.getByRole("button", {
          name: /Save & run suite/,
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
    expect(calls.filter(c => c.path === "/suites/test/runs")).toHaveLength(0);
  });
  it("accepts multiple mapped keys and tags in the guided editor", async () => {
    await editor();
    const keys = screen.getByLabelText("Keys (baseline:candidate, …)");
    fireEvent.change(keys, { target: { value: "customer_id:id, region" } });
    fireEvent.blur(keys);
    const tags = screen.getByLabelText("Tags (comma separated)");
    fireEvent.change(tags, { target: { value: "daily, critical" } });
    fireEvent.blur(tags);
    const source = JSON.parse(
      (screen.getByLabelText("Configuration source") as HTMLTextAreaElement)
        .value,
    );
    expect(source.pairs[0].keys).toEqual([
      { baseline: "customer_id", candidate: "id" },
      { baseline: "region", candidate: "region" },
    ]);
    expect(source.pairs[0].tags).toEqual(["daily", "critical"]);
  });

  it("runs tag-selected pairs without sending preselected pair IDs", async () => {
    saved = cfg;
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
    await screen.findByText("test · revision 1");
    expect(
      (screen.getByRole("checkbox", { name: "customers" }) as HTMLInputElement)
        .checked,
    ).toBe(true);
    fireEvent.change(screen.getByLabelText("Filter tags (comma separated)"), {
      target: { value: "daily, critical" },
    });
    expect(
      (screen.getByRole("checkbox", { name: "customers" }) as HTMLInputElement)
        .checked,
    ).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: /Run tagged pairs/ }));
    await screen.findByText("Row counts unavailable for this comparison.");
    const run = calls.find((c) => c.path === "/suites/test/runs");
    expect(run.body.tags).toEqual(["daily", "critical"]);
    expect(run.body).not.toHaveProperty("pair_ids");
  });

  it("requires patch targets and switches between exclusive pair and tag modes", async () => {
    saved = cfg;
    await editor();
    fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
    await screen.findByText("test · revision 1");
    fireEvent.click(screen.getByText("Bulk patch saved pairs"));
    fireEvent.click(screen.getByRole("checkbox", { name: "customers" }));
    expect(
      (
        screen.getByRole("button", {
          name: "Preview patch",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.change(screen.getByLabelText("Filter tags (comma separated)"), {
      target: { value: "daily" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Preview patch" }));
    await waitFor(() =>
      expect(
        (
          screen.getByRole("button", {
            name: "Apply patch",
          }) as HTMLButtonElement
        ).disabled,
      ).toBe(false),
    );
    fireEvent.click(screen.getByRole("button", { name: "Apply patch" }));
    await screen.findByText("Patch applied");
    const patches = calls.filter((c) => c.path === "/suites/test/patch");
    expect(patches.map((c) => c.body.apply)).toEqual([false, true]);
    for (const request of patches) {
      expect(request.body.tags).toEqual(["daily"]);
      expect(request.body).not.toHaveProperty("pair_ids");
    }
    expect(
      (
        screen.getByLabelText(
          "Filter tags (comma separated)",
        ) as HTMLInputElement
      ).value,
    ).toBe("");
    fireEvent.change(screen.getByLabelText("Filter tags (comma separated)"), {
      target: { value: "daily" },
    });
    fireEvent.click(screen.getByRole("checkbox", { name: "customers" }));
    expect(
      (
        screen.getByLabelText(
          "Filter tags (comma separated)",
        ) as HTMLInputElement
      ).value,
    ).toBe("");
    fireEvent.click(screen.getByRole("button", { name: "Preview patch" }));
    await waitFor(() =>
      expect(calls.filter((c) => c.path === "/suites/test/patch")).toHaveLength(
        3,
      ),
    );
    const pairPatch = calls.filter((c) => c.path === "/suites/test/patch")[2];
    expect(pairPatch.body.pair_ids).toEqual(["customers"]);
    expect(pairPatch.body).not.toHaveProperty("tags");
  });
  it.each([
    ["passed", "TERMINATED", "SUCCESS"],
    ["failed", "TERMINATED", "FAILED"],
    ["cancelled", "TERMINATED", "CANCELED"],
    ["error", "INTERNAL_ERROR", null],
    ["error", "SKIPPED", null],
  ])(
    "recognizes backend terminal job status %s / %s",
    async (status, lifecycle, result_state) => {
      saved = cfg;
      jobState = { status, lifecycle, result_state, run_id: "123" };
      await editor();
      fireEvent.click(screen.getByRole("button", { name: /test Revision 1/ }));
      await screen.findByText("test · revision 1");
      fireEvent.click(
        screen.getByRole("button", { name: /Run selected pairs/ }),
      );
      await screen.findByText(status);
      await waitFor(() =>
        expect(screen.queryByRole("button", { name: "Cancel run" })).toBeNull(),
      );
      expect(calls.filter((c) => c.path === "/jobs/123")).toHaveLength(1);
      expect(calls.some((c) => c.path === "/runs/123")).toBe(false);
    },
  );
  it("loads saved suites even when history fails and marks run counts unavailable", async () => {
    saved = cfg;
    historyFailure = true;
    render(<App />);
    await screen.findByText("History unavailable");
    const passedCard = screen.getByText("Passed").closest(".stat")!;
    expect(passedCard.querySelector("strong")!.textContent).toBe("—");
    fireEvent.click(screen.getByRole("button", { name: "Configurations" }));
    expect(
      screen.getByRole("button", { name: /test Revision 1/ }),
    ).toBeTruthy();
  });
  it("retains successful history loading when suite discovery fails", async () => {
    suiteFailure = true;
    unavailableCount = 2;
    render(<App />);
    await screen.findByText("Suites unavailable");
    expect(screen.getByText(/2 run reports are unavailable/)).toBeTruthy();
    expect(
      screen
        .getByText("Configurations", { selector: ".stat span" })
        .closest(".stat")!
        .querySelector("strong")!.textContent,
    ).toBe("—");
    expect(
      screen.getByText("Recent runs").closest(".stat")!.querySelector("strong")!
        .textContent,
    ).toBe("0");
  });
});
