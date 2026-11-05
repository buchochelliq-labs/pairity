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
beforeEach(() => {
  saved = null;
  calls = [];
  conflict = false;
  jobState = null;
  historyFailure = false;
  suiteFailure = false;
  unavailableCount = 0;
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
        } else {
          saved =
            body.format === "yaml"
              ? parse(body.content)
              : JSON.parse(body.content);
          data = { config: saved, revision: 1, config_hash: "abc123" };
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
          name: /Run saved suite/,
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
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
