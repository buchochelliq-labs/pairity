import React from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, act } from "@testing-library/react";
import { CatalogPairEditor } from "./CatalogPairEditor";
import { Config } from "./api";

const config: Config = {
  schema_version: 1, suite: "test",
  defaults: { implicit_casts: false },
  pairs: [{ id: "p", baseline: "cat.sch.left", candidate: "cat.sch.right",
    compare: { column_policy: "common" }, tags: ["keep"] }],
};
const descriptor = (table: string) => ({ table, primary_key: ["id", "version"], columns: [
  { name: "id", type: "BIGINT", nullable: false },
  { name: "version", type: "INT", nullable: false },
  { name: "other_id", type: "BIGINT", nullable: true },
] });
const keys = [{ baseline: "id", candidate: "id" }, { baseline: "version", candidate: "version" }];
const info = { pair_id: "p", valid: true, key_origin: "declared", keys,
  baseline: descriptor("cat.sch.left"), candidate: descriptor("cat.sch.right"),
  issues: [], warnings: ["Metadata does not prove uniqueness."], casts: [],
  settings: { implicit_casts: false, null_safe: true },
};
const response = (data: unknown, ok = true) => ({ ok, status: ok ? 200 : 500, json: async () => data });
let requests: any[];
beforeEach(() => {
  requests = [];
  vi.stubGlobal("fetch", vi.fn(async (url: string, init: any) => {
    const body = init.body && JSON.parse(init.body);
    requests.push({ url, body });
    if (url.endsWith("/inspect")) {
      const pair = JSON.parse(body.content).pairs[0];
      return response({ ...info, keys: pair.keys || keys });
    }
    if (url.includes("/catalogs")) return response({ items: [{ name: "cat" }], next_after: null });
    if (url.includes("/schemas")) return response({ items: [{ name: "sch" }], next_after: null });
    if (url.includes("after=left")) return response({ items: [{ name: "right" }], next_after: null });
    return response({ items: [{ name: "left" }], next_after: "left" });
  }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

function editor(c = config) {
  const onChange = vi.fn();
  const view = render(<CatalogPairEditor config={c} pairIndex={0} onChange={onChange} />);
  fireEvent.click(screen.getByRole("button", { name: "Browse Unity Catalog" }));
  return { onChange, ...view };
}

it("suggests composite keys only after inspection and preserves pair configuration", async () => {
  const { onChange } = editor();
  fireEvent.click(screen.getByRole("button", { name: "Inspect tables and keys" }));
  await screen.findByText("Metadata checks passed");
  expect(onChange).not.toHaveBeenCalled();
  expect(screen.getByText("Metadata does not prove uniqueness.")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Apply key mappings" }));
  expect(onChange).toHaveBeenCalledWith({ ...config.pairs[0], keys });
  expect(JSON.parse(requests.find(r => r.body).body.content).defaults).toEqual(config.defaults);
});

it("requires revalidation after editing a mapping and supports renamed keys", async () => {
  const { onChange } = editor();
  fireEvent.click(screen.getByRole("button", { name: "Inspect tables and keys" }));
  await screen.findByText("Metadata checks passed");
  fireEvent.change(screen.getByLabelText("Candidate key 1"), { target: { value: "other_id" } });
  expect((screen.getByRole("button", { name: "Apply key mappings" }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Validate key mapping" }));
  await waitFor(() => expect((screen.getByRole("button", { name: "Apply key mappings" }) as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(screen.getByRole("button", { name: "Apply key mappings" }));
  expect(onChange.mock.calls[0][0].keys[0].candidate).toBe("other_id");
});

it("browses pages and applies a new table at its current version", async () => {
  const c = { ...config, pairs: [{ ...config.pairs[0], baseline: { kind: "table", table: "cat.sch.left", version: 2 }, baseline_version: 2 }] };
  const { onChange } = editor(c);
  await waitFor(() => expect(screen.getByLabelText("Baseline catalog").querySelector('option[value="cat"]')).toBeTruthy());
  fireEvent.change(screen.getByLabelText("Baseline catalog"), { target: { value: "cat" } });
  await waitFor(() => expect(screen.getByLabelText("Baseline schema").querySelector('option[value="sch"]')).toBeTruthy());
  fireEvent.change(screen.getByLabelText("Baseline schema"), { target: { value: "sch" } });
  await screen.findByRole("button", { name: "More Baseline table selection" });
  fireEvent.click(screen.getByRole("button", { name: "More Baseline table selection" }));
  await screen.findByRole("option", { name: "right" });
  fireEvent.change(screen.getByLabelText("Baseline table selection"), { target: { value: "right" } });
  fireEvent.click(screen.getByRole("button", { name: "Use selected baseline table" }));
  const changed = onChange.mock.calls[0][0];
  expect(changed.baseline).toBe("cat.sch.right");
  expect(changed.baseline_version).toBeUndefined();
  expect(changed.compare).toEqual(config.pairs[0].compare);
});

it("keeps composed sources in the source editor", () => {
  const source = { kind: "deduplicate", source: "cat.sch.left" };
  const { onChange } = editor({ ...config, pairs: [{ ...config.pairs[0], baseline: source }] });
  expect(screen.getByText(/Baseline uses a composed source/)).toBeTruthy();
  expect(screen.queryByLabelText("Baseline catalog")).toBeNull();
  expect(onChange).not.toHaveBeenCalled();
});

it("shows discovery failure and can retry", async () => {
  vi.mocked(fetch).mockResolvedValueOnce(response({ detail: "Permission denied" }, false) as Response);
  editor();
  await screen.findByText("Permission denied");
  fireEvent.click(screen.getByRole("button", { name: "Retry Baseline catalog" }));
  await waitFor(() => expect(screen.getByLabelText("Baseline catalog").querySelector('option[value="cat"]')).toBeTruthy());
});

it("ignores an inspection response for an older configuration", async () => {
  let finish: (value: any) => void = () => {};
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/inspect")
    ? new Promise(resolve => { finish = resolve; }) : original(url, init));
  const { rerender, onChange } = editor();
  fireEvent.click(screen.getByRole("button", { name: "Inspect tables and keys" }));
  rerender(<CatalogPairEditor config={{ ...config, defaults: { implicit_casts: true } }} pairIndex={0} onChange={onChange} />);
  await act(async () => { finish(response(info)); });
  expect(screen.queryByText("Metadata checks passed")).toBeNull();
  expect(screen.queryByRole("button", { name: "Apply key mappings" })).toBeNull();
});

it("shows empty matches and ignores stale catalog search responses", async () => {
  let finish: (value: any) => void = () => {};
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => {
    if (String(url).includes("search=old")) return new Promise(resolve => { finish = resolve; });
    if (String(url).includes("search=new")) return Promise.resolve(response({ items: [], next_after: null }) as Response);
    return original(url, init);
  });
  editor();
  await waitFor(() => expect(screen.getByLabelText("Baseline catalog").querySelector('option[value="cat"]')).toBeTruthy());
  fireEvent.change(screen.getByLabelText("Search Baseline catalog"), { target: { value: "old" } });
  fireEvent.click(screen.getByRole("button", { name: "Search Baseline catalog" }));
  fireEvent.change(screen.getByLabelText("Search Baseline catalog"), { target: { value: "new" } });
  fireEvent.click(screen.getByRole("button", { name: "Search Baseline catalog" }));
  await screen.findByText("No accessible matches for Baseline catalog.");
  await act(async () => { finish(response({ items: [{ name: "stale" }], next_after: null })); });
  expect(screen.queryByRole("option", { name: "stale" })).toBeNull();
});

it("allows manual key selection when neither table declares primary keys", async () => {
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => String(url).endsWith("/inspect")
    ? Promise.resolve(response({ ...info, valid: false, keys: [],
        baseline: { ...info.baseline, primary_key: [] }, candidate: { ...info.candidate, primary_key: [] },
        issues: ["Supply explicit key mappings"] }) as Response)
    : original(url, init));
  editor();
  fireEvent.click(screen.getByRole("button", { name: "Inspect tables and keys" }));
  await screen.findByText("Supply explicit key mappings");
  expect(screen.queryByLabelText("Baseline key 1")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Add key column" }));
  fireEvent.change(screen.getByLabelText("Baseline key 1"), { target: { value: "id" } });
  fireEvent.change(screen.getByLabelText("Candidate key 1"), { target: { value: "other_id" } });
  expect((screen.getByRole("button", { name: "Validate key mapping" }) as HTMLButtonElement).disabled).toBe(false);
  expect((screen.getByRole("button", { name: "Apply key mappings" }) as HTMLButtonElement).disabled).toBe(true);
});
