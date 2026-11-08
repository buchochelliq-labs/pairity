import React, { useState } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { CatalogExplorer } from "./CatalogExplorer";
import { Pair } from "./api";

const table = (schema: string, name = "member") => ({ catalog: "cat", schema, name, full_name: `cat.${schema}.${name}` });
const response = (value: unknown, ok = true) => ({ ok, status: ok ? 200 : 500, json: async () => value });
let requests: string[];
beforeEach(() => {
  requests = [];
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    requests.push(url);
    if (url.includes("/catalogs")) return response({ items: [{ name: "cat" }], next_after: null });
    if (url.includes("/schemas")) return response({ items: [{ name: "baseline" }, { name: "candidate" }], next_after: null });
    if (url.includes("/table?")) return response({ table: "cat.baseline.member", primary_key: ["id"], columns: [{ name: "id", type: "BIGINT", nullable: false }] });
    const params = new URL(url, "https://local").searchParams;
    const items = params.get("schema") ? [table(params.get("schema")!)] : [table("baseline"), table("candidate")];
    return response({ items, next_after: null });
  }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
function Editor({ source = "old.schema.left" }: { source?: any }) {
  const [pair, setPair] = useState<Pair>({ id: "member", baseline: source, candidate: "old.schema.right" });
  return <CatalogExplorer pair={pair} onSelect={(side, name) => setPair(p => ({ ...p, [side]: name }))} />;
}

it("keeps the baseline visible while searching across schemas for a candidate", async () => {
  render(<Editor />);
  fireEvent.click(await screen.findByRole("button", { name: "Compare cat.baseline.member with…" }));
  expect(within(screen.getByRole("region", { name: "Baseline selection" })).getByText("cat.baseline.member")).toBeTruthy();
  expect((screen.getByRole("searchbox", { name: "Search tables" }) as HTMLInputElement).value).toBe("member");
  fireEvent.click(await screen.findByRole("button", { name: "Use cat.candidate.member as candidate" }));
  expect(within(screen.getByRole("region", { name: "Candidate selection" })).getByText("cat.candidate.member")).toBeTruthy();
});

it("navigates catalog/schema scopes without duplicate selectors and can reset to all catalogs", async () => {
  render(<Editor />);
  fireEvent.click(await screen.findByRole("button", { name: "Catalog cat" }));
  fireEvent.click(await screen.findByRole("button", { name: "Schema cat.candidate" }));
  await waitFor(() => expect(requests.some(r => r.includes("/search?") && r.includes("schema=candidate"))).toBe(true));
  expect(screen.queryByRole("combobox")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "All catalogs" }));
  await screen.findByRole("button", { name: "Inspect cat.baseline.member" });
});

it("inspects columns and declared keys without changing either selection", async () => {
  render(<Editor />);
  fireEvent.click(await screen.findByRole("button", { name: "Inspect cat.baseline.member" }));
  await screen.findByRole("table", { name: "Table columns" });
  expect(screen.getByText("BIGINT")).toBeTruthy();
  expect(screen.getByText(/not a uniqueness check/)).toBeTruthy();
  expect(within(screen.getByRole("region", { name: "Baseline selection" })).getByText("old.schema.left")).toBeTruthy();
});

it("does not replace a composed source through the table explorer", async () => {
  render(<Editor source={{ kind: "deduplicate", source: "cat.baseline.member" }} />);
  expect(screen.getByText(/Baseline uses a composed source/)).toBeTruthy();
  expect(screen.getByRole("button", { name: "Choose baseline" }).matches(":disabled")).toBe(true);
  expect(await screen.findByRole("button", { name: "Use cat.candidate.member as candidate" })).toBeTruthy();
});

it.each(["", { kind: "table", table: "" }])("fills both empty table sources through the explorer (%j)", async source => {
  function EmptyEditor() {
    const [pair, setPair] = useState<Pair>({ id: "member", baseline: source, candidate: source });
    return <CatalogExplorer pair={pair} onSelect={(side, name) => setPair(p => ({ ...p, [side]: name }))} />;
  }
  render(<EmptyEditor />);
  expect(screen.getByRole("button", { name: "Choose baseline" }).matches(":disabled")).toBe(false);
  expect(screen.getByRole("button", { name: "Choose candidate" }).matches(":disabled")).toBe(false);
  expect(screen.getAllByText("No table selected")).toHaveLength(2);
  fireEvent.click(await screen.findByRole("button", { name: "Compare cat.baseline.member with…" }));
  expect(within(screen.getByRole("region", { name: "Baseline selection" })).getByText("cat.baseline.member")).toBeTruthy();
  fireEvent.click(await screen.findByRole("button", { name: "Use cat.candidate.member as candidate" }));
  expect(within(screen.getByRole("region", { name: "Candidate selection" })).getByText("cat.candidate.member")).toBeTruthy();
});

it("ignores stale search responses and gives an explicit empty state", async () => {
  const original = vi.mocked(fetch).getMockImplementation()!;
  let finish!: (r: any) => void;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => {
    if (String(url).includes("/search?") && String(url).includes("search=old")) return new Promise(resolve => { finish = resolve; });
    if (String(url).includes("/search?") && String(url).includes("search=missing")) return Promise.resolve(response({ items: [], next_after: null }) as Response);
    return original(url, init);
  });
  render(<Editor />);
  const input = screen.getByRole("searchbox", { name: "Search tables" });
  fireEvent.change(input, { target: { value: "old" } });
  fireEvent.submit(input.closest("form")!);
  await waitFor(() => expect(finish).toBeTypeOf("function"));
  fireEvent.change(input, { target: { value: "missing" } });
  fireEvent.submit(input.closest("form")!);
  await screen.findByText("No matching Delta tables in this scope.");
  await act(async () => { finish(response({ items: [table("stale")], next_after: null })); });
  expect(screen.queryByRole("button", { name: "Inspect cat.stale.member" })).toBeNull();
});

it("retries a failed next page without losing or duplicating the first page", async () => {
  const original = vi.mocked(fetch).getMockImplementation()!;
  let failed = false;
  vi.mocked(fetch).mockImplementation((url: any, init: any) => {
    if (!String(url).includes("/search?")) return original(url, init);
    const after = new URL(String(url), "https://local").searchParams.get("after");
    if (!after) return Promise.resolve(response({ items: [table("baseline")], next_after: "cat.baseline.member" }) as Response);
    if (!failed) { failed = true; return Promise.resolve(response({ detail: "Page unavailable" }, false) as Response); }
    return Promise.resolve(response({ items: [table("candidate")], next_after: null }) as Response);
  });
  render(<Editor />);
  fireEvent.click(await screen.findByRole("button", { name: "Load more tables" }));
  await screen.findByText("Page unavailable");
  expect(screen.getAllByRole("button", { name: "Inspect cat.baseline.member" })).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "Retry tables" }));
  await screen.findByRole("button", { name: "Inspect cat.candidate.member" });
  expect(screen.getAllByRole("button", { name: "Inspect cat.baseline.member" })).toHaveLength(1);
});
