import { useEffect, useRef, useState } from "react";
import { api } from "./api";

type Named = { name: string; full_name?: string };
type Page<T> = { items: T[]; next_after: string | null };

export function useMetadataPage<T extends Named>(path: string) {
  const [page, setPage] = useState<Page<T> & { path: string }>({ path: "", items: [], next_after: null });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [retryAfter, setRetryAfter] = useState("");
  const generation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const inFlight = useRef(false);

  async function load(after = "") {
    const request = ++generation.current;
    controller.current?.abort();
    controller.current = new AbortController();
    inFlight.current = true;
    setLoading(true); setError(""); setRetryAfter(after);
    if (!after) setPage({ path, items: [], next_after: null });
    try {
      const next = await api<Page<T>>(`${path}${path.includes("?") ? "&" : "?"}${new URLSearchParams({ after, limit: "50" })}`,
        undefined, "GET", controller.current.signal);
      if (request !== generation.current) return;
      if (!Array.isArray(next.items)) throw new Error("Catalog returned an invalid page");
      setPage(previous => {
        const items = after && previous.path === path ? [...previous.items, ...next.items] : next.items;
        return { path, items: [...new Map(items.map(item => [item.full_name || item.name, item])).values()], next_after: next.next_after };
      });
    } catch (e) {
      if (request === generation.current) setError((e as Error).message);
    } finally {
      if (request === generation.current) { setLoading(false); inFlight.current = false; }
    }
  }
  useEffect(() => {
    void load();
    return () => { generation.current++; controller.current?.abort(); inFlight.current = false; };
  }, [path]);
  const current = page.path === path;
  return {
    items: current ? page.items : [], next: current ? page.next_after : null,
    loading: loading || !current, error: current ? error : "",
    more: () => { if (!inFlight.current && page.next_after) void load(page.next_after); },
    retry: () => { if (!inFlight.current) void load(retryAfter); },
  };
}
