export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function api<T = any>(
  path: string,
  body?: unknown,
  method = body === undefined ? "GET" : "POST",
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(`/api/v1${path}`, {
    method,
    credentials: "same-origin",
    signal,
    headers:
      body === undefined
        ? {}
        : { "Content-Type": "application/json", "X-Pairity-Request": "1" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok)
    throw new ApiError(
      response.status,
      response.status === 409
        ? "This suite changed since you opened it. Review the latest revision before applying your changes."
        : typeof data.detail === "string"
          ? data.detail
          : JSON.stringify(data.detail || data),
    );
  return data;
}
export const encoded = encodeURIComponent;
export const pretty = (value: unknown) => JSON.stringify(value, null, 2);
export type Pair = {
  id: string;
  baseline: any;
  candidate: any;
  keys?: { baseline: string; candidate: string }[] | null;
  tags?: string[];
  enabled?: boolean;
  [key: string]: any;
};
export type Config = {
  schema_version: 1;
  suite: string;
  pairs: Pair[];
  [key: string]: any;
};
