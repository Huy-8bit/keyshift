// Typed client for the KeyShift controller API. Types mirror the JSON
// produced by internal/controller.

export interface Rates {
  reads: number;
  local_hits: number;
  redis_reads: number;
  writes: number;
  invalidations: number;
  evictions: number;
  admissions: number;
  demotions: number;
}

export interface Warning {
  severity: "info" | "warning" | "critical";
  code: string;
  message: string;
  count?: number;
}

export interface Overview {
  generated_at: string;
  active_instances: number;
  stale_instances: number;
  services: number;
  rates: Rates;
  local_hit_ratio: number;
  cache_entries: number;
  cache_memory_bytes: number;
  cache_memory_limit_bytes: number;
  admitted_keys: number;
  tracking: Record<string, number>;
  policy_versions: Record<string, number>;
  telemetry_dropped: number;
  global_keys: number;
  warnings: Warning[] | null;
  controller: {
    started_at: string;
    version: string;
    ingest_accepted: number;
    ingest_duplicate: number;
    ingest_rejected: number;
    ingest_bytes: number;
    aggregation_ms: number;
    registry_capacity: number;
  };
}

export interface ServiceView {
  name: string;
  instances: number;
  stale: number;
  rates: Rates;
  hit_ratio: number;
  cache_entries: number;
  cache_memory_bytes: number;
  admitted_keys: number;
  tracking_unhealthy: number;
  tracking_unsupported: number;
  policy_versions: number[] | null;
  drivers: string[] | null;
}

export interface InstanceView {
  service: string;
  instance: string;
  sdk_version: string;
  driver: string;
  topology: string;
  tracking: { state: string; reason?: string; mode?: string };
  first_seen: string;
  last_seen: string;
  age_seconds: number;
  fresh: boolean;
  policy_version: number;
  policy_source: string;
  cache_mode: string;
  rates: Rates;
  hit_ratio: number;
  cache: { entries: number; memory_bytes: number; max_entries: number; max_memory_bytes: number; admitted: number };
  detector: { observed: number; recorded: number; dropped: number; tracked: number; coverage: number };
  runtime: { heap_alloc_bytes: number; goroutines: number; gc_cpu_fraction: number; telemetry_dropped: number };
  fingerprint_scope: string;
  batches: number;
  duplicates: number;
}

export interface ServiceShare {
  service: string;
  read_rate: number;
  instances: number;
}

export interface KeyView {
  fp: string;
  display?: string;
  scope: string;
  read_rate: number;
  write_rate: number;
  fill_rate: number;
  invalidation_rate: number;
  saved_rate: number;
  instances: number;
  states: Record<string, number>;
  services: ServiceShare[] | null;
  max_local_rate: number;
  mean_local_rate: number;
  local_share: number;
  classification: string;
  avg_size_bytes: number;
  cached_hit_ratio: number;
  recommendation: string;
  trend?: number[];
}

export interface CacheView {
  rates: Rates;
  hit_ratio: number;
  memory_bytes: number;
  memory_limit_bytes: number;
  memory_utilization: number;
  entries: number;
  admitted_keys: number;
  churn_per_minute: number;
  state_counts: Record<string, number>;
  top_beneficial: KeyView[] | null;
  top_churn: KeyView[] | null;
}

export interface Point {
  at: string;
  reads: number;
  local_hits: number;
  redis_reads: number;
  writes: number;
  invalidations: number;
  instances: number;
  memory_mb: number;
  hit_ratio: number;
}

export interface Policy {
  version: number;
  service?: string;
  mode: "adaptive" | "explicit" | "off";
  admission?: {
    min_read_rate?: number;
    min_hit_ratio?: number;
    exit_hit_ratio?: number;
    min_observation_windows?: number;
    exit_windows?: number;
    cooldown_seconds?: number;
    max_entry_bytes?: number;
  };
  budget?: { max_entries?: number; max_memory_bytes?: number };
  pin_patterns?: string[];
  deny_patterns?: string[];
  deny_fingerprints?: string[];
  valid_for_seconds?: number;
  comment?: string;
  created_at?: string;
  created_by?: string;
}

export interface Envelope {
  current: Policy;
  previous?: Policy;
  rollout_percent: number;
}

export interface HistoryEntry {
  at: string;
  service: string;
  action: string;
  version: number;
  rollout_percent: number;
  actor?: string;
  policy?: Policy;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

const TOKEN_KEY = "keyshift.token";

export function getToken(): string {
  try {
    return sessionStorage.getItem(TOKEN_KEY) ?? "";
  } catch {
    return "";
  }
}

export function setToken(token: string) {
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // Storage unavailable (private mode): the token lives for this page only.
  }
  memoryToken = token;
}

let memoryToken = "";

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const token = getToken() || memoryToken;
  const headers: Record<string, string> = { Accept: "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let resp: Response;
  try {
    resp = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  } catch (e) {
    throw new ApiError(0, "controller unreachable");
  }
  if (resp.status === 204) return undefined as T;
  const text = await resp.text();
  let data: unknown = undefined;
  try {
    data = text ? JSON.parse(text) : undefined;
  } catch {
    // non-JSON error page
  }
  if (!resp.ok) {
    const msg = (data as { error?: string } | undefined)?.error ?? (text || resp.statusText);
    throw new ApiError(resp.status, msg);
  }
  return data as T;
}

export const api = {
  whoami: () => request<{ role: string; version: string }>("GET", "/api/v1/whoami"),
  overview: () => request<Overview>("GET", "/api/v1/overview"),
  timeseries: () => request<Point[] | null>("GET", "/api/v1/timeseries"),
  services: () => request<ServiceView[] | null>("GET", "/api/v1/services"),
  instances: (service = "", limit = 500) =>
    request<{ total: number; items: InstanceView[] | null }>(
      "GET",
      `/api/v1/instances?limit=${limit}${service ? `&service=${encodeURIComponent(service)}` : ""}`,
    ),
  hotkeys: (params: { service?: string; classification?: string; limit?: number }) => {
    const q = new URLSearchParams({ limit: String(params.limit ?? 100) });
    if (params.service) q.set("service", params.service);
    if (params.classification) q.set("classification", params.classification);
    return request<KeyView[]>("GET", `/api/v1/hotkeys?${q}`);
  },
  cache: () => request<CacheView>("GET", "/api/v1/cache"),
  policies: () => request<Record<string, Envelope> | null>("GET", "/api/v1/policies"),
  history: () => request<HistoryEntry[] | null>("GET", "/api/v1/policies/history"),
  putPolicy: (scope: string, policy: Partial<Policy>, rollout: number) =>
    request<Envelope>("PUT", `/api/v1/policies/${encodeURIComponent(scope)}`, { policy, rollout_percent: rollout }),
  rollout: (scope: string, percent: number) =>
    request<Envelope>("POST", `/api/v1/policies/${encodeURIComponent(scope)}/rollout`, { rollout_percent: percent }),
  rollback: (scope: string) => request<Envelope>("POST", `/api/v1/policies/${encodeURIComponent(scope)}/rollback`),
  disable: (scope: string) => request<Envelope>("POST", `/api/v1/policies/${encodeURIComponent(scope)}/disable`),
  remove: (scope: string) => request<void>("DELETE", `/api/v1/policies/${encodeURIComponent(scope)}`),
};
