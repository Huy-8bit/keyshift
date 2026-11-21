import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "./api";

export interface PollState<T> {
  data: T | undefined;
  error: ApiError | undefined;
  loading: boolean;
  /** Last successful refresh. */
  updatedAt: Date | undefined;
  refresh: () => void;
}

/**
 * usePoll fetches data immediately and then every intervalMs while the page
 * is visible. Stale data stays on screen when a refresh fails, and the error
 * is reported separately so views can show a disconnected banner.
 */
export function usePoll<T>(fetcher: () => Promise<T>, intervalMs = 10_000, deps: unknown[] = []): PollState<T> {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<ApiError>();
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState<Date>();
  const fetchRef = useRef(fetcher);
  fetchRef.current = fetcher;
  const seq = useRef(0);

  const refresh = useCallback(() => {
    const id = ++seq.current;
    fetchRef
      .current()
      .then((d) => {
        if (id !== seq.current) return;
        setData(d);
        setError(undefined);
        setUpdatedAt(new Date());
      })
      .catch((e: unknown) => {
        if (id !== seq.current) return;
        setError(e instanceof ApiError ? e : new ApiError(0, String(e)));
      })
      .finally(() => {
        if (id === seq.current) setLoading(false);
      });
  }, []);

  useEffect(() => {
    setLoading(true);
    refresh();
    const t = setInterval(() => {
      if (document.visibilityState === "visible") refresh();
    }, intervalMs);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, refresh, ...deps]);

  return { data, error, loading, updatedAt, refresh };
}
