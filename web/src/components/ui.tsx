import type { ReactNode } from "react";
import { ApiError } from "../api";

export function Card({ title, actions, children, className }: { title?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card ${className ?? ""}`}>
      {(title || actions) && (
        <header className="card-head">
          {title && <h2>{title}</h2>}
          {actions && <div className="card-actions">{actions}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, hint, tone }: { label: string; value: ReactNode; hint?: ReactNode; tone?: "good" | "warn" | "bad" }) {
  return (
    <div className={`stat ${tone ? `tone-${tone}` : ""}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {hint && <div className="stat-hint">{hint}</div>}
    </div>
  );
}

const stateTone: Record<string, string> = {
  admitted: "good",
  healthy: "good",
  candidate: "info",
  cold: "muted",
  cooldown: "warn",
  bypassed: "muted",
  disabled: "muted",
  unhealthy: "bad",
  unsupported: "muted",
  unknown: "muted",
  critical: "bad",
  warning: "warn",
  info: "info",
  adaptive: "good",
  explicit: "info",
  off: "muted",
};

export function Badge({ value, title }: { value: string; title?: string }) {
  return (
    <span className={`badge badge-${stateTone[value] ?? "muted"}`} title={title}>
      {value}
    </span>
  );
}

/** Async renders loading, error, and empty states around content. */
export function Async<T>({
  state,
  empty,
  isEmpty,
  children,
}: {
  state: { data: T | undefined; error: ApiError | undefined; loading: boolean };
  empty?: ReactNode;
  isEmpty?: (d: T) => boolean;
  children: (d: T) => ReactNode;
}) {
  if (state.data === undefined) {
    if (state.error) return <ErrorBox error={state.error} />;
    return <div className="placeholder">Loading…</div>;
  }
  if (isEmpty?.(state.data)) return <div className="placeholder">{empty ?? "Nothing to show yet."}</div>;
  return <>{children(state.data)}</>;
}

export function ErrorBox({ error }: { error: ApiError }) {
  let hint = "";
  if (error.status === 0) hint = "The controller is unreachable. SDK instances keep working with their last valid policy.";
  else if (error.status === 401) hint = "Enter a valid viewer or admin token.";
  else if (error.status === 403) hint = "This token does not grant access to this view.";
  return (
    <div className="error" role="alert">
      <strong>{error.status ? `Error ${error.status}` : "Disconnected"}:</strong> {error.message}
      {hint && <div className="error-hint">{hint}</div>}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="placeholder">{children}</div>;
}
