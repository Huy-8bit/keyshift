import { useEffect, useState } from "react";
import { ApiError, api, getToken, setToken } from "./api";
import { usePoll } from "./hooks";
import { OverviewView } from "./views/Overview";
import { HotKeysView } from "./views/HotKeys";
import { CacheView } from "./views/Cache";
import { PoliciesView } from "./views/Policies";
import { InstancesView } from "./views/Instances";
import { ErrorBox } from "./components/ui";
import { ago } from "./format";

const TABS = [
  { id: "overview", label: "Overview" },
  { id: "hotkeys", label: "Hot keys" },
  { id: "cache", label: "Cache efficiency" },
  { id: "policies", label: "Policies" },
  { id: "instances", label: "Instances" },
] as const;
type Tab = (typeof TABS)[number]["id"];

function currentTab(): Tab {
  const h = window.location.hash.replace("#/", "");
  return (TABS.find((t) => t.id === h)?.id ?? "overview") as Tab;
}

function Login({ error, onToken }: { error?: ApiError; onToken: (t: string) => void }) {
  const [value, setValue] = useState("");
  return (
    <div className="login">
      <h1>KeyShift</h1>
      <p className="muted">Enter a controller viewer or admin token. It is kept in this browser tab's session storage only.</p>
      {error && error.status !== 401 && <ErrorBox error={error} />}
      {error?.status === 401 && getToken() && <div className="error">The token was rejected.</div>}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          onToken(value.trim());
        }}
      >
        <input type="password" autoFocus placeholder="API token" value={value} onChange={(e) => setValue(e.target.value)} aria-label="API token" />
        <button className="primary" type="submit" disabled={!value.trim()}>
          Sign in
        </button>
      </form>
    </div>
  );
}

export function App() {
  const [tab, setTab] = useState<Tab>(currentTab());
  const [tokenVersion, setTokenVersion] = useState(0);
  const who = usePoll(api.whoami, 30_000, [tokenVersion]);
  const health = usePoll(api.overview, 10_000, [tokenVersion]);

  useEffect(() => {
    const onHash = () => setTab(currentTab());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const unauthorized = who.error?.status === 401 || who.error?.status === 403;
  if (!who.data && (unauthorized || who.error)) {
    return (
      <Login
        error={who.error}
        onToken={(t) => {
          setToken(t);
          setTokenVersion((v) => v + 1);
        }}
      />
    );
  }
  if (!who.data) return <div className="placeholder">Connecting to the KeyShift controller…</div>;

  const disconnected = health.error && health.error.status === 0;
  const isAdmin = who.data.role === "admin";
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden>
            ⇄
          </span>
          KeyShift
        </div>
        <nav>
          {TABS.map((t) => (
            <a key={t.id} href={`#/${t.id}`} className={tab === t.id ? "active" : ""}>
              {t.label}
            </a>
          ))}
        </nav>
        <div className="session">
          <span className={`dot ${disconnected ? "bad" : "good"}`} />
          <span className="small muted">
            {disconnected ? "controller unreachable" : `updated ${ago(health.updatedAt?.toISOString())}`}
          </span>
          <span className="small muted">
            {who.data.role} · v{who.data.version}
          </span>
          <button
            className="link"
            onClick={() => {
              setToken("");
              setTokenVersion((v) => v + 1);
            }}
          >
            Sign out
          </button>
        </div>
      </header>
      {disconnected && (
        <div className="banner">
          The controller is unreachable; data shown may be stale. SDK instances keep serving Redis traffic with their last valid
          policy.
        </div>
      )}
      <main>
        {tab === "overview" && <OverviewView />}
        {tab === "hotkeys" && <HotKeysView />}
        {tab === "cache" && <CacheView />}
        {tab === "policies" && <PoliciesView isAdmin={isAdmin} />}
        {tab === "instances" && <InstancesView />}
      </main>
    </div>
  );
}
