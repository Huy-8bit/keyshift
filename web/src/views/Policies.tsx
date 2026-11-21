import { useState } from "react";
import { ApiError, api, type Envelope, type Policy } from "../api";
import { Async, Badge, Card, ErrorBox } from "../components/ui";
import { usePoll } from "../hooks";
import { ago } from "../format";

const TEMPLATE: Partial<Policy> = {
  mode: "adaptive",
  admission: { min_read_rate: 2, min_hit_ratio: 0.6, exit_hit_ratio: 0.3, cooldown_seconds: 30 },
  budget: { max_entries: 1000, max_memory_bytes: 16777216 },
  deny_patterns: ["lock:*", "ratelimit:*"],
  valid_for_seconds: 600,
  comment: "",
};

function stripMeta(p: Policy): Partial<Policy> {
  const { version: _v, created_at: _c, created_by: _b, service: _s, ...rest } = p;
  void _v;
  void _c;
  void _b;
  void _s;
  return rest;
}

export function PoliciesView({ isAdmin }: { isAdmin: boolean }) {
  const pol = usePoll(api.policies, 15_000);
  const hist = usePoll(api.history, 15_000);
  const [editing, setEditing] = useState<{ scope: string; text: string; rollout: number } | null>(null);
  const [actionError, setActionError] = useState<ApiError>();
  const [busy, setBusy] = useState(false);

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setActionError(undefined);
    try {
      await fn();
      setEditing(null);
      pol.refresh();
      hist.refresh();
    } catch (e) {
      setActionError(e instanceof ApiError ? e : new ApiError(0, String(e)));
    } finally {
      setBusy(false);
    }
  };

  const save = () => {
    if (!editing) return;
    let parsed: Partial<Policy>;
    try {
      parsed = JSON.parse(editing.text);
    } catch (e) {
      setActionError(new ApiError(400, `invalid JSON: ${(e as Error).message}`));
      return;
    }
    void act(() => api.putPolicy(editing.scope, parsed, editing.rollout));
  };

  return (
    <div className="stack">
      {actionError && <ErrorBox error={actionError} />}
      {!isAdmin && <div className="note">Read-only: use an admin token to change policies.</div>}
      {editing && (
        <Card title={`Edit policy · ${editing.scope}`}>
          <p className="muted small">
            A new version is created. Instances inside the rollout percentage use it; the rest keep the previous version. Remote
            policies can tighten but never exceed each SDK's local limits.
          </p>
          <textarea
            className="code"
            rows={18}
            spellCheck={false}
            value={editing.text}
            onChange={(e) => setEditing({ ...editing, text: e.target.value })}
            aria-label="Policy JSON"
          />
          <div className="row">
            <label>
              Rollout {editing.rollout}%
              <input
                type="range"
                min={0}
                max={100}
                step={5}
                value={editing.rollout}
                onChange={(e) => setEditing({ ...editing, rollout: Number(e.target.value) })}
              />
            </label>
            <button className="primary" disabled={busy} onClick={save}>
              Save new version
            </button>
            <button onClick={() => setEditing(null)}>Cancel</button>
          </div>
        </Card>
      )}
      <Card
        title="Policies"
        actions={
          isAdmin && (
            <>
              <button onClick={() => setEditing({ scope: "_default", text: JSON.stringify(TEMPLATE, null, 2), rollout: 100 })}>New default</button>
              <button
                onClick={() => {
                  const scope = prompt("Service name for the new policy scope")?.trim();
                  if (scope) setEditing({ scope, text: JSON.stringify(TEMPLATE, null, 2), rollout: 10 });
                }}
              >
                New service policy
              </button>
            </>
          )
        }
      >
        <Async
          state={pol}
          isEmpty={(p) => !p || Object.keys(p).length === 0}
          empty="No policies. SDK instances use their local configuration."
        >
          {(p) => (
            <div className="policies">
              {Object.entries(p!)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([scope, env]) => (
                  <PolicyCard
                    key={scope}
                    scope={scope}
                    env={env}
                    isAdmin={isAdmin}
                    busy={busy}
                    onEdit={() => setEditing({ scope, text: JSON.stringify(stripMeta(env.current), null, 2), rollout: 10 })}
                    onRollout={(n) => act(() => api.rollout(scope, n))}
                    onRollback={() => act(() => api.rollback(scope))}
                    onDisable={() => confirm(`Disable local caching for ${scope}?`) && act(() => api.disable(scope))}
                    onDelete={() => confirm(`Delete policy scope ${scope}?`) && act(() => api.remove(scope))}
                  />
                ))}
            </div>
          )}
        </Async>
      </Card>
      <Card title="History">
        <Async state={hist} isEmpty={(h) => !h || h.length === 0} empty="No policy changes yet.">
          {(h) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>When</th>
                    <th>Scope</th>
                    <th>Action</th>
                    <th>Version</th>
                    <th className="r">Rollout</th>
                    <th>Actor</th>
                  </tr>
                </thead>
                <tbody>
                  {h!.map((e, i) => (
                    <tr key={i}>
                      <td title={e.at}>{ago(e.at)}</td>
                      <td>{e.service}</td>
                      <td>{e.action}</td>
                      <td>v{e.version}</td>
                      <td className="r">{e.rollout_percent}%</td>
                      <td>{e.actor}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
    </div>
  );
}

function PolicyCard(props: {
  scope: string;
  env: Envelope;
  isAdmin: boolean;
  busy: boolean;
  onEdit: () => void;
  onRollout: (n: number) => void;
  onRollback: () => void;
  onDisable: () => void;
  onDelete: () => void;
}) {
  const { scope, env, isAdmin, busy } = props;
  const [pct, setPct] = useState(env.rollout_percent);
  const c = env.current;
  return (
    <div className="policy">
      <div className="policy-head">
        <h3>{scope === "_default" ? "Default (all services)" : scope}</h3>
        <Badge value={c.mode} />
        <span className="muted">
          v{c.version} · {env.rollout_percent}% rollout
          {env.previous && ` · baseline v${env.previous.version} (${env.previous.mode})`}
        </span>
      </div>
      <div className="kv small">
        <span>Deny patterns</span>
        <span className="mono">{(c.deny_patterns ?? []).join(", ") || "–"}</span>
        <span>Pin patterns</span>
        <span className="mono">{(c.pin_patterns ?? []).join(", ") || "–"}</span>
        <span>Denied fingerprints</span>
        <span>{(c.deny_fingerprints ?? []).length}</span>
        <span>Budget</span>
        <span>
          {c.budget?.max_entries ?? "local"} entries · {c.budget?.max_memory_bytes ?? "local"} bytes
        </span>
        <span>Created</span>
        <span>
          {ago(c.created_at)} by {c.created_by || "–"}
        </span>
        {c.comment && (
          <>
            <span>Comment</span>
            <span>{c.comment}</span>
          </>
        )}
      </div>
      {isAdmin && (
        <div className="row">
          <button onClick={props.onEdit} disabled={busy}>
            Edit
          </button>
          <label className="small">
            Rollout {pct}%
            <input type="range" min={0} max={100} step={5} value={pct} onChange={(e) => setPct(Number(e.target.value))} />
          </label>
          <button disabled={busy || pct === env.rollout_percent} onClick={() => props.onRollout(pct)}>
            Apply rollout
          </button>
          <button disabled={busy || !env.previous} onClick={props.onRollback}>
            Roll back
          </button>
          <button disabled={busy} onClick={props.onDisable}>
            Disable caching
          </button>
          <button className="danger" disabled={busy} onClick={props.onDelete}>
            Delete
          </button>
        </div>
      )}
    </div>
  );
}
