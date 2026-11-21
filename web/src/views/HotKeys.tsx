import { useState } from "react";
import { api, type KeyView } from "../api";
import { Async, Badge, Card } from "../components/ui";
import { Sparkline } from "../components/charts";
import { usePoll } from "../hooks";
import { bytes, keyLabel, pct, rate } from "../format";

export function HotKeysView() {
  const [service, setService] = useState("");
  const [cls, setCls] = useState("");
  const [selected, setSelected] = useState<KeyView>();
  const svcs = usePoll(api.services, 30_000);
  const keys = usePoll(() => api.hotkeys({ service, classification: cls, limit: 200 }), 10_000, [service, cls]);

  return (
    <div className="stack">
      <Card
        title="Global hot-key ranking"
        actions={
          <>
            <select value={service} onChange={(e) => setService(e.target.value)} aria-label="Service">
              <option value="">All services</option>
              {(svcs.data ?? []).map((s) => (
                <option key={s.name}>{s.name}</option>
              ))}
            </select>
            <select value={cls} onChange={(e) => setCls(e.target.value)} aria-label="Classification">
              <option value="">All classes</option>
              <option value="global">Global</option>
              <option value="local">Local</option>
              <option value="single-instance">Single instance</option>
            </select>
          </>
        }
      >
        <p className="muted small">
          Read rates are SDK-observed logical reads summed across instances. <b>Max local</b> is the highest single-instance rate:
          a key that is hot globally but cold on every instance gains little from per-pod caching. Keys appear as fingerprints
          unless they match an SDK display pattern.
        </p>
        <Async state={keys} isEmpty={(k) => k.length === 0} empty="No hot keys reported yet. SDK instances upload telemetry every 30 seconds.">
          {(ks) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Key</th>
                    <th className="r">Reads</th>
                    <th className="r">Writes</th>
                    <th className="r">Saved</th>
                    <th className="r">Instances</th>
                    <th className="r">Max local</th>
                    <th>Class</th>
                    <th>Cached on</th>
                    <th>Trend</th>
                  </tr>
                </thead>
                <tbody>
                  {ks.map((k, i) => (
                    <tr key={k.fp + k.scope} className={selected?.fp === k.fp ? "selected" : ""} onClick={() => setSelected(k)}>
                      <td className="muted">{i + 1}</td>
                      <td className="mono">{keyLabel(k)}</td>
                      <td className="r">{rate(k.read_rate)}</td>
                      <td className="r">{rate(k.write_rate)}</td>
                      <td className="r">{rate(k.saved_rate)}</td>
                      <td className="r">{k.instances}</td>
                      <td className="r">{rate(k.max_local_rate)}</td>
                      <td>
                        <Badge value={k.classification} />
                      </td>
                      <td>
                        {k.states.admitted ?? 0}/{k.instances}
                      </td>
                      <td>
                        <Sparkline values={k.trend ?? []} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
      {selected && <KeyDetail k={selected} onClose={() => setSelected(undefined)} />}
    </div>
  );
}

function KeyDetail({ k, onClose }: { k: KeyView; onClose: () => void }) {
  return (
    <Card title={`Key ${keyLabel(k)}`} actions={<button onClick={onClose}>Close</button>}>
      <div className="grid2">
        <div className="kv">
          <span>Fingerprint</span>
          <span className="mono small">{k.fp}</span>
          <span>Fingerprint scope</span>
          <span>{k.scope}</span>
          <span>Global reads</span>
          <span>{rate(k.read_rate)}</span>
          <span>Mean / max per instance</span>
          <span>
            {rate(k.mean_local_rate)} / {rate(k.max_local_rate)} ({pct(k.local_share)} on the busiest instance)
          </span>
          <span>Writes · invalidations</span>
          <span>
            {rate(k.write_rate)} · {rate(k.invalidation_rate)}
          </span>
          <span>Cache fills</span>
          <span>{rate(k.fill_rate)}</span>
          <span>Hit ratio where cached</span>
          <span>{k.states.admitted ? pct(k.cached_hit_ratio) : "not cached"}</span>
          <span>Average value size</span>
          <span>{bytes(k.avg_size_bytes)}</span>
          <span>Recommendation</span>
          <span>{k.recommendation}</span>
        </div>
        <div>
          <h3>Admission states across instances</h3>
          <div className="chips">
            {Object.entries(k.states).map(([s, n]) => (
              <span key={s}>
                <Badge value={s} /> {n}
              </span>
            ))}
          </div>
          <h3>Service distribution</h3>
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th className="r">Reads</th>
                <th className="r">Instances</th>
              </tr>
            </thead>
            <tbody>
              {(k.services ?? []).map((s) => (
                <tr key={s.service}>
                  <td>{s.service}</td>
                  <td className="r">{rate(s.read_rate)}</td>
                  <td className="r">{s.instances}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="muted small">
            To exclude this key from caching, add its fingerprint to <code>deny_fingerprints</code> in a policy.
          </p>
        </div>
      </div>
    </Card>
  );
}
