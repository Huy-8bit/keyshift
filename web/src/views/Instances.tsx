import { useState } from "react";
import { api } from "../api";
import { Async, Badge, Card } from "../components/ui";
import { Meter } from "../components/charts";
import { usePoll } from "../hooks";
import { ago, bytes, num, pct, rate } from "../format";

export function InstancesView() {
  const [service, setService] = useState("");
  const svcs = usePoll(api.services, 30_000);
  const inst = usePoll(() => api.instances(service), 10_000, [service]);
  return (
    <Card
      title="SDK instances"
      actions={
        <select value={service} onChange={(e) => setService(e.target.value)} aria-label="Service">
          <option value="">All services</option>
          {(svcs.data ?? []).map((s) => (
            <option key={s.name}>{s.name}</option>
          ))}
        </select>
      }
    >
      <Async state={inst} isEmpty={(r) => !r.items || r.items.length === 0} empty="No SDK instances have reported telemetry.">
        {(r) => (
          <>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Service</th>
                    <th>Instance</th>
                    <th>SDK</th>
                    <th>Tracking</th>
                    <th>Telemetry</th>
                    <th className="r">Reads</th>
                    <th className="r">Hit ratio</th>
                    <th className="r" title="Share of recorded reads attributed to the top candidates in the last window">
                      Coverage
                    </th>
                    <th className="r">Entries</th>
                    <th>Cache memory</th>
                    <th>Policy</th>
                  </tr>
                </thead>
                <tbody>
                  {r.items!.map((i) => (
                    <tr key={i.service + i.instance}>
                      <td>{i.service}</td>
                      <td className="mono small">{i.instance}</td>
                      <td className="small">
                        {i.sdk_version} · {i.driver}/{i.topology}
                      </td>
                      <td>
                        <Badge value={i.tracking.state} title={i.tracking.reason || i.tracking.mode} />
                      </td>
                      <td>
                        <span className={i.fresh ? "" : "warn-text"}>{ago(i.last_seen)}</span>
                      </td>
                      <td className="r">{rate(i.rates.reads)}</td>
                      <td className="r">{pct(i.hit_ratio)}</td>
                      <td className="r">{pct(i.detector.coverage, 0)}</td>
                      <td className="r">{num(i.cache.entries)}</td>
                      <td style={{ minWidth: 120 }}>
                        <Meter value={i.cache.max_memory_bytes > 0 ? i.cache.memory_bytes / i.cache.max_memory_bytes : 0} />
                        <span className="small muted">
                          {bytes(i.cache.memory_bytes)} / {bytes(i.cache.max_memory_bytes)}
                        </span>
                      </td>
                      <td className="small">
                        v{i.policy_version} ({i.policy_source}, <Badge value={i.cache_mode} />)
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="muted small">
              {r.items!.length} of {r.total} instances shown.
            </p>
          </>
        )}
      </Async>
    </Card>
  );
}
