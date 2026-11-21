import { api } from "../api";
import { Async, Badge, Card, Stat } from "../components/ui";
import { BarList, LineChart } from "../components/charts";
import { usePoll } from "../hooks";
import { ago, bytes, num, pct, rate } from "../format";

export function OverviewView() {
  const ov = usePoll(api.overview);
  const ts = usePoll(api.timeseries);
  const svcs = usePoll(api.services);
  return (
    <div className="stack">
      <Async state={ov}>
        {(o) => (
          <>
            <div className="stats">
              <Stat label="Active SDK instances" value={num(o.active_instances)} hint={`${o.stale_instances} stale · ${o.services} services`} />
              <Stat label="SDK reads" value={rate(o.rates.reads)} hint="logical reads observed by SDKs" />
              <Stat
                label="Local hit ratio"
                value={pct(o.local_hit_ratio)}
                tone={o.local_hit_ratio > 0.3 ? "good" : undefined}
                hint="share of SDK reads served from L1"
              />
              <Stat label="Redis reads avoided" value={rate(o.rates.local_hits)} hint={`${rate(o.rates.redis_reads)} still reach Redis`} />
              <Stat label="Invalidations" value={rate(o.rates.invalidations)} hint="server invalidations for cached keys" />
              <Stat
                label="Local cache memory"
                value={bytes(o.cache_memory_bytes)}
                hint={`${num(o.cache_entries)} entries · budget ${bytes(o.cache_memory_limit_bytes)}`}
              />
            </div>
            <Card title="Warnings">
              {o.warnings && o.warnings.length > 0 ? (
                <ul className="warnings">
                  {o.warnings.map((w) => (
                    <li key={w.code}>
                      <Badge value={w.severity} /> {w.message}
                    </li>
                  ))}
                </ul>
              ) : (
                <div className="placeholder ok">No warnings.</div>
              )}
            </Card>
            <div className="grid2">
              <Card title="Tracking health">
                <BarList items={Object.entries(o.tracking).map(([label, value]) => ({ label, value }))} format={num} />
                {Object.keys(o.tracking).length === 0 && <div className="placeholder">No active instances.</div>}
              </Card>
              <Card title="Policy versions in use">
                <BarList items={Object.entries(o.policy_versions).map(([label, value]) => ({ label, value }))} format={num} />
                {Object.keys(o.policy_versions).length === 0 && <div className="placeholder">No active instances.</div>}
              </Card>
            </div>
            <Card title="Controller">
              <div className="kv">
                <span>Version</span>
                <span>{o.controller.version}</span>
                <span>Started</span>
                <span>{ago(o.controller.started_at)}</span>
                <span>Telemetry batches</span>
                <span>
                  {num(o.controller.ingest_accepted)} accepted · {num(o.controller.ingest_duplicate)} duplicate ·{" "}
                  {num(o.controller.ingest_rejected)} rejected
                </span>
                <span>Telemetry received</span>
                <span>{bytes(o.controller.ingest_bytes)}</span>
                <span>Aggregation</span>
                <span>
                  {o.controller.aggregation_ms.toFixed(1)} ms · {num(o.global_keys)} ranked keys · updated {ago(o.generated_at)}
                </span>
              </div>
            </Card>
          </>
        )}
      </Async>
      <Card title="Global workload">
        <Async state={ts} isEmpty={(p) => !p || p.length < 2} empty="Collecting data points…">
          {(p) => (
            <LineChart
              labels={p!.map((x) => new Date(x.at).toLocaleTimeString())}
              format={(v) => rate(v).replace("/s", "")}
              series={[
                { name: "SDK reads/s", color: "var(--c1)", values: p!.map((x) => x.reads) },
                { name: "Redis reads/s", color: "var(--c2)", values: p!.map((x) => x.redis_reads) },
                { name: "Local hits/s", color: "var(--c3)", values: p!.map((x) => x.local_hits) },
              ]}
            />
          )}
        </Async>
      </Card>
      <Card title="Services">
        <Async state={svcs} isEmpty={(s) => !s || s.length === 0} empty="No services have reported telemetry yet.">
          {(s) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Service</th>
                    <th className="r">Instances</th>
                    <th className="r">Reads</th>
                    <th className="r">Hit ratio</th>
                    <th className="r">Redis reads</th>
                    <th className="r">Admitted keys</th>
                    <th className="r">Cache memory</th>
                    <th>Tracking</th>
                    <th>Policies</th>
                  </tr>
                </thead>
                <tbody>
                  {s!.map((x) => (
                    <tr key={x.name}>
                      <td>{x.name}</td>
                      <td className="r">
                        {x.instances}
                        {x.stale > 0 && <span className="muted"> (+{x.stale} stale)</span>}
                      </td>
                      <td className="r">{rate(x.rates.reads)}</td>
                      <td className="r">{pct(x.hit_ratio)}</td>
                      <td className="r">{rate(x.rates.redis_reads)}</td>
                      <td className="r">{num(x.admitted_keys)}</td>
                      <td className="r">{bytes(x.cache_memory_bytes)}</td>
                      <td>
                        {x.tracking_unhealthy > 0 ? <Badge value="unhealthy" /> : x.tracking_unsupported > 0 ? <Badge value="unsupported" /> : <Badge value="healthy" />}
                      </td>
                      <td>{(x.policy_versions ?? []).map((v) => `v${v}`).join(", ")}</td>
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
