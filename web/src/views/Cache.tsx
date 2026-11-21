import { api, type KeyView } from "../api";
import { Async, Card, Stat } from "../components/ui";
import { BarList, LineChart, Meter } from "../components/charts";
import { usePoll } from "../hooks";
import { bytes, keyLabel, num, pct, rate } from "../format";

function KeyTable({ keys, metric }: { keys: KeyView[]; metric: "saved" | "churn" }) {
  if (keys.length === 0) return <div className="placeholder">None.</div>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Key</th>
            <th className="r">Reads</th>
            <th className="r">{metric === "saved" ? "Saved" : "Invalidations"}</th>
            <th className="r">Cached on</th>
            <th>Recommendation</th>
          </tr>
        </thead>
        <tbody>
          {keys.map((k) => (
            <tr key={k.fp}>
              <td className="mono">{keyLabel(k)}</td>
              <td className="r">{rate(k.read_rate)}</td>
              <td className="r">{rate(metric === "saved" ? k.saved_rate : k.invalidation_rate)}</td>
              <td className="r">
                {k.states.admitted ?? 0}/{k.instances}
              </td>
              <td className="small">{k.recommendation}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function CacheView() {
  const cache = usePoll(api.cache);
  const ts = usePoll(api.timeseries);
  return (
    <div className="stack">
      <Async state={cache}>
        {(c) => (
          <>
            <div className="stats">
              <Stat label="Hit ratio" value={pct(c.hit_ratio)} hint="local hits / SDK reads" />
              <Stat label="Estimated Redis reads avoided" value={rate(c.rates.local_hits)} />
              <Stat label="Admitted keys" value={num(c.admitted_keys)} hint={`${num(c.entries)} resident entries`} />
              <Stat label="Invalidations" value={rate(c.rates.invalidations)} />
              <Stat label="Evictions + expiries" value={rate(c.rates.evictions)} hint="capacity evictions and max_staleness refreshes" />
              <Stat label="Admission churn" value={`${c.churn_per_minute.toFixed(1)}/min`} hint="admissions + demotions" />
            </div>
            <div className="grid2">
              <Card title="Memory utilization">
                <Meter value={c.memory_utilization} tone={c.memory_utilization > 0.9 ? "warn" : "good"} />
                <p className="muted small">
                  {bytes(c.memory_bytes)} of {bytes(c.memory_limit_bytes)} summed local budgets. Estimated entry memory, not total
                  process memory.
                </p>
              </Card>
              <Card title="Admission decisions (reported keys)">
                <BarList items={Object.entries(c.state_counts ?? {}).map(([label, value]) => ({ label, value }))} format={num} />
              </Card>
            </div>
            <Card title="Most beneficial keys">
              <KeyTable keys={c.top_beneficial ?? []} metric="saved" />
            </Card>
            <Card title="Invalidation churn">
              <KeyTable keys={c.top_churn ?? []} metric="churn" />
            </Card>
          </>
        )}
      </Async>
      <Card title="Hit ratio over time">
        <Async state={ts} isEmpty={(p) => !p || p.length < 2} empty="Collecting data points…">
          {(p) => (
            <LineChart
              labels={p!.map((x) => new Date(x.at).toLocaleTimeString())}
              format={(v) => `${(v * 100).toFixed(0)}%`}
              series={[{ name: "local hit ratio", color: "var(--c3)", values: p!.map((x) => x.hit_ratio) }]}
            />
          )}
        </Async>
      </Card>
    </div>
  );
}
