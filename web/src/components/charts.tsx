import { useMemo, useState } from "react";

export interface Series {
  name: string;
  color: string;
  values: number[];
}

function niceMax(v: number): number {
  if (v <= 0) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}

/** LineChart draws aligned series over shared x positions (oldest first). */
export function LineChart({
  series,
  labels,
  height = 180,
  format = (v) => v.toFixed(0),
}: {
  series: Series[];
  labels: string[];
  height?: number;
  format?: (v: number) => string;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const width = 640;
  const pad = { l: 56, r: 12, t: 10, b: 22 };
  const n = Math.max(0, ...series.map((s) => s.values.length));
  const max = useMemo(() => niceMax(Math.max(0, ...series.flatMap((s) => s.values))), [series]);
  if (n < 2) return <div className="placeholder">Collecting data points…</div>;
  const x = (i: number) => pad.l + (i * (width - pad.l - pad.r)) / (n - 1);
  const y = (v: number) => pad.t + (1 - v / max) * (height - pad.t - pad.b);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => f * max);
  return (
    <div className="chart">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={series.map((s) => s.name).join(", ")}
        onMouseLeave={() => setHover(null)}
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          const px = ((e.clientX - r.left) / r.width) * width;
          const i = Math.round(((px - pad.l) / (width - pad.l - pad.r)) * (n - 1));
          setHover(i >= 0 && i < n ? i : null);
        }}
      >
        {ticks.map((t) => (
          <g key={t}>
            <line x1={pad.l} x2={width - pad.r} y1={y(t)} y2={y(t)} className="grid" />
            <text x={pad.l - 6} y={y(t) + 4} textAnchor="end" className="axis">
              {format(t)}
            </text>
          </g>
        ))}
        <text x={pad.l} y={height - 4} className="axis">
          {labels[0]}
        </text>
        <text x={width - pad.r} y={height - 4} textAnchor="end" className="axis">
          {labels[labels.length - 1]}
        </text>
        {series.map((s) => (
          <polyline
            key={s.name}
            fill="none"
            stroke={s.color}
            strokeWidth={2}
            strokeLinejoin="round"
            points={s.values.map((v, i) => `${x(i)},${y(v)}`).join(" ")}
          />
        ))}
        {hover !== null && (
          <g>
            <line x1={x(hover)} x2={x(hover)} y1={pad.t} y2={height - pad.b} className="cursor" />
            {series.map((s) =>
              s.values[hover] !== undefined ? <circle key={s.name} cx={x(hover)} cy={y(s.values[hover])} r={3.5} fill={s.color} /> : null,
            )}
          </g>
        )}
      </svg>
      <div className="legend">
        {series.map((s) => (
          <span key={s.name}>
            <i style={{ background: s.color }} />
            {s.name}
            {hover !== null && s.values[hover] !== undefined && <b>{format(s.values[hover])}</b>}
          </span>
        ))}
        {hover !== null && <span className="muted">{labels[hover]}</span>}
      </div>
    </div>
  );
}

export function Sparkline({ values, width = 90, height = 22 }: { values: number[]; width?: number; height?: number }) {
  if (values.length < 2) return <span className="muted">–</span>;
  const max = Math.max(...values) || 1;
  const pts = values.map((v, i) => `${(i * width) / (values.length - 1)},${height - 2 - (v / max) * (height - 4)}`).join(" ");
  return (
    <svg width={width} height={height} className="spark" aria-hidden>
      <polyline points={pts} fill="none" stroke="var(--accent)" strokeWidth={1.5} />
    </svg>
  );
}

/** Meter shows a ratio as a horizontal bar. */
export function Meter({ value, tone }: { value: number; tone?: "good" | "warn" | "bad" }) {
  const v = Math.max(0, Math.min(1, value || 0));
  return (
    <div className="meter" title={`${(v * 100).toFixed(1)}%`}>
      <div className={`meter-fill ${tone ? `tone-${tone}` : ""}`} style={{ width: `${v * 100}%` }} />
    </div>
  );
}

/** BarList renders labelled horizontal bars. */
export function BarList({ items, format = String }: { items: { label: string; value: number }[]; format?: (v: number) => string }) {
  const max = Math.max(1, ...items.map((i) => i.value));
  return (
    <div className="barlist">
      {items.map((i) => (
        <div key={i.label} className="barlist-row">
          <span className="barlist-label">{i.label}</span>
          <div className="barlist-bar">
            <div style={{ width: `${(i.value / max) * 100}%` }} />
          </div>
          <span className="barlist-value">{format(i.value)}</span>
        </div>
      ))}
    </div>
  );
}
