export function rate(v: number | undefined): string {
  if (v === undefined || !isFinite(v)) return "–";
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}G/s`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M/s`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k/s`;
  if (v >= 10) return `${v.toFixed(0)}/s`;
  return `${v.toFixed(1)}/s`;
}

export function num(v: number | undefined): string {
  if (v === undefined || !isFinite(v)) return "–";
  return v.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

export function pct(v: number | undefined, digits = 1): string {
  if (v === undefined || !isFinite(v)) return "–";
  return `${(v * 100).toFixed(digits)}%`;
}

export function bytes(v: number | undefined): string {
  if (v === undefined || v < 0 || !isFinite(v)) return "n/a";
  if (v >= 1 << 30) return `${(v / (1 << 30)).toFixed(1)} GiB`;
  if (v >= 1 << 20) return `${(v / (1 << 20)).toFixed(1)} MiB`;
  if (v >= 1 << 10) return `${(v / (1 << 10)).toFixed(1)} KiB`;
  return `${v} B`;
}

export function ago(iso: string | undefined): string {
  if (!iso) return "–";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 60) return `${Math.max(0, Math.round(s))}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
}

export function keyLabel(k: { display?: string; fp: string }): string {
  return k.display || `fp:${k.fp.slice(0, 12)}…`;
}
