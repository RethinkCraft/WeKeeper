export type RangeMode = 'today' | 'last24' | 'last7' | 'last30' | 'custom';
export type RangeSelection = { mode: Exclude<RangeMode, 'custom'> } | { mode: 'custom'; from: string; to: string };
export type ResolvedRange = { since: number; until: number; from: string; to: string };
export const defaultRange: RangeSelection = { mode: 'last7' };
export const rangeLabels: Record<RangeMode, string> = { today: '今天', last24: '近 24 小时', last7: '近 7 天', last30: '近 30 天', custom: '固定日期' };
const memory = new Map<string, RangeSelection>();

export function shanghaiDay(seconds: number): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(seconds * 1000));
}

export function validSelection(input: unknown): input is RangeSelection {
  if (!input || typeof input !== 'object') return false;
  const value = input as Record<string, unknown>;
  if (['today', 'last24', 'last7', 'last30'].includes(String(value.mode))) return true;
  if (value.mode !== 'custom' || typeof value.from !== 'string' || typeof value.to !== 'string') return false;
  const validDay = (day: string) => /^\d{4}-\d{2}-\d{2}$/.test(day) && Number.isFinite(Date.parse(`${day}T00:00:00+08:00`)) && shanghaiDay(Date.parse(`${day}T00:00:00+08:00`) / 1000) === day;
  return validDay(value.from) && validDay(value.to) && value.from <= value.to;
}

export function resolveRange(selection: RangeSelection, now: number): ResolvedRange {
  const until = Math.floor(now);
  const to = shanghaiDay(until);
  if (selection.mode === 'custom') return { from: selection.from, to: selection.to, since: Date.parse(`${selection.from}T00:00:00+08:00`) / 1000, until: Date.parse(`${selection.to}T23:59:59+08:00`) / 1000 };
  if (selection.mode === 'last24') return { since: until - 86400, until, from: shanghaiDay(until - 86400), to };
  const days = selection.mode === 'last7' ? 7 : selection.mode === 'last30' ? 30 : 1;
  const since = Date.parse(`${to}T00:00:00+08:00`) / 1000 - (days - 1) * 86400;
  return { since, until, from: shanghaiDay(since), to };
}

export function rangeQuery(range: ResolvedRange): string {
  return new URLSearchParams({ since: String(range.since), until: String(range.until) }).toString();
}

export function preferenceKey(userId: number, scope: string, base = '/'): string {
  return `wekeeper:range:v1:${base}:${userId}:${scope}`;
}

export function readSelection(key: string, storage?: Pick<Storage, 'getItem'>): RangeSelection | null {
  try {
    const raw = storage?.getItem(key);
    if (raw) { const value: unknown = JSON.parse(raw); if (validSelection(value)) { memory.set(key, value); return value; } }
  } catch { /* Browser storage is optional. */ }
  return memory.get(key) || null;
}

export function saveSelection(key: string, value: RangeSelection, storage?: Pick<Storage, 'setItem'>): void {
  memory.set(key, value);
  try { storage?.setItem(key, JSON.stringify(value)); } catch { /* Keep in-memory preferences when storage is unavailable. */ }
}
