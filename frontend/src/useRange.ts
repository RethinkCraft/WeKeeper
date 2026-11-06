import { useCallback, useEffect, useMemo, useState } from 'react';
import { defaultRange, preferenceKey, readSelection, resolveRange, saveSelection } from './range';
import type { RangeSelection } from './range';

function storage(): Storage | undefined { try { return window.localStorage; } catch { return undefined; } }

export function useRange(userId: number, scope: string) {
  const key = preferenceKey(userId, scope, import.meta.env.BASE_URL);
  const inheritedKey = preferenceKey(userId, 'overview', import.meta.env.BASE_URL);
  const [selection, update] = useState<RangeSelection>(() => readSelection(key, storage()) || (scope !== 'overview' ? readSelection(inheritedKey, storage()) : null) || defaultRange);
  const [now, setNow] = useState(() => Date.now() / 1000);
  const refreshNow = useCallback(() => setNow(Date.now() / 1000), []);
  useEffect(() => { saveSelection(key, selection, storage()); }, [key, selection]);
  useEffect(() => {
    const visible = () => { if (document.visibilityState === 'visible') refreshNow(); };
    const timer = window.setInterval(visible, 30000);
    window.addEventListener('focus', refreshNow);
    window.addEventListener('wekeeper:refresh-time', refreshNow);
    document.addEventListener('visibilitychange', visible);
    return () => { window.clearInterval(timer); window.removeEventListener('focus', refreshNow); window.removeEventListener('wekeeper:refresh-time', refreshNow); document.removeEventListener('visibilitychange', visible); };
  }, [refreshNow]);
  const setSelection = useCallback((value: RangeSelection) => { update(value); refreshNow(); }, [refreshNow]);
  const resolved = useMemo(() => resolveRange(selection, now), [selection, now]);
  return { selection, resolved, setSelection, refreshNow };
}
