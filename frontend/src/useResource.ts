import { useEffect, useState } from 'react';
import { ApiError, api } from './api';

export function useResource<T>(path: string | null, revision = 0) {
  const [state, setState] = useState<{ data?: T; error: string; errorCode?: string; loading: boolean; loadedPath?: string; loadedAt?: number }>({ error: '', loading: true });
  useEffect(() => {
    if (!path) return;
    const controller = new AbortController();
    setState(current => ({ ...current, loading: true, error: '', errorCode: '' }));
    api<T>(path, { signal: controller.signal }).then(data => {
      if (!controller.signal.aborted) setState({ data, error: '', loading: false, loadedPath: path, loadedAt: Date.now() });
    }).catch(error => {
      if (!controller.signal.aborted) setState(current => ({ ...current, error: error.message, errorCode: error instanceof ApiError ? error.code : '', loading: false }));
    });
    return () => controller.abort();
  }, [path, revision]);
  return state;
}
