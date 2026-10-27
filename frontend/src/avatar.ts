export const AVATAR_RETRY_INTERVAL_MS = 30000;

export function displayMemberName(name: string | null | undefined, fallback = '未命名成员'): string {
  return name?.trim() || fallback;
}

export function avatarSource(value: string, origin: string, apiPrefix: string): string {
  if (!value) return '';
  try {
    const candidate = new URL(value, origin);
    return candidate.origin === origin && candidate.pathname.startsWith(apiPrefix) ? candidate.href : '';
  } catch { return ''; }
}

export function canRetryAvatar(failedAt: number | null, now: number, manual = false): boolean {
  return failedAt !== null && (manual || now - failedAt >= AVATAR_RETRY_INTERVAL_MS);
}
