let csrfToken = '';
export function setCsrfToken(value: string) { csrfToken = value; }

const basePath = import.meta.env.BASE_URL.replace(/\/+$/, '');

export function appPath(path: string): string {
  if (!basePath || !path.startsWith('/') || path === basePath || path.startsWith(`${basePath}/`)) return path;
  return `${basePath}${path}`;
}

export class ApiError extends Error {
  constructor(message: string, public code: string, public status: number) { super(message); }
}

const errors: Record<string, string> = {
  user_turn_in_progress: '你已有一轮任务正在处理，请先查看该会话或停止本轮。',
  conversation_not_found: '没有找到你的会话，可能已被删除或无权访问。',
  turn_not_found: '没有找到这轮任务。',
  invalid_credentials: '账号或密码不正确。', authentication_required: '登录已过期，请重新登录。',
  not_authenticated: '请先登录。', unauthorized: '请先登录。', forbidden: '当前账号没有操作权限。',
  admin_required: '此操作需要管理员权限。', invalid_csrf_token: '会话校验失败，请刷新页面后重试。',
  csrf_invalid: '会话校验失败，请刷新页面后重试。', rate_limited: '操作过于频繁，请稍后再试。',
  username_exists: '该账号名称已被使用。', password_too_short: '密码至少需要 12 个字符。',
  csrf_token_required: '会话校验失败，请刷新页面后重试。', too_many_login_attempts: '登录尝试过于频繁，请稍后重试。',
  invalid_username: '账号名称只能包含字母、数字和 . _ @ -。', invalid_request: '请检查填写的内容后重试。',
  last_admin_cannot_be_disabled: '至少需要保留一个可用的管理员账号。',
  not_found: '没有找到这条记录。', last_admin: '至少需要保留一个可用的管理员账号。',
};

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  let response: Response;
  try {
    const headers = new Headers(init.headers);
    if (init.body) headers.set('Content-Type', 'application/json');
    if (init.method && init.method !== 'GET') headers.set('X-CSRF-Token', csrfToken);
    response = await fetch(appPath(path), { ...init, headers, credentials: 'same-origin' });
  } catch {
    throw new Error('暂时无法连接服务器，请检查网络后重试。');
  }
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401 && !path.endsWith('/login')) window.dispatchEvent(new Event('wekeeper:expired'));
    const detail = typeof data.detail === 'string' ? data.detail : '';
    throw new ApiError(errors[detail] || (response.status === 429 ? '操作过于频繁，请稍后重试。' : `请求未完成（${response.status}），请稍后重试。`), detail, response.status);
  }
  return data as T;
}

export const post = <T>(path: string, body: unknown = {}) => api<T>(path, { method: 'POST', body: JSON.stringify(body) });
export const patch = <T>(path: string, body: unknown) => api<T>(path, { method: 'PATCH', body: JSON.stringify(body) });
export const remove = (path: string) => api<unknown>(path, { method: 'DELETE' });
