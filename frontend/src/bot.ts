export type BotState = 'unknown' | 'available' | 'unavailable';
export type BotPersona = { id: string; name: string; version: number; markdown: string };
export type BotProvider = { id: string; name: string; kind: 'siwc' | 'deepseek' | 'compatible'; model: string; reasoning_effort?: string; base_url: string | null; credential_ref: string | null };
export type BotItem = { id: string; name: string; aliases: string[] };
export type BotBusiness = { id: string; name: string; reporter_ids: string[]; items: BotItem[] };
export type BotGroup = { group_ref: string; business_id: string; persona_id: string; provider_id: string; enabled: boolean };
export type BotConfig = { enabled: boolean; history_turns: number; nearby_messages: number; businesses: BotBusiness[]; personas: BotPersona[]; providers: BotProvider[]; groups: BotGroup[] };
export type BotStatus = { business_id: string; item_id: string; day: string; status: BotState; revision: number; updated_at: number; actor: string | null; message_ref: string | null; group_ref: string | null };
export type BotAudit = BotStatus & { id: string; created_at: number; previous_status: BotState | null };
export type BotCommand = { id: string; kind: string; revision: number | null; status: 'pending' | 'applied' | 'failed'; error: string | null; created_at: number; completed_at: number | null; payload: Record<string, unknown> };
export type BotActive = {
  group_ref?: string | null; provider?: string | null; profile_id?: string | null; model?: string | null;
  reasoning_effort?: string | null; persona_id?: string | null; persona_version?: number | string | null; persona_name?: string | null;
};
export type BotLatency = { count?: number | null; p50_seconds?: number | null; p95_seconds?: number | null };
export type BotLastRequest = {
  request_id?: string | number | null; group_ref?: string | null; status?: string | null;
  started_at?: number | null; finished_at?: number | null; error?: string | null;
  trigger_to_model_seconds?: number | null; generation_seconds?: number | null;
  send_confirmation_seconds?: number | null; end_to_end_seconds?: number | null;
} & Record<string, unknown>;
export type BotStaleEvent = {
  kind?: string; request_id?: string | null; age_seconds?: number | null; at?: string | null; detail?: string | null;
};
export type BotRuntime = {
  state?: string; heartbeat_at?: number | null; enabled?: boolean; queue_depth?: number; error?: string | null;
  authenticated?: boolean | null; connection?: string | null; login_required?: boolean;
  last_receive_at?: number | null; last_send_at?: number | null;
  active?: BotActive | null; last_request?: BotLastRequest | null;
  counts?: { requests?: number | null; replied?: number | null; pending?: number | null; failed?: number | null; unknown?: number | null } | null;
  latency_metrics?: Record<string, BotLatency> | null;
  stale_events?: BotStaleEvent[] | null;
};
export type BotOverview = {
  defaults: { persona: BotPersona; provider: BotProvider }; source_id: string | null; sources: string[]; config: { revision: number; applied_revision: number; data: BotConfig };
  groups: { group_ref: string; name: string }[]; statuses: BotStatus[]; audit: BotAudit[]; commands: BotCommand[];
  received_at: number | null; runtime: BotRuntime;
  capabilities: { quote_reply?: boolean; true_mentions?: boolean; sender_verified?: boolean; native_ready?: boolean };
  providers: { id: string; configured: boolean; state?: string | null; error?: string | null }[];
};
export const MAX_PERSONA_MARKDOWN = 16000;
export function importPersonaMarkdown(filename: string, source: string): string {
  if (!/\.md$/i.test(filename)) throw new Error('请选择 .md 文件。');
  const text = source.replace(/^\uFEFF/, '').replace(/\r\n?/g, '\n');
  if (!text.trim()) throw new Error('Markdown 内容不能为空。');
  if (text.length > MAX_PERSONA_MARKDOWN) throw new Error(`Markdown 最多 ${MAX_PERSONA_MARKDOWN.toLocaleString('zh-CN')} 个字符。`);
  return text;
}
export function personaDraftError(persona: BotPersona): string {
  if (!persona.name.trim()) return '请填写角色名称。';
  if (!persona.markdown.trim()) return `角色“${persona.name}”的 Markdown 内容不能为空。`;
  if (persona.markdown.length > MAX_PERSONA_MARKDOWN) return `角色“${persona.name}”的 Markdown 最多 ${MAX_PERSONA_MARKDOWN.toLocaleString('zh-CN')} 个字符。`;
  return '';
}
export function formatBotDuration(value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return '—';
  return value < 1 ? `${Math.round(value * 1000)} ms` : `${value.toFixed(value < 10 ? 2 : 1)} s`;
}
export function botRequestStateName(value?: string | null): string {
  return ({ queued: '排队中', processing: '处理中', running: '处理中', generated: '待发送', completed: '已完成',
    accepted: '网关已接受', submitting: '提交中', rejected: '网关拒绝', sent: '已确认回复', fallback_sent: '失败回执已送达', failed: '失败', interrupted: '中断',
    unknown: '发送待确认', cancelled: '已取消' }[value || ''] || value || '尚无记录');
}
export function botPhaseName(phase: string): string {
  let stage = phase;
  let prefix = '';
  if (stage.startsWith('failed_')) { prefix = '失败请求 · '; stage = stage.slice(7); }
  else if (stage.startsWith('fallback_')) { prefix = '失败回执 · '; stage = stage.slice(9); }
  if (stage.startsWith('chat_')) { prefix += '闲聊 · '; stage = stage.slice(5); }
  else if (stage.startsWith('business_')) { prefix += '业务 · '; stage = stage.slice(9); }
  const name = ({
  trigger_to_model: '收取与排队', generation: '生成与工具处理', send_confirmation: '发送至确认', end_to_end: '消息至回复确认',
  visible_reply: '首条可见回复', model_lock: '模型锁等待', response_headers: '响应头返回', network_connect: '连接建立',
  model_call: '模型请求总时', mcp: '工具调用', ui_total: '微信界面总时', ui_lookup: '定位群与原消息',
  ui_preview: '发送预览校验', ui_select_quote: '选择原生引用', ui_write_body: '写入回复正文', ui_click: '点击发送',
  collection: '消息采集', mirror: '试用镜像', wake: '唤醒等待', model_queue: '模型排队', inference_wait: '推理锁等待',
  inference_lock: '推理锁等待', first_token: '首个输出', time_to_first_token: '首个输出', model: '模型请求', tools: '工具处理',
  mcp_startup: '业务工具启动', send: '微信发送', archive_confirmation: '归档确认',
  authorization: '登录授权', delivery: '送达',
  }[stage] || stage);
  return prefix + name;
}
export function botLatencyRows(runtime: BotRuntime): { phase: string; label: string; latest: unknown; count: number | null; p50: unknown; p95: unknown }[] {
  const metrics = runtime.latency_metrics || {};
  const latestPhases = Object.keys(runtime.last_request || {}).filter(key => key.endsWith('_seconds')).map(key => key.slice(0, -8));
  const phases = [...new Set(['trigger_to_model', 'generation', 'send_confirmation', 'end_to_end', ...Object.keys(metrics), ...latestPhases])];
  return phases.map(phase => {
    const metric = metrics[phase];
    return { phase, label: botPhaseName(phase), latest: runtime.last_request?.[`${phase}_seconds`],
      count: typeof metric?.count === 'number' && Number.isFinite(metric.count) && metric.count >= 0 ? metric.count : null,
      p50: metric?.p50_seconds, p95: metric?.p95_seconds };
  });
}
export const botStateName = (state: BotState) => ({ unknown: '未报备', available: '可用', unavailable: '已无' }[state]);
export function shanghaiDay(now = new Date()): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
}
export function botStatusFor(rows: BotStatus[], businessId: string, itemId: string, day: string): BotStatus {
  return rows.find(row => row.business_id === businessId && row.item_id === itemId && row.day === day)
    || { business_id: businessId, item_id: itemId, day, status: 'unknown', revision: 0, updated_at: 0, actor: null, message_ref: null, group_ref: null };
}
