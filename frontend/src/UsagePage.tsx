import { useRange } from './useRange';
import { useResource } from './useResource';
import { DateRange } from './RangePicker';
import { rangeQuery } from './range';
import type { User } from './types';

type Tokens = { input_tokens: number | null; cached_input_tokens: number | null; output_tokens: number | null; reasoning_output_tokens: number | null; total_tokens: number | null };
type Aggregate = Tokens & { requests: number; usage_available_requests: number; usage_unavailable_requests: number };
type OfficialWindow = { usedPercent?: number | null; windowDurationMins?: number | null; resetsAt?: number | null };
type OfficialLimits = { limitId?: string; limitName?: string; primary?: OfficialWindow | null; secondary?: OfficialWindow | null; planType?: string | null };
type QuotaSnapshot = {
  source_id: string; sampled_at: number | null; checked_at?: number | null; last_success_at?: number | null; received_at: number; stale: boolean; age_seconds: number | null;
  auth_state: unknown; rate_limits: { rateLimits?: OfficialLimits | null; rateLimitsByLimitId?: Record<string, OfficialLimits> | null }; error: string | null;
};
type UsageReport = {
  scope: 'all' | 'self'; totals: Aggregate; daily: (Aggregate & { day: string })[];
  by_kind: (Aggregate & { kind: string })[];
  recent: { run_id: string; kind: string; user_id: number | null; turn_id: string | null; model: string; started_at: number; completed_at: number | null; status: string; usage: Tokens | null }[];
  quota: null | { provider: 'codex_app_server'; binding: 'unverified'; snapshots: QuotaSnapshot[] };
};
const format = (value?: number | null) => typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat('zh-CN').format(value) : '—';
const time = (value?: number | null) => value ? new Date(value * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '未返回';
const kindName = (kind: string) => ({ summary: '群聊摘要', chat: '群聊助手', assistant: '群聊助手', interactive_chat: '群聊助手', incremental_summary: '增量摘要', daily_summary: '每日摘要', backfill_summary: '历史摘要', summary_chunk: '摘要分块' }[kind] || '后台分析');
const statusName = (status: string) => ({ completed: '已完成', success: '已完成', failed: '失败', running: '处理中', cancelled: '已取消', interrupted: '中断', queued: '等待处理' }[status] || '已记录');

function Window({ value, name }: { value?: OfficialWindow | null; name: string }) {
  if (!value || typeof value.usedPercent !== 'number' || !Number.isFinite(value.usedPercent)) return <div className="quota-window unavailable"><div><strong>{name}</strong><span>官方未返回</span></div><p>没有可确认的剩余额度数据</p></div>;
  const percent = Math.min(100, Math.max(0, value.usedPercent));
  const windowName = value.windowDurationMins === 10080 ? '周窗口' : value.windowDurationMins === 300 ? '5 小时窗口' : value.windowDurationMins ? `${format(value.windowDurationMins)} 分钟窗口` : name;
  return <div className="quota-window"><div><strong>{windowName}</strong><span>已用 {format(value.usedPercent)}%</span></div><div className="quota-track"><i className={percent >= 90 ? 'warning' : ''} style={{ width: `${percent}%` }} /></div><p>{value.resetsAt ? `官方返回的重置时间：${time(value.resetsAt)}` : '官方未返回重置时间'}</p></div>;
}

function Quota({ snapshots, loadedAt }: { snapshots: QuotaSnapshot[]; loadedAt?: number }) {
  if (!snapshots.length) return <div className="usage-empty">尚未收到 Codex 官方额度快照。项目 token 统计仍可查看，这不表示订阅余额为零。</div>;
  return <div className="quota-snapshots">{snapshots.map(snapshot => {
    const age = snapshot.age_seconds === null ? null : snapshot.age_seconds + Math.max(0, (Date.now() - (loadedAt || Date.now())) / 1000);
    const stale = snapshot.stale || age === null || age > 90;
    const main = snapshot.rate_limits?.rateLimits;
    const extras = Object.entries(snapshot.rate_limits?.rateLimitsByLimitId || {}).filter(([key]) => key !== main?.limitId);
    return <article className={`quota-snapshot ${stale ? 'stale' : ''}`} key={snapshot.source_id}><div className="quota-meta"><div><strong>Codex 官方采样</strong><small>最近成功采样 {time(snapshot.sampled_at)} · 最近检查 {time(snapshot.checked_at)} · 云端收到 {time(snapshot.received_at)}</small></div><span className={`pill ${stale ? 'warning' : 'green'}`}>{stale ? '已过期 / 无法确认' : '90 秒内的快照'}</span></div>{snapshot.error && <p className="quota-diagnostic">本次额度采样未能完整取得结果，不能据此判断余额。</p>}{stale && <p className="quota-diagnostic">快照已超过 90 秒或缺少采样时间。下方仅保留历史读数，当前额度未确认。</p>}<div className="quota-windows"><Window value={main?.primary} name="主要额度窗口" /><Window value={main?.secondary} name="次要额度窗口" /></div>{extras.length > 0 && <details className="model-quota"><summary>其他官方额度分组（{extras.length}）</summary>{extras.map(([key, limits]) => <div className="model-quota-row" key={key}><h4>{limits.limitName || limits.limitId || key}</h4><div className="quota-windows"><Window value={limits.primary} name="主要额度窗口" /><Window value={limits.secondary} name="次要额度窗口" /></div></div>)}</details>}<p className="quota-source">来源：Codex app-server · {snapshot.source_id} {main?.planType ? `· 官方返回计划 ${main.planType}` : ''}</p></article>;
  })}</div>;
}

export default function UsagePage({ user, revision, reload }: { user: User; revision: number; reload: () => void }) {
  const range = useRange(user.id, 'usage');
  const result = useResource<UsageReport>(`/api/usage?${rangeQuery(range.resolved)}`, revision);
  const data = result.data;
  const max = Math.max(1, ...(data?.daily || []).map(day => day.total_tokens || 0));
  return <><div className="page-heading"><div><p className="eyebrow">USAGE & CAPACITY</p><h1>看清用量，留好余量。</h1><p>{user.role === 'admin' ? '查看全站分析任务的 token 记录，以及单独采样的 Codex 官方额度。' : '这里只展示你自己的群聊助手任务用量。'}</p></div><span className="pill green">{user.role === 'admin' ? '管理员 · 全站' : '仅自己的任务'}</span></div><DateRange selection={range.selection} resolved={range.resolved} onChange={range.setSelection} />{result.error && <div className="notice error" role="alert"><span>{result.error}</span><button className="text-button" onClick={reload}>重试</button></div>}{!data ? <div className="skeleton-grid" aria-label="正在加载用量"><div /><div /><div /></div> : <><div className="metric-grid usage-metrics"><div className="metric"><div className="metric-label">分析任务</div><div className="metric-value">{format(data.totals.requests)}</div><p>{format(data.totals.usage_available_requests)} 次返回了 usage</p></div><div className="metric"><div className="metric-label">输入 tokens</div><div className="metric-value">{format(data.totals.input_tokens)}</div><p>其中缓存输入 {format(data.totals.cached_input_tokens)}</p></div><div className="metric"><div className="metric-label">输出 tokens</div><div className="metric-value">{format(data.totals.output_tokens)}</div><p>其中推理输出 {format(data.totals.reasoning_output_tokens)}</p></div><div className="metric"><div className="metric-label">总 tokens</div><div className="metric-value">{format(data.totals.total_tokens)}</div><p>以模型实际返回的 usage 为准</p></div></div><div className={`notice ${data.totals.usage_unavailable_requests ? 'warning' : 'usage-info'}`}><span>{data.totals.usage_unavailable_requests ? `${format(data.totals.usage_unavailable_requests)} 次任务没有返回完整 usage；这些未知部分不按 0 计，也不计入已知 token 合计。` : '缓存输入和推理输出分别是输入与输出的子项，不重复相加。'} 项目 token 数量不能换算成订阅剩余百分比。</span></div><div className="usage-grid"><section className="panel"><div className="panel-title"><div><h2>每日用量</h2><p>已返回 usage 的任务 · 上海自然日</p></div></div>{data.daily.length ? <div className="daily-usage">{data.daily.map(day => <div className="usage-day" key={day.day}><span>{day.day.slice(5)}</span><div className="usage-day-track"><i style={{ width: `${typeof day.total_tokens === 'number' ? day.total_tokens / max * 100 : 0}%` }} /></div><strong>{format(day.total_tokens)}</strong><small>{format(day.requests)} 次</small></div>)}</div> : <div className="usage-empty">这个时段还没有任务记录。</div>}</section><section className="panel"><div className="panel-title"><div><h2>按任务类型</h2><p>任务数量与已知 token 合计</p></div></div>{data.by_kind.length ? <div className="usage-kinds">{data.by_kind.map(row => <div key={row.kind}><span>{kindName(row.kind)}<small>{format(row.requests)} 次任务</small></span><strong>{format(row.total_tokens)}<small>tokens</small></strong></div>)}</div> : <div className="usage-empty">暂无可统计的任务。</div>}</section></div>{user.role === 'admin' && <section className="panel official-quota"><div className="panel-title"><div><h2>Codex 官方额度</h2><p>独立采样来源，超过 90 秒标为过期</p></div><a className="text-link" href="https://chatgpt.com/settings/usage" target="_blank" rel="noreferrer">前往官方用量 ↗</a></div><div className="notice warning quota-binding"><span>这是 Codex app-server 返回的官方额度快照，不是本项目的余额。尚未确认它与群聊摘要使用的 SIWC 授权属于同一账号或额度范围；请以官方用量页为准。</span></div><Quota snapshots={data.quota?.snapshots || []} loadedAt={result.loadedAt} /></section>}<section className="panel"><div className="panel-title"><div><h2>最近任务</h2><p>{data.scope === 'all' ? '全站任务的处理状态和实际 usage' : '只包含你的私有会话任务'}</p></div></div>{data.recent.length ? <div className="table-wrap"><table className="usage-table"><thead><tr><th>时间 / 任务</th><th>模型</th><th>状态</th><th>输入</th><th>输出</th><th>总 tokens</th></tr></thead><tbody>{data.recent.map(run => <tr key={run.run_id}><td><strong>{kindName(run.kind)}</strong><small>{time(run.started_at)}</small></td><td>{run.model || '未返回'}</td><td><span className={`pill ${run.status === 'completed' || run.status === 'success' ? 'green' : ['failed', 'interrupted'].includes(run.status) ? 'warning' : 'neutral'}`}>{statusName(run.status)}</span></td><td>{format(run.usage?.input_tokens)}</td><td>{format(run.usage?.output_tokens)}</td><td>{run.usage ? format(run.usage.total_tokens) : <span className="usage-unavailable">未返回</span>}</td></tr>)}</tbody></table></div> : <div className="usage-empty">这个时段还没有任务记录。完成分析后会保留实际用量。</div>}</section></>}</>;
}
