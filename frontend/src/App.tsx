import { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { api, appPath, patch, post, remove, setCsrfToken } from './api';
import { DateRange } from './RangePicker';
import { rangeQuery } from './range';
import type { RangeSelection, ResolvedRange } from './range';
import { useRange } from './useRange';
import { useResource } from './useResource';
import { avatarSource, canRetryAvatar, displayMemberName } from './avatar';
import ChatPage from './ChatPage';
import UsagePage from './UsagePage';
import type { Alert, Auth, Group, GroupStats, Message, Overview, Rule, Source, Summary, SummaryItem, Trend, User } from './types';

const zone = 'Asia/Shanghai';
const n = (value: number | undefined | null) => new Intl.NumberFormat('zh-CN').format(value ?? 0);
const datetime = (value?: number | null) => value ? new Date(value * 1000).toLocaleString('zh-CN', { timeZone: zone, hour12: false }) : '暂无记录';
const shortTime = (value: number) => new Date(value * 1000).toLocaleTimeString('zh-CN', { timeZone: zone, hour12: false, hour: '2-digit', minute: '2-digit' });
const ago = (value?: number | null) => {
  if (!value) return '暂无记录';
  const seconds = Math.max(0, Date.now() / 1000 - value);
  return seconds < 60 ? '刚刚' : seconds < 3600 ? `${Math.floor(seconds / 60)} 分钟前` : seconds < 86400 ? `${Math.floor(seconds / 3600)} 小时前` : `${Math.floor(seconds / 86400)} 天前`;
};
const typeName = (type: number) => ({ 1: '文字', 3: '图片', 34: '语音', 43: '视频', 47: '表情', 49: '卡片 / 文件', 10000: '系统' }[type] || `类型 ${type}`);
const groupHref = (ref: string) => `#/group/${encodeURIComponent(ref)}`;
const healthLabels: Record<string, string> = {
  receiving: '近期已收到消息', session_present: '会话存在，收取待确认', login_required: '微信需要登录', not_running: '微信未运行',
  network_unavailable: '本机网络不可用', source_unavailable: '消息来源不可用', collector_error: '采集发生错误',
  sync_unverified: '消息同步待核实', unknown: '状态无法确认',
};
const summaryLabels: Record<string, string> = {
  partial: '摘要正在分批更新', waiting: '等待整理摘要', ready: '摘要服务就绪', healthy: '摘要服务正常', idle: '等待新内容', running: '正在整理摘要', ok: '摘要服务正常',
  success: '摘要已更新', completed: '摘要已更新', not_configured: '尚未配置摘要', disabled: '摘要尚未启用', unauthorized: '摘要需要授权',
  authorization_required: '摘要需要授权', auth_required: '摘要需要授权', login_required: '摘要需要授权',
  quota_exceeded: '摘要额度已达上限', usage_limited: '摘要额度受限', paused: '摘要任务已暂停', error: '摘要任务异常',
  subscription_sharing_usage_limit_exceeded: '摘要额度已达上限',
};

function Icon({ name, size = 20, ...props }: { name: string; size?: number; className?: string }) {
  const paths: Record<string, ReactNode> = {
    grid: <><rect x="3" y="3" width="7" height="7" rx="1.4" /><rect x="14" y="3" width="7" height="7" rx="1.4" /><rect x="3" y="14" width="7" height="7" rx="1.4" /><rect x="14" y="14" width="7" height="7" rx="1.4" /></>,
    chat: <path d="M21 11.5a8.3 8.3 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.3 8.3 0 0 1-3.8-.9L3 21l1.9-5.7a8.3 8.3 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.3 8.3 0 0 1 3.8-.9h.5a8.5 8.5 0 0 1 8 8v.5Z" />,
    bell: <><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4" /><path d="M12 3V2" /></>,
    settings: <><path d="m9 3-1 3-3 1-2 3 2 2-1 3 2 3 3-1 3 3 3-1 1-3 3-1 2-3-2-2 1-3-2-3-3 1-3-3Z" /><circle cx="12" cy="11" r="3" /></>,
    arrow: <path d="M5 12h14m-5-5 5 5-5 5" />, chevron: <path d="m9 5 7 7-7 7" />,
    search: <><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 5 5" /></>,
    close: <path d="m6 6 12 12M6 18 18 6" />, logout: <><path d="M9 4H4v16h5M10 12h11m-4-4 4 4-4 4" /></>,
    refresh: <><path d="M20 7v5h-5M4 17v-5h5" /><path d="M6.2 6a8 8 0 0 1 12.6 1L20 12M4 12l1.2 5A8 8 0 0 0 17.8 18" /></>,
    clock: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
    check: <path d="m5 12 4 4L19 6" />, users: <><circle cx="9" cy="8" r="3" /><path d="M3 20v-2a6 6 0 0 1 12 0v2M16 5a3 3 0 0 1 0 6M18 15a5 5 0 0 1 3 5" /></>,
    activity: <path d="M2 12h5l3-8 4 16 3-8h5" />, calendar: <><rect x="3" y="5" width="18" height="16" rx="2" /><path d="M7 3v4m10-4v4M3 11h18" /></>,
    menu: <path d="M4 6h16M4 12h16M4 18h16" />, external: <><path d="M14 3h7v7m-1-6L10 14M10 3H4v17h17v-6" /></>,
    shield: <><path d="m12 2 8 4v6c0 5-8 10-8 10S4 17 4 12V6Z" /><path d="m8 12 3 3 5-6" /></>,
    spark: <><path d="m12 3 2.4 6.6L21 12l-6.6 2.4L12 21l-2.4-6.6L3 12l6.6-2.4Z" /><path d="m20 2 .5 1.5L22 4l-1.5.5L20 6l-.5-1.5L18 4l1.5-.5Z" /></>,
  };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name] || paths.chat}</svg>;
}

function Brand({ light = false }: { light?: boolean }) {
  return <div className={`brand ${light ? 'light' : ''}`}><div className="brand-mark"><svg viewBox="0 0 40 40" fill="none" aria-hidden="true"><path d="m7 12 5 16 8-10 8 10 5-16" stroke="currentColor" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" /></svg></div><span>WeKeeper<small>群聊工作台</small></span></div>;
}


function ErrorBox({ children, retry }: { children: ReactNode; retry?: () => void }) {
  return <div className="notice error" role="alert"><Icon name="bell" /><span>{children}</span>{retry && <button className="text-button" onClick={retry}>重试</button>}</div>;
}
function Empty({ title, children, icon = 'chat' }: { title: string; children?: ReactNode; icon?: string }) {
  return <div className="empty"><div className="empty-icon"><Icon name={icon} size={27} /></div><h3>{title}</h3><p>{children}</p></div>;
}
function Loading() { return <div className="skeleton-grid" aria-label="正在加载数据" aria-busy="true"><div /><div /><div /></div>; }
function Pill({ children, tone = 'neutral' }: { children: ReactNode; tone?: string }) { return <span className={`pill ${tone}`}><i />{children}</span>; }
function Avatar({ name, identity, avatarUrl }: { name: string; identity?: string; avatarUrl?: string | null }) {
  const [failure, setFailure] = useState<{ src: string; at: number } | null>(null);
  const [loaded, setLoaded] = useState<string | null>(null);
  const displayName = displayMemberName(name, '群');
  const hash = Array.from(identity || displayName).reduce((value, char) => (value * 31 + char.codePointAt(0)!) >>> 0, 0);
  const src = avatarSource(avatarUrl ? appPath(avatarUrl) : '', window.location.origin, appPath('/api/'));
  const failed = !!src && failure?.src === src;
  useEffect(() => {
    function retry(event: Event) {
      if (!failed || !canRetryAvatar(failure?.at ?? null, Date.now(), (event as CustomEvent<{ manual?: boolean }>).detail?.manual === true)) return;
      setLoaded(null);
      setFailure(null);
    }
    window.addEventListener('wekeeper:avatar-refresh', retry);
    return () => window.removeEventListener('wekeeper:avatar-refresh', retry);
  }, [failed, failure]);
  const state = !avatarUrl ? 'missing' : !src ? 'invalid' : failed ? 'failed' : loaded === src ? 'ready' : 'loading';
  const title = state === 'missing' ? '头像待补齐：本机尚无可用头像' : state === 'invalid' ? '头像地址不可用，暂以文字显示' : state === 'failed' ? '头像加载失败，刷新后重试' : state === 'loading' ? '头像加载中' : `${displayName}的头像`;
  return <span className={`avatar tint-${hash % 5}`} aria-hidden="true" title={title} data-avatar-state={state}>{src && !failed ? <img src={src} alt="" loading="lazy" decoding="async" onLoad={() => setLoaded(src)} onError={() => { setLoaded(null); setFailure({ src, at: Date.now() }); }} /> : Array.from(displayName)[0]}</span>;
}


function TrendChart({ data }: { data: Trend[] }) {
  const id = useId().replaceAll(':', '');
  if (!data.length) return <Empty title="这个时段暂无消息" icon="activity">选择其他日期，或等待本机同步数据。</Empty>;
  const max = Math.max(...data.map(row => row.messages), 1);
  const width = 680, height = 190, left = 42, bottom = 32, top = 14;
  const areaH = height - bottom - top;
  const gap = (width - left - 20) / data.length;
  const barWidth = Math.min(gap * .55, 46);
  return <div className="chart"><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`按日消息量：${data.map(row => `${row.day} ${row.messages}条`).join('，')}`}><defs><linearGradient id={id} x1="0" y1="0" x2="0" y2="1"><stop stopColor="#3c7d62" /><stop offset="1" stopColor="#97bda5" /></linearGradient></defs>{[0, .5, 1].map(fraction => <g key={fraction}><line x1={left} y1={top + areaH * (1 - fraction)} x2={width - 8} y2={top + areaH * (1 - fraction)} stroke="#e8ece6" strokeDasharray="4 5" /><text x={left - 10} y={top + areaH * (1 - fraction) + 4} textAnchor="end" className="chart-label">{Math.round(max * fraction)}</text></g>)}{data.map((row, index) => <g key={row.day}><rect x={left + gap * (index + .5) - barWidth / 2} y={top + areaH * (1 - row.messages / max)} width={barWidth} height={Math.max(row.messages ? 3 : 0, areaH * row.messages / max)} rx="4" fill={`url(#${id})`}><title>{row.day}：{n(row.messages)} 条</title></rect>{(index % Math.max(1, Math.ceil(data.length / 8)) === 0 || index === data.length - 1) && <text x={left + gap * (index + .5)} y={height - 8} textAnchor="middle" className="chart-label">{row.day.slice(5).replace('-', '/')}</text>}</g>)}</svg></div>;
}

function SourceStatus({ source }: { source: Source }) {
  const state = source.health?.state || 'unknown';
  const syncError = source.sync?.state === 'error';
  const good = !source.stale && !syncError && state === 'receiving';
  const bad = source.stale || syncError || ['not_running', 'login_required', 'collector_error', 'network_unavailable', 'source_unavailable'].includes(state);
  const summaryState = source.summary?.state || '';
  const summaryText = summaryLabels[summaryState] || (source.summary?.error ? '摘要任务需要关注' : '摘要状态待确认');
  const title = source.stale ? '本机同步已中断' : syncError ? '数据上传发生异常' : healthLabels[state] || '状态待确认';
  return <div className="source-status">
    <div className="source-status-head">
      <div className={`status-orb ${good ? 'good' : bad ? 'bad' : 'waiting'}`}><Icon name="activity" size={22} /></div>
      <div><strong>{title}</strong><small>{source.stale || syncError ? '已有数据可继续查看，最新数据尚未同步完成' : '本机采集 → 安全同步 → 云端看板'}</small></div>
    </div>
    <dl className="status-list">
      <div><dt>最近心跳</dt><dd title={datetime(source.received_at)}>{ago(source.received_at)}</dd></div>
      <div><dt>最近数据上传</dt><dd title={datetime(source.sync?.last_success_at)}>{ago(source.sync?.last_success_at)}</dd></div>
      {typeof source.sync?.pending_partitions === 'number' && source.sync.pending_partitions > 0 && <div><dt>等待上传</dt><dd>{n(source.sync.pending_partitions)} 个数据分区</dd></div>}
      <div><dt>微信状态</dt><dd>{healthLabels[state] || '状态待确认'}</dd></div>
      <div><dt>采集器</dt><dd>{source.collector?.running ? '正在运行' : '未运行 / 待确认'}</dd></div>
      <div><dt>最近采集</dt><dd title={datetime(source.collector?.last_cycle_at)}>{ago(source.collector?.last_cycle_at)}</dd></div>
      <div><dt>最近新消息</dt><dd title={datetime(source.last_message_at)}>{ago(source.last_message_at)}</dd></div>
      <div><dt>自动摘要</dt><dd>{summaryText}</dd></div>
    </dl>
  </div>;
}

function OverviewPage({ revision, reload, userId, groupsOnly = false }: { revision: number; reload: () => void; userId: number; groupsOnly?: boolean }) {
  const { selection, resolved: range, setSelection } = useRange(userId, 'overview');
  const [filter, setFilter] = useState('');
  const [sort, setSort] = useState('messages');
  const { data, loading, error } = useResource<Overview>(`/api/overview?${rangeQuery(range)}`, revision);
  const groups = (data?.groups || []).filter(group => group.name.toLocaleLowerCase().includes(filter.toLocaleLowerCase())).sort((a, b) => sort === 'name' ? a.name.localeCompare(b.name, 'zh-CN') : (b.messages || 0) - (a.messages || 0));
  return <><div className="page-heading"><div><p className="eyebrow">{groupsOnly ? 'GROUPS' : 'WORKSPACE OVERVIEW'}</p><h1>{groupsOnly ? '全部群聊' : '让重要的信息，浮出水面。'}</h1><p>{groupsOnly ? '查看已选择同步的群，追溯消息和讨论依据。' : '把分散的讨论、关键结论与需要关注的变化，汇聚在一处。'}</p></div><span className="timezone"><Icon name="clock" size={15} />上海时间 · UTC+8</span></div><DateRange selection={selection} resolved={range} onChange={setSelection} />{error && <ErrorBox retry={reload}>{error}</ErrorBox>}{loading && !data ? <Loading /> : data && <>{!groupsOnly && <><div className="metric-grid"><Metric label="归档消息" value={n(data.totals.messages)} icon="chat" detail="所选时段内的顶层消息" /><Metric label="活跃成员" value={n(data.totals.active_senders)} icon="users" detail="跨群、跨日去重统计" /><Metric label="已同步群聊" value={n(data.totals.groups)} icon="grid" detail="来自本机选择的群聊" /><Metric label="进行中的告警" value={n(data.totals.open_alerts)} icon="bell" detail="运行异常与关键词提醒" warning={data.totals.open_alerts > 0} /></div><div className="overview-panels"><section className="panel trend-panel"><div className="panel-title"><div><h2>讨论活跃趋势</h2><p>每天归档的消息数量</p></div><span className="legend"><i />消息量</span></div><TrendChart data={data.trend || []} /></section><section className="panel health-panel"><div className="panel-title"><div><h2>采集与上传状态</h2><p>采集状态与数据新鲜度</p></div><Icon name="activity" size={19} /></div>{data.sources.length ? data.sources.map(source => <SourceStatus source={source} key={source.source_id} />) : <Empty title="等待本机连接" icon="activity">配置本机同步服务后，这里会显示真实采集状态。</Empty>}</section></div></>}<section className="panel group-panel"><div className="panel-title"><div><h2>群聊动态 <span className="count-label">{groups.length}</span></h2><p>选择一个群，查看统计、消息和讨论摘要</p></div><div className="group-tools"><label className="search-field"><Icon name="search" size={17} /><input value={filter} onChange={event => setFilter(event.target.value)} placeholder="搜索群聊名称" aria-label="搜索群聊" /></label><select aria-label="群聊排序" value={sort} onChange={event => setSort(event.target.value)}><option value="messages">按活跃度</option><option value="name">按名称</option></select></div></div>{groups.length ? <div className="table-wrap"><table className="group-table"><thead><tr><th>群聊</th><th>消息量</th><th>活跃成员</th><th>最近消息</th><th><span className="sr-only">查看详情</span></th></tr></thead><tbody>{groups.map(group => <tr key={group.group_ref}><td><a className="group-name" href={groupHref(group.group_ref)}><Avatar name={group.name} identity={group.group_ref} avatarUrl={group.avatar_url} /><span><strong>{group.name}</strong><small>已同步的群聊</small></span></a></td><td><strong className="tabular">{n(group.messages)}</strong><span className="unit">条</span></td><td>{n(group.active_senders)}<span className="unit">人</span></td><td className="muted" title={datetime(group.last_message_at)}>{ago(group.last_message_at)}</td><td><a className="round-button" href={groupHref(group.group_ref)} aria-label={`查看${group.name}`}><Icon name="arrow" size={18} /></a></td></tr>)}</tbody></table></div> : <Empty title={filter ? '没有找到匹配的群聊' : '等待第一批群聊数据'}>{filter ? '试试其他群聊名称。' : '请先在本机选择要同步的群。群聊数据不会自动全部上传。'}</Empty>}</section>{!groupsOnly && <section className="panel"><div className="panel-title"><div><h2>需要关注</h2><p>最新的运行告警与提醒</p></div><a className="text-link" href="#/alerts">查看全部 <Icon name="arrow" size={15} /></a></div>{data.alerts?.length ? <div className="compact-alerts">{data.alerts.slice(0, 3).map(alert => <a href="#/alerts" className="compact-alert" key={alert.id}><span className={`alert-mark ${alert.severity}`}><Icon name={alert.resolved_at ? 'check' : 'bell'} size={18} /></span><div><strong>{alert.title}</strong><p>{alert.body}</p></div><time>{ago(alert.opened_at)}</time><Icon name="chevron" size={16} /></a>)}</div> : <div className="all-clear"><span><Icon name="check" size={19} /></span><div><strong>目前没有需要关注的告警</strong><p>新的异常和关键词提醒会自动出现在这里。</p></div></div>}</section>}</>}</>;
}

function Metric({ label, value, detail, icon, warning = false }: { label: string; value: string; detail: string; icon: string; warning?: boolean }) {
  return <div className={`metric ${warning ? 'warning' : ''}`}><div className="metric-label">{label}<span><Icon name={icon} size={20} /></span></div><div className="metric-value">{value}</div><p>{detail}</p></div>;
}

function SummaryList({ summaries, openMessage }: { summaries: Summary[]; openMessage: (ref: string) => void }) {
  const [all, setAll] = useState(false);
  if (!summaries.length) return <Empty title="摘要正在路上" icon="spark">本机完成 ChatGPT 授权后，每 30 分钟整理有变化的讨论；完成的摘要会自动同步到这里。</Empty>;
  function items(label: string, rows: SummaryItem[], style: string) {
    if (!rows?.length) return null;
    return <div className={`summary-section ${style}`}><h4>{label}</h4>{rows.map((item, index) => <div className="summary-item" key={index}><p>{item.text}</p><div className="citations">{item.message_refs?.map((ref, refIndex) => <button key={ref} onClick={() => openMessage(ref)} aria-label={`查看${label}第${index + 1}项的原文依据${refIndex + 1}`}>依据 {refIndex + 1}<Icon name="arrow" size={11} /></button>)}</div></div>)}</div>;
  }
  return <div className="summaries">{(all ? summaries : summaries.slice(0, 1)).map(summary => <article className="summary-card" key={`${summary.day}-${summary.generated_at}`}><div className="summary-meta"><strong>{summary.day} · 讨论摘要</strong><Pill tone={summary.stale || summary.processing?.baseline_complete === false ? 'warning' : 'green'}>{summary.stale ? '原文已更新，待重新生成' : summary.processing?.baseline_complete === false ? '部分覆盖 / 尚未完整汇总' : '已生成'}</Pill></div>{summary.processing?.baseline_complete === false && <p className="summary-partial">这份摘要只覆盖已处理的内容，仍有历史待汇总，不能视为完整日总结。</p>}{items('讨论主题', summary.summary.topics, 'topics')}{items('重要结论', summary.summary.decisions, 'decisions')}{items('明确待办', summary.summary.todos, 'todos')}<div className="summary-foot"><span>覆盖至 {datetime(summary.covered_until)}</span><span>{summary.processing?.mode === 'incremental' ? '增量摘要' : '旧版摘要'} · 生成于 {datetime(summary.generated_at)} · {summary.model}</span></div></article>)}{summaries.length > 1 && <button className="button secondary full-width" onClick={() => setAll(!all)}>{all ? '收起历史摘要' : `查看其余 ${summaries.length - 1} 份摘要`}</button>}</div>;
}

function GroupPage({ groupRef, revision, reload, userId, openMessage }: { groupRef: string; revision: number; reload: () => void; userId: number; openMessage: (ref: string) => void }) {
  const { selection, resolved, setSelection, refreshNow } = useRange(userId, `group:${groupRef}`);
  const [frozen, setFrozen] = useState<ResolvedRange | null>(null);
  const range = frozen || resolved;
  const [tab, setTab] = useState<'summary' | 'messages'>('summary');
  const [search, setSearch] = useState('');
  const [query, setQuery] = useState('');
  const [cursor, setCursor] = useState<string | null>(null);
  const [trail, setTrail] = useState<(string | null)[]>([]);
  const base = `/api/groups/${encodeURIComponent(groupRef)}`;
  const rangeQ = rangeQuery(range);
  const stats = useResource<GroupStats>(`${base}/stats?${rangeQ}`, revision);
  const messagesPath = `${base}/messages?${rangeQ}&q=${encodeURIComponent(query)}&limit=50${cursor ? `&before=${encodeURIComponent(cursor)}` : ''}`;
  const messages = useResource<{ messages: Message[]; next_cursor: string | null }>(messagesPath, revision);
  const summary = useResource<{ summaries: Summary[] }>(`${base}/summaries`, revision);
  useEffect(() => {
    if (messages.errorCode === 'cursor_snapshot_changed' && cursor) { setCursor(null); setTrail([]); }
  }, [messages.errorCode, cursor]);
  function changeRange(value: RangeSelection) { setSelection(value); setFrozen(null); setCursor(null); setTrail([]); }
  function latest() { refreshNow(); setFrozen(null); setCursor(null); setTrail([]); reload(); }
  function searchMessages(event: FormEvent) { event.preventDefault(); setQuery(search); setCursor(null); setTrail([]); }
  return <><a href="#/groups" className="back-link">← 返回群聊列表</a><div className="page-heading group-heading"><div className="detail-title"><Avatar name={stats.data?.name || '群'} identity={groupRef} avatarUrl={stats.data?.avatar_url} /><div><p className="eyebrow">GROUP INSIGHTS</p><h1>{stats.data?.name || '群聊详情'}</h1><p>从讨论到结论，保留每一条信息的依据。</p></div></div><a className="button secondary" href={`#/assistant?group=${encodeURIComponent(groupRef)}`}>询问群聊助手 ↗</a></div><DateRange selection={selection} resolved={range} onChange={changeRange} />{frozen && <div className="pagination-freeze" role="status"><span>正在浏览固定快照 · 截至 {datetime(frozen.until)}，翻页时暂停时间滚动</span><button className="button secondary small" onClick={latest}>查看最新</button></div>}{stats.error && <ErrorBox retry={reload}>{stats.error}</ErrorBox>}{!stats.data && stats.loading ? <Loading /> : stats.data && <><div className="detail-metrics"><div><span>时段消息</span><strong>{n(stats.data.messages)}<small>条</small></strong></div><div><span>活跃成员</span><strong>{n(stats.data.active_senders)}<small>人</small></strong></div><div><span>统计口径</span><p>顶层群消息 · 发言者去重</p></div></div><FreshnessNotice source={stats.data.source} /><Coverage data={stats.data.coverage} /><div className="detail-layout"><div className="detail-main"><section className="panel"><div className="tabs"><button className={tab === 'summary' ? 'active' : ''} onClick={() => setTab('summary')}><Icon name="spark" size={17} />讨论摘要</button><button className={tab === 'messages' ? 'active' : ''} onClick={() => setTab('messages')}><Icon name="chat" size={17} />消息记录</button></div>{tab === 'summary' ? <>{summary.error && <ErrorBox retry={reload}>{summary.error}</ErrorBox>}{summary.loading && !summary.data ? <Loading /> : <SummaryList summaries={(summary.data?.summaries || []).filter(row => row.day >= range.from && row.day <= range.to)} openMessage={openMessage} />}</> : <><form className="message-search" onSubmit={searchMessages}><label className="search-field"><Icon name="search" size={17} /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="搜索这个时段的消息文字" aria-label="搜索消息" /></label><button className="button secondary" type="submit">搜索</button></form>{messages.error && <ErrorBox retry={reload}>{messages.error}</ErrorBox>}{messages.loading && !messages.data ? <Loading /> : messages.data?.messages.length ? <><div className="message-list">{messages.data.messages.map(message => <MessageRow message={message} key={message.message_ref} openMessage={openMessage} />)}</div><div className="pagination"><button className="button secondary" disabled={!trail.length} onClick={() => { setCursor(trail[trail.length - 1] ?? null); setTrail(trail.slice(0, -1)); }}>上一页</button><span>第 {trail.length + 1} 页 · 每页最多 50 条</span><button className="button secondary" disabled={!messages.data?.next_cursor || messages.loading || messages.loadedPath !== messagesPath} onClick={() => { setFrozen(range); setTrail([...trail, cursor]); setCursor(messages.data?.next_cursor || null); }}>下一页</button></div></> : <Empty title="没有找到消息">换一个时间范围或搜索词试试；本地未同步的历史不会显示在这里。</Empty>}</>}</section></div><aside className="detail-side"><section className="panel group-source-panel"><div className="panel-title"><div><h2>采集与上传状态</h2><p>看板连接与本机数据更新分别确认</p></div></div>{stats.data.source ? <SourceStatus source={stats.data.source} /> : <Empty title="等待本机状态" icon="activity">还未收到来源心跳，无法确认数据新鲜度。</Empty>}</section><section className="panel"><div className="panel-title"><div><h2>活跃成员</h2><p>当前时段的发言排行</p></div></div>{stats.data.senders.length ? <div className="sender-list">{stats.data.senders.slice(0, 8).map((sender, index) => <div key={sender.sender_id}><span className="rank">{String(index + 1).padStart(2, '0')}</span><Avatar name={displayMemberName(sender.sender_name)} identity={sender.sender_id} avatarUrl={sender.avatar_url} /><span className="sender-name">{displayMemberName(sender.sender_name)}</span><strong>{n(sender.messages)}</strong></div>)}</div> : <Empty title="暂无发言记录" icon="users" />}</section><section className="panel"><div className="panel-title"><div><h2>消息构成</h2><p>按顶层消息类型统计</p></div></div><div className="type-list">{stats.data.types.map((type, index) => <div key={type.msg_type}><div><span><i className={`type-dot tint-${index % 5}`} />{typeName(type.msg_type)}</span><strong>{n(type.messages)}</strong></div><div className="type-track"><i style={{ width: `${stats.data!.messages ? type.messages / stats.data!.messages * 100 : 0}%` }} /></div></div>)}</div></section></aside></div></>}</>;
}

function Coverage({ data }: { data: Record<string, unknown> }) {
  const complete = data.local_backfill_complete === true;
  const gaps = Array.isArray(data.gaps) ? data.gaps : [];
  const hasExplicitGap = gaps.length > 0 || data.local_backfill_complete === false || data.complete === false || data.source_available === false;
  let text = '统计仅包含已归档并同步的消息；不代表微信完整历史。';
  if (hasExplicitGap) {
    text = data.source_available === false ? '消息来源存在不可用部分，数据覆盖不完整。' : data.local_backfill_complete === false ? '本机历史仍在回填，数据覆盖尚不完整。' : '数据覆盖存在缺口，不能视为完整历史。';
    text += '统计仅包含已同步到本机并上传的消息。';
    if (gaps.length) text += ` 当前记录了 ${gaps.length} 项覆盖缺口。`;
  } else if (complete) text = '本机可读历史已完成回填；统计以已归档内容为准，不代表当前收取与上传正常。';
  return <div className={`coverage ${hasExplicitGap ? 'incomplete' : ''}`} role={hasExplicitGap ? 'status' : undefined}><Icon name="shield" size={16} /><span>{text}</span></div>;
}

function FreshnessNotice({ source }: { source?: Source | null }) {
  if (!source) return <div className="notice warning" role="status"><Icon name="clock" size={17} /><span>尚未收到本机状态，无法确认数据是否仍在更新。下方内容仅代表已经上传的归档。</span></div>;
  if (source.stale) return <div className="notice warning" role="status"><Icon name="clock" size={17} /><span>本机同步已中断，当前显示的是已有数据。最近心跳：{datetime(source.received_at)}；最近数据上传：{datetime(source.sync?.last_success_at)}。</span></div>;
  if (source.sync?.state === 'error') return <div className="notice warning" role="status"><Icon name="bell" size={17} /><span>本机心跳正常，但数据上传发生异常，最新消息可能尚未显示。最近成功上传：{datetime(source.sync.last_success_at)}{source.sync.pending_partitions ? `；${n(source.sync.pending_partitions)} 个数据分区等待上传` : ''}。</span></div>;
  return null;
}

function MessageRow({ message, openMessage, expanded = false }: { message: Message; openMessage: (ref: string) => void; expanded?: boolean }) {
  return <article className={`message-row ${expanded ? 'expanded' : ''}`}><Avatar name={displayMemberName(message.sender_name)} identity={message.sender_id || message.message_ref} avatarUrl={message.avatar_url} /><div className="message-body"><div className="message-head"><strong>{displayMemberName(message.sender_name)}</strong><span className="message-type">{typeName(message.msg_type)}</span><time title={datetime(message.create_time)}>{expanded ? datetime(message.create_time) : shortTime(message.create_time)}</time></div>{message.reply && <button className="quote" onClick={() => message.reply?.target_ref && openMessage(message.reply.target_ref)} disabled={!message.reply.target_ref}>{message.reply.text || '查看引用消息'}{!message.reply.target_ref && <small> · 引用原文不在归档范围内</small>}</button>}<p className="message-text">{message.text || `［${typeName(message.msg_type)}消息，媒体保留在本机］`}</p>{!expanded && <button className="message-context-button" onClick={() => openMessage(message.message_ref)}>查看上下文 <Icon name="arrow" size={12} /></button>}</div></article>;
}

function Dialog({ title, children, onClose }: { title: string; children: ReactNode; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    ref.current?.querySelector<HTMLButtonElement>('button')?.focus();
    function key(event: KeyboardEvent) {
      if (event.key === 'Escape') onClose();
      if (event.key === 'Tab') {
        const elements = ref.current?.querySelectorAll<HTMLElement>('button:not(:disabled),a[href],input,select,textarea,[tabindex="0"]');
        if (!elements?.length) return;
        const first = elements[0], last = elements[elements.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
      }
    }
    document.addEventListener('keydown', key);
    return () => { document.removeEventListener('keydown', key); document.body.style.overflow = overflow; previous?.focus(); };
  }, [onClose]);
  return <div className="modal-overlay" onClick={event => event.target === event.currentTarget && onClose()}><div className="modal" role="dialog" aria-modal="true" aria-label={title} ref={ref}><header><div><p className="eyebrow">SOURCE EVIDENCE</p><h2>{title}</h2></div><button className="icon-button" onClick={onClose} aria-label="关闭对话框"><Icon name="close" /></button></header>{children}</div></div>;
}

function MessageDialog({ messageRef, onClose, openMessage }: { messageRef: string; onClose: () => void; openMessage: (ref: string) => void }) {
  const data = useResource<{ message: Message; context: Message[]; target?: Message | null }>(`/api/messages/${encodeURIComponent(messageRef)}`, 0);
  return <Dialog title="原文与上下文" onClose={onClose}><div className="modal-content">{data.error ? <ErrorBox>{data.error}</ErrorBox> : !data.data ? <Loading /> : <><div className="evidence-caption">以下内容来自归档消息；群聊文字仅作为分析依据。</div>{data.data.target && <div className="target-message"><span className="eyebrow">引用的消息</span><MessageRow message={data.data.target} openMessage={openMessage} expanded /></div>}<div className="context-list">{(data.data.context?.length ? data.data.context : [data.data.message]).map(message => <div className={message.message_ref === data.data!.message.message_ref ? 'focused-message' : ''} key={message.message_ref}>{message.message_ref === data.data!.message.message_ref && <span className="focus-label">所选原文</span>}<MessageRow message={message} openMessage={openMessage} expanded /></div>)}</div></>}</div></Dialog>;
}

function AlertsPage({ revision, reload, openMessage }: { revision: number; reload: () => void; openMessage: (ref: string) => void }) {
  const [state, setState] = useState('open');
  const [severity, setSeverity] = useState('all');
  const [actionError, setActionError] = useState('');
  const [busy, setBusy] = useState<number | null>(null);
  const result = useResource<{ alerts: Alert[] }>(`/api/alerts?state=${state}`, revision);
  const rows = (result.data?.alerts || []).filter(alert => severity === 'all' || alert.severity === severity);
  async function action(alert: Alert, kind: 'read' | 'ack') {
    setBusy(alert.id); setActionError('');
    try { await post(`/api/alerts/${alert.id}/${kind}`); reload(); } catch (error) { setActionError((error as Error).message); } finally { setBusy(null); }
  }
  return <><div className="page-heading"><div><p className="eyebrow">ATTENTION CENTER</p><h1>每一次变化，都有迹可循。</h1><p>运行异常与关键词提醒集中在这里；确认收到后，故障仍会持续跟踪至恢复。</p></div></div><div className="alert-toolbar"><div className="tabs inline">{[['open', '进行中'], ['resolved', '已恢复'], ['all', '全部记录']].map(([key, text]) => <button className={state === key ? 'active' : ''} key={key} onClick={() => setState(key)}>{text}</button>)}</div><select aria-label="按严重程度筛选" value={severity} onChange={event => setSeverity(event.target.value)}><option value="all">所有级别</option><option value="error">严重</option><option value="warning">警告</option><option value="info">提醒</option></select></div>{(actionError || result.error) && <ErrorBox retry={reload}>{actionError || result.error}</ErrorBox>}{result.loading && !result.data ? <Loading /> : rows.length ? <div className="alerts-list">{rows.map(alert => <article className={`alert-card ${!alert.read_at ? 'unread' : ''}`} key={alert.id}><span className={`alert-mark ${alert.severity}`}><Icon name={alert.resolved_at ? 'check' : 'bell'} size={22} /></span><div className="alert-content"><div className="alert-title"><h2>{alert.title}</h2>{!alert.read_at && <span className="unread-dot" title="未读" />}<Pill tone={alert.resolved_at ? 'green' : alert.severity === 'error' ? 'red' : 'warning'}>{alert.resolved_at ? '已恢复' : '进行中'}</Pill></div><p>{alert.body}</p><div className="alert-times"><span>发生于 {datetime(alert.opened_at)}</span>{alert.resolved_at && <span>恢复于 {datetime(alert.resolved_at)}</span>}</div><div className="alert-actions">{alert.group_ref && <a className="text-link" href={groupHref(alert.group_ref)}>查看群聊 <Icon name="arrow" size={13} /></a>}{alert.message_ref && <button className="text-button" onClick={() => openMessage(alert.message_ref!)}>查看原文依据</button>}{alert.acknowledged_at ? <span className="acknowledged"><Icon name="check" size={14} />你已确认 · {ago(alert.acknowledged_at)}</span> : <button className="button secondary small" disabled={busy === alert.id} onClick={() => action(alert, 'ack')}>确认收到</button>}{!alert.read_at && <button className="text-button muted" disabled={busy === alert.id} onClick={() => action(alert, 'read')}>标记已读</button>}</div></div></article>)}</div> : <section className="panel"><Empty title={state === 'open' ? '目前没有进行中的告警' : '暂无匹配的告警记录'} icon="check">新的运行异常和关键词提醒会自动保存在这里，关闭网页也不会丢失。</Empty></section>}</>;
}

function SettingsPage({ revision, reload, user }: { revision: number; reload: () => void; user: User }) {
  const [tab, setTab] = useState<'rules' | 'users'>('rules');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);
  const [ruleForm, setRuleForm] = useState({ name: '', keyword: '', group_ref: '' });
  const [userForm, setUserForm] = useState({ username: '', password: '', role: 'viewer' });
  const [resetUser, setResetUser] = useState<User | null>(null);
  const [password, setPassword] = useState('');
  const admin = user.role === 'admin';
  const rules = useResource<{ rules: Rule[] }>(admin ? '/api/rules' : null, revision);
  const users = useResource<{ users: User[] }>(admin ? '/api/users' : null, revision);
  const groups = useResource<{ groups: Group[] }>('/api/groups', revision);
  async function run(action: () => Promise<unknown>, message: string, done?: () => void) {
    setBusy(true); setError(''); setNotice('');
    try { await action(); setNotice(message); done?.(); reload(); } catch (failure) { setError((failure as Error).message); } finally { setBusy(false); }
  }
  const closeReset = useCallback(() => { setResetUser(null); setPassword(''); }, []);
  return <><div className="page-heading"><div><p className="eyebrow">WORKSPACE SETTINGS</p><h1>把关注的事，设为规则。</h1><p>{admin ? '管理关键词提醒和同事账号，让信息各归其位。' : '当前账号为查看者，可以查看群聊和确认自己的告警。'}</p></div><Pill tone="green">{admin ? '管理员' : '查看者'}</Pill></div><section className="panel account-panel"><div className="panel-title"><div><h2>工作台账号</h2><p>{user.username} · {admin ? '管理员' : '查看者'}</p></div><Icon name="shield" /></div><div className="settings-note"><Icon name="spark" size={20} /><div><strong>摘要在本机独立运行</strong><p>ChatGPT 授权留在采集机器，云端不保存模型凭据。摘要额度和授权状态可在总览的数据连接中查看。</p><a className="text-link" href="https://chatgpt.com/settings/usage" target="_blank" rel="noreferrer">查看 ChatGPT 用量 <Icon name="external" size={13} /></a></div></div></section>{admin && <><div className="tabs settings-tabs"><button className={tab === 'rules' ? 'active' : ''} onClick={() => setTab('rules')}>关键词规则</button><button className={tab === 'users' ? 'active' : ''} onClick={() => setTab('users')}>账号管理</button></div>{(error || (tab === 'rules' ? rules.error : users.error)) && <ErrorBox retry={reload}>{error || (tab === 'rules' ? rules.error : users.error)}</ErrorBox>}{notice && <div className="notice success" role="status"><Icon name="check" size={18} />{notice}</div>}{tab === 'rules' ? <div className="settings-layout"><section className="panel"><div className="panel-title"><div><h2>已配置规则</h2><p>匹配新消息中的文字，首次历史导入不会触发提醒</p></div></div>{rules.loading && !rules.data ? <Loading /> : rules.data?.rules.length ? <div className="rule-list">{rules.data.rules.map(rule => <article className="rule-item" key={rule.id}><div><strong>{rule.name}</strong><p>包含 <code>{rule.keyword}</code></p><small>{rule.group_ref ? groups.data?.groups.find(group => group.group_ref === rule.group_ref)?.name || '指定群聊' : '全部已同步群聊'}</small></div><div className="rule-item-actions"><button className={`toggle ${rule.enabled ? 'on' : ''}`} role="switch" aria-checked={rule.enabled} aria-label={`${rule.enabled ? '停用' : '启用'}${rule.name}`} disabled={busy} onClick={() => run(() => patch(`/api/rules/${rule.id}`, { enabled: !rule.enabled }), rule.enabled ? '规则已停用。' : '规则已启用。')}><span /></button><button className="text-button danger" disabled={busy} onClick={() => { if (window.confirm(`删除规则“${rule.name}”？已有告警记录会保留。`)) run(() => remove(`/api/rules/${rule.id}`), '规则已删除。'); }}>删除</button></div></article>)}</div> : <Empty title="还没有关键词规则" icon="bell">添加关心的词语，后续匹配的新消息会生成站内提醒。</Empty>}</section><section className="panel form-panel"><div className="panel-title"><div><h2>新建提醒规则</h2><p>使用明确的文字，减少无关提醒</p></div></div><form onSubmit={event => { event.preventDefault(); run(() => post('/api/rules', { ...ruleForm, group_ref: ruleForm.group_ref || null, enabled: true }), '提醒规则已创建。', () => setRuleForm({ name: '', keyword: '', group_ref: '' })); }}><label>规则名称<input required maxLength={100} placeholder="例如：紧急事项" value={ruleForm.name} onChange={event => setRuleForm({ ...ruleForm, name: event.target.value })} /></label><label>匹配文字<input required maxLength={200} placeholder="例如：需要尽快确认" value={ruleForm.keyword} onChange={event => setRuleForm({ ...ruleForm, keyword: event.target.value })} /><small>按文字包含关系匹配，不使用正则表达式。</small></label><label>关注的群聊<select value={ruleForm.group_ref} onChange={event => setRuleForm({ ...ruleForm, group_ref: event.target.value })}><option value="">全部已同步群聊</option>{groups.data?.groups.map(group => <option key={group.group_ref} value={group.group_ref}>{group.name}</option>)}</select></label><button className="button primary full-width" disabled={busy} type="submit">创建规则 <Icon name="arrow" size={16} /></button></form></section></div> : <div className="settings-layout"><section className="panel"><div className="panel-title"><div><h2>工作台成员</h2><p>独立账号登录，已读和确认状态各自保存</p></div></div>{users.loading && !users.data ? <Loading /> : <div className="user-list">{users.data?.users.map(member => <article className="user-item" key={member.id}><Avatar name={member.username} identity={String(member.id)} /><div><strong>{member.username}{member.id === user.id && <small>（你）</small>}</strong><p>{member.role === 'admin' ? '管理员' : '查看者'} · {member.disabled ? '已停用' : '可正常登录'}</p></div><div className="user-actions"><button className="text-button" disabled={busy} onClick={() => setResetUser(member)}>重设密码</button>{member.id !== user.id && <button className="text-button" disabled={busy} onClick={() => run(() => patch(`/api/users/${member.id}`, { role: member.role === 'admin' ? 'viewer' : 'admin' }), '账号角色已更新。')}>{member.role === 'admin' ? '设为查看者' : '设为管理员'}</button>}{member.id !== user.id && <button className={`text-button ${member.disabled ? '' : 'danger'}`} disabled={busy} onClick={() => { if (member.disabled || window.confirm(`停用 ${member.username}？此账号将无法继续访问看板。`)) run(() => patch(`/api/users/${member.id}`, { disabled: !member.disabled }), member.disabled ? '账号已启用。' : '账号已停用。'); }}>{member.disabled ? '启用' : '停用'}</button>}</div></article>)}</div>}</section><section className="panel form-panel"><div className="panel-title"><div><h2>添加成员</h2><p>为同事建立独立访问账号</p></div></div><form onSubmit={event => { event.preventDefault(); run(() => post('/api/users', userForm), '成员账号已创建，请通过可信渠道告知对方。', () => setUserForm({ username: '', password: '', role: 'viewer' })); }}><label>账号名称<input required minLength={3} maxLength={64} pattern="[A-Za-z0-9_.@-]+" title="使用字母、数字或 . _ @ -" autoComplete="off" value={userForm.username} onChange={event => setUserForm({ ...userForm, username: event.target.value })} /></label><label>初始密码<input required type="password" minLength={12} maxLength={256} autoComplete="new-password" value={userForm.password} onChange={event => setUserForm({ ...userForm, password: event.target.value })} /><small>至少 12 个字符，请使用独立密码。</small></label><label>角色<select value={userForm.role} onChange={event => setUserForm({ ...userForm, role: event.target.value })}><option value="viewer">查看者</option><option value="admin">管理员</option></select></label><button className="button primary full-width" disabled={busy} type="submit">添加成员 <Icon name="arrow" size={16} /></button></form></section></div>}</>}{resetUser && <Dialog title={`重设 ${resetUser.username} 的密码`} onClose={closeReset}><form className="modal-content reset-form" onSubmit={event => { event.preventDefault(); run(() => patch(`/api/users/${resetUser.id}`, { password }), '密码已重设，该账号需要重新登录。', closeReset); }}><label>新密码<input autoFocus type="password" required minLength={12} maxLength={256} value={password} autoComplete="new-password" onChange={event => setPassword(event.target.value)} /></label><p className="muted">至少 12 个字符。保存后，该成员的现有登录会话将失效。</p><button className="button primary" disabled={busy}>保存新密码</button></form></Dialog>}</>;
}

function Login({ onLogin }: { onLogin: (auth: Auth) => void }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try { const auth = await post<Auth>('/api/auth/login', { username, password }); setPassword(''); onLogin(auth); } catch (failure) { setError((failure as Error).message); } finally { setBusy(false); }
  }
  return <main className="login-screen"><section className="login-story"><Brand light /><div className="login-story-content"><span className="login-kicker">LESS NOISE. MORE CONTEXT.</span><h1>讨论很多，<br />重要的事<br /><em>一目了然。</em></h1><p>连接群聊里的信息，留住每个结论的来处。<br />你的群聊工作台，随时就位。</p><div className="story-orbit" aria-hidden="true"><div className="orbit-ring ring-one" /><div className="orbit-ring ring-two" /><span className="orbit-chip chip-a"><Icon name="chat" size={19} />群聊动态</span><span className="orbit-chip chip-b"><Icon name="spark" size={19} />讨论摘要</span><span className="orbit-chip chip-c"><Icon name="bell" size={19} />重要提醒</span><span className="orbit-center"><Icon name="grid" size={30} /></span></div></div><div className="login-story-foot"><span className="tiny-dot" />本机采集 · 云端汇聚 · 原文可追溯</div></section><section className="login-form-side"><div className="login-mobile-brand"><Brand /></div><div className="login-form-wrap"><span className="login-welcome"><Icon name="shield" size={19} /></span><p className="eyebrow">WELCOME TO YOUR WORKSPACE</p><h2>欢迎回来</h2><p className="login-description">登录你的账号，看看群聊里的最新进展。</p><form onSubmit={submit}>{error && <ErrorBox>{error}</ErrorBox>}<label>账号<input autoFocus name="username" autoComplete="username" placeholder="输入账号名称" value={username} onChange={event => setUsername(event.target.value)} required disabled={busy} /></label><label>密码<input name="password" type="password" autoComplete="current-password" placeholder="输入登录密码" value={password} onChange={event => setPassword(event.target.value)} required disabled={busy} /></label><button className="button primary full-width login-submit" type="submit" disabled={busy}>{busy ? '正在登录…' : '进入工作台'}{!busy && <Icon name="arrow" size={19} />}</button></form><p className="login-help">账号由工作台管理员创建。需要访问权限或重设密码，请联系管理员。</p></div><footer>WeKeeper <span>·</span> 为有依据的信息而建</footer></section></main>;
}

export default function App() {
  const [auth, setAuth] = useState<Auth | null>(null);
  const [checking, setChecking] = useState(true);
  const [bootError, setBootError] = useState('');
  const [route, setRoute] = useState(() => window.location.hash.slice(1) || '/');
  const [revision, setRevision] = useState(0);
  const [connection, setConnection] = useState<'connecting' | 'live' | 'reconnecting'>('connecting');
  const [mobileOpen, setMobileOpen] = useState(false);
  const [messageRef, setMessageRef] = useState<string | null>(null);
  const [logoutError, setLogoutError] = useState('');
  const reload = useCallback(() => { window.dispatchEvent(new Event('wekeeper:refresh-time')); window.dispatchEvent(new CustomEvent('wekeeper:avatar-refresh')); setRevision(value => value + 1); }, []);
  const closeMessage = useCallback(() => setMessageRef(null), []);
  const login = useCallback((value: Auth) => { setAuth(value); setCsrfToken(value.csrf_token); setBootError(''); }, []);
  const checkAuth = useCallback(() => {
    setChecking(true); setBootError('');
    api<Auth>('/api/auth/me').then(login).catch(error => {
      setAuth(null);
      if (!(error as Error).message.includes('登录') && !(error as Error).message.includes('账号')) setBootError((error as Error).message);
    }).finally(() => setChecking(false));
  }, [login]);
  useEffect(checkAuth, [checkAuth]);
  useEffect(() => {
    function change() { setRoute(window.location.hash.slice(1) || '/'); setMobileOpen(false); window.scrollTo(0, 0); }
    function expired() { setAuth(null); setCsrfToken(''); }
    window.addEventListener('hashchange', change); window.addEventListener('wekeeper:expired', expired);
    return () => { window.removeEventListener('hashchange', change); window.removeEventListener('wekeeper:expired', expired); };
  }, []);
  useEffect(() => {
    if (!auth) return;
    const source = new EventSource(appPath('/api/events'));
    let connected = false;
    source.onopen = () => { connected = true; setConnection('live'); reload(); };
    source.addEventListener('refresh', reload);
    source.addEventListener('auth_required', () => { source.close(); window.dispatchEvent(new Event('wekeeper:expired')); });
    source.onerror = () => { connected = false; setConnection('reconnecting'); };
    const timer = window.setInterval(() => { if (!connected && document.visibilityState === 'visible') reload(); }, 30000);
    const visibility = () => { if (document.visibilityState === 'visible') reload(); };
    document.addEventListener('visibilitychange', visibility);
    return () => { source.close(); window.clearInterval(timer); document.removeEventListener('visibilitychange', visibility); };
  }, [auth, reload]);
  async function logout() {
    try { await post('/api/auth/logout'); setAuth(null); setCsrfToken(''); setMessageRef(null); } catch (error) { setLogoutError((error as Error).message); }
  }
  if (checking) return <div className="boot-screen"><Brand /><span className="spinner" /><p>正在连接工作台…</p></div>;
  if (bootError && !auth) return <div className="boot-screen"><Brand /><ErrorBox retry={checkAuth}>{bootError}</ErrorBox></div>;
  if (!auth) return <Login onLogin={login} />;
  const page = route.startsWith('/group/') ? 'group' : route.startsWith('/assistant') ? 'assistant' : route === '/usage' ? 'usage' : route === '/groups' ? 'groups' : route === '/alerts' ? 'alerts' : route === '/settings' ? 'settings' : 'overview';
  const assistantGroup = page === 'assistant' ? new URLSearchParams(route.split('?')[1] || '').get('group') || undefined : undefined;
  let groupRef = '';
  try { groupRef = page === 'group' ? decodeURIComponent(route.slice(7)) : ''; } catch { /* malformed hash shows overview */ }
  const nav = [{ key: 'overview', href: '#/', icon: 'grid', title: '总览' }, { key: 'groups', href: '#/groups', icon: 'chat', title: '群聊' }, { key: 'assistant', href: '#/assistant', icon: 'spark', title: '群聊助手' }, { key: 'alerts', href: '#/alerts', icon: 'bell', title: '告警中心' }, { key: 'usage', href: '#/usage', icon: 'activity', title: '用量与额度' }, { key: 'settings', href: '#/settings', icon: 'settings', title: '设置' }];
  return <div className="app-layout">{mobileOpen && <button className="sidebar-backdrop" onClick={() => setMobileOpen(false)} aria-label="关闭导航" />}<aside className={`sidebar ${mobileOpen ? 'open' : ''}`}><Brand light /><div className="workspace-label"><span className="workspace-icon">W</span><div>群聊工作台<small>团队共享空间</small></div><Icon name="shield" size={16} /></div><p className="nav-label">工作空间</p><nav>{nav.map(item => <a key={item.key} className={page === item.key || page === 'group' && item.key === 'groups' ? 'active' : ''} href={item.href}><Icon name={item.icon} size={19} /><span>{item.title}</span></a>)}</nav><div className="sidebar-note"><span className="sidebar-note-icon"><Icon name="spark" size={20} /></span><strong>从信息，走向洞察。</strong><p>每一份摘要都有来处，<br />每一次变化都有记录。</p></div><div className="sidebar-footer"><span className={`connection-dot ${connection === 'live' ? 'live' : ''}`} /><span>{connection === 'live' ? '看板已连接' : '正在恢复看板连接'}</span></div></aside><div className="main-layout"><header className="topbar"><div className="topbar-left"><button className="icon-button mobile-menu" onClick={() => setMobileOpen(!mobileOpen)} aria-label="打开导航"><Icon name="menu" /></button><span className="breadcrumb">工作空间<Icon name="chevron" size={12} /><strong>{page === 'group' ? '群聊详情' : nav.find(item => item.key === page)?.title}</strong></span></div><div className="topbar-right"><span className={`connection-label ${connection === 'live' ? '' : 'waiting'}`}><span />{connection === 'live' ? '看板已连接' : '看板重连中'}</span><button className="icon-button" onClick={() => { window.dispatchEvent(new CustomEvent('wekeeper:avatar-refresh', { detail: { manual: true } })); reload(); }} title="刷新数据" aria-label="刷新数据"><Icon name="refresh" size={17} /></button><div className="user-chip"><Avatar name={auth.user.username} identity={String(auth.user.id)} /><span>{auth.user.username}<small>{auth.user.role === 'admin' ? '管理员' : '查看者'}</small></span></div><button className="icon-button" onClick={logout} title="退出登录" aria-label="退出登录"><Icon name="logout" size={18} /></button></div></header><main className="page-content">{logoutError && <ErrorBox>{logoutError}</ErrorBox>}{page === 'group' && groupRef ? <GroupPage key={`${auth.user.id}:${groupRef}`} userId={auth.user.id} groupRef={groupRef} revision={revision} reload={reload} openMessage={setMessageRef} /> : page === 'assistant' ? <ChatPage key={`${auth.user.id}:${assistantGroup || ''}`} userId={auth.user.id} initialGroup={assistantGroup} openMessage={setMessageRef} /> : page === 'usage' ? <UsagePage key={auth.user.id} user={auth.user} revision={revision} reload={reload} /> : page === 'alerts' ? <AlertsPage revision={revision} reload={reload} openMessage={setMessageRef} /> : page === 'settings' ? <SettingsPage revision={revision} reload={reload} user={auth.user} /> : <OverviewPage key={`${auth.user.id}:${page}`} userId={auth.user.id} revision={revision} reload={reload} groupsOnly={page === 'groups'} />}</main><footer className="page-footer"><span>WeKeeper · 群聊工作台</span><span>以归档数据为依据 <span className="footer-dot">·</span> 上海时间 UTC+8</span></footer></div>{messageRef && <MessageDialog key={messageRef} messageRef={messageRef} onClose={closeMessage} openMessage={setMessageRef} />}</div>;
}
