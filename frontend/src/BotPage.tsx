import { useEffect, useState } from 'react';
import { post } from './api';
import { useResource } from './useResource';
import { botLatencyRows, botRequestStateName, botStateName, botStatusFor, formatBotDuration, importPersonaMarkdown, MAX_PERSONA_MARKDOWN, personaDraftError, shanghaiDay } from './bot';
import type { BotConfig, BotOverview, BotProvider, BotState } from './bot';
import type { User } from './types';

type Member = { sender_id: string; sender_name: string; group_ref: string };
const stamp = (value?: number | null) => value ? new Date(value * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '尚无记录';
const id = (prefix: string) => `${prefix}_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
const runtimeName = (state?: string) => ({ running: '运行中', stopped: '已停止', failed: '运行失败', ready: '就绪', disabled: '已停用', starting: '启动中', error: '运行异常', paused: '已暂停', attention: '需要留意', login_required: '需要手机确认登录' }[state || ''] || '待确认');
const staleKindName = (kind?: string) => ({ expired: '过期未处理', suppressed_stale: '过期已入账未回复' }[kind || ''] || kind || '过期');
const commandName = (kind: string) => kind === 'apply_config' ? '配置更新' : '状态调整';
const commandState = (state: string) => ({ pending: '待本机应用', applied: '已生效', failed: '失败' }[state] || state);
const failureName = (error?: string | null) => ({ bot_group_not_in_protocol_bindings: '此群尚未加入本机协议绑定，请先完成本机绑定。', bot_group_not_in_dashboard_allowlist: '此群尚未加入消息同步范围。', bot_cloud_bootstrap_required: '尚未连接本机管理，请先完成配置对齐。', bot_status_revision_conflict: '状态已被其他报备更新，请刷新后重试。', offer_version_conflict: '状态已被其他报备更新，请刷新后重试。', status_revision_conflict: '状态已被其他报备更新，请刷新后重试。', admin_authorization_revoked: '操作账号已失去管理权限。', bot_native_not_ready: '本机发送能力尚未通过验收。', siwc_network_error: '连接模型或授权服务失败。', siwc_auth_network_error: '连接登录授权服务失败。' }[error || ''] || error || '');
const count = (value?: number | null) => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value.toLocaleString('zh-CN') : '—';
const reasoningName = (value?: string | null) => value ? ({ none: '无', minimal: '极低', low: '低', medium: '中', high: '高', xhigh: '极高', max: '最高' }[value] || value) : '未返回';

function RuntimeInfo({ data }: { data: BotOverview }) {
  const active = data.runtime.active;
  const request = data.runtime.last_request;
  const counters = data.runtime.counts;
  const rows = botLatencyRows(data.runtime);
  const persona = data.config.data.personas.find(value => value.id === active?.persona_id && String(value.version) === String(active?.persona_version));
  const provider = data.config.data.providers.find(value => value.id === active?.profile_id);
  return <section className="panel bot-runtime"><div className="panel-title"><div><h2>本机运行信息</h2><p>模型、角色版本和耗时由本机报告；未收到的数据保留为空。</p></div></div>
    <dl className="bot-runtime-config"><div><dt>本机请求模型</dt><dd>{active?.model || '尚无本机报告'}<small>{provider?.name || active?.provider || '提供方未返回'}</small></dd></div><div><dt>思考档位</dt><dd>{reasoningName(active?.reasoning_effort)}</dd></div><div><dt>生效角色</dt><dd>{active?.persona_name || persona?.name || active?.persona_id || '尚无本机报告'}<small>{active?.persona_version != null ? `版本 ${active.persona_version}` : '版本未返回'}</small></dd></div><div><dt>运行群聊</dt><dd>{data.groups.find(group => group.group_ref === active?.group_ref)?.name || '尚无本机报告'}</dd></div></dl>
    <div className="bot-runtime-counts">{([['requests', '本机累计请求'], ['replied', '网关已接受'], ['pending', '待处理'], ['failed', '失败'], ['unknown', '发送待确认']] as const).map(([key, label]) => <div key={key}><span>{label}</span><strong>{count(counters?.[key])}</strong></div>)}</div>
    <div className="bot-latest-request"><strong>最近请求：{botRequestStateName(request?.status)}</strong><span>开始 {stamp(request?.started_at)} · 结束 {stamp(request?.finished_at)}</span>{request?.error && <p>{failureName(request.error)}</p>}</div>
    <div className="table-wrap"><table className="bot-latency-table"><thead><tr><th>阶段</th><th>最近一次</th><th title="一半样本的耗时不超过此值">P50</th><th title="95% 样本的耗时不超过此值">P95</th><th>样本数</th></tr></thead><tbody>{rows.map(row => <tr key={row.phase}><td>{row.label}</td><td>{formatBotDuration(row.latest)}</td><td>{formatBotDuration(row.p50)}</td><td>{formatBotDuration(row.p95)}</td><td>{count(row.count)}</td></tr>)}</tbody></table></div>
    <p className="bot-metrics-note">分位数按每阶段最近最多 500 条有效记录计算。正常回复的生成与端到端统计不含失败回执，失败请求单独列示。“生成与工具处理”包含工具启动、等待和模型请求。各阶段可能重叠，不相加；发送未确认时不补造完成耗时。</p>
  </section>;
}

export default function BotPage({ user, revision, reload, openMessage }: { user: User; revision: number; reload: () => void; openMessage: (reference: string) => void }) {
  const [source, setSource] = useState('');
  const [tab, setTab] = useState('status');
  const [draft, setDraft] = useState<BotConfig | null>(null);
  const [draftRevision, setDraftRevision] = useState(-1);
  const [draftSource, setDraftSource] = useState('');
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [memberQuery, setMemberQuery] = useState('');
  const [day, setDay] = useState(shanghaiDay());
  const [followToday, setFollowToday] = useState(true);
  const [now, setNow] = useState(() => Date.now() / 1000);
  const admin = user.role === 'admin';
  const path = `/api/bot${source ? `?source_id=${encodeURIComponent(source)}` : ''}`;
  const result = useResource<BotOverview>(admin ? path : null, revision);
  const data = result.loadedPath === path ? result.data : undefined;
  const currentSource = data?.source_id || '';
  const members = useResource<{ members: Member[]; has_more: boolean }>(admin && currentSource ? `/api/bot/members?source_id=${encodeURIComponent(currentSource)}&q=${encodeURIComponent(memberQuery)}` : null, revision);
  useEffect(() => {
    if (data && (!dirty || draftSource !== currentSource) && (draftRevision < data.config.revision || draftSource !== currentSource)) {
      setDraft(structuredClone(data.config.data)); setDraftRevision(data.config.revision); setDraftSource(currentSource); setDirty(false);
    }
  }, [data, dirty, draftRevision, draftSource, currentSource]);
  useEffect(() => {
    if (!followToday) return;
    const timer = window.setInterval(() => setDay(shanghaiDay()), 30000);
    return () => window.clearInterval(timer);
  }, [followToday]);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now() / 1000), 5000);
    return () => window.clearInterval(timer);
  }, []);

  if (!admin) return <section className="panel"><div className="panel-title"><h2>机器人管理需要管理员权限</h2></div></section>;
  function change(edit: (config: BotConfig) => void) {
    setDraft(previous => { if (!previous) return previous; const next = structuredClone(previous); edit(next); return next; });
    setDirty(true); setError(''); setNotice('');
  }
  async function save(value = draft, expected = draftRevision) {
    if (!value || !currentSource) return;
    const invalid = value.personas.map(personaDraftError).find(Boolean);
    if (invalid) { setError(invalid); setNotice(''); return; }
    setBusy(true); setError(''); setNotice('');
    try {
      const response = await post<{ revision: number; command: { payload: { config: BotConfig } } }>('/api/bot/config', { source_id: currentSource, expected_revision: expected, data: value });
      setDraft(structuredClone(response.command.payload.config)); setDraftRevision(response.revision); setDirty(false);
      setNotice('已提交，等待本机应用。生效结果会自动更新。'); reload();
    } catch (failure) { setError((failure as Error).message); } finally { setBusy(false); }
  }
  async function importMarkdown(personaId: string, file?: File) {
    if (!file) return;
    setBusy(true); setError(''); setNotice('');
    try {
      if (file.size > MAX_PERSONA_MARKDOWN * 4 + 3) throw new Error('文件过大，请缩短 Markdown 内容后再导入。');
      const markdown = importPersonaMarkdown(file.name, await file.text());
      change(value => { const persona = value.personas.find(role => role.id === personaId); if (persona) persona.markdown = markdown; });
      setNotice('Markdown 已填入编辑器；点击“保存并应用”后才会提交。');
    } catch (failure) { setError((failure as Error).message); } finally { setBusy(false); }
  }
  async function adjust(businessId: string, itemId: string, status: BotState) {
    if (!data || !currentSource) return;
    const state = botStatusFor(data.statuses, businessId, itemId, day);
    setBusy(true); setError(''); setNotice('');
    try {
      await post('/api/bot/status', { source_id: currentSource, business_id: businessId, item_id: itemId, day, status, expected_revision: state.revision });
      setNotice('状态调整已排队，等待本机检查版本并应用。'); reload();
    } catch (failure) { setError((failure as Error).message); } finally { setBusy(false); }
  }
  const config = draft;
  const effectiveConfig = data?.config.revision === data?.config.applied_revision;
  const configCommand = data?.commands.find(command => command.kind === 'apply_config' && command.revision === data.config.revision);
  const configApplyError = !effectiveConfig && configCommand?.status === 'failed' ? failureName(configCommand.error) || '本机未能应用配置。' : '';
  const visibleNotice = notice.startsWith('已提交，等待本机应用') ? configApplyError ? '' : effectiveConfig ? '当前配置已在本机生效。' : notice : notice;
  const stale = !data?.received_at || now - data.received_at > 30;
  const memberName = (ref: string) => members.data?.members.find(member => member.sender_id === ref)?.sender_name || '已选成员';
  const heading = <div className="page-heading"><div><p className="eyebrow">GROUP BOT</p><h1>群里的小帮手，认真记住每次报备。</h1><p>真实 @ 才响应，回复引用原消息。首单按店铺共享，人设和模型按群选择。</p></div>{data?.sources.length ? <label className="bot-source">采集机器<select value={source || currentSource} disabled={busy} onChange={event => { setSource(event.target.value); setDraft(null); setDraftRevision(-1); setDirty(false); }} >{data.sources.map(s => <option value={s} key={s}>{s}</option>)}</select></label> : null}</div>;
  if (!data || !config || !currentSource) return <>{heading}{result.error ? <div className="notice error" role="alert">{result.error}<button onClick={reload}>重试</button></div> : <section className="panel bot-empty">{result.loading ? '正在读取机器人状态…' : '还没有采集机器。先连接采集与同步服务，机器人仍保持关闭。'}</section>}</>;
  const loginRequired = Boolean(data.runtime.login_required || data.runtime.state === 'login_required' || data.runtime.error === 'auth_required');
  return <>{heading}<section className="panel bot-health"><div><strong>{loginRequired ? '需要手机确认登录' : stale ? '本机状态待确认' : data.runtime.enabled ? '机器人运行中' : '机器人已停用'}</strong><p>最近同步 {stamp(data.received_at)} · {runtimeName(data.runtime.state)} · 当前配置 {data.config.applied_revision} / {data.config.revision} {effectiveConfig ? '已生效' : configApplyError ? '应用失败' : '待应用'}{data.runtime.queue_depth ? ` · 排队 ${data.runtime.queue_depth}` : ''}{data.runtime.authenticated === false ? ' · 网关未认证' : data.runtime.authenticated ? ' · 网关已认证' : ''}{data.runtime.last_receive_at ? ` · 最近收件 ${stamp(data.runtime.last_receive_at)}` : ''}{data.runtime.last_send_at ? ` · 最近发件 ${stamp(data.runtime.last_send_at)}` : ''}</p></div><button className="button secondary" disabled={busy || !currentSource} onClick={() => save({ ...data.config.data, enabled: false }, data.config.revision)}>停用机器人</button></section>
    {loginRequired && <div className="notice error" role="alert"><span>微信会话已掉线或未登录。请在机器人账号的手机上确认一次登录；确认前机器人不会正常收发。</span></div>}
    {(!data.capabilities.native_ready || stale) && !loginRequired && <div className="notice warning"><span>{stale ? '当前显示本机最后一次同步的记录，新的控制操作会等待机器恢复。' : '协议网关尚未报告可用状态，请检查本机服务与手机登录确认。'}</span></div>}
    {data.runtime.error && data.runtime.error !== 'auth_required' && <div className="notice error" role="status">机器人运行异常：{failureName(data.runtime.error)}</div>}
    {!!data.runtime.stale_events?.length && <div className="notice warning" role="status"><span>最近过期消息：{data.runtime.stale_events.slice(0, 8).map(entry => `${staleKindName(entry.kind)}${entry.age_seconds != null ? ` ${Math.round(entry.age_seconds / 60)} 分钟前` : ''}`).join('；')}</span></div>}
    {(error || result.error || configApplyError) && <div className="notice error" role="alert">{error || result.error || configApplyError}<button className="text-button" onClick={reload}>刷新</button></div>}{visibleNotice && <div className="notice success" role="status">{visibleNotice}</div>}
    <div className="tabs settings-tabs bot-tabs">{[['status', '首单与记录'], ['business', '店铺与报备人员'], ['groups', '群绑定'], ['personas', '角色设定'], ['providers', '模型配置']].map(([key, label]) => <button key={key} className={tab === key ? 'active' : ''} onClick={() => setTab(key)}>{label}</button>)}</div>
    {tab !== 'status' && <div className="bot-save-bar"><label className="bot-check"><input type="checkbox" checked={config.enabled} onChange={event => change(c => { c.enabled = event.target.checked; })} />启用群机器人</label><span>{dirty ? '有尚未提交的修改' : '设置将在本机应用后生效'}</span><button className="button primary" disabled={busy || !dirty} onClick={() => save()}>{tab === 'personas' ? '保存并应用' : '保存设置'}</button></div>}
    {tab === 'status' && <><RuntimeInfo data={data} /><section className="panel"><div className="panel-title"><div><h2>首单状态</h2><p>上海自然日，未报备不代表可用；调整完成后由本机回传结果。</p></div><input aria-label="状态日期" type="date" value={day} onChange={event => { setDay(event.target.value); setFollowToday(event.target.value === shanghaiDay()); }} /></div>{data.config.data.businesses.length ? data.config.data.businesses.map(business => <div className="bot-status-business" key={business.id}><h3>{business.name}</h3>{business.items.map(item => { const state = botStatusFor(data.statuses, business.id, item.id, day); return <div className="bot-status-row" key={item.id}><div><strong>{item.name}</strong><small>{state.updated_at ? `更新 ${stamp(state.updated_at)}` : '这一天尚无报备'} · 版本 {state.revision}</small></div><span className={`pill ${state.status === 'available' ? 'green' : state.status === 'unavailable' ? 'warning' : 'neutral'}`}>{botStateName(state.status)}</span><div className="bot-state-actions">{(['available', 'unavailable', 'unknown'] as BotState[]).map(status => <button key={status} className="button secondary small" disabled={busy || !effectiveConfig || state.status === status || !day} onClick={() => adjust(business.id, item.id, status)}>{status === 'unknown' ? '重置为未报备' : `设为${botStateName(status)}`}</button>)}</div>{state.message_ref && <button className="text-button" onClick={() => openMessage(state.message_ref!)}>原始报备</button>}</div>; })}</div>) : <div className="bot-empty">先在“店铺与报备人员”添加店铺和优惠项目。</div>}</section>
      <section className="panel"><div className="panel-title"><div><h2>控制操作</h2><p>显示待应用、真实生效及失败结果。</p></div></div><div className="bot-records">{data.commands.length ? data.commands.slice(0, 20).map(command => <div className="bot-record" key={command.id}><strong>{commandName(command.kind)} {command.revision ? `v${command.revision}` : ''}</strong><span className={`pill ${command.status === 'applied' ? 'green' : 'warning'}`}>{commandState(command.status)}</span><small>{stamp(command.created_at)}</small>{command.error && <p role="status">{failureName(command.error)}</p>}</div>) : <p className="bot-empty">暂无控制操作。</p>}</div></section>
      <section className="panel"><div className="panel-title"><div><h2>报备审计</h2><p>保留每次变更与原消息。撤回不自动倒推业务状态。</p></div></div><div className="bot-records">{data.audit.length ? data.audit.map(row => <div className="bot-record" key={row.id}><strong>{data.config.data.businesses.find(b => b.id === row.business_id)?.name || row.business_id} · {data.config.data.businesses.find(b => b.id === row.business_id)?.items.find(i => i.id === row.item_id)?.name || row.item_id}</strong><span>{row.previous_status ? `${botStateName(row.previous_status)} → ` : ''}{botStateName(row.status)}</span><small>{row.day} · {stamp(row.created_at)} · {row.actor?.startsWith('dashboard:') ? '看板管理员' : memberName(row.actor || '')}</small>{row.message_ref && <button className="text-button" onClick={() => openMessage(row.message_ref!)}>查看原文</button>}</div>) : <p className="bot-empty">尚无状态变更。</p>}</div></section></>}
    {tab === 'business' && <><div className="bot-section-action"><p>跨群共享同一店铺的今日首单。报备权限绑定真实成员身份，由本机再次校验。</p><button className="button secondary" onClick={() => change(c => { c.businesses.push({ id: id('business'), name: '新店铺', reporter_ids: [], items: [{ id: id('item'), name: '娱乐首单', aliases: ['娱乐'] }] }); })}>添加店铺</button></div><label className="bot-member-search">搜索已观察成员<input value={memberQuery} maxLength={100} placeholder="输入昵称查找报备人员" onChange={event => setMemberQuery(event.target.value)} /></label>{config.businesses.map((business, bi) => <section className="panel bot-editor" key={business.id}><div className="bot-editor-heading"><label>店铺名称<input maxLength={100} required value={business.name} onChange={event => change(c => { c.businesses[bi].name = event.target.value; })} /></label><button className="text-button danger" onClick={() => change(c => { c.businesses.splice(bi, 1); c.groups = c.groups.filter(g => g.business_id !== business.id); })}>删除店铺及群绑定</button></div><h3>优惠项目</h3>{business.items.map((item, ii) => <div className="bot-item-editor" key={item.id}><label>项目名称<input maxLength={100} value={item.name} onChange={event => change(c => { c.businesses[bi].items[ii].name = event.target.value; })} /></label><label>查询别名（逗号分隔）<input maxLength={2000} value={item.aliases.join('，')} onChange={event => change(c => { c.businesses[bi].items[ii].aliases = event.target.value.split(/[,，]/).map(v => v.trim()).filter(Boolean); })} /></label><button className="text-button danger" onClick={() => change(c => { c.businesses[bi].items.splice(ii, 1); })}>删除项目</button></div>)}<button className="text-button" onClick={() => change(c => { c.businesses[bi].items.push({ id: id('item'), name: '新首单', aliases: [] }); })}>添加项目</button><h3>可以报备的成员</h3><div className="bot-selected-members">{business.reporter_ids.map(ref => <button className="button secondary small" key={ref} title={ref} onClick={() => change(c => { c.businesses[bi].reporter_ids = c.businesses[bi].reporter_ids.filter(r => r !== ref); })}>{memberName(ref)} ×</button>)}</div>{members.error && <p className="notice error">{members.error}</p>}<div className="bot-member-grid">{members.data?.members.map(member => <label className="bot-check" key={member.sender_id}><input type="checkbox" checked={business.reporter_ids.includes(member.sender_id)} onChange={event => change(c => { c.businesses[bi].reporter_ids = event.target.checked ? [...c.businesses[bi].reporter_ids, member.sender_id] : c.businesses[bi].reporter_ids.filter(r => r !== member.sender_id); })} /><span>{member.sender_name || '未命名成员'}<small>{data.groups.find(g => g.group_ref === member.group_ref)?.name || '已同步群'} · {member.sender_id.slice(-8)}</small></span></label>)}</div>{members.data?.has_more && <p className="muted">还有更多成员，请输入昵称缩小范围。</p>}{!business.reporter_ids.length && <p className="muted">未选择报备人员，群成员只能查询。</p>}</section>)}</>}
    {tab === 'groups' && <section className="panel bot-editor"><h2>群聊绑定</h2><p className="muted">每群绑定一个店铺、一套角色和模型。需要店铺、角色、模型都已建立。</p>{data.groups.map(group => { const index = config.groups.findIndex(g => g.group_ref === group.group_ref); const binding = config.groups[index]; return <div className="bot-group-editor" key={group.group_ref}><strong>{group.name}</strong>{binding ? <><label>店铺<select value={binding.business_id} onChange={event => change(c => { c.groups[index].business_id = event.target.value; })}>{config.businesses.map(b => <option key={b.id} value={b.id}>{b.name}</option>)}</select></label><label>角色<select value={binding.persona_id} onChange={event => change(c => { c.groups[index].persona_id = event.target.value; })}>{config.personas.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}</select></label><label>模型<select value={binding.provider_id} onChange={event => change(c => { c.groups[index].provider_id = event.target.value; })}>{config.providers.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}</select></label><label className="bot-check"><input type="checkbox" checked={binding.enabled} onChange={event => change(c => { c.groups[index].enabled = event.target.checked; })} />启用此群</label><button className="text-button danger" onClick={() => change(c => { c.groups.splice(index, 1); })}>移除绑定</button></> : <button className="button secondary small" disabled={!config.businesses.length || !config.personas.length || !config.providers.length} onClick={() => change(c => { c.groups.push({ group_ref: group.group_ref, business_id: c.businesses[0].id, persona_id: c.personas[0].id, provider_id: c.providers[0].id, enabled: false }); })}>配置此群</button>}</div>; })}{!data.groups.length && <p className="bot-empty">尚无已同步群聊。</p>}</section>}
    {tab === 'personas' && <><div className="bot-section-action"><p>名称和一篇 Markdown 就够了。把性格、说话习惯和对话示例写在同一篇里。</p><button className="button secondary" disabled={busy} onClick={() => change(c => { c.personas.push({ ...structuredClone(data.defaults.persona), id: id('persona') }); })}>添加角色</button></div>{config.personas.map((persona, pi) => <section className="panel bot-editor bot-persona-editor" key={persona.id}><div className="bot-editor-heading"><label>角色名称<input value={persona.name} maxLength={100} onChange={event => change(c => { c.personas[pi].name = event.target.value; })} /></label><span className="pill neutral">版本 {persona.version}</span><button className="text-button danger" disabled={busy || config.groups.some(g => g.persona_id === persona.id)} onClick={() => change(c => { c.personas.splice(pi, 1); })}>删除未绑定角色</button></div><div className="bot-markdown-toolbar"><label className={`button secondary small bot-file-button ${busy ? 'disabled' : ''}`}>导入 .md<input type="file" accept=".md,text/markdown,text/plain" disabled={busy} onChange={event => { const file = event.target.files?.[0]; event.target.value = ''; void importMarkdown(persona.id, file); }} /></label><small>{persona.markdown.length.toLocaleString('zh-CN')} / {MAX_PERSONA_MARKDOWN.toLocaleString('zh-CN')} 字符 · 导入仅填入草稿</small></div><label>角色 Markdown<textarea className="bot-markdown-input" spellCheck={false} maxLength={MAX_PERSONA_MARKDOWN} value={persona.markdown} rows={20} placeholder={'## 角色设定\n描述她是谁、怎么说话。\n\n## 对话示例\n群友：你好\n角色：来啦。'} onChange={event => change(c => { c.personas[pi].markdown = event.target.value; })} /></label><p className="muted">保存后等待本机应用。修改名称或正文会生成新版本，新版本从新的对话历史开始。</p></section>)}</>}
    {tab === 'providers' && <><div className="bot-section-action"><p>GPT 复用本机 ChatGPT 授权；其他供应商使用本机凭据引用。云端不保存密钥。</p><button className="button secondary" onClick={() => change(c => { c.providers.push({ ...structuredClone(data.defaults.provider), id: id('provider') }); })}>添加模型配置</button></div>{config.providers.map((provider, pi) => { const state = data.providers.find(p => p.id === provider.id); return <section className="panel bot-editor" key={provider.id}><div className="bot-editor-heading"><h2>{provider.name || '模型配置'}</h2><span className={`pill ${state?.configured ? 'green' : 'warning'}`}>{state?.configured ? '本机已配置' : '待本机确认'}</span><button className="text-button danger" disabled={config.groups.some(g => g.provider_id === provider.id)} onClick={() => change(c => { c.providers.splice(pi, 1); })}>删除未绑定配置</button></div><div className="bot-form-grid"><label>配置名称<input maxLength={100} value={provider.name} onChange={event => change(c => { c.providers[pi].name = event.target.value; })} /></label><label>提供方<select value={provider.kind} onChange={event => change(c => { c.providers[pi].kind = event.target.value as BotProvider['kind']; c.providers[pi].base_url = null; c.providers[pi].credential_ref = null; })}><option value="siwc">ChatGPT（现有独立授权）</option><option value="deepseek">DeepSeek API</option><option value="compatible">自定义兼容 API</option></select></label><label>模型名称<input maxLength={200} value={provider.model} placeholder={provider.kind === 'siwc' ? '留空使用本机已选模型' : '填写供应商模型 ID'} onChange={event => change(c => { c.providers[pi].model = event.target.value; })} /></label><label>当前思考档位<input readOnly value={reasoningName(provider.reasoning_effort)} /><small>以本机实际请求报告为准。</small></label>{provider.kind !== 'siwc' && <label>本机凭据名称<input value={provider.credential_ref || ''} maxLength={200} pattern="[A-Za-z0-9_-]+" placeholder="填写本机已保存的凭据名称" onChange={event => change(c => { c.providers[pi].credential_ref = event.target.value || null; })} /></label>}{provider.kind === 'compatible' && <label>API 地址<input type="url" maxLength={1000} placeholder="https://example.com/v1" value={provider.base_url || ''} onChange={event => change(c => { c.providers[pi].base_url = event.target.value || null; })} /></label>}</div>{state?.error && <p className="notice warning">{failureName(state.error)}</p>}</section>; })}</>}
  </>;
}
