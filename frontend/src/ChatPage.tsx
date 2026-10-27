import { Fragment, useCallback, useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { api, appPath, post } from './api';
import { applyChatEvent, citationParts, mergeTurns, pendingStatus, terminalEvent } from './chat';
import type { ChatEvent, ChatScope, ChatTurn, Conversation } from './chat';
import { DateRange } from './RangePicker';
import { preferenceKey, readSelection, shanghaiDay } from './range';
import type { RangeSelection, ResolvedRange } from './range';
import type { Group } from './types';
import { useRange } from './useRange';
import { useResource } from './useResource';

const stamp = (value: number) => new Date(value * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
const statuses: Record<string, string> = { queued: '等待本机处理', running: '正在分析所选群聊', cancelling: '正在取消', completed: '已完成', failed: '处理失败', interrupted: '任务中断', cancelled: '已取消' };
const chatError = (value?: string | null) => {
  if (!value) return '任务未能完成，请检查本机助手状态后重试。';
  if (value.includes('quota') || value.includes('usage_limit')) return '订阅额度暂时受限。任务已停止，不会切换到付费 API。';
  if (value.includes('auth') || value.includes('login')) return '本机模型授权需要处理，请联系管理员。';
  if (value.includes('timeout')) return '处理超时，已停止本轮任务。可以缩小群聊或时间范围后再试。';
  if (value.includes('interrupt') || value.includes('worker')) return '本机任务中断，已有结果保留。请确认本机服务已恢复。';
  return '任务未能完成，已有结果仅为生成片段，请稍后重试。';
};

function Answer({ turn, openMessage }: { turn: ChatTurn; openMessage: (reference: string) => void }) {
  const references = turn.status === 'completed' ? turn.message_refs : [];
  return <><div className="assistant-answer">{citationParts(turn.response_text || '', references).map((part, index) => part.reference ? <button className="inline-citation" key={index} onClick={() => openMessage(part.reference!)} title="查看归档原文">依据 {references.indexOf(part.reference) + 1}</button> : <Fragment key={index}>{part.text}</Fragment>)}</div>{turn.status === 'completed' && references.length > 0 && <div className="answer-sources"><span>原文依据</span>{references.map((reference, index) => <button key={reference} onClick={() => openMessage(reference)}>消息 {index + 1} ↗</button>)}</div>}{['failed', 'interrupted'].includes(turn.status) && <div className="chat-failure" role="status">{chatError(turn.error)}{turn.response_text && ' 上方内容不是完整回答。'}</div>}{turn.status === 'cancelled' && <p className="chat-partial">本轮已取消。已有片段保留，不作为完整回答。</p>}</>;
}

export default function ChatPage({ userId, initialGroup, openMessage }: { userId: number; initialGroup?: string; openMessage: (reference: string) => void }) {
  const range = useRange(userId, 'assistant');
  const groups = useResource<{ groups: Group[] }>('/api/groups');
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Conversation | null>(null);
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [groupRefs, setGroupRefs] = useState<string[]>(initialGroup ? [initialGroup] : []);
  const [scopeOverride, setScopeOverride] = useState<ChatScope | null>(null);
  const [draft, setDraft] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [streamState, setStreamState] = useState('');
  const [toolState, setToolState] = useState('');
  const [follow, setFollow] = useState(true);
  const initialized = useRef(false);
  const selectedRef = useRef<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const followRef = useRef(true);
  const requestRef = useRef<{ content: string; conversationId: string | null; requestId: string; scope: ChatScope } | null>(null);
  const refreshList = useCallback(async () => {
    const result = await api<{ conversations: Conversation[] }>('/api/chat/conversations');
    setConversations(result.conversations);
    return result.conversations;
  }, []);
  useEffect(() => {
    if (initialGroup) {
      try {
        const inherited = readSelection(preferenceKey(userId, `group:${initialGroup}`, import.meta.env.BASE_URL), window.localStorage);
        if (inherited) range.setSelection(inherited);
      } catch { /* Browser preferences are optional. */ }
    }
    refreshList().then(rows => {
      if (!initialized.current && !initialGroup && rows.length) { selectedRef.current = rows[0].id; setSelectedId(rows[0].id); }
      initialized.current = true;
    }).catch(failure => setError((failure as Error).message)).finally(() => setLoading(false));
    // Initial group and user are fixed by this page's React key.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userId, initialGroup, refreshList]);
  useEffect(() => {
    if (selectedId === null) return;
    let active = true;
    setLoading(true);
    api<{ conversation: Conversation; turns: ChatTurn[] }>(`/api/chat/conversations/${selectedId}`).then(result => {
      if (!active) return;
      setConversation(result.conversation);
      setTurns(previous => mergeTurns(previous.filter(turn => turn.conversation_id === selectedId), result.turns));
      setGroupRefs(result.conversation.scope.group_refs);
      setScopeOverride(result.conversation.scope);
    }).catch(failure => active && setError((failure as Error).message)).finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [selectedId]);
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (document.visibilityState !== 'visible') return;
      refreshList().catch(() => {});
      const id = selectedRef.current;
      if (id !== null) api<{ conversation: Conversation; turns: ChatTurn[] }>(`/api/chat/conversations/${id}`).then(result => {
        if (selectedRef.current === id) { setConversation(result.conversation); setTurns(previous => mergeTurns(previous.filter(turn => turn.conversation_id === id), result.turns)); }
      }).catch(() => {});
    }, 10000);
    return () => window.clearInterval(timer);
  }, [refreshList]);
  const activeTurn = [...turns].reverse().find(turn => pendingStatus(turn.status));
  const activeId = activeTurn?.id;
  useEffect(() => {
    if (!activeTurn) { setStreamState(''); setToolState(''); return; }
    const id = activeTurn.id;
    const source = new EventSource(appPath(`/api/chat/turns/${id}/events?after=${activeTurn.last_seq || 0}`));
    source.onopen = () => setStreamState('任务连接已建立');
    source.onerror = () => setStreamState('任务连接中断，正在恢复；已生成内容保留');
    source.addEventListener('auth_required', () => { source.close(); window.dispatchEvent(new Event('wekeeper:expired')); });
    source.addEventListener('status', message => {
      try {
        const status = JSON.parse((message as MessageEvent).data) as { status: ChatTurn['status']; cancel_requested: boolean; last_seq: number };
        setTurns(previous => previous.map(turn => {
          if (turn.id !== id) return turn;
          // A status notification can precede later event batches; never skip unread text by advancing last_seq here.
          const canApply = pendingStatus(status.status) || status.last_seq <= turn.last_seq;
          return { ...turn, ...(canApply ? { status: status.status } : {}), cancel_requested: status.cancel_requested };
        }));
      } catch { /* A malformed progress notification cannot discard the stream. */ }
    });
    for (const type of ['delta', 'tool', 'heartbeat', 'completed', 'failed', 'interrupted', 'cancelled']) {
      source.addEventListener(type, message => {
        let event: ChatEvent;
        try { event = JSON.parse((message as MessageEvent).data) as ChatEvent; } catch { return; }
        if (!event || typeof event !== 'object') return;
        setTurns(previous => previous.map(turn => turn.id === id ? applyChatEvent(turn, { ...event, type }) : turn));
        if (type === 'tool') setToolState(event.state === 'completed' ? '已取得群聊依据，正在整理回答' : '正在查阅所选范围内的归档');
        if (terminalEvent(type)) { source.close(); refreshList().catch(() => {}); }
      });
    }
    return () => source.close();
    // The stream itself maintains last_seq; reconnects use Last-Event-ID.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeId, refreshList]);
  useEffect(() => {
    if (followRef.current && scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [turns, toolState]);
  function select(row: Conversation | null) {
    selectedRef.current = row?.id || null;
    setSelectedId(row?.id || null); setConversation(row); setTurns([]); setError(''); setToolState('');
    setScopeOverride(row?.scope || null); setGroupRefs(row?.scope.group_refs || (initialGroup ? [initialGroup] : []));
    setDraft(''); requestRef.current = null; followRef.current = true; setFollow(true);
  }
  const resolved: ResolvedRange = scopeOverride ? { ...scopeOverride, from: shanghaiDay(scopeOverride.since), to: shanghaiDay(scopeOverride.until) } : range.resolved;
  const pickerSelection: RangeSelection = scopeOverride ? { mode: 'custom', from: resolved.from, to: resolved.to } : range.selection;
  const otherActive = conversations.find(row => row.id !== selectedId && row.last_turn_status && pendingStatus(row.last_turn_status));
  function changeRange(selection: RangeSelection) { setScopeOverride(null); range.setSelection(selection); requestRef.current = null; }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!draft.trim() || !groupRefs.length || activeTurn || otherActive || sending) return;
    setSending(true); setError('');
    let id = selectedId;
    const pending = requestRef.current;
    const request = pending && pending.content === draft.trim() && pending.conversationId === id ? pending : {
      content: draft.trim(), conversationId: id, requestId: crypto.randomUUID(), scope: { group_refs: [...groupRefs], since: resolved.since, until: resolved.until },
    };
    requestRef.current = request;
    try {
      if (id === null) {
        const created = await post<{ conversation: Conversation }>('/api/chat/conversations', { title: request.content.slice(0, 40), ...request.scope });
        id = created.conversation.id; request.conversationId = id;
        selectedRef.current = id; setSelectedId(id); setConversation(created.conversation);
      }
      const result = await post<{ turn: ChatTurn }>(`/api/chat/conversations/${id}/turns`, { content: request.content, ...request.scope, client_request_id: request.requestId });
      setTurns(previous => mergeTurns(previous.filter(turn => turn.conversation_id === id), [result.turn]));
      setDraft(''); requestRef.current = null; setScopeOverride(result.turn.scope);
      followRef.current = true; setFollow(true); refreshList().catch(() => {});
    } catch (failure) { setError((failure as Error).message); refreshList().catch(() => {}); } finally { setSending(false); }
  }
  async function cancel() {
    if (!activeTurn) return;
    try { const result = await post<{ turn: ChatTurn }>(`/api/chat/turns/${activeTurn.id}/cancel`); setTurns(previous => mergeTurns(previous, [result.turn])); } catch (failure) { setError((failure as Error).message); }
  }
  const names = new Map((groups.data?.groups || []).map(group => [group.group_ref, group.name]));
  return <><div className="page-heading"><div><p className="eyebrow">WEKEEPER ASSISTANT</p><h1>带着问题，回到讨论现场。</h1><p>选择群聊与时间，助手会检索归档、整理回答，并给出可展开的原文依据。</p></div><span className="pill green">会话仅自己可见</span></div><div className="chat-layout"><aside className="chat-history panel"><div className="chat-history-heading"><h2>我的会话</h2><button className="button secondary small" onClick={() => select(null)}>＋ 新会话</button></div>{loading && !conversations.length ? <p className="chat-list-empty">正在读取会话…</p> : !conversations.length ? <p className="chat-list-empty">你的会话会保存在云端，登录其他设备后仍可继续。</p> : <div className="conversation-list">{conversations.map(row => <button className={row.id === selectedId ? 'active' : ''} key={row.id} onClick={() => select(row)}><strong>{row.title || '未命名会话'}</strong><small>{stamp(row.updated_at)}</small>{row.last_turn_status && pendingStatus(row.last_turn_status) && <span className="chat-live-tag">处理中</span>}</button>)}</div>}</aside><section className="chat-workspace panel"><div className="chat-workspace-heading"><div><h2>{conversation?.title || '新的群聊问答'}</h2><p>只读取已归档的群聊，无法发送微信消息或获取未缓存的内容。</p></div></div><details className="chat-scope" open={!turns.length}><summary><span>查询范围</span><strong>{groupRefs.length ? `已选 ${groupRefs.length} 个群` : '请先选择群聊'}</strong><small>{stamp(resolved.since)} — {stamp(resolved.until)}</small></summary><div className="chat-group-picker">{groups.data?.groups.length ? groups.data.groups.map(group => <label key={group.group_ref}><input type="checkbox" checked={groupRefs.includes(group.group_ref)} disabled={sending} onChange={event => { setGroupRefs(event.target.checked ? [...groupRefs, group.group_ref] : groupRefs.filter(ref => ref !== group.group_ref)); requestRef.current = null; }} /><span>{group.name}</span></label>) : <p>暂无已同步的群聊，请先配置本机同步。</p>}</div><DateRange selection={pickerSelection} resolved={resolved} onChange={changeRange} /><p className="scope-footnote">每轮发送时固定所选群与时间；调整这里的范围只影响下一轮。{scopeOverride && '当前沿用本会话上次保存的范围。'}</p></details>{(error || groups.error) && <div className="notice error chat-error" role="alert">{error || groups.error}</div>}{otherActive && <div className="notice warning chat-error"><span>另一个会话正在处理任务，每个账号同时处理一轮。</span><button className="text-button" onClick={() => select(otherActive)}>查看该会话</button></div>}<div className="chat-transcript" ref={scrollRef} onScroll={() => { const element = scrollRef.current; if (element) { const next = element.scrollHeight - element.scrollTop - element.clientHeight < 110; followRef.current = next; setFollow(next); } }}><div className="chat-transcript-inner">{loading && selectedId !== null && !turns.length ? <div className="chat-empty"><span className="spinner" /><p>正在恢复你的会话…</p></div> : !turns.length ? <div className="chat-empty"><span className="assistant-emblem">W</span><h3>从一个具体的问题开始</h3><p>例如梳理讨论结论、查找某个决定的依据，或汇总明确的待办。<br />先选择群聊；助手不会自动查询所有群。</p></div> : turns.map(turn => <article className="chat-turn" key={turn.id}><div className="chat-question"><span>你</span><p>{turn.content}</p></div><div className="turn-frozen-scope"><span>{turn.scope.group_refs.map(ref => names.get(ref) || '已选群聊').join('、')}</span><time>{stamp(turn.scope.since)} — {stamp(turn.scope.until)}</time></div><div className="chat-answer-head"><span className="assistant-emblem small">W</span><strong>WeKeeper 群聊助手</strong><span className={`turn-status ${turn.status}`}>{statuses[turn.status] || '状态待确认'}</span></div><Answer turn={turn} openMessage={openMessage} />{pendingStatus(turn.status) && <div className="chat-progress" role="status"><span className="typing-dots"><i /><i /><i /></span><span>{toolState || statuses[turn.status]}</span></div>}</article>)}</div></div>{!follow && activeTurn && <button className="jump-latest" onClick={() => { followRef.current = true; setFollow(true); if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight; }}>有新内容，滚动到底部 ↓</button>}<form className="chat-composer" onSubmit={submit}><textarea value={draft} onChange={event => { setDraft(event.target.value); if (requestRef.current?.content !== event.target.value.trim()) requestRef.current = null; }} placeholder={groupRefs.length ? '输入你想了解的问题…' : '先在上方选择要查询的群聊'} aria-label="向群聊助手提问" maxLength={8000} disabled={sending} rows={3} onKeyDown={event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); event.currentTarget.form?.requestSubmit(); } }} /><div className="composer-footer"><span>{activeTurn ? streamState || statuses[activeTurn.status] : 'Ctrl / ⌘ + Enter 发送 · 回答需结合原文核实'}</span>{activeTurn ? <button className="button secondary" type="button" onClick={cancel} disabled={activeTurn.cancel_requested || activeTurn.status === 'cancelling'}>{activeTurn.cancel_requested || activeTurn.status === 'cancelling' ? '正在取消…' : '停止本轮'}</button> : <button className="button primary" type="submit" disabled={sending || !draft.trim() || !groupRefs.length || !!otherActive}>{sending ? '正在提交…' : '发送问题 ↑'}</button>}</div></form></section></div></>;
}
