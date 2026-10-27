export type ChatScope = { group_refs: string[]; since: number; until: number };
export type ChatStatus = 'queued' | 'running' | 'cancelling' | 'completed' | 'failed' | 'interrupted' | 'cancelled';
export type Conversation = { id: string; title: string; scope: ChatScope; created_at: number; updated_at: number; last_turn_status?: ChatStatus | null };
export type ChatTurn = {
  id: string; conversation_id: string; user_id: number; content: string; status: ChatStatus; scope: ChatScope;
  created_at: number; started_at?: number | null; completed_at?: number | null; response_text: string;
  message_refs: string[]; last_seq: number; cancel_requested?: boolean; error?: string | null;
};
export type ChatEvent = { seq: number; type: string; text?: string; name?: string; state?: string; message_refs?: string[]; error?: string };
export const pendingStatus = (status: string) => ['queued', 'running', 'cancelling'].includes(status);
export const terminalEvent = (type: string) => ['completed', 'failed', 'interrupted', 'cancelled'].includes(type);
export function applyChatEvent(turn: ChatTurn, event: ChatEvent): ChatTurn {
  if (!Number.isInteger(event.seq) || event.seq <= turn.last_seq) return turn;
  const next = { ...turn, last_seq: event.seq };
  if (event.type === 'delta') { next.response_text += event.text || ''; if (next.status === 'queued') next.status = 'running'; }
  if (event.type === 'completed') { next.status = 'completed'; next.response_text = event.text ?? next.response_text; next.message_refs = event.message_refs || []; }
  if (['failed', 'interrupted', 'cancelled'].includes(event.type)) { next.status = event.type as ChatStatus; next.error = event.error || null; }
  return next;
}
export function mergeTurns(current: ChatTurn[], incoming: ChatTurn[]): ChatTurn[] {
  const map = new Map(current.map(turn => [turn.id, turn]));
  for (const turn of incoming) { const previous = map.get(turn.id); if (!previous || turn.last_seq >= previous.last_seq) map.set(turn.id, turn); }
  return [...map.values()].sort((a, b) => a.created_at - b.created_at || String(a.id).localeCompare(String(b.id)));
}
export function citationParts(text: string, allowed: string[]): { text: string; reference?: string }[] {
  const refs = new Set(allowed);
  return text.split(/(\[\[msg_[A-Za-z0-9_-]+\]\])/g).filter(Boolean).map(part => {
    const reference = part.startsWith('[[') && part.endsWith(']]') ? part.slice(2, -2) : '';
    return reference && refs.has(reference) ? { text: part, reference } : { text: part };
  });
}
