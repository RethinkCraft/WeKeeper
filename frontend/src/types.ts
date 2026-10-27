export type User = { id: number; username: string; role: 'admin' | 'viewer'; disabled?: boolean };
export type Auth = { user: User; csrf_token: string };
export type Group = { group_ref: string; name: string; avatar_url?: string | null; messages?: number; active_senders?: number; last_message_at?: number | null };
export type Source = {
  source_id: string; received_at: number; stale: boolean; last_message_at: number | null;
  health: { state?: string; reason?: string };
  collector: { running?: boolean; last_cycle_at?: number | null; last_error?: string | null };
  summary: { state?: string; last_success_at?: number | null; error?: string | null };
  sync?: { state?: string; last_success_at?: number | null; error?: string | null; pending_partitions?: number };
};
export type Trend = { day: string; messages: number };
export type Alert = {
  id: number; kind: string; severity: string; title: string; body: string; group_ref: string | null;
  message_ref: string | null; opened_at: number; resolved_at: number | null;
  read_at: number | null; acknowledged_at: number | null;
};
export type Overview = {
  revision: number; totals: { messages: number; active_senders: number; groups: number; open_alerts: number };
  groups: Group[]; sources: Source[]; trend: Trend[]; alerts: Alert[];
};
export type Message = {
  message_ref: string; create_time: number; sender_id: string; sender_name: string; avatar_url?: string | null;
  msg_type: number; text: string; reply?: { target_ref?: string | null; text?: string } | null;
};
export type GroupStats = {
  group_ref: string; name: string; avatar_url?: string | null; messages: number; active_senders: number; trend: Trend[];
  types: { msg_type: number; messages: number }[];
  senders: { sender_id: string; sender_name: string; messages: number; avatar_url?: string | null }[];
  coverage: Record<string, unknown>;
  source?: Source | null;
};
export type SummaryItem = { text: string; message_refs: string[] };
export type Summary = {
  day: string; model: string; generated_at: number; covered_until: number; stale: boolean;
  summary: { topics: SummaryItem[]; decisions: SummaryItem[]; todos: SummaryItem[] };
  processing?: { mode: 'legacy' | 'incremental'; checkpoint_seq: number; baseline_complete: boolean; processed_messages: number; pending_events: number };
};
export type Rule = { id: number; name: string; group_ref: string | null; keyword: string; enabled: boolean };
