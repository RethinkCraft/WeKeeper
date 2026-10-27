import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

async function moduleFrom(path) {
  const source = readFileSync(new URL(path, import.meta.url), 'utf8');
  const code = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText;
  return import(`data:text/javascript,${encodeURIComponent(code)}`);
}
const range = await moduleFrom('../src/range.ts');
const chat = await moduleFrom('../src/chat.ts');
const avatar = await moduleFrom('../src/avatar.ts');
const seconds = value => Date.parse(value) / 1000;

test('default range includes seven Shanghai calendar days and ends at now', () => {
  const now = seconds('2026-10-26T16:12:35+08:00');
  assert.deepEqual(range.resolveRange(range.defaultRange, now), { since: seconds('2026-10-20T00:00:00+08:00'), until: now, from: '2026-10-20', to: '2026-10-26' });
});

test('relative calendar ranges move at Shanghai midnight, independent of browser timezone', () => {
  const before = range.resolveRange({ mode: 'last7' }, seconds('2026-10-26T15:59:59Z'));
  const after = range.resolveRange({ mode: 'last7' }, seconds('2026-10-26T16:00:00Z'));
  assert.equal(before.to, '2026-10-26');
  assert.equal(after.to, '2026-10-27');
  assert.equal(after.since - before.since, 86400);
});

test('rolling 24 hours uses actual seconds, custom dates stay fixed across days', () => {
  const now = seconds('2026-10-26T00:15:00+08:00');
  const rolling = range.resolveRange({ mode: 'last24' }, now);
  assert.equal(rolling.until - rolling.since, 86400);
  assert.equal(rolling.from, '2026-10-25');
  const custom = { mode: 'custom', from: '2026-10-20', to: '2026-10-21' };
  assert.deepEqual(range.resolveRange(custom, now), range.resolveRange(custom, now + 2 * 86400));
  assert.equal(range.resolveRange(custom, now).until, seconds('2026-10-21T23:59:59+08:00'));
});

test('preferences isolate users, deployment paths and groups, storing modes rather than resolved dates', () => {
  const items = new Map();
  const storage = { getItem: key => items.get(key) ?? null, setItem: (key, value) => items.set(key, value) };
  const key = range.preferenceKey(1, 'group:one', '/wekeeper/');
  range.saveSelection(key, { mode: 'last7' }, storage);
  assert.deepEqual(range.readSelection(key, storage), { mode: 'last7' });
  assert.equal(items.get(key), '{"mode":"last7"}');
  assert.equal(range.readSelection(range.preferenceKey(2, 'group:one', '/wekeeper/'), storage), null);
  assert.equal(range.readSelection(range.preferenceKey(1, 'group:two', '/wekeeper/'), storage), null);
  assert.equal(range.readSelection(range.preferenceKey(1, 'group:one', '/'), storage), null);
});

test('invalid persisted dates do not become queries and unavailable storage retains in-memory choices', () => {
  assert.equal(range.validSelection({ mode: 'custom', from: '2026-02-30', to: '2026-03-02' }), false);
  assert.equal(range.validSelection({ mode: 'custom', from: '2026-10-27', to: '2026-10-26' }), false);
  assert.equal(range.validSelection({ mode: 'wrong' }), false);
  const storage = { getItem() { throw new Error('disabled'); }, setItem() { throw new Error('disabled'); } };
  range.saveSelection('private-mode', { mode: 'today' }, storage);
  assert.deepEqual(range.readSelection('private-mode', storage), { mode: 'today' });
});

const turn = { id: 'turn_one', conversation_id: 'conversation_one', user_id: 3, content: 'question', status: 'running', scope: { group_refs: ['group_one'], since: 1, until: 2 }, created_at: 1, response_text: '', message_refs: [], last_seq: 0 };
test('stream replay is idempotent and completion replaces partial text and authorizes citations', () => {
  const first = chat.applyChatEvent(turn, { seq: 1, type: 'delta', text: 'partial' });
  assert.equal(chat.applyChatEvent(first, { seq: 1, type: 'delta', text: 'partial' }).response_text, 'partial');
  const completed = chat.applyChatEvent(first, { seq: 2, type: 'completed', text: 'final [[msg_one]]', message_refs: ['msg_one'] });
  assert.equal(completed.response_text, 'final [[msg_one]]');
  assert.equal(completed.status, 'completed');
  assert.deepEqual(completed.scope, turn.scope);
  assert.deepEqual(completed.message_refs, ['msg_one']);
});

test('failed streams preserve partial output without treating it as a completed answer', () => {
  const partial = chat.applyChatEvent(turn, { seq: 1, type: 'delta', text: 'partial' });
  const failed = chat.applyChatEvent(partial, { seq: 2, type: 'failed', error: 'quota_exceeded' });
  assert.equal(failed.response_text, 'partial');
  assert.equal(failed.status, 'failed');
  assert.equal(chat.pendingStatus(failed.status), false);
});

test('polling an older snapshot cannot overwrite newer streamed data', () => {
  const fresh = chat.applyChatEvent(turn, { seq: 2, type: 'delta', text: 'new' });
  assert.equal(chat.mergeTurns([fresh], [{ ...turn, response_text: 'old', last_seq: 1 }])[0].response_text, 'new');
});

test('only explicit allowed message references become citation links; HTML remains plain text', () => {
  const parts = chat.citationParts('<img src=x> [[msg_allowed]] [[msg_unknown]]', ['msg_allowed']);
  assert.equal(parts.filter(part => part.reference).length, 1);
  assert.equal(parts.find(part => part.reference).reference, 'msg_allowed');
  assert.equal(parts.map(part => part.text).join(''), '<img src=x> [[msg_allowed]] [[msg_unknown]]');
});

test('cached avatars accept the deployed API prefix but reject external or non-API images', () => {
  const origin = 'https://example.test';
  const image = '/wekeeper/api/groups/group_one/senders/sender_one/avatar?hash=abc';
  assert.equal(avatar.avatarSource(image, origin, '/wekeeper/api/'), origin + image);
  assert.equal(avatar.avatarSource('https://other.test' + image, origin, '/wekeeper/api/'), '');
  assert.equal(avatar.avatarSource('/wekeeper/private/photo.jpg', origin, '/wekeeper/api/'), '');
  assert.equal(avatar.avatarSource('', origin, '/wekeeper/api/'), '');
});

test('automatic avatar retries are bounded while an explicit refresh can recover immediately', () => {
  assert.equal(avatar.canRetryAvatar(null, 100000), false);
  assert.equal(avatar.canRetryAvatar(1000, 1001), false);
  assert.equal(avatar.canRetryAvatar(1000, 30999), false);
  assert.equal(avatar.canRetryAvatar(1000, 31000), true);
  assert.equal(avatar.canRetryAvatar(1000, 1001, true), true);
  assert.equal(avatar.canRetryAvatar(31000, 31001), false);
});

test('empty and whitespace-only member names have visible fallback text', () => {
  assert.equal(avatar.displayMemberName(' \t\u3000 '), '未命名成员');
  assert.equal(avatar.displayMemberName(''), '未命名成员');
  assert.equal(avatar.displayMemberName(null), '未命名成员');
  assert.equal(avatar.displayMemberName('  群成员  '), '群成员');
});
