import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

const source = readFileSync(new URL('../src/bot.ts', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText;
const bot = await import(`data:text/javascript,${encodeURIComponent(code)}`);

test('new Shanghai day is unreported without copying yesterday or another shop state', () => {
  const rows = [{ business_id: 'shop_a', item_id: 'first', day: '2026-10-27', status: 'available', revision: 2 }];
  assert.equal(bot.shanghaiDay(new Date('2026-10-27T15:59:59Z')), '2026-10-27');
  assert.equal(bot.shanghaiDay(new Date('2026-10-27T16:00:00Z')), '2026-10-28');
  assert.equal(bot.botStatusFor(rows, 'shop_a', 'first', '2026-10-28').status, 'unknown');
  assert.equal(bot.botStatusFor(rows, 'shop_b', 'first', '2026-10-27').revision, 0);
  assert.equal(bot.botStatusFor(rows, 'shop_a', 'first', '2026-10-27').revision, 2);
});

test('unknown state is explicitly labelled as unreported', () => {
  assert.equal(bot.botStateName('unknown'), '未报备');
});

test('a delivered failure receipt is distinct from a successful final answer', () => {
  assert.equal(bot.botRequestStateName('fallback_sent'), '失败回执已送达');
  assert.equal(bot.botRequestStateName('sent'), '已确认回复');
  assert.equal(bot.botRequestStateName('accepted'), '网关已接受');
  assert.equal(bot.botRequestStateName('submitting'), '提交中');
  assert.equal(bot.botRequestStateName('rejected'), '网关拒绝');
  assert.equal(bot.botRequestStateName(null), '尚无记录');
});

test('Markdown import reads a bounded local document without converting it into extra role fields', () => {
  const markdown = '\uFEFF## 身份\r\n机灵的小管家\r\n\r\n## 对话示例\r\n群友：在吗\r\n角色：来啦\r\n';
  const imported = bot.importPersonaMarkdown('角色.MD', markdown);
  assert.equal(imported, '## 身份\n机灵的小管家\n\n## 对话示例\n群友：在吗\n角色：来啦\n');
  assert.throws(() => bot.importPersonaMarkdown('角色.html', markdown), /\.md/);
  assert.throws(() => bot.importPersonaMarkdown('角色.md', ' \n\t '), /不能为空/);
  assert.throws(() => bot.importPersonaMarkdown('角色.md', 'x'.repeat(16001)), /16,000/);
  assert.equal(bot.importPersonaMarkdown('角色.md', 'x'.repeat(16000)).length, 16000);
});

test('blank role drafts do not masquerade as a configured persona', () => {
  assert.match(bot.personaDraftError({ id: 'one', name: '  ', version: 1, markdown: '# 角色' }), /名称/);
  assert.match(bot.personaDraftError({ id: 'one', name: '阿尔法', version: 1, markdown: ' \n' }), /不能为空/);
  assert.equal(bot.personaDraftError({ id: 'one', name: '阿尔法', version: 1, markdown: '# 角色' }), '');
});

test('missing, invalid and negative timing values remain unknown while genuine zero stays visible', () => {
  for (const unknown of [null, undefined, NaN, Infinity, -1, '1.2']) assert.equal(bot.formatBotDuration(unknown), '—');
  assert.equal(bot.formatBotDuration(0), '0 ms');
  assert.equal(bot.formatBotDuration(0.125), '125 ms');
  assert.equal(bot.formatBotDuration(1.25), '1.25 s');
  assert.equal(bot.formatBotDuration(13.14), '13.1 s');
});

test('phase rows preserve sample sizes and independently reported quantiles without subtracting totals', () => {
  const rows = bot.botLatencyRows({
    last_request: { status: 'completed', generation_seconds: 2.25, end_to_end_seconds: 6.5, model_queue_seconds: 0.2 },
    latency_metrics: { generation: { count: 12, p50_seconds: 2, p95_seconds: 5 }, end_to_end: { count: 10, p50_seconds: 6, p95_seconds: 11 }, custom_queue: { count: 2, p50_seconds: 0.1, p95_seconds: 0.2 } },
  });
  const generation = rows.find(row => row.phase === 'generation');
  assert.equal(generation.label, '生成与工具处理');
  assert.equal(generation.latest, 2.25);
  assert.equal(generation.count, 12);
  assert.equal(generation.p95, 5);
  const send = rows.find(row => row.phase === 'send_confirmation');
  assert.equal(send.count, null);
  assert.equal(send.latest, undefined);
  assert.equal(send.p95, undefined);
  assert.equal(rows.find(row => row.phase === 'custom_queue').label, 'custom_queue');
  assert.equal(rows.find(row => row.phase === 'model_queue').latest, 0.2);
  assert.equal(rows.find(row => row.phase === 'model_queue').count, null);
  assert.equal(bot.botPhaseName('visible_reply'), '首条可见回复');
  assert.equal(bot.botPhaseName('chat_visible_reply'), '闲聊 · 首条可见回复');
  assert.equal(bot.botPhaseName('business_visible_reply'), '业务 · 首条可见回复');
  assert.equal(bot.botPhaseName('business_mcp'), '业务 · 工具调用');
  assert.equal(bot.botPhaseName('failed_generation'), '失败请求 · 生成与工具处理');
  assert.equal(bot.botPhaseName('failed_trigger_to_model'), '失败请求 · 收取与排队');
  assert.equal(bot.botPhaseName('failed_authorization'), '失败请求 · 登录授权');
  assert.equal(bot.botPhaseName('fallback_delivery'), '失败回执 · 送达');
  assert.equal(bot.botPhaseName('failed_chat_generation'), '失败请求 · 闲聊 · 生成与工具处理');
});
