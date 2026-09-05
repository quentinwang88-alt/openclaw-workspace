// Offline-only: pure contracts and blocked iMini entrypoints; no browser/network.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const {
  assertWsrChannel, isWsrContext, prepareWsrReferenceExecution
} = require('../lib/script-pool-reference-contract');
const { resolveReferenceSelection } = require('../minimax-h3-feishu-worker');
const { processIminiTask } = require('../platforms/imini/adapter');
const { runFirstFramePipeline, submitToImini } = require('../platforms/imini/submitter');

let assertions = 0;
function check(fn) { fn(); assertions += 1; }
function context(count = 2, overrides = {}) {
  const contract = { schema_version: '2', reference_assets: Array.from({ length: count }, (_, index) => ({
    index: index + 1, role: index === 0 ? 'person_identity' : 'product',
    file_token: `reference-${index + 1}`, original_sha256: 'a'.repeat(64), derived_sha256: 'a'.repeat(64)
  })) };
  return {
    taskSource: '成功脚本复刻', scriptId: 'wsr_abc123', prompt: '原脚本\n口播：Mira este cambio.',
    mode: '全能参考', channel: '即梦',
    attachments: Array.from({ length: count }, (_, i) => ({ fileToken: `reference-${i + 1}` })),
    record: { fields: { 人物模板合同: JSON.stringify(contract) } },
    ...overrides
  };
}

check(() => {
  const source = context();
  const before = JSON.stringify(source);
  const prepared = prepareWsrReferenceExecution(source);
  assert.strictEqual(prepared.guarded, true);
  assert(prepared.prompt.startsWith(source.prompt));
  assert(prepared.prompt.includes('图 1：人物身份与脸部参考'));
  assert(prepared.prompt.includes('图 2：商品外观参考'));
  assert.strictEqual(JSON.stringify(source), before, '不得修改源对象或飞书原文');
  assert.strictEqual(prepared.imageCount, 2);
});
check(() => {
  const source = context(12);
  assert.strictEqual(prepareWsrReferenceExecution(source).imageCount, 12);
  assert.throws(() => prepareWsrReferenceExecution(context(13)), /WSR_REFERENCE_COUNT_UNSUPPORTED/);
});
check(() => {
  assert.throws(() => prepareWsrReferenceExecution(context(2, { mode: '首尾帧' })), /WSR_REFERENCE_MODE_UNSUPPORTED/);
  assert.throws(() => prepareWsrReferenceExecution(context(1, { mode: '首帧' })), /WSR_REFERENCE_MODE_UNSUPPORTED/);
  assert.throws(() => prepareWsrReferenceExecution(context(2, { channel: 'imini' })), /WSR_REFERENCE_CHANNEL_UNSUPPORTED/);
});
check(() => {
  const source = context(9, { referenceMode: '多参考图' });
  const selected = resolveReferenceSelection(source);
  const prepared = prepareWsrReferenceExecution(source, { channel: 'minimax_h3', mode: selected.mode, attachments: selected.attachments });
  assert.strictEqual(prepared.imageCount, 9);
  assert.throws(() => prepareWsrReferenceExecution(context(10), { channel: 'minimax_h3', mode: '多参考图' }), /WSR_REFERENCE_COUNT_UNSUPPORTED/);
});
check(() => {
  const source = context(2, { referenceMode: '首尾帧' });
  const selected = resolveReferenceSelection(source);
  assert.throws(() => prepareWsrReferenceExecution(source, {
    channel: 'minimax_h3', mode: selected.mode, attachments: selected.attachments
  }), /WSR_REFERENCE_MODE_UNSUPPORTED/);
});
check(() => {
  assert.throws(() => prepareWsrReferenceExecution(context(2, { record: { fields: {} } })), /WSR_REFERENCE_CONTRACT_MISMATCH/);
  assert.throws(() => prepareWsrReferenceExecution(context(2, { personaContract: '{invalid' })), /WSR_REFERENCE_CONTRACT_INVALID/);
  assert.throws(() => prepareWsrReferenceExecution(context(2, {
    personaContract: JSON.stringify({ reference_assets: [{ index: 1, role: 'person_identity' }, { index: 1, role: 'product' }] })
  })), /WSR_REFERENCE_CONTRACT_MISMATCH/);
  assert.throws(() => prepareWsrReferenceExecution(context(2), { channel: '即梦', mode: '全能参考', attachments: [{}] }), /WSR_REFERENCE_CONTRACT_MISMATCH/);
});
check(() => {
  assert.strictEqual(prepareWsrReferenceExecution(context(0, { allowNoReferenceImage: true })).imageCount, 0);
  assert.throws(() => prepareWsrReferenceExecution(context(0)), /WSR_REFERENCE_REQUIRED/);
});
check(() => {
  const old = { prompt: '旧原创提示词', channel: 'imini', mode: '首帧', attachments: Array(30).fill({}) };
  assert.deepStrictEqual(prepareWsrReferenceExecution(old), { guarded: false, prompt: old.prompt });
  assert.doesNotThrow(() => assertWsrChannel(old, 'imini', '首帧', 30));
  assert.strictEqual(isWsrContext({ fields: { 脚本ID: 'wsr_123' } }), true);
  assert.strictEqual(isWsrContext({ prompt: '【脚本ID】\n- wsr_123\n\n正文' }), true);
});
check(() => {
  const source = context();
  const first = prepareWsrReferenceExecution(source);
  const second = prepareWsrReferenceExecution({ ...source, prompt: first.prompt, wsrSourcePrompt: first.sourcePrompt });
  assert.strictEqual(second.prompt, first.prompt, '续跑不能重复追加执行层说明');
  assert.strictEqual(second.prompt.split('人物图组定义同一人物').length - 1, 1);
  assert(second.prompt.includes('商品图组定义目标假发'));
  assert(!source.prompt.includes('不上传参考视频'), '正文无需含上传流程的固定措辞');
});
check(() => {
  const source = context();
  const contract = JSON.parse(source.record.fields['人物模板合同']);
  contract.mother_core_points = [{ point_id: 'mask', requirement: '双掌从胸前推进再全遮镜' }];
  contract.effective_checkpoints = [{ point_id: 'mask', requirement: '单掌全遮镜后换发' }];
  contract.allowed_changes = [{ point_id: 'mask', operation: 'replace', replacement_requirement: '单掌全遮镜后换发', reason: '对照拍法' }];
  source.personaContract = contract;
  const prepared = prepareWsrReferenceExecution(source);
  assert.deepStrictEqual(prepared.motherCheckpoints, contract.effective_checkpoints);
  assert.deepStrictEqual(prepared.frozenMotherCheckpoints, contract.mother_core_points);
  assert.deepStrictEqual(prepared.allowedChanges, contract.allowed_changes);
  assert(!prepared.prompt.includes('对照拍法'), '测试元数据不写进视频画面指令');
});
check(() => {
  // Static wiring check avoids importing a monitor that auto-runs at require.
  const monitor = fs.readFileSync(path.join(__dirname, '..', 'feishu-direct-monitor.js'), 'utf8');
  const guard = monitor.indexOf('const execution = prepareWsrReferenceExecution(context');
  assert(guard > 0);
  assert(guard < monitor.indexOf('const task = await materializeTask(token, context, config)', guard));
  const h3 = fs.readFileSync(path.join(__dirname, '..', 'minimax-h3-feishu-worker.js'), 'utf8');
  const h3Guard = h3.indexOf('referenceExecution = prepareWsrReferenceExecution(context');
  assert(h3Guard < h3.indexOf('const owner = await claimRecord(', h3Guard));
  assert(h3.indexOf('prompt: executionPrompt', h3Guard) < h3.indexOf('const taskId = await createTask(', h3Guard));
});

async function main() {
  // Poison browser/config objects prove all WSR iMini entrypoints return before
  // touching browser, network, first-frame generation or account configuration.
  const forbidden = new Proxy({}, { get() { throw new Error('Unexpected side effect'); } });
  const source = context();
  const adapterResult = await processIminiTask({ page: forbidden, context: source, config: forbidden });
  assert.strictEqual(adapterResult.shouldBlock, true);
  assert.strictEqual(adapterResult.shouldFallBack, false);
  const frameResult = await runFirstFramePipeline(source, forbidden, ['/unused/person.jpg', '/unused/product.jpg']);
  assert.strictEqual(frameResult.code, 'WSR_REFERENCE_CHANNEL_UNSUPPORTED');
  const submitResult = await submitToImini(forbidden, source, null, '/unused/frame.jpg', forbidden);
  assert.strictEqual(submitResult.code, 'WSR_REFERENCE_CHANNEL_UNSUPPORTED');
  console.log(`WSR 多参考图合同离线测试通过（${assertions} 组 + 3 个付费前阻断入口）`);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
