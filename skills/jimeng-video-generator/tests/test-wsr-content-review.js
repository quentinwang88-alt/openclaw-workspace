// Offline: production submit/delivery/reconciliation never invokes model QA.
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const feishu = require('../lib/feishu-client');
const updates = [];
feishu.updateRecord = async (_config, _token, record, fields) => updates.push({ record, fields });
feishu.listTableFields = async () => { throw new Error('Unexpected schema read during delivery'); };
const upload = require('../platforms/minimax-h3/feishu-upload');
upload.uploadBitableFile = async () => ({ fileToken: 'mock-video' });
const h3 = require('../platforms/minimax-h3/client');
h3.queryTask = async () => { throw new Error('Unexpected external query'); };
let modelCalls = 0;
const helper = require('../lib/wsr-content-review');
helper.runReviewProcess = async () => { modelCalls++; throw new Error('Production must not call reviewer'); };
const { pollRecord, DEFAULT_FIELDS } = require('../minimax-h3-feishu-worker');
const { writeSubmissionRecord, readSubmissionRecord } = require('../trace-state');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'h3-content-review-test-'));
// Even obsolete enablement and trace flags cannot opt production back into QA.
const config = { stateRoot: root, metasoH3: { contentReviewEnabled: true }, fields: DEFAULT_FIELDS };
const context = { recordId: 'record', platformTaskId: 'task', traceId: 'trace', taskName: 'test',
  prompt: 'unchanged', fields: { 脚本ID: 'wsr_test' } };

async function main() {
  const source = fs.readFileSync(path.join(__dirname, '..', 'minimax-h3-feishu-worker.js'), 'utf8');
  for (const forbidden of ['runReviewProcess', 'reviewCompletedVideo', 'run_wsr_content_review.py', "require('./lib/wsr-content-review')"]) {
    assert(!source.includes(forbidden), `production must not wire ${forbidden}`);
  }
  const video = path.join(root, 'mock.mp4'); fs.writeFileSync(video, 'mock media');
  writeSubmissionRecord(config, 'trace', { trace_id: 'trace', channel: 'minimax_h3', platform_task_id: 'task',
    status: 'downloaded', download_path: video, content_review_eligible: true });
  assert.deepStrictEqual(await pollRecord(context, config, 'unused', 'unused'), { status: 'uploaded' });
  assert.strictEqual(modelCalls, 0, 'fresh delivery must not call a model');
  assert.strictEqual(readSubmissionRecord(config, 'trace').status, 'uploaded');
  assert.deepStrictEqual(await pollRecord(context, config, 'unused', 'unused'), { status: 'uploaded_reconciled' });
  assert.strictEqual(modelCalls, 0, 'uploaded reconciliation must not call a model');
  assert(updates.every(item => !('内容检查结果' in item.fields) && !('内容检查说明' in item.fields)),
    'production must not overwrite test-only QA fields');
  const result = { status: 'fail', summary: '遮镜时假发提前出现', evidence: [{ check_id: 'reveal', start_seconds: 7, end_seconds: 8, description: '未全遮镜' }] };
  const mapped = helper.reviewFields(result, DEFAULT_FIELDS);
  assert.strictEqual(mapped['内容检查结果'], '有偏差');
  assert(mapped['内容检查说明'].includes('7–8s'));
  assert.strictEqual(DEFAULT_FIELDS.contentReviewResult, '内容检查结果');
  assert.strictEqual(DEFAULT_FIELDS.contentReviewSummary, '内容检查说明');
  console.log('WSR test-only QA isolation: production never calls review model; field helpers retained');
}
main().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => fs.rmSync(root, { recursive: true, force: true }));
