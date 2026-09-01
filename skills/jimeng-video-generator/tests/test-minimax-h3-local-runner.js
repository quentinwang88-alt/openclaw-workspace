const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { resolveApiKey } = require('../platforms/minimax-h3/credentials');
const { fingerprint, findVideoUrl, prepare, readState, writeState } = require('../platforms/minimax-h3/local-job-runner');

(async () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'h3-local-runner-'));
  const image = path.join(temp, 'frame.jpg');
  fs.writeFileSync(image, 'not-an-image-but-valid-offline-fixture');
  const request = { prompt: '同一人物继续分享', mode: '首帧', duration: 13,
    imagePaths: [image], ratio: '9:16', resolution: '768P' };
  const hash = fingerprint(request);
  assert.strictEqual(hash, fingerprint({ ...request }));
  const sameContentElsewhere = path.join(temp, 'frame-copy.jpg');
  fs.copyFileSync(image, sameContentElsewhere);
  assert.strictEqual(hash, fingerprint({ ...request, imagePaths: [sameContentElsewhere] }));
  fs.writeFileSync(image, 'changed-content-at-the-same-path');
  assert.notStrictEqual(hash, fingerprint({ ...request }));
  fs.writeFileSync(image, 'not-an-image-but-valid-offline-fixture');
  const prepared = await prepare(request, { watermark: false }, false);
  assert.strictEqual(prepared.mode, 'first_frame');
  assert.strictEqual(prepared.payload.duration, 13);
  assert.strictEqual(prepared.payload.content[1].role, 'first_frame');
  writeState(temp, hash, { status: 'SUBMITTED', taskId: 'task-1' });
  assert.strictEqual(readState(temp, hash).taskId, 'task-1');
  assert.strictEqual(findVideoUrl({ data: { video_url: 'https://example.com/a.mp4' } }), 'https://example.com/a.mp4');
  assert.strictEqual(findVideoUrl({ data: { status: 'processing' } }), '');
  process.env.H3_TEST_KEY = 'test-secret';
  assert.strictEqual(resolveApiKey('H3_TEST_KEY'), 'test-secret');
  delete process.env.H3_TEST_KEY;
  console.log('MiniMax H3 本地长视频网关离线测试通过');
})().catch(error => { console.error(error); process.exitCode = 1; });
