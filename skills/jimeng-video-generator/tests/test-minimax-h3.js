const assert = require('assert');
const fs = require('fs');
const path = require('path');

const { buildGenerationPayload } = require('../platforms/minimax-h3/adapter');
const { createPinnedLookup, isPrivateIp } = require('../platforms/minimax-h3/client');
const { resolveChannel } = require('../channel-router');
const {
  buildFingerprint,
  isH3Channel,
  isH3Model,
  resolveReferenceSelection,
  safeErrorText
} = require('../minimax-h3-feishu-worker');

function mustThrow(fn, pattern) {
  assert.throws(fn, pattern);
}

const text = buildGenerationPayload({ prompt: '测试', duration: 4, resolution: '720P', ratio: '16:9' });
assert.strictEqual(text.mode, 'text');
assert.strictEqual(text.payload.resolution, '768P');
assert.strictEqual(text.payload.content.length, 1);

const first = buildGenerationPayload({ prompt: '测试', mode: '首帧', imageUrls: ['mm_file://1'], duration: 5, ratio: '9:16' });
assert.strictEqual(first.payload.ratio, 'adaptive');
assert.strictEqual(first.payload.content[1].role, 'first_frame');

const firstLast = buildGenerationPayload({ prompt: '测试', mode: '首尾帧', imageUrls: ['mm_file://1', 'mm_file://2'], duration: 15 });
assert.deepStrictEqual(firstLast.payload.content.slice(1).map(item => item.role), ['first_frame', 'last_frame']);

const reference = buildGenerationPayload({ prompt: '测试', mode: '多参考图', imageUrls: ['mm_file://1', 'mm_file://2'], duration: 6, resolution: '2K' });
assert.strictEqual(reference.payload.resolution, '2K');
assert.deepStrictEqual(reference.payload.content.slice(1).map(item => item.role), ['reference_image', 'reference_image']);

mustThrow(() => buildGenerationPayload({ prompt: '测试', mode: '文生视频', imageUrls: ['mm_file://1'], duration: 4 }), /不能携带/);
mustThrow(() => buildGenerationPayload({ prompt: '测试', duration: 3 }), /4-15/);
mustThrow(() => buildGenerationPayload({ prompt: '测试', mode: '首尾帧', imageUrls: ['mm_file:\/\/1'], duration: 4 }), /必须且只能提供 2/);

const routerConfig = {
  channels: {
    default: '即梦',
    imini: { enabled: false },
    metasoH3: { channelValue: 'METASO' }
  }
};
assert.strictEqual(resolveChannel({ channel: 'METASO' }, routerConfig).channel, 'minimax_h3');
assert.strictEqual(resolveChannel({ channel: '即梦' }, routerConfig).channel, '即梦');
assert.strictEqual(resolveChannel({ channel: 'imini' }, routerConfig).channel, 'imini');
assert.strictEqual(resolveChannel({ channel: '未知渠道' }, routerConfig).channel, 'unsupported');
assert.strictEqual(resolveChannel({ channel: '' }, routerConfig).channel, '即梦');

assert.strictEqual(isH3Channel('METASO', { metasoH3: { channelValue: 'METASO' } }), true);
assert.strictEqual(isH3Channel('minimax-h3', { metasoH3: { channelValue: 'METASO' } }), true);
assert.strictEqual(isH3Channel('即梦', { metasoH3: { channelValue: 'METASO' } }), false);
assert.strictEqual(isH3Model('MiniMax H3'), true);
assert.strictEqual(isH3Model('MiniMax-H3'), true);
assert.strictEqual(isH3Model('Seedance 2.0'), false);

const ref1 = { fileToken: 'ref-1' };
const ref2 = { fileToken: 'ref-2' };
const firstFrame = { fileToken: 'first' };
const lastFrame = { fileToken: 'last' };
let selected = resolveReferenceSelection({
  referenceMode: '首帧',
  attachments: [ref1, ref2],
  firstFrameAttachments: [firstFrame],
  lastFrameAttachments: []
});
assert.deepStrictEqual(selected.attachments, [firstFrame]);
assert.strictEqual(selected.source, '首帧图片');

selected = resolveReferenceSelection({
  referenceMode: '首尾帧',
  attachments: [ref1],
  firstFrameAttachments: [firstFrame],
  lastFrameAttachments: [lastFrame]
});
assert.deepStrictEqual(selected.attachments, [firstFrame, lastFrame]);

selected = resolveReferenceSelection({
  referenceMode: '首帧',
  attachments: [ref1],
  firstFrameAttachments: [],
  lastFrameAttachments: []
});
assert.deepStrictEqual(selected.attachments, [ref1]);

selected = resolveReferenceSelection({
  referenceMode: '首尾帧',
  attachments: [ref1, ref2],
  firstFrameAttachments: [],
  lastFrameAttachments: []
});
assert.deepStrictEqual(selected.attachments, [ref1, ref2]);
mustThrow(() => resolveReferenceSelection({
  referenceMode: '首尾帧',
  attachments: [ref1, ref2],
  firstFrameAttachments: [firstFrame],
  lastFrameAttachments: []
}), /都必须各有 1 张/);
assert.strictEqual(isPrivateIp('127.0.0.1'), true);
assert.strictEqual(isPrivateIp('172.31.1.2'), true);
assert.strictEqual(isPrivateIp('192.168.1.2'), true);
assert.strictEqual(isPrivateIp('8.8.8.8'), false);
assert.strictEqual(isPrivateIp('::1'), true);
const pinnedLookup = createPinnedLookup({ address: '8.8.8.8', family: 4 });
pinnedLookup('video.example', { all: true }, (error, addresses) => {
  assert.ifError(error);
  assert.deepStrictEqual(addresses, [{ address: '8.8.8.8', family: 4 }]);
});
pinnedLookup('video.example', {}, (error, address, family) => {
  assert.ifError(error);
  assert.strictEqual(address, '8.8.8.8');
  assert.strictEqual(family, 4);
});
const fingerprintInput = { prompt: '测试', referenceMode: '文生视频', attachments: [], ratio: '9:16', duration: 4, resolution: '768P' };
assert.strictEqual(buildFingerprint(fingerprintInput), buildFingerprint(fingerprintInput));
assert(!safeErrorText(new Error('Bearer secret-token https://signed.example/video')).includes('secret-token'));

const config = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'feishu-direct.json'), 'utf8'));
const serialized = JSON.stringify(config);
assert(!/mk-[A-Za-z0-9]+/.test(serialized), '配置文件不得包含 MetaSo 密钥');
assert.strictEqual(config.channels.metasoH3.apiKeyEnv, 'METASO_MINIMAX_API_KEY');

console.log('MiniMax H3 离线测试通过');
