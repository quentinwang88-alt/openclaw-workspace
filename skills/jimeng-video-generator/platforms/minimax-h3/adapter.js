const ALLOWED_RATIOS = new Set(['adaptive', '21:9', '16:9', '4:3', '1:1', '3:4', '9:16']);
const MODE_ALIASES = new Map([
  ['文生视频', 'text'], ['text', 'text'], ['t2v', 'text'],
  ['首帧', 'first_frame'], ['首帧生视频', 'first_frame'], ['first_frame', 'first_frame'],
  ['首尾帧', 'first_last'], ['首尾帧生视频', 'first_last'], ['first_last', 'first_last'],
  ['多参考图', 'reference'], ['多参考', 'reference'], ['reference', 'reference']
]);

function normalizeMode(value, imageCount) {
  const raw = String(value || '').trim().toLowerCase();
  if (MODE_ALIASES.has(raw)) return MODE_ALIASES.get(raw);
  return imageCount > 0 ? 'reference' : 'text';
}

function normalizeResolution(value) {
  const raw = String(value || '').trim().toUpperCase();
  if (raw === '2K') return '2K';
  if (raw === '768P' || raw === '720P' || !raw) return '768P';
  throw new Error(`MiniMax-H3 不支持分辨率: ${value}`);
}

function buildGenerationPayload({ prompt, imageUrls = [], mode = '', resolution = '768P', duration = 5, ratio = '9:16', watermark = false }) {
  const text = String(prompt || '').trim();
  if (!text) throw new Error('提示词不能为空');
  if (text.length > 7000) throw new Error(`提示词超过 7000 字符（当前 ${text.length}）`);

  const seconds = Number(duration);
  if (!Number.isInteger(seconds) || seconds < 4 || seconds > 15) {
    throw new Error(`视频时长必须是 4-15 秒整数（当前 ${duration}）`);
  }
  if (imageUrls.length > 9) throw new Error('参考图最多 9 张');

  const normalizedMode = normalizeMode(mode, imageUrls.length);
  const content = [{ type: 'text', text }];
  let normalizedRatio = String(ratio || '9:16').trim();
  if (!ALLOWED_RATIOS.has(normalizedRatio)) throw new Error(`不支持的视频比例: ${normalizedRatio}`);

  if (normalizedMode === 'text') {
    if (imageUrls.length) throw new Error('文生视频模式不能携带参考图');
    if (normalizedRatio === 'adaptive') throw new Error('文生视频不能使用 adaptive 比例');
  } else if (normalizedMode === 'first_frame') {
    if (imageUrls.length !== 1) throw new Error('首帧模式必须且只能提供 1 张参考图');
    content.push({ type: 'image_url', image_url: { url: imageUrls[0] }, role: 'first_frame' });
    normalizedRatio = 'adaptive';
  } else if (normalizedMode === 'first_last') {
    if (imageUrls.length !== 2) throw new Error('首尾帧模式必须且只能提供 2 张参考图');
    content.push({ type: 'image_url', image_url: { url: imageUrls[0] }, role: 'first_frame' });
    content.push({ type: 'image_url', image_url: { url: imageUrls[1] }, role: 'last_frame' });
    normalizedRatio = 'adaptive';
  } else {
    if (!imageUrls.length) throw new Error('多参考图模式至少需要 1 张参考图');
    for (const url of imageUrls) {
      content.push({ type: 'image_url', image_url: { url }, role: 'reference_image' });
    }
  }

  return {
    mode: normalizedMode,
    payload: {
      model: 'MiniMax-H3',
      content,
      resolution: normalizeResolution(resolution),
      duration: seconds,
      ratio: normalizedRatio,
      aigc_watermark: Boolean(watermark)
    }
  };
}

module.exports = {
  ALLOWED_RATIOS,
  buildGenerationPayload,
  normalizeMode,
  normalizeResolution
};
