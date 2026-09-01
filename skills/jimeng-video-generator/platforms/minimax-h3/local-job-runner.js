#!/usr/bin/env node

/* Local MiniMax H3 gateway for the isolated long-form workflow.
 *
 * It deliberately owns no Feishu state. Real submission requires the explicit
 * --allow-real-submit flag; an uncertain create response is persisted and is
 * never retried automatically, preventing duplicate billing.
 */
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const { buildGenerationPayload } = require('./adapter');
const {
  SubmissionUncertainError,
  createTask,
  downloadPublicVideo,
  queryTask,
  uploadFile
} = require('./client');
const { requireApiKey } = require('./credentials');

const DEFAULT_CONFIG = path.resolve(__dirname, '..', '..', 'feishu-direct.json');
const DEFAULT_STATE_ROOT = '/Users/likeu3/.openclaw/shared/data/longform_original_video/h3_state';

function parseArgs(argv) {
  const result = { command: '', request: '', config: DEFAULT_CONFIG, stateRoot: DEFAULT_STATE_ROOT,
    allowRealSubmit: false, taskId: '', output: '' };
  result.command = argv[0] || '';
  for (let index = 1; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--request') result.request = argv[++index] || '';
    else if (arg === '--config') result.config = argv[++index] || '';
    else if (arg === '--state-root') result.stateRoot = argv[++index] || '';
    else if (arg === '--task-id') result.taskId = argv[++index] || '';
    else if (arg === '--output') result.output = argv[++index] || '';
    else if (arg === '--allow-real-submit') result.allowRealSubmit = true;
  }
  return result;
}

function loadRequest(filePath) {
  const raw = filePath ? fs.readFileSync(filePath, 'utf8') : fs.readFileSync(0, 'utf8');
  const value = JSON.parse(raw);
  if (!value || typeof value !== 'object') throw new Error('请求必须是 JSON 对象');
  return value;
}

function loadChannelConfig(filePath) {
  const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  const h3 = raw.channels?.metasoH3 || {};
  return {
    baseUrl: h3.baseUrl || 'https://metaso.cn/api/minimax',
    apiKeyEnv: h3.apiKeyEnv || 'METASO_MINIMAX_API_KEY',
    requestTimeoutMs: Math.max(30_000, Number(h3.requestTimeoutMs || 90_000)),
    watermark: Boolean(h3.watermark)
  };
}

function fingerprint(request) {
  const imageContent = (request.imagePaths || []).map(value => {
    const filePath = path.resolve(String(value));
    const stat = fs.statSync(filePath);
    if (!stat.isFile()) throw new Error(`参考图不是文件: ${filePath}`);
    return {
      sha256: crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex'),
      size: stat.size
    };
  });
  const stable = JSON.stringify({
    prompt: String(request.prompt || '').trim(),
    mode: String(request.mode || '').trim(),
    duration: Number(request.duration),
    ratio: String(request.ratio || '9:16'),
    resolution: String(request.resolution || '768P'),
    imageContent
  });
  return crypto.createHash('sha256').update(stable).digest('hex');
}

function statePath(root, hash) {
  return path.join(root, `${hash}.json`);
}

function readState(root, hash) {
  const target = statePath(root, hash);
  if (!fs.existsSync(target)) return null;
  return JSON.parse(fs.readFileSync(target, 'utf8'));
}

function writeState(root, hash, value) {
  fs.mkdirSync(root, { recursive: true });
  const target = statePath(root, hash);
  const temp = `${target}.tmp-${process.pid}`;
  fs.writeFileSync(temp, JSON.stringify({ ...value, updatedAt: new Date().toISOString() }, null, 2));
  fs.renameSync(temp, target);
}

function findVideoUrl(value) {
  if (!value || typeof value !== 'object') return '';
  for (const key of ['video_url', 'download_url', 'file_url', 'url']) {
    const candidate = value[key];
    if (typeof candidate === 'string' && /^https:\/\//.test(candidate)) return candidate;
  }
  for (const child of Object.values(value)) {
    if (child && typeof child === 'object') {
      const candidate = findVideoUrl(child);
      if (candidate) return candidate;
    }
  }
  return '';
}

async function prepare(request, config, upload) {
  const paths = Array.isArray(request.imagePaths) ? request.imagePaths : [];
  for (const item of paths) {
    if (!fs.existsSync(item) || !fs.statSync(item).isFile()) throw new Error(`参考图不存在: ${item}`);
  }
  let imageUrls = paths.map(item => `local-preview://${path.basename(item)}`);
  if (upload) {
    const token = requireApiKey(config.apiKeyEnv);
    imageUrls = [];
    for (const filePath of paths) {
      imageUrls.push(await uploadFile({ baseUrl: config.baseUrl, token, filePath }));
    }
  }
  return buildGenerationPayload({
    prompt: request.prompt,
    imageUrls,
    mode: request.mode,
    resolution: request.resolution || '768P',
    duration: request.duration,
    ratio: request.ratio || '9:16',
    watermark: request.watermark == null ? config.watermark : Boolean(request.watermark)
  });
}

async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (!['prepare', 'submit', 'query', 'download'].includes(args.command)) {
    throw new Error('用法: local-job-runner.js prepare|submit|query|download [参数]');
  }
  const config = loadChannelConfig(args.config);
  if (args.command === 'prepare') {
    const request = loadRequest(args.request);
    const prepared = await prepare(request, config, false);
    return { action: 'PREPARE_ONLY', fingerprint: fingerprint(request), ...prepared };
  }
  if (args.command === 'submit') {
    if (!args.allowRealSubmit) throw new Error('真实提交被关闭；确认付费后必须显式传 --allow-real-submit');
    const request = loadRequest(args.request);
    const hash = fingerprint(request);
    const existing = readState(args.stateRoot, hash);
    if (existing?.status === 'SUBMIT_UNCERTAIN') {
      throw new Error('此前提交结果不确定，禁止自动重试；请先到渠道侧人工核对');
    }
    if (existing?.taskId) return { action: 'IDEMPOTENT_REUSE', fingerprint: hash, ...existing };
    try {
      const prepared = await prepare(request, config, true);
      const token = requireApiKey(config.apiKeyEnv);
      const taskId = await createTask({ baseUrl: config.baseUrl, token, payload: prepared.payload,
        timeoutMs: config.requestTimeoutMs });
      const state = { status: 'SUBMITTED', taskId, fingerprint: hash, requestSummary: {
        mode: prepared.mode, duration: request.duration, resolution: request.resolution || '768P'
      }};
      writeState(args.stateRoot, hash, state);
      return { action: 'SUBMITTED', ...state };
    } catch (error) {
      if (error instanceof SubmissionUncertainError || error?.code === 'SUBMIT_UNCERTAIN') {
        writeState(args.stateRoot, hash, { status: 'SUBMIT_UNCERTAIN', fingerprint: hash,
          error: String(error.message || error) });
      }
      throw error;
    }
  }

  const taskId = args.taskId;
  if (!taskId) throw new Error(`${args.command} 需要 --task-id`);
  const token = requireApiKey(config.apiKeyEnv);
  const response = await queryTask({ baseUrl: config.baseUrl, token, taskId,
    timeoutMs: config.requestTimeoutMs });
  if (args.command === 'query') return { taskId, response, videoUrl: findVideoUrl(response) };
  const url = findVideoUrl(response);
  if (!url) throw new Error('任务尚未返回可下载的视频地址');
  if (!args.output) throw new Error('download 需要 --output');
  const output = await downloadPublicVideo(url, args.output);
  return { taskId, output };
}

if (require.main === module) {
  main().then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`))
    .catch(error => {
      process.stderr.write(`${JSON.stringify({ error: String(error.message || error), code: error.code || '' })}\n`);
      process.exitCode = 1;
    });
}

module.exports = { fingerprint, findVideoUrl, loadChannelConfig, prepare, readState, writeState, main };
