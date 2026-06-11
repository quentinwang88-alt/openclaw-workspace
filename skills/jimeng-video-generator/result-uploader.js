#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const os = require('os');
const puppeteer = require('puppeteer-core');

const {
  prepareAutomationPage,
  checkGeneratingStatus,
  resetVideoGenerationPage,
  sleep,
  downloadVideo
} = require('./folder-processor');
const {
  expandHome,
  getStateRoot,
  listSubmissionRecords,
  updateSubmissionRecord
} = require('./trace-state');
const { claimIminiAsset } = require('./platforms/imini/asset-claimer');
let syncJimengTaskState = null;
try {
  ({ syncJimengTaskState } = require('../short-video-automation-mvp/services/nodes/sync-jimeng-task-state'));
} catch (error) {
  syncJimengTaskState = null;
}

const DEFAULT_CONFIG = {
  appId: '',
  appSecret: '',
  appToken: '',
  tableId: '',
  statusField: '状态',
  cdpPort: 9222,
  cdpHost: '127.0.0.1',
  baseUrl: 'https://jimeng.jianying.com/ai-tool/home?workspace=0&type=video',
  assetUrl: 'https://jimeng.jianying.com/ai-tool/asset?workspace=0',
  runtimeRoot: '~/Desktop/temp/jimeng-feishu-runtime',
  maxAssetCandidates: 10,
  assetScanBatches: 5,
  assetScrollStepPx: 1200,
  concurrentClaimBatchLimit: 5,
  idleClaimBatchLimit: 8,
  assetRepeatIgnoreMinutes: 180,
  assetRepeatAbortThreshold: 10,
  assetHeadSignatureSize: 4,
  assetCursorEnabled: true,
  assetDeepScanEveryRuns: 4,
  priorityClaimContentIds: [],
  enableContentIdClaim: true,
  contentIdMode: 'grayscale',
  contentIdLabel: '内容ID',
  claimStrategyOrder: ['content_id', 'script_id', 'prompt_hash', 'prompt_anchor'],
  claimGraceMinutes: 20,
  maxClaimAttempts: 5,
  submittingTimeoutMinutes: 30,
  submissionTimeoutMinutes: 480,
  claimCooldownSeconds: 120,
  fields: {
    executionOwner: '执行归属',
    prompt: '提示词',
    result: '结果说明',
    lastProcessedAt: '最后处理时间',
    latestTraceId: '最新追踪ID',
    resultSyncStatus: '结果回传状态',
    videoAttachment: '生成视频',
    videoFileName: '生成视频文件名',
    submitTime: '提交时间',
    finishTime: '完成时间',
    errorMessage: '错误信息'
  },
  uploadParentTypes: ['bitable_file', 'bitable_image']
};

const DOWNLOAD_TAB_ROLE = 'openclaw-jimeng-download';

let cachedAccessToken = null;
let tokenExpireTime = 0;

function syncAutomationTaskFromJimeng(submission, notes = '') {
  if (!syncJimengTaskState) {
    return;
  }
  try {
    return syncJimengTaskState({
      submission,
      notes
    });
  } catch (error) {
    console.log(`⚠️  自动化库状态同步失败（Jimeng upload侧）: ${error.message}`);
    return {
      status: 'sync_failed',
      matched: false,
      error: error.message
    };
  }
}

function normalizePromptFingerprintText(value) {
  return String(value || '').replace(/\s+/g, '').trim().toLowerCase();
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseContentIdMetadata(value, label = '内容ID') {
  const rawText = String(value || '');
  if (!rawText.trim()) {
    return {
      id: '',
      label: '',
      found: false
    };
  }

  const safeLabel = escapeRegex(label);
  const markerPattern = new RegExp(`【\\s*${safeLabel}\\s*】|(?:^|\\n)\\s*${safeLabel}`, 'i');
  const markerMatch = markerPattern.exec(rawText);
  if (markerMatch) {
    const section = rawText.slice(markerMatch.index, markerMatch.index + 120);
    const idMatch = section.match(/[-:：]\s*([A-Za-z0-9][A-Za-z0-9_-]{1,63})/);
    if (idMatch) {
      const id = String(idMatch[1] || '').trim();
      if (id) {
        const labelText = section
          .slice(0, section.indexOf(id) + id.length)
          .replace(/\s+\n/g, '\n')
          .trim();

        return {
          id,
          label: labelText,
          found: true
        };
      }
    }
  }

  return {
    id: '',
    label: '',
    found: false
  };
}

function parseScriptIdMetadata(value, label = '脚本ID') {
  const rawText = String(value || '');
  if (!rawText.trim()) {
    return {
      id: '',
      label: '',
      found: false
    };
  }

  const safeLabel = escapeRegex(label);
  const markerPattern = new RegExp(`【\\s*${safeLabel}\\s*】|(?:^|\\n)\\s*${safeLabel}`, 'i');
  const markerMatch = markerPattern.exec(rawText);
  if (!markerMatch) {
    return {
      id: '',
      label: '',
      found: false
    };
  }

  const section = rawText.slice(markerMatch.index, markerMatch.index + 120);
  const idMatch = section.match(/[-:：]\s*([A-Za-z0-9][A-Za-z0-9_-]{1,63})/i);
  if (!idMatch) {
    return {
      id: '',
      label: '',
      found: false
    };
  }

  const id = String(idMatch[1] || '').trim();
  if (!id) {
    return {
      id: '',
      label: '',
      found: false
    };
  }

  const labelText = section
    .slice(0, section.indexOf(id) + id.length)
    .replace(/\s+\n/g, '\n')
    .trim();

  return {
    id,
    label: labelText,
    found: true
  };
}

function normalizeClaimStrategyOrder(strategies, includeScriptId = false) {
  const base = Array.isArray(strategies) && strategies.length > 0
    ? strategies.filter(Boolean)
    : DEFAULT_CONFIG.claimStrategyOrder.slice();

  const next = [];
  for (const strategy of base) {
    if (!next.includes(strategy)) {
      next.push(strategy);
    }
  }

  if (!includeScriptId) {
    return next;
  }

  if (!next.includes('script_id')) {
    const contentIndex = next.indexOf('content_id');
    if (contentIndex >= 0) {
      next.splice(contentIndex + 1, 0, 'script_id');
    } else {
      next.unshift('script_id');
    }
  }

  return next;
}

function buildPromptFingerprint(value) {
  const normalized = normalizePromptFingerprintText(value);
  return {
    normalized,
    length: normalized.length,
    anchor: normalized.slice(0, 120),
    preview: String(value || '').replace(/\s+/g, ' ').trim().slice(0, 200),
    hash: normalized
      ? crypto.createHash('sha1').update(normalized).digest('hex')
      : ''
  };
}

function parseArgs(argv) {
  const args = {
    configPath: path.join(__dirname, 'feishu-direct.json'),
    dryRun: false,
    limit: null,
    traceId: null,
    recordId: null,
    taskNames: null,
    channel: '',
    ignoreGeneratingCount: false,
    forceAssetRead: false
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--config' && argv[i + 1]) {
      args.configPath = argv[++i];
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--limit' && argv[i + 1]) {
      args.limit = Number(argv[++i]) || null;
    } else if (arg === '--trace-id' && argv[i + 1]) {
      args.traceId = argv[++i];
    } else if (arg === '--record-id' && argv[i + 1]) {
      args.recordId = argv[++i];
    } else if (arg === '--channel' && argv[i + 1]) {
      args.channel = normalizeChannelFilter(argv[++i]);
    } else if (arg === '--task-name' && argv[i + 1]) {
      const raw = argv[++i];
      const names = raw
        .split(',')
        .map(item => item.trim())
        .filter(Boolean);
      args.taskNames = names.length > 0 ? names : null;
    } else if (arg === '--ignore-generating-count') {
      args.ignoreGeneratingCount = true;
    } else if (arg === '--force-asset-read') {
      args.forceAssetRead = true;
    }
  }

  return args;
}

function normalizeChannelFilter(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) {
    return '';
  }
  if (raw === 'imini') {
    return 'imini';
  }
  if (raw === 'jimeng' || raw === '即梦' || raw === 'default') {
    return 'jimeng';
  }
  return raw;
}

function submissionMatchesChannelFilter(submission, channelFilter = '') {
  const channel = normalizeChannelFilter(channelFilter);
  if (!channel) {
    return true;
  }
  const actual = String(submission?.channel || submission?.platform || '').trim().toLowerCase();
  if (channel === 'imini') {
    return actual === 'imini';
  }
  if (channel === 'jimeng') {
    return actual !== 'imini';
  }
  return actual === channel;
}

function loadOpenclawFeishuCredentials() {
  try {
    const openclawConfigPath = path.join(process.env.HOME || '', '.openclaw', 'openclaw.json');
    if (!fs.existsSync(openclawConfigPath)) {
      return {};
    }
    const raw = JSON.parse(fs.readFileSync(openclawConfigPath, 'utf8'));
    return raw.channels?.feishu || {};
  } catch (error) {
    return {};
  }
}

function loadConfig(configPath) {
  let raw = {};
  if (fs.existsSync(configPath)) {
    raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  }

  const feishuCredentials = loadOpenclawFeishuCredentials();
  const config = {
    ...DEFAULT_CONFIG,
    ...raw,
    appId: raw.appId || feishuCredentials.appId || DEFAULT_CONFIG.appId,
    appSecret: raw.appSecret || feishuCredentials.appSecret || DEFAULT_CONFIG.appSecret,
    fields: {
      ...DEFAULT_CONFIG.fields,
      ...(raw.fields || {})
    }
  };
  if (!raw.fields || !Object.prototype.hasOwnProperty.call(raw.fields, 'lastProcessedAt')) {
    delete config.fields.lastProcessedAt;
  }

  config.runtimeRoot = expandHome(config.runtimeRoot);
  config.machineId = sanitizeMachineId(
    raw.machineId ||
    process.env.OPENCLAW_MACHINE_ID ||
    os.hostname() ||
    `machine-${config.cdpPort || 9222}`
  );
  return config;
}

function parseFeishuBitableUrl(url) {
  const parsed = new URL(url);
  const tableId = parsed.searchParams.get('table') || '';
  const viewId = parsed.searchParams.get('view') || '';
  const wikiMatch = parsed.pathname.match(/\/wiki\/([^/?]+)/);
  const baseMatch = parsed.pathname.match(/\/base\/([^/?]+)/);
  return {
    appToken: wikiMatch?.[1] || baseMatch?.[1] || '',
    tableId,
    viewId,
    isWiki: Boolean(wikiMatch)
  };
}

async function resolveConfigTable(config, token) {
  if (!config.tableUrl) {
    return config;
  }

  const info = parseFeishuBitableUrl(config.tableUrl);
  if (!info.tableId) {
    throw new Error(`无法从飞书 URL 解析 tableId: ${config.tableUrl}`);
  }

  config.tableId = config.tableId || info.tableId;
  config.viewId = config.viewId || info.viewId;
  if (info.isWiki) {
    const data = await requestJson(
      'GET',
      `/open-apis/wiki/v2/spaces/get_node?token=${encodeURIComponent(info.appToken)}`,
      token
    );
    const node = data.node || {};
    if (node.obj_type !== 'bitable' || !node.obj_token) {
      throw new Error(`wiki 节点不是 bitable 或未返回 obj_token: ${config.tableUrl}`);
    }
    config.appToken = node.obj_token;
  } else {
    config.appToken = config.appToken || info.appToken;
  }
  return config;
}

function sanitizeMachineId(value) {
  const cleaned = String(value || 'machine')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
  return cleaned || 'machine';
}

function parseExecutionOwner(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return {
      raw: '',
      machineId: '',
      claimToken: ''
    };
  }

  const markerIndex = raw.indexOf('#');
  if (markerIndex < 0) {
    return {
      raw,
      machineId: raw,
      claimToken: raw
    };
  }

  return {
    raw,
    machineId: raw.slice(0, markerIndex).trim(),
    claimToken: raw
  };
}

function executionOwnerMatchesMachine(value, machineId) {
  const parsed = parseExecutionOwner(value);
  return Boolean(parsed.machineId) && parsed.machineId === machineId;
}

function requestJson(method, requestPath, token, body = null, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json',
      ...extraHeaders
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    if (method === 'GET') {
      headers['Cache-Control'] = 'no-cache, no-store, max-age=0';
      headers.Pragma = 'no-cache';
    }
    if (payload) {
      headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = https.request({
      hostname: 'open.feishu.cn',
      path: requestPath,
      method,
      headers
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.code === 0) {
            resolve(json.data);
            return;
          }
          reject(new Error(`飞书 API 失败 (${json.code}): ${json.msg || '未知错误'}`));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.on('error', reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function requestRawJson(method, requestPath, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json'
    };

    if (payload) {
      headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = https.request({
      hostname: 'open.feishu.cn',
      path: requestPath,
      method,
      headers
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.on('error', reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

async function getAccessToken(config) {
  if (cachedAccessToken && Date.now() < tokenExpireTime) {
    return cachedAccessToken;
  }

  const data = await requestRawJson(
    'POST',
    '/open-apis/auth/v3/tenant_access_token/internal',
    {
      app_id: config.appId,
      app_secret: config.appSecret
    }
  );

  if (data.code !== 0 || !data.tenant_access_token) {
    throw new Error(`获取飞书 Token 失败: ${data.msg || '未知错误'}`);
  }

  cachedAccessToken = data.tenant_access_token;
  tokenExpireTime = Date.now() + ((data.expire || 7200) - 60) * 1000;
  return cachedAccessToken;
}

function detectMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.mp4') return 'video/mp4';
  if (ext === '.mov') return 'video/quicktime';
  if (ext === '.m4v') return 'video/x-m4v';
  if (ext === '.avi') return 'video/x-msvideo';
  return 'application/octet-stream';
}

async function uploadFileToFeishu(config, token, filePath) {
  const fileBuffer = fs.readFileSync(filePath);
  const filename = path.basename(filePath);
  const fileSize = fileBuffer.length;
  const mimeType = detectMimeType(filePath);

  const parentTypes = Array.isArray(config.uploadParentTypes) && config.uploadParentTypes.length > 0
    ? config.uploadParentTypes
    : ['bitable_file', 'bitable_image'];

  let lastError = null;

  for (const parentType of parentTypes) {
    try {
      const boundary = '----OpenClawJimeng' + Math.random().toString(16).slice(2);
      const CRLF = '\r\n';
      const parts = [];

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="file_name"' + CRLF + CRLF);
      parts.push(filename + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="parent_type"' + CRLF + CRLF);
      parts.push(parentType + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="parent_node"' + CRLF + CRLF);
      parts.push(config.appToken + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="size"' + CRLF + CRLF);
      parts.push(String(fileSize) + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push(`Content-Disposition: form-data; name="file"; filename="${filename}"` + CRLF);
      parts.push(`Content-Type: ${mimeType}` + CRLF + CRLF);

      const headerBuffer = Buffer.from(parts.join(''));
      const footerBuffer = Buffer.from(CRLF + '--' + boundary + '--' + CRLF);
      const payload = Buffer.concat([headerBuffer, fileBuffer, footerBuffer]);

      const data = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: 'open.feishu.cn',
          path: '/open-apis/drive/v1/medias/upload_all',
          method: 'POST',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': `multipart/form-data; boundary=${boundary}`
          }
        }, res => {
          let raw = '';
          res.setEncoding('utf8');
          res.on('data', chunk => raw += chunk);
          res.on('end', () => {
            try {
              const json = JSON.parse(raw);
              if (json.code === 0) {
                resolve({
                  fileToken: json.data.file_token,
                  parentType
                });
                return;
              }
              reject(new Error(`上传失败 (${parentType}): ${json.msg || '未知错误'}`));
            } catch (error) {
              reject(new Error(`解析上传响应失败 (${parentType}): ${error.message}`));
            }
          });
        });

        req.on('error', reject);
        req.write(payload);
        req.end();
      });

      return data;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error('上传视频到飞书失败');
}

async function updateRecord(config, token, recordId, fields) {
  const payloadFields = { ...fields };
  if (config.fields?.lastProcessedAt) {
    payloadFields[config.fields.lastProcessedAt] = new Date().toISOString();
  }
  return requestJson(
    'PUT',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}`,
    token,
    {
      fields: payloadFields
    }
  );
}

function getResumeStatusFromSubmission(submission) {
  const submittedCountBefore = Math.max(0, Number(submission?.submitted_count_before) || 0);
  return submittedCountBefore > 0 ? '部分提交' : '待处理';
}

async function getRecord(config, token, recordId) {
  const data = await requestJson(
    'GET',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}?_ts=${Date.now()}`,
    token
  );
  return data.record || null;
}

async function listAllRecords(config, token) {
  let pageToken = null;
  let records = [];

  do {
    let requestPath = `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records?page_size=100&_ts=${Date.now()}`;
    if (pageToken) {
      requestPath += `&page_token=${encodeURIComponent(pageToken)}`;
    }
    if (config.viewId) {
      requestPath += `&view_id=${encodeURIComponent(config.viewId)}`;
    }

    const data = await requestJson('GET', requestPath, token);
    records = records.concat(data.items || []);
    pageToken = data.page_token || null;
  } while (pageToken);

  return records;
}

async function connectBrowser(config) {
  const protocolTimeoutMs = Math.max(
    120000,
    Number(config.protocolTimeoutMs) > 0 ? Number(config.protocolTimeoutMs) : 300000
  );
  const browser = await puppeteer.connect({
    browserURL: `http://${config.cdpHost}:${config.cdpPort}`,
    defaultViewport: null,
    timeout: 30000,
    protocolTimeout: protocolTimeoutMs
  });

  const pages = await browser.pages();
  let page = null;

  for (const candidate of pages) {
    if (await pageHasRole(candidate, DOWNLOAD_TAB_ROLE)) {
      page = candidate;
      break;
    }
  }

  if (!page) {
    page = pages.find(p => p.url().includes('/ai-tool/asset')) || null;
  }

  if (!page) page = await browser.newPage();
  await prepareAutomationPage(page);
  await markPageRole(page, DOWNLOAD_TAB_ROLE);
  return { browser, page };
}

async function pageHasRole(page, role) {
  try {
    return await page.evaluate(expectedRole => window.name === expectedRole, role);
  } catch (error) {
    return false;
  }
}

async function markPageRole(page, role) {
  try {
    await page.evaluate(nextRole => {
      window.name = nextRole;
    }, role);
  } catch (error) {
    // Ignore pages that cannot be tagged yet; URL fallback still provides separation.
  }
}

function getDownloadDir(config) {
  const dir = path.join(config.runtimeRoot, '_state', 'downloads');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function findRecoveredDownloadPath(config, submission) {
  if (!submission?.trace_id) {
    return '';
  }

  const downloadDir = getDownloadDir(config);
  const prefixes = [
    `${submission.trace_id}.`,
    `${submission.trace_id}`
  ];

  const names = fs.readdirSync(downloadDir)
    .filter(name => prefixes.some(prefix => name === submission.trace_id || name.startsWith(prefix)))
    .filter(name => !name.endsWith('.crdownload') && !name.endsWith('.tmp'))
    .sort();

  if (names.length === 0) {
    return '';
  }

  return path.join(downloadDir, names[0]);
}

function reconcileRecoveredDownloads(config, submissions) {
  return submissions.map(submission => {
    if (!submission || !['submitted', 'rendering', 'observing', 'downloaded', 'upload_failed', 'claim_failed', 'timed_out'].includes(submission.status)) {
      return submission;
    }

    if (submission.local_file_path && fs.existsSync(submission.local_file_path)) {
      return submission;
    }

    const recoveredPath = findRecoveredDownloadPath(config, submission);
    if (!recoveredPath) {
      return submission;
    }

    const updatedSubmission = updateSubmissionRecord(config, submission.trace_id, {
      status: submission.status === 'upload_failed' ? 'upload_failed' : 'downloaded',
      local_file_path: recoveredPath,
      state_updated_at: new Date().toISOString(),
      error_message: submission.error_message || ''
    });
    syncAutomationTaskFromJimeng(updatedSubmission, '识别到即梦已下载文件，自动补齐 automation DB 下载状态');
    return updatedSubmission;
  });
}

function normalizePositiveInt(value, fallback = 1) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(num));
}

function getAssetScanCachePath(config) {
  return path.join(getStateRoot(config), 'asset-scan-cache.json');
}

function loadAssetScanCache(config) {
  const filePath = getAssetScanCachePath(config);
  if (!fs.existsSync(filePath)) {
    return { entries: {} };
  }

  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return raw && typeof raw === 'object' && raw.entries ? raw : { entries: {} };
  } catch (_) {
    return { entries: {} };
  }
}

function saveAssetScanCache(config, cache) {
  const filePath = getAssetScanCachePath(config);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(cache, null, 2)}\n`);
}

function rememberAssetDetailContentId(config, detailContentId, kind) {
  const contentId = String(detailContentId || '').trim();
  if (!contentId) {
    return;
  }

  const cache = loadAssetScanCache(config);
  cache.entries[contentId] = {
    kind: kind || 'seen',
    seen_at: new Date().toISOString()
  };
  saveAssetScanCache(config, cache);
}

function loadAssetHeadSignature(config) {
  const cache = loadAssetScanCache(config);
  return Array.isArray(cache.last_head_signature) ? cache.last_head_signature : [];
}

function saveAssetHeadSignature(config, signature) {
  const cache = loadAssetScanCache(config);
  cache.last_head_signature = Array.isArray(signature) ? signature : [];
  cache.last_head_signature_at = new Date().toISOString();
  saveAssetScanCache(config, cache);
}

function loadAssetScanCursor(config) {
  const cache = loadAssetScanCache(config);
  const offset = Number(cache.cursor_offset || 0);
  return {
    offset: Number.isFinite(offset) && offset > 0 ? Math.floor(offset) : 0,
    updatedAt: cache.cursor_updated_at || ''
  };
}

function saveAssetScanCursor(config, offset) {
  const cache = loadAssetScanCache(config);
  cache.cursor_offset = Math.max(0, Math.floor(Number(offset) || 0));
  cache.cursor_updated_at = new Date().toISOString();
  saveAssetScanCache(config, cache);
}

function resetAssetScanCursor(config) {
  saveAssetScanCursor(config, 0);
}

function advanceAssetScanCursor(config, currentOffset, delta) {
  const base = Math.max(0, Math.floor(Number(currentOffset) || 0));
  const step = Math.max(1, Math.floor(Number(delta) || 0));
  const next = base + step;
  saveAssetScanCursor(config, next);
  return next;
}

function getAssetScanStrategy(config) {
  const cursorEnabled = config.assetCursorEnabled !== false;
  const rawDeepScanEveryRuns = Number(config.assetDeepScanEveryRuns);
  const deepScanEveryRuns = Number.isFinite(rawDeepScanEveryRuns)
    ? Math.max(0, Math.floor(rawDeepScanEveryRuns))
    : 4;
  const cache = loadAssetScanCache(config);
  const completedRuns = Math.max(0, Math.floor(Number(cache.scan_run_count) || 0));
  const nextRunNumber = completedRuns + 1;
  const cursor = cursorEnabled ? loadAssetScanCursor(config) : { offset: 0, updatedAt: '' };
  const useDeepCursor = cursorEnabled && deepScanEveryRuns > 1 && nextRunNumber % deepScanEveryRuns === 0;

  return {
    cursorEnabled,
    deepScanEveryRuns,
    runNumber: nextRunNumber,
    mode: useDeepCursor ? 'deep' : 'head',
    initialOffset: useDeepCursor ? cursor.offset : 0,
    savedOffset: cursor.offset,
    cursorUpdatedAt: cursor.updatedAt || ''
  };
}

function recordAssetScanStrategy(config, strategy) {
  const cache = loadAssetScanCache(config);
  cache.scan_run_count = Math.max(0, Math.floor(Number(strategy?.runNumber) || 0));
  cache.last_scan_mode = strategy?.mode || 'head';
  cache.last_scan_mode_at = new Date().toISOString();
  saveAssetScanCache(config, cache);
}

function isRecentlySeenIrrelevantDetail(config, detailContentId, claimPoolIds) {
  const contentId = String(detailContentId || '').trim();
  if (!contentId) {
    return false;
  }
  if ((claimPoolIds || new Set()).has(contentId)) {
    return false;
  }

  const cache = loadAssetScanCache(config);
  const entry = cache.entries?.[contentId];
  if (!entry || entry.kind !== 'irrelevant') {
    return false;
  }

  const ttlMs = normalizePositiveInt(config.assetRepeatIgnoreMinutes, 180) * 60 * 1000;
  const seenAt = parseTimestampMs(entry.seen_at);
  if (!seenAt) {
    return false;
  }
  return Date.now() - seenAt <= ttlMs;
}

function parseTimestampMs(value) {
  const ms = Date.parse(value || '');
  return Number.isFinite(ms) ? ms : 0;
}

function getClaimCooldownGate(config, submissions) {
  const cooldownSeconds = normalizePositiveInt(config.claimCooldownSeconds, 120);
  const cooldownMs = cooldownSeconds * 1000;
  const now = Date.now();
  let newest = null;

  for (const submission of submissions || []) {
    if (!submission || !['submitted', 'rendering', 'observing'].includes(submission.status)) {
      continue;
    }

    const observedAt = parseTimestampMs(
      submission.first_zero_queue_at ||
      submission.queue_observed_at ||
      submission.submit_time ||
      submission.state_updated_at
    );
    if (!observedAt) {
      continue;
    }

    const ageMs = now - observedAt;
    if (ageMs < 0 || ageMs > cooldownMs) {
      continue;
    }

    if (!newest || observedAt > newest.observedAt) {
      newest = {
        traceId: submission.trace_id,
        status: submission.status,
        observedAt,
        ageMs
      };
    }
  }

  if (!newest) {
    return null;
  }

  return {
    cooldownSeconds,
    remainingSeconds: Math.max(1, Math.ceil((cooldownMs - newest.ageMs) / 1000)),
    traceId: newest.traceId,
    status: newest.status
  };
}

function getPriorityClaimContentIdSet(config) {
  return new Set((Array.isArray(config.priorityClaimContentIds) ? config.priorityClaimContentIds : [])
    .map(item => String(item || '').trim())
    .filter(Boolean));
}

function sortClaimCandidates(items, config = DEFAULT_CONFIG) {
  const priorityIds = getPriorityClaimContentIdSet(config);
  return (items || [])
    .slice()
    .sort((a, b) => {
      const aPriority = priorityIds.has(String(a.content_id || '').trim()) ? 0 : 1;
      const bPriority = priorityIds.has(String(b.content_id || '').trim()) ? 0 : 1;
      if (aPriority !== bPriority) {
        return aPriority - bPriority;
      }

      const statusRank = status => {
        if (status === 'rendering') return 0;
        if (status === 'observing') return 1;
        if (status === 'submitted') return 2;
        if (status === 'claim_failed') return 3;
        if (status === 'timed_out') return 4;
        return 5;
      };

      const rankDiff = statusRank(a.status) - statusRank(b.status);
      if (rankDiff !== 0) {
        return rankDiff;
      }

      const aTime = parseTimestampMs(a.submit_time || a.state_updated_at || a.last_claim_checked_at);
      const bTime = parseTimestampMs(b.submit_time || b.state_updated_at || b.last_claim_checked_at);
      return aTime - bTime;
    });
}

function canRetryTimedOutClaim(submission) {
  if (!submission || submission.status !== 'timed_out') {
    return false;
  }

  const channel = String(submission.channel || submission.platform || '').trim().toLowerCase();
  const confirmedBy = String(submission.submit_confirmed_by || '').trim();
  if (
    channel === 'imini' &&
    ['submitted', 'imini_create_success', 'submitted_visible_card'].includes(confirmedBy)
  ) {
    return true;
  }

  if (submission.queue_observed) {
    return true;
  }

  return [
    'queue_growth_after_error_deferred',
    'queue_growth_after_error',
    'queue_growth_after_generate_error_notice',
    'queue_growth+credits_changed',
    'queue_growth+submit_success',
    'queue_growth+credits_changed_but_unstable'
  ].includes(confirmedBy);
}

function pickRetryableClaimSubmission(items) {
  if (!items || items.length === 0) {
    return null;
  }

  return items
    .slice()
    .sort((a, b) => {
      const attemptDiff = (a.claim_attempts || 0) - (b.claim_attempts || 0);
      if (attemptDiff !== 0) {
        return attemptDiff;
      }

      const aTime = Date.parse(a.last_claim_checked_at || a.claim_failed_at || a.first_zero_queue_at || a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.last_claim_checked_at || b.claim_failed_at || b.first_zero_queue_at || b.state_updated_at || b.submit_time || 0) || 0;
      return aTime - bTime;
    })[0];
}

function noteZeroQueueWindow(config, submission) {
  if (!submission || ['submitted', 'rendering', 'observing', 'claim_failed'].includes(submission.status) === false) {
    return submission;
  }

  if (submission.first_zero_queue_at) {
    return submission;
  }

  return updateSubmissionRecord(config, submission.trace_id, {
    first_zero_queue_at: new Date().toISOString(),
    state_updated_at: new Date().toISOString()
  });
}

function shouldPromoteClaimFailure(config, submission) {
  if (!submission || submission.status === 'claim_failed') {
    return false;
  }

  const maxClaimAttempts = normalizePositiveInt(config.maxClaimAttempts, 5);
  const claimGraceMinutes = normalizePositiveInt(config.claimGraceMinutes, 20);
  const attempts = Number(submission.claim_attempts || 0);
  if (attempts >= maxClaimAttempts) {
    return true;
  }

  const firstZeroAt = Date.parse(submission.first_zero_queue_at || '');
  if (!Number.isFinite(firstZeroAt)) {
    return false;
  }

  return Date.now() - firstZeroAt >= claimGraceMinutes * 60 * 1000;
}

async function recordClaimMiss(config, token, submission, reason) {
  const nextAttempts = Number(submission.claim_attempts || 0) + 1;
  const now = new Date().toISOString();
  const patch = {
    claim_attempts: nextAttempts,
    last_claim_reason: reason,
    last_claim_checked_at: now,
    state_updated_at: now
  };

  const firstZeroPatch = submission.first_zero_queue_at ? {} : { first_zero_queue_at: now };
  let updated = updateSubmissionRecord(config, submission.trace_id, {
    ...patch,
    ...firstZeroPatch
  });

  if (!shouldPromoteClaimFailure(config, updated)) {
    return updated;
  }

  updated = updateSubmissionRecord(config, submission.trace_id, {
    ...patch,
    ...firstZeroPatch,
    status: 'claim_failed',
    claim_failed_at: now,
    state_updated_at: now,
    error_message: reason
  });
  syncAutomationTaskFromJimeng(updated, '即梦结果认领多次未命中，已转 claim_failed');

  await updateRecord(config, token, updated.record_id, {
    [config.fields.resultSyncStatus]: 'claim_failed',
    [config.fields.result]:
      `资产页连续未认领到结果，已转为低优先级补回写，不再阻塞后续任务`,
    [config.fields.errorMessage]: reason
  }).catch(error => {
    console.log(`⚠️  更新飞书认领失败状态失败: ${error.message}`);
  });

  console.log(`⚠️  ${updated.trace_id} 已转入 claim_failed，不再阻塞后续提交`);
  return updated;
}

function inferTimedOutOutcome(submission, fallbackReason) {
  const queueObserved = Boolean(submission?.queue_observed);
  const postSubmitGeneratingCount = Number(submission?.post_submit_generating_count || 0);
  const confirmedBy = String(submission?.submit_confirmed_by || '');

  if (queueObserved || postSubmitGeneratingCount > 0 || /queue_growth/.test(confirmedBy)) {
    return {
      resultSyncStatus: 'review_failed_or_missing_asset',
      reason: '已确认进入生成队列，但队列结束后长时间未在资产页找到可下载视频，疑似平台审核未通过或平台未产出资产',
      result: '已提交到即梦生成队列，但未产出可下载视频；疑似平台审核未通过或平台未生成资产。该记录不再阻塞后续提单，download-only 仍会低优先级尝试补认领'
    };
  }

  return {
    resultSyncStatus: 'timed_out',
    reason: fallbackReason,
    result: '长时间未完成闭环，已自动超时隔离，不再阻塞其他任务'
  };
}

async function isolateTimedOutSubmission(config, token, submission, reason) {
  const now = new Date().toISOString();
  const outcome = inferTimedOutOutcome(submission, reason);
  const updated = updateSubmissionRecord(config, submission.trace_id, {
    status: 'timed_out',
    error_message: outcome.reason,
    last_claim_reason: outcome.reason,
    result_sync_status: outcome.resultSyncStatus,
    state_updated_at: now,
    timed_out_at: now
  });
  syncAutomationTaskFromJimeng(updated, '即梦结果长时间未闭环，已转 timed_out');

  await updateRecord(config, token, updated.record_id, {
    [config.fields.resultSyncStatus]: outcome.resultSyncStatus,
    [config.fields.result]: outcome.result,
    [config.fields.errorMessage]: outcome.reason
  }).catch(error => {
    console.log(`⚠️  更新飞书超时状态失败: ${error.message}`);
  });

  console.log(`⏰ ${updated.trace_id} 已超时隔离，不再阻塞后续任务`);
  return updated;
}

async function recoverStaleSubmittingSubmission(config, token, submission, recordState = null) {
  const now = new Date().toISOString();
  const resumeStatus = getResumeStatusFromSubmission(submission);
  const reason = '提交过程长时间未完成确认，已自动恢复为待重试';
  const updated = updateSubmissionRecord(config, submission.trace_id, {
    status: 'retry_pending',
    submit_confirmed_by: submission.submit_confirmed_by || 'submit_interrupted_recovered',
    submit_confirmation_note: submission.submit_confirmation_note || reason,
    error_message: submission.error_message || reason,
    state_updated_at: now
  });
  syncAutomationTaskFromJimeng(updated, '即梦提交流程长时间停留在 submitting，已恢复待重试');

  if (updated.record_id) {
    const currentStatus = String(recordState?.status || '').trim();
    const fields = {
      [config.fields.result]: `检测到提交流程长时间停留在 submitting，已自动恢复为“${resumeStatus}”，等待后续重试`,
      [config.fields.resultSyncStatus]: 'pending_retry',
      [config.fields.errorMessage]: submission.error_message || reason
    };

    if (!currentStatus || currentStatus === '处理中') {
      fields[config.statusField] = resumeStatus;
    }

    await updateRecord(config, token, updated.record_id, fields).catch(error => {
      console.log(`⚠️  更新飞书 stale submitting 恢复状态失败: ${error.message}`);
    });
  }

  console.log(`♻️ ${updated.trace_id} 卡在 submitting，已恢复为 retry_pending`);
  return updated;
}

async function isolateStaleSubmissions(config, token, submissions, currentGeneratingCount, recordStateMap = null) {
  const now = Date.now();
  const submittingTimeoutMs = normalizePositiveInt(config.submittingTimeoutMinutes, 30) * 60 * 1000;
  const submissionTimeoutMs = normalizePositiveInt(config.submissionTimeoutMinutes, 480) * 60 * 1000;
  const results = [];

  for (const submission of submissions || []) {
    if (!submission || ['timed_out', 'uploaded', 'failed', 'blocked', 'claim_failed'].includes(submission.status)) {
      continue;
    }

    const baseTime = parseTimestampMs(submission.submit_time || submission.state_updated_at);
    if (!baseTime) {
      continue;
    }

    if (submission.status === 'submitting' && now - baseTime >= submittingTimeoutMs) {
      const updated = await recoverStaleSubmittingSubmission(
        config,
        token,
        submission,
        recordStateMap?.get(submission.record_id) || null
      );
      results.push(updated);
      continue;
    }

    if (currentGeneratingCount === 0 && ['submitted', 'rendering', 'observing'].includes(submission.status) && now - baseTime >= submissionTimeoutMs) {
      const updated = await isolateTimedOutSubmission(
        config,
        token,
        submission,
        '长时间未认领到结果，已自动超时隔离'
      );
      results.push(updated);
    }
  }

  return results;
}

async function openAssetPage(page, config) {
  const assetUrl = config.assetUrl || 'https://jimeng.jianying.com/ai-tool/asset?workspace=0';
  await prepareAutomationPage(page);
  try {
    await page.goto(assetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 45000
    });
  } catch (error) {
    const message = error?.message || String(error);
    if (!message.includes('Navigation timeout')) {
      throw error;
    }
    console.log('⏱️  资产页导航等待超时，继续检查当前页面内容...');
  }
  await sleep(1500);
}

async function withAssetStepTimeout(promise, timeoutMs, label) {
  let timer = null;
  const safeTimeoutMs = Math.max(5000, Number(timeoutMs) || 30000);
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label}超时 (${safeTimeoutMs}ms)`)), safeTimeoutMs);
      })
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function readAssetPageSnapshot(page) {
  return await page.evaluate(() => {
    const title = String(document.title || '').trim();
    const bodyText = String(document.body?.innerText || '')
      .replace(/\r/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    return {
      title,
      bodyText,
      url: String(location.href || '')
    };
  });
}

function describeAssetPageHealth(snapshot) {
  const title = String(snapshot?.title || '');
  const bodyText = String(snapshot?.bodyText || '');
  const url = String(snapshot?.url || '');
  const normalizedTitle = title.toLowerCase();
  const normalizedBody = bodyText.toLowerCase();

  if (url.includes('/404') || normalizedTitle.includes('404')) {
    return { ok: false, reason: `资产页进入了 404 页面 (${url || title || 'unknown'})` };
  }

  if (
    /页面不存在|访问的页面不存在|内容不存在|页面丢失|无法访问|请稍后重试/.test(bodyText) ||
    normalizedBody.includes('404')
  ) {
    return {
      ok: false,
      reason: `资产页正文出现异常提示: ${(bodyText || title).slice(0, 80)}`
    };
  }

  if (bodyText.length < 20) {
    return { ok: false, reason: '资产页正文为空或过短，疑似空壳页' };
  }

  return { ok: true, reason: '' };
}

async function ensureHealthyAssetPage(page, config, maxAttempts = 3) {
  let lastIssue = '资产页健康检查未通过';

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    if (attempt === 1) {
      console.log('🔎 打开资产页...');
      await withAssetStepTimeout(openAssetPage(page, config), 60000, '打开资产页');
    } else {
      console.log(`🔄 资产页健康检查失败，执行第 ${attempt} 次自愈重试...`);
      try {
        await page.reload({
          waitUntil: 'domcontentloaded',
          timeout: 45000
        });
      } catch (error) {
        const message = error?.message || String(error);
        if (!message.includes('Navigation timeout')) {
          throw error;
        }
        console.log('⏱️  资产页刷新等待超时，继续检查当前页面内容...');
      }
      await sleep(1500);
      const snapshotAfterReload = await readAssetPageSnapshot(page);
      const healthAfterReload = describeAssetPageHealth(snapshotAfterReload);
      if (!healthAfterReload.ok) {
        await withAssetStepTimeout(openAssetPage(page, config), 60000, '重新打开资产页');
      }
    }

    console.log('🔎 读取资产页状态...');
    const snapshot = await withAssetStepTimeout(readAssetPageSnapshot(page), 15000, '读取资产页状态');
    const health = describeAssetPageHealth(snapshot);
    if (health.ok) {
      console.log('✅ 资产页健康检查通过');
      return;
    }

    lastIssue = health.reason;
    console.log(`⚠️  ${health.reason}`);
  }

  throw new Error(`资产页健康检查失败：${lastIssue}`);
}

function isRecoverableAssetContextError(error) {
  const message = String(error?.message || error || '');
  return (
    message.includes('Runtime.callFunctionOn timed out') ||
    message.includes('Execution context was destroyed') ||
    message.includes('Cannot find context with specified id') ||
    message.includes('Attempted to use detached Frame') ||
    message.includes('Inspected target navigated or closed') ||
    message.includes('Most likely the page has been closed') ||
    message.includes('Protocol error (Runtime.callFunctionOn)') ||
    message.includes('Protocol error (DOM.describeNode)')
  );
}

async function recoverAssetPageContext(page, config, reason = '') {
  const suffix = reason ? `（${reason}）` : '';
  console.log(`🔄 资产页上下文丢失，执行页面恢复${suffix}`);
  await ensureHealthyAssetPage(page, config, 2);
  await ensureVideoAssetTab(page);
}

async function withRecoveredAssetContext(page, config, label, fn, maxAttempts = 2) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      if (!isRecoverableAssetContextError(error) || attempt >= maxAttempts) {
        throw error;
      }
      const message = String(error?.message || error || '');
      console.log(`⚠️  ${label} 遇到可恢复导航异常，第 ${attempt + 1} 次重试：${message}`);
      await recoverAssetPageContext(page, config, label);
      await sleep(1200);
    }
  }
  return await fn();
}

async function clickVisibleText(page, labels, timeoutMs = 15000) {
  const targetLabels = Array.isArray(labels) ? labels : [labels];
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const clicked = await page.evaluate((texts) => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };
      const pickClickable = el => {
        let current = el;
        for (let depth = 0; current && depth < 6; depth += 1) {
          const style = getComputedStyle(current);
          const role = current.getAttribute('role') || '';
          if (
            current.tagName === 'BUTTON' ||
            current.tagName === 'A' ||
            role === 'button' ||
            role === 'tab' ||
            style.cursor === 'pointer' ||
            current.onclick
          ) {
            return current;
          }
          current = current.parentElement;
        }
        return el;
      };

      const elements = Array.from(document.querySelectorAll('button, [role="tab"], a, div, span'));
      for (const label of texts) {
        const candidate = elements.find(el => {
          const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
          return text === label && isVisible(el);
        });
        if (candidate) {
          pickClickable(candidate).click();
          return label;
        }
      }
      return '';
    }, targetLabels);

    if (clicked) {
      await sleep(1200);
      return clicked;
    }

    await sleep(500);
  }

  return '';
}

async function hasVisibleText(page, labels) {
  const targetLabels = Array.isArray(labels) ? labels : [labels];
  return await page.evaluate((texts) => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const elements = Array.from(document.querySelectorAll('button, [role="tab"], a, div, span'));
    return texts.some(label =>
      elements.some(el => {
        const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
        return text === label && isVisible(el);
      })
    );
  }, targetLabels);
}

async function ensureVideoAssetTab(page) {
  let lastError = '资产页未找到“视频”页签';

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    console.log(`🔎 检查资产页视频 Tab... (${attempt}/3)`);
    if (await withAssetStepTimeout(hasVisibleText(page, ['所有视频', '我的收藏']), 10000, '检查资产页视频二级页签')) {
      await withAssetStepTimeout(clickVisibleText(page, ['所有视频'], 3000), 8000, '点击所有视频页签');
      await sleep(800);
      console.log('✅ 已进入资产页视频列表');
      return;
    }

    const videoTab = await withAssetStepTimeout(clickVisibleText(page, ['视频']), 20000, '点击视频页签');
    if (videoTab) {
      const allVideoTab = await withAssetStepTimeout(clickVisibleText(page, ['所有视频'], 4000), 10000, '点击所有视频页签');
      if (allVideoTab || await withAssetStepTimeout(hasVisibleText(page, ['所有视频', '我的收藏']), 10000, '检查所有视频页签')) {
        await sleep(800);
        console.log('✅ 已进入资产页视频列表');
        return;
      }
      lastError = '资产页已进入视频区域，但未找到“所有视频”页签';
    } else {
      lastError = '资产页未找到“视频”页签';
    }

    if (attempt < 3) {
      console.log(`⚠️  ${lastError}，执行第 ${attempt + 1} 次资产页恢复...`);
      await ensureHealthyAssetPage(page, page.__assetConfigForRecovery || DEFAULT_CONFIG, 2);
    }
  }

  throw new Error(lastError);
}

async function closeVideoDetail(page, timeoutMs = 4000) {
  if (!await isVideoDetailOpen(page)) {
    return true;
  }

  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    await page.keyboard.press('Escape').catch(() => {});
    await sleep(500);
    if (!await isVideoDetailOpen(page)) {
      return true;
    }
  }

  return false;
}

async function scrollAssetViewport(page, stepPx) {
  const result = await page.evaluate((step) => {
    const pickScrollable = () => {
      const elements = Array.from(document.querySelectorAll('main, section, div'));
      const viewportH = window.innerHeight || document.documentElement.clientHeight || 0;

      const candidates = elements
        .map(el => {
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          const scrollable = el.scrollHeight - el.clientHeight > 80;
          const visible = rect.width > 0 && rect.height > 0;
          const canScroll = /(auto|scroll)/.test(style.overflowY || '') || scrollable;
          return {
            el,
            rect,
            visible,
            canScroll,
            scrollHeight: el.scrollHeight,
            clientHeight: el.clientHeight,
            area: rect.width * rect.height
          };
        })
        .filter(item =>
          item.visible &&
          item.canScroll &&
          item.clientHeight > 200 &&
          item.scrollHeight - item.clientHeight > 80 &&
          item.rect.top < viewportH &&
          item.rect.bottom > 0
        )
        .sort((a, b) => b.area - a.area);

      return candidates[0]?.el || null;
    };

    const target = pickScrollable();
    if (target) {
      const before = target.scrollTop || 0;
      target.scrollBy({ top: step, left: 0, behavior: 'instant' });
      const after = target.scrollTop || 0;
      return {
        before,
        after,
        changed: Math.abs(after - before) > 8,
        target: 'container'
      };
    }

    const before = window.scrollY || 0;
    window.scrollBy({ top: step, left: 0, behavior: 'instant' });
    const after = window.scrollY || 0;
    return {
      before,
      after,
      changed: Math.abs(after - before) > 8,
      target: 'window'
    };
  }, stepPx);

  await sleep(1200);
  return result;
}

async function isVideoDetailOpen(page) {
  return await page.evaluate(() => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const visibleTexts = Array.from(document.querySelectorAll('button, div, span'))
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean);

    return visibleTexts.some(text => text.includes('下载')) &&
      visibleTexts.some(text => text.includes('视频提示词') || text.includes('重新编辑') || text.includes('再次生成'));
  });
}

async function listVideoCardTargets(page) {
  return await page.evaluate(() => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const pickScrollable = () => {
      const elements = Array.from(document.querySelectorAll('main, section, div'));
      const viewportH = window.innerHeight || document.documentElement.clientHeight || 0;

      const candidates = elements
        .map(el => {
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          const scrollable = el.scrollHeight - el.clientHeight > 80;
          const visible = rect.width > 0 && rect.height > 0;
          const canScroll = /(auto|scroll)/.test(style.overflowY || '') || scrollable;
          return {
            el,
            rect,
            visible,
            canScroll,
            scrollHeight: el.scrollHeight,
            clientHeight: el.clientHeight,
            area: rect.width * rect.height
          };
        })
        .filter(item =>
          item.visible &&
          item.canScroll &&
          item.clientHeight > 200 &&
          item.scrollHeight - item.clientHeight > 80 &&
          item.rect.top < viewportH &&
          item.rect.bottom > 0
        )
        .sort((a, b) => b.area - a.area);

      return candidates[0]?.el || null;
    };

    const pickClickable = node => {
      let current = node;
      for (let depth = 0; current && depth < 6; depth += 1) {
        const style = getComputedStyle(current);
        const text = (current.textContent || '').replace(/\s+/g, ' ').trim();
        if (
          style.cursor === 'pointer' ||
          current.tagName === 'BUTTON' ||
          current.tagName === 'A' ||
          current.getAttribute('role') === 'button' ||
          current.getAttribute('role') === 'link' ||
          current.onclick ||
          text.includes('下载')
        ) {
          return current;
        }
        current = current.parentElement;
      }
      return node;
    };

    const isInsideAssetContent = el => {
      let current = el;
      for (let depth = 0; current && current !== document.body && depth < 10; depth += 1) {
        const tag = current.tagName;
        const role = current.getAttribute('role') || '';
        const className = String(current.className || '');
        const text = (current.textContent || '').replace(/\s+/g, ' ').trim();
        if (tag === 'ASIDE' || tag === 'NAV' || tag === 'HEADER' || role === 'navigation') {
          return false;
        }
        if (/(side|sidebar|nav|menu|header)/i.test(className)) {
          return false;
        }
        if (/灵感 生成 资产 图片 视频 无限画布|批量操作 同步到剪映/.test(text) && text.length < 120) {
          return false;
        }
        current = current.parentElement;
      }
      return true;
    };

    const hasAssetCardSignal = (media, clickable) => {
      const mediaRect = media.getBoundingClientRect();
      if (mediaRect.top < 120 || mediaRect.left < 80) {
        return false;
      }
      const mediaSrc = media.currentSrc || media.src || '';
      if (media.tagName === 'IMG' && !mediaSrc) {
        return false;
      }

      let current = clickable || media;
      let combinedText = '';
      for (let depth = 0; current && current !== document.body && depth < 7; depth += 1) {
        combinedText += ` ${(current.textContent || '').replace(/\s+/g, ' ').trim()}`;
        current = current.parentElement;
      }
      if (/所有视频 我的收藏|批量操作|同步到剪映/.test(combinedText) && combinedText.length < 160) {
        return false;
      }
      return (
        media.tagName === 'VIDEO' ||
        /\.(mp4|webm|mov|m4v)(\?|$)/i.test(mediaSrc) ||
        /\b\d{1,2}:\d{2}\b|视频提示词|重新生成|下载|Seedance|Kling|创建时间/.test(combinedText)
      );
    };

    const scrollTarget = pickScrollable();
    const scrollTop = scrollTarget ? (scrollTarget.scrollTop || 0) : (window.scrollY || 0);
    const dedupe = new Set();
    return Array.from(document.querySelectorAll('video, img, canvas'))
      .filter(isVisible)
      .map(el => ({ media: el, target: pickClickable(el) }))
      .filter(({ media, target }) => isInsideAssetContent(target) && hasAssetCardSignal(media, target))
      .map(({ media, target }) => {
        const rect = target.getBoundingClientRect();
        const absoluteTop = rect.top + scrollTop;
        const absoluteBottom = rect.bottom + scrollTop;
        return {
          x: rect.x + rect.width / 2,
          y: rect.y + rect.height / 2,
          width: rect.width,
          height: rect.height,
          absoluteTop,
          absoluteBottom,
          absoluteY: rect.y + rect.height / 2 + scrollTop,
          sourceTag: media.tagName,
          sourceText: (target.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 120)
        };
      })
      .filter(item =>
        item.width >= 120 &&
        item.height >= 120 &&
        item.x >= 80 &&
        item.y >= 80
      )
      .sort((a, b) => a.y - b.y || a.x - b.x)
      .filter(item => {
        const key = `${Math.round(item.x / 20)}:${Math.round(item.absoluteY / 20)}`;
        if (dedupe.has(key)) {
          return false;
        }
        dedupe.add(key);
        return true;
      })
      .slice(0, 40);
  });
}

function estimateAssetWindowScrollStep(candidates, fallbackStepPx) {
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return normalizePositiveInt(fallbackStepPx, 1200);
  }

  const top = Math.min(...candidates.map(item => item.y - item.height / 2));
  const bottom = Math.max(...candidates.map(item => item.y + item.height / 2));
  const span = Math.max(200, Math.round(bottom - top));
  const fallback = normalizePositiveInt(fallbackStepPx, 1200);
  // Keep about one third overlap between windows so the next batch stays
  // visually connected to the previous one instead of jumping too far down.
  const conservativeStep = Math.round(span * 0.68);
  return Math.max(220, Math.min(fallback, conservativeStep));
}

function markPendingQueueObserved(config, pending, currentGeneratingCount) {
  if (!currentGeneratingCount || currentGeneratingCount <= 0) {
    return pending;
  }

  return pending.map(item => {
    if (item.queue_observed) {
      return item;
    }
    return updateSubmissionRecord(config, item.trace_id, {
      queue_observed: true,
      queue_observed_at: new Date().toISOString(),
      observed_generating_count: currentGeneratingCount,
      status: item.status === 'submitted' ? 'rendering' : item.status,
      state_updated_at: new Date().toISOString()
    });
  });
}

async function extractVideoDetailPrompt(page, timeoutMs = 10000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const extracted = await page.evaluate(() => {
      const bodyText = (document.body?.innerText || '').replace(/\r/g, '');
      const marker = '视频提示词';
      const start = bodyText.indexOf(marker);
      if (start < 0) {
        return '';
      }

      const tail = bodyText.slice(start + marker.length).trim();
      const stopMarkers = [
        '\n编辑',
        '\n更多',
        '\n对口型',
        '\nAI音效',
        '\nAI配乐',
        '\n补帧',
        '\n提升分辨率',
        '\n重新编辑',
        '\n再次生成',
        '\n在生成页定位'
      ];

      let end = tail.length;
      for (const markerText of stopMarkers) {
        const index = tail.indexOf(markerText);
        if (index >= 0 && index < end) {
          end = index;
        }
      }

      return tail.slice(0, end).replace(/\s+/g, ' ').trim();
    });

    if (extracted) {
      return extracted;
    }

    await sleep(500);
  }

  return '';
}

function isCompatibleMetadataId(detailId, expectedId) {
  const detail = String(detailId || '').trim();
  const expected = String(expectedId || '').trim();
  if (!detail || !expected) return false;
  if (detail === expected) return true;
  return detail.length >= 24 && expected.endsWith(detail);
}

function describePromptMatch(submission, detailPrompt) {
  const detailFingerprint = buildPromptFingerprint(detailPrompt);
  const detailContentIdMeta = parseContentIdMetadata(detailPrompt, submission.content_id_label_name || DEFAULT_CONFIG.contentIdLabel);
  const detailScriptIdMeta = parseScriptIdMetadata(detailPrompt);
  const contentIdEnabled = submission.content_id && submission.enable_content_id_claim !== false;
  const submissionScriptIdMeta = parseScriptIdMetadata(submission.prompt_preview || submission.prompt_anchor || '');
  const submissionHash = submission.prompt_hash || '';
  const submissionAnchor = submission.prompt_anchor || '';
  const submissionScriptId = String(submission.script_id || submissionScriptIdMeta.id || '').trim();
  const claimStrategies = normalizeClaimStrategyOrder(
    Array.isArray(submission.claim_strategy_order) && submission.claim_strategy_order.length > 0
      ? submission.claim_strategy_order
      : DEFAULT_CONFIG.claimStrategyOrder,
    Boolean(submissionScriptId)
  );

  if (!detailFingerprint.normalized) {
    return { ok: false, reason: '详情页未读取到视频提示词' };
  }

  if (!contentIdEnabled && !submissionScriptId && !submissionHash && !submissionAnchor) {
    return { ok: false, reason: 'trace 缺少提示词指纹，拒绝自动认领' };
  }

  for (const strategy of claimStrategies) {
    if (strategy === 'content_id' && contentIdEnabled) {
      if (!detailContentIdMeta.found) {
        continue;
      }
      if (isCompatibleMetadataId(detailContentIdMeta.id, submission.content_id)) {
        return {
          ok: true,
          by: detailContentIdMeta.id === submission.content_id ? 'content_id' : 'content_id_suffix',
          detailContentId: detailContentIdMeta.id
        };
      }
      return {
        ok: false,
        reason: `详情页内容ID不匹配 (detail=${detailContentIdMeta.id}, trace=${submission.content_id})`,
        detailPreview: detailFingerprint.preview,
        detailContentId: detailContentIdMeta.id
      };
    }

    if (strategy === 'prompt_hash' && submissionHash && detailFingerprint.hash === submissionHash) {
      return { ok: true, by: 'prompt_hash' };
    }

    if (strategy === 'script_id' && submissionScriptId) {
      if (!detailScriptIdMeta.found) {
        continue;
      }
      if (isCompatibleMetadataId(detailScriptIdMeta.id, submissionScriptId)) {
        return {
          ok: true,
          by: detailScriptIdMeta.id === submissionScriptId ? 'script_id' : 'script_id_suffix',
          detailScriptId: detailScriptIdMeta.id
        };
      }
      return {
        ok: false,
        reason: `详情页脚本ID不匹配 (detail=${detailScriptIdMeta.id}, trace=${submissionScriptId})`,
        detailPreview: detailFingerprint.preview,
        detailScriptId: detailScriptIdMeta.id
      };
    }

    if (strategy === 'prompt_anchor' && submissionAnchor && detailFingerprint.normalized.includes(submissionAnchor)) {
      return { ok: true, by: 'prompt_anchor' };
    }
  }

  return {
    ok: false,
    reason: `详情页提示词与 trace 不匹配`,
    detailPreview: detailFingerprint.preview
  };
}

function orderClaimPool(submissions, preferredTraceId, config = DEFAULT_CONFIG) {
  const priorityIds = getPriorityClaimContentIdSet(config);
  return (submissions || [])
    .slice()
    .sort((a, b) => {
      const aPreferred = a.trace_id === preferredTraceId ? 0 : 1;
      const bPreferred = b.trace_id === preferredTraceId ? 0 : 1;
      if (aPreferred !== bPreferred) {
        return aPreferred - bPreferred;
      }

      const aPriority = priorityIds.has(String(a.content_id || '').trim()) ? 0 : 1;
      const bPriority = priorityIds.has(String(b.content_id || '').trim()) ? 0 : 1;
      if (aPriority !== bPriority) {
        return aPriority - bPriority;
      }

      const statusRank = status => {
        if (status === 'rendering') return 0;
        if (status === 'observing') return 1;
        if (status === 'submitted') return 2;
        if (status === 'claim_failed') return 3;
        return 3;
      };

      const rankDiff = statusRank(a.status) - statusRank(b.status);
      if (rankDiff !== 0) {
        return rankDiff;
      }

      const aAttempts = Number(a.claim_attempts || 0);
      const bAttempts = Number(b.claim_attempts || 0);
      if (aAttempts !== bAttempts) {
        return aAttempts - bAttempts;
      }

      return parseTimestampMs(a.submit_time || a.state_updated_at) - parseTimestampMs(b.submit_time || b.state_updated_at);
    });
}

function findMatchingSubmissionForDetail(submissions, detailPrompt, preferredTraceId, config = DEFAULT_CONFIG) {
  const ordered = orderClaimPool(submissions, preferredTraceId, config);
  for (const submission of ordered) {
    const match = describePromptMatch(submission, detailPrompt);
    if (match.ok) {
      return {
        submission,
        match
      };
    }
  }

  const detailContentIdMeta = parseContentIdMetadata(detailPrompt, DEFAULT_CONFIG.contentIdLabel);
  return {
    submission: null,
    match: null,
    detailContentId: detailContentIdMeta.id || '',
    detailPreview: buildPromptFingerprint(detailPrompt).preview
  };
}

async function ensureSubmissionPromptFingerprint(config, token, submission) {
  const needsFingerprint = !submission.prompt_hash && !submission.prompt_anchor;
  const needsContentId = config.enableContentIdClaim !== false && !submission.content_id && submission.content_id_found_in_prompt !== false;
  const needsScriptId = !String(submission.script_id || '').trim();
  if (!needsFingerprint && !needsContentId && !needsScriptId) {
    return submission;
  }

  if (!submission.record_id || !config.fields.prompt) {
    return submission;
  }

  const record = await getRecord(config, token, submission.record_id).catch(() => null);
  const promptValue = record?.fields?.[config.fields.prompt];
  const promptText = Array.isArray(promptValue)
    ? promptValue.map(item => item?.text || item?.name || '').filter(Boolean).join('\n')
    : String(promptValue || '');

  const fingerprint = buildPromptFingerprint(promptText);
  const contentIdMeta = parseContentIdMetadata(promptText, config.contentIdLabel);
  const scriptIdMeta = parseScriptIdMetadata(promptText);
  if (!fingerprint.hash && !contentIdMeta.found && !scriptIdMeta.found) {
    return submission;
  }

  const next = updateSubmissionRecord(config, submission.trace_id, {
    prompt_length: fingerprint.length,
    prompt_preview: fingerprint.preview,
    prompt_anchor: fingerprint.anchor,
    prompt_hash: fingerprint.hash,
    script_id: scriptIdMeta.id,
    content_id: contentIdMeta.id,
    content_id_label: contentIdMeta.label,
    content_id_found_in_prompt: contentIdMeta.found,
    content_id_label_name: config.contentIdLabel || DEFAULT_CONFIG.contentIdLabel,
    claim_strategy_order: normalizeClaimStrategyOrder(
      Array.isArray(config.claimStrategyOrder) ? config.claimStrategyOrder : DEFAULT_CONFIG.claimStrategyOrder,
      scriptIdMeta.found
    ),
    enable_content_id_claim: config.enableContentIdClaim !== false,
    state_updated_at: new Date().toISOString()
  });
  return next;
}

async function openMatchingVideoDetail(page, config, token, submission, claimPool = null) {
  const poolSource = Array.isArray(claimPool) && claimPool.length > 0 ? claimPool : [submission];
  const enrichedPool = [];
  for (const item of poolSource) {
    enrichedPool.push(await ensureSubmissionPromptFingerprint(config, token, item));
  }
  const claimPoolIds = new Set(enrichedPool.map(item => item.content_id).filter(Boolean));

  const candidateLimit = Math.max(1, Math.floor(Number(config.maxAssetCandidates) || 10));
  const scanBatches = normalizePositiveInt(config.assetScanBatches, 5);
  const scrollStepPx = normalizePositiveInt(config.assetScrollStepPx, 1200);
  const headSignatureSize = normalizePositiveInt(config.assetHeadSignatureSize, 4);
  const cursorOverlapCount = normalizePositiveInt(config.assetCursorOverlapCount, 1);
  const continuationOverlapPx = normalizePositiveInt(config.assetWindowContinuationOverlapPx, 80);
  const scanStrategy = getAssetScanStrategy(config);
  const cursorEnabled = scanStrategy.cursorEnabled;
  const initialCursor = { offset: scanStrategy.initialOffset, updatedAt: scanStrategy.cursorUpdatedAt };
  const seenKeys = new Set();
  const seenDetailContentIds = new Set();
  let scanned = 0;
  let currentHeadSignature = [];
  let streamIndex = 0;
  let skippedByCursor = 0;
  let openedCount = 0;
  let bufferedCandidates = [];
  let bufferIndex = 0;
  let lastWindowCandidates = [];
  let continuationThresholdTop = Number.NEGATIVE_INFINITY;
  let emptyInitialCandidateReads = 0;

  page.__assetConfigForRecovery = config;
  recordAssetScanStrategy(config, scanStrategy);
  if (scanStrategy.mode === 'deep') {
    console.log(`🧭 资产页深扫模式：第 ${scanStrategy.runNumber} 轮，从游标 ${scanStrategy.initialOffset} 继续往后补老数据`);
  } else {
    console.log(`🧭 资产页头部优先模式：第 ${scanStrategy.runNumber} 轮，从头开始优先检查新生成数据`);
  }
  await ensureHealthyAssetPage(page, config);
  await ensureVideoAssetTab(page);

  for (let batchIndex = 0; batchIndex < scanBatches; batchIndex++) {
    let windowCandidates = [];
    let batchSkippedByCursor = 0;
    let noProgressCandidateReads = 0;

    while (windowCandidates.length < candidateLimit) {
      if (bufferIndex >= bufferedCandidates.length) {
        if (batchIndex > 0 || openedCount > 0 || skippedByCursor > 0) {
          const previousWindow = windowCandidates.length > 0
            ? windowCandidates
            : (lastWindowCandidates.length > 0 ? lastWindowCandidates : bufferedCandidates.slice(Math.max(0, bufferIndex - candidateLimit), bufferIndex));
          const scrollResult = await scrollAssetViewport(page, estimateAssetWindowScrollStep(previousWindow, scrollStepPx));
          if (!scrollResult.changed) {
            break;
          }
        }

        console.log(`🔎 读取资产视频候选 batch=${batchIndex + 1}...`);
        bufferedCandidates = await withAssetStepTimeout(
          listVideoCardTargets(page),
          20000,
          `读取资产视频候选 batch=${batchIndex + 1}`
        );
        console.log(`🔎 资产视频候选 batch=${batchIndex + 1}: ${bufferedCandidates.length} 个`);
        bufferIndex = 0;
        if (bufferedCandidates.length === 0) {
          if (batchIndex === 0 && openedCount === 0) {
            emptyInitialCandidateReads += 1;
            if (emptyInitialCandidateReads <= 2) {
              console.log(`⚠️  资产视频首屏为空，第 ${emptyInitialCandidateReads}/2 次重新进入资产列表后重试...`);
              await ensureHealthyAssetPage(page, config, 2);
              await ensureVideoAssetTab(page);
              await sleep(1500);
              continue;
            }
            throw new Error('资产视频首屏连续为空，拒绝将本轮记录为成功扫描');
          }
          break;
        }
      }

      const candidate = bufferedCandidates[bufferIndex++];
      const currentBufferedCount = Math.max(1, bufferedCandidates.length);
      let addedToWindow = false;
      const key = `${Math.round((candidate.absoluteY || candidate.y) / 10)}:${Math.round(candidate.x / 10)}:${Math.round(candidate.width / 10)}:${Math.round(candidate.height / 10)}`;
      if (seenKeys.has(key)) {
        noProgressCandidateReads += 1;
        if (bufferIndex >= bufferedCandidates.length && noProgressCandidateReads >= currentBufferedCount) {
          break;
        }
        continue;
      }

      // Once we have consumed a window, the next batch should only read cards
      // that are visually after the previous batch's tail. This keeps the scan
      // contiguous instead of re-counting overlapping cards that remain in view
      // after a scroll in the waterfall layout.
      if (
        Number.isFinite(continuationThresholdTop) &&
        continuationThresholdTop > Number.NEGATIVE_INFINITY &&
        (candidate.absoluteTop || candidate.y) < continuationThresholdTop
      ) {
        noProgressCandidateReads += 1;
        if (bufferIndex >= bufferedCandidates.length && noProgressCandidateReads >= currentBufferedCount) {
          break;
        }
        continue;
      }

      seenKeys.add(key);

      if (streamIndex < initialCursor.offset) {
        streamIndex += 1;
        skippedByCursor += 1;
        batchSkippedByCursor += 1;
        noProgressCandidateReads += 1;
        if (bufferIndex >= bufferedCandidates.length && noProgressCandidateReads >= currentBufferedCount) {
          break;
        }
        continue;
      }

      windowCandidates.push(candidate);
      addedToWindow = true;
      if (addedToWindow) {
        noProgressCandidateReads = 0;
      }
    }

    if (windowCandidates.length === 0) {
      if (batchSkippedByCursor > 0) {
        continue;
      }
      break;
    }

    lastWindowCandidates = windowCandidates;
    const windowTailBottom = Math.max(...windowCandidates.map(item => item.absoluteBottom || item.absoluteY || item.y));
    continuationThresholdTop = Math.max(
      Number.NEGATIVE_INFINITY,
      Math.round(windowTailBottom - continuationOverlapPx)
    );

    let batchOpenedAny = false;

    for (let index = 0; index < windowCandidates.length; index++) {
      const candidate = windowCandidates[index];
      batchOpenedAny = true;
      openedCount += 1;
      scanned += 1;

      console.log(
        `🎞️  打开视频详情候选 batch=${batchIndex + 1} item=${index + 1}: ` +
        `x=${Math.round(candidate.x)}, y=${Math.round(candidate.y)}, absY=${Math.round(candidate.absoluteY || candidate.y)}, ` +
        `size=${Math.round(candidate.width)}x${Math.round(candidate.height)}`
      );
      await page.mouse.click(candidate.x, candidate.y);
      await sleep(1800);

      if (!await isVideoDetailOpen(page)) {
        console.log(`⏭️  候选 batch=${batchIndex + 1} item=${index + 1} 未打开视频详情，跳过`);
        streamIndex += 1;
        continue;
      }

      const detailPrompt = await extractVideoDetailPrompt(page);
      const found = findMatchingSubmissionForDetail(enrichedPool, detailPrompt, submission.trace_id, config);
      if (found.match && found.submission) {
        console.log(`🔐 已匹配视频提示词 (${found.match.by}) -> ${found.submission.trace_id}`);
        if (found.match.detailContentId) {
          rememberAssetDetailContentId(config, found.match.detailContentId, 'matched');
        }
        if (cursorEnabled && scanStrategy.mode === 'deep') {
          saveAssetScanCursor(config, streamIndex + 1);
        }
        return {
          opened: true,
          submission: found.submission,
          match: found.match,
          scannedCandidates: scanned
        };
      }

      const detailId = found.detailContentId || '';
      const detailSuffix = detailId ? `detailContentId=${detailId}` : `详情预览=${(found.detailPreview || '').slice(0, 80)}`;
      console.log(`⏭️  跳过候选 batch=${batchIndex + 1} item=${index + 1}: ${detailSuffix}`);
      if (detailId) {
        seenDetailContentIds.add(detailId);
        if (batchIndex === 0 && currentHeadSignature.length < headSignatureSize) {
          currentHeadSignature.push(detailId);
        }
        rememberAssetDetailContentId(config, detailId, claimPoolIds.has(detailId) ? 'matched' : 'irrelevant');
      }
      await closeVideoDetail(page);
      streamIndex += 1;
    }

    if (batchIndex === 0 && currentHeadSignature.length > 0) {
      const previousHeadSignature = loadAssetHeadSignature(config);
      if (
        previousHeadSignature.length > 0 &&
        currentHeadSignature.length === previousHeadSignature.length &&
        currentHeadSignature.every((item, idx) => item === previousHeadSignature[idx])
      ) {
        saveAssetHeadSignature(config, currentHeadSignature);
        console.log(`ℹ️ 资产页头部候选与上一轮一致 (${currentHeadSignature.join(', ')})，但本轮继续按连续窗口策略向后扫描`);
      }
      saveAssetHeadSignature(config, currentHeadSignature);
    }

    if (!batchOpenedAny) {
      break;
    }
  }

  if (cursorEnabled && scanStrategy.mode === 'deep') {
    if (openedCount === 0 && skippedByCursor > 0) {
      resetAssetScanCursor(config);
      return {
        opened: false,
        reason: '扫描游标已到当前候选末尾，已自动重置，下轮将从头继续'
      };
    }
    const nextOffset = openedCount > 0
      ? Math.max(0, initialCursor.offset + openedCount - cursorOverlapCount)
      : streamIndex;
    saveAssetScanCursor(config, nextOffset);
  }

  const unmatchedIds = orderClaimPool(enrichedPool, submission.trace_id, config)
    .map(item => item.content_id)
    .filter(Boolean)
    .filter(id => !seenDetailContentIds.has(id));
  const appearedTargetIds = Array.from(seenDetailContentIds).filter(id => claimPoolIds.has(id));

  if (appearedTargetIds.length === 0 && unmatchedIds.length > 0) {
    return {
      opened: false,
      reason: `本轮资产页未出现待认领内容ID（已检查 ${scanned} 个候选）: ${unmatchedIds.slice(0, 6).join(', ')}${unmatchedIds.length > 6 ? ' ...' : ''}`,
      appearedTargetIds,
      unmatchedIds,
      scannedCandidates: scanned
    };
  }

  const extra = unmatchedIds.length > 0
    ? `；当前扫描范围未见目标内容ID: ${unmatchedIds.slice(0, 6).join(', ')}${unmatchedIds.length > 6 ? ' ...' : ''}`
    : '';
  return {
    opened: false,
    reason: `未在资产页滚动扫描中找到匹配视频（已检查 ${scanned} 个候选）${extra}`,
    appearedTargetIds,
    unmatchedIds,
    scannedCandidates: scanned
  };
}

async function countDownloadButtons(page) {
  try {
    return await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };

      return Array.from(document.querySelectorAll('button, [role="button"], a'))
        .filter(el => {
          const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
          return isVisible(el) && text.includes('下载');
        })
        .length;
    });
  } catch (error) {
    console.log(`⚠️  统计下载按钮时遇到页面超时，按 0 个处理: ${error.message}`);
    return 0;
  }
}

function pickUploadableSubmissions(records, traceId, limit, recordId = null, taskNames = null) {
  let items = records.filter(item =>
    item &&
    item.record_id &&
    item.trace_id &&
    (!traceId || item.trace_id === traceId) &&
    (!recordId || item.record_id === recordId) &&
    (!taskNames || taskNames.includes(item.task_name))
  );

  if (limit) {
    items = items.slice(0, limit);
  }

  return items;
}

function buildRecordStateMap(records, config) {
  const map = new Map();
  for (const record of records || []) {
    const fields = record?.fields || {};
    map.set(record.record_id, {
      recordId: record.record_id,
      status: String(fields[config.statusField] || '').trim(),
      executionOwner: String(fields[config.fields.executionOwner] || '').trim(),
      executionOwnerMachineId: parseExecutionOwner(fields[config.fields.executionOwner]).machineId,
      latestTraceId: String(fields[config.fields.latestTraceId] || '').trim(),
      resultSyncStatus: String(fields[config.fields.resultSyncStatus] || '').trim(),
      videoAttachmentCount: Array.isArray(fields[config.fields.videoAttachment])
        ? fields[config.fields.videoAttachment].length
        : 0
    });
  }
  return map;
}

function shouldManageSubmission(config, submission, recordStateMap) {
  if (!submission || !submission.record_id) {
    return false;
  }

  const recordState = recordStateMap?.get(submission.record_id);
  if (recordState) {
    if (recordState.status === '待处理' || recordState.status === '部分提交') {
      return false;
    }
    if (recordState.latestTraceId && recordState.latestTraceId !== submission.trace_id) {
      return false;
    }
    if (recordState.executionOwnerMachineId) {
      return recordState.executionOwnerMachineId === config.machineId;
    }
  }

  if (recordStateMap && !recordState) {
    return false;
  }

  if (submission.execution_machine_id) {
    return submission.execution_machine_id === config.machineId;
  }

  if (submission.execution_owner) {
    return executionOwnerMatchesMachine(submission.execution_owner, config.machineId);
  }

  // 兼容旧 trace：还没有执行归属字段时，保留原有单机行为。
  return true;
}

async function uploadExistingDownloads(config, token, submissions, dryRun = false, recordStateMap = null) {
  let uploaded = 0;

  for (const submission of submissions) {
    if (!shouldManageSubmission(config, submission, recordStateMap)) {
      continue;
    }

    const recordState = recordStateMap?.get(submission.record_id);
    const shouldRepairUploadedRecord =
      submission.status === 'uploaded' &&
      submission.uploaded_file_token &&
      (!recordState ||
        recordState.resultSyncStatus !== 'uploaded' ||
        Number(recordState.videoAttachmentCount || 0) <= 0);

    if (shouldRepairUploadedRecord) {
      if (dryRun) {
        console.log(`🧪 dry-run: 将修复已上传但未完整落表的附件 ${submission.trace_id}`);
        uploaded++;
        continue;
      }

      await updateRecord(config, token, submission.record_id, {
        [config.statusField]: config.uploadedStatus || '已提交',
        [config.fields.resultSyncStatus]: 'uploaded',
        [config.fields.result]: '已从资产页认领并回写视频',
        [config.fields.videoFileName]: submission.local_file_path
          ? path.basename(submission.local_file_path)
          : `${submission.trace_id}.mp4`,
        [config.fields.finishTime]: new Date().toISOString(),
        [config.fields.errorMessage]: '',
        [config.fields.videoAttachment]: [
          {
            file_token: submission.uploaded_file_token
          }
        ]
      });
      const updatedSubmission = updateSubmissionRecord(config, submission.trace_id, {
        status: 'uploaded',
        state_updated_at: new Date().toISOString()
      });
      syncAutomationTaskFromJimeng(updatedSubmission, '生成视频已上传过，已补写飞书源表附件字段');
      uploaded++;
      continue;
    }

    if (!['downloaded', 'upload_failed'].includes(submission.status)) {
      continue;
    }
    if (!submission.local_file_path || !fs.existsSync(submission.local_file_path)) {
      continue;
    }

    if (dryRun) {
      console.log(`🧪 dry-run: 将上传已下载文件 ${submission.trace_id} -> ${submission.local_file_path}`);
      uploaded++;
      continue;
    }

    const upload = await uploadFileToFeishu(config, token, submission.local_file_path);
    await updateRecord(config, token, submission.record_id, {
      [config.statusField]: config.uploadedStatus || '已提交',
      [config.fields.resultSyncStatus]: 'uploaded',
      [config.fields.result]: '已从资产页认领并回写视频',
      [config.fields.videoFileName]: path.basename(submission.local_file_path),
      [config.fields.finishTime]: new Date().toISOString(),
      [config.fields.errorMessage]: '',
      [config.fields.videoAttachment]: [
        {
          file_token: upload.fileToken
        }
      ]
    });
    const updatedSubmission = updateSubmissionRecord(config, submission.trace_id, {
      status: 'uploaded',
      uploaded_file_token: upload.fileToken,
      upload_parent_type: upload.parentType,
      state_updated_at: new Date().toISOString()
    });
    syncAutomationTaskFromJimeng(updatedSubmission, '即梦生成视频已上传回飞书源表');
    uploaded++;
  }

  return uploaded;
}

async function tryDownloadLatestForSinglePending(page, config, token, pendingSubmission, dryRun = false, claimPool = null) {
  if (String(pendingSubmission.platform || pendingSubmission.channel || '').toLowerCase() === 'imini') {
    if (dryRun) {
      return { downloaded: false, reason: 'dry-run 暂不执行 imini 资产下载' };
    }

    const claimResult = await claimIminiAsset(page, config, pendingSubmission, claimPool);
    if (claimResult.platformFailed) {
      const matchedSubmission = claimResult.matched?.submission || pendingSubmission;
      const reason = claimResult.reason || 'imini 平台返回生成失败';
      const updatedSubmission = updateSubmissionRecord(config, matchedSubmission.trace_id, {
        status: 'blocked',
        error_message: reason,
        submit_confirmation_note: reason,
        state_updated_at: new Date().toISOString()
      });
      syncAutomationTaskFromJimeng(updatedSubmission, 'imini 平台返回生成失败，已停止自动重试');

      try {
        await updateRecord(config, token, matchedSubmission.record_id, {
          [config.statusField || '状态']: '阻塞',
          [config.fields.resultSyncStatus]: 'blocked',
          [config.fields.result]: `${reason}。平台已明确返回生成失败，当前不再自动重试；请人工替换首帧/参考图或调整任务后再改回待处理`,
          [config.fields.errorMessage]: reason,
          [config.fields.finishTime]: new Date().toISOString()
        });
      } catch (error) {
        console.log(`⚠️  ${matchedSubmission.trace_id} imini 平台失败已记录本地，但写回飞书失败: ${error.message}`);
      }

      return {
        downloaded: false,
        platformFailed: true,
        reason,
        matchedTraceId: matchedSubmission.trace_id
      };
    }

    if (!claimResult.ok || !claimResult.filePath) {
      return {
        downloaded: false,
        reason: claimResult.reason || 'imini 资产页未匹配到结果'
      };
    }

    const matchedSubmission = claimResult.matched?.submission || pendingSubmission;
    const updatedSubmission = updateSubmissionRecord(config, matchedSubmission.trace_id, {
      status: 'downloaded',
      local_file_path: claimResult.filePath,
      claim_by: claimResult.matched?.by || claimResult.matched?.match?.by || '',
      content_id_found_in_detail: claimResult.matched?.detailContentId || '',
      state_updated_at: new Date().toISOString()
    });
    syncAutomationTaskFromJimeng(updatedSubmission, 'imini 生成视频已下载到本地，等待上传回飞书源表');

    try {
      await updateRecord(config, token, matchedSubmission.record_id, {
        [config.fields.resultSyncStatus]: 'downloaded',
        [config.fields.videoFileName]: path.basename(claimResult.filePath),
        [config.fields.finishTime]: new Date().toISOString(),
        [config.fields.errorMessage]: ''
      });
    } catch (error) {
      console.log(`⚠️  ${matchedSubmission.trace_id} imini 已下载，但写回飞书 downloaded 状态失败，后续将自动补传: ${error.message}`);
    }

    return {
      downloaded: true,
      filePath: claimResult.filePath,
      matchedTraceId: matchedSubmission.trace_id
    };
  }

  return await withRecoveredAssetContext(page, config, `认领 ${pendingSubmission.trace_id}`, async () => {
    const detailMatch = await openMatchingVideoDetail(page, config, token, pendingSubmission, claimPool);
    if (!detailMatch.opened) {
      return {
        downloaded: false,
        reason: detailMatch.reason,
        appearedTargetIds: detailMatch.appearedTargetIds || [],
        unmatchedIds: detailMatch.unmatchedIds || [],
        scannedCandidates: detailMatch.scannedCandidates || 0
      };
    }
    const downloadCount = await countDownloadButtons(page);
    if (downloadCount === 0) {
      return { downloaded: false, reason: '资产页暂无可下载视频' };
    }

    const downloadDir = getDownloadDir(config);
    const matchedSubmission = detailMatch.submission || pendingSubmission;
    const claimMeta = detailMatch.match || null;
    const tempFile = dryRun ? path.join(downloadDir, `${matchedSubmission.trace_id}.mp4`) : await downloadVideo(page, downloadDir);
    if (!tempFile) {
      return { downloaded: false, reason: '点击下载失败' };
    }

    const ext = path.extname(tempFile) || '.mp4';
    const targetPath = path.join(downloadDir, `${matchedSubmission.trace_id}${ext}`);

    if (!dryRun) {
      if (tempFile !== targetPath) {
        fs.renameSync(tempFile, targetPath);
      }

      const updatedSubmission = updateSubmissionRecord(config, matchedSubmission.trace_id, {
        status: 'downloaded',
        local_file_path: targetPath,
        claim_by: claimMeta?.by || '',
        content_id_found_in_detail: claimMeta?.detailContentId || '',
        state_updated_at: new Date().toISOString()
      });
      syncAutomationTaskFromJimeng(updatedSubmission, '即梦生成视频已下载到本地，等待上传回飞书源表');

      try {
        await updateRecord(config, token, matchedSubmission.record_id, {
          [config.fields.resultSyncStatus]: 'downloaded',
          [config.fields.videoFileName]: path.basename(targetPath),
          [config.fields.finishTime]: new Date().toISOString(),
          [config.fields.errorMessage]: ''
        });
      } catch (error) {
        console.log(`⚠️  ${matchedSubmission.trace_id} 本地已下载，但写回飞书的 downloaded 状态失败，后续将自动补传: ${error.message}`);
      }
    }

    return {
      downloaded: true,
      filePath: targetPath,
      matchedTraceId: matchedSubmission.trace_id
    };
  });
}

async function syncCompletedSubmissions({
  config,
  token,
  page,
  dryRun = false,
  limit = null,
  traceId = null,
  recordId = null,
  taskNames = null,
  currentGeneratingCount = null,
  includeClaimFailedRetries = true,
  allowAssetScan = true,
  forceAssetRead = false,
  assetScanCooldownMessage = '',
  allowConcurrentClaim = false,
  claimBatchLimit = null,
  channelFilter = ''
}) {
  const baseResult = (overrides = {}) => ({
    uploaded: 0,
    downloaded: 0,
    skipped: 0,
    reason: '',
    assetScanAttempted: false,
    ...overrides
  });

  const recordStateMap = buildRecordStateMap(await listAllRecords(config, token), config);
  let all = reconcileRecoveredDownloads(
    config,
    pickUploadableSubmissions(listSubmissionRecords(config), traceId, limit, recordId, taskNames)
  )
    .filter(item => submissionMatchesChannelFilter(item, channelFilter))
    .filter(item => shouldManageSubmission(config, item, recordStateMap));
  if (!dryRun) {
    await isolateStaleSubmissions(config, token, all, currentGeneratingCount, recordStateMap);
    all = reconcileRecoveredDownloads(
      config,
      pickUploadableSubmissions(listSubmissionRecords(config), traceId, limit, recordId, taskNames)
    )
      .filter(item => submissionMatchesChannelFilter(item, channelFilter))
      .filter(item => shouldManageSubmission(config, item, recordStateMap));
  }
  let uploadedCount = await uploadExistingDownloads(config, token, all, dryRun, recordStateMap);

  let pendingActive = all.filter(item => ['submitted', 'rendering', 'observing'].includes(item.status));
  const pendingClaimFailed = includeClaimFailedRetries
    ? all.filter(item => item.status === 'claim_failed')
    : [];
  const pendingTimedOut = all.filter(item => canRetryTimedOutClaim(item));

  if (pendingActive.length === 0 && pendingClaimFailed.length === 0 && pendingTimedOut.length === 0) {
    return baseResult({
      uploaded: uploadedCount,
      reason: uploadedCount > 0 ? '仅补传已下载文件' : '无待回写任务'
    });
  }

  if (currentGeneratingCount != null && currentGeneratingCount > 0) {
    if (pendingActive.length > 0) {
      pendingActive = markPendingQueueObserved(config, pendingActive, currentGeneratingCount);
    }
    if (!allowConcurrentClaim) {
      return baseResult({
        uploaded: uploadedCount,
        skipped: pendingActive.length + pendingClaimFailed.length + pendingTimedOut.length,
        reason: `仍有 ${currentGeneratingCount} 个任务生成中`
      });
    }
  }

  const newlyZeroQueued = pendingActive.filter(item => !item.first_zero_queue_at);
  if (!forceAssetRead && newlyZeroQueued.length > 0) {
    for (const item of newlyZeroQueued) {
      noteZeroQueueWindow(config, item);
    }
    return baseResult({
      uploaded: uploadedCount,
      skipped: pendingActive.length + pendingClaimFailed.length + pendingTimedOut.length,
      reason: `首次观测到 ${newlyZeroQueued.length} 条活跃任务队列归零，先进入冷却窗口再检查资产`
    });
  }

  const claimCooldownGate = getClaimCooldownGate(config, pendingActive);
  if (!forceAssetRead && claimCooldownGate) {
    return baseResult({
      uploaded: uploadedCount,
      skipped: pendingActive.length + pendingClaimFailed.length + pendingTimedOut.length,
      reason: `最近仍有新提交 (${claimCooldownGate.traceId}, ${claimCooldownGate.status})，冷却 ${claimCooldownGate.remainingSeconds}s 后再检查资产`
    });
  }

  if (!allowAssetScan && !forceAssetRead) {
    return baseResult({
      uploaded: uploadedCount,
      skipped: pendingActive.length + pendingClaimFailed.length + pendingTimedOut.length,
      reason: assetScanCooldownMessage || '资产扫描冷却中，暂不切换资产页'
    });
  }

  if (forceAssetRead) {
    allowAssetScan = true;
  }

  const candidates = [
    ...(currentGeneratingCount != null && currentGeneratingCount > 0
      ? [
          ...sortClaimCandidates(pendingTimedOut, config),
          ...(includeClaimFailedRetries ? sortClaimCandidates(pendingClaimFailed, config) : []),
          ...sortClaimCandidates(pendingActive, config)
        ]
      : [
          ...sortClaimCandidates(pendingActive, config),
          ...sortClaimCandidates(pendingTimedOut, config),
          ...(includeClaimFailedRetries ? sortClaimCandidates(pendingClaimFailed, config) : [])
        ])
  ];
  if (candidates.length === 0) {
    return baseResult({
      uploaded: uploadedCount,
      reason: uploadedCount > 0 ? '仅补传已下载文件' : '无待回写任务'
    });
  }

  let downloadedCount = 0;
  let skippedCount = 0;
  const reasons = [];
  const processedTraceIds = new Set();
  const maxClaims = Math.max(
    1,
    Math.floor(
      Number(
        claimBatchLimit ||
        (currentGeneratingCount != null && currentGeneratingCount > 0
          ? config.concurrentClaimBatchLimit
          : config.idleClaimBatchLimit)
      ) || 1
    )
  );
  let stopAfterCurrentRound = false;
  let assetScanAttempted = false;

  while (!stopAfterCurrentRound && downloadedCount < maxClaims) {
    const claimPool = [];

    for (let candidate of candidates) {
      if (processedTraceIds.has(candidate.trace_id)) {
        continue;
      }
      if (['uploaded', 'downloaded', 'upload_failed', 'failed', 'blocked'].includes(candidate.status)) {
        continue;
      }

      if (currentGeneratingCount === 0 && ['submitted', 'rendering', 'observing', 'claim_failed'].includes(candidate.status)) {
        candidate = noteZeroQueueWindow(config, candidate);
      }

      const allowSubmittedCompatibilityClaim =
        !candidate.queue_observed &&
        candidate.status === 'submitted' &&
        Boolean(candidate.prompt_hash || candidate.prompt_anchor);

      if (!candidate.queue_observed && candidate.status !== 'claim_failed' && candidate.status !== 'timed_out' && !allowSubmittedCompatibilityClaim) {
        skippedCount++;
        reasons.push(`${candidate.trace_id}: 尚未观测到任务进入生成队列`);
        continue;
      }

      if (allowSubmittedCompatibilityClaim) {
        console.log(`ℹ️  ${candidate.trace_id} 缺少 queue_observed，按兼容模式进行一次提示词校验认领`);
      }

      if (candidate.status === 'claim_failed') {
        console.log(`♻️  低优先级重试认领 ${candidate.trace_id} (attempts=${candidate.claim_attempts || 0})`);
      } else if (candidate.status === 'timed_out') {
        console.log(`🕰️  超时隔离任务继续尝试认领 ${candidate.trace_id} (${candidate.submit_confirmed_by || 'unknown'})`);
      }

      claimPool.push(candidate);
    }

    if (claimPool.length === 0) {
      break;
    }

    const primaryCandidate = claimPool[0];
    assetScanAttempted = true;
    const result = await tryDownloadLatestForSinglePending(page, config, token, primaryCandidate, dryRun, claimPool);
    if (!result.downloaded) {
      skippedCount += claimPool.length;
      reasons.push(`claim-pool(${claimPool.length}): ${result.reason}`);

      // 补偿只由“资产页真实出现”驱动：如果本轮没出现待认领内容ID，
      // 就整轮结束，不再对单条 trace 做猜测式 miss。
      if (String(result.reason || '').includes('本轮资产页未出现待认领内容ID')) {
        stopAfterCurrentRound = true;
        continue;
      }

      if (String(result.reason || '').includes('当前资产页头部连续命中')) {
        stopAfterCurrentRound = true;
        continue;
      }

      // 其余失败原因不做单条降级猜测，本轮结束等待下次真实资产变化后再补偿。
      stopAfterCurrentRound = true;
      continue;
    }

    downloadedCount++;
    const matchedTraceId = result.matchedTraceId || primaryCandidate.trace_id;
    processedTraceIds.add(matchedTraceId);
    reasons.push(`${matchedTraceId}: ${dryRun ? 'dry-run 命中' : '已下载'}`);

    if (!dryRun) {
      const refreshed = listSubmissionRecords(config).find(item => item.trace_id === matchedTraceId);
      const uploadedAfterDownload = await uploadExistingDownloads(config, token, [refreshed], dryRun, recordStateMap);
      uploadedCount += uploadedAfterDownload;
      if (uploadedAfterDownload > 0) {
        reasons.push(`${matchedTraceId}: 已回写飞书`);
      }
      if (downloadedCount >= maxClaims) {
        return baseResult({
          uploaded: uploadedCount,
          downloaded: downloadedCount,
          skipped: skippedCount,
          reason: reasons.join(' | ') || '已下载并回写飞书',
          assetScanAttempted
        });
      }
      continue;
    }

    if (downloadedCount >= maxClaims) {
      return baseResult({
        uploaded: uploadedCount,
        downloaded: downloadedCount,
        skipped: skippedCount,
        reason: reasons.join(' | ') || 'dry-run 命中',
        assetScanAttempted
      });
    }
  }

  return baseResult({
    uploaded: uploadedCount,
    downloaded: downloadedCount,
    skipped: skippedCount,
    reason: reasons.join(' | ') || (dryRun ? 'dry-run 未命中' : '无可认领结果'),
    assetScanAttempted
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig(args.configPath);

  if (!config.appId || !config.appSecret || (!config.tableUrl && (!config.appToken || !config.tableId))) {
    throw new Error('飞书配置不完整：需要 appId/appSecret，以及 tableUrl 或 appToken/tableId');
  }

  const token = await getAccessToken(config);
  await resolveConfigTable(config, token);
  if (!config.appToken || !config.tableId) {
    throw new Error('飞书配置不完整：未能解析 appToken/tableId');
  }
  const { browser, page } = await connectBrowser(config);

  try {
    let currentGeneratingCount = null;
    const iminiOnlyDirectClaim = args.channel === 'imini' && args.ignoreGeneratingCount;

    if (iminiOnlyDirectClaim) {
      console.log('ℹ️  imini 定向回流：跳过通用生成状态检查，直接扫描 imini 资产页');
    } else {
      try {
        const queueStatus = await checkGeneratingStatus(page);
        currentGeneratingCount = queueStatus?.generating ?? null;
        if (currentGeneratingCount != null) {
          console.log(`ℹ️  当前生成中任务: ${currentGeneratingCount}`);
        }
      } catch (error) {
        console.log(`⚠️  获取生成中数量失败，将按保守模式继续结果回写: ${error.message}`);
      }
    }

    if (args.ignoreGeneratingCount) {
      console.log('🧪 已启用并发下载测试模式：忽略当前生成中数量，仅做定向认领/下载验证');
      currentGeneratingCount = 0;
    }

    const summary = await syncCompletedSubmissions({
      config,
      token,
      page,
      dryRun: args.dryRun,
      limit: args.limit,
      traceId: args.traceId,
      recordId: args.recordId,
      taskNames: args.taskNames,
      channelFilter: args.channel,
      currentGeneratingCount,
      forceAssetRead: args.forceAssetRead || iminiOnlyDirectClaim,
      allowConcurrentClaim: iminiOnlyDirectClaim,
      claimBatchLimit: iminiOnlyDirectClaim ? args.limit : null
    });
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await browser.disconnect();
  }
}

module.exports = {
  loadConfig,
  getAccessToken,
  syncCompletedSubmissions
};

if (require.main === module) {
  main().catch(error => {
    console.error(`❌ 结果回写失败: ${error.message}`);
    process.exit(1);
  });
}
