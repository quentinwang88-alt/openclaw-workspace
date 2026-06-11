#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const os = require('os');
const https = require('https');
const crypto = require('crypto');
const puppeteer = require('puppeteer-core');

const {
  checkGeneratingStatus,
  cleanupRecentUnnamedConversations,
  formatBeijingTimestamp,
  prepareAutomationPage,
  prepareImagesForUpload,
  processTask,
  resetVideoGenerationPage,
  sleep
} = require('./folder-processor');
const {
  getStateRoot,
  getSubmissionsDir,
  buildWorkerId,
  ensureStateDirs,
  generateTraceId,
  listSubmissionRecords,
  updateSubmissionRecord,
  writeSubmissionRecord
} = require('./trace-state');
const { syncCompletedSubmissions } = require('./result-uploader');
let syncJimengTaskState = null;
try {
  ({ syncJimengTaskState } = require('../short-video-automation-mvp/services/nodes/sync-jimeng-task-state'));
} catch (error) {
  syncJimengTaskState = null;
}
const { resolveChannel } = require('./channel-router');
const { processIminiTask } = require('./platforms/imini/adapter');

const STATUS = {
  PENDING: '待处理',
  PROCESSING: '处理中',
  PARTIAL: '部分提交',
  SUBMITTED: '已提交',
  BLOCKED: '阻塞',
  FAILED: '失败'
};

const ACTIVE_SUBMISSION_STATUSES = new Set([
  'submitting',
  'submitted',
  'rendering',
  'observing',
  'downloaded',
  'upload_failed',
  'broken_state'
]);

const SUBMISSION_GATE_BLOCKING_STATUSES = new Set([
  'submitting',
  'broken_state'
]);

const PENDING_CLAIM_SUBMISSION_STATUSES = new Set([
  'submitted',
  'rendering',
  'observing'
]);

const DUPLICATE_GUARD_SUBMISSION_STATUSES = new Set([
  'submitted',
  'rendering',
  'observing',
  'downloaded',
  'uploaded'
]);

const DEFAULT_CONFIG = {
  appId: '',
  appSecret: '',
  appToken: '',
  tableId: '',
  viewId: '',
  pageSize: 500,
  submitInflightRecentLimit: 250,
  statusField: '状态',
  pendingStatuses: [STATUS.PENDING, STATUS.PARTIAL],
  statusOptions: [
    STATUS.PENDING,
    STATUS.PROCESSING,
    STATUS.PARTIAL,
    STATUS.SUBMITTED,
    STATUS.BLOCKED,
    STATUS.FAILED
  ],
  fields: {
    taskName: '任务名',
    prompt: '提示词',
    images: ['参考图'],
    executionOwner: '执行归属',
    allowNoReferenceImage: '免参考图',
    repeatCount: '生成次数',
    model: '模型',
    mode: '参考模式',
    ratio: '视频比例',
    duration: '视频时长',
    submittedCount: '已提交次数',
    result: '结果说明',
    lastProcessedAt: '最后处理时间',
    blockedPath: '阻塞截图路径',
    latestTraceId: '最新追踪ID',
    resultSyncStatus: '结果回传状态',
    videoAttachment: '生成视频',
    videoFileName: '生成视频文件名',
    submitTime: '提交时间',
    finishTime: '完成时间',
    errorMessage: '错误信息',
    channel: '渠道',
    channelSource: '渠道来源'
  },
  runtimeRoot: '~/Desktop/temp/jimeng-feishu-runtime',
  cdpPort: 9222,
  cdpHost: '127.0.0.1',
  baseUrl: 'https://jimeng.jianying.com/ai-tool/generate?workspace=0&type=video',
  defaultModel: 'Seedance 2.0 Fast',
  defaultMode: '全能参考',
  defaultRatio: '9:16',
  defaultDuration: 4,
  timeout: 600000,
  protocolTimeoutMs: 300000,
  feishuRequestTimeoutMs: 45000,
  feishuDownloadTimeoutMs: 120000,
  insufficientCreditsThreshold: 45,
  maxConcurrent: 8,
  maxInflightSubmissions: 8,
  checkIntervalMinutes: 15,
  queueFullCheckIntervalMinutes: 30,
  assetScanIntervalMinutes: 120,
  concurrentAssetScanIntervalMinutes: 120,
  assetScanTimeoutMs: 20 * 60 * 1000,
  concurrentClaimBatchLimit: 5,
  idleClaimBatchLimit: 8,
  assetScanBatches: 5,
  assetScrollStepPx: 1200,
  postSubmitQueueCheckAttempts: 4,
  postSubmitQueueCheckIntervalMs: 3000,
  maxPlatformLimitedRetries: 10,
  maxSubmitUnconfirmedRetries: 2,
  submitUnconfirmedRetryWindowMinutes: 120,
  maxGenerateErrorNoticeRetries: 2,
  generateErrorNoticeRetryWindowMinutes: 180,
  allowQueueGrowthRecoveryOnError: false,
  enableContentIdClaim: true,
  contentIdMode: 'grayscale',
  contentIdLabel: '内容ID',
  claimStrategyOrder: ['content_id', 'script_id', 'prompt_hash', 'prompt_anchor'],
  claimGraceMinutes: 20,
  maxClaimAttempts: 5,
  recoverStaleProcessingMinutes: 30,
  submitPauseOnInsufficientCredits: true,
  ensureSchema: true
};

const SUBMIT_TAB_ROLE = 'openclaw-jimeng-submit';
const IMINI_SUBMIT_TAB_ROLE = 'openclaw-imini-submit';
const DOWNLOAD_TAB_ROLE = 'openclaw-jimeng-download';
const IMINI_DOWNLOAD_TAB_ROLE = 'openclaw-imini-download';
const DEFAULT_ASSET_URL = 'https://jimeng.jianying.com/ai-tool/asset?workspace=0';

const FIELD_TYPE = {
  TEXT: 1,
  NUMBER: 2,
  SINGLE_SELECT: 3,
  ATTACHMENT: 17
};

const STATUS_COLORS = [0, 1, 2, 4, 5, 6];
const MODEL_ALIASES = {
  'seedance 2.0 fast vip': 'Seedance 2.0 Fast VIP',
  'seedance 2.0 vip': 'Seedance 2.0 VIP',
  'seedance 2.0 fast': 'Seedance 2.0 Fast',
  'seedance 2.0': 'Seedance 2.0'
};

let cachedAccessToken = null;
let tokenExpireTime = 0;
let activeConfigForNetwork = null;

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
    console.log(`⚠️  自动化库状态同步失败（Jimeng submit侧）: ${error.message}`);
    return {
      status: 'sync_failed',
      matched: false,
      error: error.message
    };
  }
}

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, process.env.HOME || '~');
}

function parseArgs(argv) {
  const args = {
    configPath: path.join(__dirname, 'feishu-direct.json'),
    dryRun: false,
    ensureSchemaOnly: false,
    limit: null,
    recordId: null,
    taskNames: null,
    resumeOnly: false,
    oneShot: false,
    scheduled: false,
    submitOnly: false,
    downloadOnly: false,
    channel: ''
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--config' && argv[i + 1]) {
      args.configPath = argv[++i];
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--ensure-schema-only') {
      args.ensureSchemaOnly = true;
    } else if (arg === '--limit' && argv[i + 1]) {
      args.limit = Number(argv[++i]) || null;
    } else if (arg === '--record-id' && argv[i + 1]) {
      args.recordId = argv[++i];
    } else if (arg === '--task-name' && argv[i + 1]) {
      const raw = argv[++i];
      const names = raw
        .split(',')
        .map(item => item.trim())
        .filter(Boolean);
      args.taskNames = names.length > 0 ? names : null;
    } else if (arg === '--resume-only') {
      args.resumeOnly = true;
    } else if (arg === '--oneshot') {
      args.oneShot = true;
    } else if (arg === '--scheduled') {
      args.scheduled = true;
    } else if (arg === '--submit-only') {
      args.submitOnly = true;
    } else if (arg === '--download-only') {
      args.downloadOnly = true;
    } else if (arg === '--channel' && argv[i + 1]) {
      args.channel = normalizeChannelFilter(argv[++i]);
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

function getSubmitPauseFilePath(config) {
  return path.join(getStateRoot(config), 'submit-paused-insufficient-credits.json');
}

function readSubmitPauseState(config) {
  const filePath = getSubmitPauseFilePath(config);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return raw && typeof raw === 'object' ? raw : null;
  } catch (_) {
    return null;
  }
}

function writeSubmitPauseState(config, payload) {
  const filePath = getSubmitPauseFilePath(config);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
  return filePath;
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

  config.runtimeRoot = expandHome(config.runtimeRoot);
  config.machineId = sanitizeMachineId(
    raw.machineId ||
    process.env.OPENCLAW_MACHINE_ID ||
    os.hostname() ||
    `machine-${config.cdpPort || 9222}`
  );
  activeConfigForNetwork = config;
  return config;
}

function getFeishuRequestTimeoutMs() {
  return Math.max(5000, Number(activeConfigForNetwork?.feishuRequestTimeoutMs) || 45000);
}

function getFeishuDownloadTimeoutMs() {
  return Math.max(10000, Number(activeConfigForNetwork?.feishuDownloadTimeoutMs) || 120000);
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

function buildOwnershipClaimToken(config) {
  return String(config.machineId || '').trim();
}

function parseExecutionOwner(value) {
  const raw = normalizeTextField(value);
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

function getChannelConfig(config, channel) {
  return (config.channels || {})[channel] || {};
}

function shouldRunScheduledOneShot(now = new Date(), mode = 'mixed', config = {}, channelFilter = '') {
  const hour = now.getHours();
  const minute = now.getMinutes();
  const isDownloadOnly = mode === 'download-only';
  if (isDownloadOnly) {
    if (normalizeChannelFilter(channelFilter) === 'imini') {
      const iminiConfig = getChannelConfig(config, 'imini');
      const interval = Math.max(1, Number(iminiConfig.assetScanIntervalMinutes) || 30);
      const offset = Math.max(0, Number(iminiConfig.assetScanScheduleMinuteOffset) || 10);
      return minute % interval === offset % interval;
    }
    return minute === 5 && hour % 2 === 1;
  }

  const submitInterval = Math.max(1, Number(config.checkIntervalMinutes) || 10);
  if (mode === 'submit-only') {
    return minute % submitInterval === 0;
  }

  const isNightBurstWindow = (hour === 23 && minute >= 30) || hour < 9;

  if (isNightBurstWindow) {
    return minute % submitInterval === 0;
  }

  return minute === 0 || minute === 30;
}

function requestJson(method, requestPath, token, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json'
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

    const timeoutMs = getFeishuRequestTimeoutMs();
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
          if (json.code === 91403) {
            reject(new Error('飞书权限不足（91403 Forbidden）：当前应用可以读取表格，但没有写入这张多维表格的权限。请为应用开启该表的可编辑权限，或将表授权给当前飞书应用。'));
            return;
          }
          reject(new Error(`飞书 API 失败 (${json.code}): ${json.msg || '未知错误'}`));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书请求超时 (${timeoutMs}ms): ${method} ${requestPath}`));
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

    const timeoutMs = getFeishuRequestTimeoutMs();
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

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书请求超时 (${timeoutMs}ms): ${method} ${requestPath}`));
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
    if (data.code === 91403) {
      throw new Error('飞书权限不足（91403 Forbidden）：当前应用没有访问该资源所需的权限。');
    }
    throw new Error(`获取飞书 Token 失败: ${data.msg || '未知错误'}`);
  }

  cachedAccessToken = data.tenant_access_token;
  tokenExpireTime = Date.now() + ((data.expire || 7200) - 60) * 1000;
  return cachedAccessToken;
}

async function listTableFields(config, token) {
  const data = await requestJson(
    'GET',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields?page_size=200&_ts=${Date.now()}`,
    token
  );
  return data.items || [];
}

async function createField(config, token, fieldSpec) {
  return requestJson(
    'POST',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields`,
    token,
    fieldSpec
  );
}

function isFieldNameDuplicatedError(error) {
  return /FieldNameDuplicated|1254014/.test(String(error?.message || error || ''));
}

async function updateField(config, token, fieldId, fieldSpec) {
  return requestJson(
    'PUT',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields/${fieldId}`,
    token,
    fieldSpec
  );
}

async function updateRecord(config, token, recordId, fields) {
  return requestJson(
    'PUT',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}`,
    token,
    { fields }
  );
}

async function listAllRecords(config, token) {
  let pageToken = null;
  let records = [];

  do {
    let requestPath = `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records?page_size=${config.pageSize || 100}&_ts=${Date.now()}`;
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

async function searchRecords(config, token, searchBody) {
  let pageToken = null;
  let records = [];

  do {
    const body = {
      ...searchBody,
      page_size: config.pageSize || 100
    };
    if (pageToken) {
      body.page_token = pageToken;
    }
    if (config.viewId) {
      body.view_id = config.viewId;
    }

    const data = await requestJson(
      'POST',
      `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/search`,
      token,
      body
    );
    records = records.concat(data.items || []);
    pageToken = data.page_token || null;
  } while (pageToken);

  return records;
}

async function listSubmitCandidateRecords(config, token) {
  const pendingStatuses = (config.pendingStatuses || [])
    .map(status => String(status || '').trim())
    .filter(Boolean);

  if (pendingStatuses.length === 0) {
    return listAllRecords(config, token);
  }

  const conditions = pendingStatuses.map(status => ({
    field_name: config.statusField,
    operator: 'is',
    value: [status]
  }));

  return searchRecords(config, token, {
    filter: {
      conjunction: 'or',
      conditions
    }
  });
}

function normalizeTextField(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number') return String(value);
  if (Array.isArray(value)) {
    return value
      .map(item => {
        if (typeof item === 'string') return item;
        if (item == null) return '';
        if (typeof item.text === 'string') return item.text;
        if (typeof item.name === 'string') return item.name;
        return '';
      })
      .filter(Boolean)
      .join('\n')
      .trim();
  }
  if (typeof value === 'object') {
    if (typeof value.text === 'string') return value.text.trim();
    if (typeof value.name === 'string') return value.name.trim();
    if (typeof value.value === 'string') return value.value.trim();
  }
  return String(value).trim();
}

function normalizeNumberField(value, fallback = 0) {
  if (typeof value === 'number') return value;
  const text = normalizeTextField(value);
  const match = text.match(/-?\d+(\.\d+)?/);
  return match ? Number(match[0]) : fallback;
}

function normalizeBooleanField(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (Array.isArray(value)) {
    return normalizeBooleanField(
      value
        .map(item => {
          if (item == null) return '';
          if (typeof item === 'string') return item;
          if (typeof item.name === 'string') return item.name;
          if (typeof item.text === 'string') return item.text;
          return '';
        })
        .filter(Boolean)
        .join(' ')
    );
  }

  const normalized = normalizeTextField(value).replace(/\s+/g, '').toLowerCase();
  if (!normalized) return false;

  const truthyValues = new Set([
    'true',
    '1',
    'yes',
    'y',
    'on',
    '是',
    '开',
    '开启',
    '允许',
    '已开启',
    '需要',
    '免参考图'
  ]);
  const falsyValues = new Set([
    'false',
    '0',
    'no',
    'n',
    'off',
    '否',
    '关',
    '关闭',
    '不允许',
    '未开启'
  ]);

  if (truthyValues.has(normalized)) return true;
  if (falsyValues.has(normalized)) return false;
  return false;
}

function normalizeModelName(value, fallback) {
  const normalized = normalizeTextField(value);
  if (!normalized) return fallback;
  const lower = normalized.toLowerCase();
  if (lower.includes('seedance 2.0 fast vip')) {
    return 'Seedance 2.0 Fast VIP';
  }
  if (lower.includes('seedance 2.0 vip')) {
    return 'Seedance 2.0 VIP';
  }
  if (lower.includes('seedance 2.0 fast')) {
    return 'Seedance 2.0 Fast';
  }
  if (lower.includes('seedance 2.0')) {
    return 'Seedance 2.0';
  }
  return MODEL_ALIASES[normalized.toLowerCase()] || normalized;
}

function normalizeTimestampField(value) {
  const text = normalizeTextField(value);
  if (!text) return null;

  const timestamp = Date.parse(text);
  if (Number.isNaN(timestamp)) {
    return null;
  }

  return new Date(timestamp);
}

function sanitizeTaskName(value, fallback) {
  const cleaned = String(normalizeTextField(value) || fallback || 'task')
    .replace(/[\\/:*?"<>|]/g, '_')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || fallback || 'task';
}

function getAttachmentList(recordFields, imageFieldNames) {
  const attachments = [];
  for (const fieldName of imageFieldNames) {
    const value = recordFields[fieldName];
    if (!Array.isArray(value)) continue;
    for (const item of value) {
      if (item && item.file_token) {
        attachments.push({
          fieldName,
          fileToken: item.file_token,
          fileName: item.name || `${fieldName}.bin`
        });
      }
    }
  }
  return attachments;
}

function sanitizeFileName(fileName, fallback = 'image.jpg') {
  const base = path.basename(fileName || fallback);
  const cleaned = base.replace(/[\\/:*?"<>|]/g, '_').trim();
  return cleaned || fallback;
}

function buildUniqueAttachmentName(fileName, index, fallback = 'image.jpg') {
  const safeName = sanitizeFileName(fileName, fallback);
  const ext = path.extname(safeName);
  const stem = ext ? safeName.slice(0, -ext.length) : safeName;
  const suffix = String(index + 1).padStart(2, '0');
  return `${suffix}-${stem || 'image'}${ext || '.jpg'}`;
}

function buildFieldMap(fields) {
  const map = new Map();
  for (const field of fields) {
    map.set(field.field_name, field);
  }
  return map;
}

async function ensureSchema(config, token, fields) {
  const fieldMap = buildFieldMap(fields);
  const createdOrUpdated = [];

  const automationFields = [
    { field_name: config.fields.submittedCount, type: FIELD_TYPE.NUMBER, property: { formatter: '0.0' } },
    { field_name: config.fields.result, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.lastProcessedAt, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.blockedPath, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.executionOwner, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.latestTraceId, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.resultSyncStatus, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.videoAttachment, type: FIELD_TYPE.ATTACHMENT },
    { field_name: config.fields.videoFileName, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.submitTime, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.finishTime, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.errorMessage, type: FIELD_TYPE.TEXT }
  ];

  for (const spec of automationFields) {
    if (!fieldMap.has(spec.field_name)) {
      try {
        await createField(config, token, spec);
        createdOrUpdated.push(`新增字段 ${spec.field_name}`);
      } catch (error) {
        if (!isFieldNameDuplicatedError(error)) {
          throw error;
        }

        const latestMap = buildFieldMap(await listTableFields(config, token));
        if (!latestMap.has(spec.field_name)) {
          throw error;
        }
        for (const [fieldName, field] of latestMap.entries()) {
          fieldMap.set(fieldName, field);
        }
        createdOrUpdated.push(`字段 ${spec.field_name} 已存在，跳过重复创建`);
      }
    }
  }

  const refreshedFields = await listTableFields(config, token);
  const refreshedMap = buildFieldMap(refreshedFields);
  const statusField = refreshedMap.get(config.statusField);

  if (statusField && statusField.type === FIELD_TYPE.SINGLE_SELECT) {
    const existingOptions = statusField.property?.options || [];
    const existingNames = new Set(existingOptions.map(option => option.name));
    const mergedOptions = [...existingOptions];

    for (let i = 0; i < config.statusOptions.length; i++) {
      const statusName = config.statusOptions[i];
      if (!existingNames.has(statusName)) {
        mergedOptions.push({
          name: statusName,
          color: STATUS_COLORS[i % STATUS_COLORS.length]
        });
      }
    }

    if (mergedOptions.length !== existingOptions.length) {
      await updateField(config, token, statusField.field_id, {
        field_name: statusField.field_name,
        type: statusField.type,
        property: {
          options: mergedOptions
        }
      });
      createdOrUpdated.push(`补齐状态选项 ${config.statusOptions.join(' / ')}`);
    }
  }

  return {
    fields: await listTableFields(config, token),
    changes: createdOrUpdated
  };
}

function shouldProcessRecord(record, config, cliArgs) {
  if (cliArgs.recordId && record.record_id !== cliArgs.recordId) {
    return false;
  }

  const fields = record.fields || {};
  const taskName = sanitizeTaskName(fields[config.fields.taskName], record.record_id);
  if (cliArgs.taskNames && !cliArgs.taskNames.includes(taskName)) {
    return false;
  }

  const hasTaskPayload =
    Boolean(normalizeTextField(fields[config.fields.taskName])) ||
    Boolean(normalizeTextField(fields[config.fields.prompt])) ||
    getAttachmentList(fields, config.fields.images || []).length > 0;

  if (!hasTaskPayload) {
    return false;
  }

  if (hasLocalDeliverableSubmissionForRecord(config, record.record_id, taskName)) {
    return false;
  }

  const status = normalizeTextField(fields[config.statusField]);
  const executionOwner = normalizeTextField(fields[config.fields.executionOwner]);
  if (executionOwner && !executionOwnerMatchesMachine(executionOwner, config.machineId)) {
    return false;
  }

  if (!status) {
    return true;
  }

  if (status === STATUS.PROCESSING) {
    return false;
  }

  return (config.pendingStatuses || []).includes(status);
}

function buildTaskContext(record, config) {
  const fields = record.fields || {};
  const repeatTarget = Math.max(1, normalizeNumberField(fields[config.fields.repeatCount], 1));
  const submittedCount = Math.max(0, normalizeNumberField(fields[config.fields.submittedCount], 0));
  const taskName = sanitizeTaskName(fields[config.fields.taskName], record.record_id);

  const executionOwner = normalizeTextField(fields[config.fields.executionOwner]);
  const executionOwnerParsed = parseExecutionOwner(executionOwner);
  const channel = normalizeTextField(fields[config.fields.channel || '渠道']);
  const channelSource = normalizeTextField(fields[config.fields.channelSource || '渠道来源']);

  return {
    recordId: record.record_id,
    record,
    taskName,
    prompt: normalizeTextField(fields[config.fields.prompt]),
    attachments: getAttachmentList(fields, config.fields.images || []),
    allowNoReferenceImage: normalizeBooleanField(fields[config.fields.allowNoReferenceImage]),
    model: normalizeModelName(fields[config.fields.model], config.defaultModel),
    mode: normalizeTextField(fields[config.fields.mode]) || config.defaultMode,
    ratio: normalizeTextField(fields[config.fields.ratio]) || config.defaultRatio,
    duration: Math.max(1, normalizeNumberField(fields[config.fields.duration], config.defaultDuration)),
    repeatTarget,
    submittedCount,
    remainingCount: Math.max(0, repeatTarget - submittedCount),
    currentStatus: normalizeTextField(fields[config.statusField]),
    executionOwner,
    executionOwnerMachineId: executionOwnerParsed.machineId,
    channel,
    channelSource,
    resultSyncStatus: normalizeTextField(fields[config.fields.resultSyncStatus]),
    result: normalizeTextField(fields[config.fields.result]),
    blockedPath: normalizeTextField(fields[config.fields.blockedPath]),
    lastProcessedAt: normalizeTimestampField(fields[config.fields.lastProcessedAt])
  };
}

function sortSubmitContexts(contexts) {
  return (contexts || [])
    .slice()
    .sort((a, b) => {
      const aPriority = a.resultSyncStatus === 'priority_pending' ? 0 : 1;
      const bPriority = b.resultSyncStatus === 'priority_pending' ? 0 : 1;
      if (aPriority !== bPriority) {
        return aPriority - bPriority;
      }

      const aTime = a.lastProcessedAt ? a.lastProcessedAt.getTime() : 0;
      const bTime = b.lastProcessedAt ? b.lastProcessedAt.getTime() : 0;
      if (aTime !== bTime) {
        return aTime - bTime;
      }

      return String(a.taskName || '').localeCompare(String(b.taskName || ''));
    });
}

function contextMatchesChannelFilter(context, config, cliArgs) {
  const channelFilter = normalizeChannelFilter(cliArgs.channel);
  if (!channelFilter) {
    return true;
  }

  const resolved = resolveChannel(context, config);
  if (channelFilter === 'imini') {
    return resolved.channel === 'imini';
  }
  if (channelFilter === 'jimeng') {
    return resolved.channel !== 'imini';
  }

  return String(resolved.channel || '').trim().toLowerCase() === channelFilter;
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

function buildTraceSubmissionPayload(context, config, traceId, submitIndex) {
  const submittedAt = formatBeijingTimestamp();
  const normalizedPrompt = String(context.prompt || '').replace(/\s+/g, '').trim().toLowerCase();
  const contentIdMeta = parseContentIdMetadata(context.prompt, config.contentIdLabel);
  const scriptIdMeta = parseScriptIdMetadata(context.prompt);
  const contentIdEnabled = config.enableContentIdClaim !== false;
  return {
    trace_id: traceId,
    record_id: context.recordId,
    task_name: context.taskName,
    submit_index: submitIndex,
    submit_time: submittedAt,
    status: 'submitting',
    worker_id: buildWorkerId(config),
    execution_owner: context.executionOwner || '',
    execution_machine_id: context.executionOwnerMachineId || config.machineId,
    model: context.model,
    mode: context.mode,
    ratio: context.ratio,
    duration: context.duration,
    repeat_target: context.repeatTarget,
    submitted_count_before: context.submittedCount,
    allow_no_reference_image: context.allowNoReferenceImage === true,
    prompt_length: context.prompt.length,
    prompt_preview: context.prompt.slice(0, 200),
    script_id: scriptIdMeta.id,
    content_id: contentIdEnabled ? contentIdMeta.id : '',
    content_id_label: contentIdEnabled ? contentIdMeta.label : '',
    content_id_found_in_prompt: contentIdEnabled ? contentIdMeta.found : false,
    content_id_label_name: config.contentIdLabel || DEFAULT_CONFIG.contentIdLabel,
    enable_content_id_claim: contentIdEnabled,
    claim_strategy_order: Array.isArray(config.claimStrategyOrder) ? config.claimStrategyOrder : DEFAULT_CONFIG.claimStrategyOrder,
    prompt_anchor: normalizedPrompt.slice(0, 120),
    prompt_hash: crypto.createHash('sha1').update(normalizedPrompt).digest('hex'),
    pre_submit_generating_count: 0,
    post_submit_generating_count: 0,
    submit_confirmed_by: '',
    submit_confirmation_note: '',
    state_updated_at: submittedAt,
    local_file_path: '',
    error_message: ''
  };
}

function findDuplicateSubmissionTrace(config, context, submitIndex) {
  const scriptId = parseScriptIdMetadata(context.prompt).id;
  if (!context?.recordId || !scriptId) {
    return null;
  }

  return listRecentSubmissionRecords(config, config.submitInflightRecentLimit)
    .filter(item => item && item.trace_id)
    .find(item =>
      item.record_id === context.recordId &&
      item.script_id === scriptId &&
      Number(item.submit_index || 0) === Number(submitIndex || 0) &&
      DUPLICATE_GUARD_SUBMISSION_STATUSES.has(item.status)
    ) || null;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function normalizePositiveInt(value, fallback = 1) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(num));
}

async function withOperationTimeout(promise, timeoutMs, label) {
  const safeTimeoutMs = Math.max(10000, Number(timeoutMs) || 10 * 60 * 1000);
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label}超时 (${safeTimeoutMs}ms)`));
        }, safeTimeoutMs);
      })
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function canRetryTimedOutClaim(submission) {
  if (!submission || submission.status !== 'timed_out') {
    return false;
  }

  if (submission.queue_observed) {
    return true;
  }

  const confirmedBy = String(submission.submit_confirmed_by || '').trim();
  return [
    'queue_growth_after_error_deferred',
    'queue_growth_after_error',
    'queue_growth+credits_changed',
    'queue_growth+submit_success',
    'queue_growth+credits_changed_but_unstable'
  ].includes(confirmedBy);
}

function listRecentSubmissionRecords(config, maxRecords) {
  const limit = Number(maxRecords) || 0;
  if (limit <= 0) {
    return listSubmissionRecords(config);
  }

  const dir = getSubmissionsDir(config);
  if (!fs.existsSync(dir)) {
    return [];
  }

  return fs.readdirSync(dir)
    .filter(name => name.endsWith('.json'))
    .map(name => {
      const filePath = path.join(dir, name);
      let mtimeMs = 0;
      try {
        mtimeMs = fs.statSync(filePath).mtimeMs;
      } catch (_) {
        mtimeMs = 0;
      }
      return { name, filePath, mtimeMs };
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
    .slice(0, limit)
    .map(item => {
      try {
        return JSON.parse(fs.readFileSync(item.filePath, 'utf8'));
      } catch (error) {
        return {
          trace_id: path.basename(item.name, '.json'),
          status: 'broken_state',
          error_message: error.message,
          state_file: item.filePath
        };
      }
    });
}

function summarizeInflightSubmissions(config, currentGeneratingCount = null, options = {}) {
  const limit = normalizePositiveInt(config.maxInflightSubmissions, 1);
  const all = listRecentSubmissionRecords(config, options.maxRecords)
    .filter(item => item && item.trace_id);
  const active = all
    .filter(item => ACTIVE_SUBMISSION_STATUSES.has(item.status))
    .sort((a, b) => {
      const aTime = Date.parse(a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.state_updated_at || b.submit_time || 0) || 0;
      return bTime - aTime;
    });

  const retryableClaimFailed = all
    .filter(item => item.status === 'claim_failed')
    .sort((a, b) => {
      const aTime = Date.parse(a.last_claim_checked_at || a.claim_failed_at || a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.last_claim_checked_at || b.claim_failed_at || b.state_updated_at || b.submit_time || 0) || 0;
      return aTime - bTime;
    });

  const retryableTimedOut = all
    .filter(item => canRetryTimedOutClaim(item))
    .sort((a, b) => {
      const aTime = Date.parse(a.last_claim_checked_at || a.timed_out_at || a.first_zero_queue_at || a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.last_claim_checked_at || b.timed_out_at || b.first_zero_queue_at || b.state_updated_at || b.submit_time || 0) || 0;
      return aTime - bTime;
    });

  const pendingClaim = active.filter(item => PENDING_CLAIM_SUBMISSION_STATUSES.has(item.status));
  const blocking = active.filter(item => {
    if (isSubmissionForChannel(item, 'imini')) {
      return false;
    }

    if (SUBMISSION_GATE_BLOCKING_STATUSES.has(item.status)) {
      return true;
    }

    if (item.status === 'submitted' && !item.queue_observed) {
      return true;
    }

    // 已确认进入过队列、但当前页面生成中数量为 0 的任务，后续交给资产认领补偿，
    // 不再继续占用提单闸门。
    if ((item.status === 'submitted' || item.status === 'rendering' || item.status === 'observing') && Number(currentGeneratingCount || 0) <= 0) {
      return false;
    }

    return false;
  });

  return {
    limit,
    all,
    active,
    activeCount: active.length,
    blocking,
    blockingCount: blocking.length,
    pendingClaim,
    pendingClaimCount: pendingClaim.length,
    retryableClaimFailed,
    retryableClaimFailedCount: retryableClaimFailed.length,
    retryableTimedOut,
    retryableTimedOutCount: retryableTimedOut.length
  };
}

function getChannelMaxConcurrentSubmits(config, channel) {
  const channelConfig = (config.channels || {})[channel] || {};
  return normalizePositiveInt(channelConfig.maxConcurrentSubmits, config.maxInflightSubmissions || 1);
}

function isSubmissionForChannel(item, channel) {
  return String(item.channel || item.platform || '').trim().toLowerCase() === channel;
}

function summarizeChannelInflightSubmissions(config, channel, options = {}) {
  const limit = getChannelMaxConcurrentSubmits(config, channel);
  const all = listRecentSubmissionRecords(config, options.maxRecords)
    .filter(item => item && item.trace_id)
    .filter(item => isSubmissionForChannel(item, channel));
  const active = all
    .filter(item => ACTIVE_SUBMISSION_STATUSES.has(item.status))
    .sort((a, b) => {
      const aTime = Date.parse(a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.state_updated_at || b.submit_time || 0) || 0;
      return bTime - aTime;
    });
  const retryableClaimFailed = all
    .filter(item => item.status === 'claim_failed')
    .sort((a, b) => {
      const aTime = Date.parse(a.last_claim_checked_at || a.claim_failed_at || a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.last_claim_checked_at || b.claim_failed_at || b.state_updated_at || b.submit_time || 0) || 0;
      return aTime - bTime;
    });
  const retryableTimedOut = all
    .filter(item => canRetryTimedOutClaim(item))
    .sort((a, b) => {
      const aTime = Date.parse(a.last_claim_checked_at || a.timed_out_at || a.first_zero_queue_at || a.state_updated_at || a.submit_time || 0) || 0;
      const bTime = Date.parse(b.last_claim_checked_at || b.timed_out_at || b.first_zero_queue_at || b.state_updated_at || b.submit_time || 0) || 0;
      return aTime - bTime;
    });
  const pendingClaim = active.filter(item => PENDING_CLAIM_SUBMISSION_STATUSES.has(item.status));

  return {
    limit,
    all,
    active,
    activeCount: active.length,
    pendingClaim,
    pendingClaimCount: pendingClaim.length,
    retryableClaimFailed,
    retryableClaimFailedCount: retryableClaimFailed.length,
    retryableTimedOut,
    retryableTimedOutCount: retryableTimedOut.length
  };
}

function formatTraceSummary(items, maxItems = 3) {
  if (!items || items.length === 0) {
    return '';
  }

  const labels = items.slice(0, maxItems).map(item => {
    const traceId = item.trace_id || 'unknown';
    const taskName = item.task_name || item.record_id || 'unknown';
    return `${taskName}:${item.status}@${traceId}`;
  });
  if (items.length > maxItems) {
    labels.push(`... +${items.length - maxItems}`);
  }
  return labels.join(', ');
}

async function downloadFile(token, fileToken, outputPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outputPath);
    const timeoutMs = getFeishuDownloadTimeoutMs();
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: `/open-apis/drive/v1/medias/${fileToken}/download`,
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`
      }
    }, res => {
      if (res.statusCode !== 200) {
        let errorData = '';
        res.setEncoding('utf8');
        res.on('data', chunk => errorData += chunk);
        res.on('end', () => {
          fs.unlink(outputPath, () => {
            reject(new Error(`下载失败 (${res.statusCode}): ${errorData || '空响应'}`));
          });
        });
        return;
      }

      res.pipe(file);
      file.on('finish', () => {
        file.close(() => resolve(outputPath));
      });
    });

    req.on('error', error => {
      file.close(() => {
        fs.unlink(outputPath, () => reject(error));
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书文件下载超时 (${timeoutMs}ms): ${fileToken}`));
    });

    req.end();
  });
}

async function materializeTask(token, context, config) {
  const taskRoot = path.join(config.runtimeRoot, context.taskName);
  const imageDir = path.join(taskRoot, '图片');
  fs.rmSync(taskRoot, { recursive: true, force: true });
  ensureDir(imageDir);

  const imagePaths = [];
  for (let i = 0; i < context.attachments.length; i++) {
    const attachment = context.attachments[i];
    const outputPath = path.join(
      imageDir,
      buildUniqueAttachmentName(attachment.fileName, i, `image-${i + 1}.jpg`)
    );
    await downloadFile(token, attachment.fileToken, outputPath);
    imagePaths.push(outputPath);
    await sleep(150);
  }

  const preparedImages = prepareImagesForUpload(imagePaths);
  const uploadImages = preparedImages.map(item => item.uploadPath);
  const convertedImages = preparedImages.filter(item => item.converted);

  if (convertedImages.length > 0) {
    console.log(`  ♻️ ${context.taskName}: 已将 ${convertedImages.length} 张高负载图片转换为 JPG 上传副本`);
    convertedImages.forEach((item, index) => {
      console.log(`    [${index + 1}] ${path.basename(item.sourcePath)} -> ${path.basename(item.uploadPath)}`);
    });
  }

  fs.writeFileSync(path.join(taskRoot, 'prompt.txt'), `${context.prompt}\n`);
  fs.writeFileSync(path.join(taskRoot, 'config.json'), `${JSON.stringify({
    model: context.model,
    mode: context.mode,
    ratio: context.ratio,
    duration: context.duration
  }, null, 2)}\n`);

  return {
    name: context.taskName,
    folder: taskRoot,
    images: uploadImages,
    prompt: context.prompt,
    config: {}
  };
}

async function cleanupTaskDir(task) {
  if (!task || !task.folder) return;
  try {
    fs.rmSync(task.folder, { recursive: true, force: true });
  } catch (error) {
    console.log(`  ⚠️ 清理临时目录失败: ${error.message}`);
  }
}

function readBlockedSnapshot(task) {
  const blockedFile = path.join(task.folder, '.blocked');
  if (!fs.existsSync(blockedFile)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(blockedFile, 'utf8'));
  } catch (error) {
    return null;
  }
}

function readFailedSnapshot(task) {
  const failedFile = path.join(task.folder, '.failed');
  if (!fs.existsSync(failedFile)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(failedFile, 'utf8'));
  } catch (error) {
    return null;
  }
}

function buildRecordUpdate(config, fields) {
  return {
    ...fields,
    [config.fields.lastProcessedAt]: formatBeijingTimestamp()
  };
}

function getResumeStatus(context) {
  return context.submittedCount > 0 ? STATUS.PARTIAL : STATUS.PENDING;
}

function getResumeStatusFromSubmissionRecord(submission) {
  const submittedCountBefore = Math.max(0, Number(submission?.submitted_count_before) || 0);
  return submittedCountBefore > 0 ? STATUS.PARTIAL : STATUS.PENDING;
}

function listTaskSubmissionRecords(config, context) {
  return listSubmissionRecords(config).filter(item =>
    item && item.trace_id && (
      item.record_id === context.recordId ||
      item.task_name === context.taskName
    )
  );
}

function hasLocalDeliverableSubmissionForRecord(config, recordId, taskName) {
  return listSubmissionRecords(config).some(item => {
    if (!item || !item.trace_id) {
      return false;
    }
    if (item.record_id !== recordId && item.task_name !== taskName) {
      return false;
    }
    if (item.status === 'uploaded' || item.uploaded_file_token) {
      return true;
    }
    if ((item.status === 'downloaded' || item.status === 'upload_failed') && item.local_file_path && fs.existsSync(item.local_file_path)) {
      return true;
    }
    return false;
  });
}

function countPlatformLimitedRetries(config, context) {
  return listTaskSubmissionRecords(config, context).filter(item =>
    item.submit_confirmed_by === 'platform_limited' ||
    /高峰期|平台限流|无法提交更多任务/.test(String(item.error_message || '')) ||
    /高峰期|平台限流/.test(String(item.submit_confirmation_note || ''))
  ).length;
}

function countSubmitUnconfirmedRetries(config, context) {
  const retryWindowMinutes = Math.max(1, normalizePositiveInt(config.submitUnconfirmedRetryWindowMinutes, 120));
  const cutoff = Date.now() - retryWindowMinutes * 60 * 1000;
  return listTaskSubmissionRecords(config, context).filter(item =>
    {
      const updatedAt = Date.parse(item.state_updated_at || item.submit_time || 0) || 0;
      if (!updatedAt || updatedAt < cutoff) {
        return false;
      }

      return item.submit_confirmed_by === 'submit_unconfirmed' ||
        /提交确认未通过|submit_unconfirmed/.test(String(item.error_message || '')) ||
        /提交确认未通过/.test(String(item.submit_confirmation_note || ''));
    }
  ).length;
}

function countGenerateErrorNoticeRetries(config, context) {
  const retryWindowMinutes = Math.max(1, normalizePositiveInt(config.generateErrorNoticeRetryWindowMinutes, 180));
  const cutoff = Date.now() - retryWindowMinutes * 60 * 1000;
  return listTaskSubmissionRecords(config, context).filter(item => {
    const updatedAt = Date.parse(item.state_updated_at || item.submit_time || 0) || 0;
    if (!updatedAt || updatedAt < cutoff) {
      return false;
    }

    const submitConfirmedBy = String(item.submit_confirmed_by || '');
    const errorMessage = String(item.error_message || '');
    const note = String(item.submit_confirmation_note || '');
    return submitConfirmedBy === 'generate_error_notice' ||
      /0\/1生成完成，1 项失败回到底部/.test(errorMessage) ||
      /0\/1生成完成，1 项失败回到底部|视频未通过审核/.test(errorMessage) ||
      /0\/1生成完成，1 项失败回到底部|视频未通过审核/.test(note);
  }).length;
}

function countTransientSubmitFailureRetries(config, context, kind) {
  const retryWindowMinutes = Math.max(1, normalizePositiveInt(config.transientSubmitFailureRetryWindowMinutes, 240));
  const cutoff = Date.now() - retryWindowMinutes * 60 * 1000;
  return listTaskSubmissionRecords(config, context).filter(item => {
    const updatedAt = Date.parse(item.state_updated_at || item.submit_time || 0) || 0;
    if (!updatedAt || updatedAt < cutoff) {
      return false;
    }

    const submitConfirmedBy = String(item.submit_confirmed_by || '');
    const errorMessage = String(item.error_message || '');
    const note = String(item.submit_confirmation_note || '');
    if (kind === 'upload_failed') {
      return submitConfirmedBy === 'upload_failed' ||
        /参考图片上传失败|图片上传未完成|upload_failed/.test(errorMessage) ||
        /参考图片上传失败|图片上传未完成|upload_failed/.test(note);
    }

    return submitConfirmedBy === 'recoverable_page_error' ||
      /Runtime\.callFunctionOn timed out|Protocol error|Execution context was destroyed|页面上下文/.test(errorMessage) ||
      /Runtime\.callFunctionOn timed out|Protocol error|Execution context was destroyed|页面上下文/.test(note);
  }).length;
}

async function markTaskBlockedForManualReview({
  config,
  token,
  context,
  traceId,
  resultMessage,
  errorMessage,
  submitConfirmedBy = 'manual_review_required',
  submissionObservation = null
}) {
  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'blocked',
    post_submit_generating_count: submissionObservation?.maxGeneratingCount || 0,
    submit_confirmed_by: submitConfirmedBy,
    submit_confirmation_note: resultMessage,
    error_message: errorMessage,
    state_updated_at: formatBeijingTimestamp()
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交确认未通过，已转人工复核阻塞');

  await updateStatus(config, token, context.recordId, STATUS.BLOCKED, {
    [config.fields.result]: resultMessage,
    [config.fields.blockedPath]: '',
    [config.fields.resultSyncStatus]: 'blocked',
    [config.fields.errorMessage]: errorMessage
  });
}

async function markTaskRetryPendingForSubmitUnconfirmed({
  config,
  token,
  context,
  traceId,
  resultMessage,
  errorMessage,
  submitConfirmedBy = 'submit_unconfirmed',
  submissionObservation = null
}) {
  const resumeStatus = getResumeStatus(context);
  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'retry_pending',
    post_submit_generating_count: submissionObservation?.maxGeneratingCount || 0,
    submit_confirmed_by: submitConfirmedBy,
    submit_confirmation_note: resultMessage,
    error_message: errorMessage,
    state_updated_at: formatBeijingTimestamp()
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交确认未通过，等待自动重试');

  await updateStatus(config, token, context.recordId, resumeStatus, {
    [config.fields.result]: resultMessage,
    [config.fields.blockedPath]: '',
    [config.fields.resultSyncStatus]: 'pending_retry',
    [config.fields.errorMessage]: errorMessage
  });
}

async function markTaskRetryPendingForGenerateErrorNotice({
  config,
  token,
  context,
  traceId,
  resultMessage,
  errorMessage,
  submitConfirmedBy = 'generate_error_notice'
}) {
  const resumeStatus = getResumeStatus(context);
  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'retry_pending',
    submit_confirmed_by: submitConfirmedBy,
    submit_confirmation_note: resultMessage,
    error_message: errorMessage,
    state_updated_at: formatBeijingTimestamp()
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦生成后即时失败，等待自动重试');

  await updateStatus(config, token, context.recordId, resumeStatus, {
    [config.fields.result]: resultMessage,
    [config.fields.blockedPath]: '',
    [config.fields.resultSyncStatus]: 'pending_retry',
    [config.fields.errorMessage]: errorMessage
  });
}

async function markTaskRetryPendingForTransientSubmitFailure({
  config,
  token,
  context,
  traceId,
  resultMessage,
  errorMessage,
  submitConfirmedBy
}) {
  const resumeStatus = getResumeStatus(context);
  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'retry_pending',
    submit_confirmed_by: submitConfirmedBy,
    submit_confirmation_note: resultMessage,
    error_message: errorMessage,
    state_updated_at: formatBeijingTimestamp()
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦瞬时提交失败，等待自动重试');

  await updateStatus(config, token, context.recordId, resumeStatus, {
    [config.fields.result]: resultMessage,
    [config.fields.blockedPath]: '',
    [config.fields.resultSyncStatus]: 'pending_retry',
    [config.fields.errorMessage]: errorMessage
  });
}

async function markTaskObservedForDelayedClaim({
  config,
  token,
  context,
  traceId,
  resultMessage,
  errorMessage,
  beforeGeneratingCount,
  afterGeneratingCount,
  submitConfirmedBy = 'queue_growth_after_error_deferred'
}) {
  const newSubmittedCount = context.submittedCount + 1;
  const done = newSubmittedCount >= context.repeatTarget;
  const now = formatBeijingTimestamp();

  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'observing',
    queue_observed: true,
    queue_observed_at: now,
    observed_generating_count: afterGeneratingCount,
    post_submit_generating_count: afterGeneratingCount,
    submit_confirmed_by: submitConfirmedBy,
    submit_confirmation_note: resultMessage,
    error_message: errorMessage,
    state_updated_at: now
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交异常但确认已进队，转待观察认领');

  await updateStatus(config, token, context.recordId, done ? STATUS.SUBMITTED : STATUS.PARTIAL, {
    [config.fields.submittedCount]: newSubmittedCount,
    [config.fields.result]: resultMessage,
    [config.fields.latestTraceId]: traceId,
    [config.fields.blockedPath]: '',
    [config.fields.resultSyncStatus]: 'observing',
    [config.fields.errorMessage]:
      errorMessage || `提交结果待观察，已记录队列从 ${beforeGeneratingCount} 增长到 ${afterGeneratingCount}`
  });
}

async function activateSubmitPauseForInsufficientCredits({
  config,
  token,
  context,
  traceId,
  result
}) {
  const now = formatBeijingTimestamp();
  const resumeStatus = getResumeStatus(context);
  const pauseMessage = `检测到即梦积分不足，本机已暂停后续提单；当前任务已释放归属并恢复为“${resumeStatus}”，等待人工恢复`;
  const errorMessage = result?.error || '积分不足，无法继续提交';

  const updatedSubmission = updateSubmissionRecord(config, traceId, {
    status: 'retry_pending',
    submit_confirmed_by: 'insufficient_credits',
    submit_confirmation_note: pauseMessage,
    error_message: errorMessage,
    state_updated_at: now
  });
  syncAutomationTaskFromJimeng(updatedSubmission, '即梦积分不足，提交暂停等待后续重试');

  await updateStatus(config, token, context.recordId, resumeStatus, {
    [config.fields.result]: pauseMessage,
    [config.fields.blockedPath]: '',
    [config.fields.executionOwner]: '',
    [config.fields.resultSyncStatus]: 'pending_retry',
    [config.fields.errorMessage]: errorMessage
  });

  const pauseFile = writeSubmitPauseState(config, {
    paused_at: now,
    reason: 'insufficient_credits',
    machine_id: config.machineId,
    record_id: context.recordId,
    task_name: context.taskName,
    trace_id: traceId,
    error_message: errorMessage,
    resume_status: resumeStatus
  });

  return {
    pauseFile,
    resumeStatus,
    pauseMessage,
    errorMessage
  };
}

function activateSubmitPauseForLowCreditsAfterSuccess({
  config,
  context,
  traceId,
  result
}) {
  const now = formatBeijingTimestamp();
  const threshold = Math.max(0, Number(config.insufficientCreditsThreshold) || 0);
  const afterCreditsValue = Number(result?.afterCreditsValue);
  const errorMessage = Number.isFinite(afterCreditsValue)
    ? `当前积分 ${afterCreditsValue} 低于阈值 ${threshold}，已暂停后续提单`
    : `当前积分已低于阈值 ${threshold}，已暂停后续提单`;

  const pauseFile = writeSubmitPauseState(config, {
    paused_at: now,
    reason: 'low_credits_threshold',
    machine_id: config.machineId,
    record_id: context.recordId,
    task_name: context.taskName,
    trace_id: traceId,
    error_message: errorMessage,
    resume_status: null,
    threshold,
    after_credits: Number.isFinite(afterCreditsValue) ? afterCreditsValue : null
  });

  return {
    pauseFile,
    errorMessage
  };
}

function isProcessingRecordStale(context, config, now = Date.now()) {
  if (context.currentStatus !== STATUS.PROCESSING) {
    return false;
  }

  if (!context.lastProcessedAt) {
    return true;
  }

  const staleAfterMs = Math.max(1, config.recoverStaleProcessingMinutes || 30) * 60 * 1000;
  return now - context.lastProcessedAt.getTime() >= staleAfterMs;
}

async function recoverInterruptedRecords(config, token, records) {
  const contexts = records.map(record => buildTaskContext(record, config));
  const staleContexts = contexts.filter(context =>
    context.remainingCount > 0 &&
    isProcessingRecordStale(context, config) &&
    (!context.executionOwnerMachineId || context.executionOwnerMachineId === config.machineId)
  );

  for (const context of staleContexts) {
    const resumeStatus = getResumeStatus(context);
    await updateStatus(config, token, context.recordId, resumeStatus, {
      [config.fields.result]:
        `检测到上次运行中断，已从“处理中”恢复为“${resumeStatus}”，准备从 ${context.submittedCount}/${context.repeatTarget} 继续`,
      [config.fields.blockedPath]: ''
    });
  }

  return staleContexts;
}

function isSubmittingTraceStale(submission, config, now = Date.now()) {
  if (!submission || submission.status !== 'submitting') {
    return false;
  }

  const baseTime = Date.parse(submission.state_updated_at || submission.submit_time || 0) || 0;
  if (!baseTime) {
    return false;
  }

  const staleAfterMs = Math.max(1, normalizePositiveInt(config.submittingTimeoutMinutes, 30)) * 60 * 1000;
  return now - baseTime >= staleAfterMs;
}

async function recoverStaleSubmittingTraces(config, token, records) {
  const recordMap = new Map((records || []).map(record => [record.record_id, record]));
  const now = formatBeijingTimestamp();
  const staleSubmissions = listSubmissionRecords(config).filter(submission => {
    if (!submission || !submission.trace_id || !isSubmittingTraceStale(submission, config)) {
      return false;
    }

    if (submission.execution_machine_id) {
      return submission.execution_machine_id === config.machineId;
    }

    if (submission.execution_owner) {
      return executionOwnerMatchesMachine(submission.execution_owner, config.machineId);
    }

    return true;
  });

  const recovered = [];

  for (const submission of staleSubmissions) {
    const resumeStatus = getResumeStatusFromSubmissionRecord(submission);
    const recoverMessage = `检测到提交过程长时间停留在 submitting，已自动恢复为“${resumeStatus}”，等待后续重试`;
    const recoverError = submission.error_message || '提交过程长时间未完成确认，已自动恢复为待重试';

    updateSubmissionRecord(config, submission.trace_id, {
      status: 'retry_pending',
      submit_confirmed_by: submission.submit_confirmed_by || 'submit_interrupted_recovered',
      submit_confirmation_note: submission.submit_confirmation_note || recoverMessage,
      error_message: recoverError,
      state_updated_at: now
    });

    const record = recordMap.get(submission.record_id);
    const currentStatus = normalizeTextField(record?.fields?.[config.statusField]);
    const extraFields = {
      [config.fields.result]: recoverMessage,
      [config.fields.blockedPath]: '',
      [config.fields.resultSyncStatus]: 'pending_retry',
      [config.fields.errorMessage]: recoverError
    };

    if (submission.record_id) {
      if (!currentStatus || currentStatus === STATUS.PROCESSING) {
        await updateStatus(config, token, submission.record_id, resumeStatus, extraFields);
      } else if ((config.pendingStatuses || []).includes(currentStatus)) {
        await updateRecord(config, token, submission.record_id, buildRecordUpdate(config, extraFields));
      }
    }

    recovered.push({
      traceId: submission.trace_id,
      taskName: submission.task_name || submission.record_id || submission.trace_id,
      resumeStatus
    });
  }

  return recovered;
}

async function connectBrowser(config, mode = 'submit') {
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
  const targetRole = mode === 'download'
    ? DOWNLOAD_TAB_ROLE
    : (mode === 'imini-submit'
      ? IMINI_SUBMIT_TAB_ROLE
      : (mode === 'imini-download' ? IMINI_DOWNLOAD_TAB_ROLE : SUBMIT_TAB_ROLE));

  if (mode !== 'download' && mode !== 'imini-submit' && mode !== 'imini-download') {
    for (const candidate of pages) {
      if (
        candidate.url().includes('/ai-tool/generate') &&
        candidate.url().includes('workspace=0') &&
        await pageLooksLikeVideoSubmitToolbar(candidate)
      ) {
        page = candidate;
        break;
      }
    }
  }

  if (!page) {
    for (const candidate of pages) {
      if (await pageHasRole(candidate, targetRole)) {
        page = candidate;
        break;
      }
    }
  }

  if (!page) {
    if (mode === 'download') {
      page = pages.find(p => p.url().includes('/ai-tool/asset')) || null;
    } else if (mode === 'imini-download') {
      page = pages.find(p => p.url().includes('imini.com') && p.url().includes('/asset')) ||
        pages.find(p => p.url().includes('imini.com')) ||
        null;
    } else if (mode === 'imini-submit') {
      page = pages.find(p => p.url().includes('imini.com')) || null;
    } else {
      page =
        pages.find(p => p.url().includes('/ai-tool/generate') && p.url().includes('workspace=0')) ||
        pages.find(p => p.url().includes('/ai-tool/home') && p.url().includes('type=video')) ||
        null;
    }
  }

  if (!page) page = await browser.newPage();
  await prepareAutomationPage(page);
  if (mode === 'download') {
    await markPageRole(page, DOWNLOAD_TAB_ROLE);
  } else if (mode === 'imini-download') {
    await markPageRole(page, IMINI_DOWNLOAD_TAB_ROLE);
  } else if (mode === 'imini-submit') {
    await markPageRole(page, IMINI_SUBMIT_TAB_ROLE);
  } else {
    await resetVideoGenerationPage(page, config.baseUrl);
    await markPageRole(page, SUBMIT_TAB_ROLE);
  }
  return { browser, page };
}

async function getIminiSubmitPage(browser, config) {
  const pages = await browser.pages();
  let page = null;

  for (const candidate of pages) {
    if (await pageHasRole(candidate, IMINI_SUBMIT_TAB_ROLE)) {
      page = candidate;
      break;
    }
  }

  if (!page) {
    page = pages.find(candidate => candidate.url().includes('imini.com')) || null;
  }

  if (!page) {
    page = await browser.newPage();
  }

  await prepareAutomationPage(page);
  await markPageRole(page, IMINI_SUBMIT_TAB_ROLE);
  return page;
}

async function pageLooksLikeVideoSubmitToolbar(page) {
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
      const visibleText = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span'))
        .filter(isVisible)
        .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
        .filter(Boolean)
        .join(' ');
      return (
        visibleText.includes('视频生成') &&
        visibleText.includes('Seedance') &&
        /\b9:16\b|\b16:9\b|\b1:1\b/.test(visibleText)
      );
    });
  } catch (error) {
    return false;
  }
}

function isRecoverableSubmitPageError(error) {
  const message = String(error?.message || error || '');
  return (
    message.includes('Runtime.callFunctionOn timed out') ||
    message.includes('Protocol error (Runtime.callFunctionOn)') ||
    message.includes('Protocol error (DOM.describeNode)') ||
    message.includes('Execution context was destroyed') ||
    message.includes('Cannot find context with specified id') ||
    message.includes('Inspected target navigated or closed') ||
    message.includes('Most likely the page has been closed')
  );
}

async function processTaskWithRecoverableRetry(page, task, options, config, beforeGeneratingCount) {
  const initialResult = await processTask(page, task, options, true);
  if (initialResult.success || !isRecoverableSubmitPageError(initialResult.error || initialResult.message || initialResult)) {
    return initialResult;
  }

  let queueObservation = null;
  try {
    queueObservation = await observeQueueAfterSubmit(page, config, beforeGeneratingCount);
  } catch (error) {
    console.log(`  ⚠️ 超时后补查队列失败，将按未进队处理重试: ${error.message}`);
  }

  if (queueObservation?.queueIncreased) {
    console.log(
      `  ⚠️ 提交流程虽报页面超时，但已观测到队列增长 (${beforeGeneratingCount} -> ${queueObservation.maxGeneratingCount})，不重复提交`
    );
    return {
      ...initialResult,
      queueObservedAfterRecoverableError: queueObservation
    };
  }

  console.log(`  ⚠️ 提交流程遇到可恢复页面超时，刷新页面后重试一次: ${initialResult.error || 'unknown error'}`);
  await refreshPageAfterWait(page, config, '提交阶段页面上下文超时');
  const retryResult = await processTask(page, task, options, true);
  return {
    ...retryResult,
    recoveredFromPageError: true,
    firstAttemptError: initialResult.error || initialResult.message || ''
  };
}

async function cleanupFailedSubmitSession(page, reason) {
  try {
    return await cleanupRecentUnnamedConversations(page, {
      reason,
      maxDelete: 2
    });
  } catch (error) {
    console.log(`  ⚠️ 清理失败尝试未命名会话失败: ${error.message}`);
    return { deleted: 0, reason, error: error.message };
  }
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
    // Ignore pages that cannot be tagged yet; the URL fallback still works.
  }
}

async function readGeneratingStatus(browser, page, config, options = {}) {
  const { downloadOnly = false, skipGeneratingProbe = false } = options;
  if (skipGeneratingProbe) {
    return {
      generating: 0,
      source: 'skipped',
      rawText: ''
    };
  }
  if (!downloadOnly) {
    return checkGeneratingStatus(page);
  }

  let probePage = null;
  try {
    probePage = await browser.newPage();
    try {
      await probePage.setViewport({ width: 1600, height: 1000 });
    } catch (error) {
      // 某些连接场景下无需设置 viewport
    }

    console.log('📍 download-only：使用临时探测页读取生成中数量，不切换主下载页');
    await probePage.goto(config.baseUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 45000
    });
    await sleep(1500);
    return await checkGeneratingStatus(probePage, true);
  } catch (error) {
    console.log(`  ⚠️ 临时探测页读取生成中数量失败，按 0 个任务继续下载线判断: ${error.message}`);
    return {
      generating: 0,
      source: 'probe-failed',
      rawText: ''
    };
  } finally {
    if (probePage) {
      try {
        await probePage.close();
      } catch (error) {
        // 忽略探测页关闭失败
      }
    }
  }
}

async function updateStatus(config, token, recordId, status, extraFields = {}) {
  return updateRecord(
    config,
    token,
    recordId,
    buildRecordUpdate(config, {
      [config.statusField]: status,
      ...extraFields
    })
  );
}

async function getRecord(config, token, recordId) {
  const data = await requestJson(
    'GET',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}?_ts=${Date.now()}`,
    token
  );
  return data.record || null;
}

async function claimRecordOwnership(config, token, context) {
  if (context.executionOwnerMachineId && context.executionOwnerMachineId !== config.machineId) {
    return {
      ok: false,
      reason: `记录已归属 ${context.executionOwnerMachineId}`
    };
  }

  const claimToken = buildOwnershipClaimToken(config);
  await updateStatus(config, token, context.recordId, STATUS.PROCESSING, {
    [config.fields.executionOwner]: claimToken,
    [config.fields.result]:
      `已由 ${config.machineId} 认领，准备执行第 ${context.submittedCount + 1}/${context.repeatTarget} 次`,
    [config.fields.errorMessage]: ''
  });

  const latestRecord = await getRecord(config, token, context.recordId);
  const latestFields = latestRecord?.fields || {};
  const latestStatus = normalizeTextField(latestFields[config.statusField]);
  const latestOwner = normalizeTextField(latestFields[config.fields.executionOwner]);
  if (latestStatus !== STATUS.PROCESSING || latestOwner !== claimToken) {
    return {
      ok: false,
      reason: `认领确认失败（status=${latestStatus || '空'}, owner=${latestOwner || '空'}）`,
      latestRecord
    };
  }

  return {
    ok: true,
    claimToken,
    latestRecord
  };
}

async function ensureClaimOwnership(config, token, recordId, claimToken) {
  const latestRecord = await getRecord(config, token, recordId);
  const latestFields = latestRecord?.fields || {};
  const latestOwner = normalizeTextField(latestFields[config.fields.executionOwner]);
  const latestStatus = normalizeTextField(latestFields[config.statusField]);
  return executionOwnerMatchesMachine(latestOwner, claimToken) && latestStatus === STATUS.PROCESSING;
}

async function refreshTaskContext(config, token, recordId, cliArgs) {
  const latestRecord = await getRecord(config, token, recordId);
  if (!latestRecord) {
    return null;
  }
  if (!shouldProcessRecord(latestRecord, config, cliArgs)) {
    return {
      record: latestRecord,
      context: buildTaskContext(latestRecord, config),
      shouldProcess: false
    };
  }
  return {
    record: latestRecord,
    context: buildTaskContext(latestRecord, config),
    shouldProcess: true
  };
}

async function refreshPageAfterWait(page, config, reason, mode = 'submit') {
  try {
    console.log(`🔄 等待结束，刷新即梦页面后继续检测 (${reason})...`);
    if (mode === 'download') {
      await page.goto(config.assetUrl || DEFAULT_ASSET_URL, {
        waitUntil: 'domcontentloaded',
        timeout: 45000
      });
    } else {
      await resetVideoGenerationPage(page, config.baseUrl);
    }
    await sleep(1500);
  } catch (error) {
    console.log(`  ⚠️ 刷新即梦页面失败，将直接继续下一轮检测: ${error.message}`);
  }
}

function getQueueFullWaitMinutes(config) {
  return Math.max(1, Number(config.queueFullCheckIntervalMinutes) || 30);
}

function getAssetScanIntervalMinutes(config, channelFilter = '') {
  const channel = normalizeChannelFilter(channelFilter);
  if (channel === 'imini') {
    const iminiConfig = getChannelConfig(config, 'imini');
    return Math.max(1, Number(iminiConfig.assetScanIntervalMinutes) || 30);
  }
  return Math.max(1, Number(config.assetScanIntervalMinutes) || 120);
}

function getConcurrentAssetScanIntervalMinutes(config, channelFilter = '') {
  const channel = normalizeChannelFilter(channelFilter);
  if (channel === 'imini') {
    const iminiConfig = getChannelConfig(config, 'imini');
    return Math.max(
      1,
      Number(iminiConfig.concurrentAssetScanIntervalMinutes) ||
        Number(iminiConfig.assetScanIntervalMinutes) ||
        30
    );
  }
  return Math.max(1, Number(config.concurrentAssetScanIntervalMinutes) || 120);
}

function getAssetScanCachePath(config) {
  return path.join(getStateRoot(config), 'asset-scan-cache.json');
}

function getAssetScanCacheKey(channelFilter = '') {
  const channel = normalizeChannelFilter(channelFilter);
  return channel === 'imini' ? 'imini' : 'default';
}

function loadLastAssetScanAt(config, channelFilter = '') {
  const filePath = getAssetScanCachePath(config);
  if (!fs.existsSync(filePath)) {
    return 0;
  }

  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const key = getAssetScanCacheKey(channelFilter);
    const channelValue = raw.entries?.[key]?.last_asset_scan_at;
    const fallbackValue = key === 'default' ? raw.last_asset_scan_at : '';
    const value = Date.parse(channelValue || fallbackValue || '');
    return Number.isFinite(value) ? value : 0;
  } catch (_) {
    return 0;
  }
}

function saveLastAssetScanAt(config, timestamp = new Date(), channelFilter = '') {
  const filePath = getAssetScanCachePath(config);
  let cache = { entries: {} };
  try {
    if (fs.existsSync(filePath)) {
      const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      if (raw && typeof raw === 'object') {
        cache = raw;
      }
    }
  } catch (_) {
    cache = { entries: {} };
  }

  const iso = timestamp instanceof Date ? timestamp.toISOString() : new Date(timestamp).toISOString();
  const key = getAssetScanCacheKey(channelFilter);
  cache.entries = cache.entries && typeof cache.entries === 'object' ? cache.entries : {};
  cache.entries[key] = {
    ...(cache.entries[key] || {}),
    last_asset_scan_at: iso
  };
  if (key === 'default') {
    cache.last_asset_scan_at = iso;
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(cache, null, 2)}\n`);
}

async function stabilizeGeneratingBaseline(page, config, observedCount) {
  const attempts = normalizePositiveInt(config.preSubmitQueueCheckAttempts, 3);
  const intervalMs = Math.max(500, Number(config.preSubmitQueueCheckIntervalMs) || 1500);
  const samples = [{ index: 1, generating: observedCount, at: formatBeijingTimestamp(), source: 'loop' }];
  let baseline = observedCount;
  let lastError = '';

  for (let index = 1; index < attempts; index++) {
    await sleep(intervalMs);
    try {
      const status = await checkGeneratingStatus(page, true);
      const generating = status?.generating || 0;
      baseline = Math.max(baseline, generating);
      samples.push({
        index: index + 1,
        generating,
        at: formatBeijingTimestamp(),
        source: 'pre_submit_probe'
      });
    } catch (error) {
      lastError = error.message;
      samples.push({
        index: index + 1,
        generating: null,
        at: formatBeijingTimestamp(),
        source: 'pre_submit_probe',
        error: error.message
      });
    }
  }

  return {
    baseline,
    samples,
    lastError
  };
}

async function observeQueueAfterSubmit(page, config, beforeGeneratingCount) {
  const attempts = normalizePositiveInt(config.postSubmitQueueCheckAttempts, 4);
  const intervalMs = Math.max(500, Number(config.postSubmitQueueCheckIntervalMs) || 3000);
  const samples = [];
  let maxGeneratingCount = beforeGeneratingCount;
  let lastError = '';

  for (let index = 0; index < attempts; index++) {
    if (index > 0) {
      await sleep(intervalMs);
    }

    try {
      const status = await checkGeneratingStatus(page, true);
      const generating = status?.generating || 0;
      maxGeneratingCount = Math.max(maxGeneratingCount, generating);
      samples.push({
        index: index + 1,
        generating,
        at: formatBeijingTimestamp()
      });

      if (generating > beforeGeneratingCount) {
        return {
          queueIncreased: true,
          maxGeneratingCount,
          samples,
          lastError
        };
      }
    } catch (error) {
      lastError = error.message;
      samples.push({
        index: index + 1,
        generating: null,
        at: formatBeijingTimestamp(),
        error: error.message
      });
    }
  }

  return {
    queueIncreased: maxGeneratingCount > beforeGeneratingCount,
    maxGeneratingCount,
    samples,
    lastError
  };
}

async function main() {
  const cliArgs = parseArgs(process.argv.slice(2));
  const config = loadConfig(cliArgs.configPath);

  if (!config.appId || !config.appSecret || !config.appToken || !config.tableId) {
    throw new Error('飞书配置不完整：需要 appId、appSecret、appToken、tableId');
  }

  ensureDir(config.runtimeRoot);
  ensureStateDirs(config);

  console.log('📋 飞书直连即梦监测');
  console.log('================================');
  console.log(`配置文件: ${cliArgs.configPath}`);
  console.log(`表格: ${config.appToken}/${config.tableId}`);
  if (config.viewId) {
    console.log(`视图: ${config.viewId}`);
  }
  console.log(`运行目录: ${config.runtimeRoot}`);
  if (cliArgs.dryRun) {
    console.log('模式: 仅演练，不提交即梦');
  }

  const token = await getAccessToken(config);
  let fields = await listTableFields(config, token);

  if (config.ensureSchema && !cliArgs.dryRun) {
    const ensured = await ensureSchema(config, token, fields);
    fields = ensured.fields;
    for (const change of ensured.changes) {
      console.log(`🧩 ${change}`);
    }
  }

  if (cliArgs.ensureSchemaOnly) {
    console.log('✅ 飞书字段检查/补齐完成，按要求退出');
    return;
  }

  if (cliArgs.dryRun) {
    let records;
    if (cliArgs.submitOnly && !cliArgs.recordId && !cliArgs.taskNames) {
      try {
        records = await listSubmitCandidateRecords(config, token);
        console.log(`待处理候选查询: ${records.length} 条`);
      } catch (error) {
        console.log(`⚠️ 待处理候选查询失败，回退全视图扫描: ${error.message}`);
        records = await listAllRecords(config, token);
      }
    } else {
      records = await listAllRecords(config, token);
    }
    const contexts = records
      .map(record => buildTaskContext(record, config));
    const staleContexts = contexts.filter(context =>
      context.remainingCount > 0 && isProcessingRecordStale(context, config)
    );
    const target = sortSubmitContexts(contexts
      .filter(context =>
        (context.remainingCount > 0 && shouldProcessRecord(context.record, config, cliArgs)) ||
        staleContexts.some(item => item.recordId === context.recordId)
      ));
    const slicedTarget = cliArgs.limit ? target.slice(0, cliArgs.limit) : target;

    console.log(`待处理记录: ${slicedTarget.length}`);
    console.log(`可恢复处理中记录: ${staleContexts.length}`);
    for (const context of slicedTarget) {
      const resumeHint = staleContexts.some(item => item.recordId === context.recordId)
        ? ' | 将自动恢复续跑'
        : '';
      const noReferenceHint = context.allowNoReferenceImage ? ' | 免参考图=是' : '';
      const channelDecision = resolveChannel(context, config);
      const channelHint = channelDecision.channel && channelDecision.channel !== '即梦'
        ? ` | 渠道=${channelDecision.channel}(${channelDecision.source})`
        : '';
      console.log(`- ${context.taskName} | 状态=${context.currentStatus || '空'} | 已提交=${context.submittedCount}/${context.repeatTarget} | 图片=${context.attachments.length} | 提示词=${context.prompt.length} 字符${channelHint}${noReferenceHint}${resumeHint}`);
    }
    return;
  }

  const resumeOnly = cliArgs.resumeOnly === true;
  const oneShot = cliArgs.oneShot === true;
  const scheduled = cliArgs.scheduled === true;
  const submitOnly = cliArgs.submitOnly === true;
  const downloadOnly = cliArgs.downloadOnly === true;
  const channelFilter = normalizeChannelFilter(cliArgs.channel);
  const submitPauseState = readSubmitPauseState(config);

  if (submitOnly && downloadOnly) {
    throw new Error('不能同时启用 --submit-only 和 --download-only');
  }

  if (
    submitPauseState &&
    !downloadOnly &&
    !resumeOnly &&
    channelFilter !== 'imini' &&
    config.submitPauseOnInsufficientCredits !== false
  ) {
    console.log('⛔ 当前提单已因积分不足被暂停，跳过本轮提单。');
    console.log(`   暂停时间: ${submitPauseState.paused_at || '未知'}`);
    console.log(`   触发任务: ${submitPauseState.task_name || submitPauseState.record_id || '未知'}`);
    console.log(`   原因: ${submitPauseState.error_message || submitPauseState.reason || '积分不足'}`);
    console.log(`   手动恢复: 删除 ${getSubmitPauseFilePath(config)} 后，再重新触发 submit 线`);
    return;
  }

  if (submitOnly) {
    console.log('🧾 当前为 submit-only 模式：只负责提单，不执行资产页下载');
  } else if (downloadOnly) {
    console.log('📥 当前为 download-only 模式：只负责资产页认领与回写，不新增提单');
  }

  const scheduleMode = downloadOnly ? 'download-only' : (submitOnly ? 'submit-only' : 'mixed');
  if (oneShot && scheduled && !shouldRunScheduledOneShot(new Date(), scheduleMode, config, channelFilter)) {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    console.log(`⏭️ 当前为定时 one-shot，时间 ${hh}:${mm} 不在本轮执行窗口内，直接退出`);
    return;
  }

  if (resumeOnly) {
    const inflightSummary = summarizeInflightSubmissions(config);
    console.log('♻️ 当前为 resume-only 模式：只续跑未闭环 trace，不新增提交');
    console.log(`   未闭环提交: ${inflightSummary.activeCount}/${inflightSummary.limit}`);
    if (inflightSummary.retryableClaimFailedCount > 0) {
      console.log(`   待低优先级补回写: ${inflightSummary.retryableClaimFailedCount}`);
    }
    if (
      inflightSummary.activeCount === 0 &&
      inflightSummary.retryableClaimFailedCount === 0 &&
      inflightSummary.retryableTimedOutCount === 0
    ) {
      console.log('✅ resume-only 模式下未发现未闭环提交，跳过连接 Chrome');
      return;
    }
  }

  if (channelFilter) {
    console.log(`🧭 当前渠道过滤: ${channelFilter === 'jimeng' ? '即梦' : channelFilter}`);
  }

  const browserMode = downloadOnly ? (channelFilter === 'imini' ? 'imini-download' : 'download') : (channelFilter === 'imini' ? 'imini-submit' : 'submit');
  const { browser, page } = await connectBrowser(config, browserMode);
  let justSubmitted = false;
  let submittedThisRun = 0;
  let loopCount = 0;
  let lastAssetScanAt = loadLastAssetScanAt(config, channelFilter);

  try {
    while (true) {
      loopCount++;
      console.log(`\n========================================`);
      console.log(`🔄 检测循环 #${loopCount}`);
      console.log('========================================');

      if (justSubmitted) {
        console.log('⏳ 等待 10 秒后获取生成中数量...');
        await sleep(10000);
        justSubmitted = false;
      }

      console.log(channelFilter === 'imini' && submitOnly ? '📊 imini 提单线跳过即梦生成中读取...' : '📊 获取生成中数量...');
      const queueStatus = channelFilter === 'imini' && submitOnly
        ? { generating: 0, limited: false }
        : await readGeneratingStatus(browser, page, config, {
            downloadOnly,
            skipGeneratingProbe: downloadOnly && channelFilter === 'imini'
          });
      const generatingCount = queueStatus.generating || 0;

      if (submitOnly && generatingCount >= config.maxConcurrent) {
        console.log(`   生成中: ${generatingCount}`);
        console.log(`ℹ️ submit-only：即梦生成中已达到最大并发数 (${generatingCount}/${config.maxConcurrent})，继续扫描飞书以检查 imini 是否有独立空位`);
      }

      const useSubmitCandidateScan =
        submitOnly &&
        !resumeOnly &&
        !cliArgs.recordId &&
        !cliArgs.taskNames;

      let records = [];
      if (useSubmitCandidateScan) {
        console.log('📄 开始读取飞书待处理候选记录...');
        try {
          records = await listSubmitCandidateRecords(config, token);
          console.log(`📄 飞书待处理候选读取完成: ${records.length} 条`);
        } catch (error) {
          console.log(`⚠️ 待处理候选查询失败，回退全视图扫描: ${error.message}`);
          records = await listAllRecords(config, token);
          console.log(`📄 飞书记录读取完成: ${records.length} 条`);
        }
        console.log('♻️ 检查并恢复卡在 submitting 的本地 trace...');
        const recoveredStaleSubmissions = await recoverStaleSubmittingTraces(config, token, records);
        if (recoveredStaleSubmissions.length > 0) {
          console.log(`♻️ 已回收 ${recoveredStaleSubmissions.length} 条卡在 submitting 的记录: ${recoveredStaleSubmissions.map(item => item.taskName).join(', ')}`);
          console.log('📄 回收 trace 后重新读取飞书待处理候选记录...');
          records = await listSubmitCandidateRecords(config, token);
          console.log(`📄 飞书待处理候选重新读取完成: ${records.length} 条`);
        }
      } else {
        console.log('📄 开始读取飞书记录...');
        records = await listAllRecords(config, token);
        console.log(`📄 飞书记录读取完成: ${records.length} 条`);
        console.log('♻️ 检查并恢复中断中的飞书记录...');
        const recovered = await recoverInterruptedRecords(config, token, records);
        if (recovered.length > 0) {
          console.log(`♻️ 已恢复 ${recovered.length} 条中断中的记录: ${recovered.map(item => item.taskName).join(', ')}`);
          console.log('📄 恢复后重新读取飞书记录...');
          records = await listAllRecords(config, token);
          console.log(`📄 飞书记录重新读取完成: ${records.length} 条`);
        }
        console.log('♻️ 检查并恢复卡在 submitting 的本地 trace...');
        const recoveredStaleSubmissions = await recoverStaleSubmittingTraces(config, token, records);
        if (recoveredStaleSubmissions.length > 0) {
          console.log(`♻️ 已回收 ${recoveredStaleSubmissions.length} 条卡在 submitting 的记录: ${recoveredStaleSubmissions.map(item => item.taskName).join(', ')}`);
          console.log('📄 回收 trace 后重新读取飞书记录...');
          records = await listAllRecords(config, token);
          console.log(`📄 飞书记录重新读取完成: ${records.length} 条`);
        }
      }

      console.log('🧮 开始计算本轮待处理上下文...');
      let contexts = sortSubmitContexts(records
        .filter(record => shouldProcessRecord(record, config, cliArgs))
        .map(record => buildTaskContext(record, config))
        .filter(context => context.remainingCount > 0)
        .filter(context => contextMatchesChannelFilter(context, config, cliArgs)));

      const inflightSummary = summarizeInflightSubmissions(
        config,
        generatingCount,
        useSubmitCandidateScan ? { maxRecords: config.submitInflightRecentLimit || 250 } : {}
      );
      const iminiInflightSummary = summarizeChannelInflightSubmissions(
        config,
        'imini',
        useSubmitCandidateScan ? { maxRecords: config.submitInflightRecentLimit || 250 } : {}
      );
      const claimWorkSummary = channelFilter === 'imini'
        ? iminiInflightSummary
        : inflightSummary;
      const iminiHasCapacity = iminiInflightSummary.activeCount < iminiInflightSummary.limit;
      if (iminiInflightSummary.activeCount >= iminiInflightSummary.limit) {
        const beforeFilterCount = contexts.length;
        contexts = contexts.filter(context => resolveChannel(context, config).channel !== 'imini');
        const skippedCount = beforeFilterCount - contexts.length;
        if (skippedCount > 0) {
          console.log(`   imini 并发已满: ${iminiInflightSummary.activeCount}/${iminiInflightSummary.limit}，本轮跳过 ${skippedCount} 条 imini 待处理`);
        }
      } else {
        const iminiContexts = contexts.filter(context => resolveChannel(context, config).channel === 'imini');
        if (iminiContexts.length > 0) {
          contexts = [
            ...iminiContexts,
            ...contexts.filter(context => resolveChannel(context, config).channel !== 'imini')
          ];
          console.log(`   imini 可用空位: ${iminiInflightSummary.limit - iminiInflightSummary.activeCount}，本轮优先补 imini 待处理 ${iminiContexts.length} 条`);
        }
      }
      const hasClaimWork =
        claimWorkSummary.activeCount > 0 ||
        claimWorkSummary.retryableClaimFailedCount > 0 ||
        claimWorkSummary.retryableTimedOutCount > 0;
      const allowConcurrentClaim = generatingCount > 0 && hasClaimWork;
      const assetScanIntervalMinutes = allowConcurrentClaim
        ? getConcurrentAssetScanIntervalMinutes(config, channelFilter)
        : getAssetScanIntervalMinutes(config, channelFilter);
      const assetScanIntervalMs = assetScanIntervalMinutes * 60 * 1000;
      const assetScanRemainingMinutes = lastAssetScanAt > 0
        ? Math.max(0, Math.ceil((assetScanIntervalMs - (Date.now() - lastAssetScanAt)) / 60000))
        : 0;
      const allowAssetScan = hasClaimWork &&
        (lastAssetScanAt === 0 || (Date.now() - lastAssetScanAt) >= assetScanIntervalMs);

      if (cliArgs.limit) {
        contexts = contexts.slice(0, cliArgs.limit);
      }

      console.log(`   生成中: ${generatingCount}`);
      console.log(`   未闭环提交: ${inflightSummary.activeCount}/${inflightSummary.limit}`);
      console.log(`   imini 未闭环提交: ${iminiInflightSummary.activeCount}/${iminiInflightSummary.limit}`);
      if (inflightSummary.activeCount > 0) {
        console.log(`   Trace: ${formatTraceSummary(inflightSummary.active)}`);
      }
      if (inflightSummary.blockingCount > 0) {
        console.log(`   提单阻塞: ${inflightSummary.blockingCount}/${inflightSummary.limit}`);
      }
      if (inflightSummary.retryableClaimFailedCount > 0) {
        console.log(`   待低优先级补回写: ${inflightSummary.retryableClaimFailedCount}`);
      }
      if (inflightSummary.retryableTimedOutCount > 0) {
        console.log(`   待超时补认领: ${inflightSummary.retryableTimedOutCount}`);
      }
      console.log(`   待处理记录: ${contexts.length}`);

      const preferSubmissionThisRound =
        !resumeOnly &&
        contexts.length > 0 &&
        inflightSummary.blockingCount < inflightSummary.limit &&
        generatingCount < config.maxConcurrent &&
        !(queueStatus.limited && generatingCount >= config.maxConcurrent);
      const deferAssetScanForSubmission =
        !submitOnly && allowAssetScan && hasClaimWork && preferSubmissionThisRound;

      const runDeferredAssetScan = async (reason, currentGeneratingCountForScan = generatingCount) => {
        if (!deferAssetScanForSubmission) {
          return;
        }

        console.log(`📦 ${reason}，先补执行本轮资产页下载，再结束当前 one-shot`);
        const uploadSummary = await withOperationTimeout(
          syncCompletedSubmissions({
            config,
            token,
            page,
            dryRun: false,
            recordId: cliArgs.recordId,
            taskNames: cliArgs.taskNames,
            currentGeneratingCount: currentGeneratingCountForScan,
            includeClaimFailedRetries: resumeOnly,
            allowAssetScan: true,
            channelFilter,
            allowConcurrentClaim: currentGeneratingCountForScan > 0 && hasClaimWork,
            claimBatchLimit: currentGeneratingCountForScan > 0
              ? Math.max(1, Number(config.concurrentClaimBatchLimit) || 3)
              : Math.max(1, Number(config.idleClaimBatchLimit) || 5),
            assetScanCooldownMessage: ''
          }),
          config.assetScanTimeoutMs,
          '资产抓取/回写'
        );
        if (uploadSummary.assetScanAttempted) {
          lastAssetScanAt = Date.now();
          saveLastAssetScanAt(config, lastAssetScanAt, channelFilter);
          console.log(`🕒 已记录资产扫描时间: ${new Date(lastAssetScanAt).toISOString()}`);
        } else {
          console.log('ℹ️ 本轮尝试补执行资产扫描，但未真正进入资产页扫描，不更新资产扫描冷却时间');
        }
        if ((uploadSummary.uploaded || 0) > 0 || (uploadSummary.downloaded || 0) > 0) {
          console.log(`📥 结果回写: downloaded=${uploadSummary.downloaded || 0}, uploaded=${uploadSummary.uploaded || 0}`);
        }
      };

      if (downloadOnly) {
        const uploadSummary = await withOperationTimeout(
          syncCompletedSubmissions({
            config,
            token,
            page,
            dryRun: false,
            recordId: cliArgs.recordId,
            taskNames: cliArgs.taskNames,
            currentGeneratingCount: generatingCount,
            includeClaimFailedRetries: resumeOnly,
            allowAssetScan,
            forceAssetRead: true,
            channelFilter,
            allowConcurrentClaim,
            claimBatchLimit: allowConcurrentClaim
              ? Math.max(1, Number(config.concurrentClaimBatchLimit) || 3)
              : Math.max(1, Number(config.idleClaimBatchLimit) || 5),
            assetScanCooldownMessage: hasClaimWork && !allowAssetScan
              ? `${allowConcurrentClaim ? '并发认领' : '资产页扫描'}冷却中，约 ${assetScanRemainingMinutes} 分钟后再检查前 ${config.maxAssetCandidates || 10} 个视频`
              : ''
          }),
          config.assetScanTimeoutMs,
          '资产抓取/回写'
        );
        if (allowAssetScan && uploadSummary.assetScanAttempted) {
          lastAssetScanAt = Date.now();
          saveLastAssetScanAt(config, lastAssetScanAt, channelFilter);
          console.log(`🕒 已记录资产扫描时间: ${new Date(lastAssetScanAt).toISOString()}`);
        } else if (allowAssetScan && hasClaimWork) {
          console.log('ℹ️ 本轮虽到资产扫描窗口，但未真正进入资产页扫描，不更新资产扫描冷却时间');
        }
        if ((uploadSummary.uploaded || 0) > 0 || (uploadSummary.downloaded || 0) > 0) {
          console.log(`📥 结果回写: downloaded=${uploadSummary.downloaded || 0}, uploaded=${uploadSummary.uploaded || 0}`);
        }

        if (oneShot) {
          console.log('\n🛑 one-shot 模式：download-only 本轮检查完成，退出等待下次定时触发');
          break;
        }

        const waitMinutes = hasClaimWork
          ? Math.max(1, assetScanRemainingMinutes || getAssetScanIntervalMinutes(config, channelFilter))
          : Math.max(getQueueFullWaitMinutes(config), getAssetScanIntervalMinutes(config, channelFilter));
        console.log(`\n⏳ download-only：${waitMinutes} 分钟后继续检测资产页结果...`);
        await sleep(waitMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, 'download-only 检查资产页结果', 'download');
        continue;
      }

      if (allowAssetScan && hasClaimWork && preferSubmissionThisRound) {
        console.log('ℹ️ 本轮同时具备提单与下载条件，按最新规则优先提单；资产扫描顺延到本轮提单完成后或下一轮执行');
      } else if (!submitOnly) {
        const uploadSummary = await withOperationTimeout(
          syncCompletedSubmissions({
            config,
            token,
            page,
            dryRun: false,
            recordId: cliArgs.recordId,
            taskNames: cliArgs.taskNames,
            currentGeneratingCount: generatingCount,
            includeClaimFailedRetries: resumeOnly,
            allowAssetScan,
            channelFilter,
            allowConcurrentClaim,
            claimBatchLimit: allowConcurrentClaim
              ? Math.max(1, Number(config.concurrentClaimBatchLimit) || 3)
              : Math.max(1, Number(config.idleClaimBatchLimit) || 5),
            assetScanCooldownMessage: hasClaimWork && !allowAssetScan
              ? `${allowConcurrentClaim ? '并发认领' : '资产页扫描'}冷却中，约 ${assetScanRemainingMinutes} 分钟后再检查前 ${config.maxAssetCandidates || 10} 个视频`
              : ''
          }),
          config.assetScanTimeoutMs,
          '资产抓取/回写'
        );
        if (allowAssetScan && uploadSummary.assetScanAttempted) {
          lastAssetScanAt = Date.now();
          saveLastAssetScanAt(config, lastAssetScanAt, channelFilter);
          console.log(`🕒 已记录资产扫描时间: ${new Date(lastAssetScanAt).toISOString()}`);
        } else if (allowAssetScan && hasClaimWork) {
          console.log('ℹ️ 本轮虽到资产扫描窗口，但未真正进入资产页扫描，不更新资产扫描冷却时间');
        }
        if ((uploadSummary.uploaded || 0) > 0 || (uploadSummary.downloaded || 0) > 0) {
          console.log(`📥 结果回写: downloaded=${uploadSummary.downloaded || 0}, uploaded=${uploadSummary.uploaded || 0}`);
        }
      } else if (hasClaimWork) {
        console.log('ℹ️ submit-only 模式：本轮存在待认领结果，但不会在当前进程执行资产页下载');
      }

      if (resumeOnly) {
        if (inflightSummary.activeCount === 0 && inflightSummary.retryableClaimFailedCount === 0) {
          console.log('\n✅ resume-only 模式下未发现未闭环提交，停止检测');
          break;
        }

        if (inflightSummary.pendingClaimCount > 1 && generatingCount === 0) {
          console.log('\n⚠️ resume-only 模式检测到多条待认领结果，当前安全模式无法自动唯一认领。');
          console.log('⚠️ 请先人工处理资产页结果或清理对应 trace 状态，再重新等待补回写 watcher 接手。');
          break;
        }

        const retryHint = inflightSummary.retryableClaimFailedCount > 0
          ? `，${inflightSummary.retryableClaimFailedCount} 条低优先级补回写`
          : '';
        if (oneShot) {
          console.log(`\n🛑 one-shot 模式：resume-only 本轮检查完成，保留 ${inflightSummary.activeCount} 条未闭环提交${retryHint}，退出等待下次定时触发`);
          break;
        }
        console.log(`\n⏳ resume-only：仍有 ${inflightSummary.activeCount} 条未闭环提交${retryHint}，等待 ${config.checkIntervalMinutes} 分钟后继续检测...`);
        await sleep(config.checkIntervalMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, 'resume-only 续跑未闭环提交');
        continue;
      }

      if (contexts.length === 0) {
        if (claimWorkSummary.activeCount === 0) {
          const idleWaitMinutes = Math.max(
            getQueueFullWaitMinutes(config),
            getAssetScanIntervalMinutes(config, channelFilter)
          );
          if (oneShot) {
            console.log('\n🛑 one-shot 模式：当前无待处理记录且无未闭环提交，本轮结束');
            break;
          }
          console.log(`\n🛌 没有待处理记录，也没有未闭环提交；进入空闲巡检，${idleWaitMinutes} 分钟后再检查一次...`);
          await sleep(idleWaitMinutes * 60 * 1000);
          continue;
        }

        if (claimWorkSummary.pendingClaimCount > 1 && generatingCount === 0) {
          console.log('\n⚠️ 检测到多条待认领结果，当前安全模式无法自动唯一认领；已停止新增提交。');
          console.log('⚠️ 请先人工处理资产页结果或清理对应 trace 状态，再重新启动流程。');
          break;
        }

        if (oneShot) {
          console.log(`\n🛑 one-shot 模式：当前无新待处理记录，但仍有 ${claimWorkSummary.activeCount} 条未闭环提交，本轮结束`);
          break;
        }
        console.log(`\n⏳ 没有新的待处理记录，但还有 ${claimWorkSummary.activeCount} 条未闭环提交，等待 ${config.checkIntervalMinutes} 分钟后继续检测...`);
        await sleep(config.checkIntervalMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, '等待未闭环提交闭环');
        continue;
      }

      if (generatingCount >= config.maxConcurrent) {
        const iminiContexts = contexts.filter(context => resolveChannel(context, config).channel === 'imini');
        if (iminiHasCapacity && iminiContexts.length > 0) {
          contexts = iminiContexts;
          console.log(`\nℹ️ 即梦并发已满 (${generatingCount}/${config.maxConcurrent})，但 imini 仍有空位，本轮只尝试 imini 任务`);
        } else {
          const waitMinutes = getQueueFullWaitMinutes(config);
          if (oneShot) {
            console.log(`\n🛑 one-shot 模式：即梦已达到最大并发数 (${generatingCount}/${config.maxConcurrent})，且无可提交 imini，本轮结束`);
            break;
          }
          console.log(`\n⏳ 已达到最大并发数 (${generatingCount}/${config.maxConcurrent})，等待 ${waitMinutes} 分钟后继续检测...`);
          await sleep(waitMinutes * 60 * 1000);
          await refreshPageAfterWait(page, config, '并发队列已满');
          continue;
        }
      }

      if (inflightSummary.blockingCount >= inflightSummary.limit) {
        const waitMinutes = getQueueFullWaitMinutes(config);
        if (oneShot) {
          console.log(`\n🛑 one-shot 模式：提单阻塞已达到安全上限 (${inflightSummary.blockingCount}/${inflightSummary.limit})，本轮结束`);
          break;
        }
        console.log(`\n⏳ 提单阻塞已达到安全上限 (${inflightSummary.blockingCount}/${inflightSummary.limit})，暂停新增任务，等待 ${waitMinutes} 分钟后继续检测...`);
        await sleep(waitMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, '提单阻塞达到安全上限');
        continue;
      }

      if (queueStatus.limited && generatingCount >= config.maxConcurrent) {
        const waitMinutes = getQueueFullWaitMinutes(config);
        if (oneShot) {
          console.log(`\n🛑 one-shot 模式：平台高峰期限流，本轮结束`);
          break;
        }
        console.log(`\n⏳ 平台高峰期限流，等待 ${waitMinutes} 分钟后继续检测...`);
        await sleep(waitMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, '高峰期限流');
        continue;
      }

      let context = contexts[0];
      const refreshedTask = await refreshTaskContext(config, token, context.recordId, cliArgs);
      if (!refreshedTask) {
        console.log(`  ⏭️ 记录已不存在，跳过: ${context.taskName}`);
        await sleep(800);
        continue;
      }
      context = refreshedTask.context;
      if (!refreshedTask.shouldProcess) {
        console.log(`  ⏭️ 已按飞书最新状态刷新，当前记录不再符合处理条件，跳过: ${context.taskName}`);
        await sleep(800);
        continue;
      }
      if (!contextMatchesChannelFilter(context, config, cliArgs)) {
        console.log(`  ⏭️ 已按飞书最新渠道刷新，当前记录不属于本提单线，跳过: ${context.taskName}`);
        await sleep(800);
        continue;
      }

      console.log(`\n🚀 处理记录: ${context.taskName}`);
      console.log(`   record_id: ${context.recordId}`);
      console.log(`   已提交: ${context.submittedCount}/${context.repeatTarget}`);
      if (context.executionOwnerMachineId) {
        console.log(`   执行归属: ${context.executionOwnerMachineId}`);
      }

      const claimAttempt = await claimRecordOwnership(config, token, context);
      if (!claimAttempt.ok) {
        console.log(`  ⏭️ 认领失败，跳过当前记录: ${claimAttempt.reason}`);
        await sleep(1000);
        continue;
      }
      const claimToken = claimAttempt.claimToken;
      context.executionOwner = claimToken;
      context.executionOwnerMachineId = config.machineId;

      if (!context.prompt) {
        await updateStatus(config, token, context.recordId, STATUS.FAILED, {
          [config.fields.result]: '提示词为空，无法提交',
          [config.fields.blockedPath]: ''
        });
        console.log('  ❌ 提示词为空，已标记为失败');
        continue;
      }

      if (context.attachments.length === 0 && !context.allowNoReferenceImage) {
        await updateStatus(config, token, context.recordId, STATUS.FAILED, {
          [config.fields.result]: '参考图为空，无法提交',
          [config.fields.blockedPath]: ''
        });
        console.log('  ❌ 参考图为空，已标记为失败');
        continue;
      }

      if (context.attachments.length === 0 && context.allowNoReferenceImage) {
        console.log('  ℹ️  已启用免参考图，本次将按无图任务提交');
      }

      const channelDecision = resolveChannel(context, config);
      const baselineObservation = channelDecision.channel === 'imini'
        ? {
            baseline: generatingCount,
            samples: [{
              index: 1,
              generating: generatingCount,
              at: formatBeijingTimestamp(),
              source: 'imini_skip_jimeng_baseline'
            }]
          }
        : await stabilizeGeneratingBaseline(page, config, generatingCount);
      const beforeGeneratingCount = baselineObservation.baseline;
      if (beforeGeneratingCount !== generatingCount) {
        console.log(`  🔁 提交前基线复核: 生成中 ${generatingCount} -> ${beforeGeneratingCount}`);
      }

      if (channelDecision.channel !== 'imini' && beforeGeneratingCount >= config.maxConcurrent) {
        const waitMinutes = getQueueFullWaitMinutes(config);
        if (oneShot) {
          await runDeferredAssetScan('提交前复核发现并发已满', beforeGeneratingCount);
          console.log(`  🛑 one-shot 模式：提交前复核发现并发已满 (${beforeGeneratingCount}/${config.maxConcurrent})，本轮结束`);
          break;
        }
        console.log(`  ⏳ 提交前复核发现并发已满 (${beforeGeneratingCount}/${config.maxConcurrent})，等待 ${waitMinutes} 分钟后重试`);
        await sleep(waitMinutes * 60 * 1000);
        await refreshPageAfterWait(page, config, '提交前复核发现并发已满');
        continue;
      }

      const nextAttempt = context.submittedCount + 1;
      const duplicateTrace = channelDecision.channel === 'imini'
        ? null
        : findDuplicateSubmissionTrace(config, context, nextAttempt);
      if (duplicateTrace) {
        const recoveredSubmittedCount = Math.max(context.submittedCount, nextAttempt);
        const done = recoveredSubmittedCount >= context.repeatTarget;
        const duplicateStatus = done ? STATUS.SUBMITTED : STATUS.PARTIAL;
        console.log(
          `  🛡️ 检测到同一记录/脚本/提交序号已有本地 trace，跳过重复提交: ${duplicateTrace.trace_id} (${duplicateTrace.status})`
        );
        await updateStatus(config, token, context.recordId, duplicateStatus, {
          [config.fields.submittedCount]: recoveredSubmittedCount,
          [config.fields.result]:
            `本地已存在同一脚本第 ${nextAttempt}/${context.repeatTarget} 次提交 trace (${duplicateTrace.trace_id}, ${duplicateTrace.status})，已跳过重复提单`,
          [config.fields.latestTraceId]: duplicateTrace.trace_id,
          [config.fields.resultSyncStatus]: duplicateTrace.status === 'uploaded' ? 'uploaded' : 'rendering',
          [config.fields.errorMessage]: ''
        });
        syncAutomationTaskFromJimeng(duplicateTrace, '检测到重复提交风险，已跳过本轮提单');
        await sleep(800);
        continue;
      }

      const traceId = generateTraceId({
        recordId: context.recordId,
        taskName: context.taskName,
        submitIndex: nextAttempt
      });
      const tracePayload = buildTraceSubmissionPayload(context, config, traceId, nextAttempt);
      tracePayload.pre_submit_generating_count = beforeGeneratingCount;
      tracePayload.pre_submit_generating_samples = baselineObservation.samples;
      writeSubmissionRecord(config, traceId, tracePayload);
      syncAutomationTaskFromJimeng(tracePayload, '即梦提交流程已开始，等待提交确认');

      await updateStatus(config, token, context.recordId, STATUS.PROCESSING, {
        [config.fields.result]: context.attachments.length === 0 && context.allowNoReferenceImage
          ? `准备以免参考图模式提交第 ${nextAttempt}/${context.repeatTarget} 次`
          : `准备提交第 ${nextAttempt}/${context.repeatTarget} 次`,
        [config.fields.blockedPath]: '',
        [config.fields.executionOwner]: claimToken,
        [config.fields.latestTraceId]: traceId,
        [config.fields.resultSyncStatus]: 'rendering',
        [config.fields.submitTime]: tracePayload.submit_time,
        [config.fields.finishTime]: '',
        [config.fields.videoFileName]: '',
        [config.fields.errorMessage]: ''
      });

      const task = await materializeTask(token, context, config);
      context.taskFolderImages = task.images;
      const options = {
        model: context.model || config.defaultModel,
        mode: context.mode || config.defaultMode,
        ratio: context.ratio || config.defaultRatio,
        duration: context.duration || config.defaultDuration,
        baseUrl: config.baseUrl,
        timeout: config.timeout
      };

      const stillOwned = await ensureClaimOwnership(config, token, context.recordId, claimToken);
      if (!stillOwned) {
        console.log(`  ⏭️ 记录在提交前已被其它执行方接管，放弃本轮提交: ${context.taskName}`);
        await cleanupTaskDir(task);
        await sleep(1000);
        continue;
      }

      if (channelDecision.channel === 'imini') {
        const iminiPage = await getIminiSubmitPage(browser, config);
        const iminiTracePayload = updateSubmissionRecord(config, traceId, {
          platform: 'imini',
          channel: 'imini',
          status: 'submitting',
          submit_confirmed_by: '',
          submit_confirmation_note: `imini 提交流程开始 (${channelDecision.source})`,
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(iminiTracePayload, 'imini 提交流程已开始，等待提交确认');

        const result = await processIminiTask({
          page: iminiPage,
          context,
          config,
          token,
          traceId
        });

        if (result.success) {
          const newSubmittedCount = context.submittedCount + 1;
          const done = newSubmittedCount >= context.repeatTarget;
          const updatedSubmission = updateSubmissionRecord(config, traceId, {
            platform: 'imini',
            channel: 'imini',
            status: 'submitted',
            submit_confirmed_by: result.code || 'imini_create_success',
            submit_confirmation_note: 'imini 页面点击创建成功，等待资产页抓取回传',
            platform_task_id: result.taskId || traceId,
            state_updated_at: formatBeijingTimestamp()
          });
          syncAutomationTaskFromJimeng(updatedSubmission, 'imini 提交成功，等待资产页认领');
          await updateStatus(config, token, context.recordId, done ? STATUS.SUBMITTED : STATUS.PARTIAL, {
            [config.fields.submittedCount]: newSubmittedCount,
            [config.fields.result]: done
              ? `imini 已成功提交 ${newSubmittedCount}/${context.repeatTarget} 次，等待资产抓取回传`
              : `imini 已成功提交 ${newSubmittedCount}/${context.repeatTarget} 次，等待继续`,
            [config.fields.latestTraceId]: traceId,
            [config.fields.resultSyncStatus]: 'rendering',
            [config.fields.platformTaskId]: result.taskId || traceId,
            [config.fields.errorMessage]: ''
          });
          submittedThisRun++;
          justSubmitted = true;
          console.log(`  ✅ imini 已更新飞书状态: ${done ? STATUS.SUBMITTED : STATUS.PARTIAL}`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条 imini 提交完成，继续检查本轮是否还能追加任务');
          }
          continue;
        }

        const failedStatus = result.shouldBlock ? STATUS.BLOCKED : STATUS.FAILED;
        const syncStatus = result.shouldBlock ? 'blocked' : 'failed';
        const updatedSubmission = updateSubmissionRecord(config, traceId, {
          platform: 'imini',
          channel: 'imini',
          status: syncStatus,
          error_message: result.error || 'imini 提交失败',
          submit_confirmed_by: result.code || 'imini_submit_failed',
          submit_confirmation_note: result.error || 'imini 提交失败',
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(updatedSubmission, failedStatus === STATUS.BLOCKED ? 'imini 提交阻塞，等待人工处理' : 'imini 提交失败');
        await updateStatus(config, token, context.recordId, failedStatus, {
          [config.fields.result]: result.error || 'imini 提交失败',
          [config.fields.blockedPath]: '',
          [config.fields.resultSyncStatus]: syncStatus,
          [config.fields.errorMessage]: result.error || 'imini 提交失败'
        });
        console.log(`  ❌ imini 已标记为${failedStatus}: ${result.error || '未知错误'}`);
        await cleanupTaskDir(task);
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条 imini 已结束，继续尝试下一条可执行任务');
        }
        await sleep(1500);
        continue;
      }

      const result = await processTaskWithRecoverableRetry(
        page,
        task,
        options,
        config,
        beforeGeneratingCount
      );
      if (result.success) {
        const submissionObservation = result.queueObservedAfterRecoverableError ||
          await observeQueueAfterSubmit(page, config, beforeGeneratingCount);
        const newSubmittedCount = context.submittedCount + 1;
        const done = newSubmittedCount >= context.repeatTarget;
        const hasStrongSubmitSignal = result.confirmedBy === 'ui_queue_signal';
        const confirmedBy = submissionObservation.queueIncreased
          ? `queue_growth+${result.confirmedBy || 'submit_success'}`
          : (hasStrongSubmitSignal ? (result.confirmedBy || 'submit_success') : 'submit_unverified');

        if (!submissionObservation.queueIncreased && !hasStrongSubmitSignal) {
          await cleanupFailedSubmitSession(page, 'submit_success_without_queue_growth');
          const maxSubmitUnconfirmedRetries = Math.max(0, normalizePositiveInt(config.maxSubmitUnconfirmedRetries, 2));
          const previousSubmitUnconfirmedRetries = countSubmitUnconfirmedRetries(config, context);
          const nextSubmitUnconfirmedAttempt = previousSubmitUnconfirmedRetries + 1;

          if (nextSubmitUnconfirmedAttempt <= maxSubmitUnconfirmedRetries) {
            await markTaskRetryPendingForSubmitUnconfirmed({
              config,
              token,
              context,
              traceId,
              submitConfirmedBy: 'submit_unconfirmed',
              submissionObservation,
              resultMessage:
                `提交确认未通过：确认窗口内队列未增长 (${beforeGeneratingCount} -> ${submissionObservation.maxGeneratingCount})。已保留自动重试机会（${nextSubmitUnconfirmedAttempt}/${maxSubmitUnconfirmedRetries}），后续将自动重试`,
              errorMessage: '提交确认未通过，等待自动重试'
            });
            console.log(`  🔁 未检测到真实进队，已恢复为待重试（${nextSubmitUnconfirmedAttempt}/${maxSubmitUnconfirmedRetries}）`);
            await cleanupTaskDir(task);
            if (oneShot) {
              console.log('  ▶️ one-shot 模式：本条已转待重试，继续尝试下一条可执行任务');
            }
            await sleep(1500);
            continue;
          }

          await markTaskBlockedForManualReview({
            config,
            token,
            context,
            traceId,
            submitConfirmedBy: 'submit_unconfirmed',
            submissionObservation,
            resultMessage:
              `提交确认未通过：确认窗口内队列未增长 (${beforeGeneratingCount} -> ${submissionObservation.maxGeneratingCount})。已连续 ${nextSubmitUnconfirmedAttempt} 次未确认进队，当前转人工复核阻塞`,
            errorMessage: '提交确认未通过，需人工复核'
          });
          console.log(`  ⛔ 未检测到真实进队，且已超过自动重试上限 (${maxSubmitUnconfirmedRetries})，转为阻塞`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转阻塞，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }

        const updatedSubmission = updateSubmissionRecord(config, traceId, {
          status: submissionObservation.queueIncreased ? 'rendering' : 'submitted',
          queue_observed: submissionObservation.queueIncreased ? true : undefined,
          queue_observed_at: submissionObservation.queueIncreased ? formatBeijingTimestamp() : undefined,
          observed_generating_count: submissionObservation.queueIncreased ? submissionObservation.maxGeneratingCount : undefined,
          post_submit_generating_count: submissionObservation.maxGeneratingCount,
          submit_confirmed_by: confirmedBy,
          submit_confirmation_note: submissionObservation.queueIncreased
            ? `提交后队列从 ${beforeGeneratingCount} 增长到 ${submissionObservation.maxGeneratingCount}`
            : `提交成功，但确认窗口内队列未明显增长 (${beforeGeneratingCount} -> ${submissionObservation.maxGeneratingCount})`,
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(
          updatedSubmission,
          submissionObservation.queueIncreased
            ? '即梦提交成功并确认进队'
            : '即梦提交成功，待进一步观察队列状态'
        );
        await updateStatus(config, token, context.recordId, done ? STATUS.SUBMITTED : STATUS.PARTIAL, {
          [config.fields.submittedCount]: newSubmittedCount,
          [config.fields.result]: done
            ? `已成功提交 ${newSubmittedCount}/${context.repeatTarget} 次${submissionObservation.queueIncreased ? `，队列已从 ${beforeGeneratingCount} 增长到 ${submissionObservation.maxGeneratingCount}` : ''}`
            : `已成功提交 ${newSubmittedCount}/${context.repeatTarget} 次，等待继续${submissionObservation.queueIncreased ? `（队列 ${beforeGeneratingCount} -> ${submissionObservation.maxGeneratingCount}）` : ''}`,
          [config.fields.latestTraceId]: traceId,
          [config.fields.resultSyncStatus]: 'rendering'
        });
        submittedThisRun++;
        justSubmitted = true;
        console.log(`  ✅ 已更新飞书状态: ${done ? STATUS.SUBMITTED : STATUS.PARTIAL}`);
        if (result.lowCreditsPauseSuggested && config.submitPauseOnInsufficientCredits !== false) {
          const pauseState = activateSubmitPauseForLowCreditsAfterSuccess({
            config,
            context,
            traceId,
            result
          });
          console.log(`  ⛔ 当前提交后积分已低于阈值 ${config.insufficientCreditsThreshold}，已暂停后续提单`);
          console.log(`  📄 暂停标记: ${pauseState.pauseFile}`);
          console.log('  🛑 等待人工恢复后再继续 submit 线');
          await cleanupTaskDir(task);
          break;
        }
        await cleanupTaskDir(task);
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条提交完成，继续检查本轮是否还能追加任务');
        }
        continue;
      }

      if (result.code === 'platform_limited') {
        const maxPlatformLimitedRetries = Math.max(0, Number(config.maxPlatformLimitedRetries) || 1);
        const previousPlatformLimitedRetries = countPlatformLimitedRetries(config, context);
        const nextPlatformLimitedAttempt = previousPlatformLimitedRetries + 1;

        if (nextPlatformLimitedAttempt <= maxPlatformLimitedRetries) {
          const resumeStatus = getResumeStatus(context);
          const updatedSubmission = updateSubmissionRecord(config, traceId, {
            status: 'retry_pending',
            submit_confirmed_by: 'platform_limited',
            submit_confirmation_note: `提交时遇到高峰期限流，第 ${nextPlatformLimitedAttempt}/${maxPlatformLimitedRetries} 次自动重试机会`,
            error_message: result.error || '平台限流',
            state_updated_at: formatBeijingTimestamp()
          });
          syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交遇到平台限流，等待自动重试');
          await updateStatus(config, token, context.recordId, resumeStatus, {
            [config.fields.result]:
              `提交时遇到高峰期限流，本次未提交成功；已保留自动重试机会（${nextPlatformLimitedAttempt}/${maxPlatformLimitedRetries}）`,
            [config.fields.blockedPath]: '',
            [config.fields.resultSyncStatus]: 'pending_retry',
            [config.fields.errorMessage]: result.error || '平台限流'
          });
          console.log(`  ⏳ 提交时遇到高峰期限流，已恢复为 ${resumeStatus}，等待后续自动重试`);
        } else {
          const updatedSubmission = updateSubmissionRecord(config, traceId, {
            status: 'failed',
            submit_confirmed_by: 'platform_limited_retry_exhausted',
            submit_confirmation_note: `提交时连续遇到高峰期限流，已超过自动重试上限 (${maxPlatformLimitedRetries})，本条先跳过`,
            error_message: result.error || '平台限流重试次数已耗尽',
            state_updated_at: formatBeijingTimestamp()
          });
          syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交多次限流后失败');
          await updateStatus(config, token, context.recordId, STATUS.FAILED, {
            [config.fields.result]:
              `提交时连续遇到高峰期限流，已超过自动重试上限 (${maxPlatformLimitedRetries})；为避免阻断后续流程，已先跳过此记录，如需继续请人工改回待处理`,
            [config.fields.blockedPath]: '',
            [config.fields.resultSyncStatus]: 'failed',
            [config.fields.errorMessage]: result.error || '平台限流重试次数已耗尽'
          });
          console.log('  ⏭️ 高峰期限流重试次数已耗尽，已跳过当前记录，后续任务可继续推进');
        }
        await cleanupTaskDir(task);
        if (oneShot) {
          await runDeferredAssetScan('本轮遇到高峰期限流', beforeGeneratingCount);
          console.log('  🛑 one-shot 模式：本轮遇到高峰期限流，退出等待下次定时触发');
          break;
        }
        await sleep(getQueueFullWaitMinutes(config) * 60 * 1000);
        await refreshPageAfterWait(page, config, '提交后遇到高峰期限流');
        continue;
      }

      if (result.code === 'insufficient_credits') {
        const pauseState = await activateSubmitPauseForInsufficientCredits({
          config,
          token,
          context,
          traceId,
          result
        });
        await cleanupTaskDir(task);
        console.log(`  ⛔ 检测到积分不足，已暂停后续提单并释放当前任务归属`);
        console.log(`  📄 暂停标记: ${pauseState.pauseFile}`);
        console.log('  🛑 等待人工恢复后再继续 submit 线');
        break;
      }

      const failed = readFailedSnapshot(task);
      const hasGenerateErrorNotice = failed?.code === 'generate_error_notice' || result.code === 'generate_error_notice';
      const submissionObservation = result.queueObservedAfterRecoverableError ||
        await observeQueueAfterSubmit(page, config, beforeGeneratingCount);
      const afterGeneratingCount = submissionObservation.maxGeneratingCount;
      if (!hasGenerateErrorNotice && submissionObservation.queueIncreased && config.allowQueueGrowthRecoveryOnError === true) {
        const newSubmittedCount = context.submittedCount + 1;
        const done = newSubmittedCount >= context.repeatTarget;
        const updatedSubmission = updateSubmissionRecord(config, traceId, {
          status: 'rendering',
          queue_observed: true,
          queue_observed_at: formatBeijingTimestamp(),
          observed_generating_count: afterGeneratingCount,
          post_submit_generating_count: afterGeneratingCount,
          submit_confirmed_by: 'queue_growth_after_error',
          submit_confirmation_note: `页面返回异常，但确认窗口内队列从 ${beforeGeneratingCount} 增长到 ${afterGeneratingCount}`,
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(updatedSubmission, '即梦页面异常但确认已进队，按成功提交处理');
        await updateStatus(config, token, context.recordId, done ? STATUS.SUBMITTED : STATUS.PARTIAL, {
          [config.fields.submittedCount]: newSubmittedCount,
          [config.fields.result]: `页面返回异常，但队列已从 ${beforeGeneratingCount} 增加到 ${afterGeneratingCount}，按提交成功处理 (${newSubmittedCount}/${context.repeatTarget})`,
          [config.fields.latestTraceId]: traceId,
          [config.fields.resultSyncStatus]: 'rendering'
        });
        submittedThisRun++;
        justSubmitted = true;
        console.log(`  ⚠️ 队列已增长，按成功提交处理`);
        await cleanupTaskDir(task);
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条异常后确认已进队，继续检查本轮是否还能追加任务');
        }
        continue;
      }

      if (!hasGenerateErrorNotice && submissionObservation.queueIncreased) {
        await markTaskObservedForDelayedClaim({
          config,
          token,
          context,
          traceId,
          submitConfirmedBy: 'queue_growth_after_error_deferred',
          resultMessage:
            `页面返回异常，但确认窗口内队列已从 ${beforeGeneratingCount} 增长到 ${afterGeneratingCount}。当前先按待观察处理：不重复提单，后续继续通过资产页认领结果`,
          errorMessage: result.error || '页面返回异常但队列已增长，转入待观察认领',
          beforeGeneratingCount,
          afterGeneratingCount
        });
        console.log('  👀 页面返回异常但队列已增长，已转为待观察，后续继续通过资产页认领');
        await cleanupTaskDir(task);
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条已转待观察，继续尝试下一条可执行任务');
        }
        await sleep(1500);
        continue;
      }

      if (result.code === 'submit_unconfirmed') {
        if (!result.cleanupResult) {
          await cleanupFailedSubmitSession(page, 'submit_unconfirmed_no_queue_growth');
        }
        const maxSubmitUnconfirmedRetries = Math.max(0, normalizePositiveInt(config.maxSubmitUnconfirmedRetries, 2));
        const previousSubmitUnconfirmedRetries = countSubmitUnconfirmedRetries(config, context);
        const nextSubmitUnconfirmedAttempt = previousSubmitUnconfirmedRetries + 1;

        if (nextSubmitUnconfirmedAttempt <= maxSubmitUnconfirmedRetries) {
          await markTaskRetryPendingForSubmitUnconfirmed({
            config,
            token,
            context,
            traceId,
            submitConfirmedBy: 'submit_unconfirmed',
            submissionObservation,
            resultMessage:
              `提交确认未通过：确认窗口内队列未增长 (${beforeGeneratingCount} -> ${afterGeneratingCount})。已保留自动重试机会（${nextSubmitUnconfirmedAttempt}/${maxSubmitUnconfirmedRetries}），后续将自动重试`,
            errorMessage: result.error || '提交确认未通过，等待自动重试'
          });
          console.log(`  🔁 提交确认未通过，已恢复为待重试（${nextSubmitUnconfirmedAttempt}/${maxSubmitUnconfirmedRetries}）`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转待重试，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }

        await markTaskBlockedForManualReview({
          config,
          token,
          context,
          traceId,
          submitConfirmedBy: 'submit_unconfirmed',
          submissionObservation,
          resultMessage:
            `提交确认未通过：确认窗口内队列未增长 (${beforeGeneratingCount} -> ${afterGeneratingCount})。已连续 ${nextSubmitUnconfirmedAttempt} 次未确认进队，当前转人工复核阻塞`,
          errorMessage: result.error || '提交确认未通过，需人工复核'
        });
        console.log(`  ⛔ 提交确认未通过，且已超过自动重试上限 (${maxSubmitUnconfirmedRetries})，已转为阻塞等待人工处理`);
        await cleanupTaskDir(task);
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条已转阻塞，继续尝试下一条可执行任务');
        }
        await sleep(1500);
        continue;
      }

      if (result.code === 'task_blocked') {
        const blocked = readBlockedSnapshot(task);
        const updatedSubmission = updateSubmissionRecord(config, traceId, {
          status: 'blocked',
          error_message: blocked?.reason || result.error || '生成按钮为灰色，需人工处理',
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(updatedSubmission, '即梦页面阻塞，等待人工处理');
        await updateStatus(config, token, context.recordId, STATUS.BLOCKED, {
          [config.fields.result]: blocked?.reason || result.error || '生成按钮为灰色，需人工处理',
          [config.fields.blockedPath]: blocked?.screenshot || path.join(task.folder, '.blocked'),
          [config.fields.resultSyncStatus]: 'blocked',
          [config.fields.errorMessage]: blocked?.reason || result.error || '生成按钮为灰色，需人工处理'
        });
        console.log('  🚫 已标记为阻塞，等待人工处理');
        if (oneShot) {
          console.log('  ▶️ one-shot 模式：本条已转阻塞，继续尝试下一条可执行任务');
        }
        continue;
      }

      if (failed?.code === 'generate_error_notice' || result.code === 'generate_error_notice') {
        if (!result.cleanupResult) {
          await cleanupFailedSubmitSession(page, 'generate_error_notice_no_queue_growth');
        }
        const maxGenerateErrorNoticeRetries = Math.max(0, normalizePositiveInt(config.maxGenerateErrorNoticeRetries, 2));
        const previousGenerateErrorNoticeRetries = countGenerateErrorNoticeRetries(config, context);
        const nextGenerateErrorNoticeAttempt = previousGenerateErrorNoticeRetries + 1;
        const failureReason = failed?.reason || result.error || '0/1生成完成，1 项失败回到底部';

        if (submissionObservation.queueIncreased) {
          await markTaskObservedForDelayedClaim({
            config,
            token,
            context,
            traceId,
            submitConfirmedBy: 'queue_growth_after_generate_error_notice',
            resultMessage:
              `已确认提交成功：队列已从 ${beforeGeneratingCount} 增长到 ${afterGeneratingCount}。当前按已提交待观察处理，不重复提单，后续继续通过资产页认领结果`,
            errorMessage: '',
            beforeGeneratingCount,
            afterGeneratingCount
          });
          console.log('  👀 生成页提示失败但队列已增长，已转为待观察，后续通过资产页认领，避免重复提交');
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转待观察，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }

        if (/视频未通过审核/.test(failureReason)) {
          const updatedSubmission = updateSubmissionRecord(config, traceId, {
            status: 'failed',
            submit_confirmed_by: 'platform_review_failed',
            submit_confirmation_note:
              `${failureReason}。平台已明确返回审核失败，当前不再自动重试，避免同一脚本重复进入会话/队列`,
            error_message: failureReason,
            result_sync_status: 'review_failed',
            state_updated_at: formatBeijingTimestamp()
          });
          syncAutomationTaskFromJimeng(updatedSubmission, '即梦返回视频未通过审核，已停止自动重试');
          await updateStatus(config, token, context.recordId, STATUS.FAILED, {
            [config.fields.result]:
              `${failureReason}。平台已明确返回审核失败，自动重试已关闭；如需继续请人工修改脚本或手动改回待处理`,
            [config.fields.blockedPath]: failed?.screenshot || path.join(task.folder, '.failed'),
            [config.fields.resultSyncStatus]: 'review_failed',
            [config.fields.errorMessage]: failureReason
          });
          console.log('  ❌ 视频未通过审核，已转失败并停止自动重试，避免重复提交');
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  🛑 one-shot 模式：审核失败任务已记录，退出等待下次定时触发');
            break;
          }
          await sleep(3000);
          continue;
        }

        if (nextGenerateErrorNoticeAttempt <= maxGenerateErrorNoticeRetries) {
          await markTaskRetryPendingForGenerateErrorNotice({
            config,
            token,
            context,
            traceId,
            resultMessage:
              `${failureReason}。已保留自动重试机会（${nextGenerateErrorNoticeAttempt}/${maxGenerateErrorNoticeRetries}），下次提交前会先重置生成页再重试`,
            errorMessage: failureReason
          });
          console.log(`  🔁 生成后即时失败，已恢复为待重试（${nextGenerateErrorNoticeAttempt}/${maxGenerateErrorNoticeRetries}）`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转待重试，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }

        const updatedSubmission = updateSubmissionRecord(config, traceId, {
          status: 'failed',
          submit_confirmed_by: 'generate_error_notice_retry_exhausted',
          submit_confirmation_note:
            `${failureReason}。已连续 ${nextGenerateErrorNoticeAttempt} 次发生同类即时失败，自动重试已用尽，当前转失败待人工复核`,
          error_message: failureReason,
          state_updated_at: formatBeijingTimestamp()
        });
        syncAutomationTaskFromJimeng(updatedSubmission, '即梦生成后即时失败，自动重试耗尽');
        await updateStatus(config, token, context.recordId, STATUS.FAILED, {
          [config.fields.result]:
            `${failureReason}。已连续 ${nextGenerateErrorNoticeAttempt} 次发生同类即时失败，自动重试已用尽；如需继续请人工检查页面状态后改回待处理`,
          [config.fields.blockedPath]: failed?.screenshot || path.join(task.folder, '.failed'),
          [config.fields.resultSyncStatus]: 'failed',
          [config.fields.errorMessage]: failureReason
        });
        console.log(`  ❌ 生成后即时失败已超过自动重试上限 (${maxGenerateErrorNoticeRetries})，已转失败等待人工复核`);
        await cleanupTaskDir(task);
        if (oneShot) {
          await runDeferredAssetScan('本轮即时失败任务已记录', beforeGeneratingCount);
          console.log('  🛑 one-shot 模式：本轮即时失败任务已记录，退出等待下次定时触发');
          break;
        }
        await sleep(3000);
        continue;
      }

      if (failed?.code === 'upload_failed' || result.code === 'upload_failed') {
        if (!result.cleanupResult) {
          await cleanupFailedSubmitSession(page, 'upload_failed_no_queue_growth');
        }
        const maxUploadFailedRetries = Math.max(0, normalizePositiveInt(config.maxUploadFailedRetries, 3));
        const previousUploadFailedRetries = countTransientSubmitFailureRetries(config, context, 'upload_failed');
        const nextUploadFailedAttempt = previousUploadFailedRetries + 1;
        const failureReason = failed?.reason || result.error || '参考图片上传失败';

        if (nextUploadFailedAttempt <= maxUploadFailedRetries) {
          await markTaskRetryPendingForTransientSubmitFailure({
            config,
            token,
            context,
            traceId,
            submitConfirmedBy: 'upload_failed',
            resultMessage:
              `${failureReason}。已保留自动重试机会（${nextUploadFailedAttempt}/${maxUploadFailedRetries}），下次会先重置生成页并使用单张补传策略`,
            errorMessage: failureReason
          });
          console.log(`  🔁 参考图上传失败，已恢复为待重试（${nextUploadFailedAttempt}/${maxUploadFailedRetries}）`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转待重试，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }
      }

      if (isRecoverableSubmitPageError(failed?.reason || result.error || '')) {
        if (!result.cleanupResult) {
          await cleanupFailedSubmitSession(page, 'recoverable_page_error_no_queue_growth');
        }
        const maxRecoverablePageErrorRetries = Math.max(0, normalizePositiveInt(config.maxRecoverablePageErrorRetries, 3));
        const previousRecoverablePageErrorRetries = countTransientSubmitFailureRetries(config, context, 'recoverable_page_error');
        const nextRecoverablePageErrorAttempt = previousRecoverablePageErrorRetries + 1;
        const failureReason = failed?.reason || result.error || '页面上下文超时';

        if (nextRecoverablePageErrorAttempt <= maxRecoverablePageErrorRetries) {
          await markTaskRetryPendingForTransientSubmitFailure({
            config,
            token,
            context,
            traceId,
            submitConfirmedBy: 'recoverable_page_error',
            resultMessage:
              `${failureReason}。已保留自动重试机会（${nextRecoverablePageErrorAttempt}/${maxRecoverablePageErrorRetries}），后续轮次会重新打开生成页再提交`,
            errorMessage: failureReason
          });
          console.log(`  🔁 页面上下文超时，已恢复为待重试（${nextRecoverablePageErrorAttempt}/${maxRecoverablePageErrorRetries}）`);
          await cleanupTaskDir(task);
          if (oneShot) {
            console.log('  ▶️ one-shot 模式：本条已转待重试，继续尝试下一条可执行任务');
          }
          await sleep(1500);
          continue;
        }
      }

      if (!result.cleanupResult) {
        await cleanupFailedSubmitSession(page, result.code || 'submit_failed_no_queue_growth');
      }

      const updatedSubmission = updateSubmissionRecord(config, traceId, {
        status: 'failed',
        error_message: failed?.reason || result.error || '未知错误',
        state_updated_at: formatBeijingTimestamp()
      });
      syncAutomationTaskFromJimeng(updatedSubmission, '即梦提交失败');
      await updateStatus(config, token, context.recordId, STATUS.FAILED, {
        [config.fields.result]: failed?.reason || result.error || '未知错误',
        [config.fields.blockedPath]: failed?.screenshot || path.join(task.folder, '.failed'),
        [config.fields.resultSyncStatus]: 'failed',
        [config.fields.errorMessage]: failed?.reason || result.error || '未知错误'
      });
      console.log(`  ❌ 已标记为失败: ${failed?.reason || result.error || '未知错误'}`);
      if (oneShot) {
        await runDeferredAssetScan('本轮失败任务已记录', beforeGeneratingCount);
        console.log('  🛑 one-shot 模式：本轮失败任务已记录，退出等待下次定时触发');
        break;
      }
      await sleep(3000);
    }
  } finally {
    await browser.disconnect();
    console.log('\n================================');
    console.log('📊 本次运行统计');
    console.log('================================');
    console.log(`🚀 新提交: ${submittedThisRun}`);
  }
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch(error => {
    console.error(`❌ 运行失败: ${error.message}`);
    process.exit(1);
  });
