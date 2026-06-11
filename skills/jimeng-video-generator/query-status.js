#!/usr/bin/env node

const fs = require('fs');
const https = require('https');
const path = require('path');
const { expandHome, getStateRoot, listSubmissionRecords } = require('./trace-state');

const OPENCLAW_PATH = path.join(process.env.HOME || '', '.openclaw', 'openclaw.json');
const DISPLAY_TIME_ZONE = 'Asia/Shanghai';

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function loadConfig(configPath) {
  const raw = loadJson(configPath);
  raw.runtimeRoot = expandHome(raw.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  return raw;
}

function requestJson(method, reqPath, token, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = { 'Content-Type': 'application/json' };
    if (token) headers.Authorization = `Bearer ${token}`;
    if (payload) headers['Content-Length'] = Buffer.byteLength(payload);

    const req = https.request({
      hostname: 'open.feishu.cn',
      path: reqPath,
      method,
      headers
    }, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (reqPath === '/open-apis/auth/v3/tenant_access_token/internal' && json.tenant_access_token) {
            resolve(json);
            return;
          }
          if (json.code === 0) {
            resolve(json.data);
            return;
          }
          reject(new Error(`飞书 API 失败 (${json.code}): ${json.msg || 'unknown'}`));
        } catch (error) {
          reject(error);
        }
      });
    });

    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function getTenantAccessToken() {
  const openclaw = loadJson(OPENCLAW_PATH);
  const appId = openclaw?.channels?.feishu?.appId;
  const appSecret = openclaw?.channels?.feishu?.appSecret;
  if (!appId || !appSecret) {
    throw new Error('missing Feishu credentials');
  }
  const auth = await requestJson(
    'POST',
    '/open-apis/auth/v3/tenant_access_token/internal',
    null,
    { app_id: appId, app_secret: appSecret }
  );
  return auth.tenant_access_token;
}

async function listFeishuRecords(config, token) {
  let pageToken = '';
  let items = [];
  do {
    let requestPath = `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records?page_size=${config.pageSize || 100}`;
    if (config.viewId) {
      requestPath += `&view_id=${encodeURIComponent(config.viewId)}`;
    }
    if (pageToken) {
      requestPath += `&page_token=${encodeURIComponent(pageToken)}`;
    }
    const data = await requestJson('GET', requestPath, token);
    items = items.concat(data.items || []);
    pageToken = data.page_token || '';
  } while (pageToken);
  return items;
}

function readAssetScanCache(config) {
  const cachePath = path.join(getStateRoot(config), 'asset-scan-cache.json');
  if (!fs.existsSync(cachePath)) {
    return { path: cachePath, raw: null, lastAssetScanAt: null };
  }
  try {
    const raw = loadJson(cachePath);
    const value = Date.parse(raw.last_asset_scan_at || '');
    return {
      path: cachePath,
      raw,
      lastAssetScanAt: Number.isFinite(value) ? new Date(value) : null
    };
  } catch (error) {
    return { path: cachePath, raw: null, lastAssetScanAt: null, error: error.message };
  }
}

function readSubmitPauseState(config) {
  const filePath = path.join(getStateRoot(config), 'submit-paused-insufficient-credits.json');
  if (!fs.existsSync(filePath)) {
    return { path: filePath, active: false, raw: null };
  }
  try {
    const raw = loadJson(filePath);
    return {
      path: filePath,
      active: true,
      raw
    };
  } catch (error) {
    return {
      path: filePath,
      active: true,
      raw: null,
      error: error.message
    };
  }
}

function getSubmitIntervalMinutes(config) {
  return Math.max(1, Number(config.checkIntervalMinutes) || 10);
}

function isEffectiveScheduledRun(date, config = {}) {
  const minute = date.getMinutes();
  return minute % getSubmitIntervalMinutes(config) === 0;
}

function ceilToNextSubmitInterval(date, config = {}) {
  const next = new Date(date.getTime());
  next.setSeconds(0, 0);
  const minute = next.getMinutes();
  const interval = getSubmitIntervalMinutes(config);
  const add = (interval - (minute % interval)) % interval;
  if (add === 0 && date.getSeconds() === 0 && date.getMilliseconds() === 0) {
    return next;
  }
  next.setMinutes(minute + (add || interval));
  return next;
}

function findNextEffectiveRun(fromDate = new Date(), config = {}) {
  let cursor = ceilToNextSubmitInterval(fromDate, config);
  const interval = getSubmitIntervalMinutes(config);
  for (let i = 0; i < 300; i++) {
    if (isEffectiveScheduledRun(cursor, config)) return cursor;
    cursor = new Date(cursor.getTime() + interval * 60 * 1000);
  }
  return null;
}

function findNextEffectiveRunOnOrAfter(fromDate, config = {}) {
  let cursor = ceilToNextSubmitInterval(fromDate, config);
  const interval = getSubmitIntervalMinutes(config);
  for (let i = 0; i < 300; i++) {
    if (isEffectiveScheduledRun(cursor, config)) return cursor;
    cursor = new Date(cursor.getTime() + interval * 60 * 1000);
  }
  return null;
}

function findNextDownloadScheduledRunOnOrAfter(fromDate = new Date()) {
  for (let i = 0; i < 48; i++) {
    const candidate = new Date(fromDate.getTime());
    candidate.setHours(candidate.getHours() + i, 5, 0, 0);
    if (candidate.getHours() % 2 === 1 && candidate >= fromDate) {
      return candidate;
    }
  }
  return null;
}

function readLockStatus(lockDir = '/tmp/jimeng-feishu-direct.lock') {
  const pidFile = path.join(lockDir, 'pid');
  if (!fs.existsSync(pidFile)) {
    return { lockDir, exists: false, running: false, pid: null };
  }
  const pid = String(fs.readFileSync(pidFile, 'utf8')).trim();
  let running = false;
  if (pid) {
    try {
      process.kill(Number(pid), 0);
      running = true;
    } catch (_) {
      running = false;
    }
  }
  return { lockDir, exists: true, running, pid: pid || null };
}

function summarizeLocalSubmissions(records) {
  const byStatus = {};
  for (const item of records) {
    const key = item.status || 'unknown';
    byStatus[key] = (byStatus[key] || 0) + 1;
  }

  const activeStatuses = new Set(['submitting', 'submitted', 'rendering', 'downloaded', 'upload_failed']);
  const blockingStatuses = new Set(['submitting', 'broken_state']);
  const active = records.filter(item => activeStatuses.has(item.status));
  const blocking = records.filter(item => blockingStatuses.has(item.status) || (item.status === 'submitted' && item.queue_observed !== true));

  return { byStatus, active, blocking };
}

function dedupeLocalTaskEntries(records, targetStatus) {
  const picked = new Map();
  const filtered = records
    .filter(item => item.status === targetStatus)
    .sort((a, b) => {
      const aTime = Date.parse(a.state_updated_at || a.updated_at || a.created_at || 0) || 0;
      const bTime = Date.parse(b.state_updated_at || b.updated_at || b.created_at || 0) || 0;
      return bTime - aTime;
    });

  for (const item of filtered) {
    const key = item.record_id || item.task_name || item.trace_id;
    if (!key || picked.has(key)) continue;
    picked.set(key, {
      recordId: item.record_id,
      taskName: item.task_name,
      status: item.status,
      resultSyncStatus: item.submit_confirmed_by || '',
      traceId: item.trace_id
    });
  }

  return Array.from(picked.values());
}

function summarizeFeishuRecords(config, records) {
  const result = {
    byStatus: {},
    pendingLike: [],
    blocked: [],
    retryLike: [],
    uploaded: []
  };

  for (const item of records) {
    const fields = item.fields || {};
    const status = String(fields[config.statusField] || '').trim() || '空';
    const resultSyncStatus = String(fields[config.fields.resultSyncStatus] || '').trim();
    const taskName = String(fields[config.fields.taskName] || '').trim();
    const recordId = item.record_id;

    result.byStatus[status] = (result.byStatus[status] || 0) + 1;

    const entry = { recordId, taskName, status, resultSyncStatus };
    if ((config.pendingStatuses || []).includes(status)) {
      result.pendingLike.push(entry);
    }
    if (status === '阻塞' || resultSyncStatus === 'blocked') {
      result.blocked.push(entry);
    }
    if (status === '待处理' && resultSyncStatus === 'retry_pending') {
      result.retryLike.push(entry);
    }
    if (resultSyncStatus === 'uploaded') {
      result.uploaded.push(entry);
    }
  }

  return result;
}

function formatDate(date) {
  if (!date) return 'N/A';
  const formatter = new Intl.DateTimeFormat('zh-CN', {
    timeZone: DISPLAY_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23'
  });
  const parts = formatter.formatToParts(date);
  const map = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
}

function printSection(title) {
  console.log(`\n[${title}]`);
}

function minutesBetween(later, earlier) {
  return Math.max(0, Math.round((later.getTime() - earlier.getTime()) / 60000));
}

function buildRecommendation({ now, nextRun, nextAssetRun, lock, feishuSummary, submitPauseState }) {
  const retryPendingCount = feishuSummary.retryLike.length;
  const blockedCount = feishuSummary.blocked.length;

  if (submitPauseState?.active) {
    return {
      action: '人工恢复积分后再唤醒 submit 线',
      reason: `当前提单已因积分不足暂停；删除 ${submitPauseState.path} 后才能恢复自动提单。`
    };
  }

  if (lock.running) {
    return {
      action: '等待',
      reason: `当前已有主流程在运行（PID=${lock.pid}），不建议再人工补跑。`
    };
  }

  if (retryPendingCount > 0) {
    const nextRunMinutes = nextRun ? minutesBetween(nextRun, now) : null;
    if (nextRunMinutes !== null && nextRunMinutes > 5) {
      return {
        action: '建议立即补跑一轮',
        reason: `飞书当前有 ${retryPendingCount} 条待处理且标记为重试候选，距离下一次自动提单约 ${nextRunMinutes} 分钟。`
      };
    }
    return {
      action: '可等待自动轮次',
      reason: `飞书当前有 ${retryPendingCount} 条待处理且标记为重试候选，距离下一次自动提单约 ${nextRunMinutes} 分钟。`
    };
  }

  if (blockedCount > 0) {
    return {
      action: '等待人工修复阻塞项',
      reason: `飞书当前有 ${blockedCount} 条阻塞任务，建议先人工处理提示词或图片问题。`
    };
  }

  const nextAssetMinutes = nextAssetRun ? minutesBetween(nextAssetRun, now) : null;
  if (nextAssetMinutes !== null && nextAssetMinutes > 30) {
    return {
      action: '建议仅在确认有已生成视频时手动补下载',
      reason: `下一次自动资产扫描约在 ${nextAssetMinutes} 分钟后；如你已确认资产页有可下载视频，可人工补跑一轮。`
    };
  }

  return {
    action: '等待',
    reason: '当前主流程状态正常，建议等待下一次自动轮次。'
  };
}

async function main() {
  const configArgIndex = process.argv.indexOf('--config');
  const configPath = configArgIndex >= 0
    ? path.resolve(process.argv[configArgIndex + 1])
    : path.resolve(__dirname, 'feishu-direct.json');
  const now = new Date();
  const config = loadConfig(configPath);
  const localSubmissions = listSubmissionRecords(config);
  const localSummary = summarizeLocalSubmissions(localSubmissions);
  const cache = readAssetScanCache(config);
  const submitPauseState = readSubmitPauseState(config);
  const nextRun = findNextEffectiveRun(now, config);
  const assetIntervalMinutes = Number(config.assetScanIntervalMinutes || 120);
  const assetEligibleAt = cache.lastAssetScanAt
    ? new Date(cache.lastAssetScanAt.getTime() + assetIntervalMinutes * 60 * 1000)
    : now;
  const nextAssetAnchor = assetEligibleAt > now ? assetEligibleAt : now;
  const nextAssetRun = findNextDownloadScheduledRunOnOrAfter(nextAssetAnchor);
  const lock = readLockStatus();

  let feishuSummary = {
    byStatus: {},
    pendingLike: [],
    blocked: [],
    retryLike: [],
    uploaded: []
  };
  let feishuError = null;
  try {
    const tenantToken = await getTenantAccessToken();
    const feishuRecords = await listFeishuRecords(config, tenantToken);
    feishuSummary = summarizeFeishuRecords(config, feishuRecords);
  } catch (error) {
    feishuError = error.message;
  }
  const recommendation = buildRecommendation({
    now,
    nextRun,
    nextAssetRun,
    lock,
    submitPauseState,
    feishuSummary: feishuError ? {
      ...feishuSummary,
      retryLike: dedupeLocalTaskEntries(localSubmissions, 'retry_pending'),
      blocked: dedupeLocalTaskEntries(localSubmissions, 'blocked')
    } : feishuSummary
  });

  console.log('即梦飞书主流程状态');
  console.log(`当前时间(北京时间): ${formatDate(now)}`);
  console.log(`配置文件: ${configPath}`);

  printSection('调度');
  console.log(`下一次有效提单轮次(北京时间): ${formatDate(nextRun)}`);
  console.log(`上次资产扫描(北京时间): ${formatDate(cache.lastAssetScanAt)}`);
  console.log(`资产扫描冷却到期(北京时间): ${formatDate(assetEligibleAt)}`);
  console.log(`下一次有效资产扫描轮次(北京时间): ${formatDate(nextAssetRun)}`);

  printSection('进程锁');
  console.log(`锁目录存在: ${lock.exists ? '是' : '否'}`);
  console.log(`当前正在运行: ${lock.running ? `是 (PID=${lock.pid})` : '否'}`);

  printSection('提单保护');
  console.log(`积分不足暂停: ${submitPauseState.active ? '是' : '否'}`);
  if (submitPauseState.active) {
    console.log(`暂停文件: ${submitPauseState.path}`);
    if (submitPauseState.raw) {
      console.log(`暂停时间(北京时间): ${submitPauseState.raw.paused_at || '未知'}`);
      console.log(`触发任务: ${submitPauseState.raw.task_name || submitPauseState.raw.record_id || '未知'}`);
      console.log(`原因: ${submitPauseState.raw.error_message || submitPauseState.raw.reason || '积分不足'}`);
    } else if (submitPauseState.error) {
      console.log(`读取暂停信息失败: ${submitPauseState.error}`);
    }
  }

  printSection('飞书当前口径');
  if (feishuError) {
    console.log('数据源: 本地回退口径');
    console.log(`飞书状态读取失败，已回退本地口径: ${feishuError}`);
  } else {
    console.log('数据源: 飞书实时数据');
    const orderedFeishuStatusKeys = Object.keys(feishuSummary.byStatus).sort();
    for (const key of orderedFeishuStatusKeys) {
      console.log(`- ${key}: ${feishuSummary.byStatus[key]}`);
    }
    console.log(`飞书待处理/部分提交: ${feishuSummary.pendingLike.length}`);
    console.log(`飞书阻塞: ${feishuSummary.blocked.length}`);
    console.log(`飞书重试候选: ${feishuSummary.retryLike.length}`);

    if (feishuSummary.retryLike.length > 0) {
      printSection('飞书重试候选任务');
      for (const item of feishuSummary.retryLike.slice(0, 8)) {
        console.log(`- ${item.taskName || item.recordId} | status=${item.status} | resultSyncStatus=${item.resultSyncStatus || '空'}`);
      }
    }
  }

  printSection('本地辅助口径');
  console.log(`本地 trace 总数: ${localSubmissions.length}`);
  const orderedLocalStatusKeys = Object.keys(localSummary.byStatus).sort();
  for (const key of orderedLocalStatusKeys) {
    console.log(`- ${key}: ${localSummary.byStatus[key]}`);
  }
  console.log(`本地活跃未闭环: ${localSummary.active.length}`);
  console.log(`本地提单阻塞: ${localSummary.blocking.length}`);
  const localRetryLike = dedupeLocalTaskEntries(localSubmissions, 'retry_pending');
  if (feishuError && localRetryLike.length > 0) {
    console.log(`本地去重后重试候选: ${localRetryLike.length}`);
    for (const item of localRetryLike.slice(0, 8)) {
      console.log(`- ${item.taskName || item.recordId || item.traceId} | status=${item.status} | signal=${item.resultSyncStatus || '空'}`);
    }
  }

  printSection('建议动作');
  console.log(`建议: ${recommendation.action}`);
  console.log(`原因: ${recommendation.reason}`);
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exit(1);
});
