#!/usr/bin/env node

'use strict';

const fs = require('fs');
const path = require('path');

const { dispatchFlowNotification } = require('../short-video-automation-mvp/lib/openclaw-flow-notify');
const { WORKSPACE_ROOT } = require('../short-video-automation-mvp/lib/runtime-paths');
const { expandHome, listSubmissionRecords, getStateRoot } = require('./trace-state');

const DEFAULT_CONFIG_PATH = path.resolve(__dirname, 'feishu-direct.json');

const SUCCESSFUL_SUBMIT_CONFIRMERS = new Set([
  'queue_growth+credits_changed',
  'queue_growth_after_error',
  'queue_growth_after_error_deferred'
]);

const ASSET_FAILURE_STATUSES = new Set(['timed_out', 'claim_failed']);
const GENERATION_FAILURE_STATUSES = new Set(['failed', 'blocked']);
const ASSET_PENDING_STATUSES = new Set(['submitted', 'rendering', 'observing', 'downloaded']);

function parseArgs(argv) {
  const args = {
    configPath: DEFAULT_CONFIG_PATH,
    hours: 24,
    notifyConfig: '',
    reportFile: '',
    dryRun: false,
    notifyDryRun: false
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--config') {
      args.configPath = path.resolve(argv[++i]);
    } else if (arg === '--hours') {
      args.hours = Math.max(1, Number(argv[++i]) || 24);
    } else if (arg === '--notify-config') {
      args.notifyConfig = path.resolve(argv[++i]);
    } else if (arg === '--report-file') {
      args.reportFile = expandPath(argv[++i]);
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--notify-dry-run') {
      args.notifyDryRun = true;
    } else if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`未知参数: ${arg}`);
    }
  }

  return args;
}

function printHelp() {
  console.log(`
用法:
  node skills/jimeng-video-generator/send-jimeng-daily-report.js [options]

说明:
  每天早上 9 点统计过去 24 小时即梦提单和资产抓取情况，并通过 OpenClaw/飞书发送日报。

选项:
  --config <path>         即梦主配置，默认 skills/jimeng-video-generator/feishu-direct.json
  --hours <n>             统计过去 n 小时，默认 24
  --notify-config <path>  通知配置，默认读取 feishu-direct.json.dailyReport.notifyConfigPath
  --report-file <path>    报表 JSON 输出路径，默认读取 feishu-direct.json.dailyReport.reportFile
  --dry-run               只输出日报 JSON，不发送通知
  --notify-dry-run        生成通知文本但不真正发送
`);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function expandPath(value) {
  if (!value) return '';
  const expanded = expandHome(String(value));
  return path.isAbsolute(expanded) ? expanded : path.resolve(WORKSPACE_ROOT, expanded);
}

function loadConfig(configPath) {
  const raw = readJson(configPath);
  raw.runtimeRoot = expandHome(raw.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  raw.dailyReport = raw.dailyReport || {};
  return raw;
}

function toBeijingDate(input = new Date()) {
  const date = input instanceof Date ? input : new Date(input);
  return new Date(date.getTime() + 8 * 60 * 60 * 1000);
}

function formatBeijing(input) {
  if (!input) return '';
  const date = input instanceof Date ? input : new Date(input);
  if (Number.isNaN(date.getTime())) return '';
  const bj = toBeijingDate(date).toISOString().slice(0, 19).replace('T', ' ');
  return `${bj} +08:00`;
}

function parseTimestamp(value) {
  const ts = Date.parse(String(value || ''));
  return Number.isFinite(ts) ? ts : null;
}

function inWindow(ts, startMs, endMs) {
  return Number.isFinite(ts) && ts >= startMs && ts < endMs;
}

function normalizeReasonText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function bucketGenerationFailureReason(submission) {
  const raw = normalizeReasonText(
    submission.error_message ||
    submission.submit_confirmation_note ||
    submission.result ||
    '未知失败'
  );

  if (/高峰期|无法提交更多任务|平台限流/.test(raw)) return '高峰期限流';
  if (/参考图片上传失败/.test(raw)) return '参考图片上传失败';
  if (/上传前参数校验失败|最终参数校验失败/.test(raw)) return '参数校验失败';
  if (/0\/1生成完成，1 项失败回到底部/.test(raw)) return '生成后即时失败并回到底部';
  if (/提交确认未通过|submit_unconfirmed/.test(raw)) return '提交确认未通过（队列未增长）';
  if (/积分不足|点数不足|余额不足|灵感值不足|低于阈值/.test(raw)) return '积分不足';
  if (/未找到生成按钮/.test(raw)) return '生成按钮识别失败';
  if (/404 Not Found|Guru Meditation|Goofy Deploy Page Server/.test(raw)) return '即梦生成页异常（404/Guru）';
  if (/参考图为空/.test(raw)) return '参考图为空';
  if (/提示词为空/.test(raw)) return '提示词为空';
  if (/上传失败/.test(raw)) return '图片上传失败';
  return raw.length > 40 ? `${raw.slice(0, 40)}...` : raw;
}

function bucketRetryReason(submission) {
  const raw = normalizeReasonText(
    submission.error_message ||
    submission.submit_confirmation_note ||
    submission.result ||
    '自动重试'
  );

  if (submission.submit_confirmed_by === 'platform_limited' || /高峰期|无法提交更多任务|平台限流/.test(raw)) {
    return '高峰期限流自动重试（明确未提交）';
  }
  if (submission.submit_confirmed_by === 'submit_unconfirmed' || /提交确认未通过|队列未增长/.test(raw)) {
    return '队列未确认进队自动重试（高风险保护）';
  }
  if (submission.submit_confirmed_by === 'generate_error_notice' || /0\/1生成完成，1 项失败回到底部/.test(raw)) {
    return '生成后即时失败自动重试';
  }
  if (submission.submit_confirmed_by === 'insufficient_credits' || /积分不足|点数不足|余额不足|灵感值不足|低于阈值/.test(raw)) {
    return '积分不足暂停后待重试';
  }
  return raw.length > 40 ? `${raw.slice(0, 40)}...` : raw;
}

function bucketAssetFailureReason(submission) {
  const raw = normalizeReasonText(
    submission.error_message ||
    submission.submit_confirmation_note ||
    submission.result ||
    submission.status ||
    '资产抓取失败'
  );
  if (submission.status === 'timed_out' || /超时/.test(raw)) return '资产认领超时';
  if (submission.status === 'claim_failed' || /claim_failed|认领失败/.test(raw)) return '资产认领失败';
  return raw.length > 40 ? `${raw.slice(0, 40)}...` : raw;
}

function getAssetUpdateTimestamp(submission) {
  if (!submission || typeof submission !== 'object') return null;
  return parseTimestamp(
    submission.state_updated_at ||
    submission.uploaded_at ||
    submission.downloaded_at
  );
}

function getAssetFailureTimestamp(submission) {
  if (!submission || typeof submission !== 'object') return null;
  const status = String(submission.status || '');
  if (status === 'timed_out') {
    return parseTimestamp(submission.timed_out_at) || getAssetUpdateTimestamp(submission);
  }
  if (status === 'claim_failed') {
    return parseTimestamp(submission.claim_failed_at) || getAssetUpdateTimestamp(submission);
  }
  return getAssetUpdateTimestamp(submission);
}

function isSuccessfulSubmit(submission) {
  if (!submission || typeof submission !== 'object') return false;
  if (SUCCESSFUL_SUBMIT_CONFIRMERS.has(String(submission.submit_confirmed_by || ''))) return true;
  if (submission.queue_observed === true) return true;
  if (parseTimestamp(submission.first_zero_queue_at)) return true;
  if (String(submission.status || '') === 'uploaded') return true;
  return false;
}

function countBy(values) {
  const counter = new Map();
  for (const value of values) {
    const key = String(value || '未知');
    counter.set(key, (counter.get(key) || 0) + 1);
  }
  return Array.from(counter.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], 'zh-CN'))
    .map(([label, count]) => ({ label, count }));
}

function topLines(items, limit = 5) {
  return items.slice(0, limit).map(item => `- ${item.label}：${item.count}`);
}

function hasReason(items, pattern) {
  return items.some(item => pattern.test(String(item.label || '')));
}

function summarizeSubmissions(config, hours) {
  const now = new Date();
  const endMs = now.getTime();
  const startMs = endMs - hours * 60 * 60 * 1000;
  const submissions = listSubmissionRecords(config);
  const latestSubmitMs = submissions.reduce((max, item) => {
    const ts = parseTimestamp(item.submit_time);
    return Number.isFinite(ts) && ts > max ? ts : max;
  }, Number.NEGATIVE_INFINITY);
  const latestAssetUpdateMs = submissions.reduce((max, item) => {
    const ts = parseTimestamp(item.state_updated_at || item.uploaded_at || item.downloaded_at);
    return Number.isFinite(ts) && ts > max ? ts : max;
  }, Number.NEGATIVE_INFINITY);

  const attempts = submissions.filter(item => inWindow(parseTimestamp(item.submit_time), startMs, endMs));
  const attemptTaskNames = new Set(
    attempts
      .map(item => String(item.task_name || '').trim())
      .filter(Boolean)
  );

  const generationFailures = attempts.filter(item => GENERATION_FAILURE_STATUSES.has(String(item.status || '')));
  const retryPending = attempts.filter(item => String(item.status || '') === 'retry_pending');
  const successfulSubmits = attempts.filter(isSuccessfulSubmit);

  const assetSuccesses = submissions.filter(item =>
    String(item.status || '') === 'uploaded' && inWindow(getAssetUpdateTimestamp(item), startMs, endMs)
  );
  const assetFailures = submissions.filter(item =>
    ASSET_FAILURE_STATUSES.has(String(item.status || '')) && inWindow(getAssetFailureTimestamp(item), startMs, endMs)
  );

  const pendingAssetClaims = attempts.filter(item =>
    isSuccessfulSubmit(item) && ASSET_PENDING_STATUSES.has(String(item.status || ''))
  );

  return {
    generatedAt: now.toISOString(),
    machineId: String(config.machineId || '').trim() || '未命名机器',
    windowHours: hours,
    windowStart: new Date(startMs).toISOString(),
    windowEnd: now.toISOString(),
    runtimeRoot: config.runtimeRoot,
    stateRoot: getStateRoot(config),
    latestSubmitAt: Number.isFinite(latestSubmitMs) ? new Date(latestSubmitMs).toISOString() : '',
    latestAssetUpdateAt: Number.isFinite(latestAssetUpdateMs) ? new Date(latestAssetUpdateMs).toISOString() : '',
    generation: {
      attemptCount: attempts.length,
      uniqueTaskCount: attemptTaskNames.size,
      successfulSubmitCount: successfulSubmits.length,
      failedCount: generationFailures.length,
      retryPendingCount: retryPending.length,
      failureReasons: countBy(generationFailures.map(bucketGenerationFailureReason)),
      retryReasons: countBy(retryPending.map(bucketRetryReason)),
      retryPolicy: {
        maxPlatformLimitedRetries: Math.max(0, Number(config.maxPlatformLimitedRetries) || 1),
        maxSubmitUnconfirmedRetries: Math.max(0, Number(config.maxSubmitUnconfirmedRetries) || 2),
        submitUnconfirmedRetryWindowMinutes: Math.max(1, Number(config.submitUnconfirmedRetryWindowMinutes) || 120)
      }
    },
    asset: {
      uploadedCount: assetSuccesses.length,
      failedCount: assetFailures.length,
      pendingCount: pendingAssetClaims.length,
      failureReasons: countBy(assetFailures.map(bucketAssetFailureReason))
    }
  };
}

function buildNotificationText(report) {
  const lines = [
    `【即梦日报｜${report.machineId}｜前${report.windowHours}小时】`,
    `统计窗口：${formatBeijing(report.windowStart)} ~ ${formatBeijing(report.windowEnd)}`,
    `机器：${report.machineId}`,
    `数据源：本机即梦 runtime trace`,
    '',
    '提单情况：',
    `- 发起提单尝试：${report.generation.attemptCount} 次`,
    `- 涉及任务：${report.generation.uniqueTaskCount} 条`,
    `- 明确成功进队：${report.generation.successfulSubmitCount} 次`,
    `- 最终失败：${report.generation.failedCount} 次`,
    `- 自动重试中：${report.generation.retryPendingCount} 次`
  ];

  if (report.generation.failureReasons.length > 0) {
    lines.push('- 失败主因：');
    lines.push(...topLines(report.generation.failureReasons, 5));
  }

  if (report.generation.retryReasons.length > 0) {
    lines.push('- 自动重试主因：');
    lines.push(...topLines(report.generation.retryReasons, 5));

    const policy = report.generation.retryPolicy || {};
    const hasPlatformLimited = hasReason(report.generation.retryReasons, /高峰期|平台限流/);
    const hasSubmitUnconfirmed = hasReason(report.generation.retryReasons, /队列未确认|提交确认未通过/) ||
      hasReason(report.generation.failureReasons || [], /提交确认未通过|队列未增长/);
    if (hasPlatformLimited || hasSubmitUnconfirmed) {
      lines.push('- 自动重试规则说明：');
      if (hasPlatformLimited) {
        lines.push(`- 高峰期限流：即梦明确拒绝提交，按未提交处理，最多自动重试 ${policy.maxPlatformLimitedRetries || 10} 次`);
      }
      if (hasSubmitUnconfirmed) {
        const maxRetries = policy.maxSubmitUnconfirmedRetries ?? 2;
        const blockAttempt = maxRetries + 1;
        const windowMinutes = policy.submitUnconfirmedRetryWindowMinutes || 120;
        lines.push(`- 队列未确认进队：点击生成后队列未增长，${windowMinutes} 分钟内只自动重试 ${maxRetries} 次；第 ${blockAttempt} 次仍未确认会转人工复核，避免重复提交`);
      }
    }
  }

  lines.push('');
  lines.push('资产抓取情况：');
  lines.push(`- 成功写回飞书：${report.asset.uploadedCount} 条`);
  lines.push(`- 抓取失败：${report.asset.failedCount} 条`);
  lines.push(`- 仍待认领：${report.asset.pendingCount} 条`);

  if (report.asset.failureReasons.length > 0) {
    lines.push('- 资产失败主因：');
    lines.push(...topLines(report.asset.failureReasons, 5));
  }

  if (report.generation.attemptCount === 0 && report.asset.uploadedCount === 0 && report.asset.failedCount === 0) {
    lines.push('');
    lines.push('说明：');
    lines.push('- 过去统计窗口内，本机没有新的提单或资产回写记录');
    if (report.latestSubmitAt) {
      lines.push(`- 本机最近一次提单记录：${formatBeijing(report.latestSubmitAt)}`);
    }
    if (report.latestAssetUpdateAt) {
      lines.push(`- 本机最近一次资产状态更新：${formatBeijing(report.latestAssetUpdateAt)}`);
    }
  }

  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);
  const config = loadConfig(args.configPath);
  const dailyReportConfig = config.dailyReport || {};

  if (dailyReportConfig.enabled === false) {
    console.log(JSON.stringify({ mode: 'jimeng_daily_report', skipped: 'disabled_in_config' }, null, 2));
    return;
  }

  const hours = Math.max(1, Number(args.hours || dailyReportConfig.hours || 24) || 24);
  const notifyConfig = args.notifyConfig || expandPath(dailyReportConfig.notifyConfigPath || 'data/fastmoss-notify-config.json');
  const reportFile = args.reportFile || expandPath(
    dailyReportConfig.reportFile || path.join(config.runtimeRoot, '_state', 'jimeng-daily-report-latest.json')
  );

  const report = summarizeSubmissions(config, hours);
  const text = buildNotificationText(report);

  fs.mkdirSync(path.dirname(reportFile), { recursive: true });
  fs.writeFileSync(reportFile, `${JSON.stringify({ ...report, notificationText: text }, null, 2)}\n`, 'utf8');

  const event = {
    event: 'jimeng_daily_summary',
    title: `即梦日报｜${report.machineId}｜过去 ${hours} 小时`,
    severity: report.generation.failedCount > 0 || report.asset.failedCount > 0 ? 'warning' : 'info',
    batch_status: 'daily_summary',
    reason: `${report.machineId} 过去 ${hours} 小时发起提单尝试 ${report.generation.attemptCount} 次，成功写回视频 ${report.asset.uploadedCount} 条。`,
    state_file: reportFile
  };

  if (args.dryRun) {
    console.log(JSON.stringify({ mode: 'jimeng_daily_report', dry_run: true, report, text, notifyConfig, reportFile }, null, 2));
    return;
  }

  const notifyResult = await dispatchFlowNotification(event, {
    configPath: notifyConfig,
    dryRun: args.notifyDryRun,
    text
  });

  console.log(JSON.stringify({
    mode: 'jimeng_daily_report',
    reportFile,
    notifyConfig,
    report,
    notifyResult
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exit(1);
});
