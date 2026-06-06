#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const {
  expandHome,
  getStateRoot,
  listSubmissionRecords
} = require('./trace-state');

const SCRIPT_DIR = __dirname;
const DEFAULT_CONFIG = path.join(SCRIPT_DIR, 'segment-package.json');
const OPEN_STATUSES = new Set([
  'submitted',
  'rendering',
  'observing',
  'downloaded',
  'upload_failed',
  'claim_failed',
  'timed_out'
]);
const TERMINAL_STATUSES = new Set(['uploaded', 'failed', 'blocked', 'broken_state']);

function parseArgs(argv) {
  const args = {
    configPath: DEFAULT_CONFIG,
    channel: 'imini',
    limit: 10,
    maxEmpty: 5,
    firstDelayMinutes: 30,
    intervalMinutes: 20,
    pollSeconds: 60,
    lockDir: '/tmp/jimeng-segment-asset-watch.lock',
    dryRun: false,
    once: false,
    startNow: false,
    planOnly: false
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--config' && argv[i + 1]) args.configPath = path.resolve(argv[++i]);
    else if (arg.startsWith('--config=')) args.configPath = path.resolve(arg.slice('--config='.length));
    else if (arg === '--channel' && argv[i + 1]) args.channel = argv[++i];
    else if (arg.startsWith('--channel=')) args.channel = arg.slice('--channel='.length);
    else if (arg === '--limit' && argv[i + 1]) args.limit = Number(argv[++i]) || args.limit;
    else if (arg.startsWith('--limit=')) args.limit = Number(arg.slice('--limit='.length)) || args.limit;
    else if (arg === '--max-empty' && argv[i + 1]) args.maxEmpty = Number(argv[++i]) || args.maxEmpty;
    else if (arg.startsWith('--max-empty=')) args.maxEmpty = Number(arg.slice('--max-empty='.length)) || args.maxEmpty;
    else if (arg === '--first-delay-minutes' && argv[i + 1]) args.firstDelayMinutes = Number(argv[++i]) || args.firstDelayMinutes;
    else if (arg.startsWith('--first-delay-minutes=')) args.firstDelayMinutes = Number(arg.slice('--first-delay-minutes='.length)) || args.firstDelayMinutes;
    else if (arg === '--interval-minutes' && argv[i + 1]) args.intervalMinutes = Number(argv[++i]) || args.intervalMinutes;
    else if (arg.startsWith('--interval-minutes=')) args.intervalMinutes = Number(arg.slice('--interval-minutes='.length)) || args.intervalMinutes;
    else if (arg === '--poll-seconds' && argv[i + 1]) args.pollSeconds = Number(argv[++i]) || args.pollSeconds;
    else if (arg.startsWith('--poll-seconds=')) args.pollSeconds = Number(arg.slice('--poll-seconds='.length)) || args.pollSeconds;
    else if (arg === '--lock-dir' && argv[i + 1]) args.lockDir = argv[++i];
    else if (arg.startsWith('--lock-dir=')) args.lockDir = arg.slice('--lock-dir='.length);
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--once') args.once = true;
    else if (arg === '--start-now') args.startNow = true;
    else if (arg === '--plan-only') args.planOnly = true;
  }
  return args;
}

function loadConfig(configPath) {
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  return raw;
}

function log(message) {
  console.log(`[${new Date().toLocaleString('zh-CN', { hour12: false })}] ${message}`);
}

function minuteOfDay(date) {
  return date.getHours() * 60 + date.getMinutes();
}

function isNightWindow(date = new Date()) {
  const minute = minuteOfDay(date);
  return minute >= 22 * 60 || minute < 8 * 60 + 30;
}

function nightStartFor(date = new Date()) {
  if (!isNightWindow(date)) return null;
  const start = new Date(date);
  start.setHours(22, 0, 0, 0);
  if (minuteOfDay(date) < 8 * 60 + 30) {
    start.setDate(start.getDate() - 1);
  }
  return start;
}

function midnightForNightStart(nightStart) {
  const midnight = new Date(nightStart);
  midnight.setDate(midnight.getDate() + 1);
  midnight.setHours(0, 0, 0, 0);
  return midnight;
}

function daySubmitWindowStart(nightStart) {
  const start = new Date(nightStart);
  start.setHours(8, 30, 0, 0);
  return start;
}

function parseTime(value) {
  const ms = Date.parse(value || '');
  return Number.isFinite(ms) ? ms : 0;
}

function normalizeChannel(value) {
  return String(value || '').trim().toLowerCase();
}

function matchesChannel(record, channel) {
  const wanted = normalizeChannel(channel);
  if (!wanted) return true;
  return normalizeChannel(record.channel || record.platform) === wanted;
}

function isRealSubmittedRecord(record) {
  const status = normalizeChannel(record.status);
  if (TERMINAL_STATUSES.has(status) && !record.uploaded_file_token) return false;
  if (status === 'failed' || status === 'blocked') return false;
  return Boolean(record.submit_time || record.submit_confirmed_by || record.platform_task_id);
}

function getRecords(config, channel) {
  return listSubmissionRecords(config)
    .filter(record => record && record.trace_id)
    .filter(record => matchesChannel(record, channel));
}

function recordsSince(records, sinceMs) {
  return records.filter(record => parseTime(record.submit_time || record.state_updated_at) >= sinceMs);
}

function openRecords(records) {
  return records.filter(record => OPEN_STATUSES.has(normalizeChannel(record.status)));
}

function describePlan(config, args, now = new Date()) {
  const nightStart = nightStartFor(now);
  if (!nightStart && !args.startNow) {
    return {
      active: false,
      reason: 'daytime_manual_only'
    };
  }

  const records = getRecords(config, args.channel);
  const start = nightStart || now;
  const nightRecords = recordsSince(records, start.getTime()).filter(isRealSubmittedRecord);
  const latestNightSubmitMs = Math.max(0, ...nightRecords.map(record => parseTime(record.submit_time || record.state_updated_at)));
  const midnight = nightStart ? midnightForNightStart(nightStart) : now;
  const dayStart = nightStart ? daySubmitWindowStart(nightStart) : new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const relevantRecords = recordsSince(records, dayStart.getTime()).filter(isRealSubmittedRecord);
  const openCount = openRecords(relevantRecords).length;

  let trigger = 'midnight_fallback';
  let dueAt = midnight;
  if (latestNightSubmitMs > 0) {
    trigger = 'night_submit_delay';
    dueAt = new Date(latestNightSubmitMs + args.firstDelayMinutes * 60 * 1000);
  }
  if (args.startNow) {
    trigger = 'manual_start_now';
    dueAt = now;
  }

  return {
    active: true,
    trigger,
    due: now.getTime() >= dueAt.getTime(),
    dueAt,
    nightStart,
    dayStart,
    latestNightSubmitAt: latestNightSubmitMs ? new Date(latestNightSubmitMs) : null,
    nightSubmitCount: nightRecords.length,
    relevantSubmitCount: relevantRecords.length,
    openCount
  };
}

function parseLastJson(output) {
  const text = String(output || '');
  for (let index = text.lastIndexOf('{'); index >= 0; index = text.lastIndexOf('{', index - 1)) {
    const candidate = text.slice(index).trim();
    try {
      return JSON.parse(candidate);
    } catch {
      continue;
    }
  }
  return null;
}

function runAssetScan(configPath, args) {
  const cmdArgs = [
    path.join(SCRIPT_DIR, 'result-uploader.js'),
    '--config', configPath,
    '--channel', args.channel,
    '--ignore-generating-count',
    '--limit', String(args.limit)
  ];
  if (args.dryRun) cmdArgs.push('--dry-run');

  log(`run asset scan: node ${cmdArgs.map(item => /\s/.test(item) ? JSON.stringify(item) : item).join(' ')}`);
  const result = spawnSync('node', cmdArgs, {
    cwd: SCRIPT_DIR,
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
    env: process.env
  });

  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);

  const parsed = parseLastJson(`${result.stdout || ''}\n${result.stderr || ''}`);
  if (result.status !== 0) {
    return {
      ok: false,
      uploaded: 0,
      downloaded: 0,
      reason: `result-uploader exited with ${result.status}`,
      parsed
    };
  }
  return {
    ok: true,
    uploaded: Number(parsed?.uploaded || 0),
    downloaded: Number(parsed?.downloaded || 0),
    reason: parsed?.reason || '',
    parsed
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function acquireLock(lockDir) {
  fs.mkdirSync(path.dirname(lockDir), { recursive: true });
  try {
    fs.mkdirSync(lockDir);
    fs.writeFileSync(path.join(lockDir, 'pid'), `${process.pid}\n`);
    return true;
  } catch {
    const pidPath = path.join(lockDir, 'pid');
    const existing = fs.existsSync(pidPath) ? Number(fs.readFileSync(pidPath, 'utf8').trim()) : 0;
    if (existing > 0) {
      try {
        process.kill(existing, 0);
        log(`asset watcher already running (PID=${existing}), skip`);
        return false;
      } catch {
        fs.rmSync(lockDir, { recursive: true, force: true });
        fs.mkdirSync(lockDir);
        fs.writeFileSync(pidPath, `${process.pid}\n`);
        return true;
      }
    }
    return false;
  }
}

function releaseLock(lockDir) {
  const pidPath = path.join(lockDir, 'pid');
  try {
    const owner = fs.existsSync(pidPath) ? fs.readFileSync(pidPath, 'utf8').trim() : '';
    if (owner === String(process.pid)) fs.rmSync(lockDir, { recursive: true, force: true });
  } catch {
    // Best effort.
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig(args.configPath);
  const stateRoot = getStateRoot(config);
  fs.mkdirSync(stateRoot, { recursive: true });

  if (!args.planOnly && !acquireLock(args.lockDir)) return;
  process.on('SIGINT', () => {
    releaseLock(args.lockDir);
    process.exit(0);
  });
  process.on('SIGTERM', () => {
    releaseLock(args.lockDir);
    process.exit(0);
  });

  let scanStarted = false;
  let emptyCount = 0;

  try {
    while (true) {
      const now = new Date();
      const plan = describePlan(config, args, now);
      if (args.planOnly) {
        console.log(JSON.stringify({
          ...plan,
          dueAt: plan.dueAt?.toISOString?.() || null,
          nightStart: plan.nightStart?.toISOString?.() || null,
          dayStart: plan.dayStart?.toISOString?.() || null,
          latestNightSubmitAt: plan.latestNightSubmitAt?.toISOString?.() || null
        }, null, 2));
        return;
      }

      if (!plan.active) {
        log('daytime manual-only window, asset watcher stops');
        return;
      }

      if (!scanStarted && !plan.due) {
        const waitMs = Math.max(
          1000,
          Math.min(args.pollSeconds * 1000, plan.dueAt.getTime() - now.getTime())
        );
        log(`asset scan not due: trigger=${plan.trigger}, dueAt=${plan.dueAt.toLocaleString('zh-CN', { hour12: false })}, nightSubmits=${plan.nightSubmitCount}, open=${plan.openCount}`);
        if (args.once) return;
        await sleep(waitMs);
        continue;
      }

      scanStarted = true;
      const scan = runAssetScan(args.configPath, args);
      const produced = scan.uploaded + scan.downloaded;
      const afterPlan = describePlan(config, args, new Date());
      log(`asset scan result: uploaded=${scan.uploaded}, downloaded=${scan.downloaded}, open=${afterPlan.openCount}, reason=${scan.reason || 'none'}`);

      if (produced > 0) {
        emptyCount = 0;
      } else {
        emptyCount += 1;
      }

      if (afterPlan.openCount === 0) {
        log('no open submitted segment-package assets in current watch window, stop asset watcher');
        return;
      }
      if (emptyCount >= args.maxEmpty) {
        log(`no new asset for ${emptyCount}/${args.maxEmpty} consecutive scans, stop asset watcher`);
        return;
      }
      if (args.once) return;
      if (!isNightWindow(new Date())) {
        log('reached daytime manual-only window, stop asset watcher');
        return;
      }
      log(`sleep ${args.intervalMinutes} minutes before next asset scan (empty=${emptyCount}/${args.maxEmpty})`);
      await sleep(args.intervalMinutes * 60 * 1000);
    }
  } finally {
    releaseLock(args.lockDir);
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error(`asset watcher failed: ${error.message}`);
    releaseLock(parseArgs(process.argv.slice(2)).lockDir);
    process.exit(1);
  });
}
