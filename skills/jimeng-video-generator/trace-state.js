const fs = require('fs');
const path = require('path');
const os = require('os');

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, process.env.HOME || '~');
}

function getStateRoot(config) {
  if (config && config.stateRoot) {
    return expandHome(config.stateRoot);
  }
  const runtimeRoot = expandHome(config?.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  return path.join(runtimeRoot, '_state');
}

function getSubmissionsDir(config) {
  return path.join(getStateRoot(config), 'submissions');
}

function ensureStateDirs(config) {
  fs.mkdirSync(getSubmissionsDir(config), { recursive: true });
}

function getSubmissionFilePath(config, traceId) {
  return path.join(getSubmissionsDir(config), `${traceId}.json`);
}

function buildWorkerId(config) {
  return `${os.hostname()}:${config?.cdpHost || '127.0.0.1'}:${config?.cdpPort || 9222}`;
}

function formatTraceTimestamp(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mm = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${y}${m}${d}${hh}${mm}${ss}`;
}

function sanitizeSlug(value, fallback = 'TASK') {
  const cleaned = String(value || fallback)
    .replace(/[^\w-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 24);
  return cleaned || fallback;
}

function generateTraceId(context = {}, date = new Date()) {
  const stamp = formatTraceTimestamp(date);
  const recordTail = String(context.recordId || 'record').slice(-6);
  const submitIndex = String(context.submitIndex || 1).padStart(2, '0');
  const slug = sanitizeSlug(context.taskName || context.recordId || 'task');
  return `JM_${stamp}_${submitIndex}_${recordTail}_${slug}`;
}

function writeSubmissionRecord(config, traceId, payload) {
  ensureStateDirs(config);
  const filePath = getSubmissionFilePath(config, traceId);
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
  return filePath;
}

function readSubmissionRecord(config, traceId) {
  const filePath = getSubmissionFilePath(config, traceId);
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function updateSubmissionRecord(config, traceId, patch) {
  const existing = readSubmissionRecord(config, traceId) || { trace_id: traceId };
  const next = {
    ...existing,
    ...patch
  };
  writeSubmissionRecord(config, traceId, next);
  return next;
}

function listSubmissionRecords(config) {
  const dir = getSubmissionsDir(config);
  if (!fs.existsSync(dir)) {
    return [];
  }

  return fs.readdirSync(dir)
    .filter(name => name.endsWith('.json'))
    .map(name => {
      const filePath = path.join(dir, name);
      try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
      } catch (error) {
        return {
          trace_id: path.basename(name, '.json'),
          status: 'broken_state',
          error_message: error.message,
          state_file: filePath
        };
      }
    });
}

module.exports = {
  expandHome,
  getStateRoot,
  getSubmissionsDir,
  ensureStateDirs,
  getSubmissionFilePath,
  buildWorkerId,
  generateTraceId,
  writeSubmissionRecord,
  readSubmissionRecord,
  updateSubmissionRecord,
  listSubmissionRecords
};
