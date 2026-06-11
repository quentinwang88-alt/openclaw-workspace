const fs = require('fs');
const path = require('path');
const { expandHome } = require('./trace-state');

const SUBMIT_LOCK_DIR = () => {
  const config = typeof global !== 'undefined' && global.__jimengConfig;
  if (config) {
    return path.join(expandHome(config.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime'), '_state', 'submit-locks');
  }
  return path.join(require('os').homedir(), 'Desktop', 'temp', 'jimeng-feishu-runtime', '_state', 'submit-locks');
};

const DEFAULT_MAX_CONCURRENT_SUBMITS = 1;

function getMaxConcurrentSubmits(config, channel = 'imini') {
  const channelConfig = (config.channels || {})[channel] || {};
  const configured = Number(channelConfig.maxConcurrentSubmits || config.maxConcurrentIminiSubmits);
  if (Number.isFinite(configured) && configured > 0) {
    return Math.max(1, Math.floor(configured));
  }
  return DEFAULT_MAX_CONCURRENT_SUBMITS;
}

function getSubmitLockDir(config) {
  const dir = path.join(expandHome(config.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime'), '_state', 'submit-locks');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function acquireSubmitLock(config, recordId, channel = 'imini') {
  const lockDir = getSubmitLockDir(config);
  const activeLocks = fs.readdirSync(lockDir).filter(f => f.startsWith(`${channel}-`) && f.endsWith('.lock'));

  // Clean stale locks (older than 10 minutes)
  const now = Date.now();
  for (const lockFile of activeLocks) {
    const lockPath = path.join(lockDir, lockFile);
    try {
      const stat = fs.statSync(lockPath);
      if (now - stat.mtimeMs > 10 * 60 * 1000) {
        fs.unlinkSync(lockPath);
      }
    } catch (e) { /* ignore */ }
  }

  // Re-read after cleanup
  const currentLocks = fs.readdirSync(lockDir).filter(f => f.startsWith(`${channel}-`) && f.endsWith('.lock'));

  const maxConcurrentSubmits = getMaxConcurrentSubmits(config, channel);

  if (currentLocks.length >= maxConcurrentSubmits) {
    const lockIds = currentLocks.map(f => f.replace(`${channel}-`, '').replace('.lock', ''));
    return { acquired: false, reason: `已达到最大并发数(${maxConcurrentSubmits})`, activeLocks: lockIds };
  }

  const lockFile = path.join(lockDir, `${channel}-${recordId}.lock`);
  fs.writeFileSync(lockFile, JSON.stringify({ recordId, channel, acquiredAt: new Date().toISOString() }));
  return { acquired: true, lockFile };
}

function releaseSubmitLock(config, recordId, channel = 'imini') {
  const lockDir = getSubmitLockDir(config);
  const lockFile = path.join(lockDir, `${channel}-${recordId}.lock`);
  try {
    if (fs.existsSync(lockFile)) {
      fs.unlinkSync(lockFile);
    }
  } catch (e) { /* ignore */ }
}

function getChannelHealthDir(config) {
  const runtimeRoot = expandHome(config.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  const healthDir = path.join(runtimeRoot, '_state', 'channel-health');
  fs.mkdirSync(healthDir, { recursive: true });
  return healthDir;
}

function getChannelHealthPath(config, channel) {
  return path.join(getChannelHealthDir(config), `${channel}.json`);
}

function initChannelHealth(config) {
  const healthDir = getChannelHealthDir(config);
  const iminiPath = path.join(healthDir, 'imini.json');

  const defaultHealth = {
    platform: 'imini',
    disabled: false,
    consecutiveGenerationFailures: 0,
    lastFailureAt: '',
    lastSuccessAt: '',
    disabledAt: '',
    disabledReason: ''
  };

  if (!fs.existsSync(iminiPath)) {
    fs.writeFileSync(iminiPath, `${JSON.stringify(defaultHealth, null, 2)}\n`);
  }

  return defaultHealth;
}

function loadChannelHealth(config, channel = 'imini') {
  const healthPath = getChannelHealthPath(config, channel);
  if (!fs.existsSync(healthPath)) {
    return initChannelHealth(config);
  }

  try {
    return JSON.parse(fs.readFileSync(healthPath, 'utf8'));
  } catch (error) {
    return initChannelHealth(config);
  }
}

function saveChannelHealth(config, health, channel = 'imini') {
  const healthPath = getChannelHealthPath(config, channel);
  fs.mkdirSync(path.dirname(healthPath), { recursive: true });
  fs.writeFileSync(healthPath, `${JSON.stringify(health, null, 2)}\n`);
  return healthPath;
}

function recordChannelSuccess(config, channel = 'imini') {
  const health = loadChannelHealth(config, channel);
  health.consecutiveGenerationFailures = 0;
  health.lastSuccessAt = new Date().toISOString();
  if (health.disabled) {
    health.disabled = false;
    health.disabledAt = '';
    health.disabledReason = '';
  }
  saveChannelHealth(config, health, channel);
  return health;
}

function recordChannelFailure(config, channel = 'imini', reason = '') {
  const iminiConfig = (config.channels || {}).imini || {};
  const threshold = Math.max(1, Number(iminiConfig.failureCircuitBreakerThreshold) || 5);

  const health = loadChannelHealth(config, channel);
  health.consecutiveGenerationFailures = (health.consecutiveGenerationFailures || 0) + 1;
  health.lastFailureAt = new Date().toISOString();

  if (health.consecutiveGenerationFailures >= threshold && !health.disabled) {
    health.disabled = true;
    health.disabledAt = new Date().toISOString();
    health.disabledReason = `连续 ${health.consecutiveGenerationFailures} 个明确失败，自动熔断`;
    console.log(`🔴 ${channel} 已熔断: ${health.disabledReason}`);
  }

  saveChannelHealth(config, health, channel);
  return health;
}

function resetChannelCircuitBreaker(config, channel = 'imini') {
  const health = loadChannelHealth(config, channel);
  health.disabled = false;
  health.consecutiveGenerationFailures = 0;
  health.disabledAt = '';
  health.disabledReason = '';
  saveChannelHealth(config, health, channel);
  return health;
}

module.exports = {
  initChannelHealth,
  loadChannelHealth,
  saveChannelHealth,
  recordChannelSuccess,
  recordChannelFailure,
  resetChannelCircuitBreaker,
  acquireSubmitLock,
  releaseSubmitLock,
  getMaxConcurrentSubmits,
  MAX_CONCURRENT_SUBMITS: DEFAULT_MAX_CONCURRENT_SUBMITS
};
