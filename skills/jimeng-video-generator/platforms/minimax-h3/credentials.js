const { execFileSync } = require('child_process');

function resolveApiKey(envName = 'METASO_MINIMAX_API_KEY') {
  const direct = String(process.env[envName] || '').trim();
  if (direct) return direct;
  if (process.platform !== 'darwin') return '';
  try {
    return String(execFileSync('launchctl', ['getenv', envName], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5000
    }) || '').trim();
  } catch (_) {
    return '';
  }
}

function requireApiKey(envName = 'METASO_MINIMAX_API_KEY') {
  const value = resolveApiKey(envName);
  if (!value) {
    throw new Error(`缺少环境变量 ${envName}；请通过当前进程环境或 launchctl setenv 正常注入`);
  }
  return value;
}

module.exports = { requireApiKey, resolveApiKey };
