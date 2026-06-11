const crypto = require('crypto');

const UINT32_MAX = 4294967295;

function stableHashForChannelRouting(recordId, taskName, submitIndex, channel) {
  const input = `${recordId || ''}:${taskName || ''}:${submitIndex || 0}:${channel || ''}`;
  const hash = crypto.createHash('sha256').update(input).digest('hex');
  return parseInt(hash.slice(0, 8), 16);
}

function resolveChannel(context, config) {
  const channelConfig = config.channels || {};
  const iminiConfig = channelConfig.imini || {};
  const defaultChannel = channelConfig.default || '即梦';

  const recordChannel = (context.channel || '').trim().toLowerCase();

  if (recordChannel === 'imini') {
    return {
      channel: 'imini',
      source: 'manual',
      reason: '渠道字段指定 imini'
    };
  }

  if (recordChannel === '即梦' || recordChannel) {
    return {
      channel: '即梦',
      source: 'manual',
      reason: `渠道字段指定 ${context.channel || defaultChannel}`
    };
  }

  if (!iminiConfig.enabled) {
    return {
      channel: defaultChannel,
      source: 'default',
      reason: 'imini 未启用'
    };
  }

  if (isChannelCircuitBroken(config)) {
    return {
      channel: defaultChannel,
      source: 'circuit_broken',
      reason: 'imini 已熔断，退回即梦'
    };
  }

  const autoAssignOwners = Array.isArray(iminiConfig.autoAssignOwners) ? iminiConfig.autoAssignOwners : [];
  const ownerMachineId = context.executionOwnerMachineId || '';

  if (autoAssignOwners.length > 0 && !autoAssignOwners.includes(ownerMachineId)) {
    return {
      channel: defaultChannel,
      source: 'owner_excluded',
      reason: `执行归属 ${ownerMachineId} 不在自动分流列表中`
    };
  }

  if (autoAssignOwners.length === 0) {
    return {
      channel: defaultChannel,
      source: 'no_auto_assign_owners',
      reason: '未配置自动分流执行归属列表'
    };
  }

  const ratio = Number(iminiConfig.autoAssignRatio) || 0;
  if (ratio <= 0 || ratio >= 1) {
    return {
      channel: defaultChannel,
      source: 'ratio_out_of_range',
      reason: `自动分流比例 ${ratio} 不在 (0, 1) 范围内`
    };
  }

  const submitIndex = context.submittedCount + 1;
  const hash = stableHashForChannelRouting(context.recordId, context.taskName, submitIndex, 'imini');

  if (hash / UINT32_MAX < ratio) {
    return {
      channel: 'imini',
      source: 'auto_random',
      reason: `稳定 hash ${hash} / ${UINT32_MAX} = ${(hash / UINT32_MAX).toFixed(4)} < 配置比例 ${ratio}`
    };
  }

  return {
    channel: defaultChannel,
    source: 'auto_random_miss',
    reason: `稳定 hash ${hash} / ${UINT32_MAX} = ${(hash / UINT32_MAX).toFixed(4)} >= 配置比例 ${ratio}`
  };
}

function isChannelCircuitBroken(config) {
  const fs = require('fs');
  const path = require('path');
  const { expandHome } = require('./trace-state');

  const runtimeRoot = expandHome(config.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  const healthFile = path.join(runtimeRoot, '_state', 'channel-health', 'imini.json');

  if (!fs.existsSync(healthFile)) {
    return false;
  }

  try {
    const health = JSON.parse(fs.readFileSync(healthFile, 'utf8'));
    return health.disabled === true;
  } catch (error) {
    return false;
  }
}

module.exports = {
  stableHashForChannelRouting,
  resolveChannel,
  isChannelCircuitBroken
};