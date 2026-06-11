function canHandle(context, config) {
  const resolved = require('../../channel-router').resolveChannel(context, config);
  return resolved.channel === '即梦' || !resolved.channel;
}

async function processJimengTask({ context }) {
  return {
    success: true,
    channel: '即梦',
    handled: false,
    message: '即梦任务由现有 feishu-direct-monitor.js 处理，无需 adapter 执行'
  };
}

module.exports = {
  canHandle,
  processJimengTask
};
