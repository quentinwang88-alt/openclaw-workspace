const { resolveChannel } = require('../../channel-router');
const { isModelSupportedOnImini, mapModelForImini } = require('./model-map');
const { runFirstFramePipeline, submitToImini } = require('./submitter');
const { formatProductLockCard } = require('./product-lock');
const { recordChannelSuccess, recordChannelFailure } = require('../../channel-health');
const { uploadFileToFeishu, updateRecord } = require('../../lib/feishu-client');

function compactFields(fields) {
  return Object.fromEntries(
    Object.entries(fields).filter(([key, value]) => key && value !== undefined)
  );
}

function canHandle(context, config) {
  const resolved = resolveChannel(context, config);
  return resolved.channel === 'imini';
}

async function processIminiTask({ page, context, config, token, traceId }) {
  const resolved = resolveChannel(context, config);

  if (resolved.channel !== 'imini') {
    return { success: false, error: `渠道路由结果不是 imini: ${resolved.channel}`, code: 'wrong_channel' };
  }

  if (!isModelSupportedOnImini(context.model)) {
    const fallbackModel = mapModelForImini(context.model);
    if (!fallbackModel) {
      return {
        success: false,
        error: `模型 ${context.model} 不支持 imini`,
        code: 'unsupported_model',
        shouldBlock: context.channelSource !== 'auto_random',
        shouldFallBack: context.channelSource === 'auto_random'
      };
    }
  }

  const { channels: channelConfig } = config;
  const iminiConfig = (channelConfig || {}).imini || {};
  const originalImagePaths = context.taskFolderImages || [];

  console.log(`  🎯 imini 渠道处理: ${context.taskName}`);
  console.log(`    渠道来源: ${resolved.source}`);
  console.log(`    路由原因: ${resolved.reason}`);

  if (iminiConfig.firstFrame?.enabled !== false && originalImagePaths.length > 0) {
    const pipelineResult = await runFirstFramePipeline(context, config, originalImagePaths);

    if (!pipelineResult.success) {
      const failureResult = {
        success: false,
        error: pipelineResult.error,
        code: pipelineResult.code,
        shouldFallBack: pipelineResult.shouldFallBack,
        shouldBlock: pipelineResult.shouldBlock
      };

      if (pipelineResult.shouldFallBack) {
        console.log(`  ↩️ 首帧流水线失败，退回即梦: ${pipelineResult.error}`);
      } else if (pipelineResult.shouldBlock) {
        console.log(`  🛑 首帧流水线失败，转阻塞: ${pipelineResult.error}`);
      }

      recordChannelFailure(config, 'imini', pipelineResult.error);
      return failureResult;
    }

    context._firstFramePrompt = pipelineResult.firstFramePrompt;
    context._firstFrameImagePath = pipelineResult.firstFrameImagePath;
    context._productLock = pipelineResult.lock;
    context._category = pipelineResult.category;

    if (token && context._firstFrameImagePath) {
      try {
        const writebackFields = compactFields({
          [config.fields.firstFramePrompt || '首帧提示词']: String(context._firstFramePrompt || '').slice(0, 5000),
          [config.fields.productConsistencyDescription || config.fields.productLock || '商品一致性描述']: formatProductLockCard(context._productLock || {}).slice(0, 5000),
          [config.fields.firstFrameStatus || '首帧状态']: '已生成'
        });

        if (iminiConfig.firstFrame?.writeBackImageToFeishu === true) {
          const upload = await uploadFileToFeishu(config, token, context._firstFrameImagePath);
          writebackFields[config.fields.firstFrameImage || '首帧图片'] = [
            {
              file_token: upload.fileToken
            }
          ];
          console.log(`  🖼️ 首帧图片已上传飞书: ${upload.fileToken}`);
        } else {
          console.log(`  🖼️ 首帧图片已生成，仅用于 imini 上传: ${context._firstFrameImagePath}`);
        }

        await updateRecord(config, token, context.recordId, writebackFields);
      } catch (error) {
        console.log(`  ⚠️ 首帧图片回写飞书失败，不影响 imini 提交: ${error.message}`);
      }
    }
  }

  const submitResult = await submitToImini(page, context, context._productLock || null, context._firstFrameImagePath || null, config);

  if (submitResult.success) {
    recordChannelSuccess(config, 'imini');
  } else {
    const isHardFailure = /platform_error|mode_switch_failed|unsupported_model/.test(submitResult.code || '');
    if (isHardFailure) {
      recordChannelFailure(config, 'imini', submitResult.error);
    }
  }

  return submitResult;
}

module.exports = {
  canHandle,
  processIminiTask
};
