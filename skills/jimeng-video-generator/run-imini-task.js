#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const os = require('os');
const { getAccessToken, listAllRecords, updateRecord, downloadFile, setActiveConfigForNetwork } = require('./lib/feishu-client');
const { buildTaskContext, formatBeijingTimestamp } = require('./lib/task-context');
const { FIELD_TYPE, normalizeTextField, getAttachmentList } = require('./lib/field-normalizers');
const { resolveChannel } = require('./channel-router');
const { loadChannelHealth, initChannelHealth, recordChannelSuccess, recordChannelFailure } = require('./channel-health');
const { isModelSupportedOnImini, mapModelForImini } = require('./platforms/imini/model-map');
const { buildProductLockCard, formatProductLockCard, extractHardConstraints, enrichProductLockCardWithLLM } = require('./platforms/imini/product-lock');
const { buildFirstFramePrompt } = require('./platforms/imini/first-frame');
const { detectCategory } = require('./platforms/imini/category-rules');

const config = require('./feishu-direct.json');

const ocPath = path.join(os.homedir(), '.openclaw', 'openclaw.json');
const oc = JSON.parse(fs.readFileSync(ocPath, 'utf8'));
const fc = oc.channels?.feishu || {};
config.appId = fc.appId || config.appId;
config.appSecret = fc.appSecret || config.appSecret;
setActiveConfigForNetwork(config);

async function main() {
  const recordId = process.argv.find(a => a.startsWith('--record-id='))?.split('=')[1];
  const dryRun = process.argv.includes('--dry-run');

  console.log('🎯 imini 渠道测试');
  console.log('================================');

  initChannelHealth(config);

  const token = await getAccessToken(config);
  const records = await listAllRecords(config, token);

  let targetRecord = null;
  if (recordId) {
    targetRecord = records.find(r => r.record_id === recordId);
    if (!targetRecord) {
      console.error(`❌ 找不到 record_id=${recordId}`);
      process.exit(1);
    }
  } else {
    targetRecord = records.find(r => {
      const status = String(r.fields?.['状态'] || '').trim();
      const channel = String(r.fields?.['渠道'] || '').trim();
      return (status === '待处理' || status === '部分提交') && channel === 'imini';
    });
    if (!targetRecord) {
      targetRecord = records.find(r => {
        const status = String(r.fields?.['状态'] || '').trim();
        return status === '待处理' || status === '部分提交';
      });
    }
  }

  if (!targetRecord) {
    console.error('❌ 没有找到待处理任务');
    process.exit(1);
  }

  const context = buildTaskContext(targetRecord, config);
  console.log(`\n📋 任务信息:`);
  console.log(`   record_id: ${context.recordId}`);
  console.log(`   任务名: ${context.taskName}`);
  console.log(`   渠道: ${context.channel || '(空)'}`);
  console.log(`   渠道来源: ${context.channelSource || '(空)'}`);
  console.log(`   模型: ${context.model}`);
  console.log(`   参考图数: ${context.attachments.length}`);
  console.log(`   提示词长度: ${context.prompt.length}`);

  const channelDecision = resolveChannel(context, config);
  console.log(`\n🔀 渠道路由:`);
  console.log(`   渠道: ${channelDecision.channel}`);
  console.log(`   来源: ${channelDecision.source}`);
  console.log(`   原因: ${channelDecision.reason}`);

  if (channelDecision.channel !== 'imini') {
    console.log('\n⚠️ 渠道路由结果不是 imini，退出');
    process.exit(0);
  }

  const modelSupported = isModelSupportedOnImini(context.model);
  console.log(`\n🤖 模型: ${context.model} -> imini: ${modelSupported ? mapModelForImini(context.model) : '不支持'}`);

  if (!modelSupported) {
    console.log('⚠️ 当前模型不支持 imini，将使用默认 Seedance 2.0');
  }

  console.log(`\n📦 下载参考图...`);
  const taskDir = path.join(config.runtimeRoot, context.taskName, '图片');
  fs.mkdirSync(taskDir, { recursive: true });

  const imagePaths = [];
  for (let i = 0; i < context.attachments.length; i++) {
    const attachment = context.attachments[i];
    const outputPath = path.join(taskDir, `${String(i + 1).padStart(2, '0')}-${attachment.fileName || `image-${i + 1}.jpg`}`);
    console.log(`   下载 ${attachment.fileName}...`);
    await downloadFile(token, attachment.fileToken, outputPath);
    imagePaths.push(outputPath);
  }
  console.log(`   已下载 ${imagePaths.length} 张参考图`);

  console.log(`\n🔍 生成商品一致性锁定卡...`);
  const hardConstraints = extractHardConstraints(context.prompt);
  const lock = buildProductLockCard(context.prompt, imagePaths, null);
  const category = detectCategory(context.prompt, lock);
  console.log(`   商品类目: ${category.category || '未识别'}`);
  console.log(`   锁定卡:\n${formatProductLockCard(lock)}`);

  if (!lock.productType && imagePaths.length > 0) {
    console.log('\n🧠 LLM 补充锁定卡...');
    try {
      const enriched = await enrichProductLockCardWithLLM(lock, context.prompt, imagePaths);
      console.log(`   补充后商品类型: ${enriched.productType || '仍缺失'}`);
    } catch (e) {
      console.log(`   ⚠️ LLM 补充失败: ${e.message}`);
    }
  }

  console.log(`\n🎨 生成首帧提示词...`);
  const firstFramePrompt = buildFirstFramePrompt(context, lock, category.category);
  console.log(`   提示词长度: ${firstFramePrompt.length} 字符`);
  console.log(`   提示词预览:\n${firstFramePrompt.slice(0, 300)}...`);

  if (dryRun) {
    console.log('\n✅ dry-run 模式，不执行实际提交');
    console.log(`\n📊 熔断状态:`);
    const health = loadChannelHealth(config);
    console.log(JSON.stringify(health, null, 2));
    return;
  }

  console.log(`\n📊 熔断状态:`);
  const health = loadChannelHealth(config);
  console.log(JSON.stringify(health, null, 2));

  if (health.disabled) {
    console.log('\n🔴 imini 已熔断，无法提交');
    console.log('如需重置熔断: node -e "require(\'./channel-health\').resetChannelCircuitBreaker(require(\'./feishu-direct.json\'))"');
    return;
  }

  console.log('\n⚠️ 实际提交需要 Chrome 已登录 imini，目前仅输出路由和首帧结果');
  console.log('完整 imini 提交流程请使用: node run-imini-task.js --submit');

  await updateRecord(config, token, context.recordId, {
    '首帧提示词': firstFramePrompt.slice(0, 5000),
    '首帧状态': '生成中',
    '商品一致性描述': formatProductLockCard(lock)
  });
  console.log('\n✅ 已将首帧提示词和锁定卡回写飞书');
}

main().catch(error => {
  console.error(`❌ 错误: ${error.message}`);
  console.error(error.stack);
  process.exit(1);
});