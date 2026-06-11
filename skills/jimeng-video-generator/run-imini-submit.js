#!/usr/bin/env node

const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const { getAccessToken, listAllRecords, updateRecord, downloadFile, uploadFileToFeishu, setActiveConfigForNetwork } = require('./lib/feishu-client');
const { buildTaskContext, formatBeijingTimestamp } = require('./lib/task-context');
const { normalizeTextField, getAttachmentList } = require('./lib/field-normalizers');
const { buildWorkerId, generateTraceId, writeSubmissionRecord } = require('./trace-state');
const { parseContentIdMetadata, parseScriptIdMetadata, buildPromptFingerprint } = require('./lib/asset-match');
const { resolveChannel } = require('./channel-router');
const { loadChannelHealth, initChannelHealth, acquireSubmitLock, releaseSubmitLock, getMaxConcurrentSubmits } = require('./channel-health');
const { isModelSupportedOnImini, mapModelForImini, mapDurationForImini, mapRatioForImini, mapResolutionForImini } = require('./platforms/imini/model-map');
const { buildProductLockCard, formatProductLockCard, extractHardConstraints, enrichProductLockCardWithLLM } = require('./platforms/imini/product-lock');
const { buildFirstFramePrompt, generateFirstFrameImageWithLLM } = require('./platforms/imini/first-frame');
const { detectCategory } = require('./platforms/imini/category-rules');
const { submitToImini } = require('./platforms/imini/submitter');

const config = require('./feishu-direct.json');
const ocPath = path.join(os.homedir(), '.openclaw', 'openclaw.json');
const oc = JSON.parse(fs.readFileSync(ocPath, 'utf8'));
const fc = oc.channels?.feishu || {};
config.appId = fc.appId || config.appId;
config.appSecret = fc.appSecret || config.appSecret;
setActiveConfigForNetwork(config);

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, os.homedir());
}

function buildIminiTraceSubmissionPayload(context, config, traceId, submitIndex, result = {}) {
  const submittedAt = formatBeijingTimestamp();
  const prompt = String(context.prompt || '');
  const fingerprint = buildPromptFingerprint(prompt);
  const scriptIdMeta = parseScriptIdMetadata(prompt);
  const contentIdMeta = parseContentIdMetadata(prompt, config.contentIdLabel || '内容ID');
  const normalizedPrompt = prompt.replace(/\s+/g, '').trim().toLowerCase();

  return {
    trace_id: traceId,
    platform: 'imini',
    channel: 'imini',
    record_id: context.recordId,
    task_name: context.taskName,
    submit_index: submitIndex,
    submit_time: submittedAt,
    status: 'submitted',
    worker_id: buildWorkerId(config),
    execution_owner: config.machineId || '',
    execution_machine_id: config.machineId || '',
    model: mapModelForImini(context.model) || 'Seedance 2.0',
    mode: '图片转视频',
    ratio: mapRatioForImini(context.ratio),
    duration: mapDurationForImini(context.duration),
    repeat_target: context.repeatTarget,
    submitted_count_before: context.submittedCount,
    prompt_length: prompt.length,
    prompt_preview: fingerprint.preview,
    prompt_anchor: fingerprint.anchor,
    prompt_hash: fingerprint.hash || crypto.createHash('sha1').update(normalizedPrompt).digest('hex'),
    script_id: scriptIdMeta.id,
    content_id: contentIdMeta.id,
    content_id_label: contentIdMeta.label,
    content_id_found_in_prompt: contentIdMeta.found,
    content_id_label_name: config.contentIdLabel || '内容ID',
    enable_content_id_claim: config.enableContentIdClaim !== false,
    claim_strategy_order: Array.isArray(config.claimStrategyOrder) ? config.claimStrategyOrder : ['content_id', 'script_id', 'prompt_hash', 'prompt_anchor'],
    submit_confirmed_by: result.code || 'imini_create_success',
    submit_confirmation_note: 'imini 页面点击创建成功，等待资产页抓取回传',
    platform_task_id: result.taskId || traceId,
    state_updated_at: submittedAt,
    local_file_path: ''
  };
}

async function main() {
  const noSubmit = process.argv.includes('--no-submit');
  const dryRun = process.argv.includes('--dry-run');
  const preflightSubmit = process.argv.includes('--preflight-submit');
  const skipLLM = process.argv.includes('--skip-llm');
  const forceFirstFrame = process.argv.includes('--force-first-frame');
  const writeFirstFrame = process.argv.includes('--write-first-frame');
  const allowReferenceFallback = process.argv.includes('--allow-reference-fallback');
  const allowRealSubmit = process.argv.includes('--confirm-imini-real-submit') || process.env.IMINI_ALLOW_REAL_SUBMIT === '1';
  const allowDuplicateVisible = process.argv.includes('--allow-duplicate-visible') || process.env.IMINI_ALLOW_DUPLICATE_VISIBLE === '1';
  const recordId = process.argv.find(a => a.startsWith('--record-id='))?.split('=')[1];
  if (allowRealSubmit) {
    config.channels = config.channels || {};
    config.channels.imini = config.channels.imini || {};
    config.channels.imini.allowRealSubmit = true;
    config.channels.imini.allowDuplicateVisible = allowDuplicateVisible;
  }
  if (preflightSubmit) {
    config.channels = config.channels || {};
    config.channels.imini = config.channels.imini || {};
    config.channels.imini.preflightOnly = true;
  }

  console.log('%c imini 完整提交流程');
  console.log('================================');
  if (dryRun) {
    console.log('模式: dry-run，不回写飞书、不连接浏览器、不提交');
  } else if (preflightSubmit) {
    console.log('模式: preflight-submit，打开浏览器并执行到创建前停止，不点击创建');
  } else if (noSubmit) {
    console.log('模式: no-submit，不连接浏览器、不提交');
  }

  if (!noSubmit && !dryRun && !preflightSubmit && !allowRealSubmit) {
    console.error('真实提交已被默认禁止。必须显式追加 --confirm-imini-real-submit 才允许点击 imini 创建按钮。');
    process.exit(1);
  }

  if (!recordId && !noSubmit && !dryRun) {
    console.error('真实提交必须显式传入 --record-id=...，避免误提交第一条 imini 待处理任务。');
    process.exit(1);
  }

  initChannelHealth(config);

  const token = await getAccessToken(config);
  const records = await listAllRecords(config, token);

  let targetRecord;
  if (recordId) {
    targetRecord = records.find(r => {
      if (r.record_id === recordId) return true;
      return buildTaskContext(r, config).taskName === recordId;
    });
  } else {
    targetRecord = records.find(r => {
      const channel = String(r.fields?.['渠道'] || '').trim();
      const status = String(r.fields?.['状态'] || '').trim();
      return channel === 'imini' && (status === '待处理' || status === '部分提交');
    });
  }

  if (!targetRecord) {
    console.error('没有找到 imini 渠道的待处理任务');
    process.exit(1);
  }

  const context = buildTaskContext(targetRecord, config);
  console.log(`任务: ${context.taskName}`);
  console.log(`   渠道: ${context.channel}, 来源: ${context.channelSource}`);
  console.log(`   模型: ${context.model}, 参考图: ${context.attachments.length}`);
  console.log(`   提示词: ${(context.prompt || '').slice(0, 100)}...`);
  console.log(`   比例: ${context.ratio}, 时长: ${context.duration}`);

  const channelDecision = resolveChannel(context, config);
  if (channelDecision.channel !== 'imini') {
    console.error('渠道路由不是 imini:', channelDecision);
    process.exit(1);
  }

  // Download reference images
  const runtimeRoot = expandHome(config.runtimeRoot || path.join(os.homedir(), 'Desktop', 'temp', 'jimeng-feishu-runtime'));
  const taskDir = path.join(runtimeRoot, context.taskName, '图片');
  fs.mkdirSync(taskDir, { recursive: true });
  const imagePaths = [];
  for (let i = 0; i < context.attachments.length; i++) {
    const att = context.attachments[i];
    const outPath = path.join(taskDir, `${String(i + 1).padStart(2, '0')}-${att.fileName || `image-${i + 1}.jpg`}`);
    console.log(`   下载 ${att.fileName}...`);
    await downloadFile(token, att.fileToken, outPath);
    imagePaths.push(outPath);
  }

  // Build product lock card
  const hardConstraints = extractHardConstraints(context.prompt);
  let lock = buildProductLockCard(context.prompt, imagePaths, null);
  const category = detectCategory(context.prompt, lock);
  console.log(`商品类目: ${category.category || '未识别'}`);

  if (!lock.productType && imagePaths.length > 0 && !skipLLM) {
    console.log('LLM 补充锁定卡...');
    try {
      const enriched = await enrichProductLockCardWithLLM(lock, context.prompt, imagePaths);
      lock = enriched;
      console.log(`    补充后: productType=${enriched.productType}`);
    } catch (e) {
      console.log(`    LLM 补充失败: ${e.message}`);
    }
  }

  // Generate first-frame prompt
  const firstFramePrompt = buildFirstFramePrompt(context, lock, category.category, ((config.channels || {}).imini || {}).firstFrame || {});
  console.log(`\n首帧提示词 (${firstFramePrompt.length} 字符):`);
  console.log(firstFramePrompt.slice(0, 300) + '...');

  // Generate first-frame image via LLM
  let firstFrameImagePath = null;
  const cachedFirstFramePath = path.join(runtimeRoot, context.taskName, 'first-frame.png');
  if (!forceFirstFrame && fs.existsSync(cachedFirstFramePath) && fs.statSync(cachedFirstFramePath).size > 1024) {
    firstFrameImagePath = cachedFirstFramePath;
    console.log(`\n复用已有首帧图片: ${firstFrameImagePath}`);
  } else if (!skipLLM && !dryRun && imagePaths.length > 0) {
    console.log('\n生成首帧图片...');
    const imgDir = path.join(runtimeRoot, context.taskName);
    const imgResult = await generateFirstFrameImageWithLLM(firstFramePrompt, imagePaths, {
      ...((config.channels || {}).imini || {}).firstFrame,
      outputPath: cachedFirstFramePath,
      ratio: context.ratio
    });
    if (imgResult.success && imgResult.imagePath) {
      firstFrameImagePath = imgResult.imagePath;
      console.log(`首帧图片已保存: ${firstFrameImagePath} (${imgResult.imageBytes || 0} bytes)`);
    } else {
      console.log(`首帧图片生成失败: ${imgResult.error || 'unknown'}`);
    }
  } else if (dryRun) {
    console.log('\ndry-run 模式跳过首帧图片生成');
  }

  // Fallback is opt-in only. imini relies on the first frame for product consistency,
  // so using a raw reference image by default can hide a broken first-frame pipeline.
  if (!firstFrameImagePath && imagePaths.length > 0 && allowReferenceFallback) {
    firstFrameImagePath = imagePaths[0];
    console.log(`使用参考图作为首帧: ${firstFrameImagePath}`);
  }

  // Write back to Feishu
  if (writeFirstFrame && !dryRun) {
    const firstFrameFields = {
      '首帧提示词': firstFramePrompt.slice(0, 5000),
      '商品一致性描述': formatProductLockCard(lock).slice(0, 5000),
      '首帧状态': firstFrameImagePath ? '已生成' : '失败'
    };

    if (firstFrameImagePath) {
      const upload = await uploadFileToFeishu(config, token, firstFrameImagePath);
      firstFrameFields[config.fields.firstFrameImage || '首帧图片'] = [
        {
          file_token: upload.fileToken
        }
      ];
      console.log(`首帧图片已上传飞书: ${upload.fileToken}`);
    }

    await updateRecord(config, token, context.recordId, firstFrameFields);
    console.log('首帧提示词和锁定卡已回写飞书');
  } else {
    console.log('跳过飞书首帧回写（需要 --write-first-frame 才会写入）');
  }

  if (noSubmit || dryRun) {
    if (noSubmit && !skipLLM && imagePaths.length > 0 && !firstFrameImagePath) {
      console.error('\nno-submit 首帧实测失败：没有生成可上传的首帧图片。');
      process.exit(1);
    }
    console.log('\n不执行浏览器提交');
    process.exit(0);
  }

  if (!firstFrameImagePath) {
    console.error('\n首帧图片未生成，停止提交。需要先接入真正的图片生成/落盘能力，或显式使用 --allow-reference-fallback 做人工测试。');
    process.exit(1);
  }

  // Acquire submit lock (max concurrent submissions)
  const lockResult = acquireSubmitLock(config, context.recordId, 'imini');
  if (!lockResult.acquired) {
    console.error(`\n提交被拒绝: ${lockResult.reason}`);
    console.error(`当前活跃提交: ${lockResult.activeLocks.join(', ')}`);
    process.exit(1);
  }
  console.log(`\n获取提交锁: ${context.recordId} (并发数上限 ${getMaxConcurrentSubmits(config, 'imini')})`);

  try {
    // Connect to Chrome
    console.log('连接 Chrome...');
    const browser = await puppeteer.connect({
      browserURL: 'http://127.0.0.1:9222',
      defaultViewport: null,
      timeout: 30000,
      protocolTimeout: Number(config.protocolTimeoutMs || 300000)
    });

    const pages = await browser.pages();
    let iminiPage = pages.find(p => p.url().includes('imini.com'));
    if (!iminiPage) {
      iminiPage = await browser.newPage();
    }
    await iminiPage.bringToFront().catch(() => {});
    await iminiPage.setViewport({ width: 1600, height: 1000 }).catch(() => {});

  // Build submit context with mapped values
  const iminiConfig = (config.channels || {}).imini || {};
    const submitContext = {
      ...context,
      model: mapModelForImini(context.model) || 'Seedance 2.0',
      duration: mapDurationForImini(context.duration),
      ratio: mapRatioForImini(context.ratio),
      prompt: context.prompt,
    };

    console.log('\n提交到 imini...');
    console.log(`   模型: ${submitContext.model}`);
    console.log(`   比例: ${submitContext.ratio}`);
    console.log(`   时长: ${submitContext.duration}s`);
    console.log(`   分辨率: ${iminiConfig.defaultResolution || '480P'}`);
    console.log(`   首帧: ${firstFrameImagePath ? path.basename(firstFrameImagePath) : '无'}`);

    const result = await submitToImini(iminiPage, submitContext, lock, firstFrameImagePath, config);

    if (result.success && result.code === 'preflight_ok') {
      console.log('\n预检成功，未点击创建按钮');
      console.log(`   code: ${result.code}`);
      console.log(`   preCheck: ${JSON.stringify(result.preCheck || {})}`);
    } else if (result.success) {
      console.log('\n提交成功!');
      console.log(`   code: ${result.code}`);

      const nextSubmitIndex = Number(context.submittedCount || 0) + 1;
      const traceId = generateTraceId({
        recordId: context.recordId,
        taskName: context.taskName,
        submitIndex: nextSubmitIndex
      });
      const tracePayload = buildIminiTraceSubmissionPayload(context, config, traceId, nextSubmitIndex, result);
      writeSubmissionRecord(config, traceId, tracePayload);

      await updateRecord(config, token, context.recordId, {
        [config.fields.firstFrameStatus || '首帧状态']: '已生成',
        [config.statusField || '状态']: '已提交',
        [config.fields.submittedCount || '已提交次数']: nextSubmitIndex,
        [config.fields.latestTraceId || '最新追踪ID']: traceId,
        [config.fields.resultSyncStatus || '结果回传状态']: 'rendering',
        [config.fields.submitTime || '提交时间']: tracePayload.submit_time,
        [config.fields.finishTime || '完成时间']: '',
        [config.fields.videoFileName || '生成视频文件名']: '',
        [config.fields.result || '结果说明']: 'imini 已提交，等待资产抓取回传',
        [config.fields.errorMessage || '错误信息']: '',
        [config.fields.platformTaskId || '平台任务ID']: result.taskId || traceId,
        [config.fields.channelSource || '渠道来源']: context.channelSource || '人工指定'
      });
      console.log('飞书状态已更新');
    } else {
      console.log(`\n提交失败: ${result.error}`);
      console.log(`   code: ${result.code}`);

      await updateRecord(config, token, context.recordId, {
        '首帧状态': '失败',
        '状态': '失败'
      });
    }

    // Screenshot
    const screenshotPath = path.join(runtimeRoot, '_state', 'imini-submit-result.png');
    fs.mkdirSync(path.dirname(screenshotPath), { recursive: true });
    await iminiPage.screenshot({ path: screenshotPath, fullPage: false });
    console.log(`截图: ${screenshotPath}`);

    await browser.disconnect();
  } finally {
    releaseSubmitLock(config, context.recordId, 'imini');
    console.log(`释放提交锁: ${context.recordId}`);
  }

  console.log('\n完成');
}

main().catch(e => {
  console.error('ERROR:', e.message);
  process.exit(1);
});
