#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const puppeteer = require('puppeteer-core');

const {
  processTask,
  checkGeneratingStatus,
  sleep
} = require('./folder-processor');
const { syncCompletedSubmissions } = require('./result-uploader');
const {
  loadOpenclawFeishuCredentials,
  getAccessToken,
  listTableFields,
  createField,
  updateField,
  updateRecord,
  getRecord,
  listAllRecords,
  requestJson,
  setActiveConfigForNetwork
} = require('./lib/feishu-client');
const {
  normalizeTextField,
  normalizeNumberField,
  normalizeBooleanField,
  sanitizeTaskName
} = require('./lib/field-normalizers');
const {
  parseExecutionOwner,
  formatBeijingTimestamp
} = require('./lib/task-context');
const {
  parseContentIdMetadata,
  parseScriptIdMetadata,
  buildPromptFingerprint
} = require('./lib/asset-match');
const {
  buildWorkerId,
  expandHome,
  generateTraceId,
  writeSubmissionRecord,
  updateSubmissionRecord
} = require('./trace-state');
const { resolveChannel } = require('./channel-router');

let processIminiTask = null;

const FIELD_TYPE = {
  TEXT: 1,
  NUMBER: 2,
  SINGLE_SELECT: 3,
  CHECKBOX: 7,
  URL: 15,
  ATTACHMENT: 17
};

const STATUS_COLORS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

function parseArgs(argv) {
  const args = {
    configPath: path.join(__dirname, 'segment-package.json'),
    dryRun: false,
    submitOnly: false,
    downloadOnly: false,
    oneShot: false,
    forceSubmit: false,
    limit: 1,
    recordId: '',
    productId: '',
    ensureSchema: true
  };
  for (const arg of argv) {
    if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--submit-only') args.submitOnly = true;
    else if (arg === '--download-only' || arg === '--resume-only') args.downloadOnly = true;
    else if (arg === '--one-shot') args.oneShot = true;
    else if (arg === '--force-submit') args.forceSubmit = true;
    else if (arg === '--no-ensure-schema') args.ensureSchema = false;
    else if (arg.startsWith('--config=')) args.configPath = arg.slice('--config='.length);
    else if (arg === '--config') args._expectConfig = true;
    else if (args._expectConfig) {
      args.configPath = arg;
      args._expectConfig = false;
    } else if (arg.startsWith('--record-id=')) args.recordId = arg.slice('--record-id='.length);
    else if (arg.startsWith('--product-id=')) args.productId = arg.slice('--product-id='.length);
    else if (arg.startsWith('--limit=')) args.limit = Math.max(1, Number(arg.slice('--limit='.length)) || 1);
  }
  return args;
}

function loadConfig(configPath) {
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const feishuCredentials = loadOpenclawFeishuCredentials();
  const config = {
    ...raw,
    appId: raw.appId || feishuCredentials.appId || '',
    appSecret: raw.appSecret || feishuCredentials.appSecret || '',
    runtimeRoot: expandHome(raw.runtimeRoot || '~/Desktop/temp/jimeng-segment-runtime')
  };
  config.fields = config.fields || {};
  config.channels = config.channels || { default: config.defaultChannel || '即梦' };
  config.statusOptions = [
    ...(Array.isArray(config.pendingStatuses) ? config.pendingStatuses : ['待提单']),
    config.processingStatus || '生成中',
    config.submittedStatus || '已提单',
    config.uploadedStatus || '已回流',
    config.failedStatus || '失败',
    '暂停'
  ].filter((item, index, list) => item && list.indexOf(item) === index);
  setActiveConfigForNetwork(config);
  return config;
}

async function resolveConfigTable(config, token) {
  if (!config.tableUrl) {
    if (!config.appToken || !config.tableId) {
      throw new Error('缺少 tableUrl 或 appToken/tableId');
    }
    return config;
  }
  const info = parseFeishuBitableUrl(config.tableUrl);
  if (!info.tableId) {
    throw new Error(`无法从飞书 URL 解析 tableId: ${config.tableUrl}`);
  }
  config.tableId = config.tableId || info.tableId;
  config.viewId = config.viewId || info.viewId;
  if (info.isWiki) {
    const data = await requestJson(
      'GET',
      `/open-apis/wiki/v2/spaces/get_node?token=${encodeURIComponent(info.appToken)}`,
      token
    );
    const node = data.node || {};
    if (node.obj_type !== 'bitable' || !node.obj_token) {
      throw new Error(`wiki 节点不是 bitable 或未返回 obj_token: ${config.tableUrl}`);
    }
    config.appToken = node.obj_token;
  } else {
    config.appToken = config.appToken || info.appToken;
  }
  return config;
}

function parseFeishuBitableUrl(url) {
  const parsed = new URL(url);
  const tableId = parsed.searchParams.get('table') || '';
  const viewId = parsed.searchParams.get('view') || '';
  const wikiMatch = parsed.pathname.match(/\/wiki\/([^/?]+)/);
  const baseMatch = parsed.pathname.match(/\/base\/([^/?]+)/);
  return {
    appToken: wikiMatch?.[1] || baseMatch?.[1] || '',
    tableId,
    viewId,
    isWiki: Boolean(wikiMatch)
  };
}

async function ensureSegmentSchema(config, token) {
  const fields = await listTableFields(config, token);
  const fieldMap = new Map(fields.map(field => [field.field_name, field]));
  const specs = [
    { field_name: config.fields.channel, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.model, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.ratio, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.duration, type: FIELD_TYPE.NUMBER, property: { formatter: '0' } },
    { field_name: config.fields.repeatCount, type: FIELD_TYPE.NUMBER, property: { formatter: '0' } },
    { field_name: config.fields.submittedCount, type: FIELD_TYPE.NUMBER, property: { formatter: '0' } },
    { field_name: config.fields.executionOwner, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.latestTraceId, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.resultSyncStatus, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.result, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.videoAttachment, type: FIELD_TYPE.ATTACHMENT },
    { field_name: config.fields.videoFileName, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.submitTime, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.finishTime, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.errorMessage, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.platformTaskId, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.firstFramePrompt, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.firstFrameStatus, type: FIELD_TYPE.SINGLE_SELECT, property: { options: [{ name: '未生成' }, { name: '生成中' }, { name: '已生成' }, { name: '失败' }] } },
    { field_name: config.fields.firstFrameImage, type: FIELD_TYPE.ATTACHMENT },
    { field_name: config.fields.productConsistencyDescription || config.fields.productLock, type: FIELD_TYPE.TEXT },
    { field_name: config.fields.firstFrameConsistencyStatus, type: FIELD_TYPE.SINGLE_SELECT, property: { options: [{ name: '未检查' }, { name: '通过' }, { name: '失败' }] } },
    { field_name: config.fields.firstFrameConsistencyScore, type: FIELD_TYPE.NUMBER, property: { formatter: '0.00' } }
  ].filter(spec => spec.field_name);

  const changes = [];
  for (const spec of specs) {
    if (fieldMap.has(spec.field_name)) continue;
    try {
      await createField(config, token, spec);
      changes.push(`新增字段 ${spec.field_name}`);
    } catch (error) {
      if (!String(error.message || '').includes('FieldNameDuplicated')) {
        throw error;
      }
    }
  }

  const latestFields = await listTableFields(config, token);
  const latestMap = new Map(latestFields.map(field => [field.field_name, field]));
  const statusField = latestMap.get(config.statusField);
  if (statusField && statusField.type === FIELD_TYPE.SINGLE_SELECT) {
    const existingOptions = statusField.property?.options || [];
    const existingNames = new Set(existingOptions.map(option => option.name));
    const merged = [...existingOptions];
    for (let i = 0; i < config.statusOptions.length; i++) {
      const name = config.statusOptions[i];
      if (!existingNames.has(name)) {
        merged.push({ name, color: STATUS_COLORS[i % STATUS_COLORS.length] });
      }
    }
    if (merged.length !== existingOptions.length) {
      await updateField(config, token, statusField.field_id, {
        field_name: statusField.field_name,
        type: statusField.type,
        property: { options: merged }
      });
      changes.push(`补齐状态选项 ${config.statusOptions.join(' / ')}`);
    }
  }
  return changes;
}

function buildSegmentContext(record, config) {
  const fields = record.fields || {};
  const promptPackageId = text(fields[config.fields.taskName]) || record.record_id;
  const taskName = sanitizeTaskName(promptPackageId, record.record_id);
  const rawPrompt = text(fields[config.fields.prompt]);
  const prompt = ensurePromptMetadata(rawPrompt, promptPackageId);
  const executionOwner = text(fields[config.fields.executionOwner]);
  const executionOwnerParsed = parseExecutionOwner(executionOwner);
  const repeatTarget = Math.max(1, normalizeNumberField(fields[config.fields.repeatCount], 1));
  const submittedCount = Math.max(0, normalizeNumberField(fields[config.fields.submittedCount], 0));
  const market = text(fields[config.fields.market]);
  const skuId = text(fields[config.fields.skuId]) || 'DEFAULT';
  return {
    recordId: record.record_id,
    record,
    taskName,
    promptPackageId,
    productId: text(fields[config.fields.productId]),
    productName: text(fields[config.fields.productName]),
    skuId,
    market,
    category: text(fields[config.fields.category]),
    segmentType: text(fields[config.fields.segmentType]),
    grade: text(fields[config.fields.grade]),
    prompt,
    rawPrompt,
    referenceImagePackId: text(fields[config.fields.referenceImagePackId]),
    referenceImageVersion: normalizeNumberField(fields[config.fields.referenceImageVersion], 0),
    referenceImagePreviewUrl: text(fields[config.fields.referenceImagePreviewUrl]),
    referenceImageStatus: text(fields[config.fields.referenceImageStatus]),
    channel: text(fields[config.fields.channel]) || config.defaultChannel || config.channels?.default || '即梦',
    channelSource: text(fields[config.fields.channel]) ? 'manual' : 'default',
    model: text(fields[config.fields.model]) || config.defaultModel,
    mode: config.defaultMode,
    ratio: text(fields[config.fields.ratio]) || config.defaultRatio,
    duration: Math.max(1, normalizeNumberField(fields[config.fields.duration], config.defaultDuration)),
    repeatTarget,
    submittedCount,
    remainingCount: Math.max(0, repeatTarget - submittedCount),
    currentStatus: text(fields[config.statusField]),
    canSubmit: normalizeBooleanField(fields[config.fields.canSubmit]),
    executionOwner,
    executionOwnerMachineId: executionOwnerParsed.machineId,
    resultSyncStatus: text(fields[config.fields.resultSyncStatus])
  };
}

function ensurePromptMetadata(prompt, promptPackageId) {
  const base = String(prompt || '').trim();
  const parts = [base];
  if (!parseContentIdMetadata(base, '内容ID').found) {
    parts.push(`【内容ID】- ${promptPackageId}`);
  }
  if (!parseScriptIdMetadata(base).found) {
    parts.push(`【脚本ID】- ${promptPackageId}`);
  }
  return parts.filter(Boolean).join('\n\n');
}

function shouldSubmitRecord(record, config, args) {
  if (args.recordId && record.record_id !== args.recordId && text((record.fields || {})[config.fields.taskName]) !== args.recordId) {
    return false;
  }
  const context = buildSegmentContext(record, config);
  if (args.productId && context.productId !== args.productId) return false;
  if (!context.prompt || !context.productId) return false;
  if (!context.canSubmit && !args.recordId) return false;
  if (!args.recordId && !config.pendingStatuses.includes(context.currentStatus)) return false;
  if (context.remainingCount <= 0 && !args.forceSubmit) return false;
  if (context.executionOwnerMachineId && context.executionOwnerMachineId !== config.machineId) return false;
  if (!args.recordId && referenceReadinessIssue(context)) return false;
  return true;
}

function referenceReadinessIssue(context) {
  if (!context.referenceImagePackId) return '参考图包ID为空';
  const status = String(context.referenceImageStatus || '').trim().toLowerCase();
  const readyStatuses = new Set(['可用', 'ready', 'active', 'ok']);
  if (!readyStatuses.has(status)) return `参考图状态不是可用: ${context.referenceImageStatus || '空'}`;
  if (!context.referenceImagePreviewUrl) return '参考图预览地址为空';
  return '';
}

async function claimRecord(config, token, context) {
  if (context.executionOwnerMachineId && context.executionOwnerMachineId !== config.machineId) {
    return { ok: false, reason: `记录已归属 ${context.executionOwnerMachineId}` };
  }
  const claimToken = config.machineId || `${os.hostname()}`;
  await updateRecord(config, token, context.recordId, {
    [config.statusField]: config.processingStatus || '生成中',
    [config.fields.executionOwner]: claimToken,
    [config.fields.result]: `已由 ${claimToken} 认领，准备提单`,
    [config.fields.errorMessage]: ''
  });
  const latest = await getRecord(config, token, context.recordId);
  const latestFields = latest?.fields || {};
  const latestStatus = text(latestFields[config.statusField]);
  const latestOwner = text(latestFields[config.fields.executionOwner]);
  if (latestStatus !== (config.processingStatus || '生成中') || parseExecutionOwner(latestOwner).machineId !== claimToken) {
    return { ok: false, reason: `认领确认失败 status=${latestStatus || '空'} owner=${latestOwner || '空'}` };
  }
  return { ok: true, claimToken, latestRecord: latest };
}

function resolveReferenceImages(config, context, traceId) {
  const outputDir = path.join(config.runtimeRoot, '_tmp', traceId, 'reference-images');
  fs.mkdirSync(outputDir, { recursive: true });
  const scriptPath = path.join(config.autoMixcutRoot, config.referenceImageResolver || 'scripts/resolve_reference_image_pack.py');
  const args = [
    scriptPath,
    '--product-id', context.productId,
    '--market', context.market,
    '--sku-id', context.skuId || 'DEFAULT',
    '--output-dir', outputDir
  ];
  if (context.referenceImagePackId) {
    args.push('--reference-image-pack-id', context.referenceImagePackId);
  }
  const pythonPath = config.autoMixcutPython || 'python3';
  const stdout = execFileSync(pythonPath, args, {
    cwd: config.autoMixcutRoot,
    env: {
      ...process.env,
      ...(config.autoMixcutEnv || {})
    },
    encoding: 'utf8',
    maxBuffer: 10 * 1024 * 1024
  });
  const parsed = JSON.parse(stdout);
  if (!parsed.success) {
    throw new Error(`参考图包解析失败: ${stdout}`);
  }
  return {
    pack: parsed.pack || null,
    images: parsed.images || [],
    imagePaths: (parsed.images || []).map(item => item.local_path).filter(Boolean),
    outputDir
  };
}

function buildTask(context, config, referenceResult, traceId) {
  const taskRoot = path.join(config.runtimeRoot, '_tmp', traceId);
  fs.mkdirSync(taskRoot, { recursive: true });
  fs.writeFileSync(path.join(taskRoot, 'prompt.txt'), `${context.prompt}\n`);
  fs.writeFileSync(path.join(taskRoot, 'config.json'), `${JSON.stringify({
    model: context.model,
    mode: context.mode,
    ratio: context.ratio,
    duration: context.duration,
    reference_image_pack_id: referenceResult.pack?.reference_image_pack_id || ''
  }, null, 2)}\n`);
  return {
    name: context.taskName,
    folder: taskRoot,
    images: referenceResult.imagePaths,
    prompt: context.prompt,
    config: {}
  };
}

function buildTracePayload(context, config, traceId, submitIndex, referenceResult, platform) {
  const submittedAt = formatBeijingTimestamp();
  const fingerprint = buildPromptFingerprint(context.prompt);
  const contentIdMeta = parseContentIdMetadata(context.prompt, config.contentIdLabel || '内容ID');
  const scriptIdMeta = parseScriptIdMetadata(context.prompt);
  return {
    trace_id: traceId,
    platform,
    channel: platform,
    record_id: context.recordId,
    task_name: context.taskName,
    prompt_package_id: context.promptPackageId,
    product_id: context.productId,
    market: context.market,
    sku_id: context.skuId,
    segment_type: context.segmentType,
    grade: context.grade,
    reference_image_pack_id: referenceResult.pack?.reference_image_pack_id || context.referenceImagePackId || '',
    reference_image_version: Number(referenceResult.pack?.version || context.referenceImageVersion || 0),
    reference_image_count: referenceResult.images.length,
    reference_image_object_keys: referenceResult.images.map(item => item.object_key).filter(Boolean),
    submit_index: submitIndex,
    submit_time: submittedAt,
    status: 'submitting',
    worker_id: buildWorkerId(config),
    execution_owner: context.executionOwner || config.machineId || '',
    execution_machine_id: config.machineId || '',
    model: context.model,
    mode: context.mode,
    ratio: context.ratio,
    duration: context.duration,
    repeat_target: context.repeatTarget,
    submitted_count_before: context.submittedCount,
    prompt_length: context.prompt.length,
    prompt_preview: fingerprint.preview,
    prompt_anchor: fingerprint.anchor,
    prompt_hash: fingerprint.hash || crypto.createHash('sha1').update(context.prompt).digest('hex'),
    script_id: scriptIdMeta.id,
    content_id: contentIdMeta.id,
    content_id_label: contentIdMeta.label,
    content_id_found_in_prompt: contentIdMeta.found,
    content_id_label_name: config.contentIdLabel || '内容ID',
    enable_content_id_claim: config.enableContentIdClaim !== false,
    claim_strategy_order: Array.isArray(config.claimStrategyOrder) ? config.claimStrategyOrder : ['content_id', 'script_id', 'prompt_hash', 'prompt_anchor'],
    result_sync_status: 'rendering',
    state_updated_at: submittedAt,
    local_file_path: '',
    error_message: ''
  };
}

async function connectBrowser(config) {
  const browser = await puppeteer.connect({
    browserURL: `http://${config.cdpHost || '127.0.0.1'}:${config.cdpPort || 9222}`,
    defaultViewport: null,
    timeout: 30000,
    protocolTimeout: Number(config.protocolTimeoutMs || 300000)
  });
  const pages = await browser.pages();
  const page = pages.find(item => item.url().includes('jimeng.jianying.com')) || pages[0] || await browser.newPage();
  await page.bringToFront().catch(() => {});
  await page.setViewport({ width: 1600, height: 1000 }).catch(() => {});
  return { browser, page };
}

async function getIminiPage(browser, config) {
  const pages = await browser.pages();
  let page = pages.find(item => item.url().includes('imini.com'));
  if (!page) {
    page = await browser.newPage();
  }
  await page.bringToFront().catch(() => {});
  await page.setViewport({ width: 1600, height: 1000 }).catch(() => {});
  if (config.channels?.imini?.baseUrl) {
    await page.goto(config.channels.imini.baseUrl, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
  }
  return page;
}

async function submitContext({ config, token, browser, page, context, dryRun }) {
  const nextAttempt = context.submittedCount + 1;
  const traceId = generateTraceId({ recordId: context.recordId, taskName: context.taskName, submitIndex: nextAttempt });
  const channelDecision = resolveChannel(context, config);
  const platform = channelDecision.channel === 'imini' ? 'imini' : 'jimeng';
  const referenceIssue = referenceReadinessIssue(context);
  if (referenceIssue) {
    throw new Error(`参考图未就绪，停止提单: ${referenceIssue}`);
  }
  const referenceResult = resolveReferenceImages(config, context, traceId);
  if (referenceResult.imagePaths.length === 0) {
    throw new Error('参考图包没有可下载图片');
  }
  const tracePayload = buildTracePayload(context, config, traceId, nextAttempt, referenceResult, platform);
  writeSubmissionRecord(config, traceId, tracePayload);

  await updateRecord(config, token, context.recordId, {
    [config.statusField]: config.processingStatus || '生成中',
    [config.fields.latestTraceId]: traceId,
    [config.fields.resultSyncStatus]: 'rendering',
    [config.fields.submitTime]: tracePayload.submit_time,
    [config.fields.result]: `已解析 OSS 参考图包 ${tracePayload.reference_image_pack_id}，准备提交到 ${platform}`,
    [config.fields.referenceImagePackId]: tracePayload.reference_image_pack_id,
    [config.fields.referenceImageVersion]: tracePayload.reference_image_version,
    [config.fields.errorMessage]: '',
    [config.fields.videoFileName]: '',
    [config.fields.finishTime]: ''
  });

  if (dryRun) {
    return { success: true, dryRun: true, traceId, referenceResult };
  }

  const task = buildTask(context, config, referenceResult, traceId);
  context.taskFolderImages = task.images;
  context.referenceImagePack = referenceResult.pack || null;
  context.referenceImages = referenceResult.images || [];
  try {
    let result;
    if (platform === 'imini') {
      if (!processIminiTask) {
        ({ processIminiTask } = require('./platforms/imini/adapter'));
      }
      const iminiPage = await getIminiPage(browser, config);
      result = await processIminiTask({ page: iminiPage, context, config, token, traceId });
    } else {
      result = await processTask(page, task, {
        model: context.model || config.defaultModel,
        mode: context.mode || config.defaultMode,
        ratio: context.ratio || config.defaultRatio,
        duration: context.duration || config.defaultDuration,
        baseUrl: config.baseUrl,
        timeout: config.timeout,
        insufficientCreditsThreshold: config.insufficientCreditsThreshold
      }, true);
    }

    if (!result.success) {
      if (platform === 'jimeng' && result.code === 'submit_unconfirmed' && result.creditsChanged) {
        const newSubmittedCount = context.submittedCount + 1;
        const done = newSubmittedCount >= context.repeatTarget;
        const updated = updateSubmissionRecord(config, traceId, {
          status: 'submitted',
          submit_confirmed_by: 'credits_changed+submit_unconfirmed',
          submit_confirmation_note: '点击生成后积分发生变化，但页面未出现明确排队文案；按已提交观察，等待资产页认领',
          before_credits: result.beforeCredits || '',
          after_credits: result.afterCredits || '',
          credits_changed: true,
          state_updated_at: formatBeijingTimestamp()
        });
        await updateRecord(config, token, context.recordId, {
          [config.statusField]: done ? (config.submittedStatus || '已提单') : (config.processingStatus || '生成中'),
          [config.fields.submittedCount]: newSubmittedCount,
          [config.fields.resultSyncStatus]: 'rendering',
          [config.fields.result]: '已点击生成且积分发生变化，按已提交观察，等待资产抓取回传',
          [config.fields.latestTraceId]: traceId,
          [config.fields.errorMessage]: ''
        });
        return { success: true, traceId, result: updated };
      }

      const updated = updateSubmissionRecord(config, traceId, {
        status: result.retryable ? 'retry_pending' : 'failed',
        error_message: result.error || '提交失败',
        submit_confirmed_by: result.code || 'submit_failed',
        submit_confirmation_note: result.error || '提交失败',
        state_updated_at: formatBeijingTimestamp()
      });
      await updateRecord(config, token, context.recordId, {
        [config.statusField]: config.failedStatus || '失败',
        [config.fields.resultSyncStatus]: updated.status,
        [config.fields.result]: result.error || '提交失败',
        [config.fields.errorMessage]: result.error || '提交失败'
      });
      return { success: false, traceId, result };
    }

    const newSubmittedCount = context.submittedCount + 1;
    const done = newSubmittedCount >= context.repeatTarget;
    const updated = updateSubmissionRecord(config, traceId, {
      status: platform === 'jimeng' ? 'rendering' : 'submitted',
      submit_confirmed_by: result.code || result.confirmedBy || `${platform}_submit_success`,
      submit_confirmation_note: platform === 'jimeng'
        ? '页面检测到排队信号，等待资产页抓取回传'
        : 'imini 页面提交成功，等待资产页抓取回传',
      platform_task_id: result.taskId || traceId,
      state_updated_at: formatBeijingTimestamp()
    });
    await updateRecord(config, token, context.recordId, {
      [config.statusField]: done ? (config.submittedStatus || '已提单') : (config.processingStatus || '生成中'),
      [config.fields.submittedCount]: newSubmittedCount,
      [config.fields.resultSyncStatus]: 'rendering',
      [config.fields.result]: `已提交到 ${platform}，等待资产抓取回传`,
      [config.fields.platformTaskId]: result.taskId || traceId,
      [config.fields.latestTraceId]: traceId,
      [config.fields.errorMessage]: ''
    });
    return { success: true, traceId, result: updated };
  } finally {
    await cleanupTempTask(traceId, config);
  }
}

async function cleanupTempTask(traceId, config) {
  const dir = path.join(config.runtimeRoot, '_tmp', traceId);
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch (error) {
    console.log(`⚠️ 清理临时参考图失败: ${error.message}`);
  }
}

async function runDownloadOnly(config, token, args) {
  const { browser, page } = await connectBrowser(config);
  let assetPage = null;
  try {
    let currentGeneratingCount = null;
    try {
      const status = await checkGeneratingStatus(page);
      currentGeneratingCount = status?.generating ?? null;
    } catch (error) {
      console.log(`⚠️ 获取生成中数量失败，按保守模式继续回流: ${error.message}`);
    }
    assetPage = await browser.newPage();
    await assetPage.setViewport({ width: 1600, height: 1000 }).catch(() => {});
    return syncCompletedSubmissions({
      config,
      token,
      page: assetPage,
      dryRun: args.dryRun,
      limit: args.limit,
      traceId: '',
      recordId: args.recordId,
      taskNames: null,
      channelFilter: '',
      currentGeneratingCount
    });
  } finally {
    if (assetPage && !assetPage.isClosed?.()) {
      await assetPage.close().catch(() => {});
    }
    await browser.disconnect();
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig(args.configPath);
  if (!config.appId || !config.appSecret) {
    throw new Error('飞书 appId/appSecret 缺失，请检查 ~/.openclaw/openclaw.json');
  }
  const token = await getAccessToken(config);
  await resolveConfigTable(config, token);

  if (args.ensureSchema && !args.dryRun) {
    const changes = await ensureSegmentSchema(config, token);
    changes.forEach(change => console.log(`🧩 ${change}`));
  }

  if (!args.submitOnly) {
    const summary = await runDownloadOnly(config, token, args);
    console.log(`📦 回流扫描: ${JSON.stringify(summary)}`);
    if (args.downloadOnly) return;
  }

  const records = await listAllRecords(config, token);
  const candidates = records.filter(record => shouldSubmitRecord(record, config, args)).slice(0, args.limit);
  if (candidates.length === 0) {
    console.log('没有找到可提单的 Prompt Package 记录');
    return;
  }
  if (args.dryRun) {
    console.log(JSON.stringify(candidates.map(record => buildSegmentContext(record, config)), null, 2));
    return;
  }

  const { browser, page } = await connectBrowser(config);
  try {
    for (const record of candidates) {
      const context = buildSegmentContext(record, config);
      const claim = await claimRecord(config, token, context);
      if (!claim.ok) {
        console.log(`⏭️ ${context.taskName}: ${claim.reason}`);
        continue;
      }
      const latestContext = buildSegmentContext(claim.latestRecord || record, config);
      console.log(`\n🎞️ 提交片段任务: ${latestContext.taskName} / ${latestContext.productId}`);
      try {
        const result = await submitContext({ config, token, browser, page, context: latestContext, dryRun: false });
        console.log(result.success ? `✅ 已提单: ${result.traceId}` : `❌ 提单失败: ${result.traceId}`);
      } catch (error) {
        console.log(`❌ ${latestContext.taskName}: ${error.message}`);
        await updateRecord(config, token, latestContext.recordId, {
          [config.statusField]: config.failedStatus || '失败',
          [config.fields.resultSyncStatus]: 'failed',
          [config.fields.result]: error.message,
          [config.fields.errorMessage]: error.message
        });
      }
      if (args.oneShot) break;
      await sleep(1000);
    }
  } finally {
    await browser.disconnect();
  }
}

function text(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? 'true' : '';
  if (Array.isArray(value)) {
    return value.map(text).filter(Boolean).join('\n').trim();
  }
  if (typeof value === 'object') {
    for (const key of ['text', 'name', 'value', 'link', 'url']) {
      if (typeof value[key] === 'string' && value[key].trim()) return value[key].trim();
    }
  }
  return String(value).trim();
}

if (require.main === module) {
  main().catch(error => {
    console.error(`❌ 视频片段 worker 失败: ${error.message}`);
    process.exit(1);
  });
}
