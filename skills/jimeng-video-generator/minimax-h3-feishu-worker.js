#!/usr/bin/env node

const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { buildGenerationPayload, normalizeMode } = require('./platforms/minimax-h3/adapter');
const {
  SubmissionUncertainError,
  createTask,
  downloadPublicVideo,
  queryTask,
  uploadFile
} = require('./platforms/minimax-h3/client');
const { requireApiKey } = require('./platforms/minimax-h3/credentials');
const { uploadBitableFile } = require('./platforms/minimax-h3/feishu-upload');
const {
  createField,
  downloadFile,
  getAccessToken,
  getRecord,
  listAllRecords,
  listTableFields,
  loadOpenclawFeishuCredentials,
  setActiveConfigForNetwork,
  updateField,
  updateRecord
} = require('./lib/feishu-client');
const {
  buildUniqueAttachmentName,
  getAttachmentList,
  normalizeBooleanField,
  normalizeNumberField,
  normalizeTextField,
  sanitizeTaskName
} = require('./lib/field-normalizers');
const {
  expandHome,
  listSubmissionRecords,
  updateSubmissionRecord,
  writeSubmissionRecord
} = require('./trace-state');

const CHANNEL = 'METASO';
const MODEL_LABEL = 'MiniMax H3';
const ACTIVE_STATUSES = new Set(['已提交', '生成中', '提交中', '处理中']);
const PENDING_STATUSES = new Set(['待处理', '部分提交']);
const API_TERMINAL_FAILURES = new Set(['failed', 'cancelled', 'canceled', 'expired']);
const API_SUCCESS = new Set(['succeeded', 'success', 'completed', 'done']);

const DEFAULT_FIELDS = {
  taskName: '任务名',
  contentId: '内容ID',
  scriptId: '脚本ID',
  prompt: '提示词',
  images: ['参考图'],
  firstFrameImage: '首帧图片',
  lastFrameImage: '尾帧图片',
  allowNoReferenceImage: '免参考图',
  repeatCount: '生成次数',
  model: '模型',
  ratio: '视频比例',
  duration: '视频时长',
  resolution: '分辨率',
  channel: '渠道',
  executionOwner: '执行归属',
  submittedCount: '已提交次数',
  result: '结果说明',
  latestTraceId: '最新追踪ID',
  resultSyncStatus: '结果回传状态',
  videoAttachment: '生成视频',
  videoFileName: '生成视频文件名',
  submitTime: '提交时间',
  finishTime: '完成时间',
  errorMessage: '错误信息',
  platformTaskId: '平台任务ID',
  platformTaskStatus: '平台任务状态',
  submitFingerprint: '提交指纹',
  referenceMode: 'MiniMax参考模式'
};

function parseArgs(argv) {
  const args = {
    configPath: path.join(__dirname, 'feishu-direct.json'),
    dryRun: false,
    ensureSchemaOnly: false,
    submitOnly: false,
    pollOnly: false,
    recordId: '',
    limit: null
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--config' && argv[i + 1]) args.configPath = argv[++i];
    else if (arg === '--dry-run') args.dryRun = true;
    else if (arg === '--ensure-schema-only') args.ensureSchemaOnly = true;
    else if (arg === '--submit-only') args.submitOnly = true;
    else if (arg === '--poll-only') args.pollOnly = true;
    else if (arg === '--record-id' && argv[i + 1]) args.recordId = argv[++i];
    else if (arg === '--limit' && argv[i + 1]) args.limit = Math.max(1, Number(argv[++i]) || 1);
  }
  if (args.submitOnly && args.pollOnly) throw new Error('不能同时使用 --submit-only 和 --poll-only');
  return args;
}

function loadConfig(configPath) {
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const credentials = loadOpenclawFeishuCredentials();
  const h3 = raw.channels?.metasoH3 || {};
  const runtimeRoot = expandHome(raw.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  return {
    ...raw,
    appId: raw.appId || credentials.appId || '',
    appSecret: raw.appSecret || credentials.appSecret || '',
    pageSize: 500,
    fields: { ...DEFAULT_FIELDS, ...(raw.fields || {}) },
    metasoH3: {
      enabled: h3.enabled !== false,
      baseUrl: h3.baseUrl || 'https://metaso.cn/api/minimax',
      apiKeyEnv: h3.apiKeyEnv || 'METASO_MINIMAX_API_KEY',
      channelValue: h3.channelValue || CHANNEL,
      maxSubmitsPerRun: Math.max(1, Number(h3.maxSubmitsPerRun || 2)),
      requestTimeoutMs: Math.max(30_000, Number(h3.requestTimeoutMs || 90_000)),
      maxDownloadBytes: Math.max(20 * 1024 * 1024, Number(h3.maxDownloadBytes || 500 * 1024 * 1024)),
      watermark: Boolean(h3.watermark)
    },
    stateRoot: path.join(runtimeRoot, '_state', 'minimax-h3'),
    h3DownloadRoot: path.join(runtimeRoot, 'minimax-h3-downloads')
  };
}

function isH3Channel(value, config) {
  const normalized = String(value || '').trim().toLowerCase();
  const configured = String(config.metasoH3.channelValue || CHANNEL).trim().toLowerCase();
  return normalized === configured || normalized === 'metaso' || normalized === 'minimax h3 api' ||
    normalized === 'minimax-h3' || normalized === 'metaso h3';
}

function isH3Model(value) {
  const normalized = String(value || '').trim().toLowerCase().replace(/[-_]+/g, ' ');
  return normalized === 'minimax h3';
}

function getContext(record, config) {
  const fields = record.fields || {};
  const attachments = getAttachmentList(fields, config.fields.images || ['参考图']);
  const firstFrameAttachments = getAttachmentList(fields, [config.fields.firstFrameImage]);
  const lastFrameAttachments = getAttachmentList(fields, [config.fields.lastFrameImage]);
  return {
    recordId: record.record_id,
    taskName: sanitizeTaskName(fields[config.fields.taskName], record.record_id),
    prompt: normalizeTextField(fields[config.fields.prompt]),
    channel: normalizeTextField(fields[config.fields.channel]),
    status: normalizeTextField(fields[config.statusField || '状态']),
    platformTaskId: normalizeTextField(fields[config.fields.platformTaskId]),
    platformTaskStatus: normalizeTextField(fields[config.fields.platformTaskStatus]),
    traceId: normalizeTextField(fields[config.fields.latestTraceId]),
    referenceMode: normalizeTextField(fields[config.fields.referenceMode]),
    attachments,
    firstFrameAttachments,
    lastFrameAttachments,
    allowNoReferenceImage: normalizeBooleanField(fields[config.fields.allowNoReferenceImage]),
    repeatCount: Math.max(1, normalizeNumberField(fields[config.fields.repeatCount], 1)),
    submittedCount: Math.max(0, normalizeNumberField(fields[config.fields.submittedCount], 0)),
    model: normalizeTextField(fields[config.fields.model]),
    ratio: normalizeTextField(fields[config.fields.ratio]) || '9:16',
    duration: normalizeNumberField(fields[config.fields.duration], 5),
    resolution: normalizeTextField(fields[config.fields.resolution]) || '768P',
    executionOwner: normalizeTextField(fields[config.fields.executionOwner]),
    fields
  };
}

function resolveReferenceSelection(context, modeValue = '') {
  const references = context.attachments || [];
  const firstFrames = context.firstFrameAttachments || [];
  const lastFrames = context.lastFrameAttachments || [];
  const explicitMode = String(modeValue || context.referenceMode || '').trim();
  const inferredMode = explicitMode || (
    firstFrames.length && lastFrames.length ? '首尾帧' :
      firstFrames.length ? '首帧' :
        references.length ? '多参考图' : '文生视频'
  );
  const mode = normalizeMode(inferredMode, references.length + firstFrames.length + lastFrames.length);

  if (mode === 'text') {
    if (references.length || firstFrames.length || lastFrames.length) {
      throw new Error('文生视频模式不能携带参考图、首帧图片或尾帧图片');
    }
    return { mode: '文生视频', attachments: [] };
  }

  if (mode === 'first_frame') {
    if (lastFrames.length) throw new Error('首帧模式不能填写尾帧图片');
    if (firstFrames.length) {
      if (firstFrames.length !== 1) throw new Error('首帧图片字段必须且只能上传 1 张图');
      return { mode: '首帧', attachments: [firstFrames[0]], source: '首帧图片' };
    }
    if (references.length !== 1) throw new Error('首帧模式需要 1 张“首帧图片”，或在“参考图”中上传 1 张图');
    return { mode: '首帧', attachments: [references[0]], source: '参考图' };
  }

  if (mode === 'first_last') {
    if (firstFrames.length || lastFrames.length) {
      if (firstFrames.length !== 1 || lastFrames.length !== 1) {
        throw new Error('首尾帧模式使用专用字段时，“首帧图片”和“尾帧图片”都必须各有 1 张图');
      }
      return { mode: '首尾帧', attachments: [firstFrames[0], lastFrames[0]], source: '首帧图片+尾帧图片' };
    }
    if (references.length !== 2) throw new Error('首尾帧模式需要“首帧图片 + 尾帧图片”，或在“参考图”中按首帧、尾帧顺序上传 2 张图');
    return { mode: '首尾帧', attachments: [references[0], references[1]], source: '参考图' };
  }

  if (!references.length) throw new Error('多参考图模式至少需要 1 张“参考图”');
  if (references.length > 9) throw new Error('多参考图最多支持 9 张');
  return { mode: '多参考图', attachments: references, source: '参考图' };
}

function safeErrorText(error) {
  return String(error?.message || error || '未知错误')
    .replace(/Bearer\s+[A-Za-z0-9._-]+/gi, 'Bearer [REDACTED]')
    .replace(/https:\/\/\S+/g, '[URL已隐藏]')
    .slice(0, 1800);
}

function makeTraceId(context) {
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14);
  const tail = String(context.recordId || 'record').slice(-6);
  return `MH3_${stamp}_${tail}_${crypto.randomBytes(3).toString('hex')}`;
}

function buildFingerprint(context) {
  const canonical = JSON.stringify({
    prompt: context.prompt,
    mode: context.referenceMode,
    attachments: context.attachments.map(item => item.fileToken),
    ratio: context.ratio,
    duration: context.duration,
    resolution: context.resolution
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function stateConfig(config) {
  return { ...config, stateRoot: config.stateRoot };
}

function findTaskState(config, taskId) {
  return listSubmissionRecords(stateConfig(config)).find(item =>
    item.channel === 'minimax_h3' && String(item.platform_task_id || '') === String(taskId || '')
  ) || null;
}

function fieldMap(fields) {
  return new Map((fields || []).map(field => [field.field_name, field]));
}

async function mergeSelectOptions(config, token, map, fieldName, options) {
  const field = map.get(fieldName);
  if (!field || field.type !== 3) return [];
  const existing = field.property?.options || [];
  const names = new Set(existing.map(item => item.name));
  const missing = options.filter(name => !names.has(name));
  if (!missing.length) return [];
  await updateField(config, token, field.field_id, {
    field_name: field.field_name,
    type: field.type,
    property: { options: [...existing, ...missing.map(name => ({ name }))] }
  });
  return missing.map(name => `补齐 ${fieldName} 选项 ${name}`);
}

async function ensureSchema(config, token, dryRun = false) {
  let fields = await listTableFields(config, token);
  let map = fieldMap(fields);
  const changes = [];
  const required = [
    { name: config.fields.platformTaskId, type: 1 },
    { name: config.fields.platformTaskStatus, type: 3, property: { options: ['queued', 'running', 'succeeded', 'failed', 'cancelled'].map(name => ({ name })) } },
    { name: config.fields.submitFingerprint, type: 1 },
    { name: config.fields.referenceMode, type: 3, property: { options: ['文生视频', '首帧', '首尾帧', '多参考图'].map(name => ({ name })) } },
    { name: config.fields.lastFrameImage, type: 17 }
  ];
  for (const spec of required) {
    if (map.has(spec.name)) continue;
    changes.push(`新增字段 ${spec.name}`);
    if (!dryRun) await createField(config, token, { field_name: spec.name, type: spec.type, property: spec.property });
  }
  if (dryRun) return changes;

  fields = await listTableFields(config, token);
  map = fieldMap(fields);
  changes.push(...await mergeSelectOptions(config, token, map, config.statusField || '状态', ['待处理', '处理中', '已提交', '生成中', '已完成', '阻塞', '失败']));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.channel, [config.metasoH3.channelValue]));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.model, [MODEL_LABEL]));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.resolution, ['768P', '2K']));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.ratio, ['21:9', '16:9', '4:3', '1:1', '3:4', '9:16', 'adaptive']));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.referenceMode, ['文生视频', '首帧', '首尾帧', '多参考图']));
  changes.push(...await mergeSelectOptions(config, token, map, config.fields.platformTaskStatus, ['queued', 'running', 'succeeded', 'failed', 'cancelled']));
  return changes;
}

async function materializeAndUploadReferences(attachments, config, feishuToken, apiKey, tempDir) {
  const urls = [];
  for (let index = 0; index < attachments.length; index++) {
    const attachment = attachments[index];
    const filename = buildUniqueAttachmentName(attachment.fileName, index, 'reference.jpg');
    const localPath = path.join(tempDir, filename);
    await downloadFile(feishuToken, attachment.fileToken, localPath);
    const size = fs.statSync(localPath).size;
    if (size > 30 * 1024 * 1024) throw new Error(`参考图 ${filename} 超过 30 MB`);
    urls.push(await uploadFile({
      baseUrl: config.metasoH3.baseUrl,
      token: apiKey,
      filePath: localPath
    }));
  }
  return urls;
}

async function claimRecord(context, config, token, fingerprint) {
  if (context.executionOwner && !context.executionOwner.startsWith(`${config.machineId || '主机'}-H3#`)) {
    return null;
  }
  const owner = `${config.machineId || os.hostname()}-H3#${fingerprint.slice(0, 12)}`;
  await updateRecord(config, token, context.recordId, {
    [config.statusField || '状态']: '处理中',
    [config.fields.executionOwner]: owner,
    [config.fields.resultSyncStatus]: 'submitting',
    [config.fields.result]: 'MiniMax H3 正在准备参考素材',
    [config.fields.errorMessage]: ''
  });
  const current = await getRecord(config, token, context.recordId);
  if (!current || normalizeTextField(current.fields?.[config.fields.executionOwner]) !== owner) return null;
  if (normalizeTextField(current.fields?.[config.fields.platformTaskId])) return null;
  return owner;
}

async function failBeforeSubmission(context, config, token, error, status = '失败', syncStatus = 'failed') {
  const message = safeErrorText(error);
  await updateRecord(config, token, context.recordId, {
    [config.statusField || '状态']: status,
    [config.fields.resultSyncStatus]: syncStatus,
    [config.fields.result]: message,
    [config.fields.errorMessage]: message,
    [config.fields.platformTaskStatus]: status === '阻塞' ? '' : 'failed'
  });
}

async function submitRecord(context, config, feishuToken, apiKey) {
  if (context.repeatCount !== 1) {
    await failBeforeSubmission(context, config, feishuToken, new Error('MiniMax H3 第一版每行只允许生成 1 次，请把“生成次数”设为 1'), '阻塞', 'blocked');
    return { status: 'blocked' };
  }
  if (!context.prompt) {
    await failBeforeSubmission(context, config, feishuToken, new Error('提示词不能为空'), '阻塞', 'blocked');
    return { status: 'blocked' };
  }
  if (!isH3Model(context.model)) {
    await failBeforeSubmission(context, config, feishuToken, new Error(`METASO 渠道必须选择“${MODEL_LABEL}”模型`), '阻塞', 'blocked');
    return { status: 'blocked' };
  }
  let referenceSelection;
  try {
    referenceSelection = resolveReferenceSelection(context);
  } catch (error) {
    await failBeforeSubmission(context, config, feishuToken, error, '阻塞', 'blocked');
    return { status: 'blocked' };
  }

  const fingerprint = buildFingerprint({
    ...context,
    referenceMode: referenceSelection.mode,
    attachments: referenceSelection.attachments
  });
  const owner = await claimRecord(context, config, feishuToken, fingerprint);
  if (!owner) return { status: 'claim_lost' };

  const traceId = makeTraceId(context);
  const baseState = {
    trace_id: traceId,
    channel: 'minimax_h3',
    platform: 'metaso',
    record_id: context.recordId,
    task_name: context.taskName,
    submit_fingerprint: fingerprint,
    status: 'submitting',
    state_updated_at: new Date().toISOString()
  };
  writeSubmissionRecord(stateConfig(config), traceId, baseState);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'metaso-h3-input-'));
  try {
    const imageUrls = await materializeAndUploadReferences(referenceSelection.attachments, config, feishuToken, apiKey, tempDir);
    const { mode, payload } = buildGenerationPayload({
      prompt: context.prompt,
      imageUrls,
      mode: referenceSelection.mode,
      resolution: context.resolution,
      duration: context.duration,
      ratio: context.ratio,
      watermark: config.metasoH3.watermark
    });
    const taskId = await createTask({
      baseUrl: config.metasoH3.baseUrl,
      token: apiKey,
      payload,
      timeoutMs: config.metasoH3.requestTimeoutMs
    });
    updateSubmissionRecord(stateConfig(config), traceId, {
      status: 'submitted',
      platform_task_id: taskId,
      reference_mode: mode,
      reference_source: referenceSelection.source || '',
      state_updated_at: new Date().toISOString()
    });
    await updateRecord(config, feishuToken, context.recordId, {
      [config.statusField || '状态']: '已提交',
      [config.fields.executionOwner]: owner,
      [config.fields.submittedCount]: 1,
      [config.fields.latestTraceId]: traceId,
      [config.fields.platformTaskId]: taskId,
      [config.fields.platformTaskStatus]: 'queued',
      [config.fields.submitFingerprint]: fingerprint,
      [config.fields.resultSyncStatus]: 'rendering',
      [config.fields.result]: 'MiniMax H3 任务已提交',
      [config.fields.submitTime]: new Date().toISOString(),
      [config.fields.errorMessage]: ''
    });
    return { status: 'submitted', taskId, traceId };
  } catch (error) {
    if (error instanceof SubmissionUncertainError) {
      updateSubmissionRecord(stateConfig(config), traceId, {
        status: 'submit_uncertain',
        error_message: safeErrorText(error),
        state_updated_at: new Date().toISOString()
      });
      await failBeforeSubmission(context, config, feishuToken, error, '阻塞', 'submit_uncertain');
      return { status: 'submit_uncertain' };
    }
    updateSubmissionRecord(stateConfig(config), traceId, {
      status: 'failed',
      error_message: safeErrorText(error),
      state_updated_at: new Date().toISOString()
    });
    await failBeforeSubmission(context, config, feishuToken, error);
    return { status: 'failed' };
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

async function finishUploadedRecord(context, config, token, taskId, traceId, fileToken, filename) {
  await updateRecord(config, token, context.recordId, {
    [config.statusField || '状态']: '已完成',
    [config.fields.platformTaskStatus]: 'succeeded',
    [config.fields.resultSyncStatus]: 'uploaded',
    [config.fields.result]: `MiniMax H3 生成完成（task_id=${taskId}）`,
    [config.fields.videoAttachment]: [{ file_token: fileToken }],
    [config.fields.videoFileName]: filename,
    [config.fields.finishTime]: new Date().toISOString(),
    [config.fields.errorMessage]: '',
    [config.fields.latestTraceId]: traceId
  });
}

async function pollRecord(context, config, feishuToken, apiKey) {
  const taskId = context.platformTaskId;
  let state = findTaskState(config, taskId);
  const traceId = context.traceId || state?.trace_id || `MH3_RECOVER_${taskId}`;

  if (state?.status === 'uploaded' && state.uploaded_file_token) {
    await finishUploadedRecord(context, config, feishuToken, taskId, traceId, state.uploaded_file_token, state.video_file_name || `${context.taskName}.mp4`);
    return { status: 'uploaded_reconciled' };
  }

  let outputPath = state?.download_path || '';
  let taskStatus = '';
  let videoUrl = '';
  if (!(state?.status === 'downloaded' && outputPath && fs.existsSync(outputPath))) {
    const result = await queryTask({ baseUrl: config.metasoH3.baseUrl, token: apiKey, taskId });
    const task = result.task || {};
    taskStatus = String(task.status || '').toLowerCase();
    if (!taskStatus) return { status: 'not_synced_yet' };
    videoUrl = task.content?.url || '';

    if (API_TERMINAL_FAILURES.has(taskStatus)) {
      const message = safeErrorText(task.error?.message || taskStatus);
      updateSubmissionRecord(stateConfig(config), traceId, {
        ...(!state ? { trace_id: traceId, channel: 'minimax_h3', record_id: context.recordId, platform_task_id: taskId } : {}),
        status: 'failed', error_message: message, state_updated_at: new Date().toISOString()
      });
      await updateRecord(config, feishuToken, context.recordId, {
        [config.statusField || '状态']: '失败',
        [config.fields.platformTaskStatus]: taskStatus === 'cancelled' ? 'cancelled' : 'failed',
        [config.fields.resultSyncStatus]: 'failed',
        [config.fields.result]: message,
        [config.fields.errorMessage]: message,
        [config.fields.finishTime]: new Date().toISOString()
      });
      return { status: taskStatus };
    }
    if (!API_SUCCESS.has(taskStatus)) {
      if (context.platformTaskStatus !== taskStatus || context.status !== '生成中') {
        await updateRecord(config, feishuToken, context.recordId, {
          [config.statusField || '状态']: '生成中',
          [config.fields.platformTaskStatus]: taskStatus,
          [config.fields.resultSyncStatus]: 'rendering',
          [config.fields.result]: `MiniMax H3 状态：${taskStatus}`
        });
      }
      updateSubmissionRecord(stateConfig(config), traceId, {
        ...(!state ? { trace_id: traceId, channel: 'minimax_h3', record_id: context.recordId, platform_task_id: taskId } : {}),
        status: taskStatus, state_updated_at: new Date().toISOString()
      });
      return { status: taskStatus };
    }
    if (!videoUrl) throw new Error('MiniMax H3 任务成功但没有返回成片地址');

    await fs.promises.mkdir(config.h3DownloadRoot, { recursive: true });
    outputPath = path.join(config.h3DownloadRoot, `${traceId}.mp4`);
    await downloadPublicVideo(videoUrl, outputPath, { maxBytes: config.metasoH3.maxDownloadBytes });
    state = updateSubmissionRecord(stateConfig(config), traceId, {
      ...(!state ? { trace_id: traceId, channel: 'minimax_h3', record_id: context.recordId, platform_task_id: taskId } : {}),
      status: 'downloaded',
      download_path: outputPath,
      state_updated_at: new Date().toISOString()
    });
    await updateRecord(config, feishuToken, context.recordId, {
      [config.fields.platformTaskStatus]: 'succeeded',
      [config.fields.resultSyncStatus]: 'downloaded',
      [config.fields.result]: 'MiniMax H3 成片已下载，正在上传飞书'
    });
  }

  const uploaded = await uploadBitableFile(config, feishuToken, outputPath);
  const filename = `${context.taskName || traceId}.mp4`.replace(/[\\/:*?"<>|]/g, '_');
  updateSubmissionRecord(stateConfig(config), traceId, {
    status: 'uploaded',
    uploaded_file_token: uploaded.fileToken,
    video_file_name: filename,
    state_updated_at: new Date().toISOString()
  });
  await finishUploadedRecord(context, config, feishuToken, taskId, traceId, uploaded.fileToken, filename);
  fs.rmSync(outputPath, { force: true });
  return { status: 'uploaded' };
}

async function reconcileLocalStates(records, config, token) {
  const byId = new Map(records.map(record => [record.record_id, record]));
  let changed = 0;
  for (const state of listSubmissionRecords(stateConfig(config))) {
    if (state.channel !== 'minimax_h3' || !state.record_id || !state.platform_task_id) continue;
    const record = byId.get(state.record_id);
    if (!record) continue;
    const context = getContext(record, config);
    if (!isH3Channel(context.channel, config)) continue;
    if (state.status === 'uploaded' && state.uploaded_file_token && context.status !== '已完成') {
      await finishUploadedRecord(context, config, token, state.platform_task_id, state.trace_id, state.uploaded_file_token, state.video_file_name || `${context.taskName}.mp4`);
      changed++;
    } else if (!context.platformTaskId && ['submitted', 'queued', 'running', 'downloaded'].includes(state.status)) {
      await updateRecord(config, token, context.recordId, {
        [config.statusField || '状态']: state.status === 'downloaded' ? '生成中' : '已提交',
        [config.fields.platformTaskId]: state.platform_task_id,
        [config.fields.latestTraceId]: state.trace_id,
        [config.fields.resultSyncStatus]: state.status === 'downloaded' ? 'downloaded' : 'rendering'
      });
      changed++;
    }
  }
  return changed;
}

async function run(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const config = loadConfig(args.configPath);
  setActiveConfigForNetwork(config);
  if (!config.appId || !config.appSecret || !config.appToken || !config.tableId) {
    throw new Error('飞书配置不完整');
  }
  const feishuToken = await getAccessToken(config);

  if (args.ensureSchemaOnly) {
    const changes = await ensureSchema(config, feishuToken, args.dryRun);
    console.log(changes.length ? changes.join('\n') : 'MiniMax H3 飞书字段已就绪');
    return { schemaChanges: changes };
  }

  let records = await listAllRecords(config, feishuToken);
  if (!args.dryRun) {
    const reconciled = await reconcileLocalStates(records, config, feishuToken);
    if (reconciled) records = await listAllRecords(config, feishuToken);
  }
  let contexts = records.map(record => getContext(record, config))
    .filter(context => isH3Channel(context.channel, config))
    .filter(context => !args.recordId || context.recordId === args.recordId);

  const active = contexts.filter(context => context.platformTaskId && ACTIVE_STATUSES.has(context.status));
  const pending = contexts.filter(context => !context.platformTaskId && PENDING_STATUSES.has(context.status));
  console.log(`MiniMax H3：待提交 ${pending.length} 条，待轮询/回传 ${active.length} 条`);
  if (args.dryRun) {
    for (const context of [...active, ...pending].slice(0, args.limit || 20)) {
      console.log(`- ${context.taskName} | ${context.status} | ${context.model || '未选模型'} | ${context.referenceMode || '自动模式'} | 参考=${context.attachments.length} 首帧=${context.firstFrameAttachments.length} 尾帧=${context.lastFrameAttachments.length} | ${context.duration}s ${context.resolution} ${context.ratio}`);
    }
    return { pending: pending.length, active: active.length, dryRun: true };
  }
  if (!config.metasoH3.enabled) throw new Error('MiniMax H3 worker 尚未启用');
  const apiKey = requireApiKey(config.metasoH3.apiKeyEnv);

  const result = { polled: [], submitted: [] };
  if (!args.submitOnly) {
    for (const context of active) {
      try {
        result.polled.push({ recordId: context.recordId, ...(await pollRecord(context, config, feishuToken, apiKey)) });
      } catch (error) {
        console.log(`轮询/回传失败 ${context.taskName}: ${safeErrorText(error)}`);
        result.polled.push({ recordId: context.recordId, status: 'retry_later', error: safeErrorText(error) });
      }
    }
  }
  if (!args.pollOnly) {
    const limit = args.limit || config.metasoH3.maxSubmitsPerRun;
    for (const context of pending.slice(0, limit)) {
      result.submitted.push({ recordId: context.recordId, ...(await submitRecord(context, config, feishuToken, apiKey)) });
    }
  }
  console.log(JSON.stringify(result, null, 2));
  return result;
}

if (require.main === module) {
  run().catch(error => {
    console.error(`MiniMax H3 worker 失败: ${safeErrorText(error)}`);
    process.exitCode = 1;
  });
}

module.exports = {
  ACTIVE_STATUSES,
  CHANNEL,
  DEFAULT_FIELDS,
  MODEL_LABEL,
  PENDING_STATUSES,
  buildFingerprint,
  ensureSchema,
  getContext,
  isH3Channel,
  isH3Model,
  loadConfig,
  parseArgs,
  pollRecord,
  resolveReferenceSelection,
  run,
  safeErrorText,
  submitRecord
};
