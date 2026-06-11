#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');

const {
  loadOpenclawFeishuCredentials,
  getAccessToken,
  listAllRecords,
  requestJson,
  updateRecord,
  uploadFileToFeishu,
  setActiveConfigForNetwork
} = require('../lib/feishu-client');
const {
  normalizeTextField,
  normalizeNumberField,
  normalizeBooleanField,
  sanitizeTaskName
} = require('../lib/field-normalizers');
const { parseExecutionOwner } = require('../lib/task-context');
const { parseContentIdMetadata, parseScriptIdMetadata } = require('../lib/asset-match');
const { runFirstFramePipeline } = require('../platforms/imini/submitter');
const { formatProductLockCard } = require('../platforms/imini/product-lock');

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, os.homedir());
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

async function resolveConfigTable(config, token) {
  if (!config.tableUrl) return config;
  const info = parseFeishuBitableUrl(config.tableUrl);
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

function loadConfig() {
  const configPath = path.join(__dirname, '..', 'segment-package.json');
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
  setActiveConfigForNetwork(config);
  return config;
}

function text(value) {
  return normalizeTextField(value);
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

function buildContext(record, config) {
  const fields = record.fields || {};
  const promptPackageId = text(fields[config.fields.taskName]) || record.record_id;
  const executionOwner = text(fields[config.fields.executionOwner]);
  const executionOwnerParsed = parseExecutionOwner(executionOwner);
  const repeatTarget = Math.max(1, normalizeNumberField(fields[config.fields.repeatCount], 1));
  const submittedCount = Math.max(0, normalizeNumberField(fields[config.fields.submittedCount], 0));
  return {
    recordId: record.record_id,
    record,
    taskName: sanitizeTaskName(promptPackageId, record.record_id),
    promptPackageId,
    productId: text(fields[config.fields.productId]),
    productName: text(fields[config.fields.productName]),
    skuId: text(fields[config.fields.skuId]) || 'DEFAULT',
    market: text(fields[config.fields.market]),
    category: text(fields[config.fields.category]),
    segmentType: text(fields[config.fields.segmentType]),
    grade: text(fields[config.fields.grade]),
    prompt: ensurePromptMetadata(text(fields[config.fields.prompt]), promptPackageId),
    referenceImagePackId: text(fields[config.fields.referenceImagePackId]),
    referenceImageVersion: normalizeNumberField(fields[config.fields.referenceImageVersion], 0),
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

function resolveReferenceImages(config, context, runId) {
  const outputDir = path.join(config.runtimeRoot, '_tmp', runId, context.recordId, 'reference-images');
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
  const stdout = execFileSync(config.autoMixcutPython || 'python3', args, {
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

async function main() {
  const config = loadConfig();
  const argv = process.argv.slice(2);
  const dryRun = argv.includes('--dry-run');
  const all = argv.includes('--all') || argv.includes('--current-view');
  const force = argv.includes('--force') || argv.includes('--force-regenerate');
  const limitArg = argv.find(item => item.startsWith('--limit='));
  const limit = limitArg ? Math.max(1, Number(limitArg.slice('--limit='.length)) || 1) : 0;
  const ids = argv.filter(item => item && !item.startsWith('--'));
  if (ids.length === 0 && !all) {
    throw new Error('请传入 record_id / 提示词包ID，或使用 --all 处理当前视图全部 imini 记录');
  }
  const token = await getAccessToken(config);
  await resolveConfigTable(config, token);
  setActiveConfigForNetwork(config);

  const records = await listAllRecords(config, token);
  const runId = `regen-first-frame-${Date.now()}`;
  const results = [];
  const selectedRecords = all
    ? records.filter(record => {
      const context = buildContext(record, config);
      if (!context.prompt || !context.productId || !context.market) return false;
      return String(context.channel || '').trim().toLowerCase() === 'imini';
    })
    : ids.map(id => records.find(item => {
      if (item.record_id === id) return true;
      const fields = item.fields || {};
      return text(fields[config.fields.taskName]) === id;
    }) || { _missingId: id });
  const targetRecords = limit > 0 ? selectedRecords.slice(0, limit) : selectedRecords;

  if (dryRun) {
    console.log(`匹配到 ${selectedRecords.length} 条，计划处理 ${targetRecords.length} 条`);
    for (const record of targetRecords) {
      if (record._missingId) {
        console.log(`- ${record._missingId}: 未找到记录`);
        continue;
      }
      const context = buildContext(record, config);
      console.log(`- ${context.recordId} / ${context.taskName} / channel=${context.channel} / status=${context.currentStatus} / canSubmit=${context.canSubmit} / submitted=${context.submittedCount}/${context.repeatTarget} / remaining=${context.remainingCount} / product=${context.productId} / market=${context.market}`);
    }
    return;
  }

  for (const record of targetRecords) {
    if (record._missingId) {
      results.push({ id: record._missingId, success: false, error: '未找到记录' });
      continue;
    }

    const context = buildContext(record, config);
    console.log(`\n🎨 重新生成首帧: ${context.recordId} / ${context.taskName}`);
    if (force) {
      const firstFramePath = path.join(config.runtimeRoot, context.taskName, 'first-frame.png');
      try {
        fs.rmSync(firstFramePath, { force: true });
      } catch (error) {
        console.log(`  ⚠️ 删除旧首帧失败，继续尝试重生: ${error.message}`);
      }
      config.channels = config.channels || {};
      config.channels.imini = config.channels.imini || {};
      config.channels.imini.firstFrame = {
        ...(config.channels.imini.firstFrame || {}),
        reuseExistingFirstFrame: false
      };
    }
    const referenceResult = resolveReferenceImages(config, context, runId);
    if (referenceResult.imagePaths.length === 0) {
      throw new Error(`${context.recordId} 参考图包没有可下载图片`);
    }
    context.taskFolderImages = referenceResult.imagePaths;
    context.referenceImagePack = referenceResult.pack || null;
    context.referenceImages = referenceResult.images || [];

    try {
      const pipelineResult = await runFirstFramePipeline(context, config, referenceResult.imagePaths);
      if (!pipelineResult.success || !pipelineResult.firstFrameImagePath) {
        await updateRecord(config, token, context.recordId, {
          [config.fields.firstFrameStatus || '首帧状态']: '失败',
          [config.fields.errorMessage || '失败原因']: pipelineResult.error || '首帧生成失败'
        });
        results.push({ id, recordId: context.recordId, success: false, error: pipelineResult.error || '首帧生成失败' });
        continue;
      }

      const upload = await uploadFileToFeishu(config, token, pipelineResult.firstFrameImagePath);
      await updateRecord(config, token, context.recordId, {
        [config.fields.firstFramePrompt || '首帧提示词']: String(pipelineResult.firstFramePrompt || '').slice(0, 5000),
        [config.fields.productConsistencyDescription || config.fields.productLock || '商品一致性描述']: formatProductLockCard(pipelineResult.lock || {}).slice(0, 5000),
        [config.fields.firstFrameStatus || '首帧状态']: '已生成',
        [config.fields.firstFrameImage || '首帧参考图']: [
          {
            file_token: upload.fileToken
          }
        ]
      });

      console.log(`  ✅ 已上传首帧参考图: ${upload.fileToken}`);
      results.push({
        id: context.recordId,
        recordId: context.recordId,
        taskName: context.taskName,
        success: true,
        imagePath: pipelineResult.firstFrameImagePath,
        fileToken: upload.fileToken
      });
    } finally {
      try {
        fs.rmSync(referenceResult.outputDir, { recursive: true, force: true });
      } catch (error) {
        console.log(`  ⚠️ 清理 OSS 临时参考图失败: ${error.message}`);
      }
    }
  }

  console.log('\n结果:');
  console.log(JSON.stringify(results, null, 2));
}

main().catch(error => {
  console.error('ERROR:', error.message);
  process.exit(1);
});
