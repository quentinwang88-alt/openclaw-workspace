#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const { getAccessToken, listTableFields, createField } = require('./lib/feishu-client');

function loadConfig(configPath) {
  if (!configPath) {
    configPath = path.join(__dirname, 'feishu-direct.json');
  }
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const openclawConfigPath = path.join(process.env.HOME || '', '.openclaw', 'openclaw.json');
  let feishuCredentials = {};
  try {
    if (fs.existsSync(openclawConfigPath)) {
      const openclawConfig = JSON.parse(fs.readFileSync(openclawConfigPath, 'utf8'));
      feishuCredentials = openclawConfig.channels?.feishu || {};
    }
  } catch (error) {
    // ignore
  }

  return {
    ...raw,
    appId: raw.appId || feishuCredentials.appId || '',
    appSecret: raw.appSecret || feishuCredentials.appSecret || '',
    fields: {
      taskName: '任务名',
      executionOwner: '执行归属',
      prompt: '提示词',
      images: ['参考图'],
      allowNoReferenceImage: '免参考图',
      repeatCount: '生成次数',
      model: '模型',
      ratio: '视频比例',
      duration: '视频时长',
      submittedCount: '已提交次数',
      result: '结果说明',
      lastProcessedAt: '最后处理时间',
      blockedPath: '阻塞截图路径',
      latestTraceId: '最新追踪ID',
      resultSyncStatus: '结果回传状态',
      videoAttachment: '生成视频',
      videoFileName: '生成视频文件名',
      submitTime: '提交时间',
      finishTime: '完成时间',
      errorMessage: '错误信息',
      ...raw.fields
    }
  };
}

const FIELD_TYPE = {
  TEXT: 1,
  NUMBER: 2,
  SINGLE_SELECT: 3,
  ATTACHMENT: 17
};

const IMINI_FIELDS = [
  { field_name: '渠道', type: FIELD_TYPE.SINGLE_SELECT, property: { options: [{ name: '即梦' }, { name: 'imini' }] } },
  { field_name: '渠道来源', type: FIELD_TYPE.TEXT },
  { field_name: '首帧提示词', type: FIELD_TYPE.TEXT },
  { field_name: '首帧图片', type: FIELD_TYPE.ATTACHMENT },
  { field_name: '首帧状态', type: FIELD_TYPE.SINGLE_SELECT, property: { options: [{ name: '未生成' }, { name: '生成中' }, { name: '已生成' }, { name: '失败' }] } },
  { field_name: '商品一致性描述', type: FIELD_TYPE.TEXT },
  { field_name: '首帧一致性状态', type: FIELD_TYPE.SINGLE_SELECT, property: { options: [{ name: '未检查' }, { name: '通过' }, { name: '失败' }] } },
  { field_name: '首帧一致性评分', type: FIELD_TYPE.NUMBER },
  { field_name: '平台任务ID', type: FIELD_TYPE.TEXT }
];

async function main() {
  const configPathCandidate = process.argv.slice(2).find(a => !a.startsWith('--') && a.endsWith('.json'));
  const configPath = configPathCandidate || path.join(__dirname, 'feishu-direct.json');
  const dryRun = process.argv.includes('--dry-run');

  const config = loadConfig(configPath);

  if (!config.appId || !config.appSecret || !config.appToken || !config.tableId) {
    console.error('飞书配置不完整：需要 appId、appSecret、appToken、tableId');
    process.exit(1);
  }

  const { setActiveConfigForNetwork } = require('./lib/feishu-client');
  setActiveConfigForNetwork(config);

  const token = await getAccessToken(config);
  let fields = await listTableFields(config, token);
  const fieldMap = new Map(fields.map(f => [f.field_name, f]));

  let created = 0;
  let skipped = 0;

  for (const spec of IMINI_FIELDS) {
    if (fieldMap.has(spec.field_name)) {
      console.log(`  ⏭️ 字段已存在: ${spec.field_name}`);
      skipped++;
      continue;
    }

    if (dryRun) {
      console.log(`  🧪 将新增字段: ${spec.field_name} (类型=${spec.type})`);
      created++;
      continue;
    }

    try {
      await createField(config, token, spec);
      console.log(`  ✅ 新增字段: ${spec.field_name}`);
      created++;
    } catch (error) {
      if (/FieldNameDuplicated|1254014/.test(String(error.message || ''))) {
        console.log(`  ⏭️ 字段已存在（并发创建）: ${spec.field_name}`);
        skipped++;
      } else {
        console.error(`  ❌ 创建字段失败: ${spec.field_name} - ${error.message}`);
      }
    }
  }

  console.log(`\n📊 imini 字段迁移完成: 新增=${created}, 跳过=${skipped}`);
}

main().catch(error => {
  console.error(`❌ imini 字段迁移失败: ${error.message}`);
  process.exit(1);
});