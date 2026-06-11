#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const https = require('https');

const DEFAULT_CONFIG = {
  appId: '',
  appSecret: '',
  appToken: '',
  tableId: '',
  viewId: '',
  outputDir: '~/Desktop/jimeng',
  pageSize: 100,
  skipExisting: true,
  pendingOnly: true,
  statusField: '状态',
  pendingValues: ['待处理', '待开始', '未开始'],
  fields: {
    taskName: 'SKU编码',
    prompt: '整合提示词',
    images: ['产品主图'],
    model: '视频生成模型',
    mode: '参考模式',
    ratio: '视频比例',
    duration: '视频时长'
  }
};

let cachedAccessToken = null;
let tokenExpireTime = 0;

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, process.env.HOME || '~');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs(argv) {
  const args = {
    configPath: path.join(__dirname, 'feishu-source.json'),
    outputDir: null,
    force: false,
    limit: null,
    recordId: null
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--config' && argv[i + 1]) {
      args.configPath = argv[++i];
    } else if (arg === '--output' && argv[i + 1]) {
      args.outputDir = argv[++i];
    } else if (arg === '--force') {
      args.force = true;
    } else if (arg === '--limit' && argv[i + 1]) {
      args.limit = Number(argv[++i]) || null;
    } else if (arg === '--record-id' && argv[i + 1]) {
      args.recordId = argv[++i];
    }
  }

  return args;
}

function loadConfig(configPath, cliArgs) {
  if (!fs.existsSync(configPath)) {
    throw new Error(`配置文件不存在: ${configPath}`);
  }

  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const config = {
    ...DEFAULT_CONFIG,
    ...raw,
    fields: {
      ...DEFAULT_CONFIG.fields,
      ...(raw.fields || {})
    }
  };

  if (cliArgs.outputDir) {
    config.outputDir = cliArgs.outputDir;
  }

  config.outputDir = expandHome(config.outputDir);
  return config;
}

function requestJson(method, requestPath, token, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json'
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    if (payload) {
      headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = https.request({
      hostname: 'open.feishu.cn',
      path: requestPath,
      method,
      headers
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.code === 0) {
            resolve(json.data);
            return;
          }
          reject(new Error(`飞书 API 失败 (${json.code}): ${json.msg || '未知错误'}`));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.on('error', reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

async function getAccessToken(config) {
  if (cachedAccessToken && Date.now() < tokenExpireTime) {
    return cachedAccessToken;
  }

  const data = await requestJson(
    'POST',
    '/open-apis/auth/v3/tenant_access_token/internal',
    null,
    {
      app_id: config.appId,
      app_secret: config.appSecret
    }
  );

  cachedAccessToken = data.tenant_access_token;
  tokenExpireTime = Date.now() + ((data.expire || 7200) - 60) * 1000;
  return cachedAccessToken;
}

async function listAllRecords(config, token) {
  let pageToken = null;
  let records = [];

  do {
    let requestPath = `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records?page_size=${config.pageSize || 100}`;
    if (pageToken) {
      requestPath += `&page_token=${encodeURIComponent(pageToken)}`;
    }
    if (config.viewId) {
      requestPath += `&view_id=${encodeURIComponent(config.viewId)}`;
    }

    const data = await requestJson('GET', requestPath, token);
    records = records.concat(data.items || []);
    pageToken = data.page_token || null;
  } while (pageToken);

  return records;
}

function normalizeTextField(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number') return String(value);
  if (Array.isArray(value)) {
    return value
      .map(item => {
        if (typeof item === 'string') return item;
        if (item == null) return '';
        if (typeof item.text === 'string') return item.text;
        if (typeof item.name === 'string') return item.name;
        return '';
      })
      .filter(Boolean)
      .join('\n')
      .trim();
  }
  return String(value).trim();
}

function normalizeDuration(value) {
  const text = normalizeTextField(value);
  const match = text.match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function getAttachmentList(fields, imageFieldNames) {
  const attachments = [];

  for (const fieldName of imageFieldNames) {
    const value = fields[fieldName];
    if (!Array.isArray(value)) continue;
    for (const item of value) {
      if (item && item.file_token) {
        attachments.push({
          fieldName,
          fileToken: item.file_token,
          fileName: item.name || `${fieldName}.bin`,
          raw: item
        });
      }
    }
  }

  return attachments;
}

function sanitizeFileName(fileName, fallback = 'image.jpg') {
  const base = path.basename(fileName || fallback);
  const cleaned = base.replace(/[\\/:*?"<>|]/g, '_').trim();
  return cleaned || fallback;
}

function sanitizeTaskName(value, fallback) {
  const cleaned = String(value || fallback || 'task')
    .replace(/[\\/:*?"<>|]/g, '_')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || fallback || 'task';
}

function pickTaskName(record, config) {
  const fields = record.fields || {};
  const nameField = config.fields.taskName;
  const candidate = normalizeTextField(fields[nameField]);
  return sanitizeTaskName(candidate, record.record_id);
}

function shouldProcessRecord(record, config, cliArgs) {
  const fields = record.fields || {};
  if (cliArgs.recordId && record.record_id !== cliArgs.recordId) {
    return false;
  }

  if (!config.pendingOnly) {
    return true;
  }

  const status = normalizeTextField(fields[config.statusField]);
  if (!status) {
    return true;
  }

  return (config.pendingValues || []).includes(status);
}

function buildTaskConfig(fields, config) {
  const taskConfig = {};

  const model = normalizeTextField(fields[config.fields.model]);
  const mode = normalizeTextField(fields[config.fields.mode]);
  const ratio = normalizeTextField(fields[config.fields.ratio]);
  const duration = normalizeDuration(fields[config.fields.duration]);

  if (model) taskConfig.model = model;
  if (mode) taskConfig.mode = mode;
  if (ratio) taskConfig.ratio = ratio;
  if (duration) taskConfig.duration = duration;

  return taskConfig;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

async function downloadFile(token, fileToken, outputPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outputPath);
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: `/open-apis/drive/v1/medias/${fileToken}/download`,
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`
      }
    }, res => {
      if (res.statusCode !== 200) {
        let errorData = '';
        res.setEncoding('utf8');
        res.on('data', chunk => errorData += chunk);
        res.on('end', () => {
          fs.unlink(outputPath, () => {
            reject(new Error(`下载失败 (${res.statusCode}): ${errorData || '空响应'}`));
          });
        });
        return;
      }

      res.pipe(file);
      file.on('finish', () => {
        file.close(() => resolve(outputPath));
      });
    });

    req.on('error', error => {
      file.close(() => {
        fs.unlink(outputPath, () => reject(error));
      });
    });

    req.end();
  });
}

async function materializeRecord(record, config, token, cliArgs) {
  const fields = record.fields || {};
  const taskName = pickTaskName(record, config);
  const taskDir = path.join(config.outputDir, taskName);
  const prompt = normalizeTextField(fields[config.fields.prompt]);
  const attachments = getAttachmentList(fields, config.fields.images || []);
  const taskConfig = buildTaskConfig(fields, config);

  if (!prompt) {
    console.log(`⏭️  跳过 ${taskName}: 提示词字段为空`);
    return { status: 'skipped', reason: 'missing_prompt', taskName };
  }

  if (attachments.length === 0) {
    console.log(`⏭️  跳过 ${taskName}: 未找到参考图附件`);
    return { status: 'skipped', reason: 'missing_images', taskName };
  }

  const hasExistingTask = fs.existsSync(taskDir);
  const hasProtectedMarker =
    fs.existsSync(path.join(taskDir, '.submitted')) ||
    fs.existsSync(path.join(taskDir, '.completed')) ||
    fs.existsSync(path.join(taskDir, '.blocked'));

  if (hasExistingTask && config.skipExisting && !cliArgs.force) {
    console.log(`⏭️  跳过 ${taskName}: 本地任务目录已存在`);
    return { status: 'skipped', reason: 'task_exists', taskName };
  }

  if (hasProtectedMarker && !cliArgs.force) {
    console.log(`⏭️  跳过 ${taskName}: 本地已有状态标记`);
    return { status: 'skipped', reason: 'protected_marker', taskName };
  }

  ensureDir(taskDir);
  const imageDir = path.join(taskDir, '图片');
  ensureDir(imageDir);

  fs.writeFileSync(path.join(taskDir, 'prompt.txt'), `${prompt}\n`);
  if (Object.keys(taskConfig).length > 0) {
    fs.writeFileSync(path.join(taskDir, 'config.json'), `${JSON.stringify(taskConfig, null, 2)}\n`);
  }

  const meta = {
    syncedAt: new Date().toISOString(),
    recordId: record.record_id,
    source: {
      appToken: config.appToken,
      tableId: config.tableId,
      viewId: config.viewId || null
    },
    taskName,
    promptField: config.fields.prompt,
    imageFields: config.fields.images,
    taskConfig
  };
  fs.writeFileSync(path.join(taskDir, '.feishu.json'), `${JSON.stringify(meta, null, 2)}\n`);

  let downloaded = 0;
  for (let i = 0; i < attachments.length; i++) {
    const attachment = attachments[i];
    const outputName = sanitizeFileName(attachment.fileName, `image-${i + 1}.jpg`);
    const outputPath = path.join(imageDir, outputName);
    await downloadFile(token, attachment.fileToken, outputPath);
    downloaded++;
    await sleep(200);
  }

  console.log(`✅ 已同步 ${taskName}: ${downloaded} 张图, 提示词 ${prompt.length} 字符`);
  return {
    status: 'created',
    taskName,
    taskDir,
    downloaded
  };
}

async function main() {
  const cliArgs = parseArgs(process.argv.slice(2));
  const config = loadConfig(cliArgs.configPath, cliArgs);

  if (!config.appId || !config.appSecret || !config.appToken || !config.tableId) {
    throw new Error('飞书配置不完整：需要 appId、appSecret、appToken、tableId');
  }

  ensureDir(config.outputDir);

  console.log('📥 飞书表格 -> 即梦任务目录');
  console.log('================================');
  console.log(`配置文件: ${cliArgs.configPath}`);
  console.log(`输出目录: ${config.outputDir}`);
  console.log(`表格: ${config.appToken}/${config.tableId}`);
  if (cliArgs.recordId) {
    console.log(`仅同步记录: ${cliArgs.recordId}`);
  }

  const token = await getAccessToken(config);
  const allRecords = await listAllRecords(config, token);
  const targetRecords = allRecords.filter(record => shouldProcessRecord(record, config, cliArgs));
  const records = cliArgs.limit ? targetRecords.slice(0, cliArgs.limit) : targetRecords;

  console.log(`飞书记录总数: ${allRecords.length}`);
  console.log(`待同步记录数: ${records.length}`);

  let createdCount = 0;
  let skippedCount = 0;

  for (const record of records) {
    const result = await materializeRecord(record, config, token, cliArgs);
    if (result.status === 'created') {
      createdCount++;
    } else {
      skippedCount++;
    }
  }

  console.log('\n================================');
  console.log('📊 同步完成');
  console.log('================================');
  console.log(`✅ 新建任务: ${createdCount}`);
  console.log(`⏭️  跳过记录: ${skippedCount}`);
}

main().catch(error => {
  console.error(`❌ 同步失败: ${error.message}`);
  process.exit(1);
});
