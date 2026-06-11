#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const puppeteer = require('puppeteer-core');

function expandHome(value) {
  return String(value || '').replace(/^~(?=$|\/)/, process.env.HOME || '~');
}

function loadOpenclawFeishuCredentials() {
  try {
    const openclawConfigPath = path.join(process.env.HOME || '', '.openclaw', 'openclaw.json');
    if (!fs.existsSync(openclawConfigPath)) {
      return {};
    }
    const raw = JSON.parse(fs.readFileSync(openclawConfigPath, 'utf8'));
    return raw.channels?.feishu || {};
  } catch (error) {
    return {};
  }
}

function loadConfig(configPath) {
  let raw = {};
  if (fs.existsSync(configPath)) {
    raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  }

  const feishuCredentials = loadOpenclawFeishuCredentials();
  return {
    cdpHost: raw.cdpHost || '127.0.0.1',
    cdpPort: raw.cdpPort || 9222,
    baseUrl: raw.baseUrl || 'https://jimeng.jianying.com/ai-tool/home?workspace=0&type=video',
    appId: raw.appId || feishuCredentials.appId || '',
    appSecret: raw.appSecret || feishuCredentials.appSecret || '',
    preflight: {
      timeoutMs: raw.preflight?.timeoutMs || 30000,
      notifyOnSuccess: raw.preflight?.notifyOnSuccess !== false,
      notifyOnFailure: raw.preflight?.notifyOnFailure !== false,
      webhookUrl: raw.preflight?.webhookUrl || feishuCredentials.webhookUrl || '',
      receiveIdType: raw.preflight?.receiveIdType || feishuCredentials.receiveIdType || '',
      receiveId: raw.preflight?.receiveId || feishuCredentials.receiveId || ''
    }
  };
}

function requestJson(method, hostname, requestPath, headers = {}, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = https.request({
      hostname,
      path: requestPath,
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
        ...headers
      }
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data || '{}');
          resolve({ statusCode: res.statusCode || 0, json });
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function fetchVersion(config) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: config.cdpHost,
      port: config.cdpPort,
      path: '/json/version',
      method: 'GET'
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data || '{}'));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getTenantAccessToken(config) {
  if (!config.appId || !config.appSecret) {
    throw new Error('缺少飞书 appId/appSecret，无法发送 IM 通知');
  }

  const response = await requestJson(
    'POST',
    'open.feishu.cn',
    '/open-apis/auth/v3/tenant_access_token/internal',
    {},
    {
      app_id: config.appId,
      app_secret: config.appSecret
    }
  );

  if (response.json.code !== 0 || !response.json.tenant_access_token) {
    throw new Error(`获取飞书 tenant_access_token 失败: ${response.json.msg || response.statusCode}`);
  }

  return response.json.tenant_access_token;
}

async function sendWebhookNotification(webhookUrl, text) {
  const url = new URL(webhookUrl);
  return await new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      msg_type: 'text',
      content: {
        text
      }
    });

    const req = https.request({
      hostname: url.hostname,
      path: `${url.pathname}${url.search}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if ((res.statusCode || 500) >= 200 && (res.statusCode || 500) < 300) {
          resolve(data);
          return;
        }
        reject(new Error(`Webhook 通知失败: HTTP ${res.statusCode}`));
      });
    });

    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function sendImNotification(config, text) {
  const token = await getTenantAccessToken(config);
  const receiveIdType = config.preflight.receiveIdType || 'open_id';
  const receiveId = config.preflight.receiveId;
  if (!receiveId) {
    throw new Error('缺少飞书 receiveId，无法发送 IM 通知');
  }

  const response = await requestJson(
    'POST',
    'open.feishu.cn',
    `/open-apis/im/v1/messages?receive_id_type=${encodeURIComponent(receiveIdType)}`,
    {
      Authorization: `Bearer ${token}`
    },
    {
      receive_id: receiveId,
      msg_type: 'text',
      content: JSON.stringify({ text })
    }
  );

  if (response.json.code !== 0) {
    throw new Error(`发送飞书 IM 通知失败: ${response.json.msg || response.statusCode}`);
  }
}

async function sendNotification(config, summary, passed) {
  const shouldNotify = passed ? config.preflight.notifyOnSuccess : config.preflight.notifyOnFailure;
  if (!shouldNotify) {
    return { sent: false, skipped: 'notification-disabled' };
  }

  if (config.preflight.webhookUrl) {
    await sendWebhookNotification(config.preflight.webhookUrl, summary);
    return { sent: true, via: 'webhook' };
  }

  if (config.preflight.receiveId) {
    await sendImNotification(config, summary);
    return { sent: true, via: 'im' };
  }

  return { sent: false, skipped: 'no-target-configured' };
}

async function inspectJimengPage(page, baseUrl) {
  try {
    await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  } catch (error) {
    const message = error?.message || String(error);
    if (!message.includes('Navigation timeout')) {
      throw error;
    }
  }
  await page.setViewport({ width: 1728, height: 1117 });
  await new Promise(resolve => setTimeout(resolve, 1500));

  return await page.evaluate(() => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
    };

    const visibleComboboxes = Array.from(document.querySelectorAll('div[role="combobox"]'))
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean);

    const bodyText = (document.body?.innerText || '').replace(/\s+/g, ' ').trim();
    const loginDetected =
      /登录|手机号|验证码|继续登录|注册|登录即可/.test(bodyText) &&
      !visibleComboboxes.some(text => text.includes('视频生成'));

    return {
      url: location.href,
      title: document.title || '',
      visibleComboboxes,
      hasVideoToolbar: visibleComboboxes.some(text => text.includes('视频生成')),
      loginDetected,
      bodyPreview: bodyText.slice(0, 300)
    };
  });
}

async function waitForJimengPage(page, baseUrl, timeoutMs) {
  const startedAt = Date.now();
  let lastState = null;
  let shouldNavigate = true;

  while (Date.now() - startedAt < timeoutMs) {
    lastState = await inspectJimengPage(
      page,
      shouldNavigate ? baseUrl : page.url() || baseUrl
    );
    shouldNavigate = false;
    if (lastState.loginDetected || lastState.hasVideoToolbar) {
      return lastState;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  return lastState;
}

function buildSummary(result) {
  const prefix = result.ok ? '✅ 即梦预检通过' : '❌ 即梦预检失败';
  const lines = [
    prefix,
    `时间: ${new Date().toLocaleString('zh-CN', { hour12: false })}`,
    `9222: ${result.version?.Browser || '不可用'}`,
    `页面: ${result.page?.url || '未获取'}`
  ];

  if (result.page?.hasVideoToolbar) {
    lines.push('视频生成页: 可用');
  }
  if (result.page?.loginDetected) {
    lines.push('登录态: 可能已失效');
  }
  if (result.reason) {
    lines.push(`原因: ${result.reason}`);
  }
  return lines.join('\n');
}

async function runPreflight(config) {
  const result = {
    ok: false,
    reason: '',
    version: null,
    page: null
  };

  try {
    result.version = await fetchVersion(config);
  } catch (error) {
    result.reason = `9222 不可连: ${error.message}`;
    return result;
  }

  let browser;
  try {
    browser = await puppeteer.connect({
      browserURL: `http://${config.cdpHost}:${config.cdpPort}`,
      defaultViewport: null,
      timeout: Math.min(config.preflight.timeoutMs, 30000),
      protocolTimeout: Math.max(config.preflight.timeoutMs, 60000)
    });
    let page = (await browser.pages()).find(item => item.url().includes('jimeng')) || null;
    if (!page) {
      page = await browser.newPage();
    }

    result.page = await waitForJimengPage(
      page,
      config.baseUrl,
      Math.max(config.preflight.timeoutMs, 45000)
    );
    if (result.page.loginDetected) {
      result.reason = '即梦登录态可能已失效';
      return result;
    }
    if (!result.page.hasVideoToolbar) {
      result.reason = '未检测到即梦视频生成页控件';
      return result;
    }

    result.ok = true;
    return result;
  } catch (error) {
    result.reason = `页面预检失败: ${error.message}`;
    return result;
  } finally {
    if (browser) {
      await browser.disconnect().catch(() => {});
    }
  }
}

async function main() {
  const configPath = process.argv[2] || path.join(__dirname, 'feishu-direct.json');
  const config = loadConfig(configPath);

  console.log('🩺 飞书到即梦预检');
  console.log('==============================');
  console.log(`配置文件: ${configPath}`);
  console.log(`目标页面: ${config.baseUrl}`);
  console.log(`CDP: ${config.cdpHost}:${config.cdpPort}`);

  const result = await runPreflight(config);
  const summary = buildSummary(result);
  console.log(summary);

  try {
    const notify = await sendNotification(config, summary, result.ok);
    if (notify.sent) {
      console.log(`📨 已发送飞书通知 (${notify.via})`);
    } else {
      console.log(`ℹ️ 未发送飞书通知: ${notify.skipped}`);
    }
  } catch (error) {
    console.log(`⚠️ 飞书通知发送失败: ${error.message}`);
  }

  process.exit(result.ok ? 0 : 1);
}

main().catch(error => {
  console.error(`❌ 预检运行失败: ${error.message}`);
  process.exit(1);
});
