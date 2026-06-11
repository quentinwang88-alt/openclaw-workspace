const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

// 默认配置
const DEFAULT_CONFIG = {
  cdpPort: 9222,
  cdpHost: '127.0.0.1',
  baseUrl: 'https://jimeng.jianying.com/ai-tool/generate?workspace=0&type=video',
  defaultModel: 'Seedance 2.0',
  defaultMode: '全能参考',
  defaultRatio: '9:16',
  defaultDuration: 15,
  outputDir: './output',
  timeout: 600000,
  protocolTimeoutMs: 300000,
  insufficientCreditsThreshold: 45
};

// 支持的图片格式
const IMAGE_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.webp', '.gif', '.heic', '.heif'];
const HEIC_EXTENSIONS = new Set(['.heic', '.heif']);
const LARGE_PNG_UPLOAD_BYTES = 1024 * 1024;
const INSUFFICIENT_CREDITS_PATTERNS = [
  '积分不足',
  '点数不足',
  '余额不足',
  '灵感值不足',
  '剩余积分不足',
  '剩余点数不足',
  '今日积分已用完',
  '今日点数已用完',
  'credit not enough',
  'credits not enough',
  'insufficient credits',
  'insufficient balance'
];
const SESSION_LIMIT_PATTERNS = [
  '会话数量',
  '会话数',
  '会话上限',
  '对话数量',
  '对话数',
  '对话上限',
  '新建会话',
  '新建对话',
  '新对话',
  '无法创建会话',
  '无法新建会话',
  '无法创建对话',
  '无法新建对话'
];

function containsInsufficientCreditsText(value) {
  const text = String(value || '').toLowerCase();
  if (!text) return false;
  return INSUFFICIENT_CREDITS_PATTERNS.some(pattern => text.includes(String(pattern).toLowerCase()));
}

function containsSessionLimitText(value) {
  const text = String(value || '').toLowerCase();
  if (!text) return false;
  return SESSION_LIMIT_PATTERNS.some(pattern => text.includes(String(pattern).toLowerCase()));
}

function isHiddenFile(fileName) {
  return path.basename(fileName || '').startsWith('.');
}

function isSupportedImageFile(fileName) {
  if (!fileName || isHiddenFile(fileName)) return false;
  return IMAGE_EXTENSIONS.some(ext => fileName.toLowerCase().endsWith(ext));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function shouldConvertToUploadJpeg(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  if (HEIC_EXTENSIONS.has(ext)) return true;
  return ext === '.png' && fs.statSync(imagePath).size >= LARGE_PNG_UPLOAD_BYTES;
}

function convertToUploadJpeg(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  if (!shouldConvertToUploadJpeg(imagePath)) return imagePath;

  const sourceStat = fs.statSync(imagePath);
  const cacheDir = path.join(path.dirname(imagePath), '.upload-cache');
  ensureDir(cacheDir);

  const stem = path.basename(imagePath, ext);
  const outputPath = path.join(cacheDir, `${stem}.upload.jpg`);

  if (fs.existsSync(outputPath)) {
    const outputStat = fs.statSync(outputPath);
    if (outputStat.size > 0 && outputStat.mtimeMs >= sourceStat.mtimeMs) {
      return outputPath;
    }
  }

  execFileSync('/usr/bin/sips', [
    '-s', 'format', 'jpeg',
    '-s', 'formatOptions', HEIC_EXTENSIONS.has(ext) ? '95' : '90',
    imagePath,
    '--out', outputPath
  ], {
    stdio: 'pipe'
  });

  return outputPath;
}

function prepareImagesForUpload(imagePaths = []) {
  return imagePaths.map(imagePath => {
    if (!imagePath || !fs.existsSync(imagePath)) {
      return { sourcePath: imagePath, uploadPath: imagePath, converted: false };
    }

    if (!shouldConvertToUploadJpeg(imagePath)) {
      return { sourcePath: imagePath, uploadPath: imagePath, converted: false };
    }

    const uploadPath = convertToUploadJpeg(imagePath);
    return {
      sourcePath: imagePath,
      uploadPath,
      converted: uploadPath !== imagePath
    };
  });
}

function formatBeijingTimestamp(date = new Date()) {
  const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
  return `${beijingDate.toISOString().slice(0, -1)}+08:00`;
}

function parseCreditsValue(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return null;
  }

  const text = String(rawValue).replace(/,/g, '').trim();
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) {
    return null;
  }

  const value = Number(match[0]);
  if (!Number.isFinite(value)) {
    return null;
  }

  if (/[万萬]/.test(text)) {
    return value * 10000;
  }

  if (/[千kK]/.test(text)) {
    return value * 1000;
  }

  // 即梦积分正常展示通常是整数；像 "1.5" 这类无单位小数更像前端缩写态，
  // 直接当作真实积分会误判低积分，因此这里视为歧义值，交给后续显式报错来兜底。
  if (String(match[0]).includes('.')) {
    return null;
  }

  return value;
}

function isCreditLabelText(rawValue) {
  const text = String(rawValue || '');
  return /积分|点数|余额|会员|credit|point|coin|vip/i.test(text);
}

function isReliableCreditsBelowThreshold(rawValue, parsedValue, threshold) {
  return parsedValue !== null && parsedValue < threshold && isCreditLabelText(rawValue);
}

async function readJimengCredits(page) {
  const candidates = await page.evaluate(() => {
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const values = [];
    const seen = new Set();
    const add = (text, source) => {
      const normalized = normalize(text);
      if (!normalized || seen.has(normalized)) return;
      seen.add(normalized);
      values.push({ text: normalized, source });
    };

    for (const selector of [
      '[class*="credit" i]',
      '[class*="point" i]',
      '[class*="coin" i]',
      '[class*="vip" i]',
      '[class*="member" i]'
    ]) {
      for (const el of document.querySelectorAll(selector)) {
        add(el.textContent, selector);
      }
    }

    const bodyText = normalize(document.body?.textContent);
    for (const match of bodyText.matchAll(/(\d[\d,]*(?:\.\d+)?)\s*(高级会员|会员|积分|点数|credits?|points?|coins?)/gi)) {
      add(match[0], 'body-credit-pattern');
    }
    for (const match of bodyText.matchAll(/(高级会员|会员|积分|点数|credits?|points?|coins?)[^\d]{0,20}(\d[\d,]*(?:\.\d+)?)/gi)) {
      add(match[0], 'body-credit-pattern');
    }

    return values.slice(0, 30);
  });

  let best = null;
  for (const candidate of candidates) {
    const value = parseCreditsValue(candidate.text);
    if (value === null) continue;
    const hasLabel = isCreditLabelText(candidate.text);
    const score = (hasLabel ? 1000 : 0) + Math.min(value, 100000) / 100 - Math.min(candidate.text.length, 120) / 20;
    if (!best || score > best.score) {
      best = { ...candidate, value, score };
    }
  }

  if (!best) {
    return { raw: null, value: null, source: null };
  }

  return {
    raw: best.text,
    value: best.value,
    source: best.source
  };
}

/**
 * 扫描文件夹，提取任务配置
 */
function scanFolder(folderPath) {
  const tasks = [];
  const items = fs.readdirSync(folderPath);
  
  // 检查是否是单任务文件夹（直接包含 image 和 prompt）
  const hasImage = items.some(item => 
    isSupportedImageFile(item)
  );
  const hasImageSubfolder = items.some(item => {
    const itemPath = path.join(folderPath, item);
    if (fs.statSync(itemPath).isDirectory()) {
      const subItems = fs.readdirSync(itemPath);
      return subItems.some(subItem => 
        isSupportedImageFile(subItem)
      );
    }
    return false;
  });
  const hasPrompt = items.includes('prompt.txt') || items.includes('prompt.md');
  
  if ((hasImage || hasImageSubfolder) && hasPrompt) {
    // 单任务模式：根目录有 prompt.txt，图片在根目录或子文件夹
    const task = parseTaskFolder(folderPath, folderPath);
    if (task) tasks.push(task);
  } else {
    // 多任务模式：扫描子文件夹
    for (const item of items) {
      const itemPath = path.join(folderPath, item);
      if (fs.statSync(itemPath).isDirectory() && !item.startsWith('.')) {
        const task = parseTaskFolder(itemPath, folderPath);
        if (task) tasks.push(task);
      }
    }
  }
  
  return tasks;
}

/**
 * 解析单个任务文件夹
 * 支持三种结构：
 * 1. 标准结构：task/image.png + task/prompt.txt
 * 2. 图片在子文件夹：task/prompt.txt + task/图片/image.png
 * 3. 子文件夹作为任务：task/subfolder/image.png (向上查找prompt.txt)
 */
function parseTaskFolder(folderPath, rootPath = null) {
  const items = fs.readdirSync(folderPath);
  const root = rootPath || folderPath;
  
  // 查找所有图片 - 先在根目录查找，再在子文件夹查找
  let imageFiles = [];
  
  // 1. 先在当前目录查找图片
  for (const item of items) {
    if (isSupportedImageFile(item)) {
      imageFiles.push(path.join(folderPath, item));
    }
  }
  
  // 2. 如果没有，查找常见的图片子文件夹
  if (imageFiles.length === 0) {
    const imageSubfolders = ['图片', '产品主图', 'images', 'image', 'img', 'photos'];
    for (const subfolder of imageSubfolders) {
      const subfolderPath = path.join(folderPath, subfolder);
      if (fs.existsSync(subfolderPath) && fs.statSync(subfolderPath).isDirectory()) {
        const subItems = fs.readdirSync(subfolderPath);
        for (const item of subItems) {
          if (isSupportedImageFile(item)) {
            imageFiles.push(path.join(subfolderPath, item));
          }
        }
        if (imageFiles.length > 0) break;
      }
    }
  }
  
  // 查找提示词 - 先在当前目录，再向上查找
  let prompt = null;
  let promptFile = items.find(item => item === 'prompt.txt' || item === 'prompt.md');
  
  if (promptFile) {
    prompt = fs.readFileSync(path.join(folderPath, promptFile), 'utf8').trim();
  } else if (folderPath !== root) {
    // 向上查找 prompt.txt
    const parentPromptFile = path.join(root, 'prompt.txt');
    if (fs.existsSync(parentPromptFile)) {
      prompt = fs.readFileSync(parentPromptFile, 'utf8').trim();
    }
  }
  
  // 没有必需文件则跳过
  if (imageFiles.length === 0 && !prompt) {
    return null;
  }
  
  // 读取可选配置
  let config = {};
  const configFile = items.find(item => item === 'config.json');
  if (configFile) {
    try {
      config = JSON.parse(fs.readFileSync(path.join(folderPath, configFile), 'utf8'));
    } catch (e) {
      console.log(`  ⚠️ 配置文件解析失败: ${e.message}`);
    }
  }
  
  return { 
    folder: folderPath, 
    name: path.basename(folderPath), 
    images: imageFiles,  // 改为数组
    prompt: prompt, 
    config: config 
  };
}

function isTaskCompleted(task, outputDir) {
  const outputPath = path.join(outputDir, `${task.name}.mp4`);
  const completedFile = path.join(task.folder, '.completed');
  const submittedFile = path.join(task.folder, '.submitted');
  const blockedFile = path.join(task.folder, '.blocked');
  // 如果已完成或已提交，都跳过
  return fs.existsSync(outputPath) || fs.existsSync(completedFile) || fs.existsSync(submittedFile) || fs.existsSync(blockedFile);
}

function markTaskCompleted(task) {
  const statusFile = path.join(task.folder, '.completed');
  const blockedFile = path.join(task.folder, '.blocked');
  if (fs.existsSync(blockedFile)) {
    fs.unlinkSync(blockedFile);
  }
  fs.writeFileSync(statusFile, JSON.stringify({
    time: formatBeijingTimestamp()
  }, null, 2));
}

function markTaskSubmitted(task) {
  const statusFile = path.join(task.folder, '.submitted');
  const blockedFile = path.join(task.folder, '.blocked');
  if (fs.existsSync(blockedFile)) {
    fs.unlinkSync(blockedFile);
  }
  fs.writeFileSync(statusFile, JSON.stringify({
    time: formatBeijingTimestamp()
  }, null, 2));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function isRecoverableProtocolEvaluateError(error) {
  const message = String(error?.message || error || '');
  return (
    message.includes('Runtime.callFunctionOn timed out') ||
    message.includes('Protocol error (Runtime.callFunctionOn)') ||
    message.includes('Execution context was destroyed') ||
    message.includes('Cannot find context with specified id') ||
    message.includes('Inspected target navigated or closed') ||
    message.includes('Most likely the page has been closed')
  );
}

async function safePageEvaluate(page, label, pageFunction, ...args) {
  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await page.evaluate(pageFunction, ...args);
    } catch (error) {
      lastError = error;
      if (!isRecoverableProtocolEvaluateError(error) || attempt === 2) {
        break;
      }
      console.log(`  ⚠️ ${label} 超时，准备重试 (${attempt}/2)`);
      await sleep(400);
    }
  }
  throw lastError;
}

async function safeHandleEvaluate(handle, label, pageFunction, ...args) {
  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await handle.evaluate(pageFunction, ...args);
    } catch (error) {
      lastError = error;
      if (!isRecoverableProtocolEvaluateError(error) || attempt === 2) {
        break;
      }
      console.log(`  ⚠️ ${label} 超时，准备重试 (${attempt}/2)`);
      await sleep(400);
    }
  }
  throw lastError;
}

async function prepareAutomationPage(page) {
  if (!page) return;

  try {
    await page.bringToFront();
  } catch (error) {
    // 忽略 bringToFront 失败，继续尝试后续页面操作
  }

  try {
    await page.setViewport({ width: 1600, height: 1000 });
  } catch (error) {
    // 某些连接场景下不需要设置 viewport
  }
}

async function gotoWithTolerance(page, targetUrl, options = {}) {
  const {
    label = '目标页面',
    waitUntil = 'domcontentloaded',
    timeout = 45000,
    settleMs = 1500
  } = options;

  try {
    await page.goto(targetUrl, {
      waitUntil,
      timeout
    });
  } catch (error) {
    const message = error?.message || String(error);
    if (!message.includes('Navigation timeout')) {
      throw error;
    }
    console.log(`  ⚠️ ${label} 导航等待超时，继续检查页面是否已可用...`);
  }

  if (settleMs > 0) {
    await sleep(settleMs);
  }
}

async function inspectVideoGenerationPage(page) {
  return await safePageEvaluate(page, '检查视频生成页', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const visibleComboboxes = Array.from(document.querySelectorAll('div[role="combobox"]'))
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean);
    const visibleButtons = Array.from(document.querySelectorAll('button'))
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean);
    const hasRatioButton = visibleButtons.some(text => /^\d+:\d+$/.test(text));

    const bodyText = (document.body?.innerText || '').replace(/\s+/g, ' ').trim();
    const normalizedBody = bodyText.toLowerCase();
    const loginDetected =
      /登录|手机号|验证码|继续登录|注册|登录即可/.test(bodyText) &&
      !visibleComboboxes.some(text => text.includes('视频生成'));
    const brokenPageDetected =
      normalizedBody.includes('404 not found') ||
      normalizedBody.includes('guru meditation') ||
      normalizedBody.includes('goofy deploy page server');
    const creationHubDetected =
      bodyText.includes('开启你的') &&
      bodyText.includes('视频生成') &&
      bodyText.includes('即刻造梦');
    const defaultCreationInputReady =
      bodyText.includes('你好，想创作什么') &&
      bodyText.includes('参考内容') &&
      bodyText.includes('视频生成');
    const bottomAreaTop = Math.max(600, window.innerHeight - 380);
    const composerReferenceImageCount = Array.from(document.querySelectorAll('img'))
      .filter(isVisible)
      .filter(img => {
        const rect = img.getBoundingClientRect();
        return (
          rect.left >= 520 &&
          rect.left <= window.innerWidth - 120 &&
          rect.top >= bottomAreaTop &&
          rect.top <= window.innerHeight - 20 &&
          rect.width >= 24 &&
          rect.height >= 24
        );
      }).length;
    const composerTextbox = Array.from(document.querySelectorAll('textarea, [role="textbox"], [contenteditable="true"]'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        return {
          text: (('value' in el ? el.value : el.textContent) || '').replace(/\s+/g, ' ').trim(),
          top: rect.top,
          left: rect.left
        };
      })
      .filter(item => item.left >= 520 && item.top >= bottomAreaTop)
      .sort((a, b) => a.top - b.top || a.left - b.left)[0] || null;
    const composerText = composerTextbox?.text || '';
    const composerHasUserText =
      composerText.length > 20 &&
      !composerText.includes('上传最多12个参考素材') &&
      !composerText.includes('输入想法、剧本或上传参考');
    const composerHasUserContent = composerReferenceImageCount > 0 || composerHasUserText;

    return {
      url: location.href,
      visibleComboboxes,
      visibleButtons,
      hasVideoToolbar: visibleComboboxes.some(text => text.includes('视频生成')),
      creationHubDetected,
      defaultCreationInputReady,
      toolbarReady: !creationHubDetected && (defaultCreationInputReady || (visibleComboboxes.length >= 4 && hasRatioButton)),
      composerReferenceImageCount,
      composerHasUserContent,
      loginDetected,
      brokenPageDetected,
      bodyPreview: bodyText.slice(0, 200)
    };
  });
}

async function waitForVideoGenerationPageReady(page, timeout = 45000) {
  const startedAt = Date.now();
  let lastState = null;

  while (Date.now() - startedAt < timeout) {
    lastState = await inspectVideoGenerationPage(page).catch(() => null);
    if (lastState?.loginDetected) {
      throw new Error('即梦登录态可能已失效');
    }
    if (lastState?.brokenPageDetected) {
      const suffix = lastState?.bodyPreview ? `：${lastState.bodyPreview}` : '';
      throw new Error(`即梦视频生成页进入异常发布页${suffix}`);
    }
    if (lastState?.toolbarReady) {
      return lastState;
    }
    await sleep(1000);
  }

  const suffix = lastState?.bodyPreview ? `：${lastState.bodyPreview}` : '';
  const toolbarHint = lastState
    ? `（combobox=${lastState.visibleComboboxes?.length || 0}, ratio=${lastState.visibleButtons?.find?.(text => /^\d+:\d+$/.test(text)) || '无'}）`
    : '';
  throw new Error(`未检测到即梦视频生成页完整控件${toolbarHint}${suffix}`);
}

async function switchToReusableCreationSession(page) {
  const target = await safePageEvaluate(page, '定位默认创作会话', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const findClickableRow = el => {
      let row = el;
      for (let depth = 0; depth < 8 && row?.parentElement; depth++) {
        const parent = row.parentElement;
        const rect = parent.getBoundingClientRect();
        const text = normalize(parent.innerText || parent.textContent);
        if (
          rect.left >= 20 &&
          rect.left < 330 &&
          rect.right <= 360 &&
          rect.width >= 90 &&
          rect.height >= 24 &&
          rect.height <= 78 &&
          text.includes('默认创作')
        ) {
          row = parent;
        }
      }
      const clickable = row?.closest?.('button, [role="button"], a') || row || el;
      const clickableRect = clickable.getBoundingClientRect();
      if (
        clickableRect.left >= 20 &&
        clickableRect.left < 330 &&
        clickableRect.right <= 380 &&
        clickableRect.width >= 70 &&
        clickableRect.height >= 20
      ) {
        return clickable;
      }
      return row || el;
    };

    const candidates = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))
      .filter(isVisible)
      .map(el => {
        const text = normalize(el.textContent);
        const rect = el.getBoundingClientRect();
        const selected =
          el.getAttribute('aria-selected') === 'true' ||
          el.getAttribute('aria-current') === 'true' ||
          String(el.className || '').toLowerCase().includes('active') ||
          String(el.className || '').toLowerCase().includes('selected');
        return { el, text, rect, selected };
      })
      .filter(item =>
        (item.text === '默认创作' && item.rect.left < 360 && item.rect.top < 260) ||
        (item.text.includes('默认创作') && item.rect.left < 360 && item.rect.top < 220 && item.rect.width < 320)
      )
      .map(item => {
        const clickable = findClickableRow(item.el);
        const clickRect = clickable.getBoundingClientRect();
        const rowText = normalize(clickable.innerText || clickable.textContent || item.text);
        const rowClass = String(clickable.className || '');
        const selected =
          item.selected ||
          clickable.getAttribute('aria-selected') === 'true' ||
          clickable.getAttribute('aria-current') === 'true' ||
          rowClass.toLowerCase().includes('active') ||
          rowClass.toLowerCase().includes('selected');
        return { ...item, clickable, clickRect, rowText, selected };
      })
      .filter(item =>
        item.clickRect.left >= 20 &&
        item.clickRect.left < 330 &&
        item.clickRect.right <= 380 &&
        item.clickRect.top >= 30 &&
        item.clickRect.top < 260 &&
        item.clickRect.width >= 70 &&
        item.clickRect.height >= 20 &&
        item.clickRect.height <= 90
      )
      .sort((a, b) => {
        const aTextExact = a.rowText === '默认创作' ? 0 : 1;
        const bTextExact = b.rowText === '默认创作' ? 0 : 1;
        return aTextExact - bTextExact || a.clickRect.top - b.clickRect.top || a.clickRect.left - b.clickRect.left;
      });

    const target = candidates[0] || null;
    if (!target) {
      return { clicked: false, reason: 'default_creation_not_found' };
    }

    return {
      clicked: true,
      text: target.rowText,
      x: Math.round(target.clickRect.left + target.clickRect.width / 2),
      y: Math.round(target.clickRect.top + target.clickRect.height / 2),
      rect: {
        left: Math.round(target.clickRect.left),
        top: Math.round(target.clickRect.top),
        width: Math.round(target.clickRect.width),
        height: Math.round(target.clickRect.height)
      },
      alreadyActive: target.selected
    };
  }).catch(error => ({ clicked: false, reason: error.message }));

  if (!target.clicked) {
    return target;
  }

  if (Number.isFinite(target.x) && Number.isFinite(target.y)) {
    await page.mouse.click(target.x, target.y, { delay: 80 });
  }
  return target;
}

async function openVideoCreationEntry(page) {
  return await safePageEvaluate(page, '打开视频生成入口', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const candidates = Array.from(document.querySelectorAll('button, [role="button"], a, div'))
      .filter(isVisible)
      .map(el => {
        const text = normalize(el.innerText || el.textContent);
        const rect = el.getBoundingClientRect();
        return { el, text, rect };
      })
      .filter(item =>
        item.text.includes('视频生成') &&
        /Seedance|2\.0/.test(item.text) &&
        item.rect.top > 250 &&
        item.rect.left > 250 &&
        item.rect.width >= 120 &&
        item.rect.height >= 50
      )
      .sort((a, b) => {
        const aScore = Math.abs(a.rect.top - 390) + Math.abs(a.rect.left - 1220);
        const bScore = Math.abs(b.rect.top - 390) + Math.abs(b.rect.left - 1220);
        return aScore - bScore;
      });

    const target = candidates[0]?.el || null;
    if (!target) {
      return { clicked: false, reason: 'video_creation_entry_not_found' };
    }
    const clickable = target.closest('button, [role="button"], a') || target;
    clickable.click();
    return { clicked: true, text: normalize(clickable.innerText || clickable.textContent || target.textContent) };
  }).catch(error => ({ clicked: false, reason: error.message }));
}

async function preferReusableCreationSession(page, options = {}) {
  const requireReady = options.requireReady !== false;
  const reuseResult = await switchToReusableCreationSession(page);
  if (!reuseResult.clicked) {
    return reuseResult;
  }

  console.log(reuseResult.alreadyActive
    ? '  ♻️ 默认创作会话已选中，继续复用'
    : '  ♻️ 已切换到默认创作会话，避免新建会话');
  await sleep(1200);
  await ensureReusableCreationVideoMode(page).catch(error => {
    console.log(`  ⚠️ 默认创作切换视频模式失败: ${error.message}`);
  });
  if (requireReady) {
    await waitForVideoGenerationPageReady(page);
  }
  return reuseResult;
}

async function ensureReusableCreationVideoMode(page) {
  const currentState = await inspectVideoGenerationPage(page).catch(() => null);
  if (currentState?.toolbarReady) {
    return { selected: false, ready: true, reason: 'toolbar_ready' };
  }

  const trigger = await safePageEvaluate(page, '定位默认创作类型下拉', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const candidates = Array.from(document.querySelectorAll('[role="combobox"], button, [role="button"], div, span'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        const text = normalize(el.innerText || el.textContent);
        const clickTarget = el.closest?.('[role="combobox"], button, [role="button"]') || el;
        const clickRect = clickTarget.getBoundingClientRect();
        return { el, clickTarget, rect, clickRect, text };
      })
      .filter(item =>
        item.text === 'Agent 模式' &&
        item.clickRect.left > 300 &&
        item.clickRect.top > window.innerHeight * 0.55 &&
        item.clickRect.width >= 80 &&
        item.clickRect.height >= 28
      )
      .sort((a, b) => a.clickRect.top - b.clickRect.top || a.clickRect.left - b.clickRect.left);

    const target = candidates[0];
    if (!target) {
      return { found: false, reason: 'agent_mode_trigger_not_found' };
    }
    return {
      found: true,
      x: Math.round(target.clickRect.left + target.clickRect.width / 2),
      y: Math.round(target.clickRect.top + target.clickRect.height / 2),
      text: target.text
    };
  });

  if (!trigger.found) {
    return { selected: false, ready: false, reason: trigger.reason };
  }

  await page.mouse.click(trigger.x, trigger.y, { delay: 80 });
  await sleep(800);

  const option = await safePageEvaluate(page, '定位视频生成创作类型', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const candidates = Array.from(document.querySelectorAll('[role="option"], li, div, span'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        const text = normalize(el.innerText || el.textContent);
        const clickTarget = el.closest?.('[role="option"], li') || el;
        const clickRect = clickTarget.getBoundingClientRect();
        return { el, clickTarget, rect, clickRect, text, role: el.getAttribute('role') || '' };
      })
      .filter(item =>
        item.text === '视频生成' &&
        item.clickRect.left > 300 &&
        item.clickRect.top > 300 &&
        item.clickRect.width >= 80 &&
        item.clickRect.height >= 24
      )
      .sort((a, b) => a.clickRect.top - b.clickRect.top || a.clickRect.left - b.clickRect.left);

    const target = candidates[0];
    if (!target) {
      return { found: false, reason: 'video_generation_option_not_found' };
    }
    return {
      found: true,
      x: Math.round(target.clickRect.left + target.clickRect.width / 2),
      y: Math.round(target.clickRect.top + target.clickRect.height / 2),
      text: target.text
    };
  });

  if (!option.found) {
    await page.keyboard.press('Escape').catch(() => {});
    return { selected: false, ready: false, reason: option.reason };
  }

  await page.mouse.click(option.x, option.y, { delay: 80 });
  await sleep(1500);
  const selectedState = await inspectVideoGenerationPage(page).catch(() => null);
  if (selectedState?.toolbarReady) {
    console.log('  ♻️ 已在默认创作内切换到视频生成模式');
    return { selected: true, ready: true };
  }
  return { selected: true, ready: false, reason: 'video_mode_not_ready' };
}

async function cleanupRecentUnnamedConversations(page, options = {}) {
  const maxDelete = Math.max(0, Math.min(3, Number(options.maxDelete || 2) || 0));
  const reason = options.reason || 'failed_submit_attempt';
  if (!maxDelete) {
    return { deleted: 0, reason };
  }

  await prepareAutomationPage(page).catch(() => {});
  try {
    await page.keyboard.press('Escape');
    await sleep(200);
  } catch (error) {
    // Best effort only.
  }

  let deleted = 0;
  const attempts = [];

  for (let index = 0; index < maxDelete; index++) {
    const row = await safePageEvaluate(page, '定位最近未命名会话', () => {
      const visible = el => {
        if (!el || !(el instanceof Element)) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };
      const compact = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = [];
      const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_TEXT);
      let node;

      while ((node = walker.nextNode())) {
        if (compact(node.nodeValue) !== '未命名对话') continue;
        const textEl = node.parentElement;
        if (!visible(textEl)) continue;

        let rowEl = textEl;
        for (let depth = 0; depth < 8 && rowEl.parentElement; depth++) {
          const parent = rowEl.parentElement;
          const rect = parent.getBoundingClientRect();
          const text = compact(parent.innerText || parent.textContent);
          if (
            rect.left < 380 &&
            rect.right <= 390 &&
            rect.width >= 120 &&
            rect.height >= 24 &&
            rect.height <= 80 &&
            text === '未命名对话'
          ) {
            rowEl = parent;
          }
        }

        const rect = rowEl.getBoundingClientRect();
        if (rect.bottom <= 0 || rect.top >= window.innerHeight + 80 || rect.left > 380) continue;
        candidates.push({
          active: String(rowEl.className || '').includes('active'),
          rect: {
            left: Math.min(93, Math.round(rect.left)),
            top: Math.max(0, Math.round(rect.top - 8)),
            right: Math.max(299, Math.round(rect.right + 96)),
            bottom: Math.round(rect.bottom + 8)
          }
        });
      }

      const seen = new Set();
      return candidates
        .filter(item => {
          const key = `${item.rect.left}:${item.rect.top}:${item.rect.right}:${item.rect.bottom}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        })
        .sort((a, b) => Number(b.active) - Number(a.active) || a.rect.top - b.rect.top)[0] || null;
    }).catch(error => ({ error: error.message }));

    if (!row || row.error) {
      attempts.push({ status: 'no-row', error: row?.error || '' });
      break;
    }

    const rect = row.rect;
    console.log(`  🧹 清理失败尝试产生的未命名会话 (${deleted + 1}/${maxDelete})`);
    await page.mouse.move(rect.right - 18, Math.round((rect.top + rect.bottom) / 2));
    await sleep(250);

    const moreClick = await safePageEvaluate(page, '打开未命名会话菜单', rowRect => {
      const visible = el => {
        if (!el || !(el instanceof Element)) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const centerY = (rowRect.top + rowRect.bottom) / 2;
      const targets = Array.from(document.querySelectorAll('button, [role="button"], div, span, svg'))
        .filter(visible)
        .map(el => ({ el, rect: el.getBoundingClientRect(), text: normalize(el.textContent || el.getAttribute('aria-label') || el.getAttribute('title')) }))
        .filter(item => {
          const xOk = item.rect.left >= rowRect.left + 110 && item.rect.right <= rowRect.right + 36;
          const yOk = item.rect.top >= rowRect.top - 12 && item.rect.bottom <= rowRect.bottom + 12;
          const looksMore = /更多|more|菜单|操作|···|⋯|…/.test(item.text) || item.rect.width <= 36;
          return xOk && yOk && looksMore;
        })
        .sort((a, b) => {
          const ax = Math.abs(a.rect.left + a.rect.width / 2 - (rowRect.right - 18)) + Math.abs(a.rect.top + a.rect.height / 2 - centerY);
          const bx = Math.abs(b.rect.left + b.rect.width / 2 - (rowRect.right - 18)) + Math.abs(b.rect.top + b.rect.height / 2 - centerY);
          return ax - bx;
        });
      if (!targets.length) return { clicked: false, reason: 'no-more-target' };
      const target = targets[0].el.closest('button,[role="button"]') || targets[0].el;
      target.click();
      return { clicked: true };
    }, rect).catch(error => ({ clicked: false, reason: error.message }));

    if (!moreClick.clicked) {
      await page.mouse.click(rect.right - 18, Math.round((rect.top + rect.bottom) / 2)).catch(() => {});
    }
    await sleep(500);

    const deleteClick = await safePageEvaluate(page, '点击未命名会话删除', () => {
      const visible = el => {
        if (!el || !(el instanceof Element)) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="button"], div, span'))
        .filter(visible)
        .map(el => ({ el, rect: el.getBoundingClientRect(), text: normalize(el.textContent) }))
        .filter(item => item.text === '删除' && item.rect.width >= 24 && item.rect.height >= 18)
        .sort((a, b) => b.rect.top - a.rect.top);
      if (!candidates.length) return { clicked: false, reason: 'no-delete-menu' };
      const target = candidates[0].el.closest('button,[role="button"]') || candidates[0].el;
      target.click();
      return { clicked: true };
    }).catch(error => ({ clicked: false, reason: error.message }));

    if (!deleteClick.clicked) {
      attempts.push({ status: 'delete-menu-failed', reason: deleteClick.reason || '' });
      await page.keyboard.press('Escape').catch(() => {});
      break;
    }
    await sleep(500);

    const confirmClick = await safePageEvaluate(page, '确认删除未命名会话', () => {
      const visible = el => {
        if (!el || !(el instanceof Element)) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="button"], div, span'))
        .filter(visible)
        .map(el => ({ el, rect: el.getBoundingClientRect(), text: normalize(el.textContent) }))
        .filter(item => ['删除', '确认删除', '确定', '确认'].includes(item.text) && item.rect.width >= 32 && item.rect.height >= 22)
        .sort((a, b) => {
          const aModal = a.rect.left > 260 || a.rect.top > window.innerHeight * 0.25 ? 1 : 0;
          const bModal = b.rect.left > 260 || b.rect.top > window.innerHeight * 0.25 ? 1 : 0;
          return bModal - aModal || b.rect.top - a.rect.top;
        });
      if (!candidates.length) return { clicked: false, reason: 'no-confirm' };
      const target = candidates[0].el.closest('button,[role="button"]') || candidates[0].el;
      target.click();
      return { clicked: true };
    }).catch(error => ({ clicked: false, reason: error.message }));

    if (!confirmClick.clicked) {
      attempts.push({ status: 'confirm-failed', reason: confirmClick.reason || '' });
      await page.keyboard.press('Escape').catch(() => {});
      break;
    }

    deleted += 1;
    attempts.push({ status: 'deleted' });
    await sleep(800);
  }

  if (deleted > 0) {
    console.log(`  🧹 已清理 ${deleted} 个失败尝试产生的未命名会话`);
  }

  return { deleted, reason, attempts };
}

function isRecoverableVideoGenerationPageError(error) {
  const message = String(error?.message || error || '');
  return (
    message.includes('即梦视频生成页进入异常发布页') ||
    message.includes('404 Not Found') ||
    message.includes('Guru Meditation') ||
    message.includes('Goofy Deploy Page Server') ||
    message.includes('默认创作仍有未提交内容') ||
    message.includes('未检测到即梦视频生成页完整控件') ||
    message.includes('未检测到即梦视频生成页控件')
  );
}

async function clearComposerDraftContent(page, options = {}) {
  const maxAttempts = options.maxAttempts || 12;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const state = await inspectVideoGenerationPage(page).catch(() => null);
    const currentCount = state?.composerReferenceImageCount || 0;
    if (!state?.composerHasUserContent || currentCount <= 0) {
      return { cleared: true, remaining: currentCount, attempts: attempt - 1 };
    }

    const imageTarget = await safePageEvaluate(page, '定位默认创作残留参考图', () => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };
      const bottomAreaTop = Math.max(600, window.innerHeight - 380);
      const images = Array.from(document.querySelectorAll('img'))
        .filter(isVisible)
        .map(img => {
          const rect = img.getBoundingClientRect();
          return {
            left: rect.left,
            top: rect.top,
            right: rect.right,
            bottom: rect.bottom,
            width: rect.width,
            height: rect.height
          };
        })
        .filter(rect =>
          rect.left >= 520 &&
          rect.left <= window.innerWidth - 120 &&
          rect.top >= bottomAreaTop &&
          rect.top <= window.innerHeight - 20 &&
          rect.width >= 24 &&
          rect.height >= 24
        )
        .sort((a, b) => a.top - b.top || a.left - b.left);
      const rect = images[0];
      if (!rect) return { found: false };
      return {
        found: true,
        hoverX: Math.round(rect.left + rect.width / 2),
        hoverY: Math.round(rect.top + rect.height / 2),
        fallbackX: Math.round(rect.right - 8),
        fallbackY: Math.round(rect.top + 8),
        rect
      };
    });

    if (!imageTarget.found) {
      return { cleared: false, remaining: currentCount, attempts: attempt - 1, reason: 'image_not_found' };
    }

    await page.mouse.move(imageTarget.hoverX, imageTarget.hoverY);
    await sleep(250);

    const deleteTarget = await safePageEvaluate(page, '定位默认创作参考图删除按钮', targetRect => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="button"], [aria-label], [title], svg, span, div'))
        .filter(isVisible)
        .map(el => {
          const rect = el.getBoundingClientRect();
          const text = normalize(el.textContent || el.getAttribute('aria-label') || el.getAttribute('title') || el.className);
          const clickTarget = el.closest?.('button,[role="button"]') || el;
          const clickRect = clickTarget.getBoundingClientRect();
          return { el, clickTarget, rect, clickRect, text };
        })
        .filter(item => {
          const nearImage =
            item.clickRect.left >= targetRect.left - 28 &&
            item.clickRect.right <= targetRect.right + 44 &&
            item.clickRect.top >= targetRect.top - 32 &&
            item.clickRect.bottom <= targetRect.bottom + 32;
          const looksLikeDelete =
            /删除|移除|关闭|取消|close|remove|delete/i.test(item.text) ||
            (item.clickRect.width <= 32 && item.clickRect.height <= 32 && item.clickRect.top <= targetRect.top + 18);
          return nearImage && looksLikeDelete;
        })
        .sort((a, b) => {
          const aScore = (a.clickRect.top - targetRect.top) + Math.abs(a.clickRect.right - targetRect.right);
          const bScore = (b.clickRect.top - targetRect.top) + Math.abs(b.clickRect.right - targetRect.right);
          return aScore - bScore;
        });
      const target = candidates[0];
      if (!target) return { found: false };
      return {
        found: true,
        x: Math.round(target.clickRect.left + target.clickRect.width / 2),
        y: Math.round(target.clickRect.top + target.clickRect.height / 2),
        text: target.text
      };
    }, imageTarget.rect);

    const clickX = deleteTarget.found ? deleteTarget.x : imageTarget.fallbackX;
    const clickY = deleteTarget.found ? deleteTarget.y : imageTarget.fallbackY;
    await page.mouse.click(clickX, clickY, { delay: 60 });
    await sleep(500);
  }

  const finalState = await inspectVideoGenerationPage(page).catch(() => null);
  return {
    cleared: !finalState?.composerHasUserContent,
    remaining: finalState?.composerReferenceImageCount || 0,
    attempts: maxAttempts
  };
}

async function resetComposerUploadCache(page, stage = '') {
  await safePageEvaluate(page, '清理默认创作上传缓存', () => {
    try {
      localStorage.removeItem('dreamina__upload-service-cache');
    } catch (error) {
      // 忽略 localStorage 不可用场景，后续会继续走页面级清理。
    }
  });
  console.log(`  🧹 已清理默认创作上传缓存${stage ? ` (${stage})` : ''}`);
  await page.reload({ waitUntil: 'domcontentloaded', timeout: 45000 }).catch(error => {
    const message = String(error?.message || error || '');
    if (!message.includes('Navigation timeout')) {
      throw error;
    }
    console.log('  ⚠️ 默认创作重载等待超时，继续检查页面状态');
  });
  await sleep(2000);
}

async function getVisibleElementHandles(page, selector, maxY = Infinity) {
  const handles = await page.$$(selector);
  if (handles.length === 0) {
    return [];
  }

  const metas = await safePageEvaluate(page, '读取可见元素列表', querySelector => {
    return Array.from(document.querySelectorAll(querySelector)).map(el => {
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return {
        text: (el.textContent || '').replace(/\s+/g, ' ').trim(),
        visible:
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0,
        x: rect.x,
        y: rect.y,
        width: rect.width,
        height: rect.height
      };
    });
  }, selector);

  const visible = [];

  const count = Math.min(handles.length, metas.length);
  for (let index = 0; index < count; index++) {
    const handle = handles[index];
    const meta = metas[index];

    if (meta.visible && meta.y <= maxY) {
      visible.push({ handle, ...meta });
    }
  }

  visible.sort((a, b) => a.y - b.y || a.x - b.x);
  return visible;
}

async function ensureVideoGenerationPage(page, baseUrl = DEFAULT_CONFIG.baseUrl) {
  await prepareAutomationPage(page);
  const currentState = await inspectVideoGenerationPage(page).catch(() => null);
  const onVideoPage =
    currentState &&
    currentState.url.includes('/ai-tool/generate') &&
    currentState.url.includes('workspace=0') &&
    currentState.toolbarReady;

  if (onVideoPage) {
    return false;
  }

  await resetVideoGenerationPage(page, baseUrl);
  return true;
}

function getReusableCreationUrl(baseUrl = DEFAULT_CONFIG.baseUrl) {
  try {
    const parsed = new URL(baseUrl);
    parsed.pathname = '/ai-tool/generate';
    parsed.search = '?workspace=0&type=video';
    return parsed.toString();
  } catch (error) {
    return DEFAULT_CONFIG.baseUrl;
  }
}

async function resetVideoGenerationPage(page, baseUrl = DEFAULT_CONFIG.baseUrl) {
  await prepareAutomationPage(page);
  let lastError = null;
  const reusableCreationUrl = getReusableCreationUrl(baseUrl);

  const forceOpenReusableCreationUrl = async (stage, options = {}) => {
    const blankFirst = options.blankFirst !== false;
    if (blankFirst) {
      try {
        await page.goto('about:blank', {
          waitUntil: 'domcontentloaded',
          timeout: 15000
        });
      } catch (error) {
        // 忽略跳到 about:blank 的失败，继续直达默认创作生成页
      }
      await sleep(800);
    }
    await gotoWithTolerance(page, reusableCreationUrl, { label: `默认创作工作区${stage ? `-${stage}` : ''}` });
    await ensureReusableCreationVideoMode(page).catch(error => {
      console.log(`  ⚠️ 默认创作直达后切换视频模式失败: ${error.message}`);
    });
    return await waitForVideoGenerationPageReady(page);
  };

  const ensureCleanReusableCreation = async (stage) => {
    await waitForVideoGenerationPageReady(page);
    const cleanState = await inspectVideoGenerationPage(page).catch(() => null);
    if (cleanState?.composerHasUserContent) {
      const count = cleanState.composerReferenceImageCount || 0;
      console.log(`  🧹 默认创作仍有未提交内容，尝试清空草稿 (${stage}, 参考图=${count})`);
      await resetComposerUploadCache(page, stage);
      const afterCacheState = await inspectVideoGenerationPage(page).catch(() => null);
      if (!afterCacheState?.composerHasUserContent) {
        console.log('  🧹 默认创作草稿已通过缓存清理重置');
        return;
      }
      const clearResult = await clearComposerDraftContent(page, { maxAttempts: Math.max(12, count + 4) });
      const afterClearState = await inspectVideoGenerationPage(page).catch(() => null);
      if (afterClearState?.composerHasUserContent) {
        const remaining = afterClearState.composerReferenceImageCount || clearResult.remaining || 0;
        throw new Error(`默认创作仍有未提交内容 (${stage}, 参考图=${remaining})`);
      }
      console.log('  🧹 默认创作草稿已清空');
    }
  };

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`  重置视频生成页面... (尝试 ${attempt}/3)`);
      const currentState = await inspectVideoGenerationPage(page).catch(() => null);
      const onReusableCreation =
        currentState?.url?.includes('/ai-tool/generate') &&
        currentState?.url?.includes('workspace=0');
      if (onReusableCreation && currentState?.toolbarReady && !currentState?.composerHasUserContent) {
        console.log('  ♻️ 已在默认创作工作区，直接复用');
        return;
      }
      if (onReusableCreation && currentState?.toolbarReady && currentState?.composerHasUserContent) {
        console.log(`  ♻️ 默认创作存在未提交内容，重新加载干净工作区 (参考图=${currentState.composerReferenceImageCount || 0})`);
      }
      if (onReusableCreation) {
        const reuseResult = await preferReusableCreationSession(page, { requireReady: false });
        if (reuseResult.clicked) {
          const reuseReady = await waitForVideoGenerationPageReady(page).then(
            () => true,
            error => {
              console.log(`  ⚠️ 默认创作会话未出现生成控件，强制重进生成页: ${error.message}`);
              return false;
            }
          );
          if (reuseReady) {
            await ensureCleanReusableCreation('reuse');
            return;
          }
          await forceOpenReusableCreationUrl('reuse-force', { blankFirst: true });
          await ensureCleanReusableCreation('reuse-force');
          return;
        }
      }
      if (attempt > 1) {
        try {
          await page.goto('about:blank', {
            waitUntil: 'domcontentloaded',
            timeout: 15000
          });
        } catch (error) {
          // 忽略跳到 about:blank 的失败，继续自愈
        }
        await sleep(800);
      }
      await gotoWithTolerance(page, reusableCreationUrl, { label: '默认创作工作区' });
      const afterGotoState = await inspectVideoGenerationPage(page).catch(() => null);
      const afterGotoReusable =
        afterGotoState?.url?.includes('/ai-tool/generate') &&
        afterGotoState?.url?.includes('workspace=0');
      if (afterGotoReusable) {
        console.log('  ♻️ 已直达默认创作工作区，避免新建会话');
        if (!afterGotoState?.toolbarReady) {
          const reuseResult = await preferReusableCreationSession(page, { requireReady: false });
          if (reuseResult.clicked) {
            const reuseReady = await waitForVideoGenerationPageReady(page).then(
              () => true,
              error => {
                console.log(`  ⚠️ 直达后默认创作会话仍未出现生成控件，强制重进生成页: ${error.message}`);
                return false;
              }
            );
            if (reuseReady) {
              await ensureCleanReusableCreation('goto-reuse');
              return;
            }
            await forceOpenReusableCreationUrl('goto-reuse-force', { blankFirst: true });
            await ensureCleanReusableCreation('goto-reuse-force');
            return;
          }
        }
      } else if (afterGotoState?.creationHubDetected) {
        await gotoWithTolerance(page, reusableCreationUrl, { label: '默认创作工作区兜底' });
      }
      await ensureCleanReusableCreation('goto');
      return;
    } catch (error) {
      lastError = error;
      if (!isRecoverableVideoGenerationPageError(error) || attempt >= 3) {
        throw error;
      }
      console.log(`  ⚠️ 生成页健康检查失败，执行自愈重试: ${error.message}`);
      await sleep(1500);
    }
  }

  throw lastError || new Error('重置视频生成页面失败');
}

async function getActiveFileInputs(page) {
  const handles = await page.$$('input[type="file"]');
  if (handles.length === 0) {
    return [];
  }

  const metas = await safePageEvaluate(page, '读取文件输入框状态', () => {
    return Array.from(document.querySelectorAll('input[type="file"]')).map(input => {
      let parent = input.parentElement;
      const chain = [];
      let steps = 0;

      while (parent && steps < 6) {
        const text = (parent.textContent || '').replace(/\s+/g, ' ').trim();
        const className = String(parent.className || '');
        chain.push({ text, className });
        parent = parent.parentElement;
        steps++;
      }

      const collapsed = chain.some(item => item.className.includes('collapsed'));
      const label =
        chain.find(item => /(参考内容|首帧|尾帧)/.test(item.text))?.text ||
        chain[0]?.text ||
        '';

      return {
        collapsed,
        label,
        accept: input.accept,
        multiple: input.multiple
      };
    });
  });

  const inputs = [];

  const count = Math.min(handles.length, metas.length);
  for (let index = 0; index < count; index++) {
    const handle = handles[index];
    const meta = metas[index];

    if (!meta.collapsed) {
      inputs.push({ handle, ...meta });
    }
  }

  const labelOrder = ['参考内容', '首帧', '尾帧'];
  inputs.sort((a, b) => {
    const aIndex = labelOrder.findIndex(label => a.label.includes(label));
    const bIndex = labelOrder.findIndex(label => b.label.includes(label));
    const safeA = aIndex === -1 ? 99 : aIndex;
    const safeB = bIndex === -1 ? 99 : bIndex;
    return safeA - safeB;
  });

  return inputs;
}

async function getVisibleToolbarState(page) {
  const comboboxes = await getVisibleElementHandles(page, 'div[role="combobox"]', 1200);
  const buttons = await getVisibleElementHandles(page, 'button', 1200);
  const ratioButton = buttons.find(btn => /^\d+:\d+$/.test(btn.text));
  const generationButton = buttons.find(btn => btn.text.includes('视频生成'));
  const modelButton = buttons.find(btn => /Seedance|视频\s*3/.test(btn.text));
  const modeButton = buttons.find(btn => /参考|首尾帧|全能|智能|主体/.test(btn.text));
  const durationButton = buttons.find(btn => /^\d+s$/.test(btn.text));
  const toolbarControls = await safePageEvaluate(page, '读取底部工具栏控件', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0 &&
        rect.top >= Math.max(0, window.innerHeight - 180) &&
        rect.top < window.innerHeight
      );
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    return Array.from(document.querySelectorAll('[role="combobox"], button, [role="button"], div, span'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        return {
          text: normalize(el.innerText || el.textContent),
          area: rect.width * rect.height,
          top: rect.top,
          left: rect.left
        };
      })
      .filter(item => item.text && item.text.length <= 80)
      .filter(item => /视频生成|Seedance|参考|首尾帧|全能|智能|主体|^\d+:\d+$|^\d+s$/.test(item.text))
      .sort((a, b) => a.area - b.area || a.left - b.left || a.top - b.top);
  }).catch(() => []);
  const fallbackControls = await safePageEvaluate(page, '读取新版创作控件', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
            rect.width > 0 &&
            rect.height > 0 &&
            rect.top >= 250 &&
            rect.top < window.innerHeight - 8
      );
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    return Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        return {
          text: normalize(el.innerText || el.textContent),
          area: rect.width * rect.height,
          top: rect.top,
          left: rect.left
        };
      })
      .filter(item => item.text && item.text.length <= 40)
      .filter(item => /视频生成|Seedance|参考|首尾帧|全能|智能|主体|^\d+:\d+$|^\d+s$/.test(item.text))
      .sort((a, b) => a.area - b.area || a.top - b.top || a.left - b.left);
  }).catch(() => []);
  const findFallback = predicate => fallbackControls.find(item => predicate(item.text))?.text || '';
  const findToolbar = predicate => toolbarControls.find(item => predicate(item.text))?.text || '';
  const findCombobox = predicate => comboboxes.find(item => predicate(item.text))?.text || '';

  return {
    generationType: findToolbar(text => text.includes('视频生成')) || findFallback(text => text.includes('视频生成')) || findCombobox(text => text.includes('视频生成')) || generationButton?.text || '',
    model: findToolbar(text => /Seedance|视频\s*3/.test(text)) || findFallback(text => /Seedance|视频\s*3/.test(text)) || findCombobox(text => /Seedance|视频\s*3/.test(text)) || modelButton?.text || '',
    mode: findToolbar(text => /参考|首尾帧|全能|智能|主体/.test(text)) || findFallback(text => /参考|首尾帧|全能|智能|主体/.test(text)) || findCombobox(text => /参考|首尾帧|全能|智能|主体/.test(text)) || modeButton?.text || '',
    duration: findToolbar(text => /^\d+s$/.test(text)) || findCombobox(text => /^\d+s$/.test(text)) || durationButton?.text || findFallback(text => /^\d+s$/.test(text)) || '',
    ratio: findToolbar(text => /^\d+:\d+$/.test(text)) || ratioButton?.text || findFallback(text => /^\d+:\d+$/.test(text))
  };
}

async function getReferenceInputLabels(page) {
  const inputs = await getActiveFileInputs(page);
  const labels = inputs
    .map(item => item.label || '')
    .filter(Boolean);
  if (labels.length === 0 && inputs.length > 0) {
    return ['可用上传输入框'];
  }
  return labels;
}

async function getOptionTexts(page, selector, label = '读取下拉选项') {
  return await safePageEvaluate(page, label, querySelector => {
    return Array.from(document.querySelectorAll(querySelector)).map(el =>
      (el.textContent || '').replace(/\s+/g, ' ').trim()
    );
  }, selector);
}

async function getVisibleNoticeTexts(page) {
  return await safePageEvaluate(page, '读取页面提示', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
    };

    const selectors = [
      '[role="alert"]',
      '[role="dialog"]',
      '[class*="toast"]',
      '[class*="message"]',
      '[class*="notice"]',
      '[class*="warning"]',
      '[class*="error"]',
      '[class*="modal"]'
    ];

    return Array.from(document.querySelectorAll(selectors.join(',')))
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean)
      .slice(0, 10);
  });
}

function normalizeModelVariantName(value) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';

  const lower = text.toLowerCase();
  if (lower.includes('seedance 2.0 fast vip')) {
    return 'Seedance 2.0 Fast VIP';
  }
  if (lower.includes('seedance 2.0 vip')) {
    return 'Seedance 2.0 VIP';
  }
  if (lower.includes('seedance 2.0 fast')) {
    return 'Seedance 2.0 Fast';
  }
  if (lower.includes('seedance 2.0')) {
    return 'Seedance 2.0';
  }

  const videoModel = text.match(/视频\s*\d+(?:\.\d)?(?:\s*(?:Pro|Fast))?/i)?.[0];
  if (videoModel) {
    return videoModel.replace(/\s+/g, ' ').trim();
  }

  return text;
}

function isModelMatch(actual, expected) {
  return normalizeModelVariantName(actual) === normalizeModelVariantName(expected);
}

function isModeMatch(actual, expected) {
  return actual.includes(expected);
}

function isReferenceModeEquivalent(actual, expected) {
  const actualText = String(actual || '');
  const expectedText = String(expected || '');
  return expectedText.includes('全能参考') && actualText.includes('参考内容');
}

function isReferenceInputModeMatch(labels, expectedMode) {
  if (!labels || labels.length === 0) {
    return true;
  }
  const joined = (labels || []).join(' ');
  if (!expectedMode) return true;
  if (expectedMode.includes('首尾帧')) {
    return joined.includes('首帧') || joined.includes('尾帧');
  }
  if (expectedMode.includes('全能参考')) {
    return joined.includes('参考内容') || joined.includes('可用上传输入框');
  }
  return true;
}

function isDurationMatch(actual, expected) {
  return actual.includes(`${expected}s`);
}

function isRatioMatch(actual, expected) {
  return actual === expected;
}

async function getUploadAreaPreviewState(inputHandle) {
  if (!inputHandle) {
    return {
      found: false,
      previewCount: 0,
      previewSources: [],
      label: '',
      containerText: '',
      containerClass: ''
    };
  }

  try {
    return await safeHandleEvaluate(inputHandle, '读取上传区域预览', input => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
      };

      const isPreviewImage = img => {
        if (!isVisible(img)) return false;
        const src = img.src || '';
        if (!src) return false;
        const rect = img.getBoundingClientRect();
        if (rect.width < 24 || rect.height < 24) return false;
        return src.startsWith('blob:') ||
          src.startsWith('data:') ||
          /\.(png|jpg|jpeg|webp|gif|bmp|svg)(\?|$)/i.test(src);
      };

      const ancestors = [];
      let node = input.parentElement;
      let depth = 0;
      while (node && node !== document.body && depth < 8) {
        ancestors.push(node);
        node = node.parentElement;
        depth++;
      }

      const candidates = ancestors.map((candidate, index) => {
        const text = (candidate.textContent || '').replace(/\s+/g, ' ').trim();
        const className = String(candidate.className || '');
        const images = Array.from(candidate.querySelectorAll('img')).filter(isPreviewImage);
        const rect = candidate.getBoundingClientRect();
        const inBottomComposer =
          rect.left >= 520 &&
          rect.top >= Math.max(700, window.innerHeight - 300) &&
          rect.bottom <= window.innerHeight + 40;
        let score = 0;

        if (/(参考内容|首帧|尾帧)/.test(text)) score += 6;
        if (/(upload|preview|reference|image|frame)/i.test(className)) score += 4;
        if (images.length > 0) score += 3;
        if (rect.width > 0 && rect.width < window.innerWidth * 0.9) score += 1;
        if (inBottomComposer) score += 10;
        if (rect.top < 650 || rect.left < 500) score -= 8;
        score -= index * 0.2;

        return {
          candidate,
          text,
          className,
          images,
          score
        };
      });

      const best = candidates.sort((a, b) => b.score - a.score)[0];
      const container = best?.candidate || input.parentElement || input;
      const localImages = Array.from(container.querySelectorAll('img')).filter(isPreviewImage);
      const composerImages = Array.from(document.querySelectorAll('img'))
        .filter(img => {
          if (!isVisible(img) || !(img.src || '')) return false;
          const rect = img.getBoundingClientRect();
          return rect.width >= 24 && rect.height >= 24;
        })
        .filter(img => {
          const rect = img.getBoundingClientRect();
          return (
            rect.left >= 520 &&
            rect.left <= window.innerWidth - 120 &&
            rect.top >= Math.max(600, window.innerHeight - 380) &&
            rect.top <= window.innerHeight - 20
          );
        });
      const images = [...localImages, ...composerImages]
        .filter((img, index, arr) => arr.indexOf(img) === index);
      const label =
        candidates.find(item => /(参考内容|首帧|尾帧)/.test(item.text))?.text ||
        best?.text ||
        '';

      return {
        found: true,
        previewCount: images.length,
        previewSources: images.map(img => img.src || '').filter(Boolean).slice(0, 20),
        label,
        containerText: (container.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200),
        containerClass: String(container.className || '')
      };
    });
  } catch (error) {
    return {
      found: false,
      previewCount: 0,
      previewSources: [],
      label: '',
      containerText: '',
      containerClass: ''
    };
  }
}

async function countUploadedReferencePreviews(page) {
  const inputs = await getActiveFileInputs(page);
  if (inputs.length === 0) {
    return 0;
  }

  const sourceSet = new Set();
  for (const input of inputs) {
    const state = await getUploadAreaPreviewState(input.handle);
    for (const src of state.previewSources || []) {
      sourceSet.add(src);
    }
  }

  return sourceSet.size;
}

async function getFileInputSelectionState(inputHandle) {
  if (!inputHandle) {
    return {
      count: 0,
      names: [],
      value: ''
    };
  }

  try {
    return await safeHandleEvaluate(inputHandle, '读取文件输入状态', input => {
      const files = Array.from(input.files || []);
      return {
        count: files.length,
        names: files.map(file => file.name || ''),
        value: input.value || ''
      };
    });
  } catch (error) {
    return {
      count: 0,
      names: [],
      value: ''
    };
  }
}

function buildInputSelectionSignature(state) {
  const safeState = state || { count: 0, names: [], value: '' };
  return `${safeState.count}|${(safeState.names || []).join(',')}|${safeState.value || ''}`;
}

async function waitForReferencePreviewCount(page, expectedCount, timeout = 30000) {
  const start = Date.now();
  let lastCount = 0;

  while (Date.now() - start < timeout) {
    const currentCount = await countUploadedReferencePreviews(page);
    if (currentCount >= expectedCount) {
      return currentCount;
    }
    if (currentCount !== lastCount) {
      console.log(`    等待预览: ${currentCount}/${expectedCount}`);
      lastCount = currentCount;
    }
    await sleep(1000);
  }

  const finalCount = await countUploadedReferencePreviews(page);
  console.log(`    等待超时，最终预览数: ${finalCount}/${expectedCount}`);
  return finalCount;
}

async function waitForReferenceUploadEvidence(page, inputHandle, expectedPreviewCount, previousInputState, previousAreaState, timeout = 15000) {
  const start = Date.now();
  const previousSignature = buildInputSelectionSignature(previousInputState);
  const previousAreaSignature = JSON.stringify((previousAreaState?.previewSources || []).slice(0, 20));
  let lastPreviewCount = -1;
  let lastInputSignature = previousSignature;

  while (Date.now() - start < timeout) {
    const [areaState, inputState] = await Promise.all([
      getUploadAreaPreviewState(inputHandle),
      getFileInputSelectionState(inputHandle)
    ]);
    const previewCount = areaState.previewCount || 0;

    const inputSignature = buildInputSelectionSignature(inputState);
    if (previewCount >= expectedPreviewCount) {
      return {
        success: true,
        via: 'preview',
        previewCount,
        inputState,
        areaState
      };
    }

    const currentAreaSignature = JSON.stringify((areaState.previewSources || []).slice(0, 20));
    if (currentAreaSignature !== previousAreaSignature && previewCount > 0) {
      return {
        success: true,
        via: 'preview-signature',
        previewCount,
        inputState,
        areaState
      };
    }

    if (inputState.count > 0 && inputSignature !== previousSignature) {
      return {
        success: true,
        via: 'input-state',
        previewCount,
        inputState,
        areaState
      };
    }

    if (previewCount !== lastPreviewCount) {
      console.log(`    等待预览: ${previewCount}/${expectedPreviewCount}`);
      lastPreviewCount = previewCount;
    }

    if (inputSignature !== lastInputSignature) {
      console.log(`    检测到文件输入状态变化: ${inputState.names.join(', ') || inputState.value || '已选择文件'}`);
      lastInputSignature = inputSignature;
    }

    await sleep(800);
  }

  const [areaState, inputState] = await Promise.all([
    getUploadAreaPreviewState(inputHandle),
    getFileInputSelectionState(inputHandle)
  ]);
  const previewCount = areaState.previewCount || 0;

  return {
    success: false,
    previewCount,
    inputState,
    areaState
  };
}

async function getPromptInputState(page) {
  const inputs = await getVisibleElementHandles(
    page,
    'textarea, div[contenteditable="true"][role="textbox"], div[contenteditable="true"], [role="textbox"]',
    1200
  );
  const inputBox = inputs[0]?.handle || null;

  if (!inputBox) {
    return { found: false, length: 0, preview: '' };
  }

  const content = await safeHandleEvaluate(inputBox, '读取提示词输入框', el =>
    ('value' in el ? el.value : el.textContent) || ''
  );

  return {
    found: true,
    length: content.length,
    preview: content.slice(0, 120)
  };
}

function describeGenerateButtonCandidate(candidate) {
  if (!candidate || typeof candidate !== 'object') {
    return 'unknown';
  }
  const label = String(candidate.text || candidate.ariaLabel || candidate.title || '').replace(/\s+/g, ' ').trim();
  const parts = [
    `${candidate.tagName || 'node'}@(${Math.round(candidate.x || 0)},${Math.round(candidate.y || 0)})`
  ];
  if (candidate.disabled) {
    parts.push('disabled');
  }
  if (candidate.className) {
    parts.push(String(candidate.className).slice(0, 40));
  }
  if (label) {
    parts.push(label.slice(0, 60));
  }
  return parts.join(' | ');
}

async function getGenerateButtonCandidates(page) {
  return await safePageEvaluate(page, '读取生成按钮候选', () => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
    };

    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    const promptInput = Array.from(
      document.querySelectorAll('textarea, div[contenteditable="true"][role="textbox"], div[contenteditable="true"], [role="textbox"]')
    ).find(isVisible);
    const promptRect = promptInput?.getBoundingClientRect?.() || null;
    const composerTop = promptRect ? Math.max(0, promptRect.top - 120) : viewportHeight * 0.62;
    const composerLeft = promptRect ? Math.max(0, promptRect.left - 80) : viewportWidth * 0.2;
    const composerRight = promptRect ? Math.min(viewportWidth, promptRect.right + 120) : viewportWidth;

    const denyPhrases = [
      '回到底部',
      '重新编辑',
      '再次生成',
      '反馈',
      '图片生成',
      '智能美学',
      '立即想象',
      '即刻想象',
      '详细信息',
      '生成类型',
      '操作类型',
      '高峰期',
      '取消生成'
    ];

    const candidates = Array.from(document.querySelectorAll('button, [role="button"], a'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        const text = normalize(el.textContent);
        const ariaLabel = normalize(el.getAttribute('aria-label'));
        const title = normalize(el.getAttribute('title'));
        const className = String(el.className || '');
        const combined = `${text} ${ariaLabel} ${title}`.trim();
        const submitClass = className.includes('submit-button');
        const primaryClass = className.includes('primary');
        const hasGenerateIntent =
          combined.includes('生成') ||
          combined.includes('提交');
        const iconOnly = !combined;
        const disabled = Boolean(
          el.disabled ||
          el.getAttribute('aria-disabled') === 'true' ||
          className.includes('disabled')
        );
        const insideComposer =
          rect.top >= composerTop &&
          rect.left >= composerLeft &&
          rect.right <= composerRight &&
          rect.bottom <= viewportHeight;
        const nearBottomRight =
          rect.top >= viewportHeight * 0.68 &&
          rect.left >= viewportWidth * 0.72;
        const denied = denyPhrases.some(phrase => combined.includes(phrase));

        let score = 0;
        if (className.includes('submit-button')) score += 120;
        if (className.includes('primary')) score += 40;
        if (text === '生成' || text === '立即生成' || text === '开始生成') score += 100;
        if (combined.includes('生成视频')) score += 90;
        if (combined.includes('立即生成') || combined.includes('开始生成')) score += 80;
        if (combined.includes('提交')) score += 50;
        if (combined.includes('生成')) score += 40;
        if (insideComposer) score += 140;
        if (nearBottomRight) score += 80;
        if (rect.x > viewportWidth * 0.55) score += 35;
        if (rect.y > viewportHeight * 0.35) score += 20;
        if (String(el.tagName || '').toLowerCase() === 'button') score += 10;
        if (disabled) score -= 15;
        if (denied) score -= 260;
        if (/^生成\s*\d+$/.test(text)) score -= 200;
        if (combined.includes('再次生成')) score -= 30;

        return {
          text,
          ariaLabel,
          title,
          combined,
          className,
          submitClass,
          primaryClass,
          hasGenerateIntent,
          iconOnly,
          disabled,
          x: rect.x + rect.width / 2,
          y: rect.y + rect.height / 2,
          width: rect.width,
          height: rect.height,
          left: rect.left,
          top: rect.top,
          right: rect.right,
          bottom: rect.bottom,
          tagName: String(el.tagName || '').toLowerCase(),
          insideComposer,
          nearBottomRight,
          denied,
          score
        };
      })
      .filter(item => {
        if (item.denied) return false;
        if (item.width < 28 || item.height < 28) return false;
        if (!item.submitClass && !item.insideComposer && !item.nearBottomRight) {
          return false;
        }
        if (item.iconOnly && !item.submitClass) {
          return false;
        }
        if (!item.submitClass && !item.primaryClass && !item.hasGenerateIntent) {
          return false;
        }
        if (/^生成\s*\d+$/.test(item.text)) return false;
        if (
          item.score < 30 &&
          !item.combined.includes('生成') &&
          !item.combined.includes('提交') &&
          !item.insideComposer &&
          !item.nearBottomRight &&
          !item.className.includes('submit-button')
        ) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.score - a.score || a.top - b.top || a.left - b.left)
      .slice(0, 8);

    return candidates;
  });
}

async function getGenerateButtonState(page) {
  const candidates = await getGenerateButtonCandidates(page);
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return { found: false, candidates: [] };
  }

  const primary = candidates[0];
  return {
    found: true,
    disabled: Boolean(primary.disabled),
    text: String(primary.text || primary.ariaLabel || primary.title || ''),
    className: String(primary.className || ''),
    candidates
  };
}

async function writeTaskSnapshot(page, task, details = {}) {
  const status = details.status || 'failed';
  const statusFileName = details.statusFileName || '.failed';
  const statusFile = path.join(task.folder, statusFileName);
  const timestamp = formatBeijingTimestamp();
  const safeTimestamp = timestamp.replace(/[+:.]/g, '-');
  const screenshotPath = path.join(task.folder, `.${status}-${safeTimestamp}.png`);

  let screenshotSaved = false;
  try {
    await page.screenshot({ path: screenshotPath, fullPage: false });
    screenshotSaved = true;
  } catch (error) {
    console.log(`  ⚠️ 截图保存失败: ${error.message}`);
  }

  const toolbarState = await getVisibleToolbarState(page);
  const promptState = await getPromptInputState(page);
  const previewCount = await countUploadedReferencePreviews(page);
  const buttonState = await getGenerateButtonState(page);
  const referenceInputLabels = await getReferenceInputLabels(page);
  const visibleNotices = await getVisibleNoticeTexts(page);

  const payload = {
    status,
    capturedAt: timestamp,
    reason: details.reason || '任务执行失败',
    code: details.code || 'task_failed',
    taskName: task.name,
    taskFolder: task.folder,
    pageUrl: page.url(),
    expected: {
      model: details.options?.model || '',
      mode: details.options?.mode || '',
      ratio: details.options?.ratio || '',
      duration: details.options?.duration || '',
      promptLength: task.prompt?.length || 0,
      imageCount: task.images?.length || 0
    },
    observed: {
      generationType: toolbarState.generationType,
      model: toolbarState.model,
      mode: toolbarState.mode,
      ratio: toolbarState.ratio,
      duration: toolbarState.duration,
      uploadAreas: referenceInputLabels,
      promptFound: promptState.found,
      promptLength: promptState.length,
      promptPreview: promptState.preview,
      uploadedPreviewCount: previewCount,
      generateButton: buttonState,
      recentNotices: visibleNotices
    },
    screenshot: screenshotSaved ? screenshotPath : null
  };

  fs.writeFileSync(statusFile, JSON.stringify(payload, null, 2));
  console.log(`  🧾 已写入${status === 'blocked' ? '阻塞' : '失败'}快照: ${statusFile}`);
  if (screenshotSaved) {
    console.log(`  📸 已保存截图: ${screenshotPath}`);
  }

  return payload;
}

async function markTaskBlocked(page, task, details = {}) {
  const statusFile = path.join(task.folder, '.blocked');
  const submittedFile = path.join(task.folder, '.submitted');
  const completedFile = path.join(task.folder, '.completed');
  const timestamp = formatBeijingTimestamp();
  const safeTimestamp = timestamp.replace(/[+:.]/g, '-');
  const screenshotPath = path.join(task.folder, `.blocked-${safeTimestamp}.png`);

  if (fs.existsSync(submittedFile)) {
    fs.unlinkSync(submittedFile);
  }
  if (fs.existsSync(completedFile)) {
    fs.unlinkSync(completedFile);
  }

  const payload = await writeTaskSnapshot(page, task, {
    ...details,
    status: 'blocked',
    statusFileName: '.blocked'
  });

  payload.blockedAt = timestamp;
  payload.nextAction = '请人工检查即梦页面；处理完成后删除该任务目录下的 .blocked 文件，再重新运行脚本。';
  payload.reason = details.reason || payload.reason || '生成按钮为灰色，需人工处理';
  payload.code = details.code || payload.code || 'button_disabled';
  fs.writeFileSync(statusFile, JSON.stringify(payload, null, 2));
  console.log(`  🚫 已写入阻塞标记: ${statusFile}`);

  return payload;
}

async function markTaskFailed(page, task, details = {}) {
  return writeTaskSnapshot(page, task, {
    ...details,
    status: 'failed',
    statusFileName: '.failed'
  });
}

async function ensureSettingsMatch(page, options, maxAttempts = 2) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`  🔍 配置校验尝试 ${attempt}/${maxAttempts}`);

    const settingsOk = await verifySettings(page, options);
    if (settingsOk) {
      return true;
    }

    if (attempt === maxAttempts) {
      break;
    }

    console.log('  ↻ 当前页面参数与配置不一致，重新设置...');
    await selectModel(page, options.model);
    await sleep(500);
    await selectMode(page, options.mode);
    await sleep(500);
    await selectRatio(page, options.ratio);
    await sleep(500);
    await selectDuration(page, options.duration);
    await sleep(500);
  }

  return false;
}

async function applyTaskSettings(page, taskOptions) {
  await selectModel(page, taskOptions.model);
  await sleep(500);

  await selectMode(page, taskOptions.mode);
  await sleep(500);

  await selectRatio(page, taskOptions.ratio);
  await sleep(500);

  await selectDuration(page, taskOptions.duration);
  await sleep(500);
}

/**
 * 选择模型 - 基于实际页面结构
 * combobox[包含 "Seedance" 或 "视频"] → listbox → option
 */
async function selectModel(page, modelName) {
  console.log(`  选择模型: ${modelName}`);

  const currentState = await getVisibleToolbarState(page);
  if (isModelMatch(currentState.model, modelName)) {
    console.log(`  ✅ 模型已正确: ${currentState.model}`);
    return true;
  }
  
  // 查找模型下拉框 (combobox 包含模型名称)
  const comboboxes = await getVisibleElementHandles(page, 'div[role="combobox"]', 1200);
  let modelCombobox = null;
  
  for (const box of comboboxes) {
    const text = box.text;
    // 模型下拉框包含 Seedance 或 视频 字样
    if (text.match(/Seedance|视频\s*3/)) {
      modelCombobox = box.handle;
      console.log(`  找到模型下拉框: ${text.substring(0, 30)}...`);
      break;
    }
  }
  
  if (!modelCombobox) {
    console.log('  ⚠️ 未找到模型下拉框');
    return false;
  }
  
  // 滚动到元素可见
  await safeHandleEvaluate(modelCombobox, '滚动到模型下拉框', el => el.scrollIntoView({ behavior: 'smooth', block: 'center' }));
  await sleep(300);
  
  // 检查是否已经有 listbox 打开
  let listbox = await page.$('div[role="listbox"]');
  if (!listbox) {
    // 点击打开下拉框
    await modelCombobox.click();
    await sleep(1000);
    
    // 等待 listbox 出现
    listbox = await page.waitForSelector('div[role="listbox"]', { timeout: 3000 }).catch(() => null);
    if (!listbox) {
      console.log('  ⚠️ listbox 未出现');
      return false;
    }
  } else {
    console.log('  listbox 已打开');
  }
  
  // 查找选项 - 支持 div 和 li 两种元素
  let optionSelector = 'div[role="option"], li[role="option"]';
  let options = await page.$$(optionSelector);
  console.log(`  找到 ${options.length} 个选项 (div + li)`);
  
  if (options.length === 0) {
    // 尝试其他选择器
    optionSelector = 'div[role="listbox"] > li, li[role="option"]';
    options = await page.$$(optionSelector);
    console.log(`  li[role="option"] 找到 ${options.length} 个`);
  }
  
  if (options.length === 0) {
    // 尝试查找包含选项文本的元素
    const listbox = await page.$('div[role="listbox"]');
    if (listbox) {
      const listboxContent = await listbox.evaluate(el => el.innerHTML);
      console.log(`  listbox 内容片段: ${listboxContent.substring(0, 200)}...`);
    }
    await page.keyboard.press('Escape');
    console.log('  ⚠️ 未找到选项');
    return false;
  }
  
  // 打印所有选项
  console.log('  所有选项:');
  const optionTexts = await getOptionTexts(page, optionSelector, '读取模型选项文本');
  for (let i = 0; i < Math.min(options.length, optionTexts.length); i++) {
    const text = optionTexts[i] || '';
    console.log(`    [${i}] ${text.substring(0, 50)}...`);
  }
  
  // 匹配选项 - 精确匹配模型名称
  for (let index = 0; index < Math.min(options.length, optionTexts.length); index++) {
    const option = options[index];
    const text = optionTexts[index] || '';
    if (!text) continue;
    
    // 提取选项中的模型名称（第一个空格或中文之前的部分）
    // 例如："Seedance 2.0全能王者..." → "Seedance 2.0"
    // 例如："Seedance 2.0 Fast高性价比..." → "Seedance 2.0 Fast"
    const optionModel = normalizeModelVariantName(text);
    
    // 精确匹配
    if (isModelMatch(optionModel, modelName)) {
      console.log(`  匹配到: ${text.substring(0, 50)}...`);
      await option.click();
      await sleep(500);
      
      // 验证选择是否生效
      const selectedState = await getVisibleToolbarState(page);
      const selectedModel = selectedState.model;
      console.log(`  选择后显示: ${selectedModel}`);
      
      if (isModelMatch(selectedModel, modelName)) {
        console.log(`  ✅ 已选择模型: ${modelName}`);
        return true;
      } else {
        console.log(`  ⚠️ 选择未生效，重试...`);
        // 关闭下拉框重新打开
        await page.keyboard.press('Escape');
        await sleep(300);
        await modelCombobox.click();
        await sleep(1000);
        
        // 再次点击选项
        const retrySelector = 'div[role="option"], li[role="option"]';
        const retryOptions = await page.$$(retrySelector);
        const retryTexts = await getOptionTexts(page, retrySelector, '读取重试模型选项文本');
        for (let retryIndex = 0; retryIndex < Math.min(retryOptions.length, retryTexts.length); retryIndex++) {
          const opt = retryOptions[retryIndex];
          const optText = retryTexts[retryIndex] || '';
          const optModel = normalizeModelVariantName(optText);
          if (isModelMatch(optModel, optionModel)) {
            await opt.click();
            await sleep(500);
            break;
          }
        }
        
        // 再次验证
        const finalState = await getVisibleToolbarState(page);
        const finalModel = finalState.model;
        if (isModelMatch(finalModel, optionModel)) {
          console.log(`  ✅ 重试成功: ${optionModel}`);
          return true;
        }
        
        return false;
      }
    }
  }
  
  await page.keyboard.press('Escape');
  console.log(`  ⚠️ 未找到模型选项: ${modelName}`);
  return false;
}

/**
 * 选择参考模式 - 基于实际页面结构
 * combobox[包含 "参考" 或 "首尾帧"] → listbox → option
 */
async function selectMode(page, modeName) {
  console.log(`  选择参考模式: ${modeName}`);

  const currentState = await getVisibleToolbarState(page);
  if (isModeMatch(currentState.mode, modeName) || isReferenceModeEquivalent(currentState.mode, modeName)) {
    console.log(`  ✅ 参考模式已正确: ${currentState.mode}`);
    return true;
  }
  
  const comboboxes = await getVisibleElementHandles(page, 'div[role="combobox"]', 1200);
  let modeCombobox = null;
  
  for (const box of comboboxes) {
    const text = box.text;
    // 参考模式下拉框包含 "参考"、"首尾帧"、"全能" 等字样
    if (text.includes('参考') || text.includes('首尾帧') || text.includes('全能') || text.includes('智能') || text.includes('主体')) {
      modeCombobox = box.handle;
      break;
    }
  }
  
  if (!modeCombobox) {
    console.log('  ⚠️ 未找到参考模式下拉框');
    return false;
  }
  
  await modeCombobox.click();
  await sleep(1000);
  
  const listbox = await page.waitForSelector('div[role="listbox"]', { timeout: 5000 }).catch(() => null);
  if (!listbox) {
    console.log('  ⚠️ 下拉列表未出现');
    return false;
  }
  
  const optionSelector = 'div[role="option"], li[role="option"]';
  const options = await listbox.$$(optionSelector);
  console.log(`  找到 ${options.length} 个参考模式选项`);
  
  const optionTexts = await getOptionTexts(page, optionSelector, '读取参考模式选项文本');
  for (let index = 0; index < Math.min(options.length, optionTexts.length); index++) {
    const option = options[index];
    const text = optionTexts[index] || '';
    if (text.includes(modeName)) {
      console.log(`  匹配到: ${text}`);
      await option.click();
      await sleep(500);
      const state = await getVisibleToolbarState(page);
      if (isModeMatch(state.mode, modeName)) {
        console.log(`  ✅ 已选择参考模式: ${modeName}`);
        return true;
      }
      break;
    }
  }
  
  await page.keyboard.press('Escape');
  console.log(`  ⚠️ 未找到参考模式选项: ${modeName}`);
  return false;
}

/**
 * 选择比例 - 基于实际页面结构
 * button["9:16" 等] → 点击 → 选择比例
 */
/**
 * 选择比例
 */
async function selectRatio(page, ratio) {
  console.log(`  选择比例: ${ratio}`);
  
  // 使用 page.evaluate 在浏览器上下文中操作
  const result = await page.evaluate((targetRatio) => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
    };

    // 查找当前显示的比例控件；新版默认创作里可能不是 button。
    const controls = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span'))
      .filter(el => {
        if (!isVisible(el)) return false;
        const rect = el.getBoundingClientRect();
        const text = String(el.textContent || '').replace(/\s+/g, ' ').trim();
        return rect.y >= 250 && rect.y < window.innerHeight - 8 && text.match(/^\d+:\d+$/);
      })
      .sort((a, b) => {
        const ar = a.getBoundingClientRect();
        const br = b.getBoundingClientRect();
        const targetY = window.innerHeight - 64;
        const aScore = Math.abs(ar.top - targetY) + Math.abs(ar.left - 1000);
        const bScore = Math.abs(br.top - targetY) + Math.abs(br.left - 1000);
        return aScore - bScore || (ar.width * ar.height) - (br.width * br.height);
      });
    let ratioButton = controls[0] || null;
    
    if (!ratioButton) {
      return { success: false, error: '未找到比例按钮' };
    }
    
    const currentRatio = String(ratioButton.textContent || '').replace(/\s+/g, ' ').trim();
    console.log('当前比例:', currentRatio);
    
    // 如果已经是目标比例，直接返回
    if (currentRatio === targetRatio) {
      return { success: true, already: true, current: currentRatio };
    }
    
    // 点击比例按钮打开弹窗
    const rect = ratioButton.getBoundingClientRect();
    const clickTarget = ratioButton.closest('button,[role="button"],[role="combobox"]') || ratioButton;
    clickTarget.click();
    
    return {
      success: true,
      clicked: true,
      current: currentRatio,
      anchor: {
        x: rect.left + rect.width / 2,
        y: rect.top + rect.height / 2,
        left: rect.left,
        right: rect.right,
        top: rect.top,
        bottom: rect.bottom
      }
    };
  }, ratio);
  
  if (!result.success) {
    console.log(`  ⚠️ ${result.error}`);
    return false;
  }
  
  if (result.already) {
    console.log(`  ✅ 比例已正确: ${result.current}`);
    return true;
  }
  
  // 等待弹窗出现
  await sleep(1500);
  
  // 在弹窗中选择目标比例
  const selectResult = await page.evaluate((targetRatio, anchor) => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const clickableFor = el => {
      let target = el;
      for (let depth = 0; depth < 6 && target?.parentElement; depth++) {
        const parent = target.parentElement;
        const role = parent.getAttribute('role') || '';
        const style = getComputedStyle(parent);
        const text = normalize(parent.innerText || parent.textContent);
        const rect = parent.getBoundingClientRect();
        if (
          ['option', 'menuitem', 'radio', 'button'].includes(role) ||
          parent.tagName === 'BUTTON' ||
          style.cursor === 'pointer' ||
          (text === targetRatio && rect.width >= 30 && rect.height >= 24)
        ) {
          target = parent;
          break;
        }
        target = parent;
      }
      return target;
    };

    const candidates = Array.from(document.querySelectorAll('button, [role="button"], [role="option"], [role="menuitem"], [role="radio"], div, span'))
      .filter(isVisible)
      .map(el => {
        const rect = el.getBoundingClientRect();
        const text = normalize(el.innerText || el.textContent);
        const clickTarget = clickableFor(el);
        const clickRect = clickTarget.getBoundingClientRect();
        return { el, clickTarget, rect, clickRect, text };
      })
      .filter(item => item.text === targetRatio)
      .filter(item => {
        const centerX = item.clickRect.left + item.clickRect.width / 2;
        const centerY = item.clickRect.top + item.clickRect.height / 2;
        const nearAnchor =
          Math.abs(centerX - anchor.x) <= 260 &&
          centerY >= anchor.bottom - 40 &&
          centerY <= anchor.bottom + 360;
        const inFloatingLayer =
          item.clickRect.top > 300 &&
          item.clickRect.top < window.innerHeight - 20 &&
          item.clickRect.width >= 24 &&
          item.clickRect.height >= 18 &&
          item.clickRect.left > 250;
        return nearAnchor || inFloatingLayer;
      })
      .sort((a, b) => {
        const acx = a.clickRect.left + a.clickRect.width / 2;
        const acy = a.clickRect.top + a.clickRect.height / 2;
        const bcx = b.clickRect.left + b.clickRect.width / 2;
        const bcy = b.clickRect.top + b.clickRect.height / 2;
        const aDist = Math.abs(acx - anchor.x) + Math.abs(acy - anchor.y);
        const bDist = Math.abs(bcx - anchor.x) + Math.abs(bcy - anchor.y);
        return aDist - bDist || (b.clickRect.width * b.clickRect.height) - (a.clickRect.width * a.clickRect.height);
      });

    const target = candidates[0];
    if (!target) {
      return { success: false, error: '未找到目标比例选项' };
    }

    target.clickTarget.click();
    return {
      success: true,
      clicked: targetRatio,
      rect: {
        left: Math.round(target.clickRect.left),
        top: Math.round(target.clickRect.top),
        width: Math.round(target.clickRect.width),
        height: Math.round(target.clickRect.height)
      },
      candidateCount: candidates.length
    };
  }, ratio, result.anchor);
  
  if (!selectResult.success) {
    console.log(`  ⚠️ ${selectResult.error}`);
    await page.keyboard.press('Escape');
    return false;
  }
  
  let verifyResult = '';
  for (let index = 0; index < 6; index++) {
    await sleep(500);
    verifyResult = (await getVisibleToolbarState(page)).ratio;
    if (verifyResult === ratio) {
      break;
    }
  }
  
  if (verifyResult === ratio) {
    console.log(`  ✅ 已选择比例: ${ratio}`);
    return true;
  } else {
    console.log(`  ⚠️ 比例选择失败，当前: ${verifyResult}`);
    return false;
  }
}

/**
 * 选择时长 - 基于实际页面结构
 * combobox[包含 "s" 秒数] → listbox → option
 */
async function selectDuration(page, duration) {
  console.log(`  选择时长: ${duration}s`);

  const currentState = await getVisibleToolbarState(page);
  if (isDurationMatch(currentState.duration, duration)) {
    console.log(`  ✅ 时长已正确: ${currentState.duration}`);
    return true;
  }
  
  const comboboxes = await getVisibleElementHandles(page, 'div[role="combobox"]', 1200);
  let durationCombobox = null;
  
  for (const box of comboboxes) {
    const text = box.text;
    // 时长下拉框包含秒数，如 "4s", "10s", "15s"
    if (text.match(/^\d+s$/)) {
      durationCombobox = box.handle;
      break;
    }
  }
  
  if (!durationCombobox) {
    const clicked = await safePageEvaluate(page, '点击新版时长按钮', () => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
      };
      const buttons = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span'))
        .filter(isVisible)
        .map(el => ({ el, text: (el.textContent || '').replace(/\s+/g, ' ').trim(), rect: el.getBoundingClientRect() }))
        .filter(item => /^\d+s$/.test(item.text) && item.rect.top < 700)
        .sort((a, b) => (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height) || a.rect.top - b.rect.top || a.rect.left - b.rect.left);
      if (!buttons.length) return false;
      const target = buttons[0].el.closest('button,[role="button"],[role="combobox"]') || buttons[0].el;
      target.click();
      return true;
    }).catch(() => false);

    if (!clicked) {
      console.log('  ⚠️ 未找到时长下拉框');
      return false;
    }
    await sleep(1000);
  } else {
    await durationCombobox.click();
    await sleep(1000);
  }
  
  const listbox = await page.waitForSelector('div[role="listbox"]', { timeout: 5000 }).catch(() => null);
  const targetText = `${duration}s`;

  if (listbox) {
    const optionSelector = 'div[role="option"], li[role="option"]';
    const options = await listbox.$$(optionSelector);
    console.log(`  找到 ${options.length} 个时长选项`);

    for (let index = 0; index < options.length; index++) {
      const option = options[index];
      const text = await safeHandleEvaluate(option, '读取时长选项文本', el =>
        (el.textContent || '').replace(/\s+/g, ' ').trim()
      ).catch(() => '');
      if (text === targetText || text.includes(targetText)) {
        console.log(`  匹配到: ${text}`);
        for (let attempt = 0; attempt < 3; attempt++) {
          await safeHandleEvaluate(option, '滚动时长选项到视口', el => {
            el.scrollIntoView({ block: 'center', inline: 'nearest' });
          }).catch(() => {});
          const box = await option.boundingBox().catch(() => null);
          if (box && box.width > 0 && box.height > 0) {
            await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
          } else {
            await option.click().catch(async () => {
              await safeHandleEvaluate(option, '点击时长选项', el => {
                const target = el.closest('button,[role="option"],[role="menuitem"]') || el;
                target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window }));
                target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window }));
                target.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
              });
            });
          }
          await sleep(700);
          const state = await getVisibleToolbarState(page);
          if (isDurationMatch(state.duration, duration)) {
            console.log(`  ✅ 已选择时长: ${duration}s`);
            return true;
          }
        }
        break;
      }
    }
  } else {
    const clickedOption = await safePageEvaluate(page, '选择新版时长选项', target => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="option"], [role="menuitem"], div, span'))
        .filter(isVisible)
        .map(el => ({ el, text: normalize(el.textContent), rect: el.getBoundingClientRect() }))
        .filter(item => item.text === target && item.rect.top < window.innerHeight)
        .sort((a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left);
      if (!candidates.length) return false;
      const targetEl = candidates[0].el.closest('button,[role="option"],[role="menuitem"]') || candidates[0].el;
      targetEl.click();
      return true;
    }, targetText).catch(() => false);

    if (clickedOption) {
      await sleep(500);
      const state = await getVisibleToolbarState(page);
      if (isDurationMatch(state.duration, duration)) {
        console.log(`  ✅ 已选择时长: ${duration}s`);
        return true;
      }
    }
  }
  
  await page.keyboard.press('Escape');
  console.log(`  ⚠️ 未找到时长选项: ${duration}s`);
  return false;
}

/**
 * 上传参考素材（支持多图）
 */
async function uploadReference(page, imagePaths, modeName = '') {
  if (!imagePaths || imagePaths.length === 0) {
    return false;
  }
  
  console.log(`  上传参考图片: ${imagePaths.length} 张`);

  const uploadFileWithRetry = async (handle, filePaths, label, attempts = 2) => {
    let lastError = null;
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        if (attempt > 1) {
          console.log(`    ↻ ${label} 上传重试 ${attempt}/${attempts}`);
          await sleep(1200);
        }
        await handle.uploadFile(...filePaths);
        return true;
      } catch (error) {
        lastError = error;
        console.log(`    ⚠️ ${label} 上传触发失败: ${error.message}`);
        if (!isRecoverableProtocolEvaluateError(error) && attempt >= attempts) {
          break;
        }
      }
    }
    if (lastError) {
      console.log(`    ❌ ${label} 上传触发失败: ${lastError.message}`);
    }
    return false;
  };

  const uploadToLabel = async (label, imagePath) => {
    const inputs = await getActiveFileInputs(page);
    const target =
      inputs.find(item => item.label.includes(label)) ||
      inputs[0];

    if (!target) {
      console.log(`  ⚠️ 未找到 ${label || '参考图片'} 上传输入框`);
      return false;
    }

    console.log(`    找到上传区域: ${target.label.substring(0, 50)}...`);
    const beforeState = await getFileInputSelectionState(target.handle);
    const beforeAreaState = await getUploadAreaPreviewState(target.handle);
    const uploaded = await uploadFileWithRetry(target.handle, [imagePath], label || path.basename(imagePath));
    if (!uploaded) {
      return {
        success: false,
        handle: target.handle,
        label: target.label,
        beforeState,
        beforeAreaState
      };
    }
    await sleep(1500);
    return {
      success: true,
      handle: target.handle,
      label: target.label,
      beforeState,
      beforeAreaState
    };
  };

  if (modeName.includes('首尾帧')) {
    let uploadedCount = 0;
    const firstImage = imagePaths[0];
    if (fs.existsSync(firstImage)) {
      console.log(`    [首帧] ${path.basename(firstImage)}`);
      const uploadResult = await uploadToLabel('首帧', firstImage);
      if (uploadResult.success) {
        const evidence = await waitForReferenceUploadEvidence(page, uploadResult.handle, 1, uploadResult.beforeState, uploadResult.beforeAreaState);
        if (evidence.success) {
          uploadedCount++;
        } else {
          console.log('    ⚠️ 首帧上传未检测到成功证据');
        }
      }
    }

    if (imagePaths.length > 1) {
      const lastImage = imagePaths[imagePaths.length - 1];
      if (fs.existsSync(lastImage)) {
        console.log(`    [尾帧] ${path.basename(lastImage)}`);
        const uploadResult = await uploadToLabel('尾帧', lastImage);
        if (uploadResult.success) {
          const evidence = await waitForReferenceUploadEvidence(page, uploadResult.handle, 1, uploadResult.beforeState, uploadResult.beforeAreaState);
          if (evidence.success) {
            uploadedCount++;
          } else {
            console.log('    ⚠️ 尾帧上传未检测到成功证据');
          }
        }
      }
    }

    const requiredCount = Math.min(imagePaths.length, 2);
    if (uploadedCount >= requiredCount) {
      console.log(`  ✅ 已上传 ${uploadedCount} 张图片`);
      return true;
    }

    console.log(`  ❌ 首尾帧上传未完成 (${uploadedCount}/${requiredCount})`);
    return false;
  }

  const maxImages = 12;
  const uploadQueue = imagePaths.slice(0, maxImages);
  if (imagePaths.length > maxImages) {
    console.log(`  ⚠️ 全能参考最多上传 ${maxImages} 张，已截取前 ${maxImages} 张`);
  }

  const validUploadPaths = [];
  for (let i = 0; i < uploadQueue.length; i++) {
    const imagePath = uploadQueue[i];
    if (!fs.existsSync(imagePath)) {
      console.log(`    [${i + 1}] ⚠️ 文件不存在: ${imagePath}`);
      continue;
    }
    validUploadPaths.push(imagePath);
    console.log(`    [${i + 1}/${uploadQueue.length}] ${path.basename(imagePath)}`);
  }

  if (validUploadPaths.length !== uploadQueue.length) {
    console.log(`  ❌ 图片上传未完成 (${validUploadPaths.length}/${uploadQueue.length})`);
    return false;
  }

  const inputs = await getActiveFileInputs(page);
  const target =
    inputs.find(item => item.label.includes('参考内容')) ||
    inputs[0];

  if (!target) {
    console.log('  ⚠️ 未找到参考图片上传输入框');
    return false;
  }

  console.log(`    找到上传区域: ${target.label.substring(0, 50)}...`);
  const beforeState = await getFileInputSelectionState(target.handle);
  const beforeAreaState = await getUploadAreaPreviewState(target.handle);
  const beforePreviewCount = beforeAreaState?.previewCount || 0;
  console.log(`    当前上传区域预览数: ${beforePreviewCount}`);
  if (beforePreviewCount > 0) {
    console.log(`  ❌ 参考图片区已有 ${beforePreviewCount} 张残留预览，停止提交以避免串图`);
    return false;
  }
  const batchUploadTriggered = await uploadFileWithRetry(target.handle, validUploadPaths, '批量参考图');
  if (!batchUploadTriggered) {
    return false;
  }
  await sleep(1500);

  const expectedCount = validUploadPaths.length;
  const evidence = await waitForReferenceUploadEvidence(
    page,
    target.handle,
    expectedCount,
    beforeState,
    beforeAreaState,
    Math.max(45000, validUploadPaths.length * 15000)
  );
  console.log(`    上传后预览数: ${evidence.previewCount}/${expectedCount}`);

  if (!evidence.success && evidence.previewCount < validUploadPaths.length) {
    console.log('    ⚠️ 批量上传未完成，改用单张补传校验');
    let currentCount = evidence.previewCount || beforePreviewCount;
    for (let i = currentCount; i < validUploadPaths.length; i++) {
      const imagePath = validUploadPaths[i];
      const inputs = await getActiveFileInputs(page);
      const retryTarget =
        inputs.find(item => item.label.includes('参考内容')) ||
        inputs[0] ||
        target;
      if (!retryTarget?.handle) {
        break;
      }

      const areaState = await getUploadAreaPreviewState(retryTarget.handle);
      const inputState = await getFileInputSelectionState(retryTarget.handle);
      currentCount = areaState?.previewCount || currentCount || 0;
      console.log(`    [补传 ${i + 1}/${validUploadPaths.length}] ${path.basename(imagePath)}，当前预览 ${currentCount}/${validUploadPaths.length}`);
      const singleTriggered = await uploadFileWithRetry(retryTarget.handle, [imagePath], `单张参考图 ${i + 1}`, 2);
      if (!singleTriggered) {
        continue;
      }
      const singleEvidence = await waitForReferenceUploadEvidence(
        page,
        retryTarget.handle,
        Math.min(validUploadPaths.length, currentCount + 1),
        inputState,
        areaState,
        30000
      );
      currentCount = Math.max(currentCount, singleEvidence.previewCount || 0);
      if (currentCount >= validUploadPaths.length) {
        break;
      }
    }

    const finalCount = await waitForReferencePreviewCount(page, validUploadPaths.length, 10000);
    if (finalCount >= validUploadPaths.length) {
      console.log(`  ✅ 已上传 ${validUploadPaths.length} 张图片 (单张补传)`);
      return true;
    }

    console.log(`  ❌ 图片上传未完成 (${evidence.previewCount}/${validUploadPaths.length})`);
    return false;
  }

  const via =
    evidence.via === 'preview' ? '局部预览数' :
    evidence.via === 'preview-signature' ? '局部预览签名' :
    '文件输入状态';
  console.log(`  ✅ 已上传 ${validUploadPaths.length} 张图片 (${via})`);
  return true;
}

/**
 * 输入提示词
 */
async function inputPrompt(page, prompt) {
  console.log(`  输入提示词 (${prompt.length} 字符)...`);
  
  // 查找顶部可见的输入框 - 支持 textarea 和富文本编辑器
  const inputs = await getVisibleElementHandles(
    page,
    'textarea, div[contenteditable="true"][role="textbox"], div[contenteditable="true"], [role="textbox"]',
    1200
  );
  const inputBox = inputs[0]?.handle || null;
  
  if (!inputBox) {
    console.log('  ⚠️ 未找到输入框');
    return false;
  }
  
  await inputBox.click();
  await page.keyboard.down('Meta');
  await page.keyboard.press('A');
  await page.keyboard.up('Meta');
  await page.keyboard.press('Backspace');

  // Let the editor process smaller native input batches. Dispatching one large
  // synthetic input event can block JiMeng's page runtime and time out CDP.
  const promptChars = Array.from(prompt);
  const chunkSize = 160;
  for (let offset = 0; offset < promptChars.length; offset += chunkSize) {
    const chunk = promptChars.slice(offset, offset + chunkSize).join('');
    await page.keyboard.sendCharacter(chunk);
    if (offset + chunkSize < promptChars.length) {
      await sleep(40);
    }
  }

  await sleep(800);

  const actualContent = await safeHandleEvaluate(inputBox, '校验提示词内容', el =>
    ('value' in el ? el.value : el.textContent) || ''
  );

  if (actualContent.length >= prompt.length * 0.9) {
    console.log(`  ✅ 已输入提示词 (${actualContent.length} 字符)`);
    return true;
  }

  console.log('  ⚠️ 原生分段输入不完整，清空后用更小分段重试...');
  await inputBox.click();
  await page.keyboard.down('Meta');
  await page.keyboard.press('A');
  await page.keyboard.up('Meta');
  await page.keyboard.press('Backspace');
  const retryChunkSize = 60;
  for (let offset = 0; offset < promptChars.length; offset += retryChunkSize) {
    await page.keyboard.sendCharacter(promptChars.slice(offset, offset + retryChunkSize).join(''));
    if (offset + retryChunkSize < promptChars.length) {
      await sleep(80);
    }
  }
  await sleep(800);

  const finalContent = await safeHandleEvaluate(inputBox, '校验最终提示词内容', el =>
    ('value' in el ? el.value : el.textContent) || ''
  );
  console.log(`  最终输入: ${finalContent.length} 字符`);
  return finalContent.length >= prompt.length * 0.9;
}

/**
 * 验证当前参数设置
 */
async function verifySettings(page, options) {
  console.log('  📋 验证参数设置...');

  const state = await getVisibleToolbarState(page);
  const referenceInputLabels = await getReferenceInputLabels(page);
  const currentMode = state.generationType;
  const currentModel = state.model;
  const currentRefMode = state.mode;
  const currentDuration = state.duration;
  const currentRatio = state.ratio;
  
  console.log('  当前设置:');
  console.log(`    模式: ${currentMode}`);
  console.log(`    模型: ${currentModel}`);
  console.log(`    参考模式: ${currentRefMode}`);
  console.log(`    上传区域: ${referenceInputLabels.join(' | ') || '未识别'}`);
  console.log(`    时长: ${currentDuration}`);
  
  // 验证模型
  const modelOk = isModelMatch(currentModel, options.model);
  
  // 验证参考模式
  const modeTextOk = isModeMatch(currentRefMode, options.mode) || isReferenceModeEquivalent(currentRefMode, options.mode);
  const modeInputOk = isReferenceInputModeMatch(referenceInputLabels, options.mode);
  const modeOk = modeTextOk && modeInputOk;
  
  // 验证时长
  const durationOk = isDurationMatch(currentDuration, options.duration);
  
  // 验证比例
  const ratioOk = isRatioMatch(currentRatio, options.ratio);
  console.log(`    比例: ${currentRatio || '未找到'}`);
  
  // 汇总结果
  const allOk = modelOk && modeOk && durationOk && ratioOk;
  
  console.log('  验证结果:');
  console.log(`    模型: ${modelOk ? '✅' : '❌'} (期望: ${options.model})`);
  console.log(`    参考模式: ${modeOk ? '✅' : '❌'} (期望: ${options.mode}; 文本=${modeTextOk ? '✅' : '❌'}; 上传区域=${modeInputOk ? '✅' : '❌'})`);
  console.log(`    比例: ${ratioOk ? '✅' : '❌'} (期望: ${options.ratio})`);
  console.log(`    时长: ${durationOk ? '✅' : '❌'} (期望: ${options.duration}s)`);
  
  if (!allOk) {
    console.log('  ⚠️ 参数验证失败，请检查设置');
  } else {
    console.log('  ✅ 参数验证通过');
  }
  
  return allOk;
}

/**
 * 检查是否有正在生成的视频
 */
async function checkGeneratingStatus(page, skipNavigate = false) {
  // 只在需要时导航
  if (!skipNavigate) {
    await ensureVideoGenerationPage(page);
  }
  
  // 检查生成中的任务数
  const status = await safePageEvaluate(page, '读取生成中状态', sessionLimitPatterns => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    // 查找左侧菜单中的"生成X"元素
    // 菜单结构：<div>灵感</div><div>生成10</div><div>资产</div><div>画布</div>
    let generatingCount = 0;
    let generatingSource = 'none';
    let generatingRawText = '';
    
    // 方法1：查找精确匹配"生成X"的元素
    const allDivs = document.querySelectorAll('div');
    for (const div of allDivs) {
      const text = div.textContent || '';
      // 精确匹配 "生成10"、"生成9" 等格式
      if (text.match(/^生成\s*\d+$/)) {
        const match = text.match(/(\d+)/);
        if (match) {
          generatingCount = parseInt(match[1]);
          generatingSource = 'menu-exact';
          generatingRawText = text;
          break;
        }
      }
    }
    
    // 方法2：查找包含"生成"和数字的短文本
    if (generatingSource === 'none') {
      for (const div of allDivs) {
        const text = div.textContent || '';
        if (text.includes('生成') && text.length < 20) {
          const match = text.match(/生成\s*(\d+)/);
          if (match) {
            generatingCount = parseInt(match[1]);
            generatingSource = 'menu-loose';
            generatingRawText = text;
            break;
          }
        }
      }
    }

    // 只把当前可见的 toast / alert / dialog 文本当成限流提示
    const limitPatterns = ['高峰期', '无法提交更多任务', ...sessionLimitPatterns];
    const visibleNoticeSelectors = [
      '[role="alert"]',
      '[role="dialog"]',
      '[class*="toast"]',
      '[class*="message"]',
      '[class*="notice"]',
      '[class*="warning"]',
      '[class*="error"]',
      '[class*="modal"]'
    ];

    const visibleNoticeTexts = Array.from(
      document.querySelectorAll(visibleNoticeSelectors.join(','))
    )
      .filter(isVisible)
      .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
      .filter(Boolean);

    const matchedLimitedText = visibleNoticeTexts.find(text =>
      limitPatterns.some(pattern => text.includes(pattern))
    );

    if (matchedLimitedText) {
      return {
        generating: generatingCount,
        source: generatingSource === 'none' ? 'limited-visible' : generatingSource,
        rawText: generatingRawText,
        limited: true,
        limitedText: matchedLimitedText
      };
    }

    return {
      generating: generatingCount,
      source: generatingSource,
      rawText: generatingRawText
    };
  }, SESSION_LIMIT_PATTERNS);
  
  // 处理特殊状态
  if (status.limited) {
    console.log(`  ⚠️ 平台限制：${status.limitedText || '高峰期，无法提交新任务'}`);
    return {
      generating: status.generating || 0,
      limited: true,
      source: status.source,
      rawText: status.rawText,
      limitedText: status.limitedText
    };
  }
  
  console.log(`  生成中队列: ${status.generating} 个任务`);
  if (status.rawText) {
    console.log(`  原始文本: "${status.rawText}" (${status.source})`);
  }
  
  return status;
}

/**
 * 点击生成按钮
 */
async function clickGenerate(page) {
  console.log('  点击生成按钮...');
  
  await sleep(1000);

  const candidates = await getGenerateButtonCandidates(page);
  if (!Array.isArray(candidates) || candidates.length === 0) {
    console.log('  ⚠️ 未识别到生成按钮候选');
    return { success: false, reason: '未找到生成按钮' };
  }

  console.log(`  🎯 找到 ${candidates.length} 个生成按钮候选，优先尝试: ${describeGenerateButtonCandidate(candidates[0])}`);

  let result = { success: false, reason: '未找到生成按钮' };
  for (const candidate of candidates.slice(0, 3)) {
    if (candidate.disabled) {
      result = { success: false, reason: '按钮被禁用 - 可能需要上传图片或输入提示词' };
      continue;
    }

    try {
      console.log(`  🖱️ 尝试点击生成候选: ${describeGenerateButtonCandidate(candidate)}`);
      result = await safePageEvaluate(page, '点击生成按钮', target => {
        const isVisible = el => {
          if (!el) return false;
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
        };

        const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
        const matched = Array.from(document.querySelectorAll('button, [role="button"], a'))
          .filter(isVisible)
          .find(el => {
            const rect = el.getBoundingClientRect();
            const text = normalize(el.textContent);
            const ariaLabel = normalize(el.getAttribute('aria-label'));
            const title = normalize(el.getAttribute('title'));
            const className = String(el.className || '');
            const combined = `${text} ${ariaLabel} ${title}`.trim();
            return (
              Math.abs((rect.x + rect.width / 2) - target.x) < 12 &&
              Math.abs((rect.y + rect.height / 2) - target.y) < 12 &&
              combined === target.combined &&
              className === target.className
            );
          });

        if (!matched) {
          return { success: false, reason: '生成按钮候选已失效' };
        }

        if (matched.disabled || matched.getAttribute('aria-disabled') === 'true') {
          return { success: false, reason: '按钮被禁用 - 可能需要上传图片或输入提示词' };
        }

        matched.click();
        return { success: true };
      }, candidate);
    } catch (error) {
      console.log(`  ⚠️ JS 点击生成失败，尝试鼠标兜底: ${error.message}`);
      result = { success: false, reason: error.message };
    }

    if (!result.success) {
      if (String(result.reason || '').includes('被禁用')) {
        return result;
      }
      await page.mouse.move(candidate.x, candidate.y, { steps: 8 });
      await sleep(120);
      await page.mouse.click(candidate.x, candidate.y, { delay: 80 });
      result = { success: true, backup: true };
    }

    if (result.success) {
      break;
    }
  }
  
  if (result.success) {
    console.log(result.backup ? '  ✅ 已点击生成（备用）' : '  ✅ 已点击生成');
    
    // 等待一下，检查是否有错误提示
    await sleep(2000);
    
    const errorCheck = await safePageEvaluate(page, '读取生成后提示', sessionLimitPatterns => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
      };

      const texts = Array.from(
        document.querySelectorAll('[role="alert"], [class*="toast"], [class*="message"], [class*="notice"], [class*="error"], [class*="warning"]')
      )
        .filter(isVisible)
        .map(el => (el.textContent || '').replace(/\s+/g, ' ').trim())
        .filter(Boolean);

      const combinedText = texts.join(' ');

      const hasSessionLimit = sessionLimitPatterns.some(pattern =>
        combinedText.toLowerCase().includes(String(pattern).toLowerCase())
      );

      if (combinedText.includes('高峰期') || combinedText.includes('无法提交更多任务') || hasSessionLimit) {
        return {
          hasError: true,
          isLimited: true,
          isInsufficientCredits: false,
          message: combinedText || '即梦会话或提交数量达到平台限制'
        };
      }

      const insufficientPatterns = [
        '积分不足',
        '点数不足',
        '余额不足',
        '灵感值不足',
        '剩余积分不足',
        '剩余点数不足',
        '今日积分已用完',
        '今日点数已用完',
        'credit not enough',
        'credits not enough',
        'insufficient credits',
        'insufficient balance'
      ];
      const matchedInsufficientCredits = insufficientPatterns.find(pattern =>
        combinedText.toLowerCase().includes(String(pattern).toLowerCase())
      );
      if (matchedInsufficientCredits) {
        return {
          hasError: true,
          isLimited: false,
          isInsufficientCredits: true,
          message: combinedText || '积分不足，无法继续提交'
        };
      }

      if (combinedText.includes('提交失败') || combinedText.includes('生成失败')) {
        return {
          hasError: true,
          isLimited: false,
          isInsufficientCredits: false,
          message: combinedText
        };
      }

      return { hasError: false, isInsufficientCredits: false };
    }, SESSION_LIMIT_PATTERNS);
    
    if (errorCheck.hasError) {
      console.log('  ❌ ' + errorCheck.message);
      return { 
        success: false, 
        reason: errorCheck.message,
        limited: errorCheck.isLimited,
        insufficientCredits: Boolean(errorCheck.isInsufficientCredits)
      };
    }
    
    return result;
  }
  
  return result;
}

/**
 * 等待生成完成
 */
async function waitForCompletion(page, timeout = 600000) {
  console.log('  ⏳ 等待生成完成...');
  
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    const hasDownloadButton = await safePageEvaluate(page, '检查下载按钮', () => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && rect.width > 0 && rect.height > 0;
      };

      return Array.from(document.querySelectorAll('button, [role="button"], a'))
        .filter(isVisible)
        .some(el => String(el.textContent || '').includes('下载'));
    });

    if (hasDownloadButton) {
      console.log('  ✅ 生成完成！');
      return true;
    }
    
    await sleep(5000);
  }
  
  console.log('  ⏰ 生成超时');
  return false;
}

function isRecoverableDownloadPageError(error) {
  const message = String(error?.message || error || '');
  return (
    message.includes('Runtime.callFunctionOn timed out') ||
    message.includes('Protocol error (Runtime.callFunctionOn)') ||
    message.includes('Execution context was destroyed') ||
    message.includes('Cannot find context with specified id') ||
    message.includes('Inspected target navigated or closed') ||
    message.includes('Most likely the page has been closed')
  );
}

function describeDownloadTrigger(trigger) {
  if (!trigger || typeof trigger !== 'object') {
    return 'unknown';
  }
  const label = String(trigger.text || trigger.ariaLabel || trigger.title || '').replace(/\s+/g, ' ').trim();
  const base = `${trigger.strategy || 'unknown'}@(${Math.round(trigger.x || 0)},${Math.round(trigger.y || 0)})`;
  return label ? `${base} ${label.slice(0, 60)}` : base;
}

function findNewCompletedDownload(outputPath, existingFiles) {
  const files = fs.readdirSync(outputPath);
  const newFiles = files.filter(name =>
    !existingFiles.has(name) &&
    !name.endsWith('.crdownload') &&
    !name.endsWith('.tmp')
  );

  if (newFiles.length === 0) {
    return null;
  }

  return newFiles
    .map(name => ({
      name,
      path: path.join(outputPath, name),
      mtimeMs: fs.statSync(path.join(outputPath, name)).mtimeMs
    }))
    .sort((a, b) => b.mtimeMs - a.mtimeMs)[0];
}

function findPartialDownloads(outputPath, existingFiles) {
  const files = fs.readdirSync(outputPath);
  return files
    .filter(name =>
      !existingFiles.has(name) &&
      (name.endsWith('.crdownload') || name.endsWith('.tmp'))
    )
    .map(name => {
      const filePath = path.join(outputPath, name);
      const stat = fs.statSync(filePath);
      return {
        name,
        path: filePath,
        size: stat.size,
        mtimeMs: stat.mtimeMs
      };
    });
}

async function waitForDownloadCompletion(outputPath, existingFiles, timeoutMs = 90000) {
  const timeoutAt = Date.now() + timeoutMs;
  let sawPartial = false;
  let lastPartialSignature = '';
  let lastCompletedName = '';

  while (Date.now() < timeoutAt) {
    const completed = findNewCompletedDownload(outputPath, existingFiles);
    if (completed) {
      if (completed.name !== lastCompletedName) {
        console.log(`  📦 检测到下载文件落盘: ${completed.name}`);
        lastCompletedName = completed.name;
      }
      return completed;
    }

    const partials = findPartialDownloads(outputPath, existingFiles);
    if (partials.length > 0) {
      sawPartial = true;
      const partialSignature = partials
        .map(item => `${item.name}:${item.size}`)
        .sort()
        .join('|');
      if (partialSignature !== lastPartialSignature) {
        console.log(`  ⏳ 等待下载完成: ${partials.map(item => `${item.name}(${item.size})`).join(', ')}`);
        lastPartialSignature = partialSignature;
      }
    }

    await sleep(sawPartial ? 1200 : 800);
  }

  return null;
}

async function collectDownloadTriggers(page) {
  return await page.evaluate(() => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        rect.width > 0 &&
        rect.height > 0
      );
    };

    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const getWeight = item => {
      const text = normalize(item.text);
      if (text === '下载') return 0;
      if (text.includes('下载')) return 1;
      return 2;
    };

    return Array.from(document.querySelectorAll('button, [role="button"], a'))
      .filter(el => isVisible(el))
      .map(el => {
        const rect = el.getBoundingClientRect();
        return {
          text: normalize(el.textContent),
          ariaLabel: normalize(el.getAttribute('aria-label')),
          title: normalize(el.getAttribute('title')),
          x: rect.x + rect.width / 2,
          y: rect.y + rect.height / 2,
          top: rect.top,
          left: rect.left,
          width: rect.width,
          height: rect.height,
          tagName: String(el.tagName || '').toLowerCase(),
          href: normalize(el.getAttribute('href'))
        };
      })
      .filter(item =>
        item.width > 20 &&
        item.height > 20 &&
        item.x >= 0 &&
        item.y >= 0 &&
        `${item.text} ${item.ariaLabel} ${item.title}`.includes('下载')
      )
      .sort((a, b) =>
        getWeight(a) - getWeight(b) ||
        a.top - b.top ||
        a.left - b.left
      )
      .slice(0, 5);
  });
}

async function attemptDownloadTrigger(page, trigger, strategy) {
  if (!trigger) {
    return false;
  }

  if (strategy === 'js-click') {
    return await page.evaluate(target => {
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          style.opacity !== '0' &&
          rect.width > 0 &&
          rect.height > 0
        );
      };

      const candidates = Array.from(document.querySelectorAll('button, [role="button"], a')).filter(el => {
        if (!isVisible(el)) return false;
        const rect = el.getBoundingClientRect();
        const text = normalize(el.textContent);
        const ariaLabel = normalize(el.getAttribute('aria-label'));
        const title = normalize(el.getAttribute('title'));
        const combined = `${text} ${ariaLabel} ${title}`;
        return (
          combined.includes('下载') &&
          Math.abs((rect.x + rect.width / 2) - target.x) < 8 &&
          Math.abs((rect.y + rect.height / 2) - target.y) < 8
        );
      });

      const button = candidates[0];
      if (!button) {
        return false;
      }

      button.click();
      return true;
    }, trigger);
  }

  if (strategy === 'mouse-click') {
    await page.mouse.move(trigger.x, trigger.y, { steps: 8 });
    await sleep(120);
    await page.mouse.click(trigger.x, trigger.y, { delay: 80 });
    return true;
  }

  return false;
}

/**
 * 下载视频
 */
async function downloadVideo(page, outputPath) {
  console.log('  下载视频...');
  fs.mkdirSync(outputPath, { recursive: true });

  const existingFiles = new Set(fs.readdirSync(outputPath));
  try {
    const client = await page.createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: outputPath
    });
  } catch (error) {
    console.log(`  ⚠️ 设置下载目录失败，继续尝试默认下载: ${error.message}`);
  }

  let triggers = [];
  let lastRecoverableError = null;

  try {
    triggers = await collectDownloadTriggers(page);
  } catch (error) {
    if (!isRecoverableDownloadPageError(error)) {
      throw error;
    }
    lastRecoverableError = error;
    console.log(`  ⚠️ 读取下载按钮候选时遇到可恢复超时，改走兜底等待: ${error.message}`);
  }

  if (triggers.length > 0) {
    console.log(`  🎯 找到 ${triggers.length} 个下载候选，优先尝试: ${describeDownloadTrigger(triggers[0])}`);
  }

  const strategies = ['js-click', 'mouse-click'];
  let clickTriggered = false;

  for (const trigger of triggers) {
    for (const strategy of strategies) {
      try {
        console.log(`  🖱️ 尝试触发下载: ${describeDownloadTrigger({ ...trigger, strategy })}`);
        clickTriggered = await attemptDownloadTrigger(page, trigger, strategy);
      } catch (error) {
        if (!isRecoverableDownloadPageError(error)) {
          throw error;
        }
        lastRecoverableError = error;
        console.log(`  ⚠️ ${strategy} 触发下载时遇到可恢复超时，继续等待结果: ${error.message}`);
      }

      const opportunistic = await waitForDownloadCompletion(outputPath, existingFiles, clickTriggered ? 12000 : 5000);
      if (opportunistic) {
        console.log(`  ✅ 已保存: ${opportunistic.path}`);
        return opportunistic.path;
      }

      await sleep(clickTriggered ? 1400 : 800);
    }
  }

  if (!clickTriggered && triggers.length === 0) {
    console.log('  ⚠️ 未读取到下载按钮候选，直接等待浏览器侧是否已自动开始下载');
  }

  const latest = await waitForDownloadCompletion(outputPath, existingFiles, 90000);
  if (latest) {
    console.log(`  ✅ 已保存: ${latest.path}`);
    return latest.path;
  }

  if (lastRecoverableError) {
    console.log(`  ⚠️ 下载按钮点击后未等到文件落盘: ${lastRecoverableError.message}`);
  } else if (!clickTriggered) {
    console.log('  ⚠️ 未找到可点击的下载按钮');
  }
  
  console.log('  ⚠️ 下载失败');
  return false;
}

/**
 * 处理单个任务
 * @param {Page} page - Puppeteer 页面对象
 * @param {Object} task - 任务对象
 * @param {Object} options - 配置选项
 * @param {boolean} skipQueueCheck - 是否跳过队列检查（smart-monitor 已在外部检查）
 */
async function processTask(page, task, options, skipQueueCheck = false) {
  console.log(`\n🎬 处理任务: ${task.name}`);
  console.log(`  文件夹: ${task.folder}`);
  
  if (task.images && task.images.length > 0) {
    console.log(`  参考图片: ${task.images.length} 张`);
    task.images.forEach((img, i) => console.log(`    [${i + 1}] ${path.basename(img)}`));
  }
  if (task.prompt) console.log(`  提示词: ${task.prompt.substring(0, 50)}... (${task.prompt.length} 字符)`);
  
  const taskOptions = { ...options, ...task.config };
  let touchedGenerationSession = false;
  const cleanupFailedAttempt = async (reason, maxDelete = 2) => {
    if (!touchedGenerationSession) {
      return { deleted: 0, reason, skipped: 'session_not_touched' };
    }
    return cleanupRecentUnnamedConversations(page, { reason, maxDelete }).catch(error => {
      console.log(`  ⚠️ 清理未命名会话失败: ${error.message}`);
      return { deleted: 0, reason, error: error.message };
    });
  };
  
  console.log(`  模型: ${taskOptions.model}`);
  console.log(`  参考模式: ${taskOptions.mode}`);
  console.log(`  比例: ${taskOptions.ratio}`);
  console.log(`  时长: ${taskOptions.duration}s`);
  
  try {
    await prepareAutomationPage(page);
    await resetVideoGenerationPage(page, taskOptions.baseUrl || DEFAULT_CONFIG.baseUrl);

    // 0. 检查是否已有生成中的任务（除非调用方已在外部检查）
    if (!skipQueueCheck) {
      const generatingStatus = await checkGeneratingStatus(page);
      if (generatingStatus.generating > 0) {
        console.log(`  ⚠️ 已有 ${generatingStatus.generating} 个任务在生成中，跳过提交`);
        console.log(`  💡 请等待当前任务完成后再运行脚本`);
        return { success: false, error: '队列已满', skip: true };
      }
    }
    
    // 1. 选择基础参数
    await applyTaskSettings(page, taskOptions);

    let preUploadSettingsOk = await ensureSettingsMatch(page, taskOptions, 3);
    if (!preUploadSettingsOk) {
      console.log('  ⚠️ 上传前参数校验失败，尝试重置页面后重试一次...');
      await resetVideoGenerationPage(page, taskOptions.baseUrl || DEFAULT_CONFIG.baseUrl);
      await applyTaskSettings(page, taskOptions);
      preUploadSettingsOk = await ensureSettingsMatch(page, taskOptions, 3);
    }

    if (!preUploadSettingsOk) {
      console.log('  ❌ 上传前参数校验失败，已停止本次提交');
      await markTaskFailed(page, task, {
        reason: '上传前参数校验失败',
        code: 'pre_upload_settings_failed',
        options: taskOptions
      });
      return { success: false, error: '上传前参数校验失败', code: 'pre_upload_settings_failed' };
    }
    
    // 5. 上传参考图片
    if (task.images && task.images.length > 0) {
      const validImages = task.images.filter(img => fs.existsSync(img));
      if (validImages.length > 0) {
        const preparedImages = prepareImagesForUpload(validImages);
        const uploadPaths = preparedImages
          .map(item => item.uploadPath)
          .filter(img => img && fs.existsSync(img));
        const convertedImages = preparedImages.filter(item => item.converted);

        if (convertedImages.length > 0) {
          console.log(`  ♻️ 已将 ${convertedImages.length} 张高负载图片转换为 JPG 上传副本`);
          convertedImages.forEach((item, index) => {
            console.log(`    [${index + 1}] ${path.basename(item.sourcePath)} -> ${path.basename(item.uploadPath)}`);
          });
        }

        touchedGenerationSession = uploadPaths.length > 0;
        const uploadOk = await uploadReference(page, uploadPaths, taskOptions.mode);
        if (!uploadOk) {
          console.log('  ❌ 参考图片上传失败，已停止本次提交');
          await cleanupFailedAttempt('upload_failed');
          await markTaskFailed(page, task, {
            reason: '参考图片上传失败',
            code: 'upload_failed',
            options: taskOptions
          });
          return { success: false, error: '参考图片上传失败', code: 'upload_failed' };
        }
        await sleep(convertedImages.length > 0 ? 3000 : 1000);
      }
    }
    
    // 6. 输入提示词
    if (task.prompt) {
      const promptOk = await inputPrompt(page, task.prompt);
      touchedGenerationSession = true;
      await sleep(500);
      if (!promptOk) {
        console.log('  ❌ 提示词输入失败，已停止本次提交');
        await cleanupFailedAttempt('prompt_input_failed');
        await markTaskFailed(page, task, {
          reason: '提示词输入失败',
          code: 'prompt_input_failed',
          options: taskOptions
        });
        return { success: false, error: '提示词输入失败', code: 'prompt_input_failed' };
      }
    }
    
    // 7. 点击生成前硬校验参数
    const settingsOk = await ensureSettingsMatch(page, taskOptions, 3);
    
    // 8. 点击生成并检测是否成功
    if (settingsOk) {
      // 记录当前积分数
      const beforeCreditReading = await readJimengCredits(page);
      const beforeCredits = beforeCreditReading.raw;
      const beforeCreditsValue = beforeCreditReading.value;
      const insufficientCreditsThreshold = Math.max(0, Number(taskOptions.insufficientCreditsThreshold ?? DEFAULT_CONFIG.insufficientCreditsThreshold) || 0);

      if (isReliableCreditsBelowThreshold(beforeCredits, beforeCreditsValue, insufficientCreditsThreshold)) {
        const errorMessage = `当前积分 ${beforeCreditsValue} 低于阈值 ${insufficientCreditsThreshold}，视为余额不足`;
        console.log(`  ⛔ ${errorMessage}`);
        return {
          success: false,
          error: errorMessage,
          code: 'insufficient_credits',
          insufficientCredits: true,
          beforeCredits,
          beforeCreditsValue,
          lowCreditsThreshold: insufficientCreditsThreshold
        };
      }
      
      const finalSettingsOk = await verifySettings(page, taskOptions);
      if (!finalSettingsOk) {
        console.log('  ❌ 点击生成前最终校验失败，已阻止提交');
        await cleanupFailedAttempt('final_settings_failed');
        await markTaskFailed(page, task, {
          reason: '最终参数校验失败',
          code: 'final_settings_failed',
          options: taskOptions
        });
        return { success: false, error: '最终参数校验失败' };
      }

      const generateResult = await clickGenerate(page);
      
      if (generateResult.success) {
        // 等待一下，让积分扣减生效
        await sleep(2000);

        const queueSignal = await page.evaluate(() => {
          const text = (document.body?.innerText || document.body?.textContent || '').replace(/\s+/g, ' ');
          return /排队中|排队加速中|已加入队列|加入队列/.test(text);
        });

        if (queueSignal) {
          console.log('  ✅ 已检测到排队状态，按提交成功处理');
        }
        
        // 检测是否有错误提示
        const errorCheck = await page.evaluate(() => {
          const errorSelectors = ['[class*="error"]', '[class*="fail"]', '[class*="warning"]'];
          for (const sel of errorSelectors) {
            const el = document.querySelector(sel);
            if (el && el.textContent) {
              return { hasError: true, message: el.textContent };
            }
          }
          return { hasError: false };
        });
        
        if (!queueSignal && errorCheck.hasError) {
          console.log(`  ❌ 生成失败: ${errorCheck.message}`);
          await cleanupFailedAttempt(errorCheck.isInsufficientCredits ? 'insufficient_credits_after_click' : 'generate_error_notice');
          if (errorCheck.isInsufficientCredits || containsInsufficientCreditsText(errorCheck.message)) {
            return {
              success: false,
              error: errorCheck.message || '积分不足，无法继续提交',
              code: 'insufficient_credits',
              insufficientCredits: true
            };
          }
          await markTaskFailed(page, task, {
            reason: errorCheck.message,
            code: 'generate_error_notice',
            options: taskOptions
          });
          return { success: false, error: errorCheck.message };
        }
        
        // 检测积分数是否变化（成功提交会扣积分）
        const afterCreditReading = await readJimengCredits(page);
        const afterCredits = afterCreditReading.raw;
        const afterCreditsValue = afterCreditReading.value;
        
        console.log(`  积分: ${beforeCredits || '?'} → ${afterCredits || '?'}`);
        
        // 只接受明确的排队提示；积分变化只做辅助日志，不单独作为提交成功依据。
        const successCheck = await page.evaluate(() => {
          const body = (document.body?.innerText || document.body?.textContent || '').replace(/\s+/g, ' ');
          if (/排队中|排队加速中|已加入队列|加入队列/.test(body)) {
            return { success: true };
          }
          return { success: false };
        });
        
        if (successCheck.success) {
          console.log('  ✅ 视频生成任务已成功提交');
          const lowCreditsPauseSuggested =
            isReliableCreditsBelowThreshold(afterCredits, afterCreditsValue, insufficientCreditsThreshold);
          if (lowCreditsPauseSuggested) {
            console.log(`  ⚠️ 当前提交后积分 ${afterCreditsValue} 已低于阈值 ${insufficientCreditsThreshold}，建议暂停后续提单`);
          }
          
          // 标记为已提交（等待后续检测确认完成）
          markTaskSubmitted(task);
          return {
            success: true,
            message: '已提交生成任务，等待检测完成',
            confirmedBy: 'ui_queue_signal',
            beforeCredits,
            afterCredits,
            beforeCreditsValue,
            afterCreditsValue,
            creditsChanged: beforeCredits !== afterCredits,
            lowCreditsThreshold: insufficientCreditsThreshold,
            lowCreditsPauseSuggested
          };
        } else {
          console.log('  ⚠️ 无法确认提交状态，等待外层确认窗口复核');
          return {
            success: false,
            error: '点击生成后未检测到明确成功信号，等待提交确认窗口复核',
            code: 'submit_unconfirmed',
            beforeCredits,
            afterCredits,
            creditsChanged: beforeCredits !== afterCredits
          };
        }
      } else {
        console.log(`  ❌ 点击生成失败: ${generateResult.reason}`);
        if (generateResult.limited) {
          const cleanupResult = await cleanupFailedAttempt('platform_limited');
          return {
            success: false,
            error: generateResult.reason,
            code: 'platform_limited',
            retryable: true,
            cleanupResult
          };
        }

        if (generateResult.insufficientCredits || containsInsufficientCreditsText(generateResult.reason)) {
          const cleanupResult = await cleanupFailedAttempt('insufficient_credits_after_click');
          return {
            success: false,
            error: generateResult.reason || '积分不足，无法继续提交',
            code: 'insufficient_credits',
            insufficientCredits: true,
            cleanupResult
          };
        }

        if (generateResult.reason && generateResult.reason.includes('按钮被禁用')) {
          await markTaskBlocked(page, task, {
            reason: generateResult.reason,
            code: 'button_disabled',
            options: taskOptions
          });
          return {
            success: false,
            error: generateResult.reason,
            code: 'task_blocked'
          };
        }

        const cleanupResult = await cleanupFailedAttempt('generate_click_failed');
        return {
          success: false,
          error: generateResult.reason,
          code: 'generate_click_failed',
          cleanupResult
        };
      }
    } else {
      console.log('  ❌ 参数验证失败，跳过生成');
      await cleanupFailedAttempt('settings_failed');
      await markTaskFailed(page, task, {
        reason: '参数验证失败',
        code: 'settings_failed',
        options: taskOptions
      });
      return { success: false, error: '参数验证失败' };
    }
    
  } catch (error) {
    console.log(`  ❌ 任务失败: ${error.message}`);
    try {
      await markTaskFailed(page, task, {
        reason: error.message,
        code: 'exception',
        options: task.config ? { ...options, ...task.config } : options
      });
    } catch (snapshotError) {
      console.log(`  ⚠️ 写入失败快照时出错: ${snapshotError.message}`);
    }
    return { success: false, error: error.message, code: 'exception' };
  }
}

/**
 * 主函数
 */
async function main() {
  const folderPath = process.argv[2];
  const watchMode = process.argv.includes('--watch');
  
  if (!folderPath) {
    console.log('用法: node folder-processor.js <文件夹路径> [--watch]');
    process.exit(1);
  }
  
  if (!fs.existsSync(folderPath)) {
    console.log(`❌ 文件夹不存在: ${folderPath}`);
    process.exit(1);
  }
  
  console.log('🎬 即梦视频生成器 - 文件夹模式');
  console.log('================================\n');
  console.log(`📂 扫描文件夹: ${folderPath}`);
  
  const configPath = path.join(__dirname, 'config.json');
  let config = DEFAULT_CONFIG;
  if (fs.existsSync(configPath)) {
    const userConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    config = { ...DEFAULT_CONFIG, ...userConfig };
  }
  
  const tasks = scanFolder(folderPath);
  
  if (tasks.length === 0) {
    console.log('\n⚠️ 未找到有效任务');
    console.log('\n文件夹结构要求:');
    console.log('  <文件夹>/');
    console.log('  ├── image.png      # 参考图片');
    console.log('  ├── prompt.txt     # 提示词');
    console.log('  └── config.json    # 可选配置');
    process.exit(0);
  }
  
  console.log(`\n📋 发现 ${tasks.length} 个任务`);
  
  const pendingTasks = tasks.filter(task => !isTaskCompleted(task, config.outputDir));
  const completedCount = tasks.length - pendingTasks.length;
  
  if (completedCount > 0) console.log(`✅ 已完成: ${completedCount} 个`);
  console.log(`⏳ 待处理: ${pendingTasks.length} 个`);
  
  if (pendingTasks.length === 0) {
    console.log('\n✅ 所有任务已完成！');
    process.exit(0);
  }
  
  console.log('\n🔗 连接浏览器...');
  let browser;
  try {
    const protocolTimeoutMs = Math.max(
      120000,
      Number(config.protocolTimeoutMs) > 0 ? Number(config.protocolTimeoutMs) : DEFAULT_CONFIG.protocolTimeoutMs
    );
    browser = await puppeteer.connect({
      browserURL: `http://${config.cdpHost || '127.0.0.1'}:${config.cdpPort}`,
      defaultViewport: null,
      timeout: 30000,
      protocolTimeout: protocolTimeoutMs
    });
    console.log('✅ 已连接浏览器\n');
  } catch (e) {
    console.log(`❌ 无法连接浏览器 (端口 ${config.cdpPort})`);
    console.log('请先启动 Chrome 调试模式:');
    console.log('  open -na "Google Chrome" --args \\');
    console.log(`    --remote-debugging-port=${config.cdpPort} \\`);
    console.log('    --user-data-dir="$HOME/.openclaw/jimeng-chrome-debug" \\');
    console.log('    --disable-background-timer-throttling \\');
    console.log('    --disable-renderer-backgrounding \\');
    console.log('    --disable-backgrounding-occluded-windows');
    console.log('');
    console.log('如果 Chrome 已经在运行，请先完全退出后再启动调试实例。');
    process.exit(1);
  }
  
  const pages = await browser.pages();
  let page = pages.find(p => p.url().includes('jimeng.jianying.com'));
  if (!page) page = await browser.newPage();
  
  await prepareAutomationPage(page);
  await resetVideoGenerationPage(page, config.baseUrl);
  
  let successCount = 0;
  let failCount = 0;
  
  for (const task of pendingTasks) {
    const result = await processTask(page, task, {
      model: task.config.model || config.defaultModel,
      mode: task.config.mode || config.defaultMode,
      ratio: task.config.ratio || config.defaultRatio,
      duration: task.config.duration || config.defaultDuration,
      baseUrl: config.baseUrl,
      timeout: task.config.timeout || config.timeout
    });
    
    if (result.success) {
      successCount++;
      console.log('\n💡 已提交一个任务，停止提交其他任务（账号限制：同时只能有1个生成中的视频）');
      console.log('💡 请等待当前任务完成后再运行脚本');
      break;  // 成功提交一个任务后停止
    } else if (result.skip) {
      // 队列已满，停止提交
      console.log('\n⏳ 已有任务在生成中，请稍后再运行');
      break;
    } else {
      failCount++;
    }
  }
  
  await browser.disconnect();
  
  console.log('\n================================');
  console.log('📊 处理完成');
  console.log(`✅ 成功: ${successCount}`);
  console.log(`❌ 失败: ${failCount}`);
  
  if (watchMode) {
    console.log('\n👀 监听模式：60秒后再次检查...');
    await sleep(60000);
    await main();
  }
}

// 导出函数供 smart-monitor.js 使用
module.exports = {
  // 核心函数
  scanFolder,
  parseTaskFolder,
  processTask,
  
  // 状态管理
  isTaskCompleted,
  markTaskCompleted,
  markTaskSubmitted,
  markTaskBlocked,
  markTaskFailed,
  
  // 生成状态检测
  checkGeneratingStatus,
  cleanupRecentUnnamedConversations,
  
  // 页面操作
  resetVideoGenerationPage,
  selectModel,
  selectMode,
  selectRatio,
  selectDuration,
  uploadReference,
  inputPrompt,
  verifySettings,
  ensureSettingsMatch,
  clickGenerate,
  waitForCompletion,
  downloadVideo,
  
  // 工具函数
  sleep,
  formatBeijingTimestamp,
  prepareAutomationPage,
  prepareImagesForUpload,
  DEFAULT_CONFIG,
  IMAGE_EXTENSIONS
};

// 只在直接运行时执行 main
if (require.main === module) {
  main().catch(console.error);
}
