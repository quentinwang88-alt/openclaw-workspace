const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// 默认配置
const DEFAULT_CONFIG = {
  cdpPort: 9222,
  baseUrl: 'https://jimeng.jianying.com/ai-tool/home?workspace=0&type=video',
  defaultModel: 'Seedance 2.0',
  defaultMode: '全能参考',
  defaultRatio: '9:16',
  defaultDuration: 15,
  outputDir: './output',
  timeout: 600000 // 10分钟
};

// 模型选项映射
const MODEL_MAP = {
  'Seedance 2.0': 'Seedance 2.0',
  'Seedance 2.0 Fast': 'Seedance 2.0 Fast',
  '视频 3.5 Pro': '视频 3.5 Pro',
  '视频 3.0 Pro': '视频 3.0 Pro',
  '视频 3.0 Fast': '视频 3.0 Fast',
  '视频 3.0': '视频 3.0'
};

// 参考模式映射
const MODE_MAP = {
  '全能参考': '全能参考',
  '首尾帧': '首尾帧',
  '智能多帧': '智能多帧',
  '主体参考': '主体参考'
};

// 比例映射
const RATIO_MAP = {
  '21:9': '21:9',
  '16:9': '16:9',
  '4:3': '4:3',
  '1:1': '1:1',
  '3:4': '3:4',
  '9:16': '9:16'
};

/**
 * 解析命令行参数
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const params = {};
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const value = args[i + 1];
      if (value && !value.startsWith('--')) {
        params[key] = value;
        i++;
      } else {
        params[key] = true;
      }
    }
  }
  
  return params;
}

/**
 * 等待指定时间
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 等待元素出现
 */
async function waitForElement(page, selector, timeout = 30000) {
  try {
    await page.waitForSelector(selector, { timeout });
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * 点击下拉选项
 */
async function selectOption(page, labelText, optionText) {
  console.log(`  选择: ${labelText} → ${optionText}`);
  
  // 查找包含目标文本的下拉框
  const comboboxes = await page.$$('div[role="combobox"]');
  let targetCombobox = null;
  
  for (const box of comboboxes) {
    const text = await box.evaluate(el => el.textContent);
    if (text.includes(labelText)) {
      targetCombobox = box;
      break;
    }
  }
  
  if (!targetCombobox) {
    console.log(`  ⚠️ 未找到 ${labelText} 下拉框`);
    return false;
  }
  
  // 点击打开下拉
  await targetCombobox.click();
  await sleep(500);
  
  // 等待选项列表
  await page.waitForSelector('div[role="listbox"]', { timeout: 5000 }).catch(() => {});
  
  // 查找目标选项
  const options = await page.$$('div[role="option"]');
  for (const option of options) {
    const text = await option.evaluate(el => el.textContent);
    if (text.includes(optionText)) {
      await option.click();
      await sleep(300);
      return true;
    }
  }
  
  console.log(`  ⚠️ 未找到选项: ${optionText}`);
  return false;
}

/**
 * 选择比例
 */
async function selectRatio(page, ratio) {
  console.log(`  选择比例: ${ratio}`);
  
  // 查找比例按钮（如 "9:16"）
  const buttons = await page.$$('button');
  for (const btn of buttons) {
    const text = await btn.evaluate(el => el.textContent);
    if (text.trim() === ratio || text.includes(ratio)) {
      await btn.click();
      await sleep(300);
      
      // 等待比例选择弹窗
      await page.waitForSelector('div[role="radiogroup"]', { timeout: 3000 }).catch(() => {});
      
      // 选择对应比例
      const radios = await page.$$('div[role="radio"]');
      for (const radio of radios) {
        const label = await radio.evaluate(el => el.getAttribute('aria-label'));
        if (label === ratio) {
          await radio.click();
          await sleep(300);
          return true;
        }
      }
      
      // 如果没找到 aria-label，用文本匹配
      const containers = await page.$$('div[role="radio"]');
      for (const container of containers) {
        const text = await container.evaluate(el => el.textContent);
        if (text.includes(ratio)) {
          await container.click();
          await sleep(300);
          return true;
        }
      }
      
      return true;
    }
  }
  
  console.log(`  ⚠️ 未找到比例按钮: ${ratio}`);
  return false;
}

/**
 * 选择时长
 */
async function selectDuration(page, duration) {
  console.log(`  选择时长: ${duration}s`);
  
  const durationText = `${duration}s`;
  
  // 查找时长下拉框
  const comboboxes = await page.$$('div[role="combobox"]');
  for (const box of comboboxes) {
    const text = await box.evaluate(el => el.textContent);
    if (text.match(/\d+s/)) {
      await box.click();
      await sleep(500);
      
      // 等待选项列表
      await page.waitForSelector('div[role="listbox"]', { timeout: 5000 }).catch(() => {});
      
      // 查找目标时长选项
      const options = await page.$$('div[role="option"]');
      for (const option of options) {
        const optText = await option.evaluate(el => el.textContent);
        if (optText.includes(durationText)) {
          await option.click();
          await sleep(300);
          return true;
        }
      }
    }
  }
  
  console.log(`  ⚠️ 未找到时长选项: ${duration}s`);
  return false;
}

/**
 * 上传参考素材
 */
async function uploadReference(page, files) {
  console.log(`  上传参考素材: ${files.length} 个文件`);
  
  // 点击"参考内容"区域
  const uploadArea = await page.waitForSelector('div:has-text("参考内容")', { timeout: 5000 }).catch(() => null);
  if (!uploadArea) {
    console.log('  ⚠️ 未找到上传区域');
    return false;
  }
  
  await uploadArea.click();
  await sleep(500);
  
  // 查找文件输入
  const fileInput = await page.$('input[type="file"]');
  if (!fileInput) {
    console.log('  ⚠️ 未找到文件输入');
    return false;
  }
  
  // 上传文件
  for (const file of files) {
    const filePath = path.resolve(file);
    if (fs.existsSync(filePath)) {
      await fileInput.uploadFile(filePath);
      await sleep(1000);
      console.log(`  ✅ 已上传: ${file}`);
    } else {
      console.log(`  ⚠️ 文件不存在: ${file}`);
    }
  }
  
  return true;
}

/**
 * 输入提示词
 */
async function inputPrompt(page, prompt) {
  console.log(`  输入提示词: ${prompt.substring(0, 50)}...`);
  
  // 查找文本输入框
  const textarea = await page.waitForSelector('textarea', { timeout: 5000 });
  if (!textarea) {
    console.log('  ⚠️ 未找到输入框');
    return false;
  }
  
  // 清空并输入
  await textarea.click({ clickCount: 3 });
  await sleep(100);
  await textarea.type(prompt, { delay: 20 });
  
  return true;
}

/**
 * 点击生成按钮
 */
async function clickGenerate(page) {
  console.log('  点击生成按钮...');
  
  // 等待按钮变为可用状态
  await sleep(1000);
  
  // 查找生成按钮（非禁用状态）
  const buttons = await page.$$('button');
  for (const btn of buttons) {
    const isDisabled = await btn.evaluate(el => el.disabled);
    if (isDisabled) continue;
    
    const parent = await btn.evaluateHandle(el => el.parentElement);
    const parentText = await parent.evaluate(el => el?.textContent || '');
    
    // 查找包含积分消耗的按钮区域
    if (parentText.match(/\d+/)) {
      // 这是生成按钮
      await btn.click();
      console.log('  ✅ 已点击生成');
      return true;
    }
  }
  
  // 备用方案：查找任何可点击的大按钮
  for (const btn of buttons) {
    const isDisabled = await btn.evaluate(el => el.disabled);
    if (!isDisabled) {
      const className = await btn.evaluate(el => el.className);
      if (className.includes('generate') || className.includes('submit')) {
        await btn.click();
        console.log('  ✅ 已点击生成（备用）');
        return true;
      }
    }
  }
  
  console.log('  ⚠️ 未找到生成按钮');
  return false;
}

/**
 * 等待生成完成
 */
async function waitForCompletion(page, timeout = 600000) {
  console.log('  ⏳ 等待生成完成...');
  
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    // 检查是否有下载按钮出现（表示生成完成）
    const downloadBtn = await page.$('button:has-text("下载")').catch(() => null);
    if (downloadBtn) {
      console.log('  ✅ 生成完成！');
      return true;
    }
    
    // 检查是否有错误提示
    const errorEl = await page.$('div:has-text("失败")').catch(() => null);
    if (errorEl) {
      console.log('  ❌ 生成失败');
      return false;
    }
    
    // 检查进度
    const progressEl = await page.$('[role="progressbar"]').catch(() => null);
    if (progressEl) {
      const progress = await progressEl.evaluate(el => el.getAttribute('aria-valuenow'));
      if (progress) {
        process.stdout.write(`\r  生成进度: ${progress}%`);
      }
    }
    
    await sleep(3000);
  }
  
  console.log('  ⏰ 生成超时');
  return false;
}

/**
 * 下载视频
 */
async function downloadVideo(page, outputPath) {
  console.log('  下载视频...');
  
  // 查找下载按钮
  const downloadBtn = await page.$('button:has-text("下载")').catch(() => null);
  if (!downloadBtn) {
    console.log('  ⚠️ 未找到下载按钮');
    return false;
  }
  
  // 设置下载路径
  const downloadPath = path.resolve(outputPath);
  if (!fs.existsSync(downloadPath)) {
    fs.mkdirSync(downloadPath, { recursive: true });
  }
  
  // 点击下载
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 60000 }).catch(() => null),
    downloadBtn.click()
  ]);
  
  if (download) {
    const fileName = download.suggestedFilename();
    const filePath = path.join(downloadPath, fileName);
    await download.saveAs(filePath);
    console.log(`  ✅ 已保存: ${filePath}`);
    return filePath;
  }
  
  // 如果无法通过事件捕获，尝试其他方式
  console.log('  ⚠️ 下载事件未捕获，请手动下载');
  return false;
}

/**
 * 主函数
 */
async function main() {
  const params = parseArgs();
  
  // 加载配置
  const configPath = path.join(__dirname, 'config.json');
  let config = DEFAULT_CONFIG;
  if (fs.existsSync(configPath)) {
    const userConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    config = { ...DEFAULT_CONFIG, ...userConfig };
  }
  
  // 合并参数
  const options = {
    model: params.model || config.defaultModel,
    mode: params.mode || config.defaultMode,
    ratio: params.ratio || config.defaultRatio,
    duration: parseInt(params.duration) || config.defaultDuration,
    timeout: parseInt(params.timeout) || config.timeout,
    output: params.output || config.outputDir
  };
  
  console.log('🎬 即梦视频生成器');
  console.log('==================\n');
  console.log('配置:');
  console.log(`  模型: ${options.model}`);
  console.log(`  参考模式: ${options.mode}`);
  console.log(`  比例: ${options.ratio}`);
  console.log(`  时长: ${options.duration}s`);
  console.log(`  输出: ${options.output}\n`);
  
  // 检查提示词
  if (!params.prompt && !params.batch) {
    console.log('❌ 请提供 --prompt 参数或 --batch 批量任务文件');
    console.log('\n用法示例:');
    console.log('  node generate-video.js --prompt "描述内容" --image ./image.png');
    console.log('  node generate-video.js --batch ./tasks.json');
    process.exit(1);
  }
  
  // 连接浏览器
  console.log('🔗 连接浏览器...');
  let browser;
  try {
    browser = await puppeteer.connect({
      browserURL: `http://localhost:${config.cdpPort}`,
      defaultViewport: null,
      timeout: 10000
    });
  } catch (e) {
    console.log(`❌ 无法连接浏览器 (端口 ${config.cdpPort})`);
    console.log('请先启动 Chrome 调试模式:');
    console.log('  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222');
    process.exit(1);
  }
  
  console.log('✅ 已连接浏览器\n');
  
  // 获取或创建页面
  const pages = await browser.pages();
  let page = pages.find(p => p.url().includes('jimeng.jianying.com'));
  
  if (!page) {
    page = await browser.newPage();
  }
  
  // 导航到视频生成页面
  console.log('📍 导航到即梦视频生成页面...');
  await page.goto(config.baseUrl, { waitUntil: 'networkidle2', timeout: 30000 });
  await sleep(2000);
  
  // 单个任务
  if (params.prompt) {
    await runTask(page, params, options);
  }
  
  // 批量任务
  if (params.batch) {
    const batchPath = path.resolve(params.batch);
    if (!fs.existsSync(batchPath)) {
      console.log(`❌ 批量任务文件不存在: ${batchPath}`);
      await browser.disconnect();
      process.exit(1);
    }
    
    const tasks = JSON.parse(fs.readFileSync(batchPath, 'utf8'));
    console.log(`📋 共 ${tasks.length} 个任务\n`);
    
    for (let i = 0; i < tasks.length; i++) {
      console.log(`\n[${i + 1}/${tasks.length}] 任务开始`);
      const task = tasks[i];
      await runTask(page, task, { ...options, ...task });
    }
  }
  
  // 断开连接
  await browser.disconnect();
  console.log('\n👋 完成');
}

/**
 * 执行单个任务
 */
async function runTask(page, task, options) {
  console.log('\n🎬 开始生成视频...\n');
  
  try {
    // 1. 设置参数
    console.log('⚙️ 设置参数...');
    
    // 选择模型
    await selectOption(page, '', options.model);
    await sleep(500);
    
    // 选择参考模式
    await selectOption(page, '', options.mode);
    await sleep(500);
    
    // 选择比例
    await selectRatio(page, options.ratio);
    await sleep(500);
    
    // 选择时长
    await selectDuration(page, options.duration);
    await sleep(500);
    
    // 2. 上传参考素材
    const files = [];
    if (task.image) files.push(task.image);
    if (task.video) files.push(task.video);
    if (task.audio) files.push(task.audio);
    
    if (files.length > 0) {
      await uploadReference(page, files);
      await sleep(1000);
    }
    
    // 3. 输入提示词
    await inputPrompt(page, task.prompt);
    await sleep(500);
    
    // 4. 点击生成
    await clickGenerate(page);
    
    // 5. 等待完成
    const success = await waitForCompletion(page, options.timeout);
    
    // 6. 下载视频
    if (success) {
      await downloadVideo(page, options.output);
    }
    
  } catch (error) {
    console.log(`❌ 任务失败: ${error.message}`);
  }
}

// 运行
main().catch(console.error);