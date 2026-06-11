const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');

// 导入 folder-processor 的函数
const { checkGeneratingStatus, processTask, markTaskSubmitted } = require('./folder-processor');

// 默认配置
const DEFAULT_CONFIG = {
  cdpPort: 9222,
  cdpHost: '127.0.0.1',
  baseUrl: 'https://jimeng.jianying.com/ai-tool/home?workspace=0&type=video',
  assetUrl: 'https://jimeng.jianying.com/ai-tool/asset?workspace=0',
  defaultModel: 'Seedance 2.0 Fast',
  defaultMode: '全能参考',
  defaultRatio: '9:16',
  defaultDuration: 4,
  outputDir: './output',
  timeout: 600000,
  maxConcurrent: 10,
  checkIntervalMinutes: 15
};

// 内存中记录已提交的任务（防止重复提交的双重保险）
const submittedTasks = new Set();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 扫描待处理任务
 */
function scanPendingTasks(dataDir) {
  const tasks = [];
  const imageExtensions = ['.png', '.jpg', '.jpeg', '.webp', '.gif'];
  
  const items = fs.readdirSync(dataDir);
  
  for (const item of items) {
    const folder = path.join(dataDir, item);
    if (!fs.statSync(folder).isDirectory() || item.startsWith('.')) continue;
    
    // 跳过已完成和已提交的（文件检查 + 内存检查双重保险）
    if (fs.existsSync(path.join(folder, '.completed'))) continue;
    if (fs.existsSync(path.join(folder, '.submitted'))) continue;
    if (fs.existsSync(path.join(folder, '.blocked'))) continue;
    if (submittedTasks.has(item)) continue;  // 内存中的已提交记录
    
    // 检查是否有提示词
    const hasPrompt = fs.existsSync(path.join(folder, 'prompt.txt')) || 
                      fs.existsSync(path.join(folder, 'prompt.md'));
    if (!hasPrompt) continue;
    
    // 检查是否有图片
    let hasImage = false;
    const imageDirs = [folder, path.join(folder, '图片'), path.join(folder, '产品主图'), path.join(folder, 'images')];
    for (const dir of imageDirs) {
      if (fs.existsSync(dir)) {
        const files = fs.readdirSync(dir);
        if (files.some(f => imageExtensions.some(ext => f.toLowerCase().endsWith(ext)))) {
          hasImage = true;
          break;
        }
      }
    }
    
    if (!hasImage) continue;
    
    // 读取配置
    let config = {};
    const configFile = path.join(folder, 'config.json');
    if (fs.existsSync(configFile)) {
      try {
        config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
      } catch (e) {}
    }
    
    tasks.push({
      name: item,
      folder,
      images: scanImages(folder, imageExtensions),  // 添加图片列表
      prompt: readPrompt(folder),                   // 添加提示词
      config
    });
  }
  
  return tasks;
}

/**
 * 扫描图片文件
 */
function scanImages(folder, imageExtensions) {
  const images = [];
  const imageDirs = [folder, path.join(folder, '图片'), path.join(folder, '产品主图'), path.join(folder, 'images')];
  
  for (const dir of imageDirs) {
    if (fs.existsSync(dir)) {
      const files = fs.readdirSync(dir);
      for (const file of files) {
        if (imageExtensions.some(ext => file.toLowerCase().endsWith(ext))) {
          images.push(path.join(dir, file));
        }
      }
    }
  }
  
  return images;
}

/**
 * 读取提示词
 */
function readPrompt(folder) {
  const promptFile = path.join(folder, 'prompt.txt');
  const promptMdFile = path.join(folder, 'prompt.md');
  
  if (fs.existsSync(promptFile)) {
    return fs.readFileSync(promptFile, 'utf8').trim();
  }
  if (fs.existsSync(promptMdFile)) {
    return fs.readFileSync(promptMdFile, 'utf8').trim();
  }
  
  return null;
}

/**
 * 输出统计
 */
function printStats(submittedCount, generatingCount, pendingCount) {
  console.log('\n================================');
  console.log('📊 本次检测统计');
  console.log('================================');
  console.log(`⏳ 生成中: ${generatingCount}`);
  console.log(`🚀 新提交: ${submittedCount}`);
  console.log(`📋 待处理: ${pendingCount}`);
  console.log(`📅 结束时间: ${new Date().toLocaleString()}`);
}

/**
 * 主函数
 */
async function main() {
  const dataDir = process.argv[2] || process.env.JIMENG_DATA_DIR || '~/Desktop/jimeng';
  const resolvedDir = dataDir.replace('~', process.env.HOME);
  
  // 加载配置
  const configPath = path.join(__dirname, 'config.json');
  let config = DEFAULT_CONFIG;
  if (fs.existsSync(configPath)) {
    const userConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    config = { ...DEFAULT_CONFIG, ...userConfig };
  }
  
  const maxConcurrent = config.maxConcurrent;
  
  console.log('🎬 即梦视频生成 - 智能监测');
  console.log('================================');
  console.log(`📂 数据目录: ${resolvedDir}`);
  console.log(`⚙️  最大并发: ${maxConcurrent}`);
  console.log(`⏱️  检测间隔: ${config.checkIntervalMinutes} 分钟`);
  console.log(`📅 ${new Date().toLocaleString()}\n`);
  
  // 连接浏览器
  console.log('🔗 连接浏览器...');
  let browser;
  try {
    browser = await puppeteer.connect({
      browserURL: `http://${config.cdpHost || '127.0.0.1'}:${config.cdpPort}`,
      defaultViewport: null,
      timeout: 30000,
      protocolTimeout: 120000
    });
    console.log('✅ 已连接浏览器\n');
  } catch (e) {
    console.log(`❌ 无法连接浏览器: ${e.message}`);
    console.log('请先启动 Chrome 调试模式:');
    console.log('  open -na "Google Chrome" --args \\');
    console.log(`    --remote-debugging-port=${config.cdpPort} \\`);
    console.log('    --user-data-dir="$HOME/.openclaw/jimeng-chrome-debug"');
    console.log('');
    console.log('如果 Chrome 已经在运行，请先完全退出后再启动调试实例。');
    process.exit(1);
  }
  
  let page = (await browser.pages()).find(p => p.url().includes('type=video')) ||
             (await browser.pages()).find(p => p.url().includes('jimeng'));
  if (!page) page = await browser.newPage();
  await page.goto(config.baseUrl, { waitUntil: 'networkidle2', timeout: 30000 });
  await sleep(2000);
  
  // 初始扫描待处理任务
  console.log('📋 扫描待处理任务...');
  let pendingTasks = scanPendingTasks(resolvedDir);
  console.log(`   待处理: ${pendingTasks.length} 个\n`);
  
  // 如果没有待处理任务，直接退出
  if (pendingTasks.length === 0) {
    console.log('✅ 没有待处理任务，退出检测');
    await browser.disconnect();
    printStats(0, 0, 0);
    return;
  }
  
  // 持续检测循环
  let submittedCount = 0;
  let loopCount = 0;
  let justSubmitted = false;  // 标记是否刚提交过任务
  
  while (true) {
    loopCount++;
    console.log(`\n========================================`);
    console.log(`🔄 检测循环 #${loopCount}`);
    console.log('========================================');
    
    // 如果刚提交过任务，等待10秒后再获取生成中数量
    if (justSubmitted) {
      console.log('⏳ 等待 10 秒后获取生成中数量...');
      await sleep(10000);
      justSubmitted = false;
    }
    
    // 获取生成中数量
    console.log('📊 获取生成中数量...');
    const status = await checkGeneratingStatus(page);
    const generatingCount = status.generating || 0;
    
    // 重新扫描待处理任务
    pendingTasks = scanPendingTasks(resolvedDir);
    
    console.log(`   生成中: ${generatingCount}`);
    console.log(`   待处理: ${pendingTasks.length}`);

    if (pendingTasks.length === 0) {
      console.log('\n✅ 没有待处理任务了，停止检测');
      break;  // 停止检测
    }
    
    // 判断是否需要暂停
    if (generatingCount >= maxConcurrent) {
      console.log(`\n⏳ 已达到最大并发数 (${generatingCount}/${maxConcurrent})，等待 ${config.checkIntervalMinutes} 分钟后继续检测...`);
      await sleep(config.checkIntervalMinutes * 60 * 1000);
      continue;  // 暂停，等待后继续下一次检测
    }
    
    // 检查是否高峰期限流
    if (status.limited) {
      console.log(`\n⏳ 平台高峰期限流，等待 ${config.checkIntervalMinutes} 分钟后继续检测...`);
      await sleep(config.checkIntervalMinutes * 60 * 1000);
      continue;  // 暂停，等待后继续下一次检测
    }
    
    // 计算可用槽位
    const availableSlots = maxConcurrent - generatingCount;
    console.log(`   可用槽位: ${availableSlots}`);
    
    // 提交一个任务
    const taskToSubmit = pendingTasks[0];
    console.log(`\n🚀 提交任务: ${taskToSubmit.name}`);
    
    const options = {
      model: taskToSubmit.config?.model || config.defaultModel,
      mode: taskToSubmit.config?.mode || config.defaultMode,
      ratio: taskToSubmit.config?.ratio || config.defaultRatio,
      duration: taskToSubmit.config?.duration || config.defaultDuration,
      baseUrl: config.baseUrl,
      timeout: taskToSubmit.config?.timeout || config.timeout
    };
    
    const result = await processTask(page, taskToSubmit, options, true);
    
    if (result.success) {
      submittedCount++;
      console.log('  ✅ 任务提交成功');
      // 记录到内存中，防止重复提交
      submittedTasks.add(taskToSubmit.name);
      // 标记刚提交过任务，下次循环会等待10秒
      justSubmitted = true;
    } else {
      const afterStatus = await checkGeneratingStatus(page, true);
      const afterGeneratingCount = afterStatus.generating || 0;

      if (afterGeneratingCount > generatingCount) {
        console.log(`  ⚠️ 返回了失败信息，但生成队列已从 ${generatingCount} 增加到 ${afterGeneratingCount}，按已提交处理`);
        markTaskSubmitted(taskToSubmit);
        submittedCount++;
        submittedTasks.add(taskToSubmit.name);
        justSubmitted = true;
        continue;
      }

      console.log(`  ❌ 任务提交失败: ${result.error || '未知错误'}`);
      if (result.code === 'task_blocked') {
        console.log('  🚫 当前任务已标记为 .blocked，后续扫描将自动跳过');
      } else {
        // 失败也等待一下，避免频繁重试
        await sleep(3000);
      }
    }
  }
  
  // 获取最终生成中数量
  const finalStatus = await checkGeneratingStatus(page);
  const finalGeneratingCount = finalStatus.generating || 0;
  
  // 重新扫描待处理任务
  const finalPendingTasks = scanPendingTasks(resolvedDir);
  
  await browser.disconnect();
  
  // 输出统计
  printStats(submittedCount, finalGeneratingCount, finalPendingTasks.length);
}

main().catch(console.error);
