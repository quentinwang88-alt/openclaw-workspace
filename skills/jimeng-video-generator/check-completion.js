const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const { formatBeijingTimestamp } = require('./folder-processor');

// 默认配置
const DEFAULT_CONFIG = {
  cdpPort: 9222,
  baseUrl: 'https://jimeng.jianying.com/ai-tool/asset?workspace=0',
  checkInterval: 300000, // 5分钟检查一次
  maxWaitTime: 7200000, // 最长等待2小时
  outputDir: './output'
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 检查资产页的最新视频
 */
async function checkLatestVideos(page) {
  console.log('  检查资产页...');
  
  await page.goto('https://jimeng.jianying.com/ai-tool/asset?workspace=0', { 
    waitUntil: 'networkidle2', 
    timeout: 30000 
  });
  await sleep(2000);
  
  // 获取最新视频列表
  const videos = await page.evaluate(() => {
    const results = [];
    
    // 查找所有视频卡片（按日期分组）
    const dateGroups = document.querySelectorAll('[class*="videoList"], [class*="dateGroup"]');
    
    // 简单方式：获取所有带时长的元素
    const allVideos = document.querySelectorAll('[class*="video"]');
    
    // 获取页面文本中的日期和时长
    const body = document.body.innerHTML;
    const dateMatches = body.match(/\d+月\d+日/g) || [];
    const durationMatches = body.match(/\d{2}:\d{2}/g) || [];
    
    // 获取生成中的数量（从菜单显示）
    const generatingBadge = document.querySelector('[class*="badge"], [class*="count"]');
    const generatingCount = generatingBadge ? parseInt(generatingBadge.textContent) : 0;
    
    return {
      dateCount: dateMatches.length,
      videoCount: durationMatches.length,
      latestDate: dateMatches[0] || '',
      latestDurations: durationMatches.slice(0, 5),
      generatingCount
    };
  });
  
  console.log(`  最新日期: ${videos.latestDate}`);
  console.log(`  视频数量: ${videos.videoCount}`);
  console.log(`  最新时长: ${videos.latestDurations.join(', ')}`);
  console.log(`  生成中: ${videos.generatingCount}`);
  
  return videos;
}

/**
 * 检查任务是否完成
 * 通过比较提交前后的视频数量和最新日期
 */
async function checkTaskCompletion(page, taskFolder) {
  const submittedFile = path.join(taskFolder, '.submitted');
  const completedFile = path.join(taskFolder, '.completed');
  
  if (!fs.existsSync(submittedFile)) {
    return { status: 'not_submitted' };
  }
  
  if (fs.existsSync(completedFile)) {
    return { status: 'completed' };
  }
  
  // 读取提交信息
  const submittedInfo = JSON.parse(fs.readFileSync(submittedFile, 'utf8'));
  const submittedTime = new Date(submittedInfo.time);
  const submittedDate = submittedInfo.latestDate;
  const submittedCount = submittedInfo.videoCount || 0;
  
  // 获取当前资产状态
  const currentVideos = await checkLatestVideos(page);
  
  // 判断是否完成：
  // 1. 生成中队列为0
  // 2. 且有新视频出现（视频数量增加或日期更新）
  const waitTime = Date.now() - submittedTime.getTime();
  const waitMinutes = Math.round(waitTime / 60000);
  
  console.log(`  提交时间: ${submittedTime.toLocaleString()}`);
  console.log(`  等待时长: ${waitMinutes} 分钟`);
  console.log(`  提交时视频数: ${submittedCount}, 当前: ${currentVideos.videoCount}`);
  
  // 如果生成中数量为0，且视频数量增加了
  if (currentVideos.generatingCount === 0 && currentVideos.videoCount > submittedCount) {
    return { 
      status: 'completed',
      waitMinutes,
      newVideoCount: currentVideos.videoCount - submittedCount
    };
  }
  
  // 如果还在生成中
  if (currentVideos.generatingCount > 0) {
    return { 
      status: 'generating',
      waitMinutes,
      queuePosition: currentVideos.generatingCount
    };
  }
  
  // 超时检查
  if (waitTime > DEFAULT_CONFIG.maxWaitTime) {
    return { 
      status: 'timeout',
      waitMinutes 
    };
  }
  
  return { 
    status: 'waiting',
    waitMinutes 
  };
}

/**
 * 主函数
 */
async function main() {
  const dataDir = process.argv[2] || process.env.JIMENG_DATA_DIR || '~/Desktop/jimeng';
  const resolvedDir = dataDir.replace('~', process.env.HOME);
  
  console.log('🎬 即梦视频生成 - 完成状态检测');
  console.log('================================\n');
  console.log(`📂 数据目录: ${resolvedDir}`);
  console.log(`📅 ${new Date().toLocaleString()}\n`);
  
  // 连接浏览器
  console.log('🔗 连接浏览器...');
  let browser;
  try {
    browser = await puppeteer.connect({
      browserURL: `http://localhost:${DEFAULT_CONFIG.cdpPort}`,
      defaultViewport: null,
      timeout: 30000,
      protocolTimeout: 120000
    });
    console.log('✅ 已连接浏览器\n');
  } catch (e) {
    console.log(`❌ 无法连接浏览器: ${e.message}`);
    process.exit(1);
  }
  
  const page = (await browser.pages()).find(p => p.url().includes('jimeng'));
  if (!page) {
    console.log('❌ 未找到即梦页面');
    await browser.disconnect();
    process.exit(1);
  }
  
  // 扫描所有任务文件夹
  const items = fs.readdirSync(resolvedDir);
  const tasks = [];
  
  for (const item of items) {
    const folder = path.join(resolvedDir, item);
    if (fs.statSync(folder).isDirectory() && !item.startsWith('.')) {
      const submittedFile = path.join(folder, '.submitted');
      const completedFile = path.join(folder, '.completed');
      
      if (fs.existsSync(submittedFile) && !fs.existsSync(completedFile)) {
        tasks.push({
          name: item,
          folder,
          submittedInfo: JSON.parse(fs.readFileSync(submittedFile, 'utf8'))
        });
      }
    }
  }
  
  console.log(`📋 待检查任务: ${tasks.length} 个\n`);
  
  if (tasks.length === 0) {
    console.log('✅ 没有待检查的任务');
    await browser.disconnect();
    return;
  }
  
  // 检查每个任务
  let completedCount = 0;
  let generatingCount = 0;
  let waitingCount = 0;
  
  for (const task of tasks) {
    console.log(`\n🎬 检查任务: ${task.name}`);
    
    const result = await checkTaskCompletion(page, task.folder);
    
    switch (result.status) {
      case 'completed':
        console.log(`  ✅ 已完成！等待了 ${result.waitMinutes} 分钟`);
        // 标记完成
        const completedFile = path.join(task.folder, '.completed');
        fs.writeFileSync(completedFile, JSON.stringify({
          time: formatBeijingTimestamp(),
          waitMinutes: result.waitMinutes
        }));
        // 删除提交标记
        fs.unlinkSync(path.join(task.folder, '.submitted'));
        completedCount++;
        break;
        
      case 'generating':
        console.log(`  ⏳ 生成中，队列位置: ${result.queuePosition}`);
        console.log(`  已等待 ${result.waitMinutes} 分钟`);
        generatingCount++;
        break;
        
      case 'timeout':
        console.log(`  ⏰ 超时！已等待 ${result.waitMinutes} 分钟`);
        console.log(`  请手动检查是否生成成功`);
        waitingCount++;
        break;
        
      case 'waiting':
        console.log(`  ⏳ 等待中，已等待 ${result.waitMinutes} 分钟`);
        waitingCount++;
        break;
    }
  }
  
  await browser.disconnect();
  
  console.log('\n================================');
  console.log('📊 检测统计');
  console.log('================================');
  console.log(`✅ 已完成: ${completedCount}`);
  console.log(`⏳ 生成中: ${generatingCount}`);
  console.log(`⏰ 等待中: ${waitingCount}`);
}

main().catch(console.error);
