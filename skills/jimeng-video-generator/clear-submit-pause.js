#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { expandHome, getStateRoot } = require('./trace-state');

function loadConfig(configPath) {
  const raw = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  raw.runtimeRoot = expandHome(raw.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime');
  return raw;
}

function main() {
  const configArgIndex = process.argv.indexOf('--config');
  const configPath = configArgIndex >= 0
    ? path.resolve(process.argv[configArgIndex + 1])
    : path.resolve(__dirname, 'feishu-direct.json');

  const config = loadConfig(configPath);
  const pauseFile = path.join(getStateRoot(config), 'submit-paused-insufficient-credits.json');

  if (!fs.existsSync(pauseFile)) {
    console.log(`未发现提单暂停文件: ${pauseFile}`);
    return;
  }

  fs.unlinkSync(pauseFile);
  console.log(`已清除提单暂停文件: ${pauseFile}`);
  console.log('现在可以重新触发 submit 线。');
}

main();
