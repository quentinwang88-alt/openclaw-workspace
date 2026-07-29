#!/usr/bin/env node

const puppeteer = require('puppeteer-core');

async function main() {
  const browser = await puppeteer.connect({ browserURL: 'http://127.0.0.1:9222' });
  const pages = await browser.pages();
  const page = pages.find((candidate) => /imini\.com\/zh\/video/.test(candidate.url()));
  if (!page) throw new Error('未找到 Imini 视频生成页面');

  const result = await page.evaluate(async () => {
    const summarizeStorage = (storage) => Array.from({ length: storage.length }, (_, index) => {
      const key = storage.key(index);
      const value = storage.getItem(key) || '';
      return {
        key,
        length: value.length,
        relevant: /DETAILAB_20260723|2080112979280203776|\.mp4/i.test(value),
        value: /DETAILAB_20260723|2080112979280203776|\.mp4/i.test(value) ? value.slice(0, 20000) : undefined,
      };
    });
    const databases = indexedDB.databases ? await indexedDB.databases() : [];
    return {
      localStorage: summarizeStorage(localStorage),
      sessionStorage: summarizeStorage(sessionStorage),
      databases,
      scripts: Array.from(document.scripts).map((script) => script.src).filter(Boolean),
    };
  });

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  await browser.disconnect();
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});
