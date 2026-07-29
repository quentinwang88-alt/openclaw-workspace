#!/usr/bin/env node

const puppeteer = require('puppeteer-core');

const TASK_IDS = [
  'DETAILAB_20260723_V1_S1_A_R1',
  'DETAILAB_20260723_V1_S1_A_R2',
  'DETAILAB_20260723_V1_S1_B_R1',
  'DETAILAB_20260723_V1_S1_B_R2',
  'DETAILAB_20260723_V1_S1_C_R1',
  'DETAILAB_20260723_V1_S1_C_R2',
  'DETAILAB_20260723_V1_S1_D_R1',
  'DETAILAB_20260723_V1_S1_D_R2',
  'DETAILAB_20260723_V1_S2_E_R1',
  'DETAILAB_20260723_V1_S2_E_R2',
  'DETAILAB_20260723_V1_S2_F_R1',
  'DETAILAB_20260723_V1_S2_F_R2',
];

async function main() {
  const browser = await puppeteer.connect({ browserURL: 'http://127.0.0.1:9222' });
  const pages = await browser.pages();
  const page = pages.find((candidate) => /imini\.com\/zh\/video/.test(candidate.url()));
  if (!page) throw new Error('未找到 Imini 视频生成页面');

  const results = [];
  for (const taskId of TASK_IDS) {
    const located = await page.evaluate((target) => {
      const candidates = Array.from(document.querySelectorAll('[data-id]'))
        .filter((node) => (node.textContent || '').includes(target))
        .sort((a, b) => (a.textContent || '').length - (b.textContent || '').length);
      const card = candidates[0];
      if (!card) return null;
      card.scrollIntoView({ block: 'center', inline: 'nearest' });
      return { dataId: card.getAttribute('data-id') || '' };
    }, taskId);
    if (!located) {
      results.push({ taskId, found: false });
      continue;
    }

    await new Promise((resolve) => setTimeout(resolve, 900));
    const result = await page.evaluate((target) => {
      const candidates = Array.from(document.querySelectorAll('[data-id]'))
        .filter((node) => (node.textContent || '').includes(target))
        .sort((a, b) => (a.textContent || '').length - (b.textContent || '').length);
      const card = candidates[0];
      if (!card) return { taskId: target, found: false };
      return {
        taskId: target,
        found: true,
        dataId: card.getAttribute('data-id') || '',
        textStart: (card.textContent || '').trim().slice(0, 220),
        media: Array.from(card.querySelectorAll('img,video,source')).map((el) => ({
          tag: el.tagName,
          src: el.currentSrc || el.src || el.getAttribute('src') || '',
          poster: el.getAttribute('poster') || '',
        })).filter((item) => item.src || item.poster),
      };
    }, taskId);
    results.push(result);
  }

  process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
  await browser.disconnect();
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});
