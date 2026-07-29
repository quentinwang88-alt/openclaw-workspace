#!/usr/bin/env node

const puppeteer = require('puppeteer-core');

const NEEDLES = [
  'DETAILAB_20260723_V1',
  '2080112979280203776',
  '2080113346812551168',
  '2080113500705759232',
  '2080113648728870912',
  '2080113802167164928',
  '2080114257760854016',
  '2080114686215131136',
  '2080115115221127168',
  '2080116020651102208',
];

function collectMatchingObjects(value, path = '$', result = [], depth = 0) {
  if (depth > 16 || result.length >= 80 || value == null) return result;
  if (Array.isArray(value)) {
    value.forEach((item, index) => collectMatchingObjects(item, `${path}[${index}]`, result, depth + 1));
    return result;
  }
  if (typeof value !== 'object') return result;

  let serialized = '';
  try {
    serialized = JSON.stringify(value);
  } catch (_) {
    serialized = '';
  }
  if (NEEDLES.some((needle) => serialized.includes(needle))) {
    if (serialized.length <= 16000) result.push({ path, value });
    for (const [key, child] of Object.entries(value)) {
      collectMatchingObjects(child, `${path}.${key}`, result, depth + 1);
    }
  }
  return result;
}

async function main() {
  const browser = await puppeteer.connect({ browserURL: 'http://127.0.0.1:9222' });
  const pages = await browser.pages();
  const page = pages.find((candidate) => /imini\.com\/zh\/video/.test(candidate.url()));
  if (!page) throw new Error('未找到 Imini 视频生成页面');

  const captures = [];
  page.on('response', async (response) => {
    try {
      const headers = response.headers();
      const contentType = String(headers['content-type'] || '');
      if (!/json|text/.test(contentType)) return;
      const text = await response.text();
      const isTaskEndpoint = /featureApi\/v2\/featurePage\/tasks/.test(response.url());
      const isPageDataEndpoint = /imini\.com\/api\/clVe/.test(response.url());
      if (!isTaskEndpoint && !isPageDataEndpoint && !NEEDLES.some((needle) => text.includes(needle))) return;
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch (_) {
        parsed = text.slice(0, 16000);
      }
      captures.push({
        url: response.url(),
        status: response.status(),
        matches: typeof parsed === 'string' ? parsed : (isPageDataEndpoint ? parsed : collectMatchingObjects(parsed)),
      });
    } catch (_) {
      // Some cached/streaming response bodies cannot be read; ignore them.
    }
  });

  await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 });
  await new Promise((resolve) => setTimeout(resolve, 12000));
  const resources = await page.evaluate(() => performance.getEntriesByType('resource')
    .map((entry) => entry.name)
    .filter((name) => /api|task|history|record|generate|works|asset|creation/i.test(name)));
  process.stdout.write(`${JSON.stringify({ captures, resources }, null, 2)}\n`);
  await browser.disconnect();
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});
