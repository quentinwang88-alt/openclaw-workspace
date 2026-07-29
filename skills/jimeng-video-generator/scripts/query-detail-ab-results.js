#!/usr/bin/env node

const puppeteer = require('puppeteer-core');

async function main() {
  const browser = await puppeteer.connect({ browserURL: 'http://127.0.0.1:9222' });
  const pages = await browser.pages();
  const page = pages.find((candidate) => /imini\.com\/zh\/asset/.test(candidate.url()))
    || pages.find((candidate) => /imini\.com\/zh\/video/.test(candidate.url()));
  if (!page) throw new Error('未找到 Imini 页面');
  await page.waitForSelector('body', { timeout: 60000 });
  await new Promise((resolve) => setTimeout(resolve, 5000));

  const result = await page.evaluate(async () => {
    let webpackRequire;
    const webpackChunks = window.webpackChunk_N_E || self.webpackChunk_N_E;
    if (!webpackChunks) {
      return [{
        ok: false,
        error: 'webpack runtime unavailable',
        readyState: document.readyState,
        scripts: Array.from(document.scripts).map((script) => script.src).filter(Boolean),
        webpackGlobals: Object.keys(window).filter((key) => /webpack/i.test(key)),
      }];
    }
    webpackChunks.push([
      [`detail-ab-${Date.now()}`],
      {},
      (requireFunction) => { webpackRequire = requireFunction; },
    ]);
    if (!webpackRequire) throw new Error('无法取得 Imini 页面模块加载器');
    const api = webpackRequire(37680);
    if (!api || typeof api.vbp !== 'function') throw new Error('用户任务列表接口模块不可用');

    const bodies = [
      { current: 1, size: 50 },
      { pageNum: 1, pageSize: 50 },
      { page: 1, size: 50 },
      { pageNo: 1, pageSize: 50 },
    ];
    const attempts = [];
    for (const body of bodies) {
      try {
        const response = await api.vbp({ body });
        const payload = response.data || response.error || null;
        const serialized = JSON.stringify(payload);
        const records = (payload && payload.data && Array.isArray(payload.data.records)) ? payload.data.records : [];
        const detailRecords = records.filter((record) => String(record && record.param && record.param.prompt || '').includes('DETAILAB_20260723_V1'));
        attempts.push({
          body,
          ok: Boolean(response.data),
          detailRecords: detailRecords.map((record) => ({
            taskId: record.task_id,
            status: record.status,
            failureReason: record.failure_reason,
            promptId: (String(record.param && record.param.prompt || '').match(/【内容ID】\s*-\s*([^\s]+)/) || [])[1] || '',
            createTime: record.create_time,
            results: Array.isArray(record.task_result) ? record.task_result.map((item) => ({
              assetId: item.asset_id,
              createTime: item.create_time,
              video: item.video || null,
            })) : [],
          })),
          total: payload && payload.data ? payload.data.total : null,
        });
        if (detailRecords.length > 0 || /DETAILAB_20260723_V1|2080112979280203776|2080116020651102208/.test(serialized)) break;
      } catch (error) {
        attempts.push({ body, ok: false, error: String(error && (error.stack || error.message) || error) });
      }
    }
    return attempts;
  });

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  await browser.disconnect();
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});
