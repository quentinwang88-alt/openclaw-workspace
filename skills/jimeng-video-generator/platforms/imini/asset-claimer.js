const { isModelSupportedOnImini, mapModelForImini, mapDurationForImini, mapRatioForImini, mapResolutionForImini } = require('./model-map');
const { recordChannelSuccess, recordChannelFailure } = require('../../channel-health');
const { parseContentIdMetadata, parseScriptIdMetadata, buildPromptFingerprint, describePromptMatch } = require('../../lib/asset-match');
const path = require('path');
const fs = require('fs');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const IMINI_ASSET_URL_KEY = 'imini';

async function claimIminiAsset(page, config, submission, claimPool) {
  await page.setViewport({ width: 1600, height: 1000 }).catch(() => {});
  try {
    return await claimIminiAssetOnPage(page, config, submission, claimPool);
  } catch (error) {
    const message = String(error?.message || error || '');
    if (!/detached Frame|Connection closed|Target closed|Session closed/i.test(message)) {
      throw error;
    }

    const browser = page.browser?.();
    if (!browser) {
      return { ok: false, reason: `imini 资产页连接失效: ${message}` };
    }

    console.log(`  🔄 imini 资产页连接失效，重开页面重试: ${message}`);
    const retryPage = await browser.newPage();
    try {
      await retryPage.setViewport({ width: 1600, height: 1000 }).catch(() => {});
      return await claimIminiAssetOnPage(retryPage, config, submission, claimPool);
    } finally {
      if (!retryPage.isClosed?.()) {
        await retryPage.close().catch(() => {});
      }
    }
  }
}

async function claimIminiAssetOnPage(page, config, submission, claimPool) {
  const iminiConfig = (config.channels || {}).imini || {};
  const assetUrl = iminiConfig.assetUrl || 'https://imini.com/zh/assets';

  await page.bringToFront().catch(() => {});

  try {
    await page.goto(assetUrl, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(error => {
      const msg = String(error?.message || error || '');
      if (!msg.includes('Navigation timeout')) throw error;
    });
  } catch (error) {
    const msg = String(error?.message || error || '');
    if (/detached Frame|Connection closed|Target closed|Session closed/i.test(msg)) {
      throw error;
    }
    return { ok: false, reason: `无法打开 imini 资产页: ${error.message}` };
  }

  await sleep(3000);
  await ensureIminiAssetPage(page);

  const switchedToVideo = await switchToVideoTab(page);
  if (!switchedToVideo) {
    return { ok: false, reason: '无法切换到 imini 资产页视频标签' };
  }

  const maxBatches = Math.max(1, Number(iminiConfig.assetScanBatches || config.assetScanBatches) || 1);
  const perBatchLimit = Math.max(1, Number(iminiConfig.maxAssetCandidates || config.maxAssetCandidates) || 10);
  let checked = 0;

  for (let batch = 0; batch < maxBatches; batch++) {
    const candidates = await listIminiVideoCandidates(page);
    if (candidates.length === 0) {
      return { ok: false, reason: 'imini 资产页未找到视频候选' };
    }

    const maxCandidates = Math.min(candidates.length, perBatchLimit);

    for (let i = 0; i < maxCandidates; i++) {
      const candidate = candidates[i];
      checked++;
      console.log(`  🎞️ 打开 imini 视频详情候选 batch=${batch + 1}/${maxBatches} item=${i + 1}/${maxCandidates}`);

      const clicked = await clickVideoCandidate(page, candidate);
      if (!clicked) continue;

      await sleep(2000);

      const detailInfo = await readIminiVideoDetail(page);
      if (!detailInfo || !detailInfo.prompt) {
        console.log('  ⏭️ imini 详情页未读取到提示词');
        await closeIminiDetail(page).catch(() => {});
        continue;
      }

      const poolSource = Array.isArray(claimPool) && claimPool.length > 0 ? claimPool : [submission];
      const match = findMatchingSubmissionForImini(poolSource, detailInfo.prompt, submission.trace_id, config);

      if (match.ok && match.submission) {
        if (detailInfo.failed) {
          console.log(`  🚫 imini 资产匹配到平台失败 (${match.by}) -> ${match.submission.trace_id}: ${detailInfo.failureReason || '生成失败'}`);
          await closeIminiDetail(page).catch(() => {});
          return {
            ok: false,
            platformFailed: true,
            matched: match,
            detailInfo,
            reason: detailInfo.failureReason || 'imini 平台返回生成失败'
          };
        }

        console.log(`  🔐 imini 资产匹配成功 (${match.by}) -> ${match.submission.trace_id}`);

        const downloaded = await downloadIminiVideo(page, config, match.submission);
        if (downloaded) {
          return {
            ok: true,
            matched: match,
            detailInfo,
            downloaded: true,
            filePath: downloaded
          };
        }

        return {
          ok: false,
          reason: 'imini 资产已匹配，但未能下载视频'
        };
      }

      await closeIminiDetail(page).catch(() => {});
      await sleep(1000);
    }

    if (batch < maxBatches - 1) {
      const scrolled = await scrollIminiAssetPage(page);
      if (!scrolled) {
        break;
      }
      await sleep(1800);
    }
  }

  return { ok: false, reason: `imini 资产页未匹配到结果 (已检查 ${checked} 个候选)` };
}

async function ensureIminiAssetPage(page) {
  const currentUrl = await page.url();
  const bodyHasAssetManager = await page.evaluate(() => {
    const text = String(document.body?.innerText || '');
    return text.includes('资产管理') || text.includes('批量操作') || text.includes('收藏');
  }).catch(() => false);

  if (currentUrl.includes('/assets') && bodyHasAssetManager) {
    return true;
  }

  const clicked = await page.evaluate(() => {
    const isVisible = el => {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
    };
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const candidates = Array.from(document.querySelectorAll('a, button, [role="button"], div, span'))
      .filter(isVisible)
      .filter(el => normalize(el.textContent || el.getAttribute('aria-label') || '') === '资产');
    if (candidates.length === 0) return false;
    const target = candidates.find(el => el.tagName === 'A') || candidates[0];
    target.click();
    return true;
  }).catch(() => false);

  if (clicked) {
    await sleep(3000);
  }

  return clicked;
}

async function switchToVideoTab(page) {
  try {
    const clicked = await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="tab"], a, div, span'))
        .filter(isVisible)
        .filter(el => {
          const text = normalize(el.textContent || el.getAttribute('aria-label') || '');
          return text === '视频' || text === '视频生成' || text === '所有视频';
        });

      if (candidates.length > 0) {
        candidates[0].click();
        return true;
      }
      return false;
    });

    if (clicked) await sleep(1500);
    return clicked;
  } catch (error) {
    return false;
  }
}

async function listIminiVideoCandidates(page) {
  try {
    const candidates = await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };

      return Array.from(document.querySelectorAll('video, img, canvas'))
        .filter(isVisible)
        .filter(el => {
          const rect = el.getBoundingClientRect();
          return rect.width >= 80 && rect.height >= 80;
        })
        .map((el, index) => {
          const rect = el.getBoundingClientRect();
          return {
            x: Math.round(rect.left + rect.width / 2),
            y: Math.round(rect.top + rect.height / 2),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
            index
          };
        })
        .slice(0, 20);
    });

    return candidates || [];
  } catch (error) {
    console.log(`  ⚠️ imini 视频候选列表获取失败: ${error.message}`);
    return [];
  }
}

async function scrollIminiAssetPage(page) {
  try {
    return await page.evaluate(() => {
      const beforeWindowY = window.scrollY;
      const visibleScrollers = Array.from(document.querySelectorAll('main, section, div'))
        .map(el => {
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          return {
            el,
            rect,
            scrollable: el.scrollHeight > el.clientHeight + 80,
            visible: style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 300 && rect.height > 300
          };
        })
        .filter(item => item.scrollable && item.visible)
        .sort((a, b) => (b.rect.width * b.rect.height) - (a.rect.width * a.rect.height));

      const target = visibleScrollers[0]?.el || document.scrollingElement || document.documentElement;
      const before = target.scrollTop;
      target.scrollBy({ top: Math.max(420, Math.floor(window.innerHeight * 0.75)), behavior: 'instant' });
      window.scrollBy({ top: Math.max(420, Math.floor(window.innerHeight * 0.75)), behavior: 'instant' });
      return target.scrollTop !== before || window.scrollY !== beforeWindowY;
    });
  } catch (error) {
    return false;
  }
}

async function clickVideoCandidate(page, candidate) {
  try {
    await page.mouse.click(candidate.x, candidate.y, { clickCount: 2, delay: 80 });
    await sleep(3000);
    return true;
  } catch (error) {
    return false;
  }
}

async function readIminiVideoDetail(page) {
  try {
    return await page.evaluate(() => {
      const bodyText = String(document.body?.innerText || '').replace(/\r/g, '');
      const promptIndex = bodyText.indexOf('提示词');
      const promptText = promptIndex >= 0 ? bodyText.slice(promptIndex, promptIndex + 12000) : bodyText.slice(0, 12000);
      const lines = bodyText.split('\n').map(line => line.replace(/\s+/g, ' ').trim()).filter(Boolean);
      const failureLine = lines.find(line => /失败原因[:：]/.test(line)) ||
        lines.find(line => /生成失败/.test(line) && /原因|因为|可能|参考图|输入图片/.test(line)) ||
        '';
      const failureReason = failureLine
        .replace(/^失败原因[:：]\s*/, '')
        .replace(/^生成失败[:：]?\s*/, '')
        .trim();
      const failed = /生成失败/.test(bodyText) && (
        /失败原因[:：]/.test(bodyText) ||
        /输入图片|参考图|真人|审核|拦截/.test(bodyText)
      );
      return {
        prompt: promptText,
        failed,
        failureReason,
        bodyPreview: bodyText.slice(0, 500),
        url: String(location.href || '')
      };
    });
  } catch (error) {
    return null;
  }
}

function findMatchingSubmissionForImini(submissions, detailPrompt, preferredTraceId, config) {
  const contentIdLabel = config.contentIdLabel || '内容ID';

  for (const submission of submissions) {
    const match = describePromptMatch(submission, detailPrompt, contentIdLabel);
    if (match.ok) {
      return {
        ok: true,
        submission,
        by: match.by,
        match
      };
    }
  }

  const detailContentIdMeta = parseContentIdMetadata(detailPrompt, contentIdLabel);
  return {
    ok: false,
    submission: null,
    match: null,
    detailContentId: detailContentIdMeta.id || '',
    detailPreview: buildPromptFingerprint(detailPrompt).preview
  };
}

async function downloadIminiVideo(page, config, submission) {
  const runtimeRoot = String(config.runtimeRoot || '~/Desktop/temp/jimeng-feishu-runtime').replace(/^~(?=$|\/)/, process.env.HOME || '~');
  const downloadDir = path.join(runtimeRoot, '_state', 'downloads');
  fs.mkdirSync(downloadDir, { recursive: true });

  try {
    const before = new Set(fs.readdirSync(downloadDir));
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath: downloadDir
    }).catch(() => {});

    const downloadButtons = await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      return Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))
        .filter(isVisible)
        .filter(el => {
          const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
          const aria = (el.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim();
          const title = (el.getAttribute('title') || '').replace(/\s+/g, ' ').trim();
          const cls = String(el.className || '');
          const html = String(el.innerHTML || '');
          const rect = el.getBoundingClientRect();
          const explicit = [text, aria, title].some(value => value.includes('下载') || value.includes('Download'));
          const likelyTopRightIcon = rect.top >= 20 && rect.top <= 170 && rect.left >= window.innerWidth - 220 && rect.width >= 24 && rect.width <= 80 && rect.height >= 24 && rect.height <= 80;
          const hasDownloadIcon = cls.includes('download') || html.includes('download') || html.includes('Download') || html.includes('icon-download');
          return explicit || (likelyTopRightIcon && hasDownloadIcon);
        })
        .map(el => {
          const rect = el.getBoundingClientRect();
          const text = (el.textContent || el.getAttribute('aria-label') || el.getAttribute('title') || '').replace(/\s+/g, ' ').trim();
          return {
            x: Math.round(rect.left + rect.width / 2),
            y: Math.round(rect.top + rect.height / 2),
            text: text
          };
        })
        .slice(0, 3);
    });

    let targetButton = downloadButtons[0] || null;
    if (!targetButton) {
      const topRightButtons = await page.evaluate(() => {
        const isVisible = el => {
          if (!el) return false;
          const style = getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
        };
        return Array.from(document.querySelectorAll('button, [role="button"]'))
          .filter(isVisible)
          .map(el => {
            const rect = el.getBoundingClientRect();
            return {
              x: Math.round(rect.left + rect.width / 2),
              y: Math.round(rect.top + rect.height / 2),
              left: Math.round(rect.left),
              top: Math.round(rect.top),
              width: Math.round(rect.width),
              height: Math.round(rect.height),
              text: (el.textContent || el.getAttribute('aria-label') || el.getAttribute('title') || '').replace(/\s+/g, ' ').trim()
            };
          })
          .filter(item => item.top >= 20 && item.top <= 170 && item.left >= window.innerWidth - 260 && item.width >= 24 && item.height >= 24)
          .sort((a, b) => a.left - b.left)
          .slice(0, 5);
      });
      targetButton = topRightButtons[0] || null;
    }

    if (!targetButton) {
      console.log('  ⚠️ imini 详情页未找到下载按钮');
      return null;
    }

    await page.mouse.click(targetButton.x, targetButton.y, { delay: 80 });
    const traceId = submission.trace_id || `imini-${Date.now()}`;
    const downloadedPath = await waitForDownloadedFile(downloadDir, before, 90000);
    if (!downloadedPath) {
      console.log('  ⚠️ imini 下载点击后未发现落盘文件');
      return null;
    }

    const ext = path.extname(downloadedPath) || '.mp4';
    const targetPath = path.join(downloadDir, `${traceId}${ext}`);
    if (downloadedPath !== targetPath) {
      fs.renameSync(downloadedPath, targetPath);
    }
    return targetPath;
  } catch (error) {
    console.log(`  ⚠️ imini 视频下载失败: ${error.message}`);
    return null;
  }
}

async function waitForDownloadedFile(downloadDir, before, timeoutMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const files = fs.readdirSync(downloadDir)
      .filter(name => !before.has(name))
      .filter(name => !name.endsWith('.crdownload') && !name.endsWith('.tmp'))
      .map(name => path.join(downloadDir, name))
      .filter(filePath => {
        try {
          return fs.statSync(filePath).size > 1024;
        } catch (error) {
          return false;
        }
      })
      .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);

    if (files.length > 0) {
      return files[0];
    }
    await sleep(1000);
  }
  return null;
}

async function closeIminiDetail(page) {
  try {
    await page.keyboard.press('Escape').catch(() => {});
    await sleep(500);
  } catch (error) {
    // best effort
  }
}

module.exports = {
  claimIminiAsset,
  switchToVideoTab,
  listIminiVideoCandidates,
  scrollIminiAssetPage,
  clickVideoCandidate,
  readIminiVideoDetail,
  findMatchingSubmissionForImini,
  downloadIminiVideo,
  ensureIminiAssetPage
};
