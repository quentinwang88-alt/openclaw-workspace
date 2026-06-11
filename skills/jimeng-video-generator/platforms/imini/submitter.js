const fs = require('fs');
const path = require('path');
const { mapModelForImini, mapDurationForImini, mapRatioForImini, mapResolutionForImini } = require('./model-map');
const { recordChannelSuccess, recordChannelFailure } = require('../../channel-health');
const { buildProductLockCard, formatProductLockCard, generateProductLockCardWithLLM, enrichProductLockCardWithLLM } = require('./product-lock');
const { buildFirstFramePrompt, generateFirstFrameImageWithLLM } = require('./first-frame');
const { checkFirstFrameConsistency } = require('./consistency-checker');
const { detectCategory } = require('./category-rules');
const { extractHardConstraints } = require('./product-lock');
const { expandHome } = require('../../trace-state');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let _createButtonAlreadyClicked = false;

async function submitToImini(page, context, productLock, firstFrameImagePath, config) {
  // No retry: once we click 创建, a submission may have gone through.
  // Retrying would create duplicate tasks and waste credits.
  _createButtonAlreadyClicked = false;
  try {
    const result = await attemptSubmit(page, context, productLock, firstFrameImagePath, config);
    return result;
  } catch (error) {
    console.log(`  ⚠️ imini 提交异常: ${error.message}`);
    return {
      success: false,
      error: error.message || '提交失败',
      code: 'submit_failed'
    };
  }
}

async function attemptSubmit(page, context, productLock, firstFrameImagePath, config) {
  await page.bringToFront().catch(() => {});

  const iminiConfig = (config.channels || {}).imini || {};
  const preflightOnly = iminiConfig.preflightOnly === true || process.env.IMINI_PREFLIGHT_ONLY === '1';
  if (!preflightOnly && iminiConfig.allowRealSubmit !== true && process.env.IMINI_ALLOW_REAL_SUBMIT !== '1') {
    return {
      success: false,
      error: 'imini 真实提交默认关闭，必须显式开启 allowRealSubmit 或 IMINI_ALLOW_REAL_SUBMIT=1',
      code: 'real_submit_disabled'
    };
  }

  const assetUrl = iminiConfig.assetUrl || 'https://imini.com/zh/assets';
  const baseUrl = iminiConfig.baseUrl || 'https://imini.com/zh/tools/ai-video';

  await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(error => {
    const msg = String(error?.message || error || '');
    if (!msg.includes('Navigation timeout')) throw error;
  });
  await sleep(3000);

  const modeSwitched = await switchToVideoCreation(page);
  if (!modeSwitched) {
    return { success: false, error: '无法切换到视频创作模式', code: 'mode_switch_failed' };
  }

  const imageModeSwitched = await switchToImageToVideo(page);
  if (!imageModeSwitched) {
    return { success: false, error: '无法切换到图片转视频模式', code: 'mode_switch_failed' };
  }

  const modelMapped = mapModelForImini(context.model);
  if (modelMapped) {
    const modelSwitched = await selectModel(page, modelMapped);
    if (!modelSwitched) {
      console.log(`  ⚠️ imini 模型选择失败: ${modelMapped}，使用默认模型`);
    }
  }

  if (firstFrameImagePath) {
    const uploaded = await uploadFirstFrameImage(page, firstFrameImagePath);
    if (!uploaded) {
      return { success: false, error: '首帧图片上传失败', code: 'upload_failed' };
    }
  }

  const promptFilled = await fillPrompt(page, context.prompt);
  if (!promptFilled) {
    return { success: false, error: 'imini 提示词填充失败或填充不完整，停止提交，避免沿用旧提示词', code: 'prompt_fill_failed' };
  }

  console.log('  imini 等待比例/分辨率控件出现...');
  await page.waitForFunction(() => {
    const els = document.querySelectorAll('button, div, span');
    for (const el of els) {
      const rect = el.getBoundingClientRect();
      const text = (el.textContent || '').trim();
      if (rect.y > 700 && rect.width > 30 && rect.width < 120 && rect.height > 14 && rect.height < 50) {
        if (/^\d+:\d+$/.test(text) || /^\d{3,4}P$/i.test(text)) {
          return true;
        }
      }
    }
    return false;
  }, { timeout: 10000 }).catch(() => {
    console.log('  ⚠️ imini 比例/分辨率控件未在10秒内出现');
  });

  const durationMapped = mapDurationForImini(context.duration);
  await selectDuration(page, durationMapped);

  const ratioMapped = mapRatioForImini(context.ratio);
  await selectRatio(page, ratioMapped);

  const resolutionMapped = mapResolutionForImini(iminiConfig.defaultResolution || '480P');
  await selectResolution(page, resolutionMapped);

  await sleep(1000);

  const existingSubmission = await findExistingVisibleSubmission(page, context);
  if (existingSubmission.found && iminiConfig.allowDuplicateVisible !== true && process.env.IMINI_ALLOW_DUPLICATE_VISIBLE !== '1') {
    console.log(`  imini 页面已存在同脚本任务卡，按已提交处理，停止重复点击: ${existingSubmission.scriptId}`);
    return {
      success: true,
      code: 'existing_visible_card',
      detail: existingSubmission.text || ''
    };
  } else if (existingSubmission.found) {
    console.log(`  ⚠️ imini 页面右侧已存在同脚本任务，但本次显式允许重复提交: ${existingSubmission.scriptId}`);
  }

  // Pre-submit verification: check model, ratio, resolution, prompt before clicking
  const preCheck = await page.evaluate(() => {
    const result = { model: '', ratio: '', resolution: '', duration: '', promptLength: 0, promptText: '' };
    const allEls = document.querySelectorAll('div, span, button');
    for (const el of allEls) {
      const rect = el.getBoundingClientRect();
      const text = (el.textContent || '').trim();
      const cls = String(el.className || '');
      if (rect.y > 90 && rect.y < 180 && rect.x > 90 && rect.x < 450 && rect.width > 100 && rect.height > 15 && rect.height < 60) {
        if (/Sora|Seedance|Kling|Wan|Veo/i.test(text) && text.length < 80) {
          result.model = text;
        } else if (!result.model && /模型/i.test(text) && text.length < 40) {
          result.model = text;
        }
      }
    }
    if (!/Sora|Seedance|Kling|Wan|Veo/i.test(result.model)) {
      const modelCandidates = Array.from(document.querySelectorAll('div, span'))
        .map(el => {
          const rect = el.getBoundingClientRect();
          const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
          return { text, rect };
        })
        .filter(item =>
          item.rect.y > 90 && item.rect.y < 190 &&
          item.rect.x > 80 && item.rect.x < 460 &&
          item.rect.width > 80 && item.rect.height > 15 &&
          /Sora|Seedance|Kling|Wan|Veo/i.test(item.text)
        )
        .sort((a, b) => a.text.length - b.text.length);
      if (modelCandidates[0]) result.model = modelCandidates[0].text;
    }
    const visualPicker = document.querySelector('.ToolForm_visual-config-picker__6ysqT');
    const selects = visualPicker
      ? Array.from(visualPicker.querySelectorAll('.imini-select')).filter(el => {
          const rect = el.getBoundingClientRect();
          return rect.width > 20 && rect.height > 14;
        })
      : [];
    for (const select of selects) {
      const text = (select.querySelector('.imini-select-selection-item, .imini-select-content-has-value')?.textContent || select.textContent || '').trim();
      if (/^\d+s$/.test(text)) result.duration = text;
      if (/^\d+:\d+$/.test(text)) result.ratio = text;
      if (/^\d{3,4}P$/i.test(text)) result.resolution = text;
    }
    if (!result.duration || !result.ratio || !result.resolution) {
      for (const el of allEls) {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').trim();
        const cls = String(el.className || '');
        if (!cls.includes('imini-select-selection-item')) continue;
        if (rect.x > 80 && rect.x < 460 && rect.y > window.innerHeight - 240 && rect.width > 20 && rect.width < 160 && rect.height > 14 && rect.height < 60) {
          if (/^\d+s$/.test(text)) result.duration = text;
          if (/^\d+:\d+$/.test(text)) result.ratio = text;
          if (/^\d{3,4}P$/i.test(text)) result.resolution = text;
        }
      }
    }
    const ta = document.querySelector('textarea, [role="textbox"]');
    if (ta) {
      result.promptText = (ta.value || ta.textContent || '').trim();
      result.promptLength = result.promptText.length;
    }
    return result;
  });
  console.log(`  imini 提交前检: model=${preCheck.model}, ratio=${preCheck.ratio}, res=${preCheck.resolution}, dur=${preCheck.duration}, prompt=${preCheck.promptLength}字`);

  if (preCheck.promptLength < 10) {
    return { success: false, error: `提示词为空或太短(${preCheck.promptLength}字)，不提交`, code: 'prompt_too_short' };
  }
  const promptMatchRatio = context.prompt ? preCheck.promptText.length / context.prompt.length : 0;
  const normalizePromptForCompare = value => String(value || '').replace(/\s+/g, ' ').trim();
  const expectedPromptCompact = normalizePromptForCompare(context.prompt);
  const actualPromptCompact = normalizePromptForCompare(preCheck.promptText);
  const expectedAnchor = expectedPromptCompact.slice(0, 80);
  if (promptMatchRatio < 0.8 || (expectedAnchor && !actualPromptCompact.includes(expectedAnchor))) {
    return {
      success: false,
      error: `提示词提交前校验不一致，期望${context.prompt.length}字，页面${preCheck.promptLength}字，停止提交`,
      code: 'prompt_precheck_mismatch'
    };
  }
  const settingMismatch = validateIminiPreCheck(preCheck, {
    model: modelMapped,
    ratio: ratioMapped,
    resolution: resolutionMapped,
    duration: `${durationMapped}s`
  });
  if (settingMismatch) {
    return { success: false, error: settingMismatch, code: 'precheck_mismatch', preCheck };
  }

  if (preflightOnly) {
    return {
      success: true,
      code: 'preflight_ok',
      preCheck
    };
  }

  await waitForReadyToCreate(page, { hasFirstFrame: !!firstFrameImagePath, timeoutMs: 30000 });

  const clicked = await clickGenerateButton(page);
  if (!clicked) {
    return { success: false, error: '无法点击生成按钮', code: 'generate_button_not_found' };
  }

  await sleep(5000);

  const submitResult = await checkSubmitResult(page, context);
  return submitResult;
}

function extractScriptId(context) {
  const candidates = [
    context?.taskName,
    context?.promptPackageId,
    context?.contentId,
    context?.scriptId,
    context?.prompt
  ];
  for (const value of candidates) {
    const text = String(value || '');
    const match = text.match(/\b\d{2,5}_M\d+_[A-Z0-9]+(?:_V\d+)?\b/i) ||
      text.match(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i) ||
      text.match(/\bSPK-[A-Z0-9-]{6,64}\b/i);
    if (match) return match[0];
  }
  return '';
}

async function findExistingVisibleSubmission(page, context) {
  const scriptId = extractScriptId(context);
  if (!scriptId) return { found: false, scriptId: '' };

  return page.evaluate((id) => {
    const candidates = Array.from(document.querySelectorAll('div, section, article, li'));
    for (const el of candidates) {
      const rect = el.getBoundingClientRect();
      if (rect.x < 450 || rect.width < 300 || rect.height < 40) continue;
      const text = String(el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!text.includes(id)) continue;
      if (/Image To Video|Seedance|生成中|排队|任务添加成功|重新生成|删除/.test(text)) {
        return { found: true, scriptId: id, text: text.slice(0, 160) };
      }
    }
    return { found: false, scriptId: id };
  }, scriptId);
}

async function readSubmitSignals(page, context) {
  const scriptId = extractScriptId(context);
  return page.evaluate((id) => {
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const bodyText = normalize(document.body?.innerText || '');
    const visibleElements = Array.from(document.querySelectorAll('button, [role="alert"], [role="status"], [role="dialog"], div, section, article, li, span'))
      .map(el => {
        const rect = el.getBoundingClientRect();
        return {
          text: normalize(el.innerText || el.textContent || el.getAttribute('aria-label') || ''),
          role: el.getAttribute('role') || '',
          x: rect.left,
          y: rect.top,
          width: rect.width,
          height: rect.height
        };
      })
      .filter(item => item.text && item.width > 0 && item.height > 0);

    const failurePatterns = [
      /积分不足/i, /余额不足/i, /credits? not enough/i,
      /生成失败/i, /提交失败/i, /操作失败/i,
      /超过.*限制/i, /速率限制/i, /rate.?limit/i,
      /输入图片.*真人/i, /可能包含真人/i, /参考图.*真人/i,
      /审核未通过/i, /违规/i, /敏感内容/i, /not allowed/i, /unsafe/i
    ];
    const isNoticeLike = item =>
      /alert|status|dialog/i.test(item.role) ||
      (item.text.length <= 300 && item.x > 160 && item.y > 60 && item.y < window.innerHeight - 40);
    const failureHit = visibleElements.find(item =>
      failurePatterns.some(pattern => pattern.test(item.text)) &&
      (isNoticeLike(item) || (id && item.text.includes(id)))
    );
    if (failureHit) {
      return {
        success: false,
        failed: true,
        error: failureHit.text.slice(0, 240),
        code: 'platform_error'
      };
    }

    const scriptCard = id
      ? visibleElements.find(item => {
          if (item.x < 430 || item.width < 240 || item.height < 35) return false;
          if (!item.text.includes(id)) return false;
          return /Image To Video|Seedance|生成中|排队|任务添加成功|重新生成|删除|480P|15s/.test(item.text);
        })
      : null;
    if (scriptCard) {
      return {
        success: true,
        code: 'submitted_visible_card',
        detail: scriptCard.text.slice(0, 180)
      };
    }

    const successPatterns = [
      /生成中/i, /排队中/i, /任务添加成功/i, /已提交/i, /正在进行/i,
      /generating/i, /processing/i, /queued/i
    ];
    const successHit = visibleElements.find(item =>
      successPatterns.some(pattern => pattern.test(item.text)) &&
      (isNoticeLike(item) || (id && item.text.includes(id)))
    );
    if (successHit) {
      return {
        success: true,
        code: 'submitted',
        detail: successHit.text.slice(0, 180)
      };
    }

    const createButtonText = visibleElements
      .filter(item => item.x > 70 && item.x < 520 && item.y > window.innerHeight - 160)
      .map(item => item.text)
      .find(text => /^创建|^生成|^开始|^Generate/i.test(text));

    return {
      success: false,
      failed: false,
      code: 'submit_unverified',
      createButtonText: createButtonText || '',
      bodyPreview: bodyText.slice(0, 240)
    };
  }, scriptId);
}

async function switchToVideoCreation(page) {
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
        .map(el => ({ el, text: normalize(el.textContent || el.getAttribute('aria-label') || '') }))
        .filter(item => item.text.includes('视频创作') || item.text.includes('视频'))
        .sort((a, b) => a.text.length - b.text.length);

      if (candidates.length > 0) {
        const target = candidates[0].el.closest('button, [role="tab"], a') || candidates[0].el;
        target.click();
        return true;
      }
      return false;
    });
    if (clicked) await sleep(2000);
    return clicked;
  } catch (error) {
    console.log(`  ⚠️ 切换视频创作模式失败: ${error.message}`);
    return false;
  }
}

async function switchToImageToVideo(page) {
  try {
    const clicked = await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('button, [role="tab"], [role="combobox"], a, div, span'))
        .filter(isVisible)
        .map(el => ({ el, text: normalize(el.textContent || el.getAttribute('aria-label') || '') }))
        .filter(item =>
          item.text.includes('图片转视频') ||
          item.text.includes('图生视频') ||
          item.text.includes('首帧')
        )
        .sort((a, b) => a.text.length - b.text.length);

      if (candidates.length > 0) {
        const target = candidates[0].el.closest('button, [role="tab"], [role="combobox"], a') || candidates[0].el;
        target.click();
        return true;
      }
      return false;
    });
    if (clicked) await sleep(2000);
    return clicked;
  } catch (error) {
    console.log(`  ⚠️ 切换图片转视频模式失败: ${error.message}`);
    return false;
  }
}

async function selectModel(page, modelName) {
  const targetLower = String(modelName || '').toLowerCase().replace(/\s+/g, ' ').trim();

  try {
    const currentModel = await readCurrentIminiModel(page);
    if (modelMatchesTarget(currentModel, modelName)) {
      console.log(`  imini 模型已为 ${modelName}，跳过`);
      return true;
    }

    // Step 1: Find and click the compact model selector in the left tool form.
    const modelArea = await page.evaluate(() => {
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
      const candidates = Array.from(document.querySelectorAll('.cursor-pointer, .imini-form-item, .imini-form-item-control-input-content, div'))
        .map(el => {
          const rect = el.getBoundingClientRect();
          return { el, rect, text: normalize(el.innerText || el.textContent || '') };
        })
        .filter(item =>
          item.rect.x > 80 && item.rect.x < 460 &&
          item.rect.y > 85 && item.rect.y < 260 &&
          item.rect.width > 180 && item.rect.height > 20 && item.rect.height < 90 &&
          /模型|Sora|Seedance|Kling|Wan|Veo/i.test(item.text)
        )
        .sort((a, b) => {
          const aText = /模型/.test(a.text) ? 0 : 1;
          const bText = /模型/.test(b.text) ? 0 : 1;
          if (aText !== bText) return aText - bText;
          return (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height);
        });
      const target = candidates.find(item => item.el.className && String(item.el.className).includes('cursor-pointer')) || candidates[0];
      if (!target) return { found: false };
      const clickTarget = target.el.closest('.cursor-pointer, button, [role="button"]') || target.el;
      clickTarget.click();
      return {
        found: true,
        x: Math.round(target.rect.left + target.rect.width / 2),
        y: Math.round(target.rect.top + target.rect.height / 2),
        text: target.text.slice(0, 60)
      };
    });

    if (!modelArea.found) {
      console.log('  ⚠️ imini 未找到模型选择器');
      return false;
    }

    console.log(`  imini 点击模型选择器: ${modelArea.text}`);
    await sleep(1500);
    let modelMenuOpen = await isIminiModelMenuOpen(page);
    if (!modelMenuOpen) {
      await page.mouse.click(modelArea.x, modelArea.y);
      await sleep(1500);
      modelMenuOpen = await isIminiModelMenuOpen(page);
    }

    if (!modelMenuOpen) {
      console.log('  ⚠️ imini 模型菜单未展开');
      return false;
    }

    // Step 2: Find and click the target model in the dropdown
    // The dropdown uses <LI> elements, each containing a <SPAN> with the model name
    const optionClicked = await page.evaluate((target) => {
      const targetNorm = target.toLowerCase().replace(/\s+/g, ' ');

      // First try: match by <SPAN> with exact model name inside <LI>
      const lis = document.querySelectorAll('LI');
      for (const li of lis) {
        const spans = li.querySelectorAll('span');
        for (const span of spans) {
          const name = (span.textContent || '').trim();
          const nameLower = name.toLowerCase();
          if (!name) continue;

          // Exact match on model name
          if (nameLower === targetNorm) {
            const rect = li.getBoundingClientRect();
            if (rect.width > 20 && rect.height > 10) {
              li.click();
              return { clicked: true, name };
            }
          }
        }
      }

      // Second try: partial match on <SPAN> text
      for (const li of lis) {
        const spans = li.querySelectorAll('span');
        for (const span of spans) {
          const name = (span.textContent || '').trim();
          const nameLower = name.toLowerCase();
          if (!name) continue;
          if (nameLower.includes(targetNorm) || targetNorm.includes(nameLower)) {
            const rect = li.getBoundingClientRect();
            if (rect.width > 20 && rect.height > 10) {
              li.click();
              return { clicked: true, name };
            }
          }
        }
      }

      // Fallback: match any element with model name (includes "Fast" vs non-Fast disambiguation)
      const allEls = document.querySelectorAll('div, span, [role="option"]');
      const wantsFast = targetNorm.includes('fast');
      const wantsPro = targetNorm.includes('pro');
      const wantsLite = targetNorm.includes('lite');

      for (const el of allEls) {
        const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
        const elLower = text.toLowerCase();

        // Match Seedance variants
        if (elLower.includes('seedance 2.0') && elLower.includes('fast') === wantsFast &&
            elLower.includes('pro') === wantsPro && elLower.includes('lite') === wantsLite) {
          const rect = el.getBoundingClientRect();
          if (rect.width > 20 && rect.height > 10 && rect.height < 80) {
            const target = el.closest('li') || el;
            target.click();
            return { clicked: true, text: text.slice(0, 60) };
          }
        }

        // Match other models (Sora, Kling, Wan, Veo)
        if (targetNorm.includes('sora') && elLower.includes('sora')) {
          const rect = el.getBoundingClientRect();
          if (rect.width > 20 && rect.height > 10 && rect.height < 80) {
            el.click();
            return { clicked: true, text: text.slice(0, 60) };
          }
        }
      }

      return { clicked: false };
    }, targetLower);

    if (!optionClicked.clicked) {
      console.log('  ⚠️ imini 未找到模型选项:', modelName);
      await page.keyboard.press('Escape').catch(() => {});
      return false;
    }

    console.log(`  imini 选择了模型: ${optionClicked.name || optionClicked.text}`);
    await sleep(3000);

    // Verify model selection
    const afterModel = await page.evaluate(() => {
      const allEls = document.querySelectorAll('div');
      for (const el of allEls) {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').trim();
        if (rect.x > 90 && rect.x < 450 && rect.y > 90 && rect.y < 180 && rect.width > 100 && rect.height > 20 && rect.height < 60) {
          if (/模型|Sora|Seedance|Kling|Wan|Veo/i.test(text)) {
            return text.slice(0, 60);
          }
        }
      }
      return '';
    });

    console.log(`  imini 模型选择后显示: ${afterModel}`);
    return true;
  } catch (error) {
    console.log(`  ⚠️ imini 模型选择失败: ${error.message}`);
    return false;
  }
}

function normalizeModelText(value) {
  return String(value || '')
    .replace(/^模型\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function modelMatchesTarget(current, target) {
  const currentNorm = normalizeModelText(current);
  const targetNorm = normalizeModelText(target);
  if (!currentNorm || !targetNorm) return false;
  if (currentNorm === targetNorm) return true;
  if (targetNorm === 'seedance 2.0') {
    return currentNorm.includes('seedance 2.0') && !currentNorm.includes('fast');
  }
  if (targetNorm === 'seedance 2.0 fast') {
    return currentNorm.includes('seedance 2.0') && currentNorm.includes('fast');
  }
  return currentNorm.includes(targetNorm) || targetNorm.includes(currentNorm);
}

async function readCurrentIminiModel(page) {
  return page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll('div, span'))
      .map(el => {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
        return { rect, text };
      })
      .filter(item =>
        item.rect.x > 80 && item.rect.x < 460 &&
        item.rect.y > 85 && item.rect.y < 180 &&
        item.rect.width > 40 && item.rect.height > 10 &&
        /Sora|Seedance|Kling|Wan|Veo/i.test(item.text)
      )
      .sort((a, b) => a.text.length - b.text.length);
    return candidates[0]?.text || '';
  }).catch(() => '');
}

async function isIminiModelMenuOpen(page) {
  return page.evaluate(() => {
    const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
    const popovers = Array.from(document.querySelectorAll('.imini-popover, .imini-popover-container, [role="tooltip"]'));
    return popovers.some(el => {
      const rect = el.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) return false;
      const text = normalize(el.innerText || el.textContent || '');
      return /Sora\s*2/i.test(text) && /Seedance\s*2\.0/i.test(text);
    });
  });
}

function validateIminiPreCheck(preCheck, expected) {
  const model = String(preCheck?.model || '').toLowerCase();
  const expectedModel = String(expected?.model || '').toLowerCase();
  if (expectedModel && !model.includes(expectedModel)) {
    return `模型不匹配，目标=${expected.model}，当前=${preCheck?.model || '未识别'}`;
  }
  if (expected?.ratio && preCheck?.ratio !== expected.ratio) {
    return `比例不匹配，目标=${expected.ratio}，当前=${preCheck?.ratio || '未识别'}`;
  }
  if (expected?.resolution && String(preCheck?.resolution || '').toUpperCase() !== String(expected.resolution).toUpperCase()) {
    return `分辨率不匹配，目标=${expected.resolution}，当前=${preCheck?.resolution || '未识别'}`;
  }
  if (expected?.duration && preCheck?.duration !== expected.duration) {
    return `时长不匹配，目标=${expected.duration}，当前=${preCheck?.duration || '未识别'}`;
  }
  return '';
}

async function uploadFirstFrameImage(page, imagePath) {
  try {
    const fileInputs = await page.$$('input[type="file"]');
    if (fileInputs.length === 0) {
      console.log('  ⚠️ imini 页面未找到文件输入框');
      return false;
    }

    for (const input of fileInputs) {
      try {
        await input.uploadFile(imagePath);
        await sleep(3000);
        return true;
      } catch (error) {
        continue;
      }
    }

    return false;
  } catch (error) {
    console.log(`  ⚠️ 首帧图片上传失败: ${error.message}`);
    return false;
  }
}

async function waitForReadyToCreate(page, { hasFirstFrame = false, timeoutMs = 30000 } = {}) {
  const startedAt = Date.now();
  let lastState = null;
  while (Date.now() - startedAt < timeoutMs) {
    lastState = await page.evaluate((needsImage) => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          rect.width > 0 &&
          rect.height > 0;
      };
      const all = Array.from(document.querySelectorAll('button, div, span'));
      const imageReady = !needsImage || all.some(el => {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').trim();
        const cls = String(el.className || '');
        const html = String(el.innerHTML || '');
        if (rect.x < 70 || rect.x > 470 || rect.y < 180 || rect.y > 420) return false;
        return /删除|移除|重新上传|更换|close|remove|preview|image|img/i.test(`${text} ${cls} ${html}`);
      });

      let createButton = null;
      for (const el of all) {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').trim();
        if (rect.y > 850 && rect.x > 80 && rect.x < 500 && rect.width > 60 && rect.height > 30) {
          if (/^创建/.test(text) || /^生成/.test(text) || /^开始/.test(text) || /^Generate/i.test(text)) {
            const button = el.closest('button') || el;
            const disabled = button.disabled ||
              button.getAttribute('aria-disabled') === 'true' ||
              /disabled/.test(String(button.className || ''));
            createButton = { text, disabled };
            break;
          }
        }
      }

      const buttonReady = !!createButton &&
        !createButton.disabled &&
        !/上传中|加载中|处理中|生成中|排队中|loading|uploading|processing/i.test(createButton.text);
      return {
        imageReady,
        buttonReady,
        createButtonText: createButton?.text || ''
      };
    }, hasFirstFrame).catch(error => ({
      imageReady: false,
      buttonReady: false,
      createButtonText: '',
      error: error.message
    }));

    if (lastState.imageReady && lastState.buttonReady) {
      await sleep(1000);
      return true;
    }
    await sleep(1000);
  }

  console.log(`  ⚠️ imini 创建前等待未确认就绪: imageReady=${lastState?.imageReady}, buttonReady=${lastState?.buttonReady}, button=${lastState?.createButtonText || ''}`);
  return false;
}

async function fillPrompt(page, prompt) {
  try {
    if (!prompt) return false;

    const textareaFound = await page.evaluate(() => {
      const isVisible = el => {
        if (!el) return false;
        const style = getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      };
      const textareas = Array.from(document.querySelectorAll('textarea, [role="textbox"], [contenteditable="true"]'))
        .filter(isVisible);
      if (textareas.length > 0) {
        textareas[0].focus();
        textareas[0].value = '';
        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
          window.HTMLTextAreaElement.prototype, 'value'
        );
        if (nativeInputValueSetter && nativeInputValueSetter.set) {
          nativeInputValueSetter.set.call(textareas[0], '');
        }
        textareas[0].dispatchEvent(new Event('input', { bubbles: true }));
        return true;
      }
      return false;
    });

    if (!textareaFound) return false;

    const filled = await page.evaluate((value) => {
      const ta = document.querySelector('textarea, [role="textbox"]');
      if (!ta) return false;
      ta.focus();
      const proto = ta.tagName === 'TEXTAREA'
        ? window.HTMLTextAreaElement.prototype
        : window.HTMLInputElement.prototype;
      const descriptor = Object.getOwnPropertyDescriptor(proto, 'value');
      if (descriptor && descriptor.set) {
        descriptor.set.call(ta, value);
      } else {
        ta.value = value;
      }
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      ta.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    }, prompt);
    if (!filled) return false;

    // Wait for React to process and verify the prompt was actually filled
    await sleep(1000);

    const actualContent = await page.evaluate(() => {
      const ta = document.querySelector('textarea, [role="textbox"]');
      if (!ta) return '';
      return (ta.value || ta.textContent || '').trim();
    });

    const expectedLength = prompt.length;
    const actualLength = actualContent.length;
    const matchRatio = expectedLength > 0 ? actualLength / expectedLength : 0;
    console.log(`  imini 提示词填充: 输入${expectedLength}字, 实际${actualLength}字 (${Math.round(matchRatio * 100)}%)`);

    // Require at least 80% match to consider it successful
    if (matchRatio < 0.8) {
      console.log(`  ⚠️ imini 提示词填充不完整，期望${expectedLength}字但只有${actualLength}字`);
      return false;
    }

    return true;
  } catch (error) {
    console.log(`  ⚠️ imini 提示词填充异常: ${error.message}`);
    return false;
  }
}

async function findVisualConfigSelect(page, kind) {
  return page.evaluate((selectKind) => {
    const patterns = {
      duration: /^\d+s$/,
      ratio: /^\d+:\d+$/,
      resolution: /^\d{3,4}P$/i
    };
    const pattern = patterns[selectKind];
    if (!pattern) return null;

    const readText = el => String(
      el.querySelector?.('.imini-select-selection-item, .imini-select-content-has-value')?.textContent ||
      el.textContent ||
      ''
    ).replace(/\s+/g, ' ').trim();
    const toPos = (el, text) => {
      const target = el.querySelector?.('.imini-select-selector') || el.closest?.('.imini-select-selector') || el;
      const rect = target.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) return null;
      return {
        x: Math.round(rect.left + rect.width / 2),
        arrowX: Math.round(rect.right - 16),
        y: Math.round(rect.top + rect.height / 2),
        text
      };
    };

    const picker = document.querySelector('.ToolForm_visual-config-picker__6ysqT');
    if (picker) {
      const selects = Array.from(picker.querySelectorAll('.imini-select')).filter(el => {
        const rect = el.getBoundingClientRect();
        return rect.width > 20 && rect.height > 14;
      });
      for (const select of selects) {
        const text = readText(select);
        if (pattern.test(text)) {
          const pos = toPos(select, text);
          if (pos) return pos;
        }
      }
      const order = { duration: 0, ratio: 1, resolution: 2 }[selectKind];
      const fallbackSelect = selects[order];
      if (fallbackSelect) {
        const pos = toPos(fallbackSelect, readText(fallbackSelect));
        if (pos) return pos;
      }
    }

    const els = document.querySelectorAll('button, div, span');
    for (const el of els) {
      const rect = el.getBoundingClientRect();
      const text = readText(el);
      const cls = String(el.className || '');
      if (!cls.includes('imini-select-selection-item')) continue;
      if (rect.x > 80 && rect.x < 460 && rect.y > window.innerHeight - 240 && rect.width > 20 && rect.width < 160 && rect.height > 14 && rect.height < 60) {
        if (pattern.test(text)) {
          const pos = toPos(el, text);
          if (pos) return pos;
        }
      }
    }
    return null;
  }, kind);
}

async function selectDuration(page, seconds) {
  try {
    const targetText = `${seconds}s`;
    // Find the duration button in bottom toolbar
    const btnPos = await findVisualConfigSelect(page, 'duration');

    if (!btnPos) {
      console.log('  ⚠️ imini 未找到时长按钮');
      return;
    }

    if (btnPos.text === targetText) {
      console.log(`  imini 时长已为 ${targetText}，跳过`);
      return;
    }

    // Click to open dropdown
    await page.mouse.click(btnPos.x, btnPos.y);
    await sleep(1000);

    // The duration menu is virtualized. Higher values like 15s are only mounted
    // after scrolling the rc-virtual-list holder.
    let clicked = false;
    for (let attempt = 0; attempt < 8 && !clicked; attempt++) {
      clicked = await page.evaluate((sec, attemptIndex) => {
        const target = `${sec}s`;
        if (attemptIndex > 0) {
          const holder = document.querySelector('.rc-virtual-list-holder');
          if (holder) {
            holder.scrollTop = holder.scrollTop + 96;
            holder.dispatchEvent(new Event('scroll', { bubbles: true }));
          }
        }

        const els = document.querySelectorAll('div, span');
        let best = null;
        let bestArea = Infinity;
        for (const el of els) {
          const text = (el.textContent || '').trim();
          const rect = el.getBoundingClientRect();
          const area = rect.width * rect.height;
          if (text === target && area > 0 && area < bestArea && rect.width > 20 && rect.height > 10) {
            best = el;
            bestArea = area;
          }
        }
        if (best) { best.click(); return true; }
        return false;
      }, seconds, attempt);
      if (!clicked) await sleep(150);
    }

    if (!clicked) {
      console.log(`  ⚠️ imini 下拉中未找到时长 ${targetText}`);
      await page.keyboard.press('Escape').catch(() => {});
    } else {
      await sleep(800);
      await page.keyboard.press('Escape').catch(() => {});
    }
  } catch (error) {
    // Duration selection is best-effort
  }
}

async function selectRatio(page, ratio) {
  try {
    // Find the ratio button in bottom toolbar (y > 800) and click it to open dropdown
    const btnPos = await findVisualConfigSelect(page, 'ratio');

    if (!btnPos) {
      console.log('  ⚠️ imini 未找到比例按钮');
      return;
    }

    // If current ratio already matches, skip
    if (btnPos.text === ratio) {
      console.log(`  imini 比例已为 ${ratio}，跳过`);
      return;
    }

    let clicked = false;
    for (let attempt = 0; attempt < 4 && !clicked; attempt++) {
      const clickX = attempt % 2 === 0 ? (btnPos.arrowX || btnPos.x) : btnPos.x;
      await page.mouse.click(clickX, btnPos.y);
      await sleep(250);
      if (attempt === 1) {
        await page.keyboard.press('Space').catch(() => {});
      } else {
        await page.keyboard.press('Enter').catch(() => {});
      }
      await sleep(700);

      const keyboardSelectedFirst = await selectRatioWithKeyboardFallback(page, btnPos.text, ratio);
      if (keyboardSelectedFirst) {
        return;
      }

      // Find and click the target ratio option in the dropdown.
      // imini sometimes renders options in a portal without stable class names,
      // so prefer visible small text nodes and click their option/container.
      clicked = await page.evaluate((targetRatio) => {
        const normalize = value => String(value || '').replace(/\s+/g, ' ').trim();
        const visible = el => {
          if (!el) return false;
          const rect = el.getBoundingClientRect();
          const style = getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
        };
        const els = Array.from(document.querySelectorAll('[role="option"], .imini-select-item, .imini-select-option, div, span'))
          .filter(visible);
        let best = null;
        let bestArea = Infinity;
        for (const el of els) {
          const text = normalize(el.textContent);
          const rect = el.getBoundingClientRect();
          const area = rect.width * rect.height;
          if (text === targetRatio && area > 0 && area < bestArea && rect.width >= 20 && rect.width <= 260 && rect.height >= 10 && rect.height <= 120) {
            best = el;
            bestArea = area;
          }
        }
        if (best) {
          const target = best.closest('[role="option"], .imini-select-item, .imini-select-option, [class*="option"], [class*="item"]') || best;
          target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window }));
          target.click();
          target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window }));
          return true;
        }
        return false;
      }, ratio);

      if (!clicked) {
        await page.keyboard.press('Escape').catch(() => {});
        await sleep(250);
      }
    }

    if (!clicked) {
      console.log(`  ⚠️ imini 下拉中未找到比例 ${ratio}`);
      await page.keyboard.press('Escape').catch(() => {});
    } else {
      await sleep(800);
      await page.keyboard.press('Escape').catch(() => {});
    }
  } catch (error) {
    // Ratio selection is best-effort
  }
}

async function selectRatioWithKeyboardFallback(page, currentRatio, targetRatio) {
  const current = String(currentRatio || '').trim();
  const target = String(targetRatio || '').trim();
  if (current !== '16:9' || target !== '9:16') return false;

  const attempts = [
    ['ArrowDown'],
    ['ArrowUp'],
    ['ArrowDown', 'ArrowDown'],
    ['ArrowUp', 'ArrowUp']
  ];

  for (const keys of attempts) {
    for (const key of keys) {
      await page.keyboard.press(key).catch(() => {});
      await sleep(160);
    }
    await page.keyboard.press('Enter').catch(() => {});
    await sleep(800);
    const selected = await readVisualConfigValues(page);
    if (selected.ratio === target) {
      console.log(`  imini 比例已通过键盘切换为 ${target}`);
      return true;
    }

    await page.keyboard.press('Enter').catch(() => {});
    await sleep(250);
  }

  await page.keyboard.press('Escape').catch(() => {});
  return false;
}

async function readVisualConfigValues(page) {
  return page.evaluate(() => {
    const picker = document.querySelector('.ToolForm_visual-config-picker__6ysqT');
    const values = picker ? Array.from(picker.querySelectorAll('.imini-select')).map(el => (el.textContent || '').trim()) : [];
    const result = { duration: '', ratio: '', resolution: '' };
    for (const value of values) {
      if (/^\d+s$/.test(value)) result.duration = value;
      if (/^\d+:\d+$/.test(value)) result.ratio = value;
      if (/^\d{3,4}P$/i.test(value)) result.resolution = value;
    }
    return result;
  });
}

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function uniqueShortPoints(points, limit) {
  const seen = new Set();
  const result = [];
  for (const point of Array.isArray(points) ? points : []) {
    const text = String(point || '').replace(/\s+/g, ' ').trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    result.push(text);
    if (result.length >= limit) break;
  }
  return result;
}

function buildProductAnchorFromReferencePack(context) {
  const snapshot = context?.referenceImagePack?.anchor_snapshot_json || {};
  const anchor = parseJsonMaybe(snapshot['AI生成锚点卡'] || snapshot.ai_anchor_card || snapshot.anchor_card);
  if (!anchor || typeof anchor !== 'object') return null;

  const corePoints = uniqueShortPoints(anchor.core_visual_points, 4);
  const keepPoints = uniqueShortPoints(anchor.must_not_change_points, 3);
  const forbidden = uniqueShortPoints(anchor.forbidden_mismatch, 3);

  return {
    productType: anchor.product_subtype || anchor.category || context?.category || '',
    keyDetails: corePoints.join('；'),
    mustKeep: keepPoints.join('；'),
    mustAvoid: forbidden.join('；')
  };
}

function mergeProductAnchorIntoLock(lock, productAnchor) {
  if (!productAnchor) return lock;
  const merged = { ...lock };
  if (productAnchor.productType) merged.productType = productAnchor.productType;
  if (productAnchor.keyDetails) merged.keyDetails = productAnchor.keyDetails;
  if (productAnchor.mustKeep) {
    merged.mustKeep = productAnchor.mustKeep;
  }
  if (productAnchor.mustAvoid) {
    merged.mustAvoid = [productAnchor.mustAvoid, merged.mustAvoid].filter(Boolean).join('；');
  }
  return merged;
}

async function selectResolution(page, resolution) {
  try {
    // Find the resolution button in bottom toolbar (y > 800) and click it to open dropdown
    const btnPos = await findVisualConfigSelect(page, 'resolution');

    if (!btnPos) {
      console.log('  ⚠️ imini 未找到分辨率按钮');
      return;
    }

    // If current resolution already matches, skip
    if (btnPos.text.toUpperCase() === resolution.toUpperCase()) {
      console.log(`  imini 分辨率已为 ${resolution}，跳过`);
      return;
    }

    // Click the resolution button to open dropdown
    await page.mouse.click(btnPos.x, btnPos.y);
    await sleep(1000);

    // Find and click the target resolution option in the dropdown
    // Prefer the smallest (innermost) matching element
    const clicked = await page.evaluate((targetRes) => {
      const normalize = value => String(value || '').replace(/\s+/g, ' ').trim().toUpperCase();
      const els = document.querySelectorAll('div, span');
      let best = null;
      let bestArea = Infinity;
      for (const el of els) {
        const text = normalize(el.textContent);
        const rect = el.getBoundingClientRect();
        const area = rect.width * rect.height;
        if (text === targetRes && area > 0 && area < bestArea && rect.width > 20 && rect.height > 10) {
          best = el;
          bestArea = area;
        }
      }
      if (best) {
        best.click();
        return true;
      }
      return false;
    }, resolution.toUpperCase());

    if (!clicked) {
      if (resolution.toUpperCase() === '480P') {
        console.log('  imini 分辨率文本未命中，尝试键盘选择 480P');
        await page.keyboard.press('Home').catch(() => {});
        await sleep(200);
        await page.keyboard.press('Enter').catch(() => {});
        await sleep(800);
      } else {
        console.log(`  ⚠️ imini 下拉中未找到分辨率 ${resolution}`);
        await page.keyboard.press('Escape').catch(() => {});
      }
    } else {
      await sleep(800);
      await page.keyboard.press('Escape').catch(() => {});
    }
  } catch (error) {
    // Resolution selection is best-effort
  }
}

async function clickGenerateButton(page) {
  if (_createButtonAlreadyClicked) {
    console.log('  ⚠️ imini 创建按钮已点过，不允许重复点击');
    return false;
  }
  _createButtonAlreadyClicked = true;

  try {
    const btnPos = await page.evaluate(() => {
      const els = document.querySelectorAll('button, div, span');
      for (const el of els) {
        const rect = el.getBoundingClientRect();
        const text = (el.textContent || '').trim();
        if (rect.y > 850 && rect.x > 80 && rect.x < 500 && rect.width > 60 && rect.height > 30) {
          if (/^创建/.test(text) || /^生成/.test(text) || /^开始/.test(text) || /^Generate/i.test(text)) {
            return { x: Math.round(rect.left + rect.width / 2), y: Math.round(rect.top + rect.height / 2), text };
          }
        }
      }
      return null;
    });

    if (!btnPos) {
      _createButtonAlreadyClicked = false;
      console.log('  ⚠️ imini 未找到创建按钮');
      return false;
    }

    console.log(`  imini 点击创建按钮: ${btnPos.text}`);
    await page.mouse.click(btnPos.x, btnPos.y);
    return true;
  } catch (error) {
    console.log(`  ⚠️ imini 点击创建按钮失败: ${error.message}`);
    return false;
  }
}

async function checkSubmitResult(page, context) {
  let lastSignal = null;
  try {
    for (let attempt = 1; attempt <= 10; attempt++) {
      if (attempt > 1) await sleep(3000);
      const signal = await readSubmitSignals(page, context);
      lastSignal = signal;
      if (signal.success) {
        if (signal.detail) console.log(`  imini 提交确认: ${signal.code} ${signal.detail}`);
        return { success: true, code: signal.code || 'submitted' };
      }
      if (signal.failed) {
        return { success: false, error: signal.error || '平台返回失败', code: signal.code || 'platform_error' };
      }
    }

    const extra = lastSignal?.createButtonText
      ? `；当前按钮=${lastSignal.createButtonText}`
      : '';
    return {
      success: false,
      error: `点击创建后 30 秒内未检测到当前脚本卡片/生成中/排队中/已提交等明确成功态${extra}，不标记为已提交`,
      code: 'submit_unverified'
    };
  } catch (error) {
    return {
      success: false,
      error: `提交后状态检查失败，未确认成功: ${error.message}`,
      code: 'submit_check_failed'
    };
  }
}

async function runFirstFramePipeline(context, config, originalImagePaths) {
  const iminiConfig = (config.channels || {}).imini || {};
  const firstFrameConfig = iminiConfig.firstFrame || {};
  const maxAutoRetry = firstFrameConfig.maxAutoRetry === undefined ? 1 : Math.max(0, Number(firstFrameConfig.maxAutoRetry) || 0);
  const maxManualRetry = firstFrameConfig.maxManualRetry === undefined ? 2 : Math.max(0, Number(firstFrameConfig.maxManualRetry) || 0);
  const isAutoChannel = context.channelSource === 'auto_random';
  const maxRetries = isAutoChannel ? maxAutoRetry : maxManualRetry;

  const hardConstraints = extractHardConstraints(context.prompt);
  const productAnchor = buildProductAnchorFromReferencePack(context);
  let lock = mergeProductAnchorIntoLock(
    buildProductLockCard(context.prompt, originalImagePaths, productAnchor),
    productAnchor
  );
  const category = detectCategory(context.prompt, lock);

  if (!lock.productType) {
    if (firstFrameConfig.enabled !== false && originalImagePaths && originalImagePaths.length > 0) {
      console.log('  🔍 商品锁定卡信息不足，使用 LLM 补充...');
      const llmResult = await generateProductLockCardWithLLM(context.prompt, originalImagePaths, null);
      if (llmResult.needsLLM) {
        lock = await enrichProductLockCardWithLLM(lock, context.prompt, originalImagePaths);
      } else {
        lock = llmResult.lock;
      }
    }

    if (!lock.productType) {
      if (isAutoChannel) {
        return {
          success: false,
          error: '自动随机 imini：缺少商品类型，退回即梦',
          code: 'missing_product_type',
          shouldFallBack: true
        };
      } else {
        return {
          success: false,
          error: '人工指定 imini：缺少商品类型和关键细节，转阻塞',
          code: 'missing_product_type',
          shouldFallBack: false,
          shouldBlock: true
        };
      }
    }
  }

  const firstFramePrompt = buildFirstFramePrompt(context, lock, category.category, firstFrameConfig);

  console.log('  🖼️ 首帧提示词已生成');
  console.log(`  📦 商品类目: ${category.category || '未识别'}`);

  const maxFirstFrameRetries = maxRetries;
  let lastConsistencyResult = null;
  let firstFrameImagePath = null;
  const firstFrameOutputPath = path.join(
    expandHome(config.runtimeRoot || path.join(require('os').homedir(), 'Desktop', 'temp', 'jimeng-feishu-runtime')),
    context.taskName || context.recordId || 'imini-task',
    'first-frame.png'
  );
  fs.mkdirSync(path.dirname(firstFrameOutputPath), { recursive: true });

  if (firstFrameConfig.reuseExistingFirstFrame !== false && fs.existsSync(firstFrameOutputPath)) {
    const stat = fs.statSync(firstFrameOutputPath);
    if (stat.size > 1024) {
      firstFrameImagePath = firstFrameOutputPath;
      console.log(`  🖼️ 复用已生成首帧图: ${firstFrameImagePath}`);
    }
  }

  for (let attempt = 0; !firstFrameImagePath && attempt <= maxFirstFrameRetries; attempt++) {
    console.log(`  🎨 生成首帧图 (尝试 ${attempt + 1}/${maxFirstFrameRetries + 1})...`);

    const imageResult = await generateFirstFrameImageWithLLM(firstFramePrompt, originalImagePaths, {
      ...firstFrameConfig,
      outputPath: firstFrameOutputPath,
      ratio: context.ratio
    });
    if (!imageResult.success) {
      console.log(`  ❌ 首帧图生成失败: ${imageResult.error}`);
      if (attempt < maxFirstFrameRetries) {
        await sleep(2000);
        continue;
      }

      if (firstFrameConfig.fallbackToJimengOnFirstFrameFailure !== false) {
        return {
          success: false,
          error: `首帧图生成失败: ${imageResult.error}`,
          code: 'first_frame_generation_failed',
          shouldFallBack: isAutoChannel
        };
      }

      return {
        success: false,
        error: `首帧图生成失败: ${imageResult.error}`,
        code: 'first_frame_generation_failed'
      };
    }

    if (!imageResult.imagePath) {
      return {
        success: false,
        error: '首帧图片生成结果没有可上传的本地图片路径，停止 imini 提交',
        code: 'first_frame_image_missing',
        shouldFallBack: isAutoChannel,
        shouldBlock: !isAutoChannel
      };
    }
    firstFrameImagePath = imageResult.imagePath;
    if (imageResult.imageWidth && imageResult.imageHeight) {
      console.log(`  📐 首帧图尺寸: ${imageResult.imageWidth}x${imageResult.imageHeight} (${imageResult.aspectRatio || context.ratio || '未知比例'})`);
    }

    if (originalImagePaths && originalImagePaths.length > 0 && firstFrameConfig.consistencyMode === 'strict') {
      console.log('  🔍 执行首帧一致性自检...');
      lastConsistencyResult = await checkFirstFrameConsistency(
        originalImagePaths[0],
        imageResult.imagePath,
        lock,
        hardConstraints
      );

      if (lastConsistencyResult.pass) {
        console.log(`  ✅ 首帧一致性自检通过 (score: ${lastConsistencyResult.score})`);
        break;
      }

      console.log(`  ❌ 首帧一致性自检失败 (score: ${lastConsistencyResult.score}): ${lastConsistencyResult.fatalIssues.join('; ')}`);
      const consistencyIssues = lastConsistencyResult.fatalIssues.join('; ');
      if (/insufficient_quota|quota is not enough|额度不足/i.test(consistencyIssues)) {
        return {
          success: false,
          error: `首帧一致性自检额度不足: ${consistencyIssues}`,
          code: 'first_frame_consistency_quota_exhausted',
          shouldFallBack: isAutoChannel,
          shouldBlock: !isAutoChannel
        };
      }

      if (attempt < maxFirstFrameRetries) {
        console.log(`  🔄 重试生成首帧图...`);
        await sleep(2000);
        continue;
      }

      if (isAutoChannel && firstFrameConfig.fallbackToJimengOnFirstFrameFailure !== false) {
        return {
          success: false,
          error: `首帧一致性自检失败，已重试 ${maxFirstFrameRetries + 1} 次: ${lastConsistencyResult.fatalIssues.join('; ')}`,
          code: 'first_frame_consistency_failed',
          shouldFallBack: true
        };
      }

      return {
        success: false,
        error: `首帧一致性自检失败: ${lastConsistencyResult.fatalIssues.join('; ')}`,
        code: 'first_frame_consistency_failed',
        shouldBlock: !isAutoChannel
      };
    }

    break;
  }

  return {
    success: true,
    lock,
    category: category.category,
    firstFramePrompt,
    firstFrameImagePath
  };
}

module.exports = {
  submitToImini,
  attemptSubmit,
  runFirstFramePipeline,
  switchToVideoCreation,
  switchToImageToVideo,
  selectModel,
  uploadFirstFrameImage,
  fillPrompt,
  selectDuration,
  selectRatio,
  selectResolution,
  clickGenerateButton,
  checkSubmitResult
};
