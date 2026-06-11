const crypto = require('crypto');

function normalizePromptFingerprintText(value) {
  return String(value || '').replace(/\s+/g, '').trim().toLowerCase();
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseContentIdMetadata(value, label = '内容ID') {
  const rawText = String(value || '');
  if (!rawText.trim()) {
    return { id: '', label: '', found: false };
  }

  const safeLabel = escapeRegex(label);
  const markerPattern = new RegExp(`【\\s*${safeLabel}\\s*】|(?:^|\\n)\\s*${safeLabel}`, 'i');
  const markerMatch = markerPattern.exec(rawText);
  if (markerMatch) {
    const section = rawText.slice(markerMatch.index, markerMatch.index + 120);
    const idMatch = section.match(/[-:：]\s*([A-Za-z0-9][A-Za-z0-9_-]{1,63})/);
    if (idMatch) {
      const id = String(idMatch[1] || '').trim();
      if (id) {
        const labelText = section
          .slice(0, section.indexOf(id) + id.length)
          .replace(/\s+\n/g, '\n')
          .trim();
        return { id, label: labelText, found: true };
      }
    }
  }

  return { id: '', label: '', found: false };
}

function parseScriptIdMetadata(value, label = '脚本ID') {
  const rawText = String(value || '');
  if (!rawText.trim()) {
    return { id: '', label: '', found: false };
  }

  const safeLabel = escapeRegex(label);
  const markerPattern = new RegExp(`【\\s*${safeLabel}\\s*】|(?:^|\\n)\\s*${safeLabel}`, 'i');
  const markerMatch = markerPattern.exec(rawText);
  if (!markerMatch) {
    return { id: '', label: '', found: false };
  }

  const section = rawText.slice(markerMatch.index, markerMatch.index + 120);
  const idMatch = section.match(/[-:：]\s*([A-Za-z0-9][A-Za-z0-9_-]{1,63})/i);
  if (!idMatch) {
    return { id: '', label: '', found: false };
  }

  const id = String(idMatch[1] || '').trim();
  if (!id) {
    return { id: '', label: '', found: false };
  }

  const labelText = section
    .slice(0, section.indexOf(id) + id.length)
    .replace(/\s+\n/g, '\n')
    .trim();

  return { id, label: labelText, found: true };
}

function buildPromptFingerprint(value) {
  const normalized = normalizePromptFingerprintText(value);
  return {
    normalized,
    length: normalized.length,
    anchor: normalized.slice(0, 120),
    preview: String(value || '').replace(/\s+/g, ' ').trim().slice(0, 200),
    hash: normalized
      ? crypto.createHash('sha1').update(normalized).digest('hex')
      : ''
  };
}

function isCompatibleMetadataId(detailId, expectedId) {
  const detail = String(detailId || '').trim();
  const expected = String(expectedId || '').trim();
  if (!detail || !expected) return false;
  if (detail === expected) return true;
  return detail.length >= 24 && expected.endsWith(detail);
}

function describePromptMatch(submission, detailPrompt, contentIdLabel = '内容ID') {
  const detailFingerprint = buildPromptFingerprint(detailPrompt);
  const detailContentIdMeta = parseContentIdMetadata(detailPrompt, contentIdLabel);
  const detailScriptIdMeta = parseScriptIdMetadata(detailPrompt);
  const contentIdEnabled = submission.content_id && submission.enable_content_id_claim !== false;
  const submissionScriptIdMeta = parseScriptIdMetadata(submission.prompt_preview || submission.prompt_anchor || '');
  const submissionHash = submission.prompt_hash || '';
  const submissionAnchor = submission.prompt_anchor || '';

  const submissionScriptId = String(submission.script_id || submissionScriptIdMeta.id || '').trim();

  const DEFAULT_STRATEGY = ['content_id', 'script_id', 'prompt_hash', 'prompt_anchor'];
  const claimStrategies = Array.isArray(submission.claim_strategy_order) && submission.claim_strategy_order.length > 0
    ? submission.claim_strategy_order
    : DEFAULT_STRATEGY;

  const normalizedStrategies = [];
  for (const strategy of claimStrategies) {
    if (!normalizedStrategies.includes(strategy)) {
      normalizedStrategies.push(strategy);
    }
  }
  if (submissionScriptId && !normalizedStrategies.includes('script_id')) {
    const contentIndex = normalizedStrategies.indexOf('content_id');
    if (contentIndex >= 0) {
      normalizedStrategies.splice(contentIndex + 1, 0, 'script_id');
    } else {
      normalizedStrategies.unshift('script_id');
    }
  }

  if (!detailFingerprint.normalized) {
    return { ok: false, reason: '详情页未读取到视频提示词' };
  }

  if (!contentIdEnabled && !submissionScriptId && !submissionHash && !submissionAnchor) {
    return { ok: false, reason: 'trace 缺少提示词指纹，拒绝自动认领' };
  }

  for (const strategy of normalizedStrategies) {
    if (strategy === 'content_id' && contentIdEnabled) {
      if (!detailContentIdMeta.found) continue;
      if (isCompatibleMetadataId(detailContentIdMeta.id, submission.content_id)) {
        return {
          ok: true,
          by: detailContentIdMeta.id === submission.content_id ? 'content_id' : 'content_id_suffix',
          detailContentId: detailContentIdMeta.id
        };
      }
      return {
        ok: false,
        reason: `详情页内容ID不匹配 (detail=${detailContentIdMeta.id}, trace=${submission.content_id})`,
        detailPreview: detailFingerprint.preview,
        detailContentId: detailContentIdMeta.id
      };
    }

    if (strategy === 'prompt_hash' && submissionHash && detailFingerprint.hash === submissionHash) {
      return { ok: true, by: 'prompt_hash' };
    }

    if (strategy === 'script_id' && submissionScriptId) {
      if (!detailScriptIdMeta.found) continue;
      if (isCompatibleMetadataId(detailScriptIdMeta.id, submissionScriptId)) {
        return {
          ok: true,
          by: detailScriptIdMeta.id === submissionScriptId ? 'script_id' : 'script_id_suffix',
          detailScriptId: detailScriptIdMeta.id
        };
      }
      return {
        ok: false,
        reason: `详情页脚本ID不匹配 (detail=${detailScriptIdMeta.id}, trace=${submissionScriptId})`,
        detailPreview: detailFingerprint.preview,
        detailScriptId: detailScriptIdMeta.id
      };
    }

    if (strategy === 'prompt_anchor' && submissionAnchor && detailFingerprint.normalized.includes(submissionAnchor)) {
      return { ok: true, by: 'prompt_anchor' };
    }
  }

  return {
    ok: false,
    reason: '详情页提示词与 trace 不匹配',
    detailPreview: detailFingerprint.preview
  };
}

module.exports = {
  normalizePromptFingerprintText,
  escapeRegex,
  parseContentIdMetadata,
  parseScriptIdMetadata,
  buildPromptFingerprint,
  isCompatibleMetadataId,
  describePromptMatch
};
