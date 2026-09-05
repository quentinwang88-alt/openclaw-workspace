// Pure pre-submit contract for success-script replication only. No I/O or models.
const { normalizeTextField } = require('./field-normalizers');
const crypto = require('crypto');

const ROLE_NOTES = {
  person_identity: '人物身份与脸部参考；不得把此图当作商品参考',
  product: '商品外观参考；不得用其中人物替换指定人物身份',
  composite_first_frame: '用户已选择且就绪的统一首帧，保留其中人物与商品的对应关系'
};

function sourceFields(context = {}) {
  return context.record?.fields || context.fields || {};
}

function isWsrContext(context = {}) {
  const fields = sourceFields(context);
  const source = normalizeTextField(context.taskSource || fields['任务来源'] || fields['脚本来源']);
  const scriptId = normalizeTextField(context.scriptId || fields['脚本ID']);
  return source === '成功脚本复刻' || scriptId.startsWith('wsr_') ||
    /【脚本ID】\s*\n-\s*wsr_[A-Za-z0-9_:-]+/.test(String(context.prompt || ''));
}

function referenceError(code, explanation) {
  const error = new Error(`${code}:${explanation}`);
  error.code = code;
  return error;
}

function assertWsrChannel(context, channel, mode, count) {
  if (!isWsrContext(context)) return;
  const normalized = String(channel || '').trim().toLowerCase();
  if (normalized === 'imini') {
    throw referenceError('WSR_REFERENCE_CHANNEL_UNSUPPORTED',
      '成功脚本复刻不能使用 imini 单首帧路径；人物与商品参考会丢失或被重生成。请人工选择即梦全能参考或 MiniMax 多参考图。');
  }
  const jimeng = normalized === '即梦' || normalized === 'jimeng';
  const h3 = ['minimax_h3', 'minimax-h3', 'minimax h3 api', 'metaso', 'metaso h3'].includes(normalized);
  const expectedMode = jimeng ? '全能参考' : '多参考图';
  if ((!jimeng && !h3) || String(mode || '').trim() !== expectedMode) {
    throw referenceError('WSR_REFERENCE_MODE_UNSUPPORTED',
      `成功脚本复刻需即梦“全能参考”或 MiniMax“多参考图”，当前渠道=${channel || '空'}，模式=${mode || '空'}；不自动改渠道或丢弃图片。`);
  }
  const maximum = jimeng ? 12 : 9;
  if (count > maximum || (h3 && count < 1)) {
    throw referenceError('WSR_REFERENCE_COUNT_UNSUPPORTED',
      `${channel}/${expectedMode} 需 ${h3 ? 1 : 0}–${maximum} 张完整参考图，当前 ${count} 张；禁止截断。`);
  }
}

function prepareWsrReferenceExecution(context = {}, options = {}) {
  const sourcePrompt = String(context.wsrSourcePrompt ?? context.prompt ?? '');
  if (!isWsrContext(context)) return { guarded: false, prompt: sourcePrompt };
  const attachments = options.attachments || context.attachments || [];
  assertWsrChannel(context, options.channel ?? context.channel, options.mode ?? context.mode, attachments.length);
  const fields = sourceFields(context);
  const raw = context.personaContract || fields['人物模板合同'] || fields['人物模板合同_JSON（系统）'];
  let contract = {};
  if (raw) {
    try {
      contract = typeof raw === 'object' && !Array.isArray(raw) ? raw : JSON.parse(normalizeTextField(raw));
    } catch (_error) {
      throw referenceError('WSR_REFERENCE_CONTRACT_INVALID', '人物模板合同不是有效 JSON；请重新同步本条脚本。');
    }
  }
  const roles = contract?.reference_assets || [];
  if (!Array.isArray(roles) || roles.length !== attachments.length || roles.some((asset, index) =>
    !asset || asset.index !== index + 1 || !Object.hasOwn(ROLE_NOTES, asset.role))) {
    throw referenceError('WSR_REFERENCE_CONTRACT_MISMATCH',
      '参考图角色数量、顺序或类型与附件不一致；请重新同步，不能只取部分图片继续执行。');
  }
  if (attachments.length === 0 && !context.allowNoReferenceImage) {
    throw referenceError('WSR_REFERENCE_REQUIRED', '无参考图时必须明确启用免参考图；不会自动忽略缺失资产。');
  }
  if (String(contract.schema_version || '') !== '2') {
    throw referenceError('WSR_REFERENCE_CONTRACT_UPGRADE_REQUIRED',
      '新提单需要人物模板合同 v2（图片 token 与 SHA256 绑定）；请重新同步本条，已提交任务不会重新提交。');
  }
  const sha = /^[a-f0-9]{64}$/i;
  for (let index = 0; index < roles.length; index++) {
    const asset = roles[index];
    if (!asset.file_token || asset.file_token !== attachments[index]?.fileToken ||
        !sha.test(asset.original_sha256 || '') || !sha.test(asset.derived_sha256 || '')) {
      throw referenceError('WSR_REFERENCE_ASSET_MISMATCH',
        `图 ${index + 1} 的实际附件 token 或原图/派生图 SHA256 未绑定；请重新同步，不允许换图或错序。`);
    }
  }
  const manifestRoles = contract.reference_manifest?.reference_assets;
  if (manifestRoles && JSON.stringify(manifestRoles.map(asset => [asset.index, asset.role, asset.file_token, asset.original_sha256, asset.derived_sha256])) !==
      JSON.stringify(roles.map(asset => [asset.index, asset.role, asset.file_token, asset.original_sha256, asset.derived_sha256]))) {
    throw referenceError('WSR_REFERENCE_ASSET_MISMATCH', '人物合同与参考图 manifest 不一致');
  }
  if (contract.reference_manifest) {
    const identity = roles.map(asset => ({ index: asset.index, original_sha256: asset.original_sha256, role: asset.role }));
    const manifestId = `sha256:${crypto.createHash('sha256').update(JSON.stringify(identity)).digest('hex')}`;
    if (String(contract.reference_manifest.schema_version) !== '2' || contract.reference_manifest.manifest_id !== manifestId) {
      throw referenceError('WSR_REFERENCE_ASSET_MISMATCH', '参考图 manifest 版本或身份校验不一致');
    }
  }
  const roleNotes = roles.length ? [
    '【参考图角色说明｜仅执行层】',
    '以下说明仅用于图片对应关系，不朗读、不做字幕；不改写上方脚本或口播。',
    ...roles.map(asset => `图 ${asset.index}：${ROLE_NOTES[asset.role]}。`),
    ...(roles.some(asset => asset.role === 'person_identity') ?
      ['人物图组定义同一人物的脸部与身份，不继承人物参考图中的头发。'] : []),
    ...(roles.some(asset => asset.role === 'product') ?
      ['商品图组定义目标假发的发色、长度、卷度、刘海和层次；商品图中的人不定义人物身份。'] : []),
    '必须使用上述全部参考图，按上传顺序对应，不得静默删图、取首图或把人物和商品角色对调。'
  ].join('\n') : '';
  return {
    guarded: true,
    sourcePrompt,
    prompt: roleNotes ? `${normalizeLegacyRoleClaims(sourcePrompt, roles).trimEnd()}\n\n${roleNotes}` : sourcePrompt,
    roles,
    manifest: contract.reference_manifest || null,
    motherCheckpoints: contract.effective_checkpoints || contract.mother_core_points || contract.mother_checkpoints || [],
    frozenMotherCheckpoints: contract.frozen_mother_core_points || contract.mother_core_points || contract.mother_checkpoints || [],
    allowedChanges: contract.allowed_changes || [],
    executionSummary: contract.execution_summary || null,
    generationProvenance: contract.generation_provenance || null,
    revisionKind: contract.revision_kind || '',
    parentPromptId: contract.parent_prompt_id || '',
    executionPolicyVersion: 'wsr-reference-execution-v3',
    motherCoreProvenance: contract.mother_core_provenance || '',
    imageCount: attachments.length
  };
}

function normalizeLegacyRoleClaims(prompt, roles) {
  const persons = roles.filter(asset => asset.role === 'person_identity').map(asset => asset.index);
  const products = roles.filter(asset => asset.role === 'product').map(asset => asset.index);
  const label = indices => indices.length ? `第 ${indices.join('、')} 张` : '未提供';
  // Correct only known indexing phrases in the execution copy. Preserve all
  // product appearance, identity and action requirements from the human text.
  return String(prompt || '')
    .replace(/\n*【参考图角色说明｜仅执行层】[\s\S]*$/, '')
    .replace(/前两张(?=(?:是同一人物|人物|是人物|参考人物))/g, label(persons))
    .replace(/后两张(?=(?:是目标假发|为唯一目标假发|是产品|产品|为产品))/g, label(products));
}

module.exports = { assertWsrChannel, isWsrContext, normalizeLegacyRoleClaims, prepareWsrReferenceExecution };
