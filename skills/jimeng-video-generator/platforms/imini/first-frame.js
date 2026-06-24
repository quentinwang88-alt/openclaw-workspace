const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { execFile } = require('child_process');
let loadOpenAIConfig = null;
let imagePathToInputItem = null;
const openClawImageCircuitState = {
  ssrfBlocked: false,
  reason: ''
};
try {
  ({ loadOpenAIConfig, imagePathToInputItem } = require('../../../prompt-expander/openai-responses-helper'));
} catch (error) {
  loadOpenAIConfig = null;
  imagePathToInputItem = null;
}
const { extractHardConstraints, extractShotOne, extractComposition } = require('./product-lock');
const {
  detectCategory,
  getCompositionRule,
  getForbiddenRules
} = require('./category-rules');

function extractFirstFrameIntent(prompt) {
  const text = String(prompt || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  const positiveMatch = text.match(/正向提示词[：:]\s*([\s\S]*?)(?:负向提示词[：:]|运镜\/动作弧线[：:]|参考图锚点提示[：:]|$)/);
  const source = (positiveMatch ? positiveMatch[1] : text)
    .replace(/片段脚本ID[：:]\s*\S+/g, '')
    .replace(/【[^】]+】[-：:\s]*\S+/g, '')
    .trim();
  const parts = source
    .split(/[；;。.\n，,]/)
    .map(item => item.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .filter(item => !/(真实TikTok|单镜头|一镜到底|自然手持|真实光线|4秒|对焦|水印|字幕|logo|不要|禁止|负向提示词)/i.test(item));

  const pick = (patterns, limit) => {
    const result = [];
    for (const item of parts) {
      if (result.includes(item)) continue;
      if (patterns.some(pattern => pattern.test(item))) {
        result.push(item);
        if (result.length >= limit) break;
      }
    }
    return result;
  };

  const scene = pick([
    /场景|环境|背景|玄关|卧室|客厅|餐厅|咖啡|街角|街头|门口|窗边|镜前|试衣间|室内|户外|阳台|走廊|电梯|落地灯|暖光|自然光|日光|清冷|冬季/
  ], 4);
  const person = pick([
    /人物|模特|女性|男性|女生|男生|女孩|男孩|达人|主播|身材|发型|长发|短发|肤色|气质|表情|状态|姿态/
  ], 3);
  const styling = pick([
    /穿搭|穿着|穿上|上身|搭配|外套|衣服|服装|内搭|牛仔|长裤|短裤|半裙|裙|鞋|包|配饰|发型|妆容/
  ], 4);
  const camera = pick([
    /机位|构图|镜头|近景|中景|远景|全身|半身|背面|侧面|正面|局部|手部|镜前|自拍|第三视角|俯拍|平视|低角度/
  ], 3);
  const action = pick([
    /拿起|穿上|整理|展示|拉近|转身|扣上|打开|合上|形成型|动作/
  ], 2);

  const lines = [];
  if (scene.length > 0) lines.push(`场景环境：${scene.join('；')}`);
  if (person.length > 0) lines.push(`人物状态：${person.join('；')}`);
  if (styling.length > 0) lines.push(`穿搭细节：${styling.join('；')}`);
  if (camera.length > 0) lines.push(`机位构图：${camera.join('；')}`);
  if (action.length > 0) lines.push(`起始动作：${action.join('；')}`);
  return lines.join('\n');
}

function compactText(value, maxLength = 72) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function buildStylingGuidance(context, productLock, category) {
  const prompt = String(context.prompt || '');
  const productType = String(productLock.productType || category || '');
  const isUpperWear = /上装|上衣|外套|夹克|开衫|衬衫|卫衣|毛衣|T恤|背心|吊带/.test(productType) || /女装外套|男装外套|上装|外套|夹克|开衫/.test(prompt);
  const hasInnerStyling = /内搭|打底|吊带|背心|T恤|衬衫|针织|抹胸|背心|白色上衣|黑色上衣|基础款上衣/.test(prompt);
  const hasLowerStyling = /下装|裤|牛仔裤|长裤|短裤|阔腿裤|休闲裤|西裤|裙|半裙|短裙|长裙/.test(prompt);
  const hasAccessoryStyling = /鞋|靴|包|帽|围巾|项链|耳环|配饰|腰带/.test(prompt);

  const lines = [];
  if (isUpperWear) {
    lines.push('目标商品只锁定外套/上装本体；参考图里的内搭、下装、鞋包、人物、发型、背景都不是锁定对象。');
    if (!hasInnerStyling || !hasLowerStyling || !hasAccessoryStyling) {
      lines.push('原提示词没有明确写到的内搭、下装、鞋包，请根据场景和人物状态自然补全，可以和参考图不同。');
      lines.push('不要反复照抄参考图里的白色短内搭、浅蓝牛仔裤、同一站姿或同一室内背景。');
    } else {
      lines.push('内搭、下装、鞋包如原提示词已有描述，以原提示词为准；不要被参考图中的非目标穿搭覆盖。');
    }
  } else {
    lines.push('参考图只锁定目标商品本体；人物、发型、背景、非目标服装和配饰可以按脚本场景变化。');
  }
  return lines;
}

function stableHash(value) {
  const text = String(value || '');
  let hash = 0;
  for (let i = 0; i < text.length; i++) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
}

function shouldUsePhoneCoveredFaceFrame(context, productLock, category) {
  const prompt = String(context.prompt || '');
  if (/手机遮脸|手机挡脸|镜前自拍|镜子自拍|对镜自拍|自拍遮脸/.test(prompt)) {
    return true;
  }
  if (!/(女装|男装|上装|下装|外套|上衣|夹克|开衫|卫衣|毛衣|T恤|衬衫|裤|裙|穿搭|上身|试穿)/.test(`${category || ''} ${productLock.productType || ''} ${prompt}`)) {
    return false;
  }
  return true;
}

function extractMotionArc(prompt) {
  const text = String(prompt || '');
  const match = text.match(/运镜\/动作弧线[：:]\s*([^\n]+)/);
  return match ? match[1].replace(/\s+/g, ' ').trim() : '';
}


const START_STATE_KEYWORDS = ['拿起', '取出', '靠近', '准备', '刚要', '手持', '放到', '靠到', '披上前', '夹上前', '戴上前', '扣上前', '整理前', '拿出', '抬起', '伸向', '朝'];
const PROCESS_KEYWORDS = ['戴上', '夹上', '穿上', '披上', '扣上', '整理', '拉近', '侧转', '转身', '抖开', '挂上', '系上', '别上', '套上'];
const RESULT_KEYWORDS = ['已戴好', '已穿好', '造型完成', '成型', '结果展示', '上身效果', '佩戴效果', '完成后', '稳定停留', '效果停留', '廓形成型', '展示上身', '已经', '整体展示', '最终'];
const START_MOTION_COMBOS = [
  ['拿起', '穿上'], ['手持', '穿上'], ['取出', '穿上'], ['拿起', '戴上'],
  ['拿起', '夹上'], ['靠近', '夹上'], ['准备', '穿上'], ['刚要', '披上'],
  ['拿出', '别上'], ['拿起', '系上'], ['拿起', '扣上'], ['拿起', '套上'],
];

function _splitMotionNodes(motionArc) {
  if (!motionArc) return [];
  return motionArc.split(/->|→|然后|，|；/).map(s => s.trim()).filter(Boolean);
}

function isResultOnlyMotion(motionArc, prompt) {
  if (!motionArc) return false;
  const nodes = _splitMotionNodes(motionArc);
  if (nodes.length === 0) return false;
  const firstNode = nodes[0];
  // if first node is clearly a RESULT state (已穿好/已戴好/展示效果), it's result-only
  if (RESULT_KEYWORDS.some(k => firstNode.includes(k))) {
    return true;
  }
  // if first node has process keywords but no start keywords, check if whole arc is result display
  const hasProcess = PROCESS_KEYWORDS.some(k => motionArc.includes(k));
  const hasStart = START_STATE_KEYWORDS.some(k => motionArc.includes(k));
  if (!hasStart && !hasProcess) {
    const text = motionArc;
    if (RESULT_KEYWORDS.some(k => text.includes(k))) {
      return true;
    }
  }
  return false;
}

function deriveFirstFrameStartState(prompt, category) {
  const motionArc = extractMotionArc(prompt);
  const cat = category || '';
  const nodes = _splitMotionNodes(motionArc);

  // result-only motion overrides category template — allow result state
  if (motionArc && isResultOnlyMotion(motionArc, prompt)) {
    return { isStartState: false, category: cat, hint: '动作弧线为结果展示型，首帧可为已穿戴/已整理完成状态', matched: 'result_only_motion' };
  }

  // check category templates first
  for (const [catKey, rule] of Object.entries(CATEGORY_START_STATE_RULES)) {
    if (cat.includes(catKey) || catKey === cat) {
      return { isStartState: true, category: catKey, hint: rule.default, matched: 'category_rule' };
    }
  }
  // default
  if (CATEGORY_START_STATE_RULES['default']) {
    return { isStartState: true, category: 'default', hint: CATEGORY_START_STATE_RULES['default'].default, matched: 'default_rule' };
  }

  if (!motionArc) {
    return { isStartState: false, category: cat, hint: '', matched: 'no_motion_arc' };
  }

  // has process keywords: must generate BEFORE state
  const hasProcess = PROCESS_KEYWORDS.some(k => nodes.some(n => n.includes(k)));
  const hasStart = START_STATE_KEYWORDS.some(k => nodes.some(n => n.includes(k)));
  if (hasProcess && !hasStart) {
    // find what the first process action is
    const firstProcess = PROCESS_KEYWORDS.find(k => nodes.some(n => n.includes(k)));
    const startHint = firstProcess ? `首帧应表现${firstProcess}动作开始前或刚开始的瞬间` : '首帧应表现动作开始前状态';
    return { isStartState: true, category: cat, hint: startHint, matched: 'process_without_start' };
  }

  if (nodes.length > 0) {
    const firstNode = nodes[0].toLowerCase();
    const isStartNode = START_STATE_KEYWORDS.some(k => firstNode.includes(k));
    if (isStartNode) {
      return { isStartState: true, category: cat, hint: `首帧应为：${nodes[0]}`, matched: 'first_node_is_start' };
    }
    const isResultNode = RESULT_KEYWORDS.some(k => firstNode.includes(k));
    if (isResultNode) {
      return { isStartState: false, category: cat, hint: '首帧可为结果展示状态', matched: 'first_node_is_result' };
    }
  }

  // conservative default: if any process keyword exists, force start state
  for (const [start, end] of START_MOTION_COMBOS) {
    if (nodes.some(n => n.includes(start)) || motionArc.includes(start)) {
      if (nodes.some(n => n.includes(end)) || motionArc.includes(end)) {
        return { isStartState: true, category: cat, hint: `首帧应表现${start}的瞬间，${end}尚未完成`, matched: 'combo_match' };
      }
    }
  }

  if (hasProcess) {
    return { isStartState: true, category: cat, hint: '首帧应表现动作开始前一瞬间', matched: 'conservative_default' };
  }

  // result-only: allow
  return { isStartState: false, category: cat, hint: '', matched: 'unknown_motion' };
}


const CATEGORY_START_STATE_RULES = {
  '女装外套': {
    default: '首帧优先是外套拿在手上、刚披到肩前、手扶衣领准备穿、外套尚未完整穿好。如果使用镜前自拍遮脸，手机可以遮住脸，但不能遮住外套关键版型。不要直接生成已经穿好并整理完成的成片效果。',
  },
  '发饰': {
    default: '首帧优先是手拿发饰靠近头发、头发尚未固定、夹子尚未夹上。不要直接生成发型已经完成、发饰已经戴好后的结果图。',
  },
  '耳环': {
    default: '首帧优先是手拿耳饰靠近耳侧、尚未完全戴好。不要直接生成耳饰已经佩戴完成并转头展示的结果图。',
  },
  '手链': {
    default: '首帧优先是手链靠近手腕、尚未扣好，或手刚拿起手链。不要直接生成已经扣好后的静态展示。',
  },
  '项链': {
    default: '首帧优先是项链在手中、靠近颈部、尚未完全扣好。不要直接生成已经佩戴完成的静态展示。',
  },
  '包': {
    default: '首帧优先是包在手边、刚拿起、包带尚未完全背上。不要直接生成已经背好的静态展示。',
  },
  'default': {
    default: '首帧表现动作开始前一瞬间：商品在手边或刚进入画面，动作尚未完成。不要直接生成穿戴完成、使用结束的结果态。',
  },
};

function buildFirstFrameStartStateBlock(prompt, category) {
  const startState = deriveFirstFrameStartState(prompt, category);
  const lines = [];
  lines.push('');
  lines.push('首帧动作状态硬规则：');
  lines.push('这张图是图片转视频的第 0 秒起点，不是最终结果图。');
  lines.push('必须表现动作开始前或动作刚开始的状态，让后续视频仍有动作空间。');
  lines.push('不要直接生成动作完成后的结果态。');
  lines.push('不要生成已经佩戴完成、已经穿好、已经整理完成、造型已经成型的画面，除非动作弧线明确就是从结果展示开始。');

  if (startState.hint) {
    lines.push('');
    if (startState.matched === 'result_only_motion') {
      lines.push(`本条动作弧线为结果展示型：${startState.hint}`);
    } else {
      lines.push(`本条动作弧线的首帧应为：${startState.hint}`);
    }
  }

  return { lines, startState };
}


function buildFirstFramePrompt(context, productLock, category, options = {}) {
  const ratio = context.ratio || '9:16';
  const categoryRule = getCompositionRule(category);
  const hardConstraints = extractHardConstraints(context.prompt);
  const shotOne = extractShotOne(context.prompt);
  const firstFrameIntent = extractFirstFrameIntent(context.prompt);
  const compositionRule = extractComposition(hardConstraints, category);
  const allForbidden = [...hardConstraints.mustAvoid, ...getForbiddenRules(category)];
  const faceSafetyMode = String(options.faceSafetyMode || 'no_identifiable_face').toLowerCase();
  const avoidIdentifiableFace = faceSafetyMode !== 'off' && faceSafetyMode !== 'disabled';
  const usePhoneCoveredFaceFrame = avoidIdentifiableFace && shouldUsePhoneCoveredFaceFrame(context, productLock, category);

  const productLockText = [];
  if (productLock.productType) productLockText.push(`商品大类：${compactText(productLock.productType, 40)}`);
  if (productLock.mainColor) productLockText.push(`主颜色：${compactText(productLock.mainColor, 40)}`);
  if (productLock.material) productLockText.push(`材质感：${compactText(productLock.material, 48)}`);
  if (productLock.keyDetails) productLockText.push(`一两个关键外观：${compactText(productLock.keyDetails, 72)}`);

  const lines = [
    `生成一张短视频图片转视频的开场首帧，画幅为 ${ratio}，真实手机拍摄风格。`,
    '',
    '画面内容优先级：',
    '1. 首先表现短视频脚本里的场景、环境、人物状态、穿搭和机位。',
    '2. 商品作为穿搭/动作中的自然组成部分出现，清楚可辨即可。',
    '3. 参考图只用于防止商品跑成完全不同的大类、主颜色或材质，不要让商品锚点主导画面。'
  ];

  lines.push('');
  lines.push('短视频首帧意图（重点用于场景、人物、穿搭和机位）：');
  if (shotOne) {
    lines.push(`${shotOne}`);
    lines.push('生成动作开始前的自然静止瞬间，保留脚本中的环境和人物状态。');
  } else if (firstFrameIntent) {
    lines.push(firstFrameIntent);
    lines.push('生成动作开始前的自然静止瞬间，优先让场景、人物状态和穿搭有差异。');
  } else {
    lines.push('根据短视频片段提示词，生成动作开始前的自然静止画面；重点不是商品近景，而是有内容的生活场景。');
  }

  const startStateBlock = buildFirstFrameStartStateBlock(context.prompt, category);
  for (const line of startStateBlock.lines) {
    lines.push(line);
  }

  if (productLockText.length > 0) {
    lines.push('');
    lines.push('商品弱锚点（只用于防跑款，不用于决定构图）：');
    for (const line of productLockText) {
      lines.push(`- ${line}`);
    }
  }

  const stylingGuidance = buildStylingGuidance(context, productLock, category);
  if (stylingGuidance.length > 0) {
    lines.push('');
    lines.push('穿搭补全规则：');
    for (const line of stylingGuidance) {
      lines.push(line);
    }
  }

  lines.push('');
  lines.push('画面主体：');

  if (avoidIdentifiableFace) {
    if (usePhoneCoveredFaceFrame) {
      lines.push('人物、场景和穿搭共同构成画面主体，可以使用真实镜前手机自拍构图。');
      lines.push('手机必须足够大、离脸足够近，完整遮住整张脸的外轮廓；不能露出眼睛、鼻子、嘴巴、脸颊、下巴、额头或脸部皮肤轮廓，只允许看到头发边缘、手、脖子以下身体和穿搭。');
      lines.push('手机是自然遮挡道具，不是商品主体；商品和穿搭仍然要清楚可辨，镜前卧室/试衣间/走廊等生活环境要真实。');
    } else {
      lines.push('人物、场景和穿搭共同构成画面主体，但画面中不能出现任何真人脸部或头部。');
      lines.push('构图必须自然裁切，画面上缘从肩线以下或胸口以下开始；不能出现被切断的脖子、无头人体、假人或恐怖感。');
      lines.push('可以保留身体轮廓、手部动作、衣服上身关系和环境氛围，但不要出现手机自拍、镜中手机、相机、屏幕或任何头像区域。');
      lines.push('不要用低头、头发遮脸、侧脸、背影带头部、下巴以下但露出脖子/脸部轮廓等方式规避；头部、脖子和五官必须完全不进画，裁切边缘要自然。');
    }
  } else {
    if (context.prompt) {
      const personMatch = String(context.prompt).match(/(?:人物|模特|女性|男性|女生|男生|女孩|男孩|主播|达人)[是为：:]\s*([^\n。；,，]{5,80})/);
      if (personMatch) {
        lines.push(`${personMatch[1].trim()}，人物正在展示该商品，商品是画面第一视觉重点。`);
      } else {
        lines.push('人物正在展示该商品，商品是画面第一视觉重点。');
      }
    } else {
      lines.push('人物正在展示该商品，商品是画面第一视觉重点。');
    }
  }

  lines.push('');
  lines.push('构图要求：');
  lines.push('不要默认做商品近景或半身商品硬展示；优先选择能体现环境、人物状态和穿搭关系的生活化构图。');
  lines.push('商品清楚可辨、不过曝、不贴边即可；可以作为人物穿搭的一部分，不要求占满画面。');
  if (avoidIdentifiableFace) {
    if (usePhoneCoveredFaceFrame) {
      lines.push('优先使用镜前中景或全身试穿构图：手机挡住脸，身体和穿搭完整，人物可以自然站立或整理衣服。');
      lines.push('镜子和手机可以出现，但手机必须覆盖整张脸的轮廓，画面不能出现可识别真人面部或脸部皮肤轮廓。');
    } else {
      lines.push('如需表现人物，只使用自然裁切的胸口以下半身、手部整理衣服、衣架悬挂或平铺穿搭构图；不要使用无头全身或无头假人。');
      lines.push('不要生成镜子自拍构图；如有镜子，只拍衣服和身体，不出现手机、相机或头像区域。');
    }
  }
  if (compositionRule) {
    lines.push('商品细节底线：需要能看出商品大类、主颜色和一两个关键设计点；不要为了展示细节强行改成固定近景。');
  } else if (!avoidIdentifiableFace && categoryRule) {
    lines.push(categoryRule);
  }

  lines.push('');
  lines.push('场景与光线：');
  const sceneMatch = String(context.prompt || '').match(/(?:场景|环境|背景|整体画面风格)[是为：:]\s*([^\n。；,，]{5,100})/);
  if (sceneMatch) {
    lines.push(`${sceneMatch[1].trim()}，自然真实光线，商品边缘和材质细节清楚。`);
  } else {
    lines.push('自然真实光线，商品边缘和材质细节清楚。');
  }

  lines.push('');
  lines.push('禁止：');
  lines.push('不要改变商品外观。');
  lines.push('不要生成与参考图不一致的新款式。');
  lines.push('不要复制参考图中的非目标内搭、裤子、鞋包、人物姿态或背景。');
  lines.push('不要多个商品混入。');
  lines.push('不要文字、字幕、水印、logo、UI。');
  if (avoidIdentifiableFace) {
    if (usePhoneCoveredFaceFrame) {
      lines.push('不要露出任何可识别真人脸部或脸部皮肤轮廓：眼睛、鼻子、嘴巴、脸颊、下巴、额头、侧脸、半张脸、手机两侧脸部边缘都不能出现。');
      lines.push('不要用头发遮脸、低头遮脸、模糊脸、侧脸阴影或背影头像替代手机遮挡；必须是手机完整挡脸或完全无脸构图。');
      lines.push('不要让手机遮挡商品关键版型和穿搭主体。');
    } else {
      lines.push('不要出现任何真人脸部、头部、脖子、侧脸、背影头部、眼睛、鼻子、嘴巴、脸颊、下巴、额头、头发或完整发型轮廓。');
      lines.push('不要出现手机、相机、镜中自拍、屏幕、手持拍摄设备。');
      lines.push('不要使用头发遮脸、低头遮脸、模糊脸、侧脸阴影、背影头像、半张脸、只露下巴/脖子等形式。');
      lines.push('不要生成断脖子、无头人体、头部被硬切掉、假人展示、恐怖或怪异的人体裁切。');
    }
    lines.push('不要生成可被平台识别为包含真人脸的输入图。');
  }
  if (allForbidden.length > 0) {
    for (const avoid of allForbidden) {
      lines.push(avoid);
    }
  }
  if (productLock.mustAvoid) {
    for (const avoid of String(productLock.mustAvoid).split('；').filter(Boolean).slice(0, 5)) {
      lines.push(avoid);
    }
  }

  return lines.join('\n');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function summarizeResponseBody(body) {
  if (!body) return '';
  try {
    const data = JSON.parse(body);
    if (data.error && typeof data.error === 'object') {
      return [data.error.code, data.error.type, data.error.message]
        .filter(Boolean)
        .join(' | ');
    }
    return JSON.stringify(data).slice(0, 500);
  } catch {
    return String(body).replace(/\s+/g, ' ').slice(0, 500);
  }
}

async function postJson(apiUrl, apiKey, body, timeoutMs) {
  const parsedUrl = new URL(apiUrl);
  const protocol = parsedUrl.protocol === 'https:' ? https : http;
  const postData = JSON.stringify(body);

  return new Promise((resolve, reject) => {
    const req = protocol.request({
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
      path: `${parsedUrl.pathname}${parsedUrl.search}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
        'Content-Length': Buffer.byteLength(postData)
      }
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}: ${summarizeResponseBody(data)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(new Error(`解析图片生成响应失败: ${error.message}`));
        }
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`图片生成请求超时 (${Math.round(timeoutMs / 1000)}s)`));
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

async function downloadUrlToBuffer(url, timeoutMs) {
  const parsedUrl = new URL(url);
  const protocol = parsedUrl.protocol === 'https:' ? https : http;

  return new Promise((resolve, reject) => {
    const req = protocol.get(parsedUrl, res => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error(`下载图片失败 HTTP ${res.statusCode}`));
        res.resume();
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`下载图片超时 (${Math.round(timeoutMs / 1000)}s)`));
    });
    req.on('error', reject);
  });
}

function sizeForRatio(ratio) {
  const normalized = String(ratio || '9:16').trim();
  if (normalized === '16:9') return '1536x1024';
  if (normalized === '1:1') return '1024x1024';
  return '1024x1536';
}

function normalizeAspectRatio(ratio) {
  const normalized = String(ratio || '9:16').trim();
  if (normalized === '16:9' || normalized === '9:16' || normalized === '1:1') {
    return normalized;
  }
  return '9:16';
}

function ratioToNumber(ratio) {
  const normalized = normalizeAspectRatio(ratio);
  if (normalized === '16:9') return 16 / 9;
  if (normalized === '1:1') return 1;
  return 9 / 16;
}

function targetCropForRatio(width, height, ratio) {
  const currentWidth = Number(width);
  const currentHeight = Number(height);
  if (!Number.isFinite(currentWidth) || !Number.isFinite(currentHeight) || currentWidth <= 0 || currentHeight <= 0) {
    return null;
  }

  const targetRatio = ratioToNumber(ratio);
  const currentRatio = currentWidth / currentHeight;
  if (Math.abs(currentRatio - targetRatio) < 0.003) {
    return null;
  }

  let cropWidth = currentWidth;
  let cropHeight = currentHeight;
  if (currentRatio > targetRatio) {
    cropWidth = Math.round(currentHeight * targetRatio);
  } else {
    cropHeight = Math.round(currentWidth / targetRatio);
  }

  cropWidth = Math.max(1, Math.min(currentWidth, cropWidth));
  cropHeight = Math.max(1, Math.min(currentHeight, cropHeight));

  return {
    width: cropWidth,
    height: cropHeight,
    offsetX: Math.max(0, Math.floor((currentWidth - cropWidth) / 2)),
    offsetY: Math.max(0, Math.floor((currentHeight - cropHeight) / 2))
  };
}

async function readImageDimensions(imagePath) {
  const { stdout } = await execFilePromise('sips', ['-g', 'pixelWidth', '-g', 'pixelHeight', imagePath], {
    timeout: 30000,
    maxBuffer: 1024 * 1024
  });
  const width = Number(String(stdout || '').match(/pixelWidth:\s*(\d+)/)?.[1] || 0);
  const height = Number(String(stdout || '').match(/pixelHeight:\s*(\d+)/)?.[1] || 0);
  if (!width || !height) {
    throw new Error(`无法读取图片尺寸: ${imagePath}`);
  }
  return { width, height };
}

async function normalizeImageToRatio(imagePath, ratio, options = {}) {
  if (options.normalizeOutputRatio === false) {
    return { changed: false, ...(await readImageDimensions(imagePath)) };
  }

  const before = await readImageDimensions(imagePath);
  const crop = targetCropForRatio(before.width, before.height, ratio);
  if (!crop) {
    return { changed: false, ...before };
  }

  await execFilePromise('sips', [
    '--cropToHeightWidth', String(crop.height), String(crop.width),
    '--cropOffset', String(crop.offsetY), String(crop.offsetX),
    imagePath
  ], {
    timeout: 60000,
    maxBuffer: 2 * 1024 * 1024
  });

  const after = await readImageDimensions(imagePath);
  return {
    changed: true,
    width: after.width,
    height: after.height,
    originalWidth: before.width,
    originalHeight: before.height
  };
}

function ensureOutputPath(outputPath, ratio) {
  if (outputPath) {
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    return outputPath;
  }
  const suffix = String(ratio || '9:16').replace(/[^0-9a-z]+/gi, '-');
  const generatedPath = path.join(require('os').tmpdir(), `imini-first-frame-${Date.now()}-${suffix}.png`);
  fs.mkdirSync(path.dirname(generatedPath), { recursive: true });
  return generatedPath;
}

function normalizePositiveInt(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(parsed));
}

function getFirstFrameGenerationConcurrency(options = {}) {
  return normalizePositiveInt(
    options.maxConcurrentGenerations ||
      process.env.IMINI_FIRST_FRAME_MAX_CONCURRENT_GENERATIONS ||
      process.env.IMINI_FIRST_FRAME_MAX_CONCURRENCY,
    1
  );
}

function isProcessAlive(pid) {
  const parsed = Number(pid);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return false;
  }
  try {
    process.kill(parsed, 0);
    return true;
  } catch (error) {
    return error && error.code === 'EPERM';
  }
}

function readLockOwner(lockPath) {
  try {
    return JSON.parse(fs.readFileSync(path.join(lockPath, 'owner.json'), 'utf8'));
  } catch {
    return {};
  }
}

function cleanupStaleFirstFrameLocks(lockRoot, staleMs) {
  if (!fs.existsSync(lockRoot)) {
    return;
  }
  const now = Date.now();
  for (const entry of fs.readdirSync(lockRoot)) {
    if (!entry.startsWith('slot-')) {
      continue;
    }
    const lockPath = path.join(lockRoot, entry);
    const owner = readLockOwner(lockPath);
    const startedAt = Date.parse(owner.startedAt || '') || 0;
    const staleByAge = startedAt > 0 && now - startedAt > staleMs;
    const staleByPid = owner.pid && !isProcessAlive(owner.pid);
    if (staleByAge || staleByPid) {
      fs.rmSync(lockPath, { recursive: true, force: true });
    }
  }
}

async function acquireFirstFrameGenerationSlot(options = {}) {
  const maxConcurrent = getFirstFrameGenerationConcurrency(options);
  if (maxConcurrent <= 1) {
    // A single slot still matters because overlapping launchd/manual runs share it.
  }

  const timeoutMs = Math.max(
    30000,
    Number(options.concurrencyWaitTimeoutMs || process.env.IMINI_FIRST_FRAME_CONCURRENCY_WAIT_TIMEOUT_MS) ||
      (Number(options.timeoutMs || process.env.IMINI_FIRST_FRAME_TIMEOUT_MS) || 420000) + 60000
  );
  const staleMs = Math.max(
    timeoutMs + 60000,
    Number(options.concurrencyStaleMs || process.env.IMINI_FIRST_FRAME_CONCURRENCY_STALE_MS) || 30 * 60 * 1000
  );
  const lockRoot = options.concurrencyLockDir ||
    process.env.IMINI_FIRST_FRAME_CONCURRENCY_LOCK_DIR ||
    path.join(require('os').tmpdir(), 'imini-first-frame-generation-locks');
  fs.mkdirSync(lockRoot, { recursive: true });

  const startedWait = Date.now();
  let loggedWait = false;
  while (true) {
    cleanupStaleFirstFrameLocks(lockRoot, staleMs);
    for (let slot = 0; slot < maxConcurrent; slot++) {
      const lockPath = path.join(lockRoot, `slot-${slot}`);
      try {
        fs.mkdirSync(lockPath);
        fs.writeFileSync(path.join(lockPath, 'owner.json'), JSON.stringify({
          pid: process.pid,
          slot,
          startedAt: new Date().toISOString()
        }, null, 2));
        if (loggedWait) {
          console.log(`  🎨 首帧图片生成获得并发槽 ${slot + 1}/${maxConcurrent}`);
        }
        return {
          release() {
            fs.rmSync(lockPath, { recursive: true, force: true });
          }
        };
      } catch (error) {
        if (error && error.code !== 'EEXIST') {
          throw error;
        }
      }
    }

    if (!loggedWait) {
      console.log(`  ⏳ 首帧图片生成并发已满，等待空位 (${maxConcurrent})...`);
      loggedWait = true;
    }
    if (Date.now() - startedWait > timeoutMs) {
      throw new Error(`等待首帧图片生成并发槽超时 (${maxConcurrent})`);
    }
    await sleep(1500);
  }
}

function normalizeOpenClawImageModel(model) {
  const raw = String(model || '').trim();
  const normalized = raw.toLowerCase();
  if (!raw ||
    normalized === 'image2' ||
    normalized === 'image-2' ||
    normalized === 'gpt-image-2' ||
    normalized === 'openai/image2' ||
    normalized === 'codex.openai/image2' ||
    normalized === 'openai-codex/image2') {
    return 'openai/gpt-image-2';
  }
  return raw;
}

function isOpenClawOnlyProvider(provider) {
  const normalized = String(provider || '').trim().toLowerCase();
  return !normalized ||
    normalized === 'openclaw' ||
    normalized === 'codex.openai' ||
    normalized === 'openai-codex' ||
    normalized === 'openai';
}

function isOpenClawCircuitBreakerEnabled(options = {}) {
  if (options.disableOpenClawImageCircuitBreaker === true) return false;
  return String(process.env.IMINI_OPENCLAW_IMAGE_CIRCUIT_BREAKER || '1') !== '0';
}

function getErrorText(error) {
  return [
    error?.message,
    error?.stdout,
    error?.stderr,
    error?.stack
  ].filter(Boolean).map(value => String(value)).join('\n');
}

function isOpenClawSsrFBlockedError(error) {
  const text = getErrorText(error);
  return /SsrFBlockedError|blocked URL fetch|resolves to private\/internal\/special-use IP address/i.test(text);
}

function markOpenClawImageCircuitOpen(error) {
  openClawImageCircuitState.ssrfBlocked = true;
  openClawImageCircuitState.reason = String(error?.message || 'OpenClaw image SSRF blocked').slice(0, 240);
}

function parseShellExportLine(line) {
  const match = String(line || '').match(/^export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
  if (!match) return null;
  let value = match[2].trim();
  if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"'))) {
    value = value.slice(1, -1);
  }
  return { key: match[1], value };
}

function loadOpenClawServiceProxyEnv() {
  const envPath = path.join(process.env.HOME || '', '.openclaw', 'service-env', 'ai.openclaw.gateway.env');
  if (!fs.existsSync(envPath)) return {};

  const wanted = new Set([
    'HTTP_PROXY',
    'HTTPS_PROXY',
    'ALL_PROXY',
    'http_proxy',
    'https_proxy',
    'all_proxy',
    'NO_PROXY',
    'no_proxy',
    'NODE_USE_ENV_PROXY',
    'NODE_EXTRA_CA_CERTS',
    'NODE_USE_SYSTEM_CA'
  ]);
  const env = {};
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const parsed = parseShellExportLine(line);
    if (parsed && wanted.has(parsed.key)) {
      env[parsed.key] = parsed.value;
    }
  }
  return env;
}

function buildOpenClawChildEnv(options = {}) {
  const serviceEnv = loadOpenClawServiceProxyEnv();
  const env = {
    ...process.env,
    ...serviceEnv,
    NO_COLOR: '1',
    NODE_USE_ENV_PROXY: process.env.NODE_USE_ENV_PROXY || serviceEnv.NODE_USE_ENV_PROXY || '1'
  };

  const proxyUrl = options.proxyUrl ||
    process.env.IMINI_FIRST_FRAME_PROXY_URL ||
    process.env.HTTPS_PROXY ||
    process.env.https_proxy ||
    serviceEnv.HTTPS_PROXY ||
    serviceEnv.https_proxy ||
    '';

  if (proxyUrl) {
    env.HTTP_PROXY = proxyUrl;
    env.HTTPS_PROXY = proxyUrl;
    env.http_proxy = proxyUrl;
    env.https_proxy = proxyUrl;
    env.ALL_PROXY = proxyUrl;
    env.all_proxy = proxyUrl;
  }

  const allProxy = options.allProxyUrl ||
    process.env.ALL_PROXY ||
    process.env.all_proxy ||
    serviceEnv.ALL_PROXY ||
    serviceEnv.all_proxy ||
    '';
  if (!proxyUrl && allProxy) {
    env.ALL_PROXY = env.ALL_PROXY || allProxy;
    env.all_proxy = env.all_proxy || allProxy;
  }

  return env;
}

function buildOpenClawImageArgs({ firstFramePrompt, referenceImagePaths, outputPath, ratio, options = {} }) {
  const selectedReferences = selectReferenceImagePaths(referenceImagePaths, options);
  const hasReference = selectedReferences.length > 0;
  const command = hasReference ? 'edit' : 'generate';
  const model = normalizeOpenClawImageModel(options.openclawModel ||
    options.imageModel ||
    process.env.IMINI_FIRST_FRAME_OPENCLAW_MODEL ||
    'openai/gpt-image-2');
  const size = options.size || sizeForRatio(ratio);
  const aspectRatio = normalizeAspectRatio(options.aspectRatio || ratio);
  const timeoutMs = String(Math.max(30000, Number(options.timeoutMs || process.env.IMINI_FIRST_FRAME_TIMEOUT_MS) || 420000));
  const args = [
    'infer',
    'image',
    command,
    '--model', model,
    '--prompt', firstFramePrompt,
    '--size', size,
    '--aspect-ratio', aspectRatio,
    '--output', outputPath,
    '--output-format', options.outputFormat || 'png',
    '--timeout-ms', timeoutMs,
    '--json'
  ];

  if (selectedReferences.length > 0) {
    for (const imagePath of selectedReferences) {
      args.push('--file', imagePath);
    }
  }

  return { args, command, model, size, aspectRatio };
}

function execFilePromise(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    execFile(command, args, options, (error, stdout, stderr) => {
      if (error) {
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function callOpenClawImageGeneration({ firstFramePrompt, referenceImagePaths, outputPath, ratio, options = {} }) {
  const finalPath = ensureOutputPath(outputPath, ratio);
  const openclawBin = options.openclawBin || process.env.OPENCLAW_BIN || 'openclaw';
  const { args, command, model, size, aspectRatio } = buildOpenClawImageArgs({
    firstFramePrompt,
    referenceImagePaths,
    outputPath: finalPath,
    ratio,
    options
  });
  const timeout = Math.max(30000, Number(options.timeoutMs || process.env.IMINI_FIRST_FRAME_TIMEOUT_MS) || 420000);

  console.log(`  🎨 调用 OpenClaw 首帧图片生成 (${command}, model=${model}, size=${size}, ratio=${aspectRatio})...`);
  const startedAt = Date.now();
  const result = await execFilePromise(openclawBin, args, {
    timeout: timeout + 30000,
    maxBuffer: 20 * 1024 * 1024,
    env: buildOpenClawChildEnv(options)
  });

  if (!fs.existsSync(finalPath)) {
    throw new Error(`OpenClaw 图片生成未产出文件: ${finalPath}; stdout=${String(result.stdout || '').slice(0, 500)} stderr=${String(result.stderr || '').slice(0, 500)}`);
  }

  const stat = fs.statSync(finalPath);
  if (stat.size < 1024) {
    throw new Error(`OpenClaw 图片生成结果过小 (${stat.size} bytes): ${finalPath}`);
  }
  const dimensions = await normalizeImageToRatio(finalPath, aspectRatio, options);
  const normalizedStat = fs.statSync(finalPath);
  if (dimensions.changed) {
    console.log(`  📐 首帧尺寸已校正: ${dimensions.originalWidth}x${dimensions.originalHeight} -> ${dimensions.width}x${dimensions.height} (${aspectRatio})`);
  }

  return {
    success: true,
    provider: 'openclaw',
    imagePath: finalPath,
    imageBytes: normalizedStat.size,
    imageWidth: dimensions.width,
    imageHeight: dimensions.height,
    aspectRatio,
    elapsedMs: Date.now() - startedAt,
    stdout: String(result.stdout || '').slice(0, 2000),
    stderr: String(result.stderr || '').slice(0, 2000)
  };
}

async function callOpenAIImageSkillGeneration({ firstFramePrompt, referenceImagePaths, outputPath, ratio, options = {} }) {
  const finalPath = ensureOutputPath(outputPath, ratio);
  const aspectRatio = normalizeAspectRatio(options.aspectRatio || ratio);
  const size = options.size || sizeForRatio(ratio);
  const skillRoot = options.openaiImageSkillRoot ||
    process.env.OPENAI_IMAGE_SKILL_ROOT ||
    '/Users/likeu3/.openclaw/workspace/skills/openai-image';
  const pipelinePath = path.join(skillRoot, 'run_pipeline.py');
  if (!fs.existsSync(pipelinePath)) {
    throw new Error(`openai-image fallback 不存在: ${pipelinePath}`);
  }

  const tempDir = fs.mkdtempSync(path.join('/private/tmp', 'imini-first-frame-'));
  const inputPath = path.join(tempDir, 'input.json');
  const outputDir = path.join(tempDir, 'outputs');
  const selectedReferences = selectReferenceImagePaths(referenceImagePaths, options);
  const hasReference = selectedReferences.length > 0;
  const taskId = `imini_first_frame_${Date.now()}`;
  const payload = {
    task_id: taskId,
    task_type: 'imini_first_frame',
    target_field: 'first_frame',
    mode: hasReference ? 'edit' : 'generate',
    prompt: firstFramePrompt,
    input_image_path: hasReference ? selectedReferences[0] : '',
    input_image_paths: selectedReferences,
    size,
    quality: options.quality || 'medium',
    output_format: options.outputFormat || 'png',
    output_dir: outputDir,
    n: 1,
    metadata: {
      ratio: aspectRatio,
      source: 'jimeng-video-generator'
    }
  };
  fs.writeFileSync(inputPath, JSON.stringify(payload, null, 2), 'utf8');

  const pythonBin = options.openaiImagePython ||
    process.env.OPENAI_IMAGE_PYTHON ||
    process.env.PYTHON ||
    'python3';
  const env = {
    ...process.env,
    OPENAI_IMAGE_API_MODE: process.env.OPENAI_IMAGE_API_MODE || 'codex',
    OPENAI_IMAGE_OUTPUT_DIR: outputDir,
    OPENAI_IMAGE_DEFAULT_SIZE: size,
    OPENAI_IMAGE_DEFAULT_FORMAT: options.outputFormat || 'png',
    PYTHONPATH: `${path.dirname(skillRoot)}:${skillRoot}${process.env.PYTHONPATH ? `:${process.env.PYTHONPATH}` : ''}`,
    ALL_PROXY: options.socksProxyUrl || process.env.SOCKS_PROXY || 'socks5://127.0.0.1:10808',
    all_proxy: options.socksProxyUrl || process.env.socks_proxy || 'socks5://127.0.0.1:10808'
  };

  console.log(`  🎨 调用 openai-image fallback 首帧生成 (${payload.mode}, size=${size}, ratio=${aspectRatio})...`);
  const startedAt = Date.now();
  const timeout = Math.max(30000, Number(options.timeoutMs || process.env.IMINI_FIRST_FRAME_TIMEOUT_MS) || 420000);
  const result = await execFilePromise(pythonBin, [pipelinePath, '--input', inputPath], {
    timeout: timeout + 30000,
    maxBuffer: 20 * 1024 * 1024,
    env
  });

  let parsed = null;
  try {
    parsed = JSON.parse(String(result.stdout || '').trim());
  } catch (error) {
    const resultFiles = fs.existsSync(outputDir)
      ? fs.readdirSync(outputDir).filter(name => name.endsWith('_result.json')).map(name => path.join(outputDir, name))
      : [];
    if (resultFiles.length > 0) {
      parsed = JSON.parse(fs.readFileSync(resultFiles[0], 'utf8'));
    }
  }

  if (!parsed || parsed.status !== 'success') {
    const generatedFiles = fs.existsSync(outputDir)
      ? fs.readdirSync(outputDir)
        .filter(name => /\.(png|jpe?g|webp)$/i.test(name))
        .map(name => path.join(outputDir, name))
        .sort()
      : [];
    if (generatedFiles.length > 0) {
      parsed = {
        status: 'success',
        output_image_paths: [generatedFiles[0]]
      };
    } else {
      throw new Error(`openai-image fallback 失败: ${parsed?.error_message || String(result.stderr || result.stdout || '').slice(0, 500)}`);
    }
  }

  const generatedPath = parsed.output_image_paths?.[0];
  if (!generatedPath || !fs.existsSync(generatedPath)) {
    throw new Error(`openai-image fallback 未产出图片: ${JSON.stringify(parsed).slice(0, 500)}`);
  }
  fs.copyFileSync(generatedPath, finalPath);

  const dimensions = await normalizeImageToRatio(finalPath, aspectRatio, options);
  const stat = fs.statSync(finalPath);
  if (dimensions.changed) {
    console.log(`  📐 首帧尺寸已校正: ${dimensions.originalWidth}x${dimensions.originalHeight} -> ${dimensions.width}x${dimensions.height} (${aspectRatio})`);
  }

  return {
    success: true,
    provider: 'openai-image',
    imagePath: finalPath,
    imageBytes: stat.size,
    imageWidth: dimensions.width,
    imageHeight: dimensions.height,
    aspectRatio,
    elapsedMs: Date.now() - startedAt,
    stdout: String(result.stdout || '').slice(0, 2000),
    stderr: String(result.stderr || '').slice(0, 2000)
  };
}

function extractImageResult(response) {
  const output = Array.isArray(response?.output) ? response.output : [];
  for (const item of output) {
    if (item?.type === 'image_generation_call') {
      const base64 = item.result || item.b64_json || item.image_base64;
      if (base64) {
        return {
          base64,
          revisedPrompt: item.revised_prompt || '',
          imageId: item.id || ''
        };
      }
    }
  }

  for (const item of response?.data || []) {
    if (item?.b64_json) return { base64: item.b64_json, revisedPrompt: item.revised_prompt || '', imageId: '' };
    if (item?.url) return { url: item.url, revisedPrompt: item.revised_prompt || '', imageId: '' };
  }

  return null;
}

function isNonRetryableImageError(error) {
  const message = String(error?.message || error || '').toLowerCase();
  return message.includes('insufficient_quota') ||
    message.includes('quota is not enough') ||
    message.includes('invalid_request_error') ||
    message.includes('unsupported') ||
    message.includes('permission') ||
    message.includes('forbidden') ||
    message.includes('401') ||
    message.includes('403');
}

async function callResponsesImageGeneration({ firstFramePrompt, referenceImagePaths, outputPath, ratio, options = {} }) {
  if (!loadOpenAIConfig || !imagePathToInputItem) {
    throw new Error('OpenAI Responses helper is unavailable; cannot generate first frame with Responses API');
  }
  const openaiConfig = loadOpenAIConfig();
  const apiUrl = `${openaiConfig.baseUrl}/responses`;
  const model = options.responseModel ||
    process.env.IMINI_FIRST_FRAME_RESPONSE_MODEL ||
    openaiConfig.imageResponseModel ||
    openaiConfig.model;
  const timeoutMs = Math.max(30000, Number(options.timeoutMs || process.env.IMINI_FIRST_FRAME_TIMEOUT_MS) || 420000);
  const retryCount = Math.max(1, Number(options.retryCount || process.env.IMINI_FIRST_FRAME_RETRIES) || 2);
  const requestDelayMs = Math.max(0, Number(options.requestDelayMs || process.env.IMINI_FIRST_FRAME_REQUEST_DELAY_MS) || 2000);

  const content = [{ type: 'input_text', text: firstFramePrompt }];

  const selectedReferences = selectReferenceImagePaths(referenceImagePaths, options);
  if (selectedReferences.length > 0) {
    for (let i = 0; i < selectedReferences.length; i++) {
      try {
        content.push(imagePathToInputItem(selectedReferences[i]));
      } catch (error) {
        console.log(`  ⚠️ 首帧参考图 ${i + 1} 读取失败: ${error.message}`);
      }
    }
  }

  const tool = {
    type: 'image_generation',
    size: options.size || sizeForRatio(ratio),
    quality: options.quality || 'medium',
    output_format: options.outputFormat || 'png'
  };
  if (options.action) {
    tool.action = options.action;
  }

  const body = {
    model,
    input: [
      {
        role: 'user',
        content
      }
    ],
    tools: [tool]
  };
  if (options.forceToolChoice === true) {
    body.tool_choice = { type: 'image_generation' };
  }

  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt++) {
    if (requestDelayMs > 0) {
      await sleep(requestDelayMs);
    }

    try {
      console.log(`  🎨 调用首帧图片生成 (${attempt}/${retryCount}, model=${model}, size=${tool.size})...`);
      const response = await postJson(apiUrl, openaiConfig.apiKey, body, timeoutMs);
      const imageResult = extractImageResult(response);
      if (!imageResult) {
        throw new Error('图片生成响应里没有 image_generation_call.result / b64_json');
      }

      let imageBuffer;
      if (imageResult.base64) {
        imageBuffer = Buffer.from(imageResult.base64.replace(/^data:image\/\w+;base64,/, ''), 'base64');
      } else if (imageResult.url) {
        imageBuffer = await downloadUrlToBuffer(imageResult.url, timeoutMs);
      }

      if (!imageBuffer || imageBuffer.length < 1024) {
        throw new Error(`图片生成结果为空或过小 (${imageBuffer ? imageBuffer.length : 0} bytes)`);
      }

      const aspectRatio = normalizeAspectRatio(options.aspectRatio || ratio);
      const finalPath = ensureOutputPath(outputPath, ratio);
      fs.writeFileSync(finalPath, imageBuffer);
      const dimensions = await normalizeImageToRatio(finalPath, aspectRatio, options);
      const stat = fs.statSync(finalPath);
      if (dimensions.changed) {
        console.log(`  📐 首帧尺寸已校正: ${dimensions.originalWidth}x${dimensions.originalHeight} -> ${dimensions.width}x${dimensions.height} (${aspectRatio})`);
      }
      return {
        success: true,
        imagePath: finalPath,
        imageBytes: stat.size,
        imageWidth: dimensions.width,
        imageHeight: dimensions.height,
        aspectRatio,
        responseId: response.id || '',
        revisedPrompt: imageResult.revisedPrompt || '',
        imageId: imageResult.imageId || ''
      };
    } catch (error) {
      lastError = error;
      console.log(`  ❌ 首帧图片生成失败: ${error.message}`);
      if (isNonRetryableImageError(error)) {
        break;
      }
      if (attempt < retryCount) {
        await sleep(2000);
      }
    }
  }

  return { success: false, error: lastError?.message || '首帧图片生成失败' };
}

async function generateFirstFrameImageWithLLM(firstFramePrompt, referenceImagePaths, options = {}) {
  const generationSlot = await acquireFirstFrameGenerationSlot(options);
  const provider = String(options.provider || process.env.IMINI_FIRST_FRAME_PROVIDER || 'openclaw').toLowerCase();
  try {
    if (provider !== 'responses') {
      if (
        isOpenClawOnlyProvider(provider) &&
        isOpenClawCircuitBreakerEnabled(options) &&
        openClawImageCircuitState.ssrfBlocked
      ) {
        console.log(`  ↩️ OpenClaw 图片生成已因 SSRF 熔断，本进程直接使用 openai-image fallback: ${openClawImageCircuitState.reason}`);
        try {
          return await callOpenAIImageSkillGeneration({
            firstFramePrompt,
            referenceImagePaths,
            outputPath: options.outputPath,
            ratio: options.ratio,
            options
          });
        } catch (fallbackError) {
          console.log(`  ❌ openai-image fallback 首帧图片生成失败: ${fallbackError.message}`);
          return { success: false, error: fallbackError.message, provider };
        }
      }

      try {
        return await callOpenClawImageGeneration({
          firstFramePrompt,
          referenceImagePaths,
          outputPath: options.outputPath,
          ratio: options.ratio,
          options
        });
      } catch (error) {
        console.log(`  ❌ OpenClaw 首帧图片生成失败: ${error.message}`);
        if (isOpenClawCircuitBreakerEnabled(options) && isOpenClawSsrFBlockedError(error)) {
          markOpenClawImageCircuitOpen(error);
          console.log('  ⚡ OpenClaw 图片生成 SSRF 熔断已开启：本进程后续首帧将直接走 openai-image fallback');
        }
        if (isOpenClawOnlyProvider(provider)) {
          try {
            return await callOpenAIImageSkillGeneration({
              firstFramePrompt,
              referenceImagePaths,
              outputPath: options.outputPath,
              ratio: options.ratio,
              options
            });
          } catch (fallbackError) {
            console.log(`  ❌ openai-image fallback 首帧图片生成失败: ${fallbackError.message}`);
            return { success: false, error: fallbackError.message || error.message, provider };
          }
        }
        console.log('  ↩️ 首帧图片生成切换到 Responses fallback');
      }
    }

    try {
      return await callResponsesImageGeneration({
        firstFramePrompt,
        referenceImagePaths,
        outputPath: options.outputPath,
        ratio: options.ratio,
        options
      });
    } catch (error) {
      return { success: false, error: error.message };
    }
  } finally {
    try {
      generationSlot.release();
    } catch (error) {
      console.log(`  ⚠️ 首帧图片生成并发槽释放失败: ${error.message}`);
    }
  }
}

function selectReferenceImagePaths(referenceImagePaths, options = {}) {
  if (!Array.isArray(referenceImagePaths) || referenceImagePaths.length === 0) return [];
  const configured = Number(options.referenceImageLimit || process.env.IMINI_FIRST_FRAME_REFERENCE_LIMIT || 3);
  const limit = Math.max(1, Math.min(referenceImagePaths.length, Number.isFinite(configured) && configured > 0 ? configured : 3));
  return referenceImagePaths.slice(0, limit).filter(Boolean);
}

module.exports = {
  buildFirstFramePrompt,
  generateFirstFrameImageWithLLM,
  sizeForRatio,
  normalizeAspectRatio,
  targetCropForRatio,
  readImageDimensions,
  normalizeImageToRatio,
  buildOpenClawImageArgs,
  normalizeOpenClawImageModel,
  buildOpenClawChildEnv,
  loadOpenClawServiceProxyEnv,
  selectReferenceImagePaths,
  extractImageResult,
  isNonRetryableImageError,
  extractMotionArc,
  deriveFirstFrameStartState,
  isResultOnlyMotion,
  CATEGORY_START_STATE_RULES,
  buildFirstFrameStartStateBlock,
};
