const CATEGORY_RULES = {
  '耳环': {
    keywords: ['耳环', '耳线', '耳饰', '耳坠', '耳夹', 'earring', 'earrings'],
    composition: '耳部近景，饰品清楚不被头发遮挡，长度比例准确',
    faceSafeComposition: '无脸耳部/耳垂局部近景，只保留耳朵、耳垂、颈侧和少量头发边缘；不要出现眼睛、鼻子、嘴巴、完整侧脸或正脸，可使用无脸假模特耳部',
    productWidthRatio: '1/4',
    forbidden: ['饰品被头发遮挡', '饰品比例变形', '多个首饰混入']
  },
  '项链': {
    keywords: ['项链', '吊坠', '颈链', '锁骨链', '项圈', 'necklace', 'necklaces', 'pendant', 'kalung'],
    composition: '颈部到胸前中近景，吊坠和链条完整呈现',
    faceSafeComposition: '无脸颈部到胸前中近景，画面从下巴以下开始，只展示锁骨、颈部和胸前商品；不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/3',
    forbidden: ['链条断裂', '吊坠变形', '多个首饰混入']
  },
  '发饰': {
    keywords: ['发饰', '发夹', '头饰', '发簪', '发带', '发圈', 'hair_accessories', 'hair accessory', 'hair accessories', 'hair clip', 'hairclip'],
    composition: '头发或手持近景，夹子正面结构完整',
    faceSafeComposition: '无脸头发局部或手持近景，只展示发饰与发丝/手部；不要出现眼睛、鼻子、嘴巴、完整脸或肖像感',
    productWidthRatio: '1/4',
    forbidden: ['发饰结构不完整', '夹子方向反转', '多个发饰混入']
  },
  '女装外套': {
    keywords: [
      '女装外套', '外套', '夹克', '西装', '大衣', '风衣', '开衫', '罩衫', '卫衣外套', '针织外套',
      'womens_outerwear', 'womens_top', 'womens_tops', 'outerwear', 'jacket', 'coat', 'cardigan', 'blazer',
      '女装上衣', '外搭',
    ],
    composition: '镜前中景或半身中景，衣长、肩线、领口、袖型、袖口、下摆、开合方式和整体廓形清楚',
    faceSafeComposition: '可使用镜前手机完整遮脸，或自然裁切到下巴以下/肩颈以下；不得出现可识别人脸',
    productWidthRatio: '1/3',
    forbidden: [
      '衣长错误', '袖型错误', '领口错误', '外套变成T恤/衬衫/卫衣', '廓形变形',
      '已经穿好但动作弧线要求从穿上开始'
    ],
  },
  '上装': {
    keywords: ['上装', '上衣', 'T恤', '衬衫', '卫衣', '毛衣', '吊带', '背心'],
    composition: '半身或镜前中景，领口、扣子、袖型、版型清楚',
    faceSafeComposition: '无脸半身中景或镜前中景，画面从下巴以下到腰部，领口、扣子、袖型、版型清楚；不要出现眼睛、鼻子、嘴巴、完整脸或肖像感',
    productWidthRatio: '1/3',
    forbidden: ['版型变形', '面料质感丢失', '多个上装混入']
  },
  '手链': {
    keywords: ['手链', '手镯', '腕链', 'bracelet', 'bracelets', 'bangle', 'bangles'],
    composition: '手腕近景，商品完整清楚，佩戴位置自然',
    faceSafeComposition: '手腕或手部局部近景，商品完整清楚；画面中不要出现脸、眼睛、鼻子或嘴巴',
    productWidthRatio: '1/5',
    forbidden: ['手链变成戒指', '手腕比例失调', '多个首饰混入']
  },
  '戒指': {
    keywords: ['戒指', '指环', 'ring', 'rings'],
    composition: '手部近景，商品居中，形状完整',
    faceSafeComposition: '手部近景，商品居中，形状完整；画面中不要出现脸、眼睛、鼻子或嘴巴',
    productWidthRatio: '1/5',
    forbidden: ['戒指形状不完整', '手部比例失调', '多个首饰混入']
  },
  '围巾': {
    keywords: ['围巾', '披肩', '丝巾', '方巾'],
    composition: '上半身中景，纹理、颜色、边缘形态清楚',
    faceSafeComposition: '无脸上半身中景，画面从下巴以下开始，纹理、颜色、边缘形态清楚；不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/3',
    forbidden: ['围巾纹理丢失', '颜色偏差', '多个围巾混入']
  },
  '下装': {
    keywords: ['下装', '裤子', '裙子', '半裙', '短裤', '长裤', '短裙', '牛仔裤'],
    composition: '下半身中景或全身镜前，版型、面料、长度清楚',
    faceSafeComposition: '下半身中景或无脸镜前中景，画面最多到肩颈以下，版型、面料、长度清楚；不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/3',
    forbidden: ['版型变形', '面料质感丢失', '多个下装混入']
  },
  '鞋': {
    keywords: ['鞋', '高跟鞋', '平底鞋', '凉鞋', '靴子', '运动鞋'],
    composition: '脚部或手持近景，鞋型、材质、颜色完整',
    faceSafeComposition: '脚部或手持近景，鞋型、材质、颜色完整；画面中不要出现脸、眼睛、鼻子或嘴巴',
    productWidthRatio: '1/4',
    forbidden: ['鞋型变形', '颜色偏差', '多双鞋混入']
  },
  '包': {
    keywords: ['包', '手提包', '背包', '斜挎包', '手包', '钱包', 'bag', 'bags', 'handbag', 'backpack', 'crossbody bag', 'wallet'],
    composition: '手持或肩背近景，包型、材质、颜色完整',
    faceSafeComposition: '手持或肩背无脸近景，包型、材质、颜色完整；画面从下巴以下或肩部以下开始，不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/4',
    forbidden: ['包型变形', '材质丢失', '多个包混入']
  }
};

const CATEGORY_ALIASES = {
  hair_accessories: '发饰',
  hair_accessory: '发饰',
  hair_clip: '发饰',
  hairclip: '发饰',
  earrings: '耳环',
  earring: '耳环',
  bracelets: '手链',
  bracelet: '手链',
  bags: '包',
  bag: '包',
  necklaces: '项链',
  necklace: '项链',
  rings: '戒指',
  ring: '戒指'
};

function normalizeCategoryAlias(value) {
  const key = String(value || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
  return CATEGORY_ALIASES[key] || null;
}

function matchCategory(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  const lower = raw.toLowerCase();
  for (const [category, rule] of Object.entries(CATEGORY_RULES)) {
    if (rule.keywords.some(kw => {
      const word = String(kw);
      return raw.includes(word) || lower.includes(word.toLowerCase());
    })) {
      return { category, rule };
    }
  }
  return null;
}

function detectCategory(prompt, productLock, contextCategory) {
  const alias = normalizeCategoryAlias(contextCategory);
  if (alias && CATEGORY_RULES[alias]) {
    return { category: alias, rule: CATEGORY_RULES[alias] };
  }

  const byContext = matchCategory(contextCategory);
  if (byContext) return byContext;

  if (productLock && productLock.productType) {
    const byProductType = matchCategory(productLock.productType);
    if (byProductType) return byProductType;
  }

  const byPrompt = matchCategory(prompt);
  if (byPrompt) return byPrompt;

  return { category: null, rule: null };
}

function getCompositionRule(category) {
  if (!category) return '';
  const entry = CATEGORY_RULES[category];
  return entry ? entry.composition : '';
}

function getProductWidthRatio(category) {
  if (!category) return '1/4';
  const entry = CATEGORY_RULES[category];
  return entry ? entry.productWidthRatio : '1/4';
}

function getForbiddenRules(category) {
  if (!category) return [];
  const entry = CATEGORY_RULES[category];
  return entry ? entry.forbidden : [];
}

function getFaceSafeCompositionRule(category) {
  if (!category) {
    return '无可识别人脸的商品展示构图，优先使用商品局部、手部、无脸半身、衣架、展示架或无脸假模特；不要出现眼睛、鼻子、嘴巴或完整脸';
  }
  const entry = CATEGORY_RULES[category];
  return entry && entry.faceSafeComposition
    ? entry.faceSafeComposition
    : '无可识别人脸的商品展示构图，优先使用商品局部、手部、无脸半身、衣架、展示架或无脸假模特；不要出现眼睛、鼻子、嘴巴或完整脸';
}

module.exports = {
  CATEGORY_RULES,
  detectCategory,
  getCompositionRule,
  getProductWidthRatio,
  getForbiddenRules,
  getFaceSafeCompositionRule
};
