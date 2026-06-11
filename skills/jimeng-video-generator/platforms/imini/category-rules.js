const CATEGORY_RULES = {
  '耳环': {
    keywords: ['耳环', '耳线', '耳饰', '耳坠', '耳夹'],
    composition: '耳部近景，饰品清楚不被头发遮挡，长度比例准确',
    faceSafeComposition: '无脸耳部/耳垂局部近景，只保留耳朵、耳垂、颈侧和少量头发边缘；不要出现眼睛、鼻子、嘴巴、完整侧脸或正脸，可使用无脸假模特耳部',
    productWidthRatio: '1/4',
    forbidden: ['饰品被头发遮挡', '饰品比例变形', '多个首饰混入']
  },
  '项链': {
    keywords: ['项链', '吊坠', '颈链', '锁骨链', '项圈'],
    composition: '颈部到胸前中近景，吊坠和链条完整呈现',
    faceSafeComposition: '无脸颈部到胸前中近景，画面从下巴以下开始，只展示锁骨、颈部和胸前商品；不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/3',
    forbidden: ['链条断裂', '吊坠变形', '多个首饰混入']
  },
  '发饰': {
    keywords: ['发饰', '发夹', '头饰', '发簪', '发带', '发圈'],
    composition: '头发或手持近景，夹子正面结构完整',
    faceSafeComposition: '无脸头发局部或手持近景，只展示发饰与发丝/手部；不要出现眼睛、鼻子、嘴巴、完整脸或肖像感',
    productWidthRatio: '1/4',
    forbidden: ['发饰结构不完整', '夹子方向反转', '多个发饰混入']
  },
  '上装': {
    keywords: ['上装', '上衣', 'T恤', '衬衫', '卫衣', '毛衣', '外套', '夹克', '西装', '吊带', '背心', '罩衫', '开衫'],
    composition: '半身或镜前中景，领口、扣子、袖型、版型清楚',
    faceSafeComposition: '无脸半身中景或镜前中景，画面从下巴以下到腰部，领口、扣子、袖型、版型清楚；不要出现眼睛、鼻子、嘴巴、完整脸或肖像感',
    productWidthRatio: '1/3',
    forbidden: ['版型变形', '面料质感丢失', '多个上装混入']
  },
  '戒指': {
    keywords: ['戒指', '手链', '指环'],
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
    keywords: ['包', '手提包', '背包', '斜挎包', '手包', '钱包'],
    composition: '手持或肩背近景，包型、材质、颜色完整',
    faceSafeComposition: '手持或肩背无脸近景，包型、材质、颜色完整；画面从下巴以下或肩部以下开始，不要出现眼睛、鼻子、嘴巴或完整脸',
    productWidthRatio: '1/4',
    forbidden: ['包型变形', '材质丢失', '多个包混入']
  }
};

function detectCategory(prompt, productLock) {
  if (productLock && productLock.productType) {
    const type = String(productLock.productType).trim();
    for (const [category, rule] of Object.entries(CATEGORY_RULES)) {
      if (rule.keywords.some(kw => type.includes(kw))) {
        return { category, rule };
      }
    }
  }

  const text = String(prompt || '').toLowerCase();
  for (const [category, rule] of Object.entries(CATEGORY_RULES)) {
    if (rule.keywords.some(kw => text.includes(kw))) {
      return { category, rule };
    }
  }

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
