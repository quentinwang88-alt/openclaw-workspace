let callOpenAIResponses = null;
let imagePathToInputItem = null;
let loadOpenAIConfig = null;
try {
  ({ callOpenAIResponses, imagePathToInputItem, loadOpenAIConfig } = require('../../../prompt-expander/openai-responses-helper'));
} catch (error) {
  callOpenAIResponses = null;
  imagePathToInputItem = null;
  loadOpenAIConfig = null;
}
const { detectCategory, getCompositionRule, getProductWidthRatio, getForbiddenRules } = require('./category-rules');

function extractHardConstraints(prompt) {
  const constraints = {
    mustKeep: [],
    mustAvoid: [],
    colorConstraints: [],
    materialConstraints: [],
    sizeConstraints: [],
    countConstraints: []
  };

  if (!prompt) return constraints;

  const lines = String(prompt).split(/[\n。；]/);
  const mustPatterns = [/必须/, /一定要/, /不得/, /不要/, /不能/, /禁止/, /避免/, /严禁/];
  const colorPattern = /(?:颜色|色彩|色调)[是为：:]\s*([^\s,，。；\n]+)/;
  const materialPattern = /(?:材质|面料|面料|质地)[是为：:]\s*([^\s,，。；\n]+)/;
  const sizePattern = /(?:长约|宽约|高约|尺寸)[是为：:]\s*(\d+\.?\d*\s*(?:cm|mm|m|厘米|毫米)?)/;
  const countPattern = /(?:\d+)\s*个(?:扣子|纽扣|链条|拉链|按钮)/;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    for (const pattern of mustPatterns) {
      if (pattern.test(trimmed)) {
        if (/不要|不得|禁止|避免|不能|严禁/.test(trimmed)) {
          constraints.mustAvoid.push(trimmed);
        } else {
          constraints.mustKeep.push(trimmed);
        }
        break;
      }
    }

    const colorMatch = trimmed.match(colorPattern);
    if (colorMatch) constraints.colorConstraints.push(colorMatch[1]);

    const materialMatch = trimmed.match(materialPattern);
    if (materialMatch) constraints.materialConstraints.push(materialMatch[1]);

    const sizeMatch = trimmed.match(sizePattern);
    if (sizeMatch) constraints.sizeConstraints.push(sizeMatch[1]);

    const countMatch = trimmed.match(countPattern);
    if (countMatch) constraints.countConstraints.push(countMatch[0]);
  }

  return constraints;
}

function extractShotOne(prompt) {
  if (!prompt) return '';
  const shotPatterns = [
    /镜头\s*1[：:]\s*([^\n]{10,200})/i,
    /镜头一[：:]\s*([^\n]{10,200})/i,
    /0[-—]\s*3\s*秒[：:]\s*([^\n]{10,200})/i,
    /开场[：:]\s*([^\n]{10,200})/i,
    /开场画面[：:]\s*([^\n]{10,200})/i
  ];

  for (const pattern of shotPatterns) {
    const match = String(prompt).match(pattern);
    if (match) return match[1].trim();
  }

  return '';
}

function extractComposition(hardConstraints, category) {
  const categoryRule = getCompositionRule(category);
  const parts = [];
  if (categoryRule) parts.push(categoryRule);
  for (const keep of hardConstraints.mustKeep) {
    if (keep.includes('近景') || keep.includes('中景') || keep.includes('远景') || keep.includes('特写') || keep.includes('半身') || keep.includes('手持') || keep.includes('俯拍') || keep.includes('镜前')) {
      parts.push(keep);
    }
  }
  return parts.join('，');
}

function buildProductLockCard(prompt, referenceImagePaths, productAnchor) {
  const lock = {
    productType: '',
    mainColor: '',
    secondaryColor: '',
    material: '',
    shapeStructure: '',
    sizeProportion: '',
    keyDetails: '',
    wearingMethod: '',
    mustKeep: '',
    mustAvoid: ''
  };

  const hardConstraints = extractHardConstraints(prompt);
  const category = detectCategory(prompt, null);

  if (category.category) {
    lock.productType = category.category;
  }

  for (const color of hardConstraints.colorConstraints) {
    if (!lock.mainColor) {
      lock.mainColor = color;
    } else if (!lock.secondaryColor) {
      lock.secondaryColor = color;
    }
  }

  for (const material of hardConstraints.materialConstraints) {
    lock.material = material;
  }

  for (const size of hardConstraints.sizeConstraints) {
    lock.sizeProportion = size;
  }

  if (hardConstraints.mustKeep.length > 0) {
    lock.mustKeep = hardConstraints.mustKeep.join('；');
  }

  const allAvoids = [...hardConstraints.mustAvoid, ...getForbiddenRules(category.category)];
  if (allAvoids.length > 0) {
    lock.mustAvoid = allAvoids.join('；');
  }

  if (productAnchor) {
    if (!lock.productType && productAnchor.productType) lock.productType = productAnchor.productType;
    if (!lock.mainColor && productAnchor.mainColor) lock.mainColor = productAnchor.mainColor;
    if (!lock.material && productAnchor.material) lock.material = productAnchor.material;
    if (!lock.shapeStructure && productAnchor.shapeStructure) lock.shapeStructure = productAnchor.shapeStructure;
    if (!lock.keyDetails && productAnchor.keyDetails) lock.keyDetails = productAnchor.keyDetails;
    if (!lock.wearingMethod && productAnchor.wearingMethod) lock.wearingMethod = productAnchor.wearingMethod;
  }

  return lock;
}

function formatProductLockCard(lock) {
  const lines = ['商品一致性锁定卡：'];
  if (lock.productType) lines.push(`- 商品类型：${lock.productType}`);
  if (lock.mainColor) lines.push(`- 主颜色：${lock.mainColor}`);
  if (lock.secondaryColor) lines.push(`- 辅助颜色：${lock.secondaryColor}`);
  if (lock.material) lines.push(`- 材质/光泽：${lock.material}`);
  if (lock.shapeStructure) lines.push(`- 形状结构：${lock.shapeStructure}`);
  if (lock.sizeProportion) lines.push(`- 尺寸比例：${lock.sizeProportion}`);
  if (lock.keyDetails) lines.push(`- 关键细节：${lock.keyDetails}`);
  if (lock.wearingMethod) lines.push(`- 佩戴/手持方式：${lock.wearingMethod}`);
  lines.push(`- 必须保留：${lock.mustKeep || '参考图原样'}`);
  if (lock.mustAvoid) lines.push(`- 禁止变化：${lock.mustAvoid}`);
  return lines.join('\n');
}

async function generateProductLockCardWithLLM(prompt, referenceImagePaths, productAnchor) {
  if (!callOpenAIResponses || !imagePathToInputItem || !loadOpenAIConfig) {
    throw new Error('OpenAI Responses helper is unavailable; cannot generate product lock card with LLM');
  }
  const lock = buildProductLockCard(prompt, referenceImagePaths, productAnchor);

  if (!lock.productType && !lock.keyDetails) {
    return { lock, needsLLM: true, reason: '缺少商品类型或关键细节，需要 LLM 补充' };
  }

  if (!referenceImagePaths || referenceImagePaths.length === 0) {
    return { lock, needsLLM: false, reason: '无参考图，基于提示词提取' };
  }

  return { lock, needsLLM: false, reason: '基于提示词和类目规则提取' };
}

async function enrichProductLockCardWithLLM(lock, prompt, referenceImagePaths) {
  if (!callOpenAIResponses || !imagePathToInputItem || !loadOpenAIConfig) {
    throw new Error('OpenAI Responses helper is unavailable; cannot enrich product lock card with LLM');
  }
  const input = [];
  input.push({
    type: 'input_text',
    text: `分析以下短视频提示词和参考图（如有），提取商品一致性锁定卡。

短视频提示词：
${prompt || '（无）'}

已有部分锁定卡信息：
${formatProductLockCard(lock)}

请补充或修正锁定卡中缺失的字段，特别关注：
1. 商品类型（如果缺失）
2. 主颜色和辅助颜色
3. 材质/光泽
4. 形状结构和关键细节
5. 佩戴/手持方式

输出格式为 JSON：
{
  "productType": "",
  "mainColor": "",
  "secondaryColor": "",
  "material": "",
  "shapeStructure": "",
  "sizeProportion": "",
  "keyDetails": "",
  "wearingMethod": "",
  "mustKeep": "",
  "mustAvoid": ""
}`
  });

  if (referenceImagePaths && referenceImagePaths.length > 0) {
    const maxImages = Math.min(referenceImagePaths.length, 6);
    for (let i = 0; i < maxImages; i++) {
      try {
        input.push(imagePathToInputItem(referenceImagePaths[i]));
      } catch (error) {
        console.log(`  ⚠️ 参考图 ${i + 1} 读取失败: ${error.message}`);
      }
    }
  }

  try {
    const response = await callOpenAIResponses({
      input,
      temperature: 0.3,
      maxOutputTokens: 1024
    });

    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      return {
        productType: parsed.productType || lock.productType,
        mainColor: parsed.mainColor || lock.mainColor,
        secondaryColor: parsed.secondaryColor || lock.secondaryColor,
        material: parsed.material || lock.material,
        shapeStructure: parsed.shapeStructure || lock.shapeStructure,
        sizeProportion: parsed.sizeProportion || lock.sizeProportion,
        keyDetails: parsed.keyDetails || lock.keyDetails,
        wearingMethod: parsed.wearingMethod || lock.wearingMethod,
        mustKeep: parsed.mustKeep || lock.mustKeep,
        mustAvoid: parsed.mustAvoid || lock.mustAvoid
      };
    }
  } catch (error) {
    console.log(`  ⚠️ LLM 锁定卡补充失败: ${error.message}`);
  }

  return lock;
}

module.exports = {
  extractHardConstraints,
  extractShotOne,
  extractComposition,
  buildProductLockCard,
  formatProductLockCard,
  generateProductLockCardWithLLM,
  enrichProductLockCardWithLLM
};
