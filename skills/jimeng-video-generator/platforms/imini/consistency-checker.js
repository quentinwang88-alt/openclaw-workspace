let callOpenAIResponses = null;
let imagePathToInputItem = null;
try {
  ({ callOpenAIResponses, imagePathToInputItem } = require('../../../prompt-expander/openai-responses-helper'));
} catch (error) {
  callOpenAIResponses = null;
  imagePathToInputItem = null;
}
const { formatProductLockCard } = require('./product-lock');

async function checkFirstFrameConsistency(originalImagePath, firstFrameImagePath, productLock, hardConstraints) {
  if (!callOpenAIResponses || !imagePathToInputItem) {
    return {
      pass: false,
      score: 0,
      issues: ['OpenAI Responses helper is unavailable; consistency check skipped'],
      fatalIssues: []
    };
  }

  const input = [];

  input.push({
    type: 'input_text',
    text: `你是一个商品一致性检查专家。请对比参考图和首帧图，检查商品外观是否一致。

${productLock ? formatProductLockCard(productLock) : ''}

${hardConstraints && hardConstraints.mustKeep ? `硬约束（必须满足）：${hardConstraints.mustKeep}` : ''}

${hardConstraints && hardConstraints.mustAvoid ? `硬约束（禁止出现）：${hardConstraints.mustAvoid}` : ''}

请检查以下硬失败条件：
1. 商品类型是否变了（如：耳环变成项链）
2. 主颜色是否明显不一致
3. 关键结构是否缺失（如：扣子没了、链条断了）
4. 硬约束是否不满足（如：扣子数量不对、链条长度不对、不要露出尾端但露出了）
5. 商品是否被严重遮挡或裁切
6. 商品是否不是画面主体
7. 是否出现文字、水印、logo

输出 JSON：
{
  "pass": true/false,
  "score": 0.0-1.0,
  "issues": ["问题1", "问题2"],
  "fatalIssues": ["致命问题1"]
}

判断标准：
- pass=true: 商品外观基本一致，没有严重问题
- pass=false: 存在至少一个致命问题

score 规则：
- 0.9+：非常一致
- 0.7-0.9：基本一致但有轻微差异
- 0.5-0.7：有明显差异
- <0.5：严重不一致`
  });

  if (originalImagePath) {
    try {
      input.push(imagePathToInputItem(originalImagePath));
    } catch (error) {
      console.log(`  ⚠️ 原始参考图读取失败: ${error.message}`);
    }
  }

  if (firstFrameImagePath) {
    try {
      input.push(imagePathToInputItem(firstFrameImagePath));
    } catch (error) {
      console.log(`  ⚠️ 首帧图读取失败: ${error.message}`);
    }
  }

  try {
    const response = await callOpenAIResponses({
      input,
      temperature: 0.1,
      maxOutputTokens: 1024
    });

    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      return {
        pass: Boolean(parsed.pass),
        score: Number(parsed.score) || 0,
        issues: Array.isArray(parsed.issues) ? parsed.issues : [],
        fatalIssues: Array.isArray(parsed.fatalIssues) ? parsed.fatalIssues : []
      };
    }

    return {
      pass: false,
      score: 0,
      issues: ['LLM 返回格式无法解析'],
      fatalIssues: ['无法解析一致性检查结果']
    };
  } catch (error) {
    return {
      pass: false,
      score: 0,
      issues: [`一致性检查调用失败: ${error.message}`],
      fatalIssues: [`一致性检查调用失败: ${error.message}`]
    };
  }
}

module.exports = {
  checkFirstFrameConsistency
};
