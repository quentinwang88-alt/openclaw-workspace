#!/usr/bin/env node
/** Smoke test for first-frame start state detection. */
const { extractMotionArc, deriveFirstFrameStartState, isResultOnlyMotion } = require('../platforms/imini/first-frame');

const tests = [
  {
    name: '女装外套：拿起 -> 穿上 -> 整理 -> 成型',
    prompt: '运镜/动作弧线：\n拿起大衣 -> 穿上 -> 整理肩部 -> 廓形成型',
    category: '女装外套',
    expectStartState: true,
    expectHint: /拿/,
  },
  {
    name: '发饰：手快速入画 -> 发饰固定 -> 反差揭示',
    prompt: '运镜/动作弧线：\n手快速入画 -> 发饰固定 -> 造型反差揭示',
    category: '发饰',
    expectStartState: true,
    expectHint: /手拿发饰|尚未/,
  },
  {
    name: '耳环：手拿 -> 戴上 -> 转头',
    prompt: '运镜/动作弧线：\n手拿耳饰靠近耳侧 -> 戴上 -> 转头闪光',
    category: '耳环',
    expectStartState: true,
    expectHint: /尚未/,
  },
  {
    name: '结果展示例外：穿着展示 -> 轻转身 -> 停留',
    prompt: '运镜/动作弧线：\n穿着外套展示上身效果 -> 轻微侧转 -> 停留',
    category: '女装外套',
    expectStartState: false,
    expectHint: /结果展示/,
  },
  {
    name: '发饰起点态：靠近 -> 夹上 -> 转头',
    prompt: '运镜/动作弧线：\n拿起发夹靠近头发 -> 夹上 -> 转头展示',
    category: '发饰',
    expectStartState: true,
    expectHint: /拿/,
  },
];

let passed = 0;
let failed = 0;

for (const tc of tests) {
  const motion = extractMotionArc(tc.prompt);
  const result = deriveFirstFrameStartState(tc.prompt, tc.category);

  const ok = result.isStartState === tc.expectStartState && (tc.expectHint ? tc.expectHint.test(result.hint) : true);
  if (ok) {
    console.log(`  ✅ ${tc.name}`);
    console.log(`     motion="${motion}"`);
    console.log(`     startState=${result.isStartState} hint="${result.hint}" matched=${result.matched}`);
    passed++;
  } else {
    console.log(`  ❌ ${tc.name}`);
    console.log(`     expected startState=${tc.expectStartState}, got ${result.isStartState}`);
    console.log(`     motion="${motion}" hint="${result.hint}"`);
    failed++;
  }
  console.log();
}

console.log(`${passed}/${tests.length} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
