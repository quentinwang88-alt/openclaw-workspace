#!/usr/bin/env node

const puppeteer = require('puppeteer-core');

const TASK_IDS = [
  'DETAILAB_20260723_V1_S1_A_R1',
  'DETAILAB_20260723_V1_S1_A_R2',
  'DETAILAB_20260723_V1_S1_B_R1',
  'DETAILAB_20260723_V1_S1_B_R2',
  'DETAILAB_20260723_V1_S1_C_R1',
  'DETAILAB_20260723_V1_S1_C_R2',
  'DETAILAB_20260723_V1_S1_D_R1',
  'DETAILAB_20260723_V1_S1_D_R2',
];

async function main() {
  const browser = await puppeteer.connect({ browserURL: 'http://127.0.0.1:9222' });
  const pages = await browser.pages();
  const summary = [];

  for (let pageIndex = 0; pageIndex < pages.length; pageIndex += 1) {
    const page = pages[pageIndex];
    const url = page.url();
    const title = await page.title().catch(() => '');
    const item = { pageIndex, url, title };

    if (/imini\.com\/zh\/video/.test(url)) {
      item.tasks = await page.evaluate((taskIds) => taskIds.map((taskId) => {
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
        let matched = null;
        let matchedTextLength = Number.POSITIVE_INFINITY;
        while (walker.nextNode()) {
          const text = (walker.currentNode.textContent || '').trim();
          if (text.includes(taskId) && text.length < matchedTextLength) {
            matched = walker.currentNode;
            matchedTextLength = text.length;
          }
        }
        if (!matched) return { taskId, found: false };

        let taskNode = matched;
        while (taskNode && !String(taskNode.getAttribute('data-id') || '').startsWith('task_')) {
          taskNode = taskNode.parentElement;
        }
        if (!taskNode) return { taskId, found: true, taskNode: null };

        const describe = (node, relation) => {
          if (!node) return null;
          const media = Array.from(node.querySelectorAll('img,video,source')).map((el) => ({
            tag: el.tagName,
            src: el.currentSrc || el.src || el.getAttribute('src') || '',
            poster: el.getAttribute('poster') || '',
          })).filter((entry) => entry.src || entry.poster);
          const attrs = {};
          for (const attr of Array.from(node.attributes || [])) {
            if (/^(data-|id$|class$)/.test(attr.name)) attrs[attr.name] = attr.value;
          }
          return {
            relation,
            tag: node.tagName,
            textLength: (node.textContent || '').length,
            textStart: (node.textContent || '').trim().slice(0, 180),
            media,
            attrs,
          };
        };

        const parent = taskNode.parentElement;
        const children = parent ? Array.from(parent.children) : [];
        const taskIndex = children.indexOf(taskNode);
        const neighborWindow = children.slice(Math.max(0, taskIndex - 1), taskIndex + 4)
          .map((node, offset) => describe(node, `parent-child-${Math.max(0, taskIndex - 1) + offset}`));
        return {
          taskId,
          found: true,
          taskIndex,
          taskNode: describe(taskNode, 'task'),
          previous: describe(taskNode.previousElementSibling, 'previous'),
          next: describe(taskNode.nextElementSibling, 'next'),
          neighborWindow,
        };
      }), TASK_IDS);
    }

    if (/imini\.com\/zh\/asset/.test(url)) {
      item.assets = await page.evaluate(() => {
        const media = Array.from(document.querySelectorAll('img,video,source')).map((el, index) => ({
          index,
          tag: el.tagName,
          src: el.currentSrc || el.src || el.getAttribute('src') || '',
          poster: el.getAttribute('poster') || '',
          alt: el.getAttribute('alt') || '',
        })).filter((entry) => entry.src || entry.poster);
        const links = Array.from(document.querySelectorAll('a[href]')).map((el, index) => ({ index, href: el.href, text: (el.textContent || '').trim() })).filter((entry) => entry.href);
        return { media, links, bodyText: document.body.innerText.slice(0, 5000) };
      });
    }
    summary.push(item);
  }

  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  await browser.disconnect();
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});
