// Local mocks only: no H3, Feishu or model requests.
const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { materializeAuditedReferences, verifyImage } = require('../lib/h3-reference-audit');
const { prepareWsrReferenceExecution } = require('../lib/script-pool-reference-contract');

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'h3-reference-audit-test-'));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEElEQVR4nGP8zwACTGCSAQANHQEDgslx/wAAAABJRU5ErkJggg==', 'base64');
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const asset = (index, token, derived = hash(png)) => ({ index,
  role: index === 1 ? 'person_identity' : 'product', file_token: token,
  original_sha256: 'a'.repeat(64), derived_sha256: derived });
const assets = [asset(1, 'person'), asset(2, 'product')];
const ctx = { scriptId: 'wsr_test', prompt: '前两张是同一人物的脸部参考。后两张是目标假发产品图。目标头发为粉色。',
  personaContract: { schema_version: '2', reference_assets: assets },
  attachments: [{ fileToken: 'person' }, { fileToken: 'product' }] };

async function main() {
  const prepared = prepareWsrReferenceExecution(ctx, { channel: 'minimax_h3', mode: '多参考图' });
  assert(!prepared.prompt.includes('前两张'));
  assert(!prepared.prompt.includes('后两张'));
  assert(prepared.prompt.includes('第 1 张是同一人物'));
  assert(prepared.prompt.includes('第 2 张是目标假发'));
  assert(prepared.prompt.includes('目标头发为粉色'));
  assert(ctx.prompt.includes('前两张'), 'must preserve human prompt');
  assert.throws(() => prepareWsrReferenceExecution({ ...ctx, attachments: [...ctx.attachments].reverse() },
    { channel: 'minimax_h3', mode: '多参考图' }), /ASSET_MISMATCH/);
  assert.throws(() => prepareWsrReferenceExecution({ ...ctx, personaContract: { reference_assets: assets } },
    { channel: 'minimax_h3', mode: '多参考图' }), /UPGRADE_REQUIRED/);
  let downloads = 0; let uploads = 0;
  const options = { attachments: ctx.attachments, assets, cacheRoot: path.join(root, 'cache'), tempDir: root,
    baseUrl: 'https://example.invalid', apiKey: 'mock-only', now: 1000,
    download: async (_token, target) => { downloads++; fs.writeFileSync(target, png); },
    upload: async () => { uploads++; return 'mm_file://123'; },
    inspect: () => ({ format: 'PNG', width: 1, height: 1 }) };
  const result = await materializeAuditedReferences(options);
  assert.strictEqual(downloads, 1, 'identical hashes reuse the verified local cache');
  assert.strictEqual(uploads, 2, 'remote handle reuse is disabled by default');
  assert.strictEqual(result.referenceAssets.length, 2);
  assert.deepStrictEqual(result.referenceAssets.map(a => a.file_token), ['person', 'product']);
  assert.strictEqual(result.referenceAssets[0].original_sha256, 'a'.repeat(64));
  assert.strictEqual(result.referenceAssets[0].derived_sha256, hash(png));
  assert.strictEqual(result.referenceAssets[0].uploaded_sha256, hash(png));
  await materializeAuditedReferences({ ...options, now: 2000 });
  assert.strictEqual(downloads, 1); assert.strictEqual(uploads, 4, 'new video uploads again without extra Feishu downloads');
  await materializeAuditedReferences({ ...options, now: 3000, remoteCacheTtlMs: 6000 });
  assert.strictEqual(uploads, 4, 'explicit remote TTL permits reuse');
  await materializeAuditedReferences({ ...options, now: 9000, remoteCacheTtlMs: 6000 });
  assert.strictEqual(uploads, 5, 'expired upload handles are refreshed before submission');
  await assert.rejects(() => materializeAuditedReferences({ ...options, assets: [asset(1, 'person', 'b'.repeat(64))],
    attachments: [ctx.attachments[0]] }), /HASH_MISMATCH/);
  const wrong = path.join(root, 'fake.jpg'); fs.writeFileSync(wrong, '<html>not image</html>');
  assert.throws(() => verifyImage(wrong), /INVALID_IMAGE/);
  fs.writeFileSync(wrong, png);
  assert.deepStrictEqual(verifyImage(wrong), { format: 'PNG', width: 2, height: 2 });
  const cacheJson = fs.readdirSync(options.cacheRoot).filter(name => name.endsWith('.json'))
    .map(name => fs.readFileSync(path.join(options.cacheRoot, name), 'utf8')).join('');
  assert(!cacheJson.includes('mock-only'), 'no API credential in audit/cache');
  console.log('H3 reference v2 binding, cache, audit and image rejection tests passed');
}
main().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => fs.rmSync(root, { recursive: true, force: true }));
