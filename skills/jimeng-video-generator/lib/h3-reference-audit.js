// Machine-local reference cache. Never persists API credentials.
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

function sha256(filePath) {
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function verifyImage(filePath) {
  const script = [
    'import json,sys', 'from PIL import Image',
    'Image.MAX_IMAGE_PIXELS=50000000',
    'im=Image.open(sys.argv[1]); fmt=im.format; size=im.size; im.verify()',
    'im=Image.open(sys.argv[1]); im.load()',
    'assert 0 < size[0]*size[1] <= 50000000',
    'print(json.dumps({"format":fmt,"width":size[0],"height":size[1]}))'
  ].join('\n');
  try {
    return JSON.parse(execFileSync(process.env.WSR_REVIEW_PYTHON || '/usr/bin/python3',
      ['-c', script, filePath], { encoding: 'utf8', timeout: 30000, maxBuffer: 65536, stdio: ['ignore', 'pipe', 'pipe'] }));
  } catch (_) {
    throw new Error('WSR_REFERENCE_INVALID_IMAGE:参考附件不能完整解码为图片，禁止提交');
  }
}

function atomicJson(filePath, value) {
  const temporary = `${filePath}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  fs.writeFileSync(temporary, JSON.stringify(value, null, 2), { mode: 0o600 });
  fs.renameSync(temporary, filePath);
}

async function materializeAuditedReferences({ attachments, assets, cacheRoot, tempDir, baseUrl,
  apiKey, download, upload, inspect = verifyImage, now = Date.now(), remoteCacheTtlMs = 0 }) {
  fs.mkdirSync(cacheRoot, { recursive: true, mode: 0o700 });
  const audited = [];
  const imageUrls = [];
  // Remote handle expiry is undocumented: by default only local bytes are reused.
  // Explicitly configured remote reuse remains account-scoped and time bounded.
  const route = crypto.createHash('sha256').update(`${baseUrl}\n${apiKey}`).digest('hex');
  const reuseTtl = Math.max(0, Number(remoteCacheTtlMs) || 0);
  for (let index = 0; index < attachments.length; index++) {
    const attachment = attachments[index];
    const asset = assets[index];
    const expected = asset.derived_sha256.toLowerCase();
    if (!/^[a-f0-9]{64}$/.test(expected) || asset.file_token !== attachment.fileToken) {
      throw new Error(`WSR_REFERENCE_ASSET_MISMATCH:图 ${index + 1} 的哈希或 token 不匹配`);
    }
    const objectPath = path.join(cacheRoot, `${expected}.image`);
    const cached = fs.existsSync(objectPath) && sha256(objectPath) === expected;
    const source = cached ? objectPath : path.join(tempDir, `reference-${index + 1}.input`);
    if (source !== objectPath) await download(attachment.fileToken, source);
    const size = fs.statSync(source).size;
    if (size <= 0 || size > 30 * 1024 * 1024) throw new Error(`图 ${index + 1} 大小不合法（需 1 byte–30 MB）`);
    const actual = sha256(source);
    if (actual !== expected) throw new Error(`WSR_REFERENCE_HASH_MISMATCH:图 ${index + 1} 与 manifest 派生图 SHA256 不一致`);
    const image = inspect(source);
    const extension = ({ JPEG: 'jpg', PNG: 'png', WEBP: 'webp', GIF: 'gif', HEIF: 'heif', HEIC: 'heic' })[image.format];
    if (!extension) throw new Error(`WSR_REFERENCE_INVALID_IMAGE:不支持图片格式 ${image.format}`);
    if (source !== objectPath) fs.copyFileSync(source, objectPath);
    // A real image extension preserves upload MIME type; bytes remain unchanged.
    const uploadPath = path.join(cacheRoot, `${actual}.${extension}`);
    if (!fs.existsSync(uploadPath) || sha256(uploadPath) !== actual) fs.copyFileSync(objectPath, uploadPath);
    const mappingPath = path.join(cacheRoot, `${actual}.${route}.upload.json`);
    let mapping = null;
    try { mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8')); } catch (_) { /* cold cache */ }
    let uploadRef = mapping?.upload_ref;
    let refreshed = false;
    if (!reuseTtl || !uploadRef || mapping.derived_sha256 !== actual || !Number.isFinite(Number(mapping.uploaded_at_ms)) ||
        now - Number(mapping.uploaded_at_ms) >= reuseTtl ||
        now < Number(mapping.uploaded_at_ms)) {
      uploadRef = await upload(uploadPath);
      refreshed = true;
    }
    if (!/^mm_file:\/\/[A-Za-z0-9_-]+$/.test(String(uploadRef)) && !/^https:\/\//.test(String(uploadRef))) {
      throw new Error(`WSR_REFERENCE_UPLOAD_INVALID:图 ${index + 1} 上传没有返回可用引用`);
    }
    if (refreshed) {
      atomicJson(mappingPath, { upload_ref: uploadRef, derived_sha256: actual, uploaded_at_ms: now });
    }
    imageUrls.push(uploadRef);
    audited.push({ ...asset, file_token: attachment.fileToken, path: uploadPath,
      original_sha256: asset.original_sha256.toLowerCase(), derived_sha256: actual,
      uploaded_sha256: actual, upload_ref: uploadRef, image, size });
  }
  return { imageUrls, referenceAssets: audited };
}

module.exports = { atomicJson, materializeAuditedReferences, sha256, verifyImage };
