const fs = require('fs');
const https = require('https');
const path = require('path');

const { requestJson, uploadFileToFeishu } = require('../../lib/feishu-client');

const SIMPLE_UPLOAD_LIMIT = 20 * 1024 * 1024;

function uploadPart(token, uploadId, seq, chunk, timeoutMs = 120_000) {
  return new Promise((resolve, reject) => {
    const boundary = `----OpenClawFeishuChunk${Date.now().toString(16)}${seq}`;
    const prefix = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="upload_id"\r\n\r\n${uploadId}\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="seq"\r\n\r\n${seq}\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="size"\r\n\r\n${chunk.length}\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="part"\r\n` +
      'Content-Type: application/octet-stream\r\n\r\n'
    );
    const suffix = Buffer.from(`\r\n--${boundary}--\r\n`);
    const payload = Buffer.concat([prefix, chunk, suffix]);
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: '/open-apis/drive/v1/files/upload_part',
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': payload.length
      }
    }, res => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', data => raw += data);
      res.on('end', () => {
        let result;
        try {
          result = JSON.parse(raw);
        } catch (error) {
          reject(new Error(`飞书分片 ${seq} 返回非 JSON`));
          return;
        }
        if ((res.statusCode || 0) !== 200 || result.code !== 0) {
          reject(new Error(`飞书分片 ${seq} 上传失败 (${result.code || res.statusCode}): ${result.msg || '未知错误'}`));
          return;
        }
        resolve();
      });
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`飞书分片 ${seq} 上传超时`)));
    req.on('error', reject);
    req.end(payload);
  });
}

async function uploadLargeBitableFile(config, token, filePath) {
  const stat = fs.statSync(filePath);
  const prepared = await requestJson(
    'POST',
    '/open-apis/drive/v1/files/upload_prepare',
    token,
    {
      file_name: path.basename(filePath),
      parent_type: 'bitable_file',
      parent_node: config.appToken,
      size: stat.size
    }
  );
  const uploadId = prepared.upload_id;
  const blockSize = Number(prepared.block_size || 4 * 1024 * 1024);
  const blockNum = Number(prepared.block_num || Math.ceil(stat.size / blockSize));
  if (!uploadId || !blockSize || !blockNum) throw new Error('飞书分片预上传响应不完整');

  const handle = await fs.promises.open(filePath, 'r');
  try {
    for (let seq = 0; seq < blockNum; seq++) {
      const offset = seq * blockSize;
      const expected = Math.min(blockSize, stat.size - offset);
      const chunk = Buffer.allocUnsafe(expected);
      const { bytesRead } = await handle.read(chunk, 0, expected, offset);
      if (bytesRead !== expected) throw new Error(`读取飞书上传分片 ${seq} 不完整`);
      await uploadPart(token, uploadId, seq, chunk);
    }
  } finally {
    await handle.close();
  }

  const finished = await requestJson(
    'POST',
    '/open-apis/drive/v1/files/upload_finish',
    token,
    { upload_id: uploadId, block_num: blockNum }
  );
  if (!finished.file_token) throw new Error('飞书分片上传完成但没有 file_token');
  return { fileToken: finished.file_token, parentType: 'bitable_file' };
}

async function uploadBitableFile(config, token, filePath) {
  const stat = fs.statSync(filePath);
  if (stat.size <= SIMPLE_UPLOAD_LIMIT) {
    return uploadFileToFeishu(config, token, filePath);
  }
  return uploadLargeBitableFile(config, token, filePath);
}

module.exports = {
  SIMPLE_UPLOAD_LIMIT,
  uploadBitableFile,
  uploadLargeBitableFile,
  uploadPart
};
