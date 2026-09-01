const dns = require('dns');
const fs = require('fs');
const http = require('http');
const https = require('https');
const net = require('net');
const path = require('path');

const DEFAULT_TIMEOUT_MS = 90_000;
const DEFAULT_DOWNLOAD_LIMIT = 500 * 1024 * 1024;

class SubmissionUncertainError extends Error {
  constructor(message, cause = null) {
    super(message);
    this.name = 'SubmissionUncertainError';
    this.code = 'SUBMIT_UNCERTAIN';
    this.cause = cause;
  }
}

class ApiError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = 'ApiError';
    this.statusCode = options.statusCode || 0;
    this.responseBody = options.responseBody || '';
  }
}

function assertApiResult(result, label) {
  const baseResp = result?.base_resp;
  if (baseResp && Number(baseResp.status_code || 0) !== 0) {
    throw new ApiError(`${label}失败 (${baseResp.status_code}): ${baseResp.status_msg || '未知错误'}`);
  }
  if (result?.errCode != null && Number(result.errCode) !== 0) {
    throw new ApiError(`${label}失败 (${result.errCode}): ${result.errMsg || '未知错误'}`);
  }
  return result;
}

function requestJson({ method, url, token, body = null, timeoutMs = DEFAULT_TIMEOUT_MS, uncertainOnFailure = false }) {
  return new Promise((resolve, reject) => {
    const target = new URL(url);
    if (target.protocol !== 'https:') {
      reject(new ApiError('MiniMax H3 API 只允许 HTTPS'));
      return;
    }

    const payload = body == null ? null : Buffer.from(JSON.stringify(body));
    const headers = {
      Accept: 'application/json',
      Authorization: `Bearer ${token}`,
      'User-Agent': 'openclaw-metaso-h3/1.0'
    };
    if (payload) {
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = payload.length;
    }

    let settled = false;
    const fail = error => {
      if (settled) return;
      settled = true;
      if (uncertainOnFailure && (!error.statusCode || error.statusCode >= 500)) {
        reject(new SubmissionUncertainError('创建请求结果不确定；为避免重复计费，任务已转人工核对', error));
        return;
      }
      reject(error);
    };

    const req = https.request(target, { method, headers }, res => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', chunk => {
        raw += chunk;
        if (raw.length > 2 * 1024 * 1024) {
          req.destroy(new ApiError('MiniMax H3 JSON 响应超过安全上限'));
        }
      });
      res.on('end', () => {
        if (settled) return;
        let result;
        try {
          result = JSON.parse(raw);
        } catch (error) {
          fail(new ApiError(`MiniMax H3 返回了非 JSON 响应 (HTTP ${res.statusCode || 0})`, {
            statusCode: res.statusCode || 0
          }));
          return;
        }
        if ((res.statusCode || 0) < 200 || (res.statusCode || 0) >= 300) {
          fail(new ApiError(`MiniMax H3 HTTP ${res.statusCode}: ${result?.message || result?.error?.message || '请求失败'}`, {
            statusCode: res.statusCode || 0,
            responseBody: raw.slice(0, 2000)
          }));
          return;
        }
        try {
          assertApiResult(result, 'MiniMax H3 请求');
          settled = true;
          resolve(result);
        } catch (error) {
          fail(error);
        }
      });
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`请求超时 (${timeoutMs}ms)`)));
    req.on('error', error => fail(error instanceof ApiError ? error : new ApiError(error.message)));
    if (payload) req.write(payload);
    req.end();
  });
}

function detectMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const types = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp',
    '.heic': 'image/heic', '.heif': 'image/heif', '.mp4': 'video/mp4', '.mov': 'video/quicktime',
    '.wav': 'audio/wav', '.mp3': 'audio/mpeg'
  };
  return types[ext] || 'application/octet-stream';
}

function uploadFile({ baseUrl, token, filePath, purpose = 'video_generation_input', timeoutMs = 600_000 }) {
  return new Promise((resolve, reject) => {
    const target = new URL(`${String(baseUrl).replace(/\/$/, '')}/v1/files/upload`);
    if (target.protocol !== 'https:') {
      reject(new ApiError('MiniMax H3 文件上传只允许 HTTPS'));
      return;
    }
    const stat = fs.statSync(filePath);
    const filename = path.basename(filePath).replace(/[\r\n"]/g, '_');
    const boundary = `----OpenClawMiniMaxH3${Date.now().toString(16)}`;
    const prefix = Buffer.from(
      `--${boundary}\r\n` +
      'Content-Disposition: form-data; name="purpose"\r\n\r\n' +
      `${purpose}\r\n` +
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="file"; filename="${filename}"\r\n` +
      `Content-Type: ${detectMimeType(filePath)}\r\n\r\n`
    );
    const suffix = Buffer.from(`\r\n--${boundary}--\r\n`);
    const req = https.request(target, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token}`,
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': prefix.length + stat.size + suffix.length,
        'User-Agent': 'openclaw-metaso-h3/1.0'
      }
    }, res => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', chunk => raw += chunk);
      res.on('end', () => {
        let result;
        try {
          result = JSON.parse(raw);
        } catch (error) {
          reject(new ApiError(`MiniMax H3 文件上传返回非 JSON (HTTP ${res.statusCode || 0})`));
          return;
        }
        if ((res.statusCode || 0) < 200 || (res.statusCode || 0) >= 300) {
          reject(new ApiError(`MiniMax H3 文件上传 HTTP ${res.statusCode}: ${result?.message || '失败'}`, {
            statusCode: res.statusCode || 0
          }));
          return;
        }
        try {
          assertApiResult(result, 'MiniMax H3 文件上传');
          const fileInfo = result.file || {};
          if (fileInfo.file_id != null) {
            resolve(`mm_file://${fileInfo.file_id}`);
            return;
          }
          if (fileInfo.download_url) {
            resolve(fileInfo.download_url);
            return;
          }
          reject(new ApiError('MiniMax H3 文件上传成功，但未返回 file_id'));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`素材上传超时 (${timeoutMs}ms)`)));
    req.on('error', reject);
    req.write(prefix);
    const input = fs.createReadStream(filePath);
    input.on('error', error => req.destroy(error));
    input.on('end', () => req.end(suffix));
    input.pipe(req, { end: false });
  });
}

async function createTask({ baseUrl, token, payload, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  const result = await requestJson({
    method: 'POST',
    url: `${String(baseUrl).replace(/\/$/, '')}/v2/video_generation`,
    token,
    body: payload,
    timeoutMs,
    uncertainOnFailure: true
  });
  if (!result.task_id) throw new ApiError('MiniMax H3 创建响应缺少 task_id');
  return String(result.task_id);
}

async function queryTask({ baseUrl, token, taskId, timeoutMs = 60_000 }) {
  return requestJson({
    method: 'GET',
    url: `${String(baseUrl).replace(/\/$/, '')}/v2/query/video_generation/${encodeURIComponent(taskId)}`,
    token,
    timeoutMs
  });
}

function isPrivateIp(address) {
  const value = String(address || '').toLowerCase().split('%')[0];
  if (net.isIP(value) === 4) {
    const octets = value.split('.').map(Number);
    return octets[0] === 0 || octets[0] === 10 || octets[0] === 127 ||
      (octets[0] === 100 && octets[1] >= 64 && octets[1] <= 127) ||
      (octets[0] === 169 && octets[1] === 254) ||
      (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
      (octets[0] === 192 && octets[1] === 168) ||
      (octets[0] === 198 && (octets[1] === 18 || octets[1] === 19)) || octets[0] >= 224;
  }
  if (net.isIP(value) === 6) {
    return value === '::' || value === '::1' || value.startsWith('fc') || value.startsWith('fd') ||
      /^fe[89ab]/.test(value) || value.startsWith('ff') || value.startsWith('::ffff:127.') ||
      value.startsWith('::ffff:10.') || value.startsWith('::ffff:192.168.');
  }
  return true;
}

async function resolvePublicAddress(hostname) {
  const addresses = await dns.promises.lookup(hostname, { all: true, verbatim: true });
  if (!addresses.length || addresses.some(item => isPrivateIp(item.address))) {
    throw new ApiError('拒绝下载指向本地或私网的地址');
  }
  return addresses[0];
}

function createPinnedLookup(resolved) {
  return (_hostname, options, callback) => {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (options?.all) {
      callback(null, [{ address: resolved.address, family: resolved.family }]);
      return;
    }
    callback(null, resolved.address, resolved.family);
  };
}

async function downloadPublicVideo(url, outputPath, options = {}, redirectCount = 0) {
  const maxBytes = Number(options.maxBytes || DEFAULT_DOWNLOAD_LIMIT);
  const timeoutMs = Number(options.timeoutMs || 180_000);
  const target = new URL(url);
  if (target.protocol !== 'https:' || target.username || target.password) {
    throw new ApiError('成片下载地址必须是无凭据的公网 HTTPS URL');
  }
  if (redirectCount > 5) throw new ApiError('成片下载重定向次数过多');

  const resolved = await resolvePublicAddress(target.hostname);
  await fs.promises.mkdir(path.dirname(outputPath), { recursive: true });

  return new Promise((resolve, reject) => {
    const req = https.request({
      protocol: 'https:',
      hostname: target.hostname,
      port: target.port || 443,
      path: `${target.pathname}${target.search}`,
      method: 'GET',
      servername: target.hostname,
      headers: { 'User-Agent': 'openclaw-metaso-h3/1.0', Accept: 'video/*,application/octet-stream' },
      lookup: createPinnedLookup(resolved)
    }, res => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode || 0)) {
        const location = res.headers.location;
        res.resume();
        if (!location) {
          reject(new ApiError('成片下载重定向缺少 Location'));
          return;
        }
        downloadPublicVideo(new URL(location, target).toString(), outputPath, options, redirectCount + 1)
          .then(resolve, reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        reject(new ApiError(`成片下载 HTTP ${res.statusCode || 0}`));
        return;
      }
      const declared = Number(res.headers['content-length'] || 0);
      if (declared > maxBytes) {
        res.destroy();
        reject(new ApiError(`成片超过下载上限 ${maxBytes} bytes`));
        return;
      }
      const contentType = String(res.headers['content-type'] || '').toLowerCase();
      if (contentType && !contentType.startsWith('video/') && !contentType.includes('octet-stream')) {
        res.destroy();
        reject(new ApiError(`成片 Content-Type 异常: ${contentType}`));
        return;
      }
      const partial = `${outputPath}.part`;
      const out = fs.createWriteStream(partial);
      let received = 0;
      res.on('data', chunk => {
        received += chunk.length;
        if (received > maxBytes) res.destroy(new ApiError(`成片超过下载上限 ${maxBytes} bytes`));
      });
      res.pipe(out);
      out.on('finish', () => out.close(async () => {
        try {
          await fs.promises.rename(partial, outputPath);
          resolve(outputPath);
        } catch (error) {
          reject(error);
        }
      }));
      const cleanup = error => {
        out.destroy();
        fs.unlink(partial, () => reject(error));
      };
      res.on('error', cleanup);
      out.on('error', cleanup);
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`成片下载超时 (${timeoutMs}ms)`)));
    req.on('error', error => {
      fs.unlink(`${outputPath}.part`, () => reject(error));
    });
    req.end();
  });
}

module.exports = {
  ApiError,
  SubmissionUncertainError,
  createTask,
  createPinnedLookup,
  detectMimeType,
  downloadPublicVideo,
  isPrivateIp,
  queryTask,
  requestJson,
  uploadFile
};
