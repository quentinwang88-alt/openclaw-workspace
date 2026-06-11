const https = require('https');
const fs = require('fs');
const path = require('path');

let cachedAccessToken = null;
let tokenExpireTime = 0;

function loadOpenclawFeishuCredentials() {
  try {
    const openclawConfigPath = path.join(process.env.HOME || '', '.openclaw', 'openclaw.json');
    if (!fs.existsSync(openclawConfigPath)) {
      return {};
    }
    const raw = JSON.parse(fs.readFileSync(openclawConfigPath, 'utf8'));
    return raw.channels?.feishu || {};
  } catch (error) {
    return {};
  }
}

function getFeishuRequestTimeoutMs(config) {
  return Math.max(5000, Number(activeConfigForNetwork?.feishuRequestTimeoutMs || config?.feishuRequestTimeoutMs) || 45000);
}

function getFeishuDownloadTimeoutMs(config) {
  return Math.max(10000, Number(activeConfigForNetwork?.feishuDownloadTimeoutMs || config?.feishuDownloadTimeoutMs) || 120000);
}

let activeConfigForNetwork = null;

function requestJson(method, requestPath, token, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json'
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    if (method === 'GET') {
      headers['Cache-Control'] = 'no-cache, no-store, max-age=0';
      headers.Pragma = 'no-cache';
    }
    if (payload) {
      headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const timeoutMs = getFeishuRequestTimeoutMs();
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: requestPath,
      method,
      headers
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.code === 0) {
            resolve(json.data);
            return;
          }
          if (json.code === 91403) {
            reject(new Error('飞书权限不足（91403 Forbidden）'));
            return;
          }
          reject(new Error(`飞书 API 失败 (${json.code}): ${json.msg || '未知错误'}`));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书请求超时 (${timeoutMs}ms): ${method} ${requestPath}`));
    });
    req.on('error', reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function requestRawJson(method, requestPath, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {
      'Content-Type': 'application/json'
    };

    if (payload) {
      headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const timeoutMs = getFeishuRequestTimeoutMs();
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: requestPath,
      method,
      headers
    }, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(new Error(`解析飞书响应失败: ${error.message}`));
        }
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书请求超时 (${timeoutMs}ms): ${method} ${requestPath}`));
    });
    req.on('error', reject);
    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

async function getAccessToken(config) {
  if (cachedAccessToken && Date.now() < tokenExpireTime) {
    return cachedAccessToken;
  }

  const data = await requestRawJson(
    'POST',
    '/open-apis/auth/v3/tenant_access_token/internal',
    {
      app_id: config.appId,
      app_secret: config.appSecret
    }
  );

  if (data.code !== 0 || !data.tenant_access_token) {
    throw new Error(`获取飞书 Token 失败: ${data.msg || '未知错误'}`);
  }

  cachedAccessToken = data.tenant_access_token;
  tokenExpireTime = Date.now() + ((data.expire || 7200) - 60) * 1000;
  return cachedAccessToken;
}

async function listTableFields(config, token) {
  const data = await requestJson(
    'GET',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields?page_size=200&_ts=${Date.now()}`,
    token
  );
  return data.items || [];
}

async function createField(config, token, fieldSpec) {
  return requestJson(
    'POST',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields`,
    token,
    fieldSpec
  );
}

async function updateField(config, token, fieldId, fieldSpec) {
  return requestJson(
    'PUT',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/fields/${fieldId}`,
    token,
    fieldSpec
  );
}

async function updateRecord(config, token, recordId, fields) {
  return requestJson(
    'PUT',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}`,
    token,
    { fields }
  );
}

async function getRecord(config, token, recordId) {
  const data = await requestJson(
    'GET',
    `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records/${recordId}?_ts=${Date.now()}`,
    token
  );
  return data.record || null;
}

async function listAllRecords(config, token) {
  let pageToken = null;
  let records = [];

  do {
    let requestPath = `/open-apis/bitable/v1/apps/${config.appToken}/tables/${config.tableId}/records?page_size=${config.pageSize || 100}&_ts=${Date.now()}`;
    if (pageToken) {
      requestPath += `&page_token=${encodeURIComponent(pageToken)}`;
    }
    if (config.viewId) {
      requestPath += `&view_id=${encodeURIComponent(config.viewId)}`;
    }

    const data = await requestJson('GET', requestPath, token);
    records = records.concat(data.items || []);
    pageToken = data.page_token || null;
  } while (pageToken);

  return records;
}

function detectMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.mp4') return 'video/mp4';
  if (ext === '.mov') return 'video/quicktime';
  if (ext === '.m4v') return 'video/x-m4v';
  if (ext === '.avi') return 'video/x-msvideo';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}

async function uploadFileToFeishu(config, token, filePath) {
  const fileBuffer = fs.readFileSync(filePath);
  const filename = path.basename(filePath);
  const fileSize = fileBuffer.length;
  const mimeType = detectMimeType(filePath);

  const parentTypes = Array.isArray(config.uploadParentTypes) && config.uploadParentTypes.length > 0
    ? config.uploadParentTypes
    : ['bitable_file', 'bitable_image'];

  let lastError = null;

  for (const parentType of parentTypes) {
    try {
      const boundary = '----OpenClawJimeng' + Math.random().toString(16).slice(2);
      const CRLF = '\r\n';
      const parts = [];

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="file_name"' + CRLF + CRLF);
      parts.push(filename + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="parent_type"' + CRLF + CRLF);
      parts.push(parentType + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="parent_node"' + CRLF + CRLF);
      parts.push(config.appToken + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push('Content-Disposition: form-data; name="size"' + CRLF + CRLF);
      parts.push(String(fileSize) + CRLF);

      parts.push('--' + boundary + CRLF);
      parts.push(`Content-Disposition: form-data; name="file"; filename="${filename}"` + CRLF);
      parts.push(`Content-Type: ${mimeType}` + CRLF + CRLF);

      const headerBuffer = Buffer.from(parts.join(''));
      const footerBuffer = Buffer.from(CRLF + '--' + boundary + '--' + CRLF);
      const payload = Buffer.concat([headerBuffer, fileBuffer, footerBuffer]);

      const data = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: 'open.feishu.cn',
          path: '/open-apis/drive/v1/medias/upload_all',
          method: 'POST',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': `multipart/form-data; boundary=${boundary}`
          }
        }, res => {
          let raw = '';
          res.setEncoding('utf8');
          res.on('data', chunk => raw += chunk);
          res.on('end', () => {
            try {
              const json = JSON.parse(raw);
              if (json.code === 0) {
                resolve({
                  fileToken: json.data.file_token,
                  parentType
                });
                return;
              }
              reject(new Error(`上传失败 (${parentType}): ${json.msg || '未知错误'}`));
            } catch (error) {
              reject(new Error(`解析上传响应失败 (${parentType}): ${error.message}`));
            }
          });
        });

        req.on('error', reject);
        req.write(payload);
        req.end();
      });

      return data;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error('上传文件到飞书失败');
}

async function downloadFile(token, fileToken, outputPath) {
  const timeoutMs = getFeishuDownloadTimeoutMs();
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outputPath);
    const req = https.request({
      hostname: 'open.feishu.cn',
      path: `/open-apis/drive/v1/medias/${fileToken}/download`,
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`
      }
    }, res => {
      if (res.statusCode !== 200) {
        let errorData = '';
        res.setEncoding('utf8');
        res.on('data', chunk => errorData += chunk);
        res.on('end', () => {
          fs.unlink(outputPath, () => {
            reject(new Error(`下载失败 (${res.statusCode}): ${errorData || '空响应'}`));
          });
        });
        return;
      }

      res.pipe(file);
      file.on('finish', () => {
        file.close(() => resolve(outputPath));
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`飞书文件下载超时 (${timeoutMs}ms): ${fileToken}`));
    });
    req.on('error', error => {
      file.close(() => {
        fs.unlink(outputPath, () => reject(error));
      });
    });
    req.end();
  });
}

function setActiveConfigForNetwork(config) {
  activeConfigForNetwork = config;
}

module.exports = {
  loadOpenclawFeishuCredentials,
  getFeishuRequestTimeoutMs,
  getFeishuDownloadTimeoutMs,
  requestJson,
  requestRawJson,
  getAccessToken,
  listTableFields,
  createField,
  updateField,
  updateRecord,
  getRecord,
  listAllRecords,
  detectMimeType,
  uploadFileToFeishu,
  downloadFile,
  setActiveConfigForNetwork
};
