const { requestJson } = require('./feishu-client');

const FIELD_TYPE = {
  TEXT: 1,
  NUMBER: 2,
  SINGLE_SELECT: 3,
  ATTACHMENT: 17
};

function normalizeTextField(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number') return String(value);
  if (Array.isArray(value)) {
    return value
      .map(item => {
        if (typeof item === 'string') return item;
        if (item == null) return '';
        if (typeof item.text === 'string') return item.text;
        if (typeof item.name === 'string') return item.name;
        return '';
      })
      .filter(Boolean)
      .join('\n')
      .trim();
  }
  return String(value).trim();
}

function normalizeNumberField(value, fallback = 0) {
  if (typeof value === 'number') return value;
  const text = normalizeTextField(value);
  const match = text.match(/-?\d+(\.\d+)?/);
  return match ? Number(match[0]) : fallback;
}

function normalizeBooleanField(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (Array.isArray(value)) {
    return normalizeBooleanField(
      value
        .map(item => {
          if (item == null) return '';
          if (typeof item === 'string') return item;
          if (item.name === 'string') return item.name;
          if (typeof item.text === 'string') return item.text;
          return '';
        })
        .filter(Boolean)
        .join(' ')
    );
  }

  const normalized = normalizeTextField(value).replace(/\s+/g, '').toLowerCase();
  if (!normalized) return false;

  const truthyValues = new Set([
    'true', '1', 'yes', 'y', 'on', '是', '开', '开启', '允许', '已开启', '需要', '免参考图'
  ]);
  const falsyValues = new Set([
    'false', '0', 'no', 'n', 'off', '否', '关', '关闭', '不允许', '未开启'
  ]);

  if (truthyValues.has(normalized)) return true;
  if (falsyValues.has(normalized)) return false;
  return false;
}

function sanitizeFileName(fileName, fallback = 'image.jpg') {
  const base = path.basename(fileName || fallback);
  const cleaned = base.replace(/[\\/:*?"<>|]/g, '_').trim();
  return cleaned || fallback;
}

function buildUniqueAttachmentName(fileName, index, fallback = 'image.jpg') {
  const safeName = sanitizeFileName(fileName, fallback);
  const ext = path.extname(safeName);
  const stem = ext ? safeName.slice(0, -ext.length) : safeName;
  const suffix = String(index + 1).padStart(2, '0');
  return `${suffix}-${stem || 'image'}${ext || '.jpg'}`;
}

function getAttachmentList(recordFields, imageFieldNames) {
  const attachments = [];
  for (const fieldName of imageFieldNames) {
    const value = recordFields[fieldName];
    if (!Array.isArray(value)) continue;
    for (const item of value) {
      if (item && item.file_token) {
        attachments.push({
          fieldName,
          fileToken: item.file_token,
          fileName: item.name || `${fieldName}.bin`
        });
      }
    }
  }
  return attachments;
}

function sanitizeTaskName(value, fallback) {
  const cleaned = String(value || fallback || 'task')
    .replace(/[\\/:*?"<>|]/g, '_')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || fallback || 'task';
}

const path = require('path');

module.exports = {
  FIELD_TYPE,
  normalizeTextField,
  normalizeNumberField,
  normalizeBooleanField,
  sanitizeFileName,
  buildUniqueAttachmentName,
  getAttachmentList,
  sanitizeTaskName
};