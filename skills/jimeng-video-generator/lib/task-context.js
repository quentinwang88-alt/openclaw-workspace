const path = require('path');
const os = require('os');
const fs = require('fs');
const { EXEC } = require('child_process');
const { normalizeTextField, normalizeNumberField, normalizeBooleanField, getAttachmentList, sanitizeTaskName } = require('./field-normalizers');

const HEIC_EXTENSIONS = new Set(['.heic', '.heif']);

function sanitizeMachineId(value) {
  const cleaned = String(value || 'machine')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
  return cleaned || 'machine';
}

function formatBeijingTimestamp(date = new Date()) {
  const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
  return `${beijingDate.toISOString().slice(0, -1)}+08:00`;
}

function parseExecutionOwner(value) {
  const raw = normalizeTextField(value);
  if (!raw) {
    return { raw: '', machineId: '', claimToken: '' };
  }
  const markerIndex = raw.indexOf('#');
  if (markerIndex < 0) {
    return { raw, machineId: raw, claimToken: raw };
  }
  return {
    raw,
    machineId: raw.slice(0, markerIndex).trim(),
    claimToken: raw
  };
}

function executionOwnerMatchesMachine(value, machineId) {
  const parsed = parseExecutionOwner(value);
  return Boolean(parsed.machineId) && parsed.machineId === machineId;
}

function buildOwnershipClaimToken(config) {
  return String(config.machineId || '').trim();
}

function buildTaskContext(record, config) {
  const fields = record.fields || {};
  const repeatTarget = Math.max(1, normalizeNumberField(fields[config.fields.repeatCount], 1));
  const submittedCount = Math.max(0, normalizeNumberField(fields[config.fields.submittedCount], 0));
  const taskName = sanitizeTaskName(fields[config.fields.taskName], record.record_id);

  const executionOwner = normalizeTextField(fields[config.fields.executionOwner]);
  const executionOwnerParsed = parseExecutionOwner(executionOwner);

  const channel = normalizeTextField(fields[config.fields.channel || '渠道']);
  const channelSource = normalizeTextField(fields[config.fields.channelSource || '渠道来源']);

  return {
    recordId: record.record_id,
    record,
    taskName,
    prompt: normalizeTextField(fields[config.fields.prompt]),
    attachments: getAttachmentList(fields, config.fields.images || []),
    allowNoReferenceImage: normalizeBooleanField(fields[config.fields.allowNoReferenceImage]),
    model: normalizeModelName(fields[config.fields.model], config.defaultModel),
    mode: normalizeTextField(fields[config.fields.mode]) || config.defaultMode,
    ratio: normalizeTextField(fields[config.fields.ratio]) || config.defaultRatio,
    duration: Math.max(1, normalizeNumberField(fields[config.fields.duration], config.defaultDuration)),
    repeatTarget,
    submittedCount,
    remainingCount: Math.max(0, repeatTarget - submittedCount),
    currentStatus: normalizeTextField(fields[config.statusField]),
    executionOwner,
    executionOwnerMachineId: executionOwnerParsed.machineId,
    channel,
    channelSource,
    result: normalizeTextField(fields[config.fields.result]),
    blockedPath: normalizeTextField(fields[config.fields.blockedPath]),
    lastProcessedAt: normalizeTimestampField(fields[config.fields.lastProcessedAt])
  };
}

const MODEL_ALIASES = {
  'seedance 2.0 fast vip': 'Seedance 2.0 Fast VIP',
  'seedance 2.0 vip': 'Seedance 2.0 VIP',
  'seedance 2.0 fast': 'Seedance 2.0 Fast',
  'seedance 2.0': 'Seedance 2.0'
};

function normalizeModelName(value, fallback) {
  const normalized = normalizeTextField(value);
  if (!normalized) return fallback;
  const lower = normalized.toLowerCase();
  if (lower.includes('seedance 2.0 fast vip')) {
    return 'Seedance 2.0 Fast VIP';
  }
  if (lower.includes('seedance 2.0 vip')) {
    return 'Seedance 2.0 VIP';
  }
  if (lower.includes('seedance 2.0 fast')) {
    return 'Seedance 2.0 Fast';
  }
  if (lower.includes('seedance 2.0')) {
    return 'Seedance 2.0';
  }
  return MODEL_ALIASES[normalized.toLowerCase()] || normalized;
}

function normalizeTimestampField(value) {
  const text = normalizeTextField(value);
  if (!text) return null;
  const timestamp = Date.parse(text);
  if (Number.isNaN(timestamp)) return null;
  return new Date(timestamp);
}

module.exports = {
  sanitizeMachineId,
  formatBeijingTimestamp,
  parseExecutionOwner,
  executionOwnerMatchesMachine,
  buildOwnershipClaimToken,
  buildTaskContext,
  normalizeModelName,
  normalizeTimestampField
};