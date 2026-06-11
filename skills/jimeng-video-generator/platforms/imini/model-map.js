const IMINI_MODEL_MAP = {
  'seedance 2.0': 'Seedance 2.0',
  'seedance 2.0 fast': 'Seedance 2.0 Fast',
  'seedance 2.0 fast vip': 'Seedance 2.0 Fast VIP',
  'seedance 2.0 vip': 'Seedance 2.0 VIP'
};

function mapModelForJimeng(model) {
  const normalized = String(model || '').trim().toLowerCase();
  if (IMINI_MODEL_MAP[normalized]) {
    return IMINI_MODEL_MAP[normalized];
  }
  return null;
}

function mapModelForImini(model) {
  const normalized = String(model || '').trim().toLowerCase();
  if (normalized.includes('seedance 2.0 fast')) {
    if (normalized.includes('vip')) return 'Seedance 2.0 Fast';
    return 'Seedance 2.0 Fast';
  }
  if (normalized.includes('seedance 2.0')) {
    return 'Seedance 2.0';
  }
  return null;
}

function isModelSupportedOnImini(model) {
  return mapModelForImini(model) !== null;
}

const IMINI_DURATION_MAP = {
  '4': 4,
  '5': 5,
  '6': 6,
  '8': 8,
  '10': 10,
  '15': 15
};

function mapDurationForImini(duration) {
  const num = Math.max(1, Number(duration) || 4);
  const supported = [4, 5, 6, 8, 10, 15];
  if (supported.includes(num)) return num;
  let closest = supported[0];
  for (const s of supported) {
    if (Math.abs(s - num) < Math.abs(closest - num)) {
      closest = s;
    }
  }
  return closest;
}

const IMINI_RATIO_MAP = {
  '9:16': '9:16',
  '16:9': '16:9',
  '1:1': '1:1'
};

function mapRatioForImini(ratio) {
  const normalized = String(ratio || '9:16').trim();
  if (IMINI_RATIO_MAP[normalized]) return normalized;
  return '9:16';
}

const IMINI_RESOLUTION_MAP = {
  '480p': '480P',
  '480': '480P',
  '720p': '720P',
  '720': '720P',
  '1080p': '1080P',
  '1080': '1080P'
};

function mapResolutionForImini(resolution) {
  const normalized = String(resolution || '480P').trim().toLowerCase();
  if (IMINI_RESOLUTION_MAP[normalized]) return IMINI_RESOLUTION_MAP[normalized];
  return '480P';
}

module.exports = {
  mapModelForJimeng,
  mapModelForImini,
  isModelSupportedOnImini,
  mapDurationForImini,
  mapRatioForImini,
  mapResolutionForImini,
  IMINI_MODEL_MAP
};
