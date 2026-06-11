const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { getAccessToken } = require('./feishu-client');
const { formatBeijingTimestamp } = require('./task-context');
const { FIELD_TYPE, normalizeTextField, normalizeNumberField, normalizeBooleanField, getAttachmentList, buildUniqueAttachmentName } = require('./field-normalizers');

const HEIC_EXTENSIONS = new Set(['.heic', '.heif']);

const STATUS = {
  PENDING: '待处理',
  PROCESSING: '处理中',
  PARTIAL: '部分提交',
  SUBMITTED: '已提交',
  BLOCKED: '阻塞',
  FAILED: '失败'
};

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function convertHeicToUploadJpeg(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  if (!HEIC_EXTENSIONS.has(ext)) {
    return imagePath;
  }

  const sourceStat = fs.statSync(imagePath);
  const cacheDir = path.join(path.dirname(imagePath), '.upload-cache');
  ensureDir(cacheDir);

  const stem = path.basename(imagePath, ext);
  const outputPath = path.join(cacheDir, `${stem}.upload.jpg`);

  if (fs.existsSync(outputPath)) {
    const outputStat = fs.statSync(outputPath);
    if (outputStat.size > 0 && outputStat.mtimeMs >= sourceStat.mtimeMs) {
      return outputPath;
    }
  }

  execFileSync('/usr/bin/sips', [
    '-s', 'format', 'jpeg',
    '-s', 'formatOptions', '95',
    imagePath,
    '--out', outputPath
  ], {
    stdio: 'pipe'
  });

  return outputPath;
}

function prepareImagesForUpload(imagePaths = []) {
  return imagePaths.map(imagePath => {
    if (!imagePath || !fs.existsSync(imagePath)) {
      return { sourcePath: imagePath, uploadPath: imagePath, converted: false };
    }

    const ext = path.extname(imagePath).toLowerCase();
    if (!HEIC_EXTENSIONS.has(ext)) {
      return { sourcePath: imagePath, uploadPath: imagePath, converted: false };
    }

    const uploadPath = convertHeicToUploadJpeg(imagePath);
    return {
      sourcePath: imagePath,
      uploadPath,
      converted: uploadPath !== imagePath
    };
  });
}

async function materializeTask(token, context, config) {
  const taskRoot = path.join(config.runtimeRoot, context.taskName);
  const imageDir = path.join(taskRoot, '图片');
  fs.rmSync(taskRoot, { recursive: true, force: true });
  ensureDir(imageDir);

  const imagePaths = [];
  for (let i = 0; i < context.attachments.length; i++) {
    const attachment = context.attachments[i];
    const outputPath = path.join(
      imageDir,
      buildUniqueAttachmentName(attachment.fileName, i, `image-${i + 1}.jpg`)
    );
    await downloadFile(token, attachment.fileToken, outputPath);
    imagePaths.push(outputPath);
    await new Promise(resolve => setTimeout(resolve, 150));
  }

  const preparedImages = prepareImagesForUpload(imagePaths);
  const uploadImages = preparedImages.map(item => item.uploadPath);
  const convertedImages = preparedImages.filter(item => item.converted);

  if (convertedImages.length > 0) {
    console.log(`  ♻️ ${context.taskName}: 已将 ${convertedImages.length} 张 HEIC 图片转换为 JPG 上传副本`);
  }

  fs.writeFileSync(path.join(taskRoot, 'prompt.txt'), `${context.prompt}\n`);
  fs.writeFileSync(path.join(taskRoot, 'config.json'), `${JSON.stringify({
    model: context.model,
    mode: context.mode,
    ratio: context.ratio,
    duration: context.duration
  }, null, 2)}\n`);

  return {
    name: context.taskName,
    folder: taskRoot,
    images: uploadImages,
    prompt: context.prompt,
    config: {}
  };
}

async function cleanupTaskDir(task) {
  if (!task || !task.folder) return;
  try {
    fs.rmSync(task.folder, { recursive: true, force: true });
  } catch (error) {
    console.log(`  ⚠️ 清理临时目录失败: ${error.message}`);
  }
}

module.exports = {
  STATUS,
  ensureDir,
  convertHeicToUploadJpeg,
  prepareImagesForUpload,
  materializeTask,
  cleanupTaskDir
};