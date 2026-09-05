// Explicit test-only QA helpers; production workers must not call this module.
// Never changes generation/publication state.
const path = require('path');
const { execFile } = require('child_process');

const REVIEW_LABELS = { pass: '通过', fail: '有偏差', unknown: '检查异常' };

function unknown(summary) {
  return { status: 'unknown', summary, evidence: [] };
}

function runReviewProcess(job) {
  return new Promise(resolve => {
    const script = path.resolve(__dirname, '..', 'scripts', 'run_wsr_content_review.py');
    const child = execFile(process.env.WSR_REVIEW_PYTHON || '/usr/bin/python3',
      [script, '--apply-model'], { timeout: 180000, killSignal: 'SIGKILL', maxBuffer: 1024 * 1024 },
      (error, stdout) => {
        if (error) {
          resolve(unknown(error.killed ? '成片内容检查超时；生成视频已成功，不自动重生成' :
            '成片内容检查暂不可用；生成视频已成功，不自动重生成'));
          return;
        }
        try {
          const result = JSON.parse(stdout);
          if (!Object.hasOwn(REVIEW_LABELS, result.status)) throw new Error('invalid status');
          resolve(result);
        } catch (_) {
          resolve(unknown('成片内容检查返回格式异常；不影响视频生成状态'));
        }
      });
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(job));
  });
}

function buildReviewJob(context, state, videoPath) {
  return {
    script_id: state.script_id || context.fields?.['脚本ID'] || '',
    record_id: context.recordId,
    video_path: videoPath,
    prompt: state.execution_prompt || context.prompt,
    reference_assets: (state.reference_assets || []).map(asset => ({
      index: asset.index, role: asset.role, path: asset.path,
      hash: asset.derived_sha256, original_sha256: asset.original_sha256,
      derived_sha256: asset.derived_sha256
    })),
    mother_checkpoints: state.mother_checkpoints || [],
    checkpoint_source: state.mother_core_provenance || '',
    mother_contract: state.mother_contract || null
  };
}

function reviewFields(result, fields) {
  const evidence = (Array.isArray(result.evidence) ? result.evidence : []).slice(0, 12)
    .map(item => `${item.check_id || '检查点'} ${item.start_seconds ?? ''}–${item.end_seconds ?? ''}s：${item.description || ''}`);
  return {
    [fields.contentReviewResult || '内容检查结果']: REVIEW_LABELS[result.status] || REVIEW_LABELS.unknown,
    [fields.contentReviewSummary || '内容检查说明']: [String(result.summary || ''), ...evidence].join('\n').slice(0, 3500)
  };
}

module.exports = { buildReviewJob, REVIEW_LABELS, reviewFields, runReviewProcess, unknown };
