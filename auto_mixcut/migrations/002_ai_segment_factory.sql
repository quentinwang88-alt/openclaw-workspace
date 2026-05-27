ALTER TABLE assets ADD COLUMN generation_job_id TEXT;
ALTER TABLE assets ADD COLUMN generation_type TEXT;
ALTER TABLE assets ADD COLUMN generation_model TEXT;
ALTER TABLE assets ADD COLUMN generation_prompt TEXT;

ALTER TABLE segments ADD COLUMN anchor_match_level TEXT;
ALTER TABLE segments ADD COLUMN anchor_check_reason TEXT;
ALTER TABLE segments ADD COLUMN allowed_core_roles_json TEXT;
ALTER TABLE segments ADD COLUMN allowed_soft_roles_json TEXT;
ALTER TABLE segments ADD COLUMN segment_type TEXT;

CREATE TABLE IF NOT EXISTS ai_generation_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL UNIQUE,
  product_id TEXT NOT NULL,
  market TEXT,
  category TEXT,
  segment_type TEXT,
  requested_count INTEGER DEFAULT 0,
  generated_count INTEGER DEFAULT 0,
  accepted_count INTEGER DEFAULT 0,
  imported_segment_count INTEGER DEFAULT 0,
  strict_pass_count INTEGER DEFAULT 0,
  soft_pass_count INTEGER DEFAULT 0,
  uncertain_count INTEGER DEFAULT 0,
  fail_count INTEGER DEFAULT 0,
  prompt_template_id TEXT,
  prompt_version TEXT,
  prompt_text TEXT,
  reference_asset_ids TEXT,
  reference_image_oss_ids TEXT,
  generation_type TEXT,
  model_name TEXT,
  status TEXT,
  failure_reason TEXT,
  feishu_record_id TEXT,
  scene_preference TEXT,
  style_preference TEXT,
  character_requirement TEXT,
  notes TEXT,
  created_at TEXT,
  updated_at TEXT
);
