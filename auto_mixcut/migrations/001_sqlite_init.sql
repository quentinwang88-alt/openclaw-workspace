CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_id TEXT NOT NULL UNIQUE,
  product_name TEXT,
  market TEXT,
  category TEXT,
  shop_id TEXT,
  priority TEXT,
  product_anchor_json TEXT,
  anchor_status TEXT DEFAULT 'pending',
  anchor_version TEXT,
  anchor_confirmed_at TEXT,
  anchor_confirmed_by TEXT,
  product_status TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS content_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL UNIQUE,
  product_id TEXT NOT NULL,
  task_type TEXT,
  requested_variant_count INTEGER DEFAULT 0,
  allowed_variant_count INTEGER DEFAULT 0,
  actual_variant_count INTEGER DEFAULT 0,
  material_tier TEXT,
  material_status TEXT,
  task_status TEXT,
  anchor_required INTEGER DEFAULT 1,
  anchor_status_at_task_start TEXT,
  blocked_reason TEXT,
  failure_reason TEXT,
  created_by TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS oss_objects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_id TEXT NOT NULL UNIQUE,
  bucket TEXT,
  object_key TEXT,
  object_type TEXT,
  file_name TEXT,
  file_ext TEXT,
  mime_type TEXT,
  file_size INTEGER,
  file_hash TEXT,
  storage_status TEXT,
  lifecycle_policy TEXT,
  expire_at TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_id TEXT NOT NULL UNIQUE,
  product_id TEXT NOT NULL,
  source_type TEXT,
  source_trust_level TEXT,
  product_binding_type TEXT,
  media_type TEXT,
  original_oss_object_id TEXT,
  normalized_oss_object_id TEXT,
  file_status TEXT,
  probe_status TEXT,
  duration_ms INTEGER,
  width INTEGER,
  height INTEGER,
  fps REAL,
  codec TEXT,
  orientation TEXT,
  has_audio INTEGER,
  probe_json TEXT,
  has_watermark TEXT DEFAULT 'pending',
  watermark_type TEXT,
  watermark_position TEXT,
  watermark_confidence TEXT,
  watermark_reason TEXT,
  watermark_checked_at TEXT,
  risk_level TEXT,
  asset_status TEXT,
  local_file_status TEXT,
  ai_tag_status TEXT,
  human_review_status TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS segments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  segment_id TEXT NOT NULL UNIQUE,
  asset_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  segment_oss_object_id TEXT,
  thumbnail_oss_object_id TEXT,
  start_ms INTEGER,
  end_ms INTEGER,
  duration_ms INTEGER,
  width INTEGER,
  height INTEGER,
  fps REAL,
  segment_status TEXT,
  source_type TEXT,
  source_trust_level TEXT,
  product_binding_type TEXT,
  product_match_status TEXT,
  product_match_confidence TEXT,
  product_match_review_required INTEGER,
  product_match_reason TEXT,
  effective_roles_json TEXT,
  effective_roles_updated_at TEXT,
  effective_roles_reason TEXT,
  frame_consistency_score REAL,
  frame_consistency_status TEXT,
  frame_consistency_reason TEXT,
  is_image_generated INTEGER DEFAULT 0,
  usage_count INTEGER DEFAULT 0,
  used_in_outputs_count INTEGER DEFAULT 0,
  avg_first_3s_retention REAL,
  avg_completion_rate REAL,
  performance_score REAL,
  last_performance_update TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS segment_frames (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  frame_id TEXT NOT NULL UNIQUE,
  segment_id TEXT NOT NULL,
  frame_index INTEGER,
  timestamp_ms INTEGER,
  oss_object_id TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS ai_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ai_batch_id TEXT NOT NULL UNIQUE,
  product_id TEXT,
  batch_type TEXT,
  status TEXT,
  total_segments INTEGER DEFAULT 0,
  completed_segments INTEGER DEFAULT 0,
  failed_segments INTEGER DEFAULT 0,
  model_tier TEXT,
  prompt_version TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS ai_tag_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tag_run_id TEXT NOT NULL UNIQUE,
  ai_batch_id TEXT,
  segment_id TEXT,
  model_tier TEXT,
  model_name TEXT,
  prompt_version TEXT,
  run_type TEXT,
  temperature REAL,
  raw_response TEXT,
  parsed_success INTEGER,
  error_message TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS segment_tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  segment_id TEXT NOT NULL,
  tag_source TEXT,
  primary_shot_role TEXT,
  secondary_roles_json TEXT,
  product_visibility TEXT,
  hook_strength TEXT,
  mixcut_usability TEXT,
  risk_level TEXT,
  confidence TEXT,
  needs_human_review INTEGER,
  reason TEXT,
  reviewer_id TEXT,
  reviewed_at TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS llm_calls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  call_id TEXT NOT NULL UNIQUE,
  task_id TEXT,
  product_id TEXT,
  asset_id TEXT,
  segment_id TEXT,
  output_id TEXT,
  call_type TEXT,
  model_tier TEXT,
  model_name TEXT,
  prompt_version TEXT,
  input_hash TEXT,
  cache_hit INTEGER DEFAULT 0,
  token_input INTEGER,
  token_output INTEGER,
  image_count INTEGER,
  estimated_cost REAL,
  latency_ms INTEGER,
  result_status TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS llm_cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cache_key TEXT NOT NULL UNIQUE,
  call_type TEXT,
  product_id TEXT,
  asset_id TEXT,
  segment_id TEXT,
  model_tier TEXT,
  model_name TEXT,
  prompt_version TEXT,
  input_hash TEXT,
  response_json TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS golden_segments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  golden_segment_id TEXT NOT NULL UNIQUE,
  segment_id TEXT,
  category TEXT,
  market TEXT,
  difficulty_level TEXT,
  purpose TEXT,
  active INTEGER DEFAULT 1,
  created_by TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS golden_labels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  golden_segment_id TEXT NOT NULL,
  expected_primary_shot_role TEXT,
  expected_secondary_roles_json TEXT,
  expected_product_visibility TEXT,
  expected_hook_strength TEXT,
  expected_mixcut_usability TEXT,
  expected_risk_level TEXT,
  expected_confidence TEXT,
  human_reason TEXT,
  label_version TEXT,
  approved_by TEXT,
  approved_at TEXT
);

CREATE TABLE IF NOT EXISTS golden_benchmark_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  benchmark_run_id TEXT NOT NULL UNIQUE,
  model_tier TEXT,
  model_name TEXT,
  prompt_version TEXT,
  category TEXT,
  total_segments INTEGER,
  passed_segments INTEGER,
  failed_segments INTEGER,
  overall_score REAL,
  role_accuracy REAL,
  visibility_accuracy REAL,
  hook_accuracy REAL,
  risk_recall REAL,
  risk_precision REAL,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS mixcut_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL UNIQUE,
  product_id TEXT,
  task_id TEXT,
  requested_count INTEGER,
  allowed_count INTEGER,
  rendered_count INTEGER,
  batch_status TEXT,
  material_tier TEXT,
  template_pool_json TEXT,
  experiment_batch TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS render_plans (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  render_plan_id TEXT NOT NULL UNIQUE,
  batch_id TEXT,
  product_id TEXT,
  variant_no INTEGER,
  template_id TEXT,
  planned_duration_ms INTEGER,
  plan_json TEXT,
  quality_gate_status TEXT,
  render_status TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS outputs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  output_id TEXT NOT NULL UNIQUE,
  batch_id TEXT,
  product_id TEXT,
  variant_no INTEGER,
  template_id TEXT,
  output_oss_object_id TEXT,
  cover_oss_object_id TEXT,
  duration_ms INTEGER,
  width INTEGER,
  height INTEGER,
  render_status TEXT,
  machine_quality_status TEXT,
  human_quality_status TEXT,
  feishu_preview_status TEXT,
  feishu_record_id TEXT,
  preview_expire_at TEXT,
  experiment_group TEXT,
  experiment_batch TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS output_segments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  output_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  asset_id TEXT,
  slot_index INTEGER,
  role_used TEXT,
  start_ms_in_output INTEGER,
  end_ms_in_output INTEGER,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS feishu_sync_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sync_id TEXT NOT NULL UNIQUE,
  object_type TEXT,
  object_id TEXT,
  feishu_table TEXT,
  feishu_record_id TEXT,
  sync_status TEXT,
  expire_at TEXT,
  cleanup_status TEXT,
  created_at TEXT,
  updated_at TEXT
);
