CREATE TABLE IF NOT EXISTS output_material_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usage_id TEXT NOT NULL UNIQUE,
  output_id TEXT NOT NULL,
  batch_id TEXT,
  product_id TEXT,
  target_language TEXT,
  asset_id TEXT NOT NULL,
  source_system TEXT,
  source_record_id TEXT,
  segment_count INTEGER DEFAULT 0,
  used_duration_ms INTEGER DEFAULT 0,
  roles_json TEXT,
  core_segment_count INTEGER DEFAULT 0,
  is_core_material INTEGER DEFAULT 0,
  is_first_slot INTEGER DEFAULT 0,
  first_slot_segment_id TEXT,
  created_at TEXT,
  updated_at TEXT,
  UNIQUE(output_id, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_output_material_output ON output_material_usage(output_id);
CREATE INDEX IF NOT EXISTS idx_output_material_product ON output_material_usage(product_id);
CREATE INDEX IF NOT EXISTS idx_output_material_asset ON output_material_usage(asset_id);
