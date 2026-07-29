ALTER TABLE products ADD COLUMN canonical_product_id TEXT;

ALTER TABLE assets ADD COLUMN canonical_product_id TEXT;
ALTER TABLE assets ADD COLUMN source_flow TEXT;
ALTER TABLE assets ADD COLUMN source_record_id TEXT;
ALTER TABLE assets ADD COLUMN generation_channel TEXT;
ALTER TABLE assets ADD COLUMN source_completed_at TEXT;
ALTER TABLE assets ADD COLUMN visual_scope TEXT DEFAULT 'global';

ALTER TABLE segments ADD COLUMN canonical_product_id TEXT;
ALTER TABLE segments ADD COLUMN visual_scope TEXT DEFAULT 'global';

CREATE TABLE IF NOT EXISTS product_identity_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alias_id TEXT NOT NULL UNIQUE,
  canonical_product_id TEXT NOT NULL,
  product_id TEXT,
  local_product_id TEXT,
  store_id TEXT,
  market TEXT,
  alias_type TEXT,
  alias_value TEXT,
  status TEXT DEFAULT 'active',
  source TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_product_alias_canonical
  ON product_identity_aliases(canonical_product_id);
CREATE INDEX IF NOT EXISTS idx_product_alias_product
  ON product_identity_aliases(product_id);
CREATE INDEX IF NOT EXISTS idx_product_alias_value
  ON product_identity_aliases(alias_type, alias_value);

CREATE TABLE IF NOT EXISTS material_source_registry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_key TEXT NOT NULL UNIQUE,
  source_system TEXT NOT NULL,
  source_record_id TEXT,
  source_result_index INTEGER DEFAULT 1,
  source_flow TEXT,
  script_id TEXT,
  product_id TEXT,
  canonical_product_id TEXT,
  local_product_id TEXT,
  source_market TEXT,
  source_store_id TEXT,
  channel TEXT,
  model TEXT,
  trace_id TEXT,
  platform_task_id TEXT,
  completed_at TEXT,
  attachment_token TEXT,
  file_name TEXT,
  file_size INTEGER,
  file_hash TEXT,
  ingest_policy TEXT DEFAULT 'auto',
  ingest_status TEXT DEFAULT 'discovered',
  oss_object_id TEXT,
  asset_id TEXT,
  legacy_flag INTEGER DEFAULT 0,
  retry_count INTEGER DEFAULT 0,
  last_error TEXT,
  source_payload_json TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_material_source_product_status
  ON material_source_registry(canonical_product_id, ingest_status);
CREATE INDEX IF NOT EXISTS idx_material_source_execution_product
  ON material_source_registry(product_id);
CREATE INDEX IF NOT EXISTS idx_material_source_record
  ON material_source_registry(source_record_id);
CREATE INDEX IF NOT EXISTS idx_material_source_script
  ON material_source_registry(script_id);
CREATE INDEX IF NOT EXISTS idx_material_source_completed
  ON material_source_registry(completed_at);
CREATE INDEX IF NOT EXISTS idx_material_source_legacy_status
  ON material_source_registry(legacy_flag, ingest_status);
