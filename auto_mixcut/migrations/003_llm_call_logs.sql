CREATE TABLE IF NOT EXISTS llm_call_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  call_id TEXT NOT NULL UNIQUE,

  call_type TEXT,
  route_policy TEXT,

  product_id TEXT,
  asset_id TEXT,
  segment_id TEXT,
  output_id TEXT,
  task_id TEXT,

  model_tier TEXT,
  model_name TEXT,
  provider TEXT,
  fallback_provider TEXT,

  prompt_version TEXT,
  input_hash TEXT,
  cache_hit INTEGER DEFAULT 0,

  result_status TEXT,
  error_code TEXT,
  error_message TEXT,

  retry_count INTEGER DEFAULT 0,
  escalation_count INTEGER DEFAULT 0,
  escalated_from TEXT,

  token_input INTEGER DEFAULT 0,
  token_output INTEGER DEFAULT 0,
  image_count INTEGER DEFAULT 0,
  estimated_cost REAL DEFAULT 0.0,
  latency_ms INTEGER,

  created_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_llm_call_logs_product ON llm_call_logs(product_id);
CREATE INDEX IF NOT EXISTS idx_llm_call_logs_call_type ON llm_call_logs(call_type);
CREATE INDEX IF NOT EXISTS idx_llm_call_logs_result_status ON llm_call_logs(result_status);
CREATE INDEX IF NOT EXISTS idx_llm_call_logs_created_at ON llm_call_logs(created_at);
