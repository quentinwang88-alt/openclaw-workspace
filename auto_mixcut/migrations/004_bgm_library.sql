CREATE TABLE IF NOT EXISTS bgm_tracks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bgm_id TEXT NOT NULL UNIQUE,

  track_name TEXT,
  artist_name TEXT,
  source_platform TEXT,
  source_url TEXT,
  file_name TEXT,
  download_version TEXT,
  duration_ms INTEGER,
  file_size INTEGER,
  audio_format TEXT,
  sample_rate INTEGER,
  channels INTEGER,

  official_tags_json TEXT,
  license_note TEXT,
  oss_object_id TEXT,
  local_file_path TEXT,

  mood_tags_json TEXT,
  energy_level TEXT,
  vocal_type TEXT,
  category_tags_json TEXT,
  template_tags_json TEXT,

  recommended_start_sec REAL DEFAULT 0,
  default_volume REAL DEFAULT 0.2,
  fade_in_ms INTEGER DEFAULT 500,
  fade_out_ms INTEGER DEFAULT 800,
  suitable_for_intro INTEGER DEFAULT 1,
  loop_friendly INTEGER DEFAULT 0,
  voiceover_friendly INTEGER DEFAULT 1,

  ai_suggested_tags_json TEXT,
  tag_diff_json TEXT,
  tag_confidence TEXT,
  tag_review_required INTEGER DEFAULT 0,

  bgm_tag_status TEXT DEFAULT 'untagged',
  bgm_tagged_at TEXT,
  bgm_tag_prompt_version TEXT,
  bgm_tag_reason TEXT,

  existing_human_tags_json TEXT,
  allowed_labels_json TEXT,

  mix_constraints_json TEXT,
  audio_analysis_json TEXT,
  audio_analyzed_at TEXT,
  audio_tag_source TEXT,
  audio_tag_confidence TEXT,
  performance_tags_json TEXT,
  usage_count INTEGER DEFAULT 0,

  created_at TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_bgm_tracks_platform ON bgm_tracks(source_platform);
CREATE INDEX IF NOT EXISTS idx_bgm_tracks_mood ON bgm_tracks(mood_tags_json);
CREATE INDEX IF NOT EXISTS idx_bgm_tracks_energy ON bgm_tracks(energy_level);
CREATE INDEX IF NOT EXISTS idx_bgm_tracks_category ON bgm_tracks(category_tags_json);
CREATE INDEX IF NOT EXISTS idx_bgm_tracks_tag_status ON bgm_tracks(bgm_tag_status);

CREATE TABLE IF NOT EXISTS bgm_usage_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  bgm_id TEXT NOT NULL,
  output_id TEXT,
  batch_id TEXT,
  product_id TEXT,
  template_id TEXT,
  usage_status TEXT,
  quality_status TEXT,
  reason TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_bgm_usage_bgm ON bgm_usage_events(bgm_id);
CREATE INDEX IF NOT EXISTS idx_bgm_usage_output ON bgm_usage_events(output_id);
CREATE INDEX IF NOT EXISTS idx_bgm_usage_product ON bgm_usage_events(product_id);
