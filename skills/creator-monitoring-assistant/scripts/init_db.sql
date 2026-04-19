CREATE TABLE IF NOT EXISTS creator_master (
    id BIGSERIAL PRIMARY KEY,
    creator_key VARCHAR(255) NOT NULL UNIQUE,
    creator_name VARCHAR(255) NOT NULL,
    platform VARCHAR(50) NOT NULL DEFAULT 'tiktok',
    country VARCHAR(50) NOT NULL DEFAULT 'unknown',
    store VARCHAR(100) NOT NULL DEFAULT '',
    first_seen_week VARCHAR(20),
    latest_seen_week VARCHAR(20),
    owner VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS creator_weekly_raw (
    id BIGSERIAL PRIMARY KEY,
    import_batch_id VARCHAR(100) NOT NULL,
    stat_week VARCHAR(20) NOT NULL,
    source_file_name VARCHAR(255) NOT NULL,
    creator_name_raw VARCHAR(255) NOT NULL,
    platform VARCHAR(50) NOT NULL DEFAULT 'tiktok',
    country VARCHAR(50) NOT NULL DEFAULT 'unknown',
    store VARCHAR(100) NOT NULL DEFAULT '',
    gmv_raw VARCHAR(100),
    refund_amount_raw VARCHAR(100),
    order_count_raw VARCHAR(100),
    sold_item_count_raw VARCHAR(100),
    refunded_item_count_raw VARCHAR(100),
    avg_order_value_raw VARCHAR(100),
    avg_daily_sold_item_count_raw VARCHAR(100),
    video_count_raw VARCHAR(100),
    live_count_raw VARCHAR(100),
    estimated_commission_raw VARCHAR(100),
    shipped_sample_count_raw VARCHAR(100),
    row_hash VARCHAR(64),
    imported_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS creator_weekly_clean (
    id BIGSERIAL PRIMARY KEY,
    stat_week VARCHAR(20) NOT NULL,
    creator_id BIGINT NOT NULL REFERENCES creator_master(id),
    import_batch_id VARCHAR(100) NOT NULL,
    store VARCHAR(100) NOT NULL DEFAULT '',
    gmv NUMERIC(18,2) NOT NULL DEFAULT 0,
    refund_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 0,
    sold_item_count INT NOT NULL DEFAULT 0,
    refunded_item_count INT NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(18,2) NOT NULL DEFAULT 0,
    avg_daily_sold_item_count NUMERIC(18,2) NOT NULL DEFAULT 0,
    video_count INT NOT NULL DEFAULT 0,
    live_count INT NOT NULL DEFAULT 0,
    estimated_commission NUMERIC(18,2) NOT NULL DEFAULT 0,
    shipped_sample_count INT NOT NULL DEFAULT 0,
    content_action_count INT NOT NULL DEFAULT 0,
    has_action BOOLEAN NOT NULL DEFAULT FALSE,
    has_result BOOLEAN NOT NULL DEFAULT FALSE,
    is_new_creator BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (stat_week, creator_id)
);

CREATE TABLE IF NOT EXISTS creator_weekly_metrics (
    id BIGSERIAL PRIMARY KEY,
    stat_week VARCHAR(20) NOT NULL,
    creator_id BIGINT NOT NULL REFERENCES creator_master(id),
    store VARCHAR(100) NOT NULL DEFAULT '',
    gmv NUMERIC(18,2) NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 0,
    content_action_count INT NOT NULL DEFAULT 0,
    video_count INT NOT NULL DEFAULT 0,
    live_count INT NOT NULL DEFAULT 0,
    shipped_sample_count INT NOT NULL DEFAULT 0,
    refund_rate NUMERIC(12,6),
    commission_rate NUMERIC(12,6),
    gmv_per_action NUMERIC(18,2),
    gmv_per_sample NUMERIC(18,2),
    items_per_order NUMERIC(12,4),
    gmv_wow NUMERIC(12,6),
    order_count_wow NUMERIC(12,6),
    action_count_wow NUMERIC(12,6),
    gmv_per_action_wow NUMERIC(12,6),
    refund_rate_wow NUMERIC(12,6),
    gmv_4w NUMERIC(18,2),
    order_count_4w INT,
    action_count_4w INT,
    avg_weekly_gmv_4w NUMERIC(18,2),
    avg_gmv_per_action_4w NUMERIC(18,2),
    avg_refund_rate_4w NUMERIC(12,6),
    gmv_lifetime NUMERIC(18,2),
    order_count_lifetime INT,
    weeks_active_lifetime INT,
    weeks_with_gmv_lifetime INT,
    weeks_with_action_lifetime INT,
    action_result_state VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (stat_week, creator_id)
);

CREATE TABLE IF NOT EXISTS creator_monitoring_result (
    id BIGSERIAL PRIMARY KEY,
    stat_week VARCHAR(20) NOT NULL,
    creator_id BIGINT NOT NULL REFERENCES creator_master(id),
    store VARCHAR(100) NOT NULL DEFAULT '',
    record_key VARCHAR(255) NOT NULL,
    primary_tag VARCHAR(50) NOT NULL,
    secondary_tags TEXT,
    risk_tags TEXT,
    priority_level VARCHAR(20),
    rule_version VARCHAR(20) NOT NULL DEFAULT 'v1',
    decision_reason TEXT,
    next_action TEXT,
    owner VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (stat_week, creator_id)
);

CREATE INDEX IF NOT EXISTS idx_creator_weekly_raw_batch ON creator_weekly_raw(import_batch_id);
CREATE INDEX IF NOT EXISTS idx_creator_weekly_clean_week ON creator_weekly_clean(stat_week);
CREATE INDEX IF NOT EXISTS idx_creator_weekly_metrics_week ON creator_weekly_metrics(stat_week);
CREATE INDEX IF NOT EXISTS idx_creator_monitoring_result_week ON creator_monitoring_result(stat_week);
