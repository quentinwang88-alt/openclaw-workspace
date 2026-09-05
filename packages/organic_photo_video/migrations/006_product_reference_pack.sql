-- Operator-owned product reference packs for deterministic OPV generation.
CREATE TABLE IF NOT EXISTS opv_product_reference_pack (
    pack_id VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(191) NOT NULL,
    variant_key VARCHAR(96) NOT NULL DEFAULT 'default',
    pack_version INT NOT NULL DEFAULT 1,
    product_name VARCHAR(255) NULL,
    category VARCHAR(96) NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'limited',
    is_default TINYINT(1) NOT NULL DEFAULT 0,
    assets_json JSON NOT NULL,
    asset_fingerprint CHAR(64) NOT NULL,
    source_type VARCHAR(64) NULL,
    source_ref VARCHAR(255) NULL,
    selection_reason VARCHAR(255) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_opv_product_reference_pack_version (product_id, variant_key, pack_version),
    UNIQUE KEY uq_opv_product_reference_pack_fingerprint (product_id, variant_key, asset_fingerprint),
    KEY idx_opv_product_reference_pack_resolve (product_id, status, is_default, pack_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
