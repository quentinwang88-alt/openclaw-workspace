-- 小红书穿搭素材实验室 · 本地素材库 schema（v1）
-- 原则：整篇笔记入库（保留组图顺序与结构），按张标记选用；去重以 note_id 与图片 sha256 双保险。
-- 本库只存参考素材，永不接入 OPV 发布链（见 docs/XHS_OUTFIT_MATERIAL_INTAKE_PRE_RESEARCH_20260915.md）。

PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS notes (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id          TEXT UNIQUE,              -- 小红书笔记 ID（短链入库时先留空，抓取时回填）
    xsec_token       TEXT,                     -- 从链接中提取；有时效，仅作回溯用，不假定长期有效
    source_url       TEXT NOT NULL,            -- 完整原始链接（含 query），抓取时按原样使用
    author_id        TEXT,
    author_nickname  TEXT,
    title            TEXT,
    description      TEXT,                     -- 笔记正文
    theme            TEXT,                     -- 采集主题（如：旅游冬装 / 显高搭配），人工或搜索任务写入
    origin           TEXT DEFAULT 'manual',    -- manual=粘贴链接 / search=自动搜索
    status           TEXT DEFAULT 'pending_review',
                     -- pending_review / selected / rejected
    authorization    TEXT DEFAULT 'reference_only',
                     -- reference_only（默认，仅作参考）/ cleared_manual（人工确认过授权，另行流程）
    fetch_status     TEXT DEFAULT 'pending',   -- pending / fetched / partial / failed
    fetch_error      TEXT,
    image_count      INTEGER DEFAULT 0,
    like_count       INTEGER,
    collected_count  INTEGER,
    published_at     TEXT,
    raw_json_path    TEXT,                     -- 抓取时的原始数据存档（out/<note_id>/raw.json）
    feishu_record_id TEXT,                     -- 飞书审核表行映射（审核回读用）
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS note_images (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    note_pk      INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    seq          INTEGER NOT NULL,             -- 原始组图顺序（1 起）
    file_path    TEXT,                         -- 本地相对路径（相对实验室 out/）
    remote_url   TEXT,
    width        INTEGER,
    height       INTEGER,
    sha256       TEXT UNIQUE,                  -- 跨笔记图片级去重
    phash        TEXT,                         -- 感知哈希，试点后可选
    status       TEXT DEFAULT 'ok',            -- ok / failed / duplicate
    note         TEXT,                          -- 该图不可用原因等
    created_at   TEXT DEFAULT (datetime('now')),
    UNIQUE (note_pk, seq)
);

CREATE TABLE IF NOT EXISTS fetch_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ran_at          TEXT DEFAULT (datetime('now')),
    tool            TEXT NOT NULL,             -- xhs-downloader / xiaohongshu-mcp
    mode            TEXT,                      -- link_intake / theme_search
    notes_ok        INTEGER DEFAULT 0,
    notes_failed    INTEGER DEFAULT 0,
    images_ok       INTEGER DEFAULT 0,
    images_failed   INTEGER DEFAULT 0,
    manual_interventions TEXT DEFAULT 0,       -- 本次需人工介入的次数（登录失效/验证码等）
    log             TEXT
);

-- 运营审核用简单视图：待审 + 已选
CREATE VIEW IF NOT EXISTS v_pending AS
    SELECT id, note_id, theme, title, image_count, fetch_status, created_at
    FROM notes WHERE status = 'pending_review' ORDER BY created_at DESC;

CREATE VIEW IF NOT EXISTS v_selected AS
    SELECT id, note_id, theme, title, image_count, updated_at
    FROM notes WHERE status = 'selected' ORDER BY updated_at DESC;
