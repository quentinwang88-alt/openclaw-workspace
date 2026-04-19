# Database Conventions

## Shared Root

所有 Skill 的正式业务数据库统一存放在：

- `/Users/likeu3/.openclaw/shared/data`

## Current Canonical Databases

- 店铺销量分析：
  `/Users/likeu3/.openclaw/shared/data/ecommerce.db`
- 达人监控：
  `/Users/likeu3/.openclaw/shared/data/creator_monitoring.sqlite3`

## Rules

1. 正式数据库不要放在 skill 工作区目录中
2. Skill 工作区里的旧数据库只能保留为备份，或改成指向共享库的软链接
3. 新 Skill 如果需要 SQLite，优先命名为 `<domain>.sqlite3`
4. 所有默认数据库路径都应支持 `OPENCLAW_SHARED_DATA_DIR` 覆盖
5. README / SKILL.md 需要明确说明数据库实际落点

## Migration Pattern

当一个已有 skill 仍然把数据库写在工作区时，按以下顺序迁移：

1. 选定共享库目标路径
2. 备份旧库
3. 将代码默认路径切到共享库
4. 必要时把旧路径替换为软链接，避免旧命令继续写出分叉数据

