# OPV 基线检查点：安全网修复记录

日期：2026-09-13
适用项目：`packages/organic_photo_video`

## 1. 这件事解决什么

在这份记录之前，仓库处于一种**危险状态**：主工作树上全量测试是绿的，
但"回到上一个提交再验证"这条路径是坏的。

后果很具体：当后续改动把测试搞红时，你无法用"退回上一版重跑"来判断
到底是**新改动弄坏的**，还是**本来就缺东西**。安全网是漏的。

## 2. 根因

`.gitignore` 的 `**/config/profiles/*.json` 通配过宽。

- 它的本意：保护 `skills/inventory-query/config/profiles/`，那里放的是
  本机店铺账号（`店铺A.json`）和 API 备份，**不应该入库**。
- 误伤的对象：`packages/organic_photo_video/config/profiles/`，那里放的是
  随代码发布的产品配置（渲染档位 / 质检档位 profile），**必须入库**。
  其中 `QUALITY_STANDARD_V1.json` 被配方 `PHOTO_TH_TRAVEL_OUTFIT_V2` 的
  `quality_profile_id` 直接引用。
- 结果：该目录**从未被提交过**，只在开发机本地存在。

## 3. 修复方式

保留原通配规则不动，只对 OPV 包开一个精准例外（见 `.gitignore` 第 13-17 行），
使其它模块将来在本机存放密钥配置时仍然受保护。

已逐条验证：

| 路径 | 期望 | 实测 |
|---|---|---|
| `packages/organic_photo_video/config/profiles/*.json`（5 个） | 放行 | 已放行 |
| `skills/inventory-query/config/profiles/店铺A.json` | 忽略 | 仍忽略 |
| `skills/inventory-query/config/profiles/api_config.backup.*.json` | 忽略 | 仍忽略 |
| `packages/organic_photo_video/tmp/`、`logs/`、`var/` | 忽略 | 仍忽略 |
| `shared/data/organic_photo_video/asset_sets/` | 忽略 | 仍忽略 |

## 4. 提交

```
18cc27e  fix(gitignore): 放行 organic_photo_video 随代码发布的 config/profiles
1add6ec  chore(opv): 落定原生图文域基线检查点（热转线 + 旅行体感 + 表格冒烟）
```

前一个提交是修安全网；后一个提交把此前累积在脏工作区的 OPV 改动落成
可回退的检查点。核心 services 文件被多条线共享，无法在文件粒度上切分，
故合并为一个提交。

## 5. 怎么复现绿色基线

```bash
git worktree add --detach /tmp/opv_check HEAD
cd /tmp/opv_check/packages/organic_photo_video
PYTHONPATH=.:tests /usr/bin/python3 -m unittest discover -s tests -p 'test_*.py'
```

全新检出后还需补齐**三项本机专属依赖**，否则会有 6 条测试失败：

| 依赖 | 性质 | 为什么不入库 |
|---|---|---|
| `.env.local`（工作区根） | OPV 视觉模型的地址与密钥 | 含密钥，见 `.gitignore` `.env.*.local` |
| `shared/data/organic_photo_video/asset_sets/` | 参考图素材（约 24M） | 体量大，按约定不入库 |
| `skills/lightweight-tryon-video/var/light_tryon.sqlite3` | 兄弟 skill 的运行时库 | 运行产物，按约定不入库 |

影响面：`.env.local` 缺失 → 1 条（`test_photo_planning_upgrade`）；
素材缺失 → 2 条（`test_photo_labels`）；运行时库缺失 → 3 条（`test_photo_wig_choice`）。

## 6. 验收证据

同一份本机依赖，两版对比：

| | 修复前 `6b6abac` | 修复后 `1add6ec` |
|---|---|---|
| 能收集到的测试 | 825 | 938 |
| 结果 | 3 失败 + 26 错误 | 全部通过 |
| 失败原因 | 19 处 `config/profiles/*.json` 找不到 | — |

## 7. 未做

- 未入库任何本机密钥、素材或运行产物。
- 未改动 OPV 之外的模块（其脏改动仍留在工作区）。
- 未跑真实生图、未写 RDS、未改飞书 schema、未发布。
