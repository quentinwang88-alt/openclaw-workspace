"""Single source of truth for the V1 Lite Feishu surface."""

from __future__ import annotations

from typing import Any


DEFAULT_UNIFIED_BASE_URL = (
    "https://gcngopvfvo0q.feishu.cn/wiki/"
    "RlyVw4ppwiRJcjkygNmcEYaVnXb?table=tblrxlscyh8DwgNo&view=vewVhleHjQ"
)
DEFAULT_PRIMARY_TABLE_ID = "tblrxlscyh8DwgNo"


def _options(*names: str) -> dict[str, Any]:
    return {"options": [{"name": name} for name in names]}


def _description(text: str) -> dict[str, Any]:
    return {"disable_sync": False, "text": text}


TABLE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "key": "mother",
        "name": "成功脚本复刻表",
        "primary": "母版名称",
        "view": "运营视图",
        "fields": (
            {"name": "母版名称", "type": 1, "ui_type": "Text", "primary": True, "description": _description("【人工必填】给这套成功脚本起一个便于识别的名称。")},
            {"name": "来源产品", "type": 18, "ui_type": "DuplexLink", "relation": "product", "multiple": False, "description": _description("【人工必填】选择最初跑出成功脚本的产品；该产品须先在产品素材库确认资料。")},
            {"name": "成功脚本", "type": 1, "ui_type": "Text", "description": _description("【人工必填】粘贴测试成功的完整原始脚本；系统不会覆盖或静默修改。")},
            {"name": "验证说明", "type": 1, "ui_type": "Text", "description": _description("【人工选填】可简述已有成功依据；没有补充说明时留空，不影响生成。系统不会代填或编造验证结果。")},
            {"name": "母版状态", "type": 3, "ui_type": "SingleSelect", "property": _options("待处理", "处理中", "待确认", "已确认", "失败", "已停用"), "description": _description("【系统维护，无需填写】显示母版解析、审核和人工确认进度。")},
            {"name": "母版摘要", "type": 1, "ui_type": "Text", "description": _description("【系统生成，无需填写】母版解析和独立审查后的可读规则摘要，供人工确认；可以留空等待系统生成。")},
            {"name": "待复刻产品", "type": 18, "ui_type": "DuplexLink", "relation": "product", "multiple": True, "description": _description("【生成前人工必填】人工判断适合后选择一个或多个产品；系统不做匹配度评分。")},
            {"name": "人物脸部参考图", "type": 17, "ui_type": "Attachment", "description": _description("【生成前人工必填】上传人物形象图；只参考脸部和身份特征，不使用图片中的原有头发。请勿放入产品图片字段。")},
            {"name": "发布用途", "type": 3, "ui_type": "SingleSelect", "property": _options("带货", "养号"), "description": _description("【生成前人工必填】本批选择带货或养号。两种用途分别累计数量；切换用途不修改已有脚本。")},
            {"name": "挂车设置", "type": 3, "ui_type": "SingleSelect", "property": _options("按用途默认", "不挂车", "挂车"), "description": _description("【人工选填】留空按用途默认：带货挂车、养号不挂车。可以明确覆盖；只影响发布设置，不重写已生成脚本。")},
            {"name": "每产品生成数", "type": 2, "ui_type": "Number", "property": {"formatter": "0"}, "description": _description("【人工选填】同母版版本、产品和发布用途下1–20的累计目标，不是本轮新增数。已有3条后改填6只补第4–6条；带货与养号分别累计。留空同品12条、跨品4条。")},
            {"name": "特殊要求", "type": 1, "ui_type": "Text", "description": _description("【人工选填】本批次临时要求；留空使用母版默认规则。")},
            {"name": "复刻状态", "type": 3, "ui_type": "SingleSelect", "property": _options("未发起", "校验资料", "处理母版", "生成提示词", "写入结果", "已完成", "部分完成", "失败"), "description": _description("【系统维护，无需填写】实时显示资料校验、母版处理、提示词生成和写回阶段。")},
            {"name": "结果摘要", "type": 1, "ui_type": "Text", "description": _description("【系统生成，无需填写】记录成功提示词数、失败产品数及可操作原因。")},
            {"name": "动作请求", "type": 3, "ui_type": "SingleSelect", "property": _options("开始生成"), "prune_options": True, "description": _description("【人工操作】资料填好后只需选择“开始生成”；系统自动处理母版、审查、启用并写入提示词。")},
        ),
    },
    {
        "key": "product",
        "name": "产品素材库",
        "primary": "产品ID",
        "view": "运营视图",
        "fields": (
            {"name": "产品ID", "type": 1, "ui_type": "Text", "primary": True, "description": _description("【人工必填】产品唯一编码；确认后不要随意修改。")},
            {"name": "产品名称", "type": 1, "ui_type": "Text", "description": _description("【人工必填】运营可识别的产品名称。")},
            {"name": "产品图片", "type": 17, "ui_type": "Attachment", "description": _description("【人工必填】至少上传1张清晰产品图，建议正面、侧面和背面/细节。")},
            {"name": "产品补充说明", "type": 1, "ui_type": "Text", "description": _description("【人工选填】建议按“卖点：… / 证明动作：… / 禁用说法：…”填写；未确认的信息不要写成卖点。")},
            {"name": "产品资料状态", "type": 3, "ui_type": "SingleSelect", "property": _options("待确认", "已确认"), "description": _description("【人工必填】核对图片和说明后设为“已确认”，否则不能用于复刻。")},
            {"name": "产品资料摘要", "type": 1, "ui_type": "Text", "description": _description("【人工选填】可写一段简短确认摘要；留空时系统使用产品名称和补充说明。")},
        ),
    },
    {
        "key": "prompt",
        "name": "复刻提示词表",
        "primary": "复刻产品",
        "view": "待审核",
        "fields": (
            # Feishu requires a text primary field. Store the product display name/ID;
            # lineage and the real product relation remain authoritative in RDS.
            {"name": "复刻产品", "type": 1, "ui_type": "Text", "primary": True, "description": _description("【系统生成，无需填写】目标产品ID。")},
            {"name": "版本类型", "type": 3, "ui_type": "SingleSelect", "property": _options("高保真·H1基准", "高保真·H2外壳轻变", "高保真·H3钩子轻变", "一般复刻·G1钩子重构", "一般复刻·G2揭晓重构", "一般复刻·G3证明重构", "一般复刻·G4场景CTA重构", "一般复刻·G5节奏重构", "高还原", "外壳变体", "钩子变体", "揭示动作变体", "证明动作变体", "跨产品高继承", "跨产品适配"), "description": _description("【系统生成，无需填写】第1–3条为高保真H1–H3，第4条起按一般复刻G1–G5循环。旧类型保留用于兼容历史记录。")},
            {"name": "本条改动", "type": 1, "ui_type": "Text", "description": _description("【系统生成，无需填写】说明本条的真实变化；一般复刻至少改变三个创意维度。")},
            {"name": "完整提示词", "type": 1, "ui_type": "Text", "description": _description("【系统生成，无需填写】可直接提交给视频生成模型的完整自包含提示词。")},
            {"name": "状态", "type": 3, "ui_type": "SingleSelect", "property": _options("待审核", "通过", "退回"), "description": _description("【历史审核状态】新结果自动进入视频脚本总库，在总库查看并勾选进入生产，无需在这里重复审核；历史状态保留。")},
        ),
    },
)


TABLE_BY_KEY = {spec["key"]: spec for spec in TABLE_SPECS}
