from __future__ import annotations

from dataclasses import dataclass
from typing import Any


TEXT = {"type": 1, "ui_type": "Text"}
NUMBER = {"type": 2, "ui_type": "Number"}
DATETIME = {"type": 5, "ui_type": "DateTime"}
URL = {"type": 15, "ui_type": "Url"}
ATTACHMENT = {"type": 17, "ui_type": "Attachment"}
CHECKBOX = {"type": 7, "ui_type": "Checkbox"}
CREATED_TIME = {"type": 1001, "ui_type": "CreatedTime"}
MODIFIED_TIME = {"type": 1002, "ui_type": "ModifiedTime"}


def field(name: str, backend: str, base: dict[str, Any] = TEXT, *, options: list[str] | None = None) -> dict[str, Any]:
    spec = {"name": name, "backend": backend, **base}
    if options is not None:
        spec["property"] = {"options": [{"name": value, "color": index % 54} for index, value in enumerate(options)]}
    return spec


def single(name: str, backend: str, options: list[str]) -> dict[str, Any]:
    return field(name, backend, {"type": 3, "ui_type": "SingleSelect"}, options=options)


def multi(name: str, backend: str, options: list[str]) -> dict[str, Any]:
    return field(name, backend, {"type": 4, "ui_type": "MultiSelect"}, options=options)


STATUS_OPTIONS = ["启用", "测试", "停用"]
SYNC_STATUS_OPTIONS = ["待同步", "已同步", "同步失败"]
MARKETS = ["泰国", "越南", "马来西亚", "墨西哥", "其他"]
CATEGORIES = ["T恤", "针织", "背心", "吊带", "衬衫", "外套", "裤装", "裙装", "连衣裙", "套装", "短上衣", "宽松上衣", "连体裤", "家居服"]


@dataclass(frozen=True)
class TableMapping:
    role: str
    title: str
    primary_field: str
    primary_backend: str
    fields: tuple[dict[str, Any], ...]
    view_names: tuple[str, ...]

    @property
    def backend_by_field(self) -> dict[str, str]:
        return {spec["name"]: spec["backend"] for spec in self.fields}

    @property
    def field_by_backend(self) -> dict[str, str]:
        return {spec["backend"]: spec["name"] for spec in self.fields}


COMMON_SYNC_FIELDS = (
    single("同步状态", "sync_status", SYNC_STATUS_OPTIONS),
    field("最后同步时间", "last_synced_at", DATETIME),
    field("同步错误信息", "sync_error"),
    field("创建时间", "created_at", CREATED_TIME),
    field("最后修改时间", "feishu_updated_at", MODIFIED_TIME),
    field("备注", "notes"),
)


PERSONA_FIELDS = (
    field("视觉身份ID", "persona_id"),
    field("视觉身份名称", "persona_name"),
    field("适用账号", "account_ids"),
    multi("适用市场", "markets", MARKETS),
    single("启用状态", "status", STATUS_OPTIONS),
    field("人物参考图", "reference_images", ATTACHMENT),
    single("品牌叠加状态", "brand_overlay_enabled", ["启用", "停用"]),
    field("店铺Logo", "brand_logo_images", ATTACHMENT),
    field("品牌展示名称", "brand_display_name"),
    single("品牌视觉预设", "brand_style_preset", ["奶油衬线", "极简无衬线"]),
    single("品牌主色", "brand_primary_color", ["奶油白", "纯白", "浅金", "暖灰"]),
    field("默认系列名称", "brand_default_series_title"),
    single("年龄观感", "age_group", ["18-22岁", "23-27岁", "28-32岁"]),
    single("身材类型", "body_type", ["纤细", "匀称", "微曲线", "自然普通身材"]),
    single("身高观感", "height_impression", ["偏娇小", "中等", "偏高挑"]),
    single("发型", "hair_style", ["黑长直", "深色长直", "轻微自然卷", "低马尾"]),
    single("发色", "hair_color", ["黑色", "深棕黑", "深棕色"]),
    single("肤色观感", "skin_tone", ["白皙", "自然偏白", "自然肤色"]),
    single("妆容风格", "makeup_style", ["淡妆", "自然妆", "弱妆感"]),
    multi("整体气质", "vibe", ["自然", "干净", "轻松", "日常", "轻通勤", "少女感"]),
    single("手机外观", "phone_style", ["浅色三摄", "银白三摄", "深色三摄"]),
    single("手机壳颜色", "phone_case_color", ["白色", "浅灰", "透明", "浅米色"]),
    single("遮脸方式", "face_visibility", ["完全遮脸", "露额头和下巴", "只露脸部轮廓"]),
    multi("固定配饰", "fixed_accessories", ["无", "细项链", "简单手链", "小耳钉", "发夹"]),
    field("默认场景ID", "default_scene_id"),
    field("人设核心描述", "prompt_core"),
    field("人设禁止项", "prompt_negative"),
    field("一致性版本", "consistency_version"),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


SCENE_FIELDS = (
    field("场景模板ID", "scene_id"),
    field("场景名称", "scene_name"),
    single("启用状态", "status", STATUS_OPTIONS),
    single("场景类型", "scene_type", ["环境模板", "主场景全身", "同场景半身", "轻变化全身", "高亮上半身固定", "缓慢推近"]),
    field("建议使用占比", "usage_ratio", NUMBER),
    multi("适用类目", "applicable_categories", CATEGORIES),
    field("场景参考图", "reference_images", ATTACHMENT),
    single("房间类型", "room_type", ["室内试穿空间", "卧室", "卧室角落", "窗边房间", "现代咖啡店", "客厅", "其他"]),
    single("墙面颜色", "wall_color", ["暖白色", "纯白色", "浅米色"]),
    single("地面类型", "floor_type", ["浅米亮面砖", "浅米哑光砖", "浅原木地板"]),
    single("天花板类型", "ceiling_type", ["简约吊顶", "平顶灯带"]),
    single("顶部灯光", "ceiling_light_type", ["暖白灯带", "中性暖白灯带", "无明显灯带"]),
    single("床的位置", "bed_position", ["左后方", "右后方", "左侧后方", "右侧后方", "不出现"]),
    single("床品颜色", "bed_sheet_color", ["深蓝灰", "浅灰", "米白"]),
    single("窗帘位置", "curtain_position", ["左后方", "右后方", "侧后整面", "不出现"]),
    single("窗帘颜色", "curtain_color", ["中灰", "暖灰", "深灰"]),
    single("置物架位置", "shelf_position", ["左侧后方", "右侧后方", "不出现"]),
    single("场景视觉风格", "scene_style", ["INS奶油风", "现代极简", "浅原木风"]),
    multi("背景候选类型", "background_type_pool", ["极简暖白墙", "浅色轻纹理墙", "垂直百褶帘", "浅原木纹板", "浅原木竖条格栅"]),
    single("背景简洁度", "background_cleanliness", ["极简", "少量层次", "生活化适中"]),
    multi("边缘装饰候选", "edge_decor_pool", ["不放装饰", "琴叶榕", "龟背竹", "小型绿植", "极简落地灯"]),
    single("装饰数量", "decor_count", ["不放", "1件", "最多2件"]),
    single("装饰位置", "decor_position", ["左边缘", "右边缘", "背景转角", "系统自动边缘"]),
    single("主光方向", "key_light_direction", ["左前方45°", "右前方45°", "系统左右轮换", "正面柔光"]),
    multi("必须出现锚点", "required_anchors", ["床", "灰色落地窗帘", "小型置物架", "暖白灯带", "浅米地面", "暖白墙", "极简浅色主背景", "45度斜侧柔光"]),
    multi("可选锚点", "optional_anchors", ["花瓶", "玩偶", "收纳盒", "相框", "绿植", "床头柜", "琴叶榕", "龟背竹", "极简落地灯", "浅原木纹板", "浅原木竖条格栅", "垂直百褶帘"]),
    multi("禁止出现元素", "forbidden_elements", ["摄影棚", "酒店房间", "大客厅", "木地板", "镜子自拍", "多人", "彩色灯光", "豪华装修", "强逆光", "广角畸变", "大面积空白背景", "橙黄色滤镜", "暖黄偏色", "硬侧光阴影", "装饰遮挡服装", "杂乱家具", "密集装饰"]),
    single("画面比例", "aspect_ratio", ["9:16"]),
    single("景别", "shot_type", ["全身", "大半身", "半身"]),
    single("机位高度", "camera_height", ["腰部高度", "胸口高度", "眼平高度"]),
    single("机位角度", "camera_angle", ["正面平视", "轻微俯拍", "轻微仰拍"]),
    single("镜头运动", "camera_motion", ["固定", "缓慢推近"]),
    single("人物位置", "subject_position", ["正中", "中间偏左", "中间偏右"]),
    single("人物画面占比", "subject_scale", ["35%-40%", "40%-45%", "45%-55%"]),
    single("人物运动范围", "movement_boundary", ["极小范围", "小范围"]),
    single("光线风格", "lighting_style", ["柔和自然光", "中性暖白室内光", "自然光加暖白补光", "45度斜侧柔光加正面补光"]),
    single("光线强度", "lighting_level", ["中等明亮", "明亮", "偏柔和"]),
    single("光线色调", "lighting_tone", ["中性", "略偏暖", "浅暖白不偏黄"]),
    field("核心场景描述", "prompt_core"),
    field("场景一致性要求", "consistency_prompt"),
    field("场景禁止项描述", "prompt_negative"),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


ACTION_FIELDS = (
    field("动作模板ID", "action_id"),
    field("动作名称", "action_name"),
    single("启用状态", "status", STATUS_OPTIONS),
    single("动作类型", "action_type", ["基础站立", "整理衣摆", "轻侧身", "触碰领口袖口", "扶腰插兜", "半步前移", "上装复合微动作", "外套复合微动作", "轮廓复合微动作", "自定义动作"]),
    multi("适用类目", "applicable_categories", CATEGORIES),
    multi("适用场景ID", "applicable_scenes", ["SCENE_A_001", "SCENE_B_001", "SCENE_C_001", "SCENE_D_001", "SCENE_E_001"]),
    multi("适用镜头策略", "applicable_shot_profiles", ["全身固定", "上半身固定", "上半身至大腿固定", "上半身缓慢推近"]),
    field("动作步骤", "action_steps"),
    single("动作幅度", "movement_level", ["极小", "小", "中"]),
    single("身体转动角度", "body_rotation", ["0度", "15度", "30度", "45度"]),
    multi("空闲手动作", "free_hand_action", ["自然下垂", "扶腰", "插兜", "整理衣摆", "触碰领口", "触碰袖口", "轻扶裤腰"]),
    single("是否允许前后移动", "forward_movement", ["不允许", "半步以内"]),
    single("建议动作时长", "duration_suggestion", ["3秒", "4秒", "5秒", "6秒"]),
    single("动作节奏", "movement_speed", ["缓慢", "自然偏慢", "自然"]),
    single("生成风险", "risk_level", ["低", "中", "高"]),
    field("动作核心描述", "prompt_core"),
    field("动作禁止项", "prompt_negative"),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


SHOT_NAMES = ["全身固定", "上半身固定", "上半身至大腿固定", "上半身缓慢推近"]

SHOT_PLAN_FIELDS = (
    field("镜头方案ID", "shot_plan_id"),
    field("镜头方案名称", "shot_plan_name"),
    single("启用状态", "status", STATUS_OPTIONS),
    multi("适用类目", "applicable_categories", CATEGORIES),
    single("生成1条时镜头", "single_sequence", SHOT_NAMES),
    single("第1条镜头", "shot_1", SHOT_NAMES),
    single("第2条镜头", "shot_2", SHOT_NAMES),
    single("第3条镜头", "shot_3", SHOT_NAMES),
    single("第4条镜头", "shot_4", SHOT_NAMES),
    single("第5条镜头", "shot_5", SHOT_NAMES),
    field("非标准条数循环", "fallback_cycle"),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


STYLING_FIELDS = (
    field("搭配模板ID", "styling_id"),
    field("搭配名称", "styling_name"),
    single("启用状态", "status", STATUS_OPTIONS),
    multi("适用商品类型", "applicable_product_type", CATEGORIES + ["短上衣", "宽松上衣"]),
    multi("商品版型要求", "product_fit", ["修身", "合体", "短款", "宽松", "不限"]),
    single("下装类型", "bottom_type", ["高腰阔腿裤", "直筒牛仔裤", "白色短裤", "休闲短裤", "半裙", "同色套装下装"]),
    multi("下装颜色", "bottom_color", ["白色", "黑色", "浅灰", "米色", "卡其色", "蓝色牛仔", "同色系"]),
    multi("下装版型", "bottom_fit", ["高腰", "直筒", "阔腿", "宽松", "修身"]),
    field("内搭类型", "inner_type"),
    field("内搭颜色", "inner_color"),
    field("内搭补充要求", "inner_requirements"),
    multi("穿搭风格", "vibe_tag", ["日常干净", "轻通勤", "温柔", "夏日休闲", "居家轻松", "少女感"]),
    single("配饰程度", "accessory_level", ["无配饰", "轻量配饰", "正常配饰"]),
    single("鞋子展示要求", "footwear_visibility", ["不要求入镜", "可以入镜", "必须入镜"]),
    field("适合颜色方向", "suitable_color_rules"),
    field("禁止搭配", "forbidden_pairings"),
    field("搭配核心描述", "prompt_core"),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


SUBTITLE_FIELDS = (
    field("字幕模板ID", "subtitle_id"),
    field("字幕模板名称", "subtitle_name"),
    single("启用状态", "status", STATUS_OPTIONS),
    multi("适用市场", "markets", MARKETS),
    single("字幕语言", "language", ["泰语", "越南语", "马来语", "西班牙语", "英语", "中文"]),
    single("字幕类型", "subtitle_type", ["单点评价", "版型判断", "舒适感", "搭配建议", "轻推荐"]),
    multi("卖点方向", "selling_point_angle", ["显瘦", "显高", "显腰", "长度合适", "舒适", "轻薄", "面料", "百搭", "通勤", "日常", "清爽"]),
    multi("适用类目", "applicable_category", CATEGORIES),
    field("开场字幕", "opening_text"),
    field("中段字幕", "middle_text"),
    field("结尾字幕", "ending_text"),
    single("字幕出现方式", "display_mode", ["开场一句", "结尾一句", "开场加结尾", "全程一行"]),
    single("字幕语气", "tone", ["自然分享", "朋友式", "轻推荐", "克制评价"]),
    field("最大字数", "char_limit", NUMBER),
    single("是否允许AI改写", "allow_ai_rewrite", ["允许", "不允许"]),
    single("是否需要中文对照", "need_chinese_translation", ["需要", "不需要"]),
    field("配置版本", "config_version"),
    field("使用优先级", "priority", NUMBER),
    *COMMON_SYNC_FIELDS,
)


REVIEW_FIELDS = (
    field("视频任务ID", "job_id"),
    field("脚本创建时间", "job_created_at", DATETIME),
    field("视觉方案ID", "visual_plan_id"),
    field("商品ID", "product_id"),
    field("商品名称", "product_name"),
    field("商品参考图URL", "product_images"),
    field("确认穿搭图URL", "outfit_image_url", URL),
    field("确认穿搭图路径", "outfit_image_path"),
    field("穿搭图版本", "outfit_image_version"),
    field("适用账号", "account_id"),
    single("市场", "market", MARKETS),
    field("视频变体编号", "variant_no", NUMBER),
    field("视觉身份ID", "persona_id"),
    field("场景模板ID", "scene_id"),
    field("镜头方案", "shot_plan_id"),
    field("镜头策略", "shot_profile_id"),
    field("动作模板ID", "action_id"),
    field("搭配模板ID", "styling_id"),
    field("字幕模板ID", "subtitle_id"),
    single("生成渠道", "generation_channel", ["不生成", "自动", "即梦", "iMini"]),
    single("生成模型", "generation_model", ["Seedance 2.0", "Seedance 2.0 VIP"]),
    field("视频时长", "duration_seconds", NUMBER),
    field("重新提交生成", "generation_rerun", CHECKBOX),
    field("Prompt版本", "prompt_version"),
    field("模板版本快照", "template_versions"),
    field("完整Prompt", "prompt_payload"),
    field("初始成片", "raw_video_attachments", ATTACHMENT),
    field("最终视频", "final_video_attachments", ATTACHMENT),
    single("生成状态", "generation_status", ["待生成", "生成中", "生成成功", "生成失败", "重试中"]),
    field("生成失败原因", "generation_error"),
    field("运行表记录ID", "run_manager_record_id"),
    single("队列同步状态", "run_manager_sync_status", ["未提交", "待入队", "已入队", "生成中", "已回流", "失败", "阻塞"]),
    field("队列错误信息", "run_manager_sync_error"),
    field("最新追踪ID", "run_manager_trace_id"),
    field("成片视频URL", "output_video"),
    field("视频封面URL", "output_cover"),
    single("机器质检结果", "machine_qc_status", ["待质检", "通过", "需人工复核", "不通过"]),
    field("场景一致性分", "scene_score", NUMBER),
    field("人物一致性分", "persona_score", NUMBER),
    field("服装还原分", "clothing_score", NUMBER),
    field("动作自然度分", "motion_score", NUMBER),
    field("真实感评分", "realism_score", NUMBER),
    field("综合评分", "overall_score", NUMBER),
    multi("异常类型", "abnormal_types", ["场景漂移", "人设漂移", "手机未遮脸", "服装失真", "肢体异常", "动作异常", "构图异常", "光线异常", "字幕异常", "时长异常", "其他"]),
    field("机器质检说明", "machine_qc_notes"),
    single("人工复核状态", "manual_review_status", ["待复核", "通过", "打回", "淘汰"]),
    field("人工复核原因", "manual_review_reason"),
    single("是否需要补生成", "need_regeneration", ["是", "否"]),
    single("补生成策略", "regeneration_strategy", ["原配置重试", "更换动作", "更换场景", "更换搭配", "改写Prompt", "人工处理"]),
    field("父任务ID", "parent_job_id"),
    field("补生成任务ID", "regeneration_job_id"),
    field("复核版本", "review_version", NUMBER),
    field("复核处理时间", "review_processed_at", DATETIME),
    single("发布状态", "publish_status", ["未排期", "待发布", "已发布", "暂停发布"]),
    field("发布时间", "published_at", DATETIME),
    field("发布链接", "publish_url", URL),
    field("播放量", "views", NUMBER),
    field("商品点击量", "product_clicks", NUMBER),
    field("成交金额", "gmv", NUMBER),
    field("数据回收时间", "metrics_updated_at", DATETIME),
    single("数据同步状态", "sync_status", SYNC_STATUS_OPTIONS),
    field("最后同步时间", "last_synced_at", DATETIME),
    field("同步错误信息", "sync_error"),
    field("运营备注", "operator_notes"),
    field("飞书记录创建时间", "created_at", CREATED_TIME),
    field("最后修改时间", "feishu_updated_at", MODIFIED_TIME),
)


VISUAL_PLAN_FIELDS = (
    field("视觉方案ID", "visual_plan_id"),
    field("原始脚本记录ID", "source_record_id"),
    field("产品编码", "product_code"),
    field("产品名称", "product_name"),
    field("产品参考图", "product_images"),
    field("场景名称", "scene_name"),
    field("场景模板ID", "scene_id"),
    field("搭配名称", "styling_name"),
    field("搭配模板ID", "styling_id"),
    field("实际下装颜色", "resolved_bottom_color"),
    field("实际下装版型", "resolved_bottom_fit"),
    field("实际内搭类型", "resolved_inner_type"),
    field("实际内搭颜色", "resolved_inner_color"),
    field("外套开合规则", "resolved_outerwear_state"),
    field("实际背景类型", "resolved_background_type"),
    field("实际边缘装饰", "resolved_edge_decor"),
    field("实际主光方向", "resolved_key_light_direction"),
    field("视觉身份ID", "persona_id"),
    field("产品穿搭图URL", "outfit_image_url", URL),
    field("产品穿搭图", "outfit_image_attachments", ATTACHMENT),
    field("产品穿搭图路径", "outfit_image_path"),
    single("穿搭图状态", "outfit_image_status", ["待生成", "生成中", "待确认", "已确认", "重新生成", "生成失败"]),
    field("复核反馈", "operator_feedback"),
    field("每方案视频数量", "per_plan_video_count", NUMBER),
    single("视觉方案状态", "plan_status", ["启用", "已替代", "不生成", "失败"]),
    field("视频任务ID", "job_ids"),
    field("方案版本", "plan_version", NUMBER),
    field("方案指纹", "plan_fingerprint"),
    field("穿搭图版本", "outfit_image_version"),
    field("错误信息", "error_message"),
    single("同步状态", "sync_status", SYNC_STATUS_OPTIONS),
    field("最后同步时间", "last_synced_at", DATETIME),
    field("同步错误信息", "sync_error"),
    field("确认时间", "confirmed_at", DATETIME),
    field("替代时间", "superseded_at", DATETIME),
    field("创建时间", "created_at", CREATED_TIME),
    field("最后修改时间", "feishu_updated_at", MODIFIED_TIME),
)


TABLE_MAPPINGS: dict[str, TableMapping] = {
    "persona": TableMapping("persona", "账号视觉身份表", "视觉身份ID", "persona_id", PERSONA_FIELDS, ("视觉身份配置", "待同步配置")),
    "scene": TableMapping("scene", "场景环境库", "场景模板ID", "scene_id", SCENE_FIELDS, ("环境配置", "待同步配置")),
    "action": TableMapping("action", "动作模板库", "动作模板ID", "action_id", ACTION_FIELDS, ("动作模板配置", "待同步配置")),
    "shot_plan": TableMapping("shot_plan", "镜头方案库", "镜头方案ID", "shot_plan_id", SHOT_PLAN_FIELDS, ("镜头方案配置", "待同步配置")),
    "styling": TableMapping("styling", "搭配模板库", "搭配模板ID", "styling_id", STYLING_FIELDS, ("搭配模板配置", "待同步配置")),
    "subtitle": TableMapping("subtitle", "字幕模板库", "字幕模板ID", "subtitle_id", SUBTITLE_FIELDS, ("字幕模板配置", "待同步配置")),
    "review": TableMapping("review", "轻量视频任务复核台", "视频任务ID", "job_id", REVIEW_FIELDS, ("任务复核台", "待人工复核", "需补生成", "可发布")),
    "visual_plan": TableMapping("visual_plan", "产品视觉方案表", "视觉方案ID", "visual_plan_id", VISUAL_PLAN_FIELDS, ("视觉方案", "待生成穿搭图", "待确认穿搭图", "已确认待生成视频")),
}


# 原始脚本汇总表只作为轻量视频触发入口；这些字段不会加入原创脚本的任务状态映射。
SOURCE_SCRIPT_INPUT_FIELDS: dict[str, tuple[str, ...]] = {
    "request": ("每方案视频数量", "轻量视频生成数量", "是否跑轻模型"),
    "product_images": ("产品图片", "商品图片", "主图"),
    "product_code": ("产品编码", "商品编码", "SKU编码", "SKU", "产品ID"),
    "product_name": ("产品标题", "产品名称", "商品名称"),
    "top_category": ("一级类目", "大类目"),
    "target_country": ("目标国家", "国家"),
    "target_language": ("目标语言", "语言"),
    "product_type": ("产品类型", "商品类型", "品类"),
    "selling_points": ("产品卖点说明", "产品参数信息"),
    "account_id": ("店铺ID", "账号ID", "店铺", "适用账号"),
    "scene_preference": ("轻量视频场景",),
    "styling_preference": ("轻量视频搭配",),
    "action_preference": ("轻量视频动作",),
    "shot_plan_preference": ("轻量视频镜头方案",),
}

SOURCE_SCRIPT_TRIGGER_FIELDS = (
    single("每方案视频数量", "light_video_request", ["不生成", "每方案 1 个", "每方案 5 个"]),
    multi("轻量视频场景", "light_video_scene", ["自动选择", "现代简约卧室", "明亮现代咖啡店"]),
    multi("轻量视频搭配", "light_video_styling", ["自动选择", "白色高腰阔腿裤", "经典蓝色直筒牛仔裤", "白色高腰短裤", "纯色休闲短裤", "简洁半裙", "保持商品原套装"]),
    single("轻量视频镜头方案", "light_video_shot_plan", ["自动选择", "外套重点展示", "上装平衡展示", "全身穿搭展示", "通用平衡展示"]),
    multi("轻量视频动作", "light_video_action", ["自动选择"]),
    field("预计视觉方案数", "estimated_visual_plan_count", NUMBER),
    field("预计视频总数", "estimated_video_count", NUMBER),
    field("视觉方案ID", "visual_plan_ids"),
    single("轻量视频状态", "light_video_status", ["未触发", "待编排", "待首帧生成", "待首帧确认", "生成中", "待复核", "已完成", "失败"]),
    field("轻量视频任务ID", "light_video_job_ids"),
    field("轻量视频错误信息", "light_video_error"),
    field("轻量视频最近触发时间", "light_video_last_triggered_at", DATETIME),
)


STATUS_TO_BACKEND = {"启用": "enabled", "测试": "testing", "停用": "disabled"}
STATUS_TO_FEISHU = {value: key for key, value in STATUS_TO_BACKEND.items()}
MARKET_TO_BACKEND = {"泰国": "TH", "越南": "VN", "马来西亚": "MY", "墨西哥": "MX", "其他": "OTHER"}
MARKET_TO_FEISHU = {value: key for key, value in MARKET_TO_BACKEND.items()}
LANGUAGE_TO_BACKEND = {"泰语": "th", "越南语": "vi", "马来语": "ms", "西班牙语": "es", "英语": "en", "中文": "zh"}
LANGUAGE_TO_FEISHU = {value: key for key, value in LANGUAGE_TO_BACKEND.items()}
SCENE_TYPE_TO_BACKEND = {"环境模板": "environment", "主场景全身": "main_full_body", "同场景半身": "half_body_detail", "轻变化全身": "variation_full_body", "高亮上半身固定": "upper_body_fixed", "缓慢推近": "slow_push_in"}
ACTION_TYPE_TO_BACKEND = {"基础站立": "basic_stand", "整理衣摆": "adjust_hem", "轻侧身": "side_turn", "触碰领口袖口": "touch_collar", "扶腰插兜": "hand_in_pocket", "半步前移": "half_step_forward", "上装复合微动作": "upper_detail_combo", "外套复合微动作": "outerwear_detail_combo", "轮廓复合微动作": "silhouette_combo", "自定义动作": "custom"}
CAMERA_MOTION_TO_BACKEND = {"固定": "fixed", "缓慢推近": "push_in"}
RISK_TO_BACKEND = {"低": "low", "中": "medium", "高": "high"}
CATEGORY_TO_BACKEND = {"T恤": "tshirt", "针织": "knit_top", "背心": "tank_top", "吊带": "tank_top", "衬衫": "shirt", "外套": "outerwear", "裤装": "pants", "裙装": "skirt", "连衣裙": "dress", "套装": "set", "短上衣": "top", "宽松上衣": "top", "连体裤": "jumpsuit", "家居服": "homewear"}


ENUM_MAPS_TO_BACKEND: dict[str, dict[str, str]] = {
    "status": STATUS_TO_BACKEND,
    "market": MARKET_TO_BACKEND,
    "language": LANGUAGE_TO_BACKEND,
    "scene_type": SCENE_TYPE_TO_BACKEND,
    "action_type": ACTION_TYPE_TO_BACKEND,
    "camera_motion": CAMERA_MOTION_TO_BACKEND,
    "risk_level": RISK_TO_BACKEND,
}


ENUM_MAPS_TO_FEISHU: dict[str, dict[str, str]] = {
    key: {backend: feishu for feishu, backend in mapping.items()}
    for key, mapping in ENUM_MAPS_TO_BACKEND.items()
}
