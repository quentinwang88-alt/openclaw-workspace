"""
达人建联 CRM 系统 - 核心数据字典与受控词表

本模块定义了系统中所有标准化标签、数据结构和类型约束。
所有 AI 输出和飞书 API 交互必须严格遵守此字典，防止幻觉和数据不一致。
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator


# ============================================================================
# 第一部分：标准化标签字典 (Controlled Vocabulary Enums)
# ============================================================================

class ApparelStyle(str, Enum):
    """女装风格受控词表"""
    Y2K_SPICY = "Y2K_Spicy"
    MINIMALIST = "Minimalist"
    SWEET = "Sweet"
    STREETWEAR = "Streetwear"
    ELEGANT = "Elegant"
    VACATION = "Vacation"


class AccessoryStyle(str, Enum):
    """配饰风格受控词表"""
    DAINTY_MINIMALIST = "Dainty_Minimalist"
    STATEMENT_CHUNKY = "Statement_Chunky"
    BLING_SPARKLE = "Bling_Sparkle"
    VINTAGE_PEARL = "Vintage_Pearl"
    BOHO_COLORFUL = "Boho_Colorful"
    SWEET_KAWAII = "Sweet_Kawaii"


class PreferredCategory(str, Enum):
    """品类偏好受控词表"""
    APPAREL_TOP = "Apparel_Top"
    APPAREL_DRESS = "Apparel_Dress"
    ACCESSORIES_NECK_EAR = "Accessories_Neck_Ear"
    ACCESSORIES_BAG_HAND = "Accessories_Bag_Hand"
    MIXED_FASHION = "Mixed_Fashion"


class TargetMarket(str, Enum):
    """目标市场"""
    THAILAND = "TH"
    VIETNAM = "VN"


class CampaignStatus(str, Enum):
    """货盘组状态"""
    ACTIVE = "Active"
    INACTIVE = "Inactive"


class CommStatus(str, Enum):
    """沟通状态"""
    PENDING = "待触达"
    SENT = "已发送"
    NEGOTIATING = "谈判中"
    AWAITING_SAMPLE = "待寄样"
    ORDERED = "已出单"


class IntentLevel(str, Enum):
    """意图等级（三色灯）"""
    GREEN = "绿灯"  # 顺畅，可自动化
    YELLOW = "黄灯"  # 商业谈判，需人工审批
    RED = "红灯"  # 异常，人工接管


class ContactPlatform(str, Enum):
    """联系平台"""
    LINE = "Line"
    ZALO = "Zalo"
    EMAIL = "Email"
    NONE = "None"


class InventoryStatus(str, Enum):
    """库存状态"""
    IN_STOCK = "In_Stock"
    OUT_OF_STOCK = "Out_of_Stock"


class ProductCategory(str, Enum):
    """产品类别"""
    APPAREL = "Apparel"
    ACCESSORIES = "Accessories"


# ============================================================================
# 第二部分：跨类目风格映射表 (Cross-Category Style Mapping)
# ============================================================================

STYLE_MAPPING: Dict[ApparelStyle, List[AccessoryStyle]] = {
    ApparelStyle.Y2K_SPICY: [
        AccessoryStyle.STATEMENT_CHUNKY,
        AccessoryStyle.BLING_SPARKLE
    ],
    ApparelStyle.STREETWEAR: [
        AccessoryStyle.STATEMENT_CHUNKY,
        AccessoryStyle.BLING_SPARKLE
    ],
    ApparelStyle.MINIMALIST: [
        AccessoryStyle.DAINTY_MINIMALIST
    ],
    ApparelStyle.ELEGANT: [
        AccessoryStyle.VINTAGE_PEARL
    ],
    ApparelStyle.SWEET: [
        AccessoryStyle.SWEET_KAWAII
    ],
    ApparelStyle.VACATION: [
        AccessoryStyle.BOHO_COLORFUL
    ]
}


# ============================================================================
# 第三部分：飞书核心数据表字段结构 (Pydantic Models)
# ============================================================================

class CampaignGroup(BaseModel):
    """表 A：货盘组 / 招商计划表"""
    group_id: str = Field(..., description="主键，例: CP-26-Summer-Y2K")
    group_name: str = Field(..., description="货盘名称，如'26年夏装辣妹测款组'")
    store_id: str = Field(..., description="所属店铺标识 (TH_Acc_01, VN_Apparel)")
    target_market: TargetMarket = Field(..., description="目标市场 TH 或 VN")
    group_style_tags: List[str] = Field(..., description="该货盘的核心风格（多选）")
    is_universal: bool = Field(False, description="是否为无门槛百搭兜底组")
    lookbook_url: str = Field(..., description="供达人挑选产品的精美图册/落地页链接")
    base_commission: float = Field(..., description="该组默认底线佣金比例 (如 0.15)")
    status: CampaignStatus = Field(CampaignStatus.ACTIVE, description="Active 或 Inactive")

    @validator('group_style_tags')
    def validate_style_tags(cls, v):
        """验证风格标签必须来自受控词表"""
        valid_styles = set([s.value for s in ApparelStyle] + [s.value for s in AccessoryStyle])
        for tag in v:
            if tag not in valid_styles:
                raise ValueError(f"Invalid style tag: {tag}. Must be from controlled vocabulary.")
        return v

    class Config:
        use_enum_values = True


class ProductCatalog(BaseModel):
    """表 B：商品明细库（用于 RAG 知识库）"""
    sku_id: str = Field(..., description="产品唯一标识")
    belongs_to_group_id: str = Field(..., description="关联到表 A 的 group_id")
    product_name: str = Field(..., description="内部产品名称")
    category: ProductCategory = Field(..., description="Apparel 或 Accessories")
    selling_points: str = Field(..., description="核心卖点与材质说明（用于 AI 生成回答）")
    image_url: str = Field(..., description="商品主图链接")
    inventory_status: InventoryStatus = Field(InventoryStatus.IN_STOCK, description="库存状态")

    class Config:
        use_enum_values = True


class CreatorCRM(BaseModel):
    """表 C：达人数字资产 CRM 库"""
    creator_id: str = Field(..., description="系统自增 ID")
    tk_handle: str = Field(..., description="TikTok 纯数字/字母账号名 (@lalala)")
    tk_profile_url: str = Field("", description="TikTok 达人主页完整 URL (https://www.tiktok.com/@username)")
    contact_platform: ContactPlatform = Field(ContactPlatform.NONE, description="Line / Zalo / Email / None")
    contact_account: str = Field("", description="具体的社交账号 ID")
    followers_count: int = Field(0, description="粉丝数量级")
    ai_apparel_style: Optional[ApparelStyle] = Field(None, description="视觉解析写入的女装风格")
    ai_accessory_style: Optional[AccessoryStyle] = Field(None, description="视觉解析写入的配饰风格")
    matched_group_id: str = Field("", description="AI 分配的最佳货盘组（关联表 A）")
    comm_status: CommStatus = Field(CommStatus.PENDING, description="沟通状态")
    intent_level: Optional[IntentLevel] = Field(None, description="绿灯 / 黄灯 / 红灯")
    promo_code: str = Field("", description="专属折扣码/追踪标识")
    history_cost_rmb: float = Field(0.0, description="历史合作成本累计（样品费+佣金）")

    class Config:
        use_enum_values = True


# ============================================================================
# 第四部分：Skill 输入输出数据结构
# ============================================================================

class CreatorVibeAnalysis(BaseModel):
    """Skill 1 输出：达人风格分析结果"""
    tk_handle: str
    ai_apparel_style: ApparelStyle
    ai_accessory_style: AccessoryStyle
    preferred_category: PreferredCategory
    analysis_reason: str = Field(..., description="打标理由（用于生成邀约信）")
    bio: str = Field("", description="达人简介")
    followers_count: int = Field(0, description="粉丝数")
    
    class Config:
        use_enum_values = True


class CampaignAssignment(BaseModel):
    """Skill 2 输出：货盘分配结果"""
    assigned_group_id: str
    assigned_group_name: str
    lookbook_url: str
    match_score: int = Field(..., description="匹配得分")
    match_reason: str = Field(..., description="匹配理由")


class LocalizedPitch(BaseModel):
    """Skill 3 输出：本地化邀约信"""
    message_text: str = Field(..., description="邀约信正文（纯文本）")
    language: str = Field(..., description="语言代码 (th/vi)")
    creator_handle: str


class IntentRouting(BaseModel):
    """Skill 4 输出：意图路由结果"""
    intent_level: IntentLevel
    summary_for_human: str = Field(..., description="给人工的摘要说明")
    suggested_reply: Optional[str] = Field(None, description="建议回复（仅 GREEN 时提供）")
    extracted_info: Dict[str, Any] = Field(default_factory=dict, description="提取的结构化信息")
    
    class Config:
        use_enum_values = True


# ============================================================================
# 第五部分：辅助函数
# ============================================================================

def get_mapped_accessory_styles(apparel_style: ApparelStyle) -> List[AccessoryStyle]:
    """
    根据女装风格获取映射的配饰风格
    
    Args:
        apparel_style: 女装风格
        
    Returns:
        对应的配饰风格列表
    """
    return STYLE_MAPPING.get(apparel_style, [])


def validate_style_compatibility(
    apparel_style: ApparelStyle,
    accessory_style: AccessoryStyle
) -> bool:
    """
    验证女装风格与配饰风格是否兼容
    
    Args:
        apparel_style: 女装风格
        accessory_style: 配饰风格
        
    Returns:
        是否兼容
    """
    mapped_styles = get_mapped_accessory_styles(apparel_style)
    return accessory_style in mapped_styles


def get_all_style_tags() -> List[str]:
    """获取所有有效的风格标签（用于验证）"""
    return [s.value for s in ApparelStyle] + [s.value for s in AccessoryStyle]


# ============================================================================
# 第六部分：示例数据（用于测试）
# ============================================================================

EXAMPLE_CAMPAIGN_GROUP = CampaignGroup(
    group_id="CP-26-Summer-Y2K",
    group_name="26年夏装辣妹测款组",
    store_id="TH_Acc_01",
    target_market=TargetMarket.THAILAND,
    group_style_tags=["Y2K_Spicy", "Streetwear"],
    is_universal=False,
    lookbook_url="https://example.com/lookbook/summer-y2k",
    base_commission=0.15,
    status=CampaignStatus.ACTIVE
)

EXAMPLE_PRODUCT = ProductCatalog(
    sku_id="TH-DR-8801",
    belongs_to_group_id="CP-26-Summer-Y2K",
    product_name="Y2K 辣妹短款上衣",
    category=ProductCategory.APPAREL,
    selling_points="95%棉质，修身剪裁，适合夏季穿搭，多色可选",
    image_url="https://example.com/products/TH-DR-8801.jpg",
    inventory_status=InventoryStatus.IN_STOCK
)

EXAMPLE_CREATOR = CreatorCRM(
    creator_id="CR-2026-001",
    tk_handle="@fashionista_th",
    contact_platform=ContactPlatform.LINE,
    contact_account="line_id_12345",
    followers_count=50000,
    ai_apparel_style=ApparelStyle.Y2K_SPICY,
    ai_accessory_style=AccessoryStyle.STATEMENT_CHUNKY,
    matched_group_id="CP-26-Summer-Y2K",
    comm_status=CommStatus.PENDING,
    intent_level=None,
    promo_code="FASHION2026",
    history_cost_rmb=0.0
)


if __name__ == "__main__":
    # 测试数据验证
    print("✅ 数据字典加载成功")
    print(f"女装风格: {[s.value for s in ApparelStyle]}")
    print(f"配饰风格: {[s.value for s in AccessoryStyle]}")
    print(f"\n风格映射示例:")
    print(f"Y2K_Spicy -> {[s.value for s in get_mapped_accessory_styles(ApparelStyle.Y2K_SPICY)]}")
    print(f"\n示例货盘组: {EXAMPLE_CAMPAIGN_GROUP.group_name}")
    print(f"示例达人: {EXAMPLE_CREATOR.tk_handle}")
