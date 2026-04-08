"""
Skill 2: assign_campaign_group - 跨类目货盘软匹配算法

功能：采用积分制，为达人分配最契合的货盘组 (group_id)
策略：直接命中 + 跨类目映射 + 兜底机制
"""

from typing import List, Dict, Optional, Tuple
import sys
import os

# 添加父目录到路径以支持导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import (
    CampaignGroup,
    CreatorVibeAnalysis,
    CampaignAssignment,
    ApparelStyle,
    AccessoryStyle,
    TargetMarket,
    CampaignStatus,
    get_mapped_accessory_styles,
)


def assign_campaign_group(
    creator_tags: Dict[str, str],
    market: str,
    active_campaigns: List[Dict]
) -> CampaignAssignment:
    """
    为达人分配最契合的货盘组
    
    Args:
        creator_tags: 达人标签字典，包含:
            - ai_apparel_style: 女装风格
            - ai_accessory_style: 配饰风格
            - preferred_category: 品类偏好
        market: 目标市场 ("TH" 或 "VN")
        active_campaigns: 活跃货盘组列表（来自飞书表 A）
        
    Returns:
        CampaignAssignment: 分配结果，包含 group_id, lookbook_url 等
        
    Raises:
        ValueError: 当输入参数无效时
        RuntimeError: 当无法找到合适的货盘组时
    """
    # 参数验证
    if not creator_tags:
        raise ValueError("creator_tags 不能为空")
    
    if market not in ["TH", "VN"]:
        raise ValueError(f"无效的市场代码: {market}，必须是 'TH' 或 'VN'")
    
    if not active_campaigns:
        raise ValueError("active_campaigns 不能为空")
    
    # 提取达人风格标签
    try:
        creator_apparel = ApparelStyle(creator_tags.get("ai_apparel_style", ""))
        creator_accessory = AccessoryStyle(creator_tags.get("ai_accessory_style", ""))
    except ValueError as e:
        raise ValueError(f"无效的风格标签: {e}")
    
    # 过滤符合市场且状态为 Active 的货盘组
    eligible_campaigns = [
        c for c in active_campaigns
        if c.get("target_market") == market and c.get("status") == CampaignStatus.ACTIVE.value
    ]
    
    if not eligible_campaigns:
        raise RuntimeError(f"未找到市场 {market} 的活跃货盘组")
    
    # 计算每个货盘组的匹配得分
    scored_campaigns: List[Tuple[Dict, int, str]] = []
    
    for campaign in eligible_campaigns:
        score, reason = _calculate_match_score(
            campaign=campaign,
            creator_apparel=creator_apparel,
            creator_accessory=creator_accessory
        )
        scored_campaigns.append((campaign, score, reason))
    
    # 按得分降序排序
    scored_campaigns.sort(key=lambda x: x[1], reverse=True)
    
    # 获取最高分货盘组
    best_campaign, best_score, match_reason = scored_campaigns[0]
    
    # 如果所有普通货盘得分为 0，尝试分配兜底组
    if best_score == 0:
        fallback_campaign = _find_fallback_campaign(eligible_campaigns)
        if fallback_campaign:
            return CampaignAssignment(
                assigned_group_id=fallback_campaign["group_id"],
                assigned_group_name=fallback_campaign["group_name"],
                lookbook_url=fallback_campaign["lookbook_url"],
                match_score=0,
                match_reason="未找到直接匹配的货盘组，分配到百搭兜底组"
            )
        else:
            raise RuntimeError(f"无法为达人找到合适的货盘组（市场: {market}）")
    
    # 返回最佳匹配结果
    return CampaignAssignment(
        assigned_group_id=best_campaign["group_id"],
        assigned_group_name=best_campaign["group_name"],
        lookbook_url=best_campaign["lookbook_url"],
        match_score=best_score,
        match_reason=match_reason
    )


def _calculate_match_score(
    campaign: Dict,
    creator_apparel: ApparelStyle,
    creator_accessory: AccessoryStyle
) -> Tuple[int, str]:
    """
    计算货盘组与达人的匹配得分
    
    Args:
        campaign: 货盘组数据
        creator_apparel: 达人女装风格
        creator_accessory: 达人配饰风格
        
    Returns:
        (得分, 匹配理由)
    """
    # 跳过兜底组（单独处理）
    if campaign.get("is_universal", False):
        return 0, ""
    
    score = 0
    reasons = []
    
    campaign_tags = campaign.get("group_style_tags", [])
    
    # 规则 1: 直接命中 (+10分)
    # 货盘的风格标签与达人的任一风格标签一致
    if creator_apparel.value in campaign_tags:
        score += 10
        reasons.append(f"女装风格直接命中: {creator_apparel.value}")
    
    if creator_accessory.value in campaign_tags:
        score += 10
        reasons.append(f"配饰风格直接命中: {creator_accessory.value}")
    
    # 规则 2: 跨类目映射命中 (+8分)
    # 若货盘主推配饰，达人的女装风格符合映射关系
    mapped_accessories = get_mapped_accessory_styles(creator_apparel)
    
    for tag in campaign_tags:
        try:
            # 检查货盘标签是否为配饰风格
            tag_accessory = AccessoryStyle(tag)
            # 检查是否在映射列表中
            if tag_accessory in mapped_accessories:
                score += 8
                reasons.append(
                    f"跨类目映射命中: 女装风格 {creator_apparel.value} "
                    f"映射到配饰风格 {tag_accessory.value}"
                )
        except ValueError:
            # 不是配饰风格标签，跳过
            continue
    
    # 组装匹配理由
    if reasons:
        match_reason = " | ".join(reasons)
    else:
        match_reason = "无匹配"
    
    return score, match_reason


def _find_fallback_campaign(campaigns: List[Dict]) -> Optional[Dict]:
    """
    查找兜底货盘组（is_universal = True）
    
    Args:
        campaigns: 货盘组列表
        
    Returns:
        兜底货盘组，如果不存在则返回 None
    """
    for campaign in campaigns:
        if campaign.get("is_universal", False):
            return campaign
    return None


# ============================================================================
# 测试与示例
# ============================================================================

def _test_assign_campaign_group():
    """测试货盘匹配算法"""
    
    # 模拟货盘组数据
    test_campaigns = [
        {
            "group_id": "CP-26-Summer-Y2K",
            "group_name": "26年夏装辣妹测款组",
            "store_id": "TH_Acc_01",
            "target_market": "TH",
            "group_style_tags": ["Y2K_Spicy", "Streetwear"],
            "is_universal": False,
            "lookbook_url": "https://example.com/lookbook/summer-y2k",
            "base_commission": 0.15,
            "status": "Active"
        },
        {
            "group_id": "CP-26-Minimalist-Acc",
            "group_name": "极简配饰系列",
            "store_id": "TH_Acc_02",
            "target_market": "TH",
            "group_style_tags": ["Dainty_Minimalist"],
            "is_universal": False,
            "lookbook_url": "https://example.com/lookbook/minimalist",
            "base_commission": 0.12,
            "status": "Active"
        },
        {
            "group_id": "CP-26-Universal",
            "group_name": "百搭兜底组",
            "store_id": "TH_Universal",
            "target_market": "TH",
            "group_style_tags": [],
            "is_universal": True,
            "lookbook_url": "https://example.com/lookbook/universal",
            "base_commission": 0.10,
            "status": "Active"
        }
    ]
    
    # 测试用例 1: 直接命中
    print("=" * 60)
    print("测试用例 1: 直接命中 Y2K 风格")
    print("=" * 60)
    creator_tags_1 = {
        "ai_apparel_style": "Y2K_Spicy",
        "ai_accessory_style": "Statement_Chunky",
        "preferred_category": "Mixed_Fashion"
    }
    
    result_1 = assign_campaign_group(creator_tags_1, "TH", test_campaigns)
    print(f"分配结果: {result_1.assigned_group_name}")
    print(f"货盘 ID: {result_1.assigned_group_id}")
    print(f"匹配得分: {result_1.match_score}")
    print(f"匹配理由: {result_1.match_reason}")
    print()
    
    # 测试用例 2: 跨类目映射命中
    print("=" * 60)
    print("测试用例 2: 跨类目映射（Minimalist -> Dainty_Minimalist）")
    print("=" * 60)
    creator_tags_2 = {
        "ai_apparel_style": "Minimalist",
        "ai_accessory_style": "Dainty_Minimalist",
        "preferred_category": "Accessories_Neck_Ear"
    }
    
    result_2 = assign_campaign_group(creator_tags_2, "TH", test_campaigns)
    print(f"分配结果: {result_2.assigned_group_name}")
    print(f"货盘 ID: {result_2.assigned_group_id}")
    print(f"匹配得分: {result_2.match_score}")
    print(f"匹配理由: {result_2.match_reason}")
    print()
    
    # 测试用例 3: 兜底机制
    print("=" * 60)
    print("测试用例 3: 无匹配，触发兜底机制")
    print("=" * 60)
    creator_tags_3 = {
        "ai_apparel_style": "Vacation",
        "ai_accessory_style": "Boho_Colorful",
        "preferred_category": "Apparel_Dress"
    }
    
    result_3 = assign_campaign_group(creator_tags_3, "TH", test_campaigns)
    print(f"分配结果: {result_3.assigned_group_name}")
    print(f"货盘 ID: {result_3.assigned_group_id}")
    print(f"匹配得分: {result_3.match_score}")
    print(f"匹配理由: {result_3.match_reason}")
    print()


if __name__ == "__main__":
    print("🚀 Skill 2: assign_campaign_group 测试")
    print()
    _test_assign_campaign_group()
    print("✅ 测试完成")
