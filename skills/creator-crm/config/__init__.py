"""Creator CRM 配置模块"""

from .data_schema import (
    # Enums
    ApparelStyle,
    AccessoryStyle,
    PreferredCategory,
    TargetMarket,
    CampaignStatus,
    CommStatus,
    IntentLevel,
    ContactPlatform,
    InventoryStatus,
    ProductCategory,
    
    # Models
    CampaignGroup,
    ProductCatalog,
    CreatorCRM,
    CreatorVibeAnalysis,
    CampaignAssignment,
    LocalizedPitch,
    IntentRouting,
    
    # Functions
    get_mapped_accessory_styles,
    validate_style_compatibility,
    get_all_style_tags,
    
    # Constants
    STYLE_MAPPING,
)

__all__ = [
    # Enums
    "ApparelStyle",
    "AccessoryStyle",
    "PreferredCategory",
    "TargetMarket",
    "CampaignStatus",
    "CommStatus",
    "IntentLevel",
    "ContactPlatform",
    "InventoryStatus",
    "ProductCategory",
    
    # Models
    "CampaignGroup",
    "ProductCatalog",
    "CreatorCRM",
    "CreatorVibeAnalysis",
    "CampaignAssignment",
    "LocalizedPitch",
    "IntentRouting",
    
    # Functions
    "get_mapped_accessory_styles",
    "validate_style_compatibility",
    "get_all_style_tags",
    
    # Constants
    "STYLE_MAPPING",
]
