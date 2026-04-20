#!/usr/bin/env python3
"""
补货建议 Skill 适配器
使用适配器模式包装旧代码，使其符合新的 OpenClaw Skill 架构

设计原则：
- 不修改旧代码的核心逻辑
- 在 execute 方法中调用旧代码
- 将 Kimi 传来的参数正确映射并传递给旧代码

数据流：
1. 从 BigSeller API 获取当前库存和日均销量
2. 从飞书在途库存表格读取在途数据（支持多批次累加）
3. 计算建议采购量 = max(0, 日均销量 × 补货周期 - (当前库存 + 在途库存))
4. 只要建议采购量 > 0，就纳入补货清单；预警阈值仅用于紧急程度分层
5. 默认优先输出“建议采购 > 0”的实际补货 SKU，避免大量仅预警但无需采购的噪音
6. 按可售天数排序并输出报告

在途库存来源：
- 飞书多维表格：App Token=TiykbignraDkSOshIKNcfZ9vnlg, Table ID=tblbe4xbZQ56LS55
- 配置位置：inventory-alert/config/alert_config.json
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# 添加当前目录到路径，以便导入 base_skill
SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

# 添加 inventory-alert 目录到路径，以便导入旧代码
INVENTORY_ALERT_PATH = Path(__file__).parent / "inventory-alert"
if str(INVENTORY_ALERT_PATH) not in sys.path:
    sys.path.insert(0, str(INVENTORY_ALERT_PATH))

# 添加工作区根目录到路径，以便导入 generate_restock_report
WORKSPACE_PATH = Path(__file__).parent.parent
if str(WORKSPACE_PATH) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_PATH))

# 导入基础框架
from base_skill import BaseSkill, register_skill

# 导入旧代码
try:
    from alert import InventoryAlertAPI, InventoryAlert
except ImportError as e:
    print(f"⚠️ 无法导入旧代码 alert: {e}")
    InventoryAlertAPI = None
    InventoryAlert = None


def get_priority(days: int) -> tuple:
    """获取优先级"""
    if days <= 3:
        return ('🔴 极高', 1)
    elif days <= 7:
        return ('🟠 高', 2)
    elif days <= 10:
        return ('🟡 中', 3)
    else:
        return ('🟢 低', 4)


def generate_restock_report(
    purchase_cycle_days: int = 17,
    threshold_days: int = 10,
    output_format: str = "markdown",
    profile: Optional[str] = None,
    only_actionable: bool = True
) -> Dict[str, Any]:
    """
    生成补货建议报告（核心逻辑，从旧代码迁移）
    
    Args:
        purchase_cycle_days: 补货周期天数（默认17：采购3天 + 物流12天 + 安全2天）
        threshold_days: 预警阈值天数（默认10天）
        output_format: 输出格式，"markdown" 或 "json"
        profile: 指定账号 profile；不传时使用当前激活账号
        only_actionable: 是否只保留建议采购量大于 0 的 SKU，默认 True
    
    Returns:
        包含报告数据和内容的字典
    """
    if InventoryAlertAPI is None or InventoryAlert is None:
        return {
            "success": False,
            "error": "库存预警模块未正确加载",
            "data": None
        }
    
    # 初始化
    api = InventoryAlertAPI(profile=profile)
    alert_system = InventoryAlert(profile=profile)
    
    print("正在查询所有 SKU 库存...")
    all_skus = api.query_all_skus()
    print(f"共查询到 {len(all_skus)} 个 SKU")
    
    # 加载在途库存
    sku_title_map = {item['sku']: item.get('title', item['sku']) for item in all_skus}
    in_transit = alert_system.load_in_transit_inventory(sku_title_map)
    print(f"已加载 {len(in_transit)} 个 SKU 的在途库存")
    
    restock_list = []
    
    for sku_data in all_skus:
        sku = sku_data['sku']
        available = sku_data.get('available', 0)
        avg_sales = sku_data.get('avg_daily_sales', 0)
        purchase_sale_days = sku_data.get('purchase_sale_days', 0)
        
        # 获取在途库存
        in_transit_qty = in_transit.get(sku, 0)
        
        # 计算含在途的可售天数
        total_available = available + in_transit_qty
        days_with_transit = total_available / avg_sales if avg_sales > 0 else 999
        
        # 计算建议采购量
        target_qty = avg_sales * purchase_cycle_days
        suggested_qty = max(0, target_qty - total_available)

        # 补货清单口径：只要低于补货周期导致 suggested_qty > 0，就应该纳入
        # threshold_days 仅用于预警/优先级分层，不应拦截实际需要补货的 SKU
        if suggested_qty > 0 or purchase_sale_days <= threshold_days:
            restock_list.append({
                'sku': sku,
                'title': sku_data.get('title', ''),
                'available': available,
                'in_transit': in_transit_qty,
                'avg_daily_sales': avg_sales,
                'purchase_sale_days': purchase_sale_days,
                'days_with_transit': days_with_transit,
                'suggested_qty': int(suggested_qty),
                'priority': get_priority(purchase_sale_days)
            })
    
    # 按可售天数排序
    restock_list.sort(key=lambda x: x['purchase_sale_days'])

    warning_only_list = [item for item in restock_list if item['suggested_qty'] <= 0]
    actionable_list = [item for item in restock_list if item['suggested_qty'] > 0]
    output_list = actionable_list if only_actionable else restock_list
    
    return {
        "success": True,
        "restock_list": output_list,
        "all_restock_list": restock_list,
        "actionable_list": actionable_list,
        "warning_only_list": warning_only_list,
        "purchase_cycle_days": purchase_cycle_days,
        "threshold_days": threshold_days,
        "total_skus": len(all_skus),
        "restock_skus": len(output_list),
        "actionable_skus": len(actionable_list),
        "warning_only_skus": len(warning_only_list),
        "only_actionable": only_actionable
    }


def format_markdown_report(report_data: Dict[str, Any]) -> str:
    """生成 Markdown 格式的报告"""
    restock_list = report_data['restock_list']
    cycle_days = report_data['purchase_cycle_days']
    threshold_days = report_data['threshold_days']
    actionable_skus = report_data.get('actionable_skus', len([x for x in restock_list if x['suggested_qty'] > 0]))
    warning_only_skus = report_data.get('warning_only_skus', 0)
    only_actionable = report_data.get('only_actionable', False)
    
    now = datetime.now()
    
    md = f"""# 📦 补货建议报告

**生成时间**: {now.strftime('%Y-%m-%d %H:%M')}  
**补货周期**: {cycle_days}天（采购3天 + 物流12天 + 安全2天）  
**预警阈值**: {threshold_days}天可售  
**补货判定**: 总可售覆盖低于补货周期即计算建议采购量  
**报告口径**: {'仅展示建议采购 > 0 的实际补货SKU' if only_actionable else '展示全部预警SKU（含建议采购为0）'}

---
"""
    
    # 按优先级分组
    urgent = [x for x in restock_list if x['purchase_sale_days'] <= 3]
    high = [x for x in restock_list if 4 <= x['purchase_sale_days'] <= 7]
    medium = [x for x in restock_list if 8 <= x['purchase_sale_days'] <= threshold_days]
    routine = [x for x in restock_list if x['purchase_sale_days'] > threshold_days]
    
    # 紧急补货
    if urgent:
        md += "## 🚨 紧急补货（0-3天可售）\n\n"
        for i, item in enumerate(urgent, 1):
            md += _format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 高优先级
    if high:
        md += "## ⚠️ 高优先级补货（4-7天可售）\n\n"
        for i, item in enumerate(high, len(urgent) + 1):
            md += _format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 中优先级
    if medium:
        md += "## 📋 中优先级补货（8-10天可售）\n\n"
        for i, item in enumerate(medium, len(urgent) + len(high) + 1):
            md += _format_sku_item(i, item)
        md += "\n---\n\n"

    # 提前补货
    if routine:
        md += f"## 📦 提前补货（高于{threshold_days}天预警阈值，但低于补货周期）\n\n"
        for i, item in enumerate(routine, len(urgent) + len(high) + len(medium) + 1):
            md += _format_sku_item(i, item)
        md += "\n---\n\n"
    
    # 汇总
    md += "## 📊 补货汇总\n\n"
    md += f"- 实际需要补货 SKU: **{actionable_skus}** 个\n"
    md += f"- 仅预警但当前无需采购 SKU: **{warning_only_skus}** 个\n\n"
    md += "| 优先级 | SKU数量 | 建议采购总量 |\n"
    md += "|--------|---------|-------------|\n"
    
    if urgent:
        total_urgent = sum(x['suggested_qty'] for x in urgent)
        md += f"| 🔴 极高（0-3天） | {len(urgent)} | 约 {total_urgent:,} 件 |\n"
    
    if high:
        total_high = sum(x['suggested_qty'] for x in high)
        md += f"| 🟠 高（4-7天） | {len(high)} | 约 {total_high:,} 件 |\n"
    
    if medium:
        total_medium = sum(x['suggested_qty'] for x in medium)
        md += f"| 🟡 中（8-{threshold_days}天） | {len(medium)} | 约 {total_medium:,} 件 |\n"

    if routine:
        total_routine = sum(x['suggested_qty'] for x in routine)
        md += f"| 📦 提前补货（>{threshold_days}天） | {len(routine)} | 约 {total_routine:,} 件 |\n"
    
    total_qty = sum(x['suggested_qty'] for x in restock_list)
    md += f"| **合计** | **{len(restock_list)}** | **约 {total_qty:,} 件** |\n\n"
    
    md += "---\n\n"
    md += f"**报告生成**: {now.strftime('%Y-%m-%d %H:%M')}  \n"
    md += "**数据来源**: BigSeller库存系统 + 飞书在途库存表\n"
    
    return md


def _format_sku_item(index: int, item: Dict[str, Any]) -> str:
    """格式化单个SKU条目"""
    md = f"### {index}. {item['sku']}"
    
    if item.get('title'):
        md += f" - {item['title']}"
    
    # 标记有在途库存的SKU
    if item['in_transit'] > 0:
        md += " ⚡ 有在途库存"
    
    md += "\n"
    md += f"- **当前库存**: {item['available']} 件\n"
    md += f"- **在途库存**: {item['in_transit']} 件"
    
    if item['in_transit'] > 0:
        md += " （🚚 运输中）"
    
    md += "\n"
    md += f"- **日均销量**: {item['avg_daily_sales']:.2f} 件/天\n"
    md += f"- **预计可售**: {item['purchase_sale_days']} 天"
    
    if item['in_transit'] > 0:
        md += f"（含在途：{int(item['days_with_transit'])} 天）"
    
    md += "\n"
    md += f"- **建议采购**: **{item['suggested_qty']} 件**\n"
    md += f"- **优先级**: {item['priority'][0]}\n"
    
    if item['in_transit'] > 0:
        md += f"- **备注**: 在途{item['in_transit']}件预计到货后可售{int(item['days_with_transit'])}天"
        if item['suggested_qty'] > 10:
            md += "，仍需补货"
        md += "\n"
    
    md += "\n"
    
    return md


@register_skill
class RestockSuggestionSkillAdapter(BaseSkill):
    """
    补货建议 Skill 适配器
    
    包装旧的 generate_restock_report.py 代码，使其符合新的 OpenClaw Skill 架构。
    主路由大脑（Kimi）可以通过 Function Calling 调用此 Skill。
    """
    
    @property
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        return "generate_restock_report"
    
    @property
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        return (
            "生成补货建议报告。"
            "查询所有 SKU 的库存信息，结合日均销量计算预计可售天数，"
            "生成按优先级排序的补货建议报告。"
            "支持配置补货周期和预警阈值。"
            "输出包含紧急补货、高优先级补货、中优先级补货等分类。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """
        参数的 JSON Schema
        
        参数说明：
        - purchase_cycle_days: 补货周期天数
        - threshold_days: 预警阈值天数
        - output_format: 输出格式
        """
        return {
            "type": "object",
            "properties": {
                "purchase_cycle_days": {
                    "type": "integer",
                    "description": (
                        "补货周期天数，即从下单到收货的总天数。"
                        "默认值为 17 天（采购3天 + 物流12天 + 安全库存2天）。"
                        "用于计算建议采购量：目标库存 = 日均销量 × 补货周期。"
                    )
                },
                "threshold_days": {
                    "type": "integer",
                    "description": (
                        "预警阈值天数，用于标记紧急程度和报告分层。"
                        "默认值为 10 天。"
                        "注意：实际是否需要补货，优先由补货周期决定，即 suggested_qty > 0 就应纳入补货建议。"
                    )
                },
                "output_format": {
                    "type": "string",
                    "enum": ["markdown", "json"],
                    "description": (
                        "输出格式。"
                        "markdown：生成人类可读的 Markdown 格式报告，适合直接查看或保存。"
                        "json：返回结构化的 JSON 数据，适合程序处理或前端展示。"
                    )
                },
                "profile": {
                    "type": "string",
                    "description": (
                        "可选的 BigSeller 账号 profile 名称。"
                        "适用于你已经在 inventory-query 中保存了多个店铺账号配置的场景。"
                        "不传时使用当前激活账号。"
                    )
                },
                "only_actionable": {
                    "type": "boolean",
                    "description": (
                        "是否只输出建议采购量大于 0 的 SKU。"
                        "默认 true，适合老板查看和执行。"
                        "设为 false 时，会包含仅触发预警但建议采购为 0 的 SKU，用于排查。"
                    )
                }
            },
            "required": []
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行补货建议生成
        
        Args:
            purchase_cycle_days: 补货周期天数（可选，默认 17）
            threshold_days: 预警阈值天数（可选，默认 10）
            output_format: 输出格式（可选，默认 markdown）
            
        Returns:
            补货建议报告结果
        """
        # 提取参数
        purchase_cycle_days = kwargs.get("purchase_cycle_days", 17)
        threshold_days = kwargs.get("threshold_days", 10)
        output_format = kwargs.get("output_format", "markdown")
        profile = kwargs.get("profile")
        only_actionable = kwargs.get("only_actionable", True)
        
        try:
            # 调用核心逻辑
            result = generate_restock_report(
                purchase_cycle_days=purchase_cycle_days,
                threshold_days=threshold_days,
                output_format=output_format,
                profile=profile,
                only_actionable=only_actionable
            )
            
            if not result.get("success"):
                return result
            
            # 根据输出格式返回
            if output_format == "json":
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "restock_list": result["restock_list"],
                        "summary": {
                            "total_skus": result["total_skus"],
                            "restock_skus": result["restock_skus"],
                            "purchase_cycle_days": result["purchase_cycle_days"],
                            "threshold_days": result["threshold_days"],
                            "actionable_skus": result["actionable_skus"],
                            "warning_only_skus": result["warning_only_skus"],
                            "only_actionable": result["only_actionable"]
                        }
                    }
                }
            else:
                # 生成 Markdown 报告
                markdown_report = format_markdown_report(result)
                
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "markdown": markdown_report,
                        "summary": {
                            "total_skus": result["total_skus"],
                            "restock_skus": result["restock_skus"],
                            "purchase_cycle_days": result["purchase_cycle_days"],
                            "threshold_days": result["threshold_days"],
                            "actionable_skus": result["actionable_skus"],
                            "warning_only_skus": result["warning_only_skus"],
                            "only_actionable": result["only_actionable"]
                        }
                    },
                    "markdown": markdown_report  # 便捷访问
                }
            
        except FileNotFoundError as e:
            return {
                "success": False,
                "error": f"配置文件未找到: {str(e)}",
                "data": None,
                "suggestion": "请确保 skills/inventory-alert/config/alert_config.json 存在"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"生成报告失败: {str(e)}",
                "data": None
            }


# 便捷函数：直接调用
def generate_report(
    purchase_cycle_days: int = 17,
    threshold_days: int = 10,
    output_format: str = "markdown",
    profile: Optional[str] = None,
    only_actionable: bool = True
) -> Dict[str, Any]:
    """
    便捷函数：直接调用补货建议生成
    
    Args:
        purchase_cycle_days: 补货周期天数（默认 17）
        threshold_days: 预警阈值天数（默认 10）
        output_format: 输出格式（默认 markdown）
        profile: 指定账号 profile
        only_actionable: 是否只保留建议采购量大于 0 的 SKU
        
    Returns:
        报告结果
    """
    from base_skill import get_skill
    
    skill = get_skill("generate_restock_report")
    if skill is None:
        raise RuntimeError("补货建议 Skill 未注册")
    
    return skill.execute(
        purchase_cycle_days=purchase_cycle_days,
        threshold_days=threshold_days,
        output_format=output_format,
        profile=profile,
        only_actionable=only_actionable
    )


if __name__ == "__main__":
    # 测试适配器
    print("=" * 60)
    print("补货建议 Skill 适配器测试")
    print("=" * 60)
    
    from base_skill import list_skills, get_skill
    import json
    
    # 显示已注册的 Skill
    list_skills()
    
    # 获取 Skill
    skill = get_skill("generate_restock_report")
    if skill is None:
        print("❌ Skill 未注册")
        sys.exit(1)
    
    print(f"\n📦 Skill 名称: {skill.name}")
    print(f"📝 描述: {skill.description}")
    print(f"\n📋 JSON Schema:")
    print(json.dumps(skill.json_schema, indent=2, ensure_ascii=False))
    
    # 测试执行
    print("\n" + "=" * 60)
    print("测试执行")
    print("=" * 60)
    
    # 测试：使用默认参数生成报告
    print("\n测试: 生成补货建议报告（默认参数）")
    result = skill.execute()
    
    if result.get("success"):
        print(f"\n✅ 报告生成成功")
        print(f"   总 SKU 数: {result['data']['summary']['total_skus']}")
        print(f"   需补货 SKU 数: {result['data']['summary']['restock_skus']}")
        print(f"\n{'=' * 60}")
        print("报告预览（前 2000 字符）:")
        print("=" * 60)
        print(result["markdown"][:2000])
    else:
        print(f"\n❌ 报告生成失败: {result.get('error')}")
