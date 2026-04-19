#!/usr/bin/env python3
"""
库存预警 Skill 适配器
使用适配器模式包装旧代码，使其符合新的 OpenClaw Skill 架构

设计原则：
- 不修改旧代码的核心逻辑
- 在 execute 方法中调用旧代码
- 将 Kimi 传来的参数正确映射并传递给旧代码
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

# 添加当前目录到路径，以便导入 base_skill
SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

# 添加 inventory-alert 目录到路径，以便导入旧代码
INVENTORY_ALERT_PATH = Path(__file__).parent / "inventory-alert"
if str(INVENTORY_ALERT_PATH) not in sys.path:
    sys.path.insert(0, str(INVENTORY_ALERT_PATH))

# 导入基础框架
from base_skill import BaseSkill, register_skill

# 导入旧代码
try:
    from alert import InventoryAlert, InventoryAlertAPI
except ImportError as e:
    print(f"⚠️ 无法导入旧代码 alert: {e}")
    InventoryAlert = None
    InventoryAlertAPI = None


@register_skill
class InventoryAlertSkillAdapter(BaseSkill):
    """
    库存预警 Skill 适配器
    
    包装旧的 alert.py 代码，使其符合新的 OpenClaw Skill 架构。
    主路由大脑（Kimi）可以通过 Function Calling 调用此 Skill。
    """
    
    @property
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        return "check_inventory_alert"
    
    @property
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        return (
            "检查库存预警。"
            "查询所有 SKU 的库存信息，筛选出预计可售天数低于阈值的 SKU，"
            "生成预警列表。支持发送飞书通知。"
            "预警条件：有销售记录且库存为0，或预计可售天数低于阈值。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """
        参数的 JSON Schema
        
        参数说明：
        - threshold_days: 预警阈值天数
        - send_notification: 是否发送飞书通知
        - output_format: 输出格式
        """
        return {
            "type": "object",
            "properties": {
                "threshold_days": {
                    "type": "integer",
                    "description": (
                        "预警阈值天数，预计可售天数低于此值的 SKU 会被标记为预警。"
                        "默认值为 10 天。"
                        "例如：设置为 10 天，则可售天数 <10 天的 SKU 都会被预警。"
                    )
                },
                "send_notification": {
                    "type": "boolean",
                    "description": (
                        "是否发送飞书通知。"
                        "True：发送预警消息到配置的飞书群。"
                        "False（默认）：只返回预警列表，不发送通知。"
                    )
                },
                "output_format": {
                    "type": "string",
                    "enum": ["text", "json"],
                    "description": (
                        "输出格式。"
                        "text：生成人类可读的文本格式预警消息。"
                        "json：返回结构化的 JSON 数据，适合程序处理。"
                    )
                }
            },
            "required": []
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行库存预警检查
        
        Args:
            threshold_days: 预警阈值天数（可选，默认使用配置文件值）
            send_notification: 是否发送飞书通知（可选，默认 False）
            output_format: 输出格式（可选，默认 text）
            
        Returns:
            预警检查结果
        """
        # 检查旧代码是否可用
        if InventoryAlert is None:
            return {
                "success": False,
                "error": "库存预警模块未正确加载，请检查 alert.py 是否存在",
                "data": None
            }
        
        # 提取参数
        threshold_days = kwargs.get("threshold_days")
        send_notification = kwargs.get("send_notification", False)
        output_format = kwargs.get("output_format", "text")
        
        try:
            # 初始化预警系统
            alert_system = InventoryAlert()
            
            # 如果指定了阈值，临时更新配置
            if threshold_days is not None:
                alert_system.config['threshold_days'] = threshold_days
            
            # 检查预警
            alerts = alert_system.check_alerts()
            
            # 获取实际使用的阈值
            actual_threshold = alert_system.config.get('threshold_days', 10)
            
            # 格式化消息
            message = alert_system.format_alert_message(alerts)
            
            # 发送通知（如果需要）
            notification_sent = False
            if send_notification and alerts:
                feishu_config = alert_system.config.get('feishu', {})
                webhook_url = feishu_config.get('group_webhook_url') or feishu_config.get('webhook_url', '')
                if webhook_url:
                    notification_sent = alert_system.send_feishu_notification(message, webhook_url)
            
            # 根据输出格式返回结果
            if output_format == "json":
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "alerts": alerts,
                        "summary": {
                            "total_alerts": len(alerts),
                            "threshold_days": actual_threshold,
                            "notification_sent": notification_sent
                        }
                    }
                }
            else:
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "message": message,
                        "summary": {
                            "total_alerts": len(alerts),
                            "threshold_days": actual_threshold,
                            "notification_sent": notification_sent
                        }
                    },
                    "message": message  # 便捷访问
                }
            
        except FileNotFoundError as e:
            return {
                "success": False,
                "error": f"配置文件未找到: {str(e)}",
                "data": None,
                "suggestion": "请确保 skills/inventory-alert/config/alert_config.json 存在并正确配置"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"预警检查失败: {str(e)}",
                "data": None
            }


# 便捷函数：直接调用
def check_alerts(
    threshold_days: Optional[int] = None,
    send_notification: bool = False,
    output_format: str = "text"
) -> Dict[str, Any]:
    """
    便捷函数：直接调用库存预警检查
    
    Args:
        threshold_days: 预警阈值天数（可选）
        send_notification: 是否发送飞书通知（默认 False）
        output_format: 输出格式（默认 text）
        
    Returns:
        预警检查结果
    """
    from base_skill import get_skill
    
    skill = get_skill("check_inventory_alert")
    if skill is None:
        raise RuntimeError("库存预警 Skill 未注册")
    
    return skill.execute(
        threshold_days=threshold_days,
        send_notification=send_notification,
        output_format=output_format
    )


if __name__ == "__main__":
    # 测试适配器
    print("=" * 60)
    print("库存预警 Skill 适配器测试")
    print("=" * 60)
    
    from base_skill import list_skills, get_skill
    import json
    
    # 显示已注册的 Skill
    list_skills()
    
    # 获取 Skill
    skill = get_skill("check_inventory_alert")
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
    
    # 测试：检查预警（不发送通知）
    print("\n测试: 检查库存预警")
    result = skill.execute(threshold_days=10, send_notification=False)
    
    if result.get("success"):
        print(f"\n✅ 预警检查成功")
        print(f"   预警 SKU 数: {result['data']['summary']['total_alerts']}")
        print(f"   预警阈值: {result['data']['summary']['threshold_days']} 天")
        print(f"\n{'=' * 60}")
        print("预警消息预览:")
        print("=" * 60)
        print(result["message"][:2000] if len(result.get("message", "")) > 2000 else result.get("message", ""))
    else:
        print(f"\n❌ 预警检查失败: {result.get('error')}")