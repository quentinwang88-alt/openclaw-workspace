#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenClaw Multi-Agent 业务流系统核心脚本
=========================================
系统架构：双角色双模型架构
- 主脑/前台运营（Kimi-k2.5）：负责用户对话、需求理解、工具调度
- 开发经理（GLM-5）：专门处理代码和技术架构的底层 Skill

作者：OpenClaw Team
版本：1.0.0
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import openai


# ============================================================================
# 模块一：动态技能注册底座
# ============================================================================

# 全局技能注册表：存储所有已注册的技能
SKILL_REGISTRY: Dict[str, 'BaseSkill'] = {}


def register_skill(skill_class: type) -> type:
    """
    技能注册装饰器
    
    自动将技能类实例化并注册到 SKILL_REGISTRY 中。
    使用方式：
        @register_skill
        class MySkill(BaseSkill):
            ...
    
    Args:
        skill_class: 继承自 BaseSkill 的技能类
        
    Returns:
        装饰后的技能类（保持原类不变）
    """
    # 实例化技能类
    skill_instance = skill_class()
    # 以技能名称为键注册到全局字典
    SKILL_REGISTRY[skill_instance.name] = skill_instance
    print(f"[技能注册] ✅ 已注册技能: {skill_instance.name}")
    return skill_class


class BaseSkill(ABC):
    """
    技能抽象基类
    
    所有 OpenClaw 技能必须继承此类并实现以下属性和方法：
    - name: 技能的唯一标识符
    - description: 技能的功能描述（供主脑理解何时调用）
    - json_schema: OpenAI Function Calling 的参数 Schema
    - execute(): 技能的实际执行逻辑
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """技能的唯一标识符"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """技能的功能描述，供主脑理解调用时机"""
        pass
    
    @property
    @abstractmethod
    def json_schema(self) -> Dict[str, Any]:
        """
        OpenAI Function Calling 参数 Schema
        
        定义技能所需的参数结构，遵循 JSON Schema 规范
        """
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> str:
        """
        执行技能逻辑
        
        Args:
            **kwargs: 从 Function Calling 传入的参数
            
        Returns:
            执行结果的字符串形式
        """
        pass
    
    def to_tool_definition(self) -> Dict[str, Any]:
        """
        转换为 OpenAI tools 格式的工具定义
        
        Returns:
            符合 OpenAI Function Calling 格式的工具定义字典
        """
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.json_schema
            }
        }


# ============================================================================
# 模块二：开发经理技能 (由 GLM 扮演)
# ============================================================================

@register_skill
class DevelopmentManagerSkill(BaseSkill):
    """
    开发经理技能
    
    专门处理代码编写、技术架构、爬虫开发、自动化脚本等编程任务。
    由 GLM-4/GLM-5 模型驱动，作为 OpenClaw 的底层技术能力支撑。
    """
    
    @property
    def name(self) -> str:
        return "development_manager_coding"
    
    @property
    def description(self) -> str:
        return (
            "【开发经理呼叫按钮】当用户需求涉及：编写代码、开发爬虫软件、"
            "构建自动化脚本、搭建 OpenClaw 底层服务时，请立刻调用此技能。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "coding_requirement": {
                    "type": "string",
                    "description": "具体的编程需求描述，包括功能要求、技术栈偏好、预期输出等"
                }
            },
            "required": ["coding_requirement"]
        }
    
    def execute(self, **kwargs) -> str:
        """
        执行开发任务
        
        调用阿里云 DashScope GLM-5 API，以"OpenClaw 首席开发经理"身份处理编程需求。
        
        Args:
            coding_requirement: 用户的具体编程需求描述
            
        Returns:
            生成的代码或技术方案
        """
        coding_requirement = kwargs.get("coding_requirement", "")
        
        if not coding_requirement:
            return "❌ 错误：未提供编程需求描述"
        
        print(f"\n{'='*60}")
        print(f"🔧 [开发经理] 接收到编程任务")
        print(f"📝 需求摘要: {coding_requirement[:100]}...")
        print(f"{'='*60}\n")
        
        # 初始化 GLM-5 客户端（阿里云 DashScope）
        client = openai.OpenAI(
            api_key=os.environ.get("GLM_API_KEY", "sk-sp-c0ca4d62044f4c8cbce081dee1c13a89"),
            base_url="https://coding.dashscope.aliyuncs.com/v1"
        )
        
        # 开发经理的 System Prompt
        system_prompt = """你是 OpenClaw 首席开发经理，一位拥有 15 年全栈开发经验的资深技术专家。

## 你的专业领域
- Python 后端开发与异步编程
- 爬虫开发与数据采集（Scrapy、Playwright、Selenium）
- API 设计与微服务架构
- 自动化脚本与工作流引擎
- 数据库设计与优化
- 云服务部署与 DevOps

## 工作准则
1. **代码质量优先**：编写清晰、高效、可维护的代码
2. **完整可运行**：提供的代码必须完整，用户复制后可直接运行
3. **详细注释**：关键逻辑必须有中文注释说明
4. **错误处理**：包含完善的异常处理和日志记录
5. **最佳实践**：遵循 Python PEP8 规范和行业最佳实践

## 输出格式
1. 先简要分析需求
2. 提供完整代码（使用 ```python 代码块）
3. 说明使用方法和注意事项

请以专业、高效的态度处理每一个开发任务！"""
        
        try:
            # 调用 GLM-5 API（阿里云 DashScope）
            response = client.chat.completions.create(
                model="glm-5",  # 阿里云 DashScope GLM-5 模型
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"请帮我完成以下开发任务：\n\n{coding_requirement}"}
                ],
                temperature=0.7,
                max_tokens=4096
            )
            
            result = response.choices[0].message.content
            print(f"✅ [开发经理] 任务完成\n")
            return result
            
        except Exception as e:
            error_msg = f"❌ [开发经理] API 调用失败: {str(e)}"
            print(error_msg)
            return error_msg


# ============================================================================
# 模块三：主中枢调度器 (由 Kimi 扮演)
# ============================================================================

class OpenClawOrchestrator:
    """
    OpenClaw 主中枢调度器
    
    作为系统的"大脑"，负责：
    1. 与用户进行自然语言对话
    2. 理解复杂的商业需求
    3. 判断需求类型并路由到合适的处理模块
    4. 通过 Function Calling 调度底层技能
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        初始化调度器
        
        Args:
            api_key: Moonshot API Key，如未提供则从环境变量读取
        """
        self.api_key = api_key or os.environ.get("MOONSHOT_API_KEY", "YOUR_MOONSHOT_API_KEY")
        
        # 初始化 Moonshot (Kimi) 客户端
        self.client = openai.OpenAI(
            api_key=self.api_key,
            base_url="https://api.moonshot.cn/v1"
        )
        
        # 使用的模型
        self.model = "moonshot-v1-8k"
        
        # 对话历史
        self.conversation_history: List[Dict[str, str]] = []
        
        # 系统提示词
        self.system_prompt = self._build_system_prompt()
        
        print("🧠 [OpenClaw] 中枢大脑初始化完成")
        print(f"📋 已加载技能: {list(SKILL_REGISTRY.keys())}")
    
    def _build_system_prompt(self) -> str:
        """
        构建 Kimi 的 System Prompt
        
        定义其身份、职责范围、工作流程和工具使用规范。
        """
        return """你是 OpenClaw 中枢大脑兼资深东南亚出海电商运营专员。

## 🎯 你的身份定位
你是 OpenClaw 系统的核心智能体，拥有双重身份：
1. **前台运营专家**：直接处理业务咨询，提供专业建议
2. **调度指挥官**：识别技术需求，调用开发经理处理编程任务

## 🌏 业务管辖范围（由你直接回答）

### 1. 东南亚服饰市场爆款挖掘
- 分析 TikTok、Shopee、Lazada 等平台热销趋势
- 提供选品建议和竞品分析
- 预测季节性爆款和节日营销节点

### 2. TikTok 短视频内容策划
- 短视频脚本创意和拍摄建议
- 内容矩阵规划和发布策略
- 热门话题和标签运用

### 3. 达人营销建联策略
- 达人筛选标准和合作模式
- 联系话术和谈判技巧
- ROI 评估和效果追踪

### 4. 业务人员 HR 招聘初筛
- 岗位需求分析和 JD 优化
- 面试问题设计和候选人评估框架
- 团队架构建议

### 5. 宏观商业战略
- 市场进入策略和本地化建议
- 竞争格局分析和差异化定位
- 增长黑客策略

## 🔧 技术需求处理（必须调用工具）

当用户需求涉及以下内容时，**必须调用 development_manager_coding 工具**：
- 编写代码或脚本
- 开发爬虫或数据采集工具
- 构建自动化工作流
- API 开发或集成
- 数据库设计和操作
- 系统架构设计

**重要**：遇到技术需求时，不要自己写代码，而是调用工具让开发经理处理！

## 💬 对话风格
- 专业但不生硬，像一位经验丰富的运营伙伴
- 回答结构清晰，善用列表和分点
- 适时追问以更好理解需求
- 主动提供可落地的建议

## 🚀 工作流程
1. 分析用户输入，判断需求类型
2. 如果是业务咨询 → 直接专业回答
3. 如果是技术需求 → 调用 development_manager_coding 工具
4. 整合结果，给出完整回复

现在，请以 OpenClaw 中枢大脑的身份开始工作！"""
    
    def get_tools(self) -> List[Dict[str, Any]]:
        """
        获取所有已注册技能的工具定义
        
        Returns:
            OpenAI Function Calling 格式的工具列表
        """
        tools = []
        for skill_name, skill_instance in SKILL_REGISTRY.items():
            tools.append(skill_instance.to_tool_definition())
        return tools
    
    def _execute_tool(self, tool_name: str, tool_args: Dict[str, Any]) -> str:
        """
        执行指定的工具（技能）
        
        Args:
            tool_name: 工具/技能名称
            tool_args: 传递给工具的参数
            
        Returns:
            工具执行结果
        """
        if tool_name not in SKILL_REGISTRY:
            return f"❌ 错误：未找到技能 '{tool_name}'"
        
        skill = SKILL_REGISTRY[tool_name]
        print(f"\n🔄 [调度器] 正在执行技能: {tool_name}")
        print(f"📥 参数: {json.dumps(tool_args, ensure_ascii=False, indent=2)}")
        
        result = skill.execute(**tool_args)
        return result
    
    def run_conversation(self, user_input: str, verbose: bool = True) -> str:
        """
        运行对话流程
        
        核心方法：处理用户输入，管理对话历史，处理 Function Calling 路由。
        
        Args:
            user_input: 用户输入的文本
            verbose: 是否打印详细过程
            
        Returns:
            最终的回复结果
        """
        if verbose:
            print(f"\n{'='*60}")
            print(f"👤 [用户] {user_input}")
            print(f"{'='*60}")
        
        # 添加用户消息到历史
        self.conversation_history.append({
            "role": "user",
            "content": user_input
        })
        
        # 构建消息列表
        messages = [
            {"role": "system", "content": self.system_prompt}
        ] + self.conversation_history
        
        # 第一次调用：获取模型响应
        if verbose:
            print("\n🧠 [Kimi] 正在思考...")
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=self.get_tools(),
            tool_choice="auto",
            temperature=0.7
        )
        
        message = response.choices[0].message
        
        # 检查是否需要调用工具
        if message.tool_calls:
            if verbose:
                print(f"\n🔧 [Kimi] 决定调用工具: {[tc.function.name for tc in message.tool_calls]}")
            
            # 将助手的工具调用消息添加到历史
            self.conversation_history.append({
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments
                        }
                    } for tc in message.tool_calls
                ]
            })
            
            # 执行每个工具调用
            for tool_call in message.tool_calls:
                tool_name = tool_call.function.name
                tool_args = json.loads(tool_call.function.arguments)
                
                # 执行工具
                tool_result = self._execute_tool(tool_name, tool_args)
                
                # 将工具结果添加到历史
                self.conversation_history.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result
                })
            
            # 第二次调用：让模型基于工具结果生成最终回复
            if verbose:
                print("\n🧠 [Kimi] 整合结果，生成最终回复...")
            
            messages = [
                {"role": "system", "content": self.system_prompt}
            ] + self.conversation_history
            
            final_response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.7
            )
            
            final_message = final_response.choices[0].message.content
            
        else:
            # 无需调用工具，直接返回模型回复
            final_message = message.content
            self.conversation_history.append({
                "role": "assistant",
                "content": final_message
            })
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"🤖 [OpenClaw] {final_message}")
            print(f"{'='*60}\n")
        
        return final_message
    
    def reset_conversation(self):
        """重置对话历史"""
        self.conversation_history = []
        print("🔄 [OpenClaw] 对话历史已重置")


# ============================================================================
# 模块四：测试入口
# ============================================================================

def main():
    """
    主测试入口
    
    提供两个测试用例：
    1. 纯业务咨询：测试 Kimi 直接回答能力
    2. 纯代码开发需求：测试 Function Calling 路由到开发经理
    """
    print("\n" + "="*70)
    print("🚀 OpenClaw Multi-Agent 系统启动")
    print("="*70)
    
    # 检查 API Key 配置
    glm_key = os.environ.get("GLM_API_KEY", "")
    moonshot_key = os.environ.get("MOONSHOT_API_KEY", "")
    
    if not glm_key or glm_key == "YOUR_GLM_API_KEY":
        print("\n⚠️  警告: GLM_API_KEY 未配置，开发经理功能将无法使用")
        print("   请设置环境变量: export GLM_API_KEY='your_api_key'")
    
    if not moonshot_key or moonshot_key == "YOUR_MOONSHOT_API_KEY":
        print("\n⚠️  警告: MOONSHOT_API_KEY 未配置，系统将无法正常运行")
        print("   请设置环境变量: export MOONSHOT_API_KEY='your_api_key'")
    
    # 实例化调度器
    orchestrator = OpenClawOrchestrator()
    
    print("\n" + "="*70)
    print("📋 测试用例开始")
    print("="*70)
    
    # ========== 测试用例 1：纯业务咨询 ==========
    print("\n\n" + "🔹"*35)
    print("📝 测试用例 1：纯业务咨询（东南亚电商选品）")
    print("🔹"*35 + "\n")
    
    business_query = """
    我想在东南亚做服饰类目，现在 TikTok Shop 上什么款式比较火？
    能给我一些选品建议吗？特别是针对印尼市场的。
    """
    
    result_1 = orchestrator.run_conversation(business_query)
    
    # ========== 测试用例 2：纯代码开发需求 ==========
    print("\n\n" + "🔹"*35)
    print("📝 测试用例 2：代码开发需求（爬虫脚本）")
    print("🔹"*35 + "\n")
    
    # 重置对话历史，开始新对话
    orchestrator.reset_conversation()
    
    coding_query = """
    请帮我写一个 Python 爬虫脚本，能够从 TikTok 创作者主页抓取以下信息：
    1. 创作者昵称和粉丝数
    2. 最近 10 个视频的播放量和点赞数
    3. 视频发布时间
    
    要求使用 Playwright，支持异步并发，并有完善的错误处理。
    """
    
    result_2 = orchestrator.run_conversation(coding_query)
    
    # ========== 测试总结 ==========
    print("\n\n" + "="*70)
    print("✅ 测试完成")
    print("="*70)
    print(f"\n📊 测试结果摘要:")
    print(f"   - 业务咨询测试: {'✅ 通过' if result_1 else '❌ 失败'}")
    print(f"   - 代码开发测试: {'✅ 通过' if result_2 else '❌ 失败'}")
    print("\n💡 提示: 请确保已正确配置 GLM_API_KEY 和 MOONSHOT_API_KEY 环境变量")


if __name__ == "__main__":
    main()