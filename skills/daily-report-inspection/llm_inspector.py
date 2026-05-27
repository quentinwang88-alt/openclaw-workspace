#!/usr/bin/env python3
"""
LLM 巡检增强模块

在正则预提取的基础上，调用 DeepSeek Flash 做三件事：
1. 校正/补全正则漏掉或提取错的字段
2. 按巡检规则做上下文判断（比硬编码规则灵活）
3. 生成最终 Markdown 巡检摘要卡片
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

SKILL_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SKILL_DIR / "config.json"

SYSTEM_PROMPT = """你是日报巡检 AI 助手。你会收到一份或多份跨境电商运营日报原文，以及程序预提取的结构化字段。

你需要完成三件事：
1. 校正字段：检查预提取的每个数字是否正确，补全遗漏的字段。如果原文确实没提到某字段，保持 null。
2. 执行巡检：按规则判断每份日报是否存在异常。
3. 输出摘要：生成一份 Markdown 巡检卡片（500-800 字）。

## 输出格式

严格按以下 JSON 格式输出，不要输出任何其他文字：

```json
{
  "reports": [
    {
      "business_line": "配饰",
      "corrected_fields": {
        "short_video_generation": {
          "generated_count": <数字或null>,
          "directly_usable_count": <数字或null>,
          "modified_usable_count": <数字或null>,
          "unusable_count": <数字或null>,
          "pending_tomorrow_count": <数字或null>,
          "failure_reasons": ["原因1", "原因2"]
        },
        "video_publish": {
          "publish_count_by_store": [{"store_name": "马来配饰", "count": 2}],
          "failed_count": <数字或null>,
          "failure_reason": "<原因或null>"
        },
        "product_listing": {
          "listed_count": <数字或null>,
          "product_scope": "<店铺范围>"
        },
        "creator_outreach": {
          "outreach_count": <数字或null>,
          "outreach_status": "<未进行 或 null>",
          "is_candidate_pool": <true/false 是否可能是候选人池而非实际触达>
        },
        "sample_approval": {
          "applicant_count": <数字或null>,
          "approved_count": <数字或null>,
          "notes": "<备注或null>"
        },
        "other_notes": "<原文或null>",
        "tomorrow_plan": "<原文或null>"
      },
      "findings": [
        {
          "severity": "critical|warning|info",
          "label": "简短标签（10字以内）",
          "detail": "完整描述（含业务线、数字、原因）"
        }
      ]
    }
  ],
  "summary_markdown": "# MMDD 日报巡检摘要\\n\\n## 一、今日有效进展\\n...\\n## 二、需要重点关注的问题\\n...\\n## 三、明日建议盯住的动作\\n...\\n## 四、老板需要介入吗\\n..."
}
```

## 巡检规则

### 短视频生成
- 今日生成=0 且 明日待生成≥10 → 视频生成积压风险
- 直接可用率 < 40% → 直接可用率偏低
- (直接可用 + 修改后可用) < 生成总数 × 60% → 综合可用率偏低
- 出现类目不符/锚点跑偏/生成错误品类 → 生成链路跑偏，需要排查产品锚点和类目识别

### 视频发布
- 某店铺发布=0 → 内容发布断档（写清楚哪个店铺）
- 失败数>0 → 发布失败需关注原因
- 生成数明显大于发布数 → 生成到发布转化卡点

### 达人建联
- 建联=0 或 未进行 → 达人动作断档
- 建联数>500 → 可能是候选池数量而非实际触达，口径需确认

### 样品审批
- 有申请但批出=0 → 样品审批转化为0
- 多个业务线同时出现申请有量但批出为0 → 样品审批链路集中卡点（critical）

### 选品及上品
- 上品>0 → 供给侧有推进
- 上品多但视频生成/发布明显不足 → 上品和内容承接不匹配

## 摘要要求

- 500-800 字，结论先行，抓关键问题
- 今日有效进展：3-5 条
- 重点关注问题：3-5 条（写清楚业务线、具体数字、为什么值得关注）
- 明日建议动作：3-5 条（具体可执行）
- 老板介入：0-3 条（只有需要决策/资源投入的问题）
- 禁止输出空话如"继续保持""稳步推进""注意优化"
"""

USER_PROMPT_TEMPLATE = """以下是 {date} 的运营日报原文和程序预提取数据。请校正字段、执行巡检、生成摘要。

{report_sections}

## 程序预提取字段（可能有误，请校正）

{pre_extracted_json}

请按 JSON 格式输出校正结果和巡检摘要。"""


class LLMInspector:
    """LLM 巡检增强器"""

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        self.llm_config = self.config.get("llm", {})
        self._client = None

    @property
    def enabled(self) -> bool:
        return self.llm_config.get("enabled", True)

    def _get_client(self):
        """懒加载 OpenAI 客户端"""
        if self._client is not None:
            return self._client

        provider_config_path = Path(
            self.llm_config.get("provider_config_path", "").replace("~", str(Path.home()))
        )
        provider = self.llm_config.get("provider", "deepseek-api")

        api_key = None
        base_url = "https://api.deepseek.com/v1"

        if provider_config_path.exists():
            with open(provider_config_path, "r", encoding="utf-8") as f:
                models_cfg = json.load(f)
            provider_cfg = models_cfg.get("providers", {}).get(provider, {})
            api_key = provider_cfg.get("apiKey", "")
            base_url = provider_cfg.get("baseUrl", base_url)

        if not api_key:
            raise RuntimeError(
                f"Cannot find API key for provider '{provider}' in {provider_config_path}"
            )

        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package required. Run: pip install openai")

        self._client = OpenAI(api_key=api_key, base_url=base_url)
        return self._client

    def inspect(
        self,
        reports: List[Dict[str, Any]],
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        调用 LLM 做巡检

        Args:
            reports: 日报列表，每份含 business_line, content, parsed（预提取字段）
            target_date: 巡检日期

        Returns:
            LLM 输出：corrected fields + findings + markdown summary
        """
        if target_date is None:
            target_date = datetime.now()

        date_str = target_date.strftime("%Y-%m-%d")

        # 构建报告章节
        sections = []
        for i, r in enumerate(reports, 1):
            if not r.get("found"):
                continue
            sections.append(
                f"### 日报 {i}: {r['business_line']}\n\n"
                f"```\n{r.get('content', '')}\n```"
            )

        report_sections = "\n\n".join(sections)

        # 构建预提取字段 JSON
        pre_extracted = {}
        for r in reports:
            if not r.get("found"):
                continue
            parsed = r.get("parsed", {})
            if not parsed:
                continue
            pre = {}
            for field in ["short_video_generation", "video_publish", "product_listing",
                          "creator_outreach", "sample_approval"]:
                pre[field] = parsed.get(field, {})
            pre["other_notes"] = parsed.get("other_notes", {}).get("text", "")
            pre["tomorrow_plan"] = parsed.get("tomorrow_plan", {}).get("text", "")
            pre_extracted[r["business_line"]] = pre

        pre_json = json.dumps(pre_extracted, ensure_ascii=False, indent=2)

        user_prompt = USER_PROMPT_TEMPLATE.format(
            date=date_str,
            report_sections=report_sections,
            pre_extracted_json=pre_json,
        )

        # 调用 LLM
        client = self._get_client()
        model = self.llm_config.get("model", "deepseek-v4-flash")
        temperature = self.llm_config.get("temperature", 0.3)
        max_tokens = self.llm_config.get("max_tokens", 4096)

        print(f"\n🤖 调用 LLM: {model}")
        print(f"   prompt 长度: {len(user_prompt)} 字符")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=self.llm_config.get("timeout", 60),
        )

        raw_output = response.choices[0].message.content
        usage = response.usage
        print(f"   返回长度: {len(raw_output)} 字符")
        print(f"   token 用量: {usage.prompt_tokens} in / {usage.completion_tokens} out")

        # 解析 JSON 输出
        result = self._parse_json_output(raw_output)
        result["_usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        return result

    def _parse_json_output(self, raw: str) -> dict:
        """从 LLM 输出中提取 JSON"""
        # 移除可能的 markdown 代码块包裹
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]  # 移除开头的 ```json 或 ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 尝试提取 JSON 块
        import re
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass

        return {"error": "JSON parse failed", "raw": raw[:500]}

    def get_summary(self, llm_result: dict, fallback_summary: str = "") -> str:
        """从 LLM 结果中提取摘要"""
        return llm_result.get("summary_markdown", fallback_summary)


if __name__ == "__main__":
    from datetime import datetime

    sample_reports = [
        {
            "business_line": "配饰",
            "found": True,
            "content": """短视频生成：今日暂无。明日24个待生成。
视频发布：马来配饰成功发布1个，越南配饰成功发布0个，越南发夹成功发布6个，失败0个。
选品及上品：泰国本土店商品31。
达人建联：越南配饰建联3389人，越南发饰建联3615。
样品审批：越南配饰新增样品申请19人，批出样品1人。
其它事项补充：无。""",
            "parsed": {
                "short_video_generation": {"generated_count": 0, "pending_tomorrow_count": 24},
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "马来配饰", "count": 1},
                        {"store_name": "越南配饰", "count": 0},
                        {"store_name": "越南发夹", "count": 6},
                    ]
                },
                "product_listing": {"listed_count": 31},
                "creator_outreach": {"outreach_count": 7004},
                "sample_approval": {"applicant_count": 19, "approved_count": 1},
            },
        },
        {
            "business_line": "女装",
            "found": True,
            "content": """短视频生成：今日生成9个，直接可用3个，修改后可用2个，不能使用4个。
视频发布：泰国女装发布4个，女装店发布3个。
选品及上品：今日暂无。
达人建联：今日未进行达人建联。
样品审批：新增样品申请12人，批出0人。""",
            "parsed": {
                "short_video_generation": {
                    "generated_count": 9, "directly_usable_count": 3,
                    "modified_usable_count": 2, "unusable_count": 4,
                },
                "video_publish": {
                    "publish_count_by_store": [
                        {"store_name": "泰国女装", "count": 4},
                        {"store_name": "女装店", "count": 3},
                    ]
                },
                "product_listing": {},
                "creator_outreach": {"outreach_count": 0, "outreach_status": "未进行"},
                "sample_approval": {"applicant_count": 12, "approved_count": 0},
            },
        },
    ]

    inspector = LLMInspector()
    result = inspector.inspect(sample_reports)
    print("\n=== LLM Summary ===")
    print(result.get("summary_markdown", "No summary"))
