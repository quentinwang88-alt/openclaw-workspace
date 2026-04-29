#!/usr/bin/env python3
"""Hermes 适配。"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple

from app.models import HermesBatchResult
from app.utils import extract_json_candidates, safe_float, safe_text


HERMES_PROFILE_NAME = os.environ.get("FASTMOSS_B_HERMES_PROFILE", "picker").strip() or "picker"

ALLOWED_STRATEGIES = {"自然流", "达人分销", "投流测试", "暂不建议"}
ALLOWED_ACTIONS = {"优先跟进", "观察", "暂不建议做"}

PICKER_SYSTEM_PROMPT = dedent(
    """
    你是一个面向 TikTok 内容驱动型跨境电商团队的选品最终判断助手。

    你的职责不是做原始数据筛选，也不是重新计算业务指标。
    你的职责是在规则初筛、货源匹配、采购价补充、粗毛利计算都已经完成之后，
    对 shortlist 商品做最终业务判断。

    你只回答四个问题：
    1. 这个商品是否值得当前团队跟进
    2. 更适合自然流、达人分销、投流测试，还是暂不建议
    3. 推荐理由是什么
    4. 主要风险是什么

    你必须严格遵守以下原则：

    一、不要重做规则层工作
    你处理的商品都已经进入 shortlist。
    你不能扩大候选范围，也不能重新定义是否入池。
    你只能在已有 shortlist 基础上做最终业务判断。

    二、不要重算输入字段
    输入中的以下字段都是权威结果，禁止自行重算、质疑或替换：
    - listing_days
    - avg_price_7d_rmb
    - competition_maturity
    - source_rule_score
    - procurement_price_rmb
    - gross_margin
    - gross_margin_after_commission
    如果这些字段明显异常，只能在 risk_warning 中提示，不要擅自改值。

    三、毛利是强约束
    如果毛利结构明显不成立，不要因为商品热卖就给出积极建议。
    如果 gross_margin_after_commission 明显不足，不要轻易建议达人分销。
    如果 gross_margin 明显不足，不要轻易建议投流测试。

    四、内容空间优先于表面热度
    判断时重点考虑：
    - 商品是否容易在短视频中快速讲清
    - 是否容易形成前3秒可感知的表达
    - 是否有细节、对比、场景、问题解决、变化等内容机制
    - 是否适合真实 UGC 风格，而不是只能靠低价硬推
    不要只因为销量高就给优先跟进。

    五、差异化优先于纯低价竞争
    如果商品高度标准化、过度同质化、强依赖低价竞争，即使有销量，也要谨慎。
    如果你判断这个商品主要只能靠卷价，优先下调建议级别。

    六、保持收敛，不要制造太多“看起来都不错”的结果
    对于同一批次 shortlist：
    - 优先跟进只给最值得投入资源的一小部分
    - 中间层归入观察
    - 明显一般的直接归入暂不建议做
    不要把大量商品都判成积极结果。

    七、输出必须短、稳、结构化
    不要写长篇分析。
    不要复述输入数据。
    不要输出过程推理。
    每个商品只输出最终结论。

    八、推荐动作和打法必须从固定枚举中选择
    strategy_suggestion 只能是：
    - 自然流
    - 达人分销
    - 投流测试
    - 暂不建议

    recommended_action 只能是：
    - 优先跟进
    - 观察
    - 暂不建议做

    九、理由和风险要简洁
    recommendation_reason：
    - 1到2句话
    - 只写最关键的判断
    - 必须落到内容空间、毛利、竞争、适配性中的至少两个点

    risk_warning：
    - 1句话
    - 只写主要风险
    - 不要写泛泛而谈的空话

    十、最终只能输出 JSON
    不要输出 JSON 之外的任何文字。
    """
).strip()

PICKER_JSON_OUTPUT_CONSTRAINT = dedent(
    """
    请严格按下面的 JSON 结构输出，不要增加任何额外字段，不要输出 markdown，不要输出代码块标记：

    {
      "batch_id": "<string>",
      "items": [
        {
          "work_id": "<string>",
          "strategy_suggestion": "自然流 | 达人分销 | 投流测试 | 暂不建议",
          "recommended_action": "优先跟进 | 观察 | 暂不建议做",
          "recommendation_reason": "<string>",
          "risk_warning": "<string>"
        }
      ]
    }

    约束：
    - batch_id 必须原样返回输入中的 batch_id
    - items 数量必须与输入商品数量一致
    - items 顺序必须与输入顺序一致
    - 不允许遗漏任何 work_id
    - 不允许输出 null
    - 如果判断偏负面，也必须完整输出该 item
    """
).strip()

PICKER_DISTRIBUTION_CONSTRAINT = dedent(
    """
    批次内分布约束：
    - shortlist_count <= 30 时，优先跟进通常不应超过 6 个
    - shortlist_count 在 31 到 60 时，优先跟进通常不应超过 12 个
    - 如果多个商品都一般，不要勉强给优先跟进
    - 宁可少给优先跟进，也不要为了“好看”而放宽标准
    """
).strip()

PICKER_REPAIR_PROMPT_TEMPLATE = dedent(
    """
    你上一次的输出不是合法 JSON。

    请不要重新分析，不要补充解释，只把你上一次已经做出的判断结果，重新整理成合法 JSON。

    要求：
    1. 只输出 JSON
    2. 保持 batch_id 不变
    3. 保持 work_id 数量和顺序不变
    4. 不要增加任何新字段
    5. 不要输出 markdown，不要输出代码块

    这次触发修复的原因：
    {error_reason}

    上一次输出如下：
    {previous_output}

    目标 JSON 结构如下：
    {schema}
    """
).strip()


def _round_numeric(value: Any, digits: int = 4) -> Any:
    numeric = safe_float(value)
    if numeric is None:
        return value
    return round(numeric, digits)


def build_hermes_items(selection_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for row in selection_rows:
        items.append(
            {
                "work_id": safe_text(row.get("work_id")),
                "product_id": safe_text(row.get("product_id")),
                "product_name": safe_text(row.get("product_name")),
                "country": safe_text(row.get("country")),
                "category": safe_text(row.get("category")),
                "listing_days": row.get("listing_days"),
                "sales_7d": row.get("sales_7d"),
                "sales_total": row.get("total_sales"),
                "avg_price_7d_rmb": _round_numeric(row.get("avg_price_7d_rmb")),
                "creator_count": row.get("creator_count"),
                "video_count": row.get("video_count"),
                "live_count": row.get("live_count"),
                "commission_rate": _round_numeric(row.get("commission_rate")),
                "pool_type": safe_text(row.get("pool_type")),
                "competition_maturity": safe_text(row.get("competition_maturity")),
                "source_rule_score": _round_numeric(row.get("source_rule_score", row.get("rule_score")), digits=2),
                "rule_reason": safe_text(row.get("rule_pass_reason")),
                "procurement_price_rmb": _round_numeric(row.get("procurement_price_rmb")),
                "gross_margin": _round_numeric(row.get("gross_margin_rate")),
                "gross_margin_after_commission": _round_numeric(row.get("distribution_margin_rate")),
            }
        )
    return items


def build_hermes_user_prompt(
    batch_id: str,
    country: str,
    category: str,
    items: List[Dict[str, Any]],
    shortlist_count: Optional[int] = None,
) -> str:
    items_json = json.dumps(items, ensure_ascii=False, indent=2)
    effective_shortlist_count = shortlist_count if shortlist_count is not None else len(items)
    return dedent(
        """
        下面是一批已经通过规则筛选、并且已经补充货源采购价与毛利结果的 shortlist 商品。

        请你基于以下标准做最终业务判断：

        判断重点：
        1. 是否值得当前团队跟进
        2. 是否适合内容驱动型 TikTok 打法
        3. 是否有差异化空间，避免纯低价竞争
        4. 毛利是否能支撑建议打法
        5. 当前竞争环境是否适合切入

        打法建议请在以下枚举中选择：
        - 自然流
        - 达人分销
        - 投流测试
        - 暂不建议

        推荐动作请在以下枚举中选择：
        - 优先跟进
        - 观察
        - 暂不建议做

        输出要求：
        1. 只输出 JSON
        2. 每个 item 只输出：
           - work_id
           - strategy_suggestion
           - recommended_action
           - recommendation_reason
           - risk_warning
        3. recommendation_reason 和 risk_warning 都要简洁
        4. 本批次请保持收敛，不要给太多“优先跟进”
        5. 如果某个商品毛利、竞争、内容空间明显不成立，请直接给“暂不建议做”
        6. 不要输出任何解释文字

        批次信息：
        - batch_id: {batch_id}
        - country: {country}
        - category: {category}
        - shortlist_count: {shortlist_count}

        输入商品数据如下：
        {items_json}

        {json_contract}

        {distribution_constraint}
        """
    ).strip().format(
        batch_id=batch_id,
        country=country,
        category=category,
        shortlist_count=effective_shortlist_count,
        items_json=items_json,
        json_contract=PICKER_JSON_OUTPUT_CONSTRAINT,
        distribution_constraint=PICKER_DISTRIBUTION_CONSTRAINT,
    )


def build_hermes_repair_prompt(error_reason: str, previous_output: str) -> str:
    previous = safe_text(previous_output) or "<empty>"
    return PICKER_REPAIR_PROMPT_TEMPLATE.format(
        error_reason=error_reason,
        previous_output=previous,
        schema=PICKER_JSON_OUTPUT_CONSTRAINT,
    )


def build_hermes_input(batch_id: str, selection_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    items = build_hermes_items(selection_rows)
    country = safe_text(selection_rows[0].get("country")) if selection_rows else ""
    category = safe_text(selection_rows[0].get("category")) if selection_rows else ""
    shortlist_count_override = None
    if selection_rows:
        raw_override = selection_rows[0].get("shortlist_count_override")
        numeric_override = safe_float(raw_override)
        if numeric_override is not None:
            shortlist_count_override = int(numeric_override)
    shortlist_count = shortlist_count_override if shortlist_count_override is not None else len(items)
    return {
        "profile": HERMES_PROFILE_NAME,
        "batch_id": batch_id,
        "country": country,
        "category": category,
        "shortlist_count": shortlist_count,
        "picker_system_prompt": PICKER_SYSTEM_PROMPT,
        "picker_user_prompt": build_hermes_user_prompt(
            batch_id,
            country,
            category,
            items,
            shortlist_count=shortlist_count,
        ),
        "picker_json_output_constraint": PICKER_JSON_OUTPUT_CONSTRAINT,
        "items": items,
    }


def _coerce_hermes_raw_items(payload: Any) -> Tuple[str, List[Dict[str, Any]]]:
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return safe_text(payload.get("batch_id")), [item for item in payload.get("items", []) if isinstance(item, dict)]
    if isinstance(payload, list):
        return "", [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        raw_items = []
        for work_id, value in payload.items():
            if not isinstance(value, dict):
                continue
            row = dict(value)
            row.setdefault("work_id", work_id)
            raw_items.append(row)
        return "", raw_items
    return "", []


def _normalize_hermes_payload(
    payload: Any,
    batch_id: str,
    expected_work_ids: List[str],
) -> Tuple[Optional[Dict[str, Dict[str, Any]]], str]:
    payload_batch_id, raw_items = _coerce_hermes_raw_items(payload)
    if payload_batch_id and payload_batch_id != batch_id:
        return None, "Hermes 返回的 batch_id 与输入不一致"
    if not payload_batch_id and isinstance(payload, dict):
        return None, "Hermes 输出缺少 batch_id"
    if len(raw_items) != len(expected_work_ids):
        return None, "Hermes 返回 item 数量不匹配"

    items = {}
    for index, expected_work_id in enumerate(expected_work_ids):
        raw_item = raw_items[index]
        work_id = safe_text(raw_item.get("work_id"))
        if work_id != expected_work_id:
            return None, "Hermes 返回的 work_id 顺序或内容不匹配"
        strategy = safe_text(raw_item.get("strategy_suggestion"))
        action = safe_text(raw_item.get("recommended_action"))
        reason = safe_text(raw_item.get("recommendation_reason"))
        risk = safe_text(raw_item.get("risk_warning"))
        if strategy not in ALLOWED_STRATEGIES:
            return None, "Hermes 返回了非法 strategy_suggestion"
        if action not in ALLOWED_ACTIONS:
            return None, "Hermes 返回了非法 recommended_action"
        if not reason:
            return None, "Hermes 返回缺少 recommendation_reason"
        if not risk:
            return None, "Hermes 返回缺少 risk_warning"
        items[work_id] = {
            "content_potential_score": raw_item.get("content_potential_score"),
            "differentiation_score": raw_item.get("differentiation_score"),
            "fit_judgment": raw_item.get("fit_judgment"),
            "strategy_suggestion": strategy,
            "recommended_action": action,
            "recommendation_reason": reason,
            "risk_warning": risk,
        }
    return items, ""


def _canonical_output_payload(
    batch_id: str,
    expected_work_ids: List[str],
    items: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    output_items = []
    for work_id in expected_work_ids:
        row = items[work_id]
        output_items.append(
            {
                "work_id": work_id,
                "strategy_suggestion": row.get("strategy_suggestion"),
                "recommended_action": row.get("recommended_action"),
                "recommendation_reason": row.get("recommendation_reason"),
                "risk_warning": row.get("risk_warning"),
            }
        )
    return {"batch_id": batch_id, "items": output_items}


def _parse_json_payload(raw_text: str) -> Tuple[Optional[Any], str]:
    text = raw_text.strip()
    if not text:
        return None, "Hermes 没有返回任何内容"
    for candidate in extract_json_candidates(text):
        try:
            return json.loads(candidate), ""
        except ValueError:
            continue
    return None, "Hermes 返回内容不是合法 JSON"


def _read_hermes_output(output_path: Path, stdout: str) -> Tuple[Optional[Any], str, str]:
    raw_text = ""
    if output_path.exists():
        raw_text = output_path.read_text(encoding="utf-8")
    elif safe_text(stdout):
        raw_text = stdout
    payload, error = _parse_json_payload(raw_text)
    return payload, error, raw_text


def _render_command(
    command_template: str,
    batch_id: str,
    input_path: Path,
    output_path: Path,
    system_prompt_path: Path,
    user_prompt_path: Path,
    repair_prompt_path: Path,
) -> str:
    return command_template.format(
        input=str(input_path),
        output=str(output_path),
        batch_id=batch_id,
        profile=HERMES_PROFILE_NAME,
        system_prompt=str(system_prompt_path),
        user_prompt=str(user_prompt_path),
        repair_prompt=str(repair_prompt_path),
    )


def _run_command(command: str, timeout_seconds: int) -> subprocess.CompletedProcess:
    command_args = shlex.split(command)
    return subprocess.run(
        command_args,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )


def run_hermes_batch(
    batch_id: str,
    selection_rows: List[Dict[str, Any]],
    archive_dir: Path,
    command_template: str,
    timeout_seconds: int = 300,
) -> HermesBatchResult:
    archive_dir.mkdir(parents=True, exist_ok=True)
    input_path = archive_dir / "hermes_input.json"
    output_path = archive_dir / "hermes_output.json"
    system_prompt_path = archive_dir / "hermes_system_prompt.txt"
    user_prompt_path = archive_dir / "hermes_user_prompt.txt"
    repair_prompt_path = archive_dir / "hermes_repair_prompt.txt"
    raw_output_path = archive_dir / "hermes_output_raw.txt"
    repair_input_path = archive_dir / "hermes_repair_input.json"
    repair_output_path = archive_dir / "hermes_repair_output.json"

    input_payload = build_hermes_input(batch_id, selection_rows)
    input_path.write_text(json.dumps(input_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    system_prompt_path.write_text(PICKER_SYSTEM_PROMPT, encoding="utf-8")
    user_prompt_path.write_text(safe_text(input_payload.get("picker_user_prompt")), encoding="utf-8")

    if not command_template.strip():
        return HermesBatchResult(
            status="not_configured",
            items={},
            input_path=str(input_path),
            output_path=None,
            error="未配置 FASTMOSS_B_HERMES_COMMAND",
        )

    expected_work_ids = [safe_text(item.get("work_id")) for item in input_payload.get("items", []) if safe_text(item.get("work_id"))]
    command = _render_command(
        command_template,
        batch_id,
        input_path,
        output_path,
        system_prompt_path,
        user_prompt_path,
        repair_prompt_path,
    )

    try:
        completed = _run_command(command, timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        return HermesBatchResult(
            status="failed",
            items={},
            input_path=str(input_path),
            output_path=None,
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
            error="Hermes 调用超时",
        )

    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode != 0:
        return HermesBatchResult(
            status="failed",
            items={},
            input_path=str(input_path),
            output_path=str(output_path) if output_path.exists() else None,
            stdout=stdout,
            stderr=stderr,
            error="Hermes 命令退出码非 0: {code}".format(code=completed.returncode),
        )

    payload, parse_error, raw_output = _read_hermes_output(output_path, stdout)
    raw_output_path.write_text(raw_output, encoding="utf-8")
    normalized_items = None  # type: Optional[Dict[str, Dict[str, Any]]]
    validation_error = ""
    if payload is not None:
        normalized_items, validation_error = _normalize_hermes_payload(payload, batch_id, expected_work_ids)

    if payload is None or validation_error:
        error_reason = parse_error or validation_error or "Hermes 输出不符合要求"
        repair_prompt = build_hermes_repair_prompt(error_reason, raw_output)
        repair_prompt_path.write_text(repair_prompt, encoding="utf-8")
        repair_payload = dict(input_payload)
        repair_payload["picker_user_prompt"] = repair_prompt
        repair_payload["repair_reason"] = error_reason
        repair_payload["previous_output"] = raw_output
        repair_input_path.write_text(json.dumps(repair_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        repair_command = _render_command(
            command_template,
            batch_id,
            repair_input_path,
            repair_output_path,
            system_prompt_path,
            repair_prompt_path,
            repair_prompt_path,
        )
        try:
            repaired = _run_command(repair_command, timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            return HermesBatchResult(
                status="failed",
                items={},
                input_path=str(repair_input_path),
                output_path=None,
                stdout=exc.stdout or "",
                stderr=exc.stderr or "",
                error="Hermes repair 调用超时",
            )
        repair_stdout = repaired.stdout or ""
        repair_stderr = repaired.stderr or ""
        if repaired.returncode != 0:
            return HermesBatchResult(
                status="failed",
                items={},
                input_path=str(repair_input_path),
                output_path=str(repair_output_path) if repair_output_path.exists() else None,
                stdout=repair_stdout,
                stderr=repair_stderr,
                error="Hermes repair 命令退出码非 0: {code}".format(code=repaired.returncode),
            )
        payload, parse_error, raw_output = _read_hermes_output(repair_output_path, repair_stdout)
        raw_output_path.write_text(raw_output, encoding="utf-8")
        if payload is None:
            return HermesBatchResult(
                status="failed",
                items={},
                input_path=str(repair_input_path),
                output_path=str(repair_output_path) if repair_output_path.exists() else None,
                stdout=repair_stdout,
                stderr=repair_stderr,
                error=parse_error,
            )
        normalized_items, validation_error = _normalize_hermes_payload(payload, batch_id, expected_work_ids)
        if validation_error:
            return HermesBatchResult(
                status="failed",
                items={},
                input_path=str(repair_input_path),
                output_path=str(repair_output_path) if repair_output_path.exists() else None,
                stdout=repair_stdout,
                stderr=repair_stderr,
                error=validation_error,
            )
        stdout = repair_stdout
        stderr = repair_stderr

    canonical_output = _canonical_output_payload(batch_id, expected_work_ids, normalized_items or {})
    output_path.write_text(json.dumps(canonical_output, ensure_ascii=False, indent=2), encoding="utf-8")
    return HermesBatchResult(
        status="success",
        items=normalized_items or {},
        input_path=str(input_path),
        output_path=str(output_path),
        stdout=stdout,
        stderr=stderr,
    )
