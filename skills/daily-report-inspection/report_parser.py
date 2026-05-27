#!/usr/bin/env python3
"""
日报内容解析器

将自然语言的日报文本解析为结构化字段。
采用正则表达式 + 关键词匹配，兼容非结构化填写格式。
"""

import re
from typing import Dict, List, Optional, Any


class ReportParser:
    """日报内容解析器"""

    def __init__(self):
        self._compile_patterns()
        self._compile_section_patterns()

    def _compile_section_patterns(self):
        """编译分段用的正则"""
        self.section_patterns = {
            "short_video_generation": re.compile(
                r"(?:短视频\s*生成|视频\s*生成).*?(?=\n(?:视频发布|选品|达人|样品|其它|其他|明日|明天|$))",
                re.DOTALL,
            ),
            "video_publish": re.compile(
                r"视频\s*发布.*?(?=\n(?:选品|达人|样品|其它|其他|明日|明天|$))",
                re.DOTALL,
            ),
            "product_listing": re.compile(
                r"(?:选品|上品|商品).*?(?=\n(?:达人|样品|其它|其他|明日|明天|$))",
                re.DOTALL,
            ),
            "creator_outreach": re.compile(
                r"达人.*?(?=\n(?:样品|其它|其他|明日|明天|$))",
                re.DOTALL,
            ),
            "sample_approval": re.compile(
                r"样品.*?(?=\n(?:其它|其他|明日|明天|$))",
                re.DOTALL,
            ),
        }

    def _get_section(self, text: str, section_name: str) -> str:
        """从日报文本中提取特定段落"""
        if section_name in self.section_patterns:
            m = self.section_patterns[section_name].search(text)
            if m:
                return m.group(0)
        return text

    def _compile_patterns(self):
        """预编译所有正则表达式"""
        self.patterns = {
            # 短视频 - 今日未生成/暂无
            "gen_none": re.compile(
                r"(?:今日[^。.]{0,6}(?:暂无|无|没有|0)(?:生成|视频|短视频)?)|"
                r"(?:短视频[^。.]{0,4}(?:暂无|无|没有))|"
                r"(?:生成[^。.]{0,6}(?:暂无|无|没有|为\s*0))|"
                r"(?:今日[^。.]{0,4}暂无|今日[^。.]{0,4}没有生成)",
            ),

            # 短视频生成 - 总数
            "gen_total": re.compile(
                r"(?:生成|制作|产出)\s*(?:了?)\s*(\d+)\s*(?:个|条|视频)",
            ),
            "gen_total_alt": re.compile(
                r"(?:共|总共|合计|合计生成)\s*[：:]*\s*(\d+)\s*(?:个|条)",
            ),
            "gen_total_day": re.compile(
                r"今日\s*(?:生成|制作|产出)\s*(\d+)\s*(?:个|条)",
            ),
            "gen_total_alt": re.compile(
                r"(?:共|总共|合计|合计生成)\s*[：:]*\s*(\d+)\s*(?:个|条)"
            ),

            # 短视频 - 可直接使用
            "gen_direct": re.compile(
                r"(?:直接\s*可用|可直接使用)\s*[：:]*\s*(\d+)",
            ),
            "gen_direct_alt": re.compile(
                r"(?:直接可用|可直接用)\s*(\d+)\s*(?:个|条)",
            ),

            # 短视频 - 修改后可用
            "gen_modified": re.compile(
                r"(?:修改\s*后\s*可用|修改后可用)\s*[：:]*\s*(\d+)",
            ),
            "gen_modified_alt": re.compile(
                r"(?:修改后可?用?)\s*(\d+)",
            ),

            # 短视频 - 不能使用/无法使用
            "gen_unusable": re.compile(
                r"(?:不[能可]使用|无法使用)\s*[：:]*\s*(\d+)",
            ),
            "gen_unusable_alt": re.compile(
                r"(?:失败|不可用)\s*(\d+)",
            ),

            # 短视频 - 明日待生成
            "gen_pending": re.compile(
                r"(?:明日|明天)[^\d]*?(\d+)\s*(?:个|条).*?(?:待生成|待制作|生成)",
            ),
            "gen_pending_alt": re.compile(
                r"(?:待生成|待制作|排队|等待生成)[^\d]*?(\d+)",
            ),
            "gen_pending_reverse": re.compile(
                r"(\d+)\s*(?:个|条)\s*(?:视频|短视频)?\s*(?:待生成|待制作|待处理)",
            ),

            # 视频发布 - 各店铺发布数
            "publish_store": re.compile(
                r"((?:马来|越南|泰国|印尼|菲律宾|新加坡)?(?:配饰|女装|发夹|发饰|\S*?装\S*?))"
                r"[：:\s]*(?:成功)?(?:发布|发了?)\s*(\d+)\s*(?:个|条)?",
            ),
            "publish_store2": re.compile(
                r"((?:配饰|女装|发夹|发饰|本土)[^\s，,。.]{0,8}?店?)"
                r"\s*(?:成功)?(?:发布|发了?)\s*(\d+)\s*(?:个|条)?",
            ),

            # 视频发布 - 失败数
            "publish_failed": re.compile(
                r"(?:失败|未成功)\s*(\d+)\s*(?:个|条|次)",
            ),

            # 视频发布 - 总数
            "publish_total": re.compile(
                r"(?:共\s*发布|发布\s*总数|合计\s*发布)\s*[：:]*\s*(\d+)",
            ),

            # 上品数
            "listing_count": re.compile(
                r"(?:上品|上新|上架)\s*(\d+)\s*(?:个|款|件|商品|sku)",
            ),
            "listing_count_alt": re.compile(
                r"(?:商品|选品|上品)\s*(\d+)",
            ),
            "listing_count_alt2": re.compile(
                r"(\d+)\s*(?:个|款|件)\s*(?:商品|listing|上品|上新)",
            ),

            # 店铺范围
            "listing_scope": re.compile(
                r"((?:泰国|马来|越南|印尼|菲律宾|新加坡)\S*?(?:本土店|本土|本对本|店))"
            ),

            # 达人建联 - 数量
            "outreach_count": re.compile(
                r"(?:建联|触达|联系)[^\d]*?(\d+)",
            ),

            # 达人建联 - 未进行
            "outreach_none": re.compile(
                r"(?:未进行|未建联|暂无|没有)[^\n]*(?:达人|建联|联系)",
            ),

            # 样品审批 - 申请数
            "sample_apply": re.compile(
                r"(?:新增\s*样品\s*申请|样品\s*申请\s*新增|新增\s*申请)[^\d]*(\d+)\s*(?:人|个|位)",
            ),
            "sample_apply_alt": re.compile(
                r"(?:申请\s*[样送]|新增\s*达人\s*申请)[^\d]*(\d+)\s*(?:人|个)",
            ),

            # 样品审批 - 批出数
            "sample_approved": re.compile(
                r"(?:批出|通过|批准|寄出|送出).*?\s*(\d+)\s*(?:人|个|位|件|份)",
            ),

            # 样品批出为0
            "sample_approved_zero": re.compile(
                r"(?:批出|通过|批准)\s*(?:为\s*)?(\d+|0|没有|无)",
            ),
        }

    def parse_short_video_generation(self, text: str) -> Dict[str, Any]:
        """解析短视频生成部分"""
        section = self._get_section(text, "short_video_generation")
        result = {
            "generated_count": None,
            "directly_usable_count": None,
            "modified_usable_count": None,
            "unusable_count": None,
            "pending_tomorrow_count": None,
            "failure_reasons": [],
        }

        # 检查今日是否无生成
        if self.patterns["gen_none"].search(section):
            result["generated_count"] = 0

        # 提取数字
        gen_total_pats = [
            self.patterns["gen_total_day"],
            self.patterns["gen_total"],
            self.patterns["gen_total_alt"],
        ]
        for field, patterns in [
            ("generated_count", gen_total_pats),
            ("directly_usable_count", [self.patterns["gen_direct"], self.patterns["gen_direct_alt"]]),
            ("modified_usable_count", [self.patterns["gen_modified"], self.patterns["gen_modified_alt"]]),
            ("unusable_count", [self.patterns["gen_unusable"], self.patterns["gen_unusable_alt"]]),
            ("pending_tomorrow_count", [self.patterns["gen_pending"], self.patterns["gen_pending_alt"], self.patterns["gen_pending_reverse"]]),
        ]:
            if result.get(field) is not None and field == "generated_count":
                continue  # 已经通过 gen_none 设置为 0
            for pat in patterns:
                m = pat.search(section)
                if m:
                    try:
                        result[field] = int(m.group(1))
                    except (ValueError, IndexError):
                        pass
                    break

        # 提取失败原因关键词
        failure_keywords = [
            "类目不符", "商品目标不匹配", "商品描述缺失", "生成成其它品类",
            "脚本跑偏", "无法使用", "类目不匹配", "商品目标错误",
            "产品锚点", "锚点不对", "锚点问题",
        ]
        for kw in failure_keywords:
            if kw in section:
                result["failure_reasons"].append(kw)

        return result

    def parse_video_publish(self, text: str) -> Dict[str, Any]:
        """解析视频发布部分"""
        section = self._get_section(text, "video_publish")
        result = {
            "total_publish_count": None,
            "publish_count_by_store": [],
            "failed_count": None,
            "zero_publish_store": [],
        }

        # 提取各店铺发布数
        for m in self.patterns["publish_store"].finditer(section):
            store = m.group(1).strip().lstrip("，,、：: ")
            count = int(m.group(2))
            existing = [s for s in result["publish_count_by_store"] if s["store_name"] == store]
            if existing:
                existing[0]["count"] += count
            else:
                result["publish_count_by_store"].append({"store_name": store, "count": count})

        # 备用模式：只在主模式没匹配到时使用
        if not result["publish_count_by_store"]:
            for m in self.patterns["publish_store2"].finditer(section):
                store = m.group(1).strip().lstrip("，,、：: ")
                count = int(m.group(2))
                result["publish_count_by_store"].append({"store_name": store, "count": count})

        # 失败数
        m = self.patterns["publish_failed"].search(section)
        if m:
            try:
                result["failed_count"] = int(m.group(1))
            except ValueError:
                pass

        # 合计总数（如果没有店铺明细）
        if not result["publish_count_by_store"]:
            m = self.patterns["publish_total"].search(section)
            if m:
                try:
                    result["total_publish_count"] = int(m.group(1))
                except ValueError:
                    pass
        else:
            result["total_publish_count"] = sum(
                s["count"] for s in result["publish_count_by_store"]
            )

        return result

    def parse_product_listing(self, text: str) -> Dict[str, Any]:
        """解析选品及上品部分"""
        section = self._get_section(text, "product_listing")
        result = {
            "listed_count": None,
            "product_scope": "",
        }

        for pat in [
            self.patterns["listing_count"],
            self.patterns["listing_count_alt"],
            self.patterns["listing_count_alt2"],
        ]:
            m = pat.search(section)
            if m:
                try:
                    result["listed_count"] = int(m.group(1))
                except (ValueError, IndexError):
                    pass
                break

        scopes = []
        for m in self.patterns["listing_scope"].finditer(section):
            scopes.append(m.group(1).strip())
        result["product_scope"] = ", ".join(scopes)

        return result

    def parse_creator_outreach(self, text: str) -> Dict[str, Any]:
        """解析达人建联部分"""
        section = self._get_section(text, "creator_outreach")
        result = {
            "outreach_count": None,
            "outreach_status": "",
            "possible_candidate_pool_count": None,
            "notes": "",
        }

        # 检测是否未进行
        if self.patterns["outreach_none"].search(section):
            result["outreach_status"] = "未进行"
            return result

        # 提取所有建联数字
        all_counts = []
        for m in self.patterns["outreach_count"].finditer(section):
            try:
                all_counts.append(int(m.group(1)))
            except ValueError:
                pass

        if all_counts:
            # 如果有多个数字，取最大的作为可能的总数
            result["outreach_count"] = sum(all_counts)

            # 如果数字异常大（>500），标记为候选池
            if max(all_counts) > 500:
                result["possible_candidate_pool_count"] = max(all_counts)
                result["notes"] = "数字偏大，可能是候选达人池数量而非实际新增触达"

        return result

    def parse_sample_approval(self, text: str) -> Dict[str, Any]:
        """解析样品审批部分"""
        section = self._get_section(text, "sample_approval")
        result = {
            "applicant_count": None,
            "approved_count": None,
            "product_notes": "",
            "notes": "",
        }

        # 申请数
        for pat in [self.patterns["sample_apply"], self.patterns["sample_apply_alt"]]:
            m = pat.search(section)
            if m:
                try:
                    result["applicant_count"] = int(m.group(1))
                except (ValueError, IndexError):
                    pass
                break

        # 批出数
        m = self.patterns["sample_approved"].search(section)
        if m:
            try:
                result["approved_count"] = int(m.group(1))
            except (ValueError, IndexError):
                pass

        # 如果没匹配到数字，检查是否批出为0
        if result["approved_count"] is None:
            m_zero = self.patterns["sample_approved_zero"].search(section)
            if m_zero:
                val = m_zero.group(1)
                if val in ("0", "没有", "无"):
                    result["approved_count"] = 0

        return result

    def parse_other_notes(self, text: str) -> str:
        """提取其它事项补充"""
        patterns = [
            re.compile(r"其它事项[补充]*[：:]*\s*(.+?)(?=\n\n|\n(?:明日|明天|备注)|$)", re.DOTALL),
            re.compile(r"其他事项[补充]*[：:]*\s*(.+?)(?=\n\n|\n(?:明日|明天|备注)|$)", re.DOTALL),
            re.compile(r"其它[补充]*[：:]*\s*(.+?)(?=\n\n|\n(?:明日|明天|备注)|$)", re.DOTALL),
        ]
        for pat in patterns:
            m = pat.search(text)
            if m:
                return m.group(1).strip()
        return ""

    def parse_tomorrow_plan(self, text: str) -> str:
        """提取明日计划"""
        patterns = [
            re.compile(r"明日计划[：:]*\s*(.+?)$", re.DOTALL),
            re.compile(r"明天计划[：:]*\s*(.+?)$", re.DOTALL),
            re.compile(r"明日[：:]*\s*(.+?)$", re.DOTALL),
        ]
        for pat in patterns:
            m = pat.search(text)
            if m:
                return m.group(1).strip()
        return ""

    def parse(self, text: str) -> Dict[str, Any]:
        """
        解析完整日报内容 -> 结构化数据

        Args:
            text: 日报原始文本

        Returns:
            结构化日报数据
        """
        result = {
            "short_video_generation": self.parse_short_video_generation(text),
            "video_publish": self.parse_video_publish(text),
            "product_listing": self.parse_product_listing(text),
            "creator_outreach": self.parse_creator_outreach(text),
            "sample_approval": self.parse_sample_approval(text),
            "other_notes": {"text": self.parse_other_notes(text)},
            "tomorrow_plan": {"text": self.parse_tomorrow_plan(text)},
            "raw_text": text,
        }
        return result


def parse_report(text: str) -> Dict[str, Any]:
    """便捷函数：解析日报文本"""
    parser = ReportParser()
    return parser.parse(text)


if __name__ == "__main__":
    sample = """
短视频生成：今日生成9个，直接可用3个，修改后可用2个，不能使用4个。明日24个待生成。
视频发布：马来配饰成功发布1个，越南配饰成功发布0个，越南发夹成功发布6个，失败0个。
选品及上品：泰国本土店商品31。
达人建联：越南配饰建联3389人，越南发饰建联3615。
样品审批：越南配饰新增样品申请19人，批出样品1人。
其它事项补充：已完成洞察报告越南首饰必备动作。
明日计划：继续推进视频生成，优先处理积压任务。
"""
    parser = ReportParser()
    result = parser.parse(sample)
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))
