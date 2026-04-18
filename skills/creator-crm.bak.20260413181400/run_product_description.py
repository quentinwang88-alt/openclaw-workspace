#!/usr/bin/env python3
"""
产品描述生成脚本

功能：
- 读取飞书表格中"待开始"状态的产品任务
- 下载产品图片
- 使用 LLM 分析图片生成产品描述（本地语言）
- 更新任务状态为"已完成"

使用方法：
    python3 run_product_description.py --feishu-url "https://xxx.feishu.cn/wiki/xxx?table=xxx"
"""

import os
import sys
import json
import argparse
import time
import base64
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

# 确保从任意目录运行都能找到模块
SKILL_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(SKILL_DIR))

from core.feishu_url_parser import extract_from_feishu_url
from core.product_type_resolution import resolve_product_context
from core.prompt_contract_builder import build_prompt_contract
from core.script_type_validator import validate_generated_text


# ============================================================================
# 配置
# ============================================================================

# LLM API 配置
LLM_API_URL = os.environ.get("LLM_API_URL", "https://ark.cn-beijing.volces.com/api/coding/v3")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "b5ee8fce-c898-49cf-8098-ece21150e04b")
LLM_MODEL = os.environ.get("LLM_MODEL", "Doubao-Seed-2.0-pro")

# 输出目录
OUTPUT_DIR = SKILL_DIR / "output" / "products"


# ============================================================================
# 飞书 API 客户端
# ============================================================================

class FeishuClient:
    """飞书 API 客户端"""
    
    def __init__(self):
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
    
    def _get_access_token(self) -> str:
        """获取飞书 access_token"""
        import time
        
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token
        
        config_file = Path.home() / ".openclaw/openclaw.json"
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        app_id = config['channels']['feishu']['appId']
        app_secret = config['channels']['feishu']['appSecret']
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        response = requests.post(url, json={'app_id': app_id, 'app_secret': app_secret})
        result = response.json()
        
        if result.get('code') != 0:
            raise Exception(f"获取 access_token 失败: {result.get('msg')}")
        
        self.access_token = result['tenant_access_token']
        self.token_expires_at = time.time() + result.get('expire', 7200) - 300
        
        return self.access_token
    
    def read_records(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        """读取表格记录"""
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        records = []
        has_more = True
        page_token = None
        
        while has_more:
            params = {'page_size': 500}
            if page_token:
                params['page_token'] = page_token
            
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            
            if result.get('code') != 0:
                raise Exception(f"读取记录失败: {result.get('msg')}")
            
            data = result.get('data', {})
            items = data.get('items', [])
            records.extend(items)
            
            has_more = data.get('has_more', False)
            page_token = data.get('page_token')
        
        return records
    
    def download_image(self, file_token: str, save_path: str) -> bool:
        """下载图片"""
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{file_token}/download"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        try:
            response = requests.get(url, headers=headers, timeout=60, stream=True)
            
            if response.status_code == 200:
                Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                return True
            else:
                print(f"    ⚠️ 下载失败 (HTTP {response.status_code})")
                return False
        except Exception as e:
            print(f"    ⚠️ 下载请求失败: {e}")
            return False
    
    def update_record(self, app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
        """更新记录"""
        access_token = self._get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        payload = {'fields': fields}
        
        try:
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            result = response.json()
            
            if result.get('code') == 0:
                return True
            else:
                print(f"    ⚠️ 更新失败: {result.get('msg')}")
                return False
        except Exception as e:
            print(f"    ⚠️ 更新请求失败: {e}")
            return False


# ============================================================================
# 产品描述生成器
# ============================================================================

class ProductDescriptionGenerator:
    """产品描述生成器"""
    
    def __init__(self):
        self.stats = {'total': 0, 'success': 0, 'failed': 0}
    
    def _encode_image(self, image_path: str) -> str:
        """将图片转为 Base64"""
        with open(image_path, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    
    def _call_llm(self, image_base64: str, mime_type: str, prompt: str) -> Optional[str]:
        """调用 LLM API"""
        url = f"{LLM_API_URL}/chat/completions"
        
        headers = {
            'Authorization': f'Bearer {LLM_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': LLM_MODEL,
            'messages': [
                {
                    'role': 'user',
                    'content': [
                        {'type': 'text', 'text': prompt},
                        {
                            'type': 'image_url',
                            'image_url': {
                                'url': f'data:{mime_type};base64,{image_base64}'
                            }
                        }
                    ]
                }
            ],
            'max_tokens': 2000
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            result = response.json()
            
            if response.status_code != 200:
                print(f"    ⚠️ API 返回错误: HTTP {response.status_code}")
                return None
            
            if 'choices' in result and len(result['choices']) > 0:
                return result['choices'][0]['message']['content']
            return None
        except Exception as e:
            print(f"    ⚠️ LLM 调用失败: {e}")
            return None

    @staticmethod
    def _build_anchor_info_block(
        product_type: str,
        business_category: str,
        product_params: str = "",
    ) -> str:
        """构建产品锚点信息区块。"""
        lines = [
            f"- 表格产品类型：{product_type or '未填写'}",
            f"- 业务大类：{business_category or '未填写'}",
        ]
        if product_params and str(product_params).strip():
            lines.extend(
                [
                    f"- 产品参数信息：{str(product_params).strip()}",
                    "",
                    "参数锚点规则：",
                    "1. 参数信息属于高优先级产品锚点，不得与之冲突。",
                    "2. 若图片存在尺度模糊、白底单拍、缺少参照物等情况，优先依据参数信息描述。",
                    "3. 不得擅自扩写未提供的参数。",
                    "4. 输出时可以自然吸收参数信息，但不要机械堆砌参数字段。",
                ]
            )

        return "\n".join(lines)

    def generate_description(self, image_paths: List[str], product_type: str,
                            target_country: str, target_language: str,
                            business_category: str = "",
                            product_params: str = "") -> Optional[str]:
        """
        生成产品描述
        
        Args:
            image_paths: 产品图片路径列表
            product_type: 产品类型
            target_country: 目标国家
            target_language: 目标语言
        
        Returns:
            产品描述文本
        """
        # 只使用第一张图片
        if not image_paths:
            return None
        
        image_path = image_paths[0]
        
        # 读取并编码图片
        image_base64 = self._encode_image(image_path)
        image_format = Path(image_path).suffix.lower().lstrip('.')
        if image_format == 'jpg':
            image_format = 'jpeg'
        mime_type = f"image/{image_format}"
        
        resolved_context = resolve_product_context(
            raw_product_type=product_type,
            business_category=business_category,
        )
        prompt_contract = build_prompt_contract(resolved_context)
        anchor_info_block = self._build_anchor_info_block(
            product_type=product_type,
            business_category=business_category,
            product_params=product_params,
        )

        # 构建 Prompt
        prompt = f"""你是一位跨境电商产品文案专家。请分析这张产品图片，为{target_country}市场生成产品描述。

目标语言：{target_language}

产品锚点信息：
{anchor_info_block}

类型约束（必须严格遵守）：
{prompt_contract}

额外规则：
1. 如果图片视觉上与表格产品类型冲突，必须优先遵循表格产品类型与标准佩戴/使用部位。
2. 不允许把商品改写成其他身体部位使用的品类。
3. 如果图片存在尺度歧义，必须按最终产品类型描述，不得自行改类。
4. 输出文案时，必须与最终产品类型保持一致。

请生成以下内容：

1. **产品标题**（{target_language}）：简洁吸引人的标题，15-20字
2. **产品卖点**（{target_language}）：3-5个核心卖点，每个卖点一行
3. **产品描述**（{target_language}）：详细描述产品特点、材质、适用场景等，100-150字

请按以下格式输出：

---TITLE_START---
[产品标题]
---TITLE_END---

---SELLING_POINTS_START---
[卖点1]
[卖点2]
[卖点3]
---SELLING_POINTS_END---

---DESCRIPTION_START---
[产品描述]
---DESCRIPTION_END---
"""
        
        # 调用 LLM
        response = self._call_llm(image_base64, mime_type, prompt)
        return response


# ============================================================================
# 主流程
# ============================================================================

class ProductDescriptionPipeline:
    """产品描述生成流水线"""
    
    def __init__(self, app_token: str, table_id: str):
        self.app_token = app_token
        self.table_id = table_id
        self.feishu = FeishuClient()
        self.generator = ProductDescriptionGenerator()
        
        # 确保输出目录存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    def _download_product_images(self, record: Dict[str, Any]) -> List[str]:
        """
        下载产品图片
        
        Args:
            record: 任务记录
        
        Returns:
            下载的图片路径列表
        """
        fields = record.get('fields', {})
        task_id = fields.get('任务编号', 'unknown')
        images = fields.get('产品图片', [])
        
        downloaded_paths = []
        
        for i, img in enumerate(images):
            file_token = img.get('file_token')
            if not file_token:
                continue
            
            save_path = OUTPUT_DIR / f"{task_id}_{i}.jpg"
            
            print(f"    📥 下载图片 {i+1}/{len(images)}...")
            if self.feishu.download_image(file_token, str(save_path)):
                downloaded_paths.append(str(save_path))
        
        return downloaded_paths
    
    def process_task(self, record: Dict[str, Any]) -> bool:
        """
        处理单个任务
        
        Args:
            record: 任务记录
        
        Returns:
            是否成功
        """
        record_id = record.get('record_id')
        fields = record.get('fields', {})
        task_id = fields.get('任务编号', 'unknown')
        product_type = fields.get('产品类型', '')
        business_category = fields.get('一级类目', '')
        product_params = fields.get('产品参数信息', '')
        target_country = fields.get('目标国家', '')
        target_language = fields.get('目标语言', '')
        
        print(f"\n{'='*60}")
        print(f"处理任务: {task_id}")
        print(f"{'='*60}")
        resolved_context = resolve_product_context(
            raw_product_type=product_type,
            business_category=business_category,
        )

        print(f"  产品类型: {product_type}")
        print(f"  业务大类: {business_category}")
        if str(product_params).strip():
            print(f"  产品参数: {product_params}")
        print(f"  标准类型: {resolved_context.canonical_family}/{resolved_context.canonical_slot}/{resolved_context.canonical_type}")
        print(f"  目标国家: {target_country}")
        print(f"  目标语言: {target_language}")
        
        # 下载产品图片
        image_paths = self._download_product_images(record)
        if not image_paths:
            print(f"  ❌ 无法下载产品图片")
            self.generator.stats['failed'] += 1
            return False
        
        print(f"  📷 已下载 {len(image_paths)} 张图片")
        
        # 生成产品描述
        print(f"  🤖 生成产品描述...")
        description = self.generator.generate_description(
            image_paths=image_paths,
            product_type=product_type,
            target_country=target_country,
            target_language=target_language,
            business_category=business_category,
            product_params=product_params,
        )
        
        if not description:
            print(f"  ❌ 生成描述失败")
            self.generator.stats['failed'] += 1
            return False
        
        validation = validate_generated_text(description, resolved_context)
        if validation.warnings:
            print(f"  ⚠️ 质检警告: {' | '.join(validation.warnings)}")
        if not validation.is_valid:
            print(f"  ❌ 类型质检失败: {' | '.join(validation.violations)}")
            self.generator.stats['failed'] += 1
            return False

        # 更新飞书记录
        print(f"  💾 更新飞书记录...")
        
        # 解析生成的描述，分配到三个脚本方向字段
        script1 = ""
        script2 = ""
        script3 = ""
        
        # 尝试提取各个部分
        if "---TITLE_START---" in description and "---TITLE_END---" in description:
            title_start = description.find("---TITLE_START---") + len("---TITLE_START---")
            title_end = description.find("---TITLE_END---")
            script1 = description[title_start:title_end].strip()
        
        if "---SELLING_POINTS_START---" in description and "---SELLING_POINTS_END---" in description:
            sp_start = description.find("---SELLING_POINTS_START---") + len("---SELLING_POINTS_START---")
            sp_end = description.find("---SELLING_POINTS_END---")
            script2 = description[sp_start:sp_end].strip()
        
        if "---DESCRIPTION_START---" in description and "---DESCRIPTION_END---" in description:
            desc_start = description.find("---DESCRIPTION_START---") + len("---DESCRIPTION_START---")
            desc_end = description.find("---DESCRIPTION_END---")
            script3 = description[desc_start:desc_end].strip()
        
        # 如果解析失败，将整个描述放入第一个字段
        if not script1 and not script2 and not script3:
            script1 = description[:2000] if len(description) > 2000 else description
        
        success = self.feishu.update_record(
            app_token=self.app_token,
            table_id=self.table_id,
            record_id=record_id,
            fields={
                '脚本方向一': script1,
                '脚本方向二': script2,
                '脚本方向三': script3,
                '任务状态': '已完成'
            }
        )
        
        if success:
            print(f"  ✅ 完成")
            print(f"     描述预览: {description[:100]}...")
            self.generator.stats['success'] += 1
            return True
        else:
            print(f"  ❌ 更新飞书失败")
            self.generator.stats['failed'] += 1
            return False
    
    def run(self, dry_run: bool = False):
        """
        运行流水线
        
        Args:
            dry_run: 测试模式
        """
        print("\n" + "="*70)
        print("🚀 产品描述生成器")
        print("="*70)
        print(f"飞书表格: {self.app_token[:8]}... / {self.table_id}")
        print(f"模式: {'测试模式' if dry_run else '执行模式'}")
        print("="*70)
        
        # 读取记录
        print("\n📖 读取飞书记录...")
        try:
            records = self.feishu.read_records(self.app_token, self.table_id)
        except Exception as e:
            print(f"❌ 读取飞书记录失败: {e}")
            return
        
        # 过滤出待开始的任务
        pending_tasks = []
        for record in records:
            fields = record.get('fields', {})
            if fields.get('任务状态') == '待开始':
                pending_tasks.append({
                    'record_id': record.get('record_id'),
                    'fields': fields
                })
        
        print(f"✅ 找到 {len(pending_tasks)} 个待开始的任务")
        
        if dry_run:
            print("\n📋 测试模式 - 待处理任务:")
            for i, task in enumerate(pending_tasks, 1):
                fields = task['fields']
                print(f"  {i}. {fields.get('任务编号')} - {fields.get('产品类型')} ({fields.get('目标语言')})")
            return
        
        if not pending_tasks:
            print("\n✅ 没有待开始的任务")
            return
        
        # 处理任务
        print(f"\n🎯 开始处理 {len(pending_tasks)} 个任务...")
        print("-"*70)
        
        for task in pending_tasks:
            try:
                self.process_task(task)
            except Exception as e:
                print(f"  ❌ 处理异常: {e}")
                self.generator.stats['failed'] += 1
            
            # 短暂延迟
            time.sleep(1)
        
        # 输出统计
        print("\n" + "="*70)
        print("📊 处理统计")
        print("="*70)
        print(f"总计: {len(pending_tasks)}")
        print(f"成功: {self.generator.stats['success']}")
        print(f"失败: {self.generator.stats['failed']}")
        print("="*70)


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='产品描述生成器')
    parser.add_argument('--feishu-url', type=str, help='飞书多维表格 URL')
    parser.add_argument('--dry-run', action='store_true', help='测试模式（只查看不执行）')
    
    args = parser.parse_args()
    
    # 解析飞书 URL
    if args.feishu_url:
        app_token, table_id = extract_from_feishu_url(args.feishu_url)
        if not app_token or not table_id:
            print(f"❌ 无法解析飞书 URL: {args.feishu_url}")
            return
    else:
        print("❌ 请提供飞书 URL")
        return
    
    # 运行流水线
    pipeline = ProductDescriptionPipeline(app_token=app_token, table_id=table_id)
    pipeline.run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
