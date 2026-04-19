#!/usr/bin/env python3
"""
Feishu Document Enhanced - 长内容自动分批写入

使用示例:
    from feishu_doc_enhanced import FeishuDocEnhanced
    
    client = FeishuDocEnhanced()
    
    # 方式一：增强版写入
    result = client.write_enhanced(
        doc_token="ABC123def",
        content="# 很长的内容...",
        batch_size=3000
    )
    
    # 方式二：创建并写入
    result = client.create_and_write(
        title="文档标题",
        content="# 很长的内容...",
        folder_token="fldcnXXX",
        batch_size=3000
    )
"""

import re
from typing import List, Dict, Optional


class FeishuDocEnhanced:
    """飞书文档增强版 - 支持长内容自动分批写入"""
    
    def __init__(self, batch_size: int = 3000, max_retries: int = 3):
        """
        初始化
        
        Args:
            batch_size: 每批最大字符数（默认 3000）
            max_retries: 单段最大重试次数（默认 3）
        """
        self.batch_size = batch_size
        self.max_retries = max_retries
    
    def write_enhanced(
        self,
        doc_token: str,
        content: str,
        batch_size: Optional[int] = None,
        auto_create: bool = False
    ) -> Dict:
        """
        智能分批写入长内容
        
        Args:
            doc_token: 文档 token
            content: Markdown 内容
            batch_size: 每批最大字符数（覆盖默认值）
            auto_create: 文档不存在时是否自动创建
            
        Returns:
            {
                "success": bool,
                "segments": int,
                "segment_status": [{"index": int, "success": bool, "chars": int}],
                "error": str (optional)
            }
        """
        batch_size = batch_size or self.batch_size
        
        try:
            # 1. 清理内容
            content = self._clean_content(content)
            
            # 2. 智能分段
            segments = self._split_content(content, batch_size)
            
            if not segments:
                return {"success": False, "error": "内容为空", "segments": 0}
            
            # 3. 写入第一段（清空原有内容）
            segment_status = []
            first_result = self._write_with_retry(doc_token, segments[0], is_append=False)
            segment_status.append({
                "index": 0,
                "success": first_result["success"],
                "chars": len(segments[0])
            })
            
            if not first_result["success"]:
                return {
                    "success": False,
                    "error": f"第一段写入失败: {first_result.get('error', 'unknown')}",
                    "segments": len(segments),
                    "segment_status": segment_status
                }
            
            # 4. 后续批次追加
            for i, segment in enumerate(segments[1:], 1):
                result = self._write_with_retry(doc_token, segment, is_append=True)
                segment_status.append({
                    "index": i,
                    "success": result["success"],
                    "chars": len(segment)
                })
                
                # 如果失败，记录但不中断
                if not result["success"]:
                    print(f"⚠️ 第 {i+1} 段写入失败，继续后续段落...")
            
            # 5. 统计结果
            success_count = sum(1 for s in segment_status if s["success"])
            
            return {
                "success": success_count == len(segments),
                "segments": len(segments),
                "success_segments": success_count,
                "failed_segments": len(segments) - success_count,
                "segment_status": segment_status
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "segments": 0
            }
    
    def create_and_write(
        self,
        title: str,
        content: str,
        folder_token: Optional[str] = None,
        owner_open_id: Optional[str] = None,
        batch_size: Optional[int] = None
    ) -> Dict:
        """
        创建文档并写入长内容
        
        Args:
            title: 文档标题
            content: Markdown 内容
            folder_token: 文件夹 token（可选）
            owner_open_id: 所有者 open_id（可选）
            batch_size: 每批最大字符数
            
        Returns:
            同上
        """
        batch_size = batch_size or self.batch_size
        
        try:
            # 1. 创建文档
            create_params = {
                "action": "create",
                "title": title
            }
            if folder_token:
                create_params["folder_token"] = folder_token
            if owner_open_id:
                create_params["owner_open_id"] = owner_open_id
            
            # 调用原生 API 创建文档
            # doc_result = feishu_doc(**create_params)
            # doc_token = doc_result["document_id"]
            doc_token = "placeholder"  # 实际使用时替换
            
            # 2. 分批写入内容
            return self.write_enhanced(doc_token, content, batch_size)
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _clean_content(self, content: str) -> str:
        """清理内容"""
        # 移除多余的空行
        content = re.sub(r'\n{3,}', '\n\n', content)
        # 移除行尾空格
        content = '\n'.join(line.rstrip() for line in content.split('\n'))
        return content.strip()
    
    def _split_content(self, content: str, batch_size: int) -> List[str]:
        """
        智能分段
        
        策略：
        1. 优先按 ## 标题分割
        2. 其次按 ### 标题分割
        3. 再次按空行分割
        4. 最后强制截断
        """
        if len(content) <= batch_size:
            return [content]
        
        segments = []
        
        # 策略1：按 ## 标题分割
        pattern = r'(?=\n##\s)'
        parts = re.split(pattern, content)
        
        current_segment = ""
        for part in parts:
            if not part.strip():
                continue
                
            # 如果当前段加上新部分超过 batch_size，先保存当前段
            if len(current_segment) + len(part) > batch_size and current_segment:
                segments.append(current_segment.strip())
                current_segment = part
            else:
                current_segment += part
        
        if current_segment:
            segments.append(current_segment.strip())
        
        # 如果按 ## 分割后某段仍过长，进一步分割
        final_segments = []
        for segment in segments:
            if len(segment) > batch_size:
                # 策略2：按 ### 标题分割
                sub_segments = self._split_by_header(segment, '###', batch_size)
                final_segments.extend(sub_segments)
            else:
                final_segments.append(segment)
        
        return final_segments
    
    def _split_by_header(self, content: str, header: str, batch_size: int) -> List[str]:
        """按指定标题层级分割"""
        pattern = f'(?=\n{header}\\s)'
        parts = re.split(pattern, content)
        
        segments = []
        current_segment = ""
        
        for part in parts:
            if not part.strip():
                continue
                
            if len(current_segment) + len(part) > batch_size and current_segment:
                segments.append(current_segment.strip())
                current_segment = part
            else:
                current_segment += part
        
        if current_segment:
            segments.append(current_segment.strip())
        
        # 如果仍过长，强制按段落分割
        final_segments = []
        for segment in segments:
            if len(segment) > batch_size:
                sub_segments = self._split_by_paragraph(segment, batch_size)
                final_segments.extend(sub_segments)
            else:
                final_segments.append(segment)
        
        return final_segments
    
    def _split_by_paragraph(self, content: str, batch_size: int) -> List[str]:
        """按空行（段落）分割"""
        paragraphs = content.split('\n\n')
        
        segments = []
        current_segment = ""
        
        for para in paragraphs:
            if not para.strip():
                continue
                
            # 加上空行分隔
            para_with_sep = para + '\n\n' if current_segment else para
            
            if len(current_segment) + len(para_with_sep) > batch_size and current_segment:
                segments.append(current_segment.strip())
                current_segment = para
            else:
                current_segment += para_with_sep
        
        if current_segment:
            segments.append(current_segment.strip())
        
        # 如果仍过长，强制截断
        final_segments = []
        for segment in segments:
            if len(segment) > batch_size:
                # 强制截断，尽量在句子边界
                final_segments.extend(self._force_split(segment, batch_size))
            else:
                final_segments.append(segment)
        
        return final_segments
    
    def _force_split(self, content: str, batch_size: int) -> List[str]:
        """强制截断（最后手段）"""
        segments = []
        for i in range(0, len(content), batch_size):
            segment = content[i:i+batch_size]
            # 尽量在换行处截断
            if i + batch_size < len(content):
                last_newline = segment.rfind('\n')
                if last_newline > batch_size * 0.8:  # 如果换行位置在80%之后
                    segment = segment[:last_newline]
            segments.append(segment.strip())
        return segments
    
    def _write_with_retry(
        self,
        doc_token: str,
        content: str,
        is_append: bool = False
    ) -> Dict:
        """带重试的写入"""
        action = "append" if is_append else "write"
        
        for attempt in range(self.max_retries):
            try:
                # 调用原生 feishu_doc API
                # result = feishu_doc(action=action, doc_token=doc_token, content=content)
                # return {"success": True, "result": result}
                
                # 模拟成功（实际使用时替换为真实 API 调用）
                return {"success": True}
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    print(f"⚠️ 第 {attempt + 1} 次尝试失败，重试...")
                else:
                    return {"success": False, "error": str(e)}
        
        return {"success": False, "error": "超过最大重试次数"}


# 使用示例
if __name__ == "__main__":
    client = FeishuDocEnhanced(batch_size=3000)
    
    # 测试长内容
    long_content = """
# 测试文档

## 第一章
这是很长的内容...（重复1000次）

## 第二章
这是很长的内容...（重复1000次）

## 第三章
这是很长的内容...（重复1000次）
"""
    
    result = client.write_enhanced(
        doc_token="test_token",
        content=long_content
    )
    
    print(f"写入结果: {result}")
