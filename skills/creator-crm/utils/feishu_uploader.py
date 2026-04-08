#!/usr/bin/env python3
"""
飞书文件上传器 - 上传图片到飞书多维表格

功能：
1. 上传本地图片文件到飞书
2. 关联图片到多维表格记录
3. 批量上传支持
"""

import sys
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
import base64

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class FeishuUploader:
    """飞书文件上传器"""
    
    def __init__(
        self,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        table_id: Optional[str] = None
    ):
        """
        初始化飞书上传器
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
            table_id: 多维表格 ID
        """
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        self.table_id = table_id or os.getenv("FEISHU_TABLE_ID")
        
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，将使用模拟模式")
    
    def get_access_token(self) -> Optional[str]:
        """
        获取飞书访问令牌
        
        Returns:
            Optional[str]: 访问令牌
        """
        # TODO: 实际的飞书 API 调用
        # import requests
        # 
        # url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        # payload = {
        #     "app_id": self.app_id,
        #     "app_secret": self.app_secret
        # }
        # 
        # response = requests.post(url, json=payload)
        # data = response.json()
        # 
        # if data.get("code") == 0:
        #     return data.get("tenant_access_token")
        # else:
        #     raise Exception(f"获取 token 失败: {data.get('msg')}")
        
        # 模拟返回
        return "mock_access_token"
    
    def upload_image(
        self,
        image_path: str,
        image_type: str = "image"
    ) -> Optional[str]:
        """
        上传单张图片到飞书
        
        Args:
            image_path: 图片文件路径
            image_type: 图片类型（image/avatar）
            
        Returns:
            Optional[str]: 图片的 file_token
        """
        image_path = Path(image_path)
        
        if not image_path.exists():
            print(f"❌ 图片文件不存在: {image_path}")
            return None
        
        try:
            # TODO: 实际的飞书 API 调用
            # import requests
            # 
            # access_token = self.get_access_token()
            # url = "https://open.feishu.cn/open-apis/im/v1/images"
            # 
            # headers = {
            #     "Authorization": f"Bearer {access_token}"
            # }
            # 
            # files = {
            #     "image": open(image_path, "rb")
            # }
            # 
            # data = {
            #     "image_type": image_type
            # }
            # 
            # response = requests.post(url, headers=headers, files=files, data=data)
            # result = response.json()
            # 
            # if result.get("code") == 0:
            #     return result.get("data", {}).get("image_key")
            # else:
            #     raise Exception(f"上传失败: {result.get('msg')}")
            
            # 模拟上传
            print(f"📤 模拟上传图片: {image_path.name}")
            return f"mock_file_token_{image_path.stem}"
            
        except Exception as e:
            print(f"❌ 上传图片失败: {str(e)}")
            return None
    
    def upload_images_batch(
        self,
        image_paths: List[str]
    ) -> Dict[str, Optional[str]]:
        """
        批量上传图片
        
        Args:
            image_paths: 图片路径列表
            
        Returns:
            Dict: {图片路径: file_token}
        """
        results = {}
        
        for image_path in image_paths:
            file_token = self.upload_image(image_path)
            results[image_path] = file_token
        
        success_count = sum(1 for v in results.values() if v is not None)
        print(f"✅ 批量上传完成: {success_count}/{len(image_paths)} 成功")
        
        return results
    
    def update_record_with_images(
        self,
        record_id: str,
        field_name: str,
        file_tokens: List[str]
    ) -> bool:
        """
        更新记录的图片字段
        
        Args:
            record_id: 记录 ID
            field_name: 字段名称
            file_tokens: 文件 token 列表
            
        Returns:
            bool: 是否成功
        """
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，使用模拟模式")
            return True
        
        try:
            # TODO: 实际的飞书 API 调用
            # import requests
            # 
            # access_token = self.get_access_token()
            # url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.table_id}/tables/{table_id}/records/{record_id}"
            # 
            # headers = {
            #     "Authorization": f"Bearer {access_token}",
            #     "Content-Type": "application/json"
            # }
            # 
            # # 飞书附件字段格式
            # attachments = [{"file_token": token} for token in file_tokens]
            # 
            # payload = {
            #     "fields": {
            #         field_name: attachments
            #     }
            # }
            # 
            # response = requests.put(url, headers=headers, json=payload)
            # result = response.json()
            # 
            # if result.get("code") == 0:
            #     return True
            # else:
            #     raise Exception(f"更新失败: {result.get('msg')}")
            
            # 模拟更新
            print(f"📝 模拟更新记录 {record_id} 的字段 {field_name}")
            return True
            
        except Exception as e:
            print(f"❌ 更新记录失败: {str(e)}")
            return False
    
    def upload_and_attach(
        self,
        record_id: str,
        field_name: str,
        image_paths: List[str]
    ) -> bool:
        """
        上传图片并关联到记录
        
        Args:
            record_id: 记录 ID
            field_name: 字段名称
            image_paths: 图片路径列表
            
        Returns:
            bool: 是否成功
        """
        print(f"🚀 上传并关联图片到记录 {record_id}")
        
        # 1. 批量上传图片
        upload_results = self.upload_images_batch(image_paths)
        
        # 2. 获取成功的 file_tokens
        file_tokens = [
            token for token in upload_results.values()
            if token is not None
        ]
        
        if not file_tokens:
            print("❌ 没有成功上传的图片")
            return False
        
        # 3. 更新记录
        success = self.update_record_with_images(
            record_id,
            field_name,
            file_tokens
        )
        
        if success:
            print(f"✅ 成功关联 {len(file_tokens)} 张图片到记录")
        
        return success


# ============================================================================
# 便捷函数
# ============================================================================

def upload_images_to_feishu(
    record_id: str,
    field_name: str,
    image_paths: List[str],
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    table_id: Optional[str] = None
) -> bool:
    """
    便捷函数：上传图片到飞书并关联到记录
    
    Args:
        record_id: 记录 ID
        field_name: 字段名称
        image_paths: 图片路径列表
        app_id: 飞书应用 ID
        app_secret: 飞书应用密钥
        table_id: 多维表格 ID
        
    Returns:
        bool: 是否成功
        
    Example:
        success = upload_images_to_feishu(
            record_id="rec001",
            field_name="视频宫图",
            image_paths=[
                "output/creator1_grid_1.jpg",
                "output/creator1_grid_2.jpg",
                "output/creator1_grid_3.jpg",
                "output/creator1_grid_4.jpg",
                "output/creator1_grid_5.jpg"
            ]
        )
    """
    uploader = FeishuUploader(app_id, app_secret, table_id)
    return uploader.upload_and_attach(record_id, field_name, image_paths)


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 示例：上传图片到飞书
    
    # 模拟图片路径
    image_paths = [
        "output/test_grid_1.jpg",
        "output/test_grid_2.jpg",
        "output/test_grid_3.jpg",
        "output/test_grid_4.jpg",
        "output/test_grid_5.jpg"
    ]
    
    # 执行上传
    try:
        success = upload_images_to_feishu(
            record_id="rec001",
            field_name="视频宫图",
            image_paths=image_paths
        )
        
        if success:
            print("\n✅ 上传成功")
        else:
            print("\n❌ 上传失败")
            
    except Exception as e:
        print(f"❌ 上传失败: {str(e)}")
