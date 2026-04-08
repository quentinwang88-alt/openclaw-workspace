"""
自定义异常类 - Creator CRM 系统

提供细粒度的错误分类，便于调试和针对性处理
"""


class CreatorCRMError(Exception):
    """Creator CRM 系统基础异常类"""
    pass


class KalodataAPIError(CreatorCRMError):
    """Kalodata API 调用失败"""
    
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Kalodata API Error [{status_code}]: {message}")


class DataExtractionError(CreatorCRMError):
    """数据提取失败"""
    pass


class VisionAPIError(CreatorCRMError):
    """Vision API 调用失败"""
    
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Vision API Error [{status_code}]: {message}")


class InsufficientDataError(CreatorCRMError):
    """数据不足，无法分析"""
    
    def __init__(self, required: int, actual: int, data_type: str = "视频"):
        self.required = required
        self.actual = actual
        self.data_type = data_type
        super().__init__(
            f"数据不足：需要至少 {required} 个{data_type}，实际只有 {actual} 个"
        )


class ImageProcessingError(CreatorCRMError):
    """图片处理失败"""
    pass


class CacheError(CreatorCRMError):
    """缓存操作失败"""
    pass
