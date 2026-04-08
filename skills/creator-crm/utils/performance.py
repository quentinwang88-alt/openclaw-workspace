"""
性能监控和日志系统

提供：
1. 性能计时器
2. 结构化日志
3. 统计信息收集
"""

import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any
from pathlib import Path


# ============================================================================
# 日志配置
# ============================================================================

def setup_logger(
    name: str = "creator_crm",
    log_file: Optional[str] = "logs/creator_crm.log",
    level: int = logging.INFO
) -> logging.Logger:
    """
    配置日志系统
    
    Args:
        name: Logger 名称
        log_file: 日志文件路径（None 则只输出到控制台）
        level: 日志级别
        
    Returns:
        logging.Logger: 配置好的 Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加 handler
    if logger.handlers:
        return logger
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件 handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# 全局 logger
logger = setup_logger()


# ============================================================================
# 性能计时器
# ============================================================================

@contextmanager
def timer(name: str, logger: Optional[logging.Logger] = None):
    """
    性能计时器（上下文管理器）
    
    Args:
        name: 计时器名称
        logger: Logger 实例（None 则使用全局 logger）
        
    Yields:
        None
        
    Example:
        with timer("数据获取"):
            fetch_data()
    """
    _logger = logger or globals()['logger']
    
    start = time.time()
    _logger.info(f"⏱️  {name} - 开始")
    
    try:
        yield
    finally:
        elapsed = time.time() - start
        _logger.info(f"✅ {name} - 完成 ({elapsed:.2f}s)")


class PerformanceTimer:
    """性能计时器（类版本，支持多次计时）"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or globals()['logger']
        self.timers: Dict[str, float] = {}
        self.start_times: Dict[str, float] = {}
    
    def start(self, name: str):
        """开始计时"""
        self.start_times[name] = time.time()
        self.logger.info(f"⏱️  {name} - 开始")
    
    def stop(self, name: str) -> float:
        """停止计时并返回耗时"""
        if name not in self.start_times:
            self.logger.warning(f"⚠️  计时器 '{name}' 未启动")
            return 0.0
        
        elapsed = time.time() - self.start_times[name]
        self.timers[name] = elapsed
        self.logger.info(f"✅ {name} - 完成 ({elapsed:.2f}s)")
        
        del self.start_times[name]
        return elapsed
    
    def get_summary(self) -> Dict[str, float]:
        """获取所有计时器的汇总"""
        return self.timers.copy()
    
    def log_summary(self):
        """输出汇总信息"""
        if not self.timers:
            self.logger.info("📊 无计时数据")
            return
        
        self.logger.info("📊 性能汇总:")
        total = sum(self.timers.values())
        
        for name, elapsed in sorted(self.timers.items(), key=lambda x: x[1], reverse=True):
            percentage = (elapsed / total * 100) if total > 0 else 0
            self.logger.info(f"   {name}: {elapsed:.2f}s ({percentage:.1f}%)")
        
        self.logger.info(f"   总计: {total:.2f}s")


# ============================================================================
# 统计信息收集器
# ============================================================================

class StatsCollector:
    """统计信息收集器"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or globals()['logger']
        self.stats: Dict[str, Any] = {}
    
    def record(self, key: str, value: Any):
        """记录统计信息"""
        self.stats[key] = value
    
    def increment(self, key: str, delta: int = 1):
        """增量统计"""
        self.stats[key] = self.stats.get(key, 0) + delta
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取统计信息"""
        return self.stats.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """获取所有统计信息"""
        return self.stats.copy()
    
    def log_summary(self):
        """输出统计汇总"""
        if not self.stats:
            self.logger.info("📊 无统计数据")
            return
        
        self.logger.info("📊 统计汇总:")
        for key, value in self.stats.items():
            self.logger.info(f"   {key}: {value}")


# ============================================================================
# 便捷函数
# ============================================================================

def log_info(message: str, logger: Optional[logging.Logger] = None):
    """记录 INFO 日志"""
    _logger = logger or globals()['logger']
    _logger.info(message)


def log_warning(message: str, logger: Optional[logging.Logger] = None):
    """记录 WARNING 日志"""
    _logger = logger or globals()['logger']
    _logger.warning(message)


def log_error(message: str, logger: Optional[logging.Logger] = None):
    """记录 ERROR 日志"""
    _logger = logger or globals()['logger']
    _logger.error(message)


def log_success(message: str, logger: Optional[logging.Logger] = None):
    """记录成功信息"""
    _logger = logger or globals()['logger']
    _logger.info(f"✅ {message}")


def log_failure(message: str, logger: Optional[logging.Logger] = None):
    """记录失败信息"""
    _logger = logger or globals()['logger']
    _logger.error(f"❌ {message}")
