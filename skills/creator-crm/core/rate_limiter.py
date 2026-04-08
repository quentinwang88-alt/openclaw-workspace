#!/usr/bin/env python3
"""
限流器和熔断器
防止API过载和系统崩溃
"""

import time
import threading
from typing import Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque


@dataclass
class RateLimiterConfig:
    """限流器配置"""
    max_requests: int  # 最大请求数
    time_window: float  # 时间窗口（秒）
    burst_size: int = 0  # 突发容量（0表示不允许突发）


class RateLimiter:
    """令牌桶限流器"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self.tokens = config.max_requests
        self.last_update = time.time()
        self.lock = threading.Lock()
        
        # 计算令牌生成速率
        self.rate = config.max_requests / config.time_window
    
    def acquire(self, tokens: int = 1, timeout: Optional[float] = None, default_timeout: float = 60.0) -> bool:
        """
        获取令牌
        
        Args:
            tokens: 需要的令牌数
            timeout: 超时时间（秒），None表示使用默认超时
            default_timeout: 默认超时时间（秒），防止无限等待
        
        Returns:
            bool: 是否成功获取令牌
        """
        # 防止无限等待：如果 timeout 为 None，使用默认超时
        actual_timeout = timeout if timeout is not None else default_timeout
        start_time = time.time()
        
        while True:
            with self.lock:
                self._refill()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
            
            # 检查超时
            elapsed = time.time() - start_time
            if elapsed >= actual_timeout:
                return False
            
            # 等待一小段时间（动态调整等待时间）
            wait_time = min(0.1, actual_timeout - elapsed)
            if wait_time > 0:
                time.sleep(wait_time)
    
    def _refill(self):
        """补充令牌"""
        now = time.time()
        elapsed = now - self.last_update
        
        # 计算应该补充的令牌数
        new_tokens = elapsed * self.rate
        
        # 更新令牌数（不超过最大值+突发容量）
        max_tokens = self.config.max_requests + self.config.burst_size
        self.tokens = min(self.tokens + new_tokens, max_tokens)
        
        self.last_update = now
    
    def get_available_tokens(self) -> float:
        """获取当前可用令牌数"""
        with self.lock:
            self._refill()
            return self.tokens


class SlidingWindowRateLimiter:
    """滑动窗口限流器"""
    
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
    
    def acquire(self, timeout: Optional[float] = None, default_timeout: float = 60.0) -> bool:
        """获取许可
        
        Args:
            timeout: 超时时间（秒），None表示使用默认超时
            default_timeout: 默认超时时间（秒），防止无限等待
        """
        # 防止无限等待
        actual_timeout = timeout if timeout is not None else default_timeout
        start_time = time.time()
        
        while True:
            with self.lock:
                now = time.time()
                
                # 移除过期的请求记录
                while self.requests and self.requests[0] < now - self.time_window:
                    self.requests.popleft()
                
                # 检查是否可以通过
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return True
            
            # 检查超时
            elapsed = time.time() - start_time
            if elapsed >= actual_timeout:
                return False
            
            # 等待（动态调整等待时间）
            wait_time = min(0.1, actual_timeout - elapsed)
            if wait_time > 0:
                time.sleep(wait_time)
    
    def get_current_rate(self) -> int:
        """获取当前请求数"""
        with self.lock:
            now = time.time()
            while self.requests and self.requests[0] < now - self.time_window:
                self.requests.popleft()
            return len(self.requests)


@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5  # 失败阈值
    success_threshold: int = 2  # 成功阈值（半开状态）
    timeout: float = 60.0  # 熔断超时（秒）
    half_open_max_calls: int = 3  # 半开状态最大调用次数


class CircuitBreakerState:
    """熔断器状态"""
    CLOSED = "closed"  # 正常状态
    OPEN = "open"  # 熔断状态
    HALF_OPEN = "half_open"  # 半开状态


class CircuitBreaker:
    """熔断器"""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self.lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        通过熔断器调用函数
        
        Args:
            func: 要调用的函数
            *args, **kwargs: 函数参数
        
        Returns:
            函数返回值
        
        Raises:
            Exception: 熔断器打开时抛出异常
        """
        with self.lock:
            # 检查是否可以调用
            if not self._can_call():
                raise Exception(f"熔断器打开，拒绝调用 (失败次数: {self.failure_count})")
            
            # 半开状态计数
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_calls += 1
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise e
    
    def _can_call(self) -> bool:
        """检查是否可以调用"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            # 检查是否超时，可以进入半开状态
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.config.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    return True
            return False
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            # 半开状态限制调用次数
            return self.half_open_calls < self.config.half_open_max_calls
        
        return False
    
    def _on_success(self):
        """成功回调"""
        with self.lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                
                # 达到成功阈值，关闭熔断器
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    print("✅ 熔断器关闭")
            
            elif self.state == CircuitBreakerState.CLOSED:
                # 重置失败计数
                self.failure_count = max(0, self.failure_count - 1)
    
    def _on_failure(self):
        """失败回调"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                # 半开状态失败，重新打开
                self.state = CircuitBreakerState.OPEN
                self.success_count = 0
                print(f"⚠️ 熔断器重新打开 (失败次数: {self.failure_count})")
            
            elif self.state == CircuitBreakerState.CLOSED:
                # 达到失败阈值，打开熔断器
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    print(f"🔴 熔断器打开 (失败次数: {self.failure_count})")
    
    def get_state(self) -> str:
        """获取当前状态"""
        return self.state
    
    def reset(self):
        """重置熔断器"""
        with self.lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.half_open_calls = 0
            print("🔄 熔断器已重置")


class AdaptiveRateLimiter:
    """自适应限流器 - 根据系统负载动态调整"""
    
    def __init__(self, initial_rate: int, time_window: float,
                 min_rate: int = 1, max_rate: int = 100):
        self.current_rate = initial_rate
        self.time_window = time_window
        self.min_rate = min_rate
        self.max_rate = max_rate
        
        self.limiter = SlidingWindowRateLimiter(initial_rate, time_window)
        self.success_count = 0
        self.failure_count = 0
        self.lock = threading.Lock()
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """获取许可"""
        return self.limiter.acquire(timeout)
    
    def report_success(self):
        """报告成功"""
        with self.lock:
            self.success_count += 1
            
            # 每10次成功，尝试提高速率
            if self.success_count >= 10:
                self._increase_rate()
                self.success_count = 0
    
    def report_failure(self):
        """报告失败"""
        with self.lock:
            self.failure_count += 1
            
            # 每3次失败，降低速率
            if self.failure_count >= 3:
                self._decrease_rate()
                self.failure_count = 0
    
    def _increase_rate(self):
        """提高速率"""
        new_rate = min(int(self.current_rate * 1.2), self.max_rate)
        if new_rate != self.current_rate:
            self.current_rate = new_rate
            self.limiter = SlidingWindowRateLimiter(new_rate, self.time_window)
            print(f"📈 限流速率提高到: {new_rate}/{self.time_window}s")
    
    def _decrease_rate(self):
        """降低速率"""
        new_rate = max(int(self.current_rate * 0.7), self.min_rate)
        if new_rate != self.current_rate:
            self.current_rate = new_rate
            self.limiter = SlidingWindowRateLimiter(new_rate, self.time_window)
            print(f"📉 限流速率降低到: {new_rate}/{self.time_window}s")
    
    def get_current_rate(self) -> int:
        """获取当前速率"""
        return self.current_rate
