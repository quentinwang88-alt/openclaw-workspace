#!/usr/bin/env python3
"""
任务队列管理系统
支持异步任务、优先级队列、断点续传、状态追踪
"""

import json
import time
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from enum import Enum
from dataclasses import dataclass, asdict
import threading
import queue


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"


class TaskPriority(Enum):
    """任务优先级"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class Task:
    """任务数据结构"""
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    heartbeat_at: Optional[str] = None  # 心跳时间戳，用于检测僵尸任务
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = asdict(self)
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """从字典创建"""
        data['priority'] = TaskPriority(data['priority'])
        data['status'] = TaskStatus(data['status'])
        return cls(**data)


class TaskQueue:
    """任务队列管理器"""
    
    def __init__(self, state_file: str = None):
        # 使用绝对路径，确保从任何目录运行都能找到状态文件
        if state_file is None:
            # 默认放在 creator-crm/output/ 目录下
            skill_dir = Path(__file__).parent.parent
            state_file = str(skill_dir / "output" / "task_queue_state.json")
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.tasks: Dict[str, Task] = {}
        self.queue = queue.PriorityQueue()
        self.lock = threading.Lock()
        
        self._load_state()
    
    def _load_state(self):
        """加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for task_data in data.get('tasks', []):
                        # 兼容旧版本任务数据（没有 heartbeat_at 字段）
                        if 'heartbeat_at' not in task_data:
                            task_data['heartbeat_at'] = None
                        task = Task.from_dict(task_data)
                        self.tasks[task.task_id] = task
                        
                        # 重新加入待处理和重试的任务
                        if task.status in [TaskStatus.PENDING, TaskStatus.RETRY]:
                            self.queue.put((-task.priority.value, task.task_id))
            except Exception as e:
                print(f"⚠️ 加载状态失败: {e}")
    
    def _save_state(self):
        """保存状态"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'tasks': [task.to_dict() for task in self.tasks.values()],
                    'updated_at': datetime.now().isoformat()
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 保存状态失败: {e}")
    
    def add_task(self, task: Task) -> str:
        """添加任务"""
        with self.lock:
            self.tasks[task.task_id] = task
            self.queue.put((-task.priority.value, task.task_id))
            self._save_state()
        return task.task_id
    
    def get_task(self, timeout: Optional[float] = None) -> Optional[Task]:
        """获取下一个任务"""
        try:
            _, task_id = self.queue.get(timeout=timeout)
            with self.lock:
                task = self.tasks.get(task_id)
                if task and task.status in [TaskStatus.PENDING, TaskStatus.RETRY]:
                    task.status = TaskStatus.RUNNING
                    task.started_at = datetime.now().isoformat()
                    task.heartbeat_at = datetime.now().isoformat()  # 初始化心跳
                    self._save_state()
                    return task
        except queue.Empty:
            return None
    
    def update_heartbeat(self, task_id: str):
        """更新任务心跳时间"""
        with self.lock:
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.RUNNING:
                task.heartbeat_at = datetime.now().isoformat()
                self._save_state()
    
    def recover_zombie_tasks(self, timeout_seconds: int = 300) -> int:
        """
        检测并恢复僵尸任务
        
        僵尸任务定义：状态为 RUNNING 但心跳超时的任务
        
        Args:
            timeout_seconds: 心跳超时时间（秒），默认5分钟
            
        Returns:
            恢复的僵尸任务数量
        """
        recovered = 0
        now = datetime.now()
        
        with self.lock:
            for task in self.tasks.values():
                if task.status != TaskStatus.RUNNING:
                    continue
                
                # 检查心跳超时
                if task.heartbeat_at:
                    heartbeat_time = datetime.fromisoformat(task.heartbeat_at)
                    elapsed = (now - heartbeat_time).total_seconds()
                elif task.started_at:
                    # 兼容旧任务：使用 started_at
                    started_time = datetime.fromisoformat(task.started_at)
                    elapsed = (now - started_time).total_seconds()
                else:
                    continue
                
                if elapsed > timeout_seconds:
                    # 僵尸任务：重置为待处理状态
                    print(f"⚠️ 检测到僵尸任务: {task.task_id} (超时 {int(elapsed)}秒)")
                    
                    # 增加重试计数
                    task.retry_count += 1
                    
                    if task.retry_count >= task.max_retries:
                        # 超过最大重试次数，标记为失败
                        task.status = TaskStatus.FAILED
                        task.error = f"任务超时，已重试 {task.retry_count} 次"
                        task.completed_at = now.isoformat()
                        print(f"❌ 任务 {task.task_id} 已达到最大重试次数，标记为失败")
                    else:
                        # 重置为待处理状态
                        task.status = TaskStatus.PENDING
                        task.started_at = None
                        task.heartbeat_at = None
                        task.error = f"僵尸任务恢复 (超时 {int(elapsed)}秒)"
                        self.queue.put((-task.priority.value, task.task_id))
                        print(f"🔄 任务 {task.task_id} 已恢复为待处理 (重试 {task.retry_count}/{task.max_retries})")
                    
                    recovered += 1
            
            if recovered > 0:
                self._save_state()
        
        return recovered
    
    def update_task(self, task_id: str, status: TaskStatus, 
                   result: Optional[Dict[str, Any]] = None,
                   error: Optional[str] = None):
        """更新任务状态"""
        with self.lock:
            task = self.tasks.get(task_id)
            if task:
                task.status = status
                task.completed_at = datetime.now().isoformat()
                
                if result:
                    task.result = result
                if error:
                    task.error = error
                
                # 失败重试逻辑
                if status == TaskStatus.FAILED and task.retry_count < task.max_retries:
                    task.retry_count += 1
                    task.status = TaskStatus.RETRY
                    self.queue.put((-task.priority.value, task_id))
                
                self._save_state()
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        with self.lock:
            stats = {
                'total': len(self.tasks),
                'pending': 0,
                'running': 0,
                'success': 0,
                'failed': 0,
                'retry': 0,
                'cancelled': 0
            }
            for task in self.tasks.values():
                stats[task.status.value] += 1
            return stats
    
    def get_task_by_id(self, task_id: str) -> Optional[Task]:
        """根据ID获取任务"""
        return self.tasks.get(task_id)
    
    def cancel_task(self, task_id: str):
        """取消任务"""
        with self.lock:
            task = self.tasks.get(task_id)
            if task and task.status in [TaskStatus.PENDING, TaskStatus.RETRY]:
                task.status = TaskStatus.CANCELLED
                self._save_state()
    
    def clear_completed(self):
        """清除已完成的任务"""
        with self.lock:
            completed_ids = [
                task_id for task_id, task in self.tasks.items()
                if task.status in [TaskStatus.SUCCESS, TaskStatus.CANCELLED]
            ]
            for task_id in completed_ids:
                del self.tasks[task_id]
            self._save_state()


class AsyncTaskExecutor:
    """异步任务执行器"""
    
    # 僵尸任务检测配置
    ZOMBIE_TIMEOUT = 300  # 5分钟无心跳视为僵尸任务
    ZOMBIE_CHECK_INTERVAL = 60  # 每60秒检测一次僵尸任务
    HEARTBEAT_INTERVAL = 30  # 每30秒更新一次心跳
    
    def __init__(self, task_queue: TaskQueue, max_workers: int = 3):
        self.task_queue = task_queue
        self.max_workers = max_workers
        self.handlers: Dict[str, Callable] = {}
        self.running = False
        self.workers: List[threading.Thread] = []
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.zombie_thread: Optional[threading.Thread] = None
        # 使用线程安全的字典来跟踪每个 worker 的当前任务
        self._worker_tasks: Dict[int, str] = {}
        self._worker_tasks_lock = threading.Lock()
    
    def register_handler(self, task_type: str, handler: Callable):
        """注册任务处理器"""
        self.handlers[task_type] = handler
    
    def _set_worker_task(self, worker_id: int, task_id: Optional[str]):
        """设置 worker 的当前任务（线程安全）"""
        with self._worker_tasks_lock:
            if task_id:
                self._worker_tasks[worker_id] = task_id
            elif worker_id in self._worker_tasks:
                del self._worker_tasks[worker_id]
    
    def _heartbeat_worker(self):
        """心跳工作线程 - 定期更新所有运行中任务的心跳"""
        while self.running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            
            if not self.running:
                break
            
            # 更新所有 worker 当前任务的心跳
            with self._worker_tasks_lock:
                task_ids = list(self._worker_tasks.values())
            
            for task_id in task_ids:
                try:
                    self.task_queue.update_heartbeat(task_id)
                except Exception as e:
                    print(f"⚠️ 更新心跳失败 {task_id}: {e}")
    
    def _zombie_detector(self):
        """僵尸任务检测线程"""
        while self.running:
            time.sleep(self.ZOMBIE_CHECK_INTERVAL)
            
            if not self.running:
                break
            
            try:
                # 检测并恢复僵尸任务
                recovered = self.task_queue.recover_zombie_tasks(self.ZOMBIE_TIMEOUT)
                if recovered > 0:
                    print(f"🔄 已恢复 {recovered} 个僵尸任务")
            except Exception as e:
                print(f"⚠️ 僵尸检测出错: {e}")
    
    def _worker(self, worker_id: int):
        """工作线程"""
        print(f"🔧 Worker {worker_id} 启动")
        
        while self.running:
            task = None
            try:
                task = self.task_queue.get_task(timeout=1.0)
                
                if task is None:
                    continue
                
                # 设置当前任务ID（用于心跳）- 线程安全
                self._set_worker_task(worker_id, task.task_id)
                
                print(f"🔄 Worker {worker_id} 处理任务: {task.task_id} ({task.task_type})")
                
                handler = self.handlers.get(task.task_type)
                if not handler:
                    self.task_queue.update_task(
                        task.task_id,
                        TaskStatus.FAILED,
                        error=f"未找到处理器: {task.task_type}"
                    )
                    self._set_worker_task(worker_id, None)
                    continue
                
                try:
                    result = handler(task.payload)
                    self.task_queue.update_task(
                        task.task_id,
                        TaskStatus.SUCCESS,
                        result=result
                    )
                    print(f"✅ Worker {worker_id} 完成任务: {task.task_id}")
                    
                except Exception as e:
                    import traceback
                    error_detail = traceback.format_exc()
                    print(f"❌ Worker {worker_id} 任务失败: {task.task_id} - {e}")
                    print(f"   详细错误:\n{error_detail}")
                    self.task_queue.update_task(
                        task.task_id,
                        TaskStatus.FAILED,
                        error=f"{e}\n{error_detail[:500]}"  # 限制错误信息长度
                    )
                finally:
                    self._set_worker_task(worker_id, None)
                
            except Exception as e:
                import traceback
                print(f"⚠️ Worker {worker_id} 错误: {e}")
                print(f"   详细错误:\n{traceback.format_exc()}")
                self._set_worker_task(worker_id, None)
                time.sleep(1)  # 防止错误循环占用CPU
        
        print(f"🛑 Worker {worker_id} 停止")
    
    def start(self):
        """启动执行器"""
        if self.running:
            return
        
        # 启动时先检测并恢复僵尸任务
        try:
            recovered = self.task_queue.recover_zombie_tasks(self.ZOMBIE_TIMEOUT)
            if recovered > 0:
                print(f"🔄 启动时恢复了 {recovered} 个僵尸任务")
        except Exception as e:
            print(f"⚠️ 启动时僵尸检测出错: {e}")
        
        self.running = True
        self.workers = []
        self._worker_tasks = {}
        
        # 启动工作线程
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker, args=(i,), daemon=True)
            worker.start()
            self.workers.append(worker)
        
        # 启动心跳线程
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
        self.heartbeat_thread.start()
        
        # 启动僵尸任务检测线程
        self.zombie_thread = threading.Thread(target=self._zombie_detector, daemon=True)
        self.zombie_thread.start()
        
        print(f"🚀 任务执行器启动 ({self.max_workers} workers, 僵尸检测: {self.ZOMBIE_TIMEOUT}秒超时)")
    
    def stop(self):
        """停止执行器"""
        self.running = False
        for worker in self.workers:
            worker.join(timeout=5)
        print("🛑 任务执行器停止")
    
    def wait_completion(self, check_interval: float = 2.0):
        """等待所有任务完成"""
        while True:
            stats = self.task_queue.get_stats()
            pending = stats['pending'] + stats['running'] + stats['retry']
            
            if pending == 0:
                break
            
            print(f"⏳ 等待任务完成... (待处理: {pending}, 运行中: {stats['running']}, 成功: {stats['success']}, 失败: {stats['failed']})")
            time.sleep(check_interval)
