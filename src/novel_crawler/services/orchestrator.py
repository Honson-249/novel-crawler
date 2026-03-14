#!/usr/bin/env python3
"""
爬虫编排器
- 统一爬取入口
- 任务调度管理
- 并发控制
- 优雅停止
"""
import asyncio
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from loguru import logger

# 导入可观测性模块
from src.novel_crawler.observability.metrics_collector import (
    get_metrics_collector,
    CrawlResult,
    record_crawl,
)
from src.novel_crawler.observability.alerting import (
    get_alert_manager,
    Alert,
    AlertLevel,
)
from src.novel_crawler.services.health_check import (
    get_health_checker,
    HealthStatus,
)


# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CrawlOptions:
    """爬取选项"""
    spider_name: str = "fanqie"
    crawl_all: bool = True
    limit: int = 30
    crawl_detail: bool = False
    category_idx: int = 0
    timeout: Optional[int] = None
    retry_times: int = 3
    delay_range: tuple = (1, 3)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlTask:
    """爬取任务"""
    task_id: str
    options: CrawlOptions
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "spider_name": self.options.spider_name,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result": self.result,
            "error": self.error,
        }


class SpiderOrchestrator:
    """
    爬虫编排器
    - 单例模式
    - 统一管理所有爬虫
    - 支持并发控制
    - 支持优雅停止
    """
    _instance: Optional['SpiderOrchestrator'] = None

    def __new__(cls) -> 'SpiderOrchestrator':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # 爬虫注册表
        self._spiders: Dict[str, Any] = {}

        # 任务管理
        self._tasks: Dict[str, CrawlTask] = {}
        self._running_task: Optional[CrawlTask] = None

        # 并发控制
        self._max_concurrent = 1  # 默认单任务
        self._semaphore: Optional[asyncio.Semaphore] = None

        # 停止信号
        self._stop_event = asyncio.Event()

        # 回调函数
        self._on_start_callbacks: List[Callable] = []
        self._on_complete_callbacks: List[Callable] = []

        # 指标和告警
        self._metrics = get_metrics_collector()
        self._alert_manager = get_alert_manager()

        logger.info("爬虫编排器初始化完成")

    def register_spider(self, name: str, spider: Any) -> None:
        """
        注册爬虫实例

        Args:
            name: 爬虫名称
            spider: 爬虫实例
        """
        self._spiders[name] = spider
        logger.info(f"注册爬虫：{name}")

    def unregister_spider(self, name: str) -> None:
        """注销爬虫"""
        if name in self._spiders:
            del self._spiders[name]
            logger.info(f"注销爬虫：{name}")

    def get_spider(self, name: str) -> Optional[Any]:
        """获取爬虫实例"""
        return self._spiders.get(name)

    def set_max_concurrent(self, max_concurrent: int) -> None:
        """设置最大并发数"""
        self._max_concurrent = max(1, max_concurrent)
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        logger.info(f"设置最大并发数：{self._max_concurrent}")

    def on_start(self, callback: Callable) -> None:
        """注册任务开始回调"""
        self._on_start_callbacks.append(callback)

    def on_complete(self, callback: Callable) -> None:
        """注册任务完成回调"""
        self._on_complete_callbacks.append(callback)

    async def execute(self, options: CrawlOptions) -> CrawlTask:
        """
        执行爬取任务

        Args:
            options: 爬取选项

        Returns:
            爬取任务结果
        """
        # 检查是否有运行中的任务
        if self._running_task and self._running_task.status == TaskStatus.RUNNING:
            logger.warning("已有任务正在运行")
            task = CrawlTask(
                task_id=f"rejected_{int(time.time())}",
                options=options,
                status=TaskStatus.FAILED,
                error="已有任务正在运行"
            )
            return task

        # 健康检查
        health = await get_health_checker().check()
        if health.status == HealthStatus.UNHEALTHY:
            logger.error("健康检查失败，无法执行任务")
            task = CrawlTask(
                task_id=f"health_failed_{int(time.time())}",
                options=options,
                status=TaskStatus.FAILED,
                error=f"健康检查失败：{health.checks}"
            )
            return task

        # 创建任务
        task_id = f"task_{int(time.time())}"
        task = CrawlTask(
            task_id=task_id,
            options=options,
            status=TaskStatus.PENDING
        )

        self._tasks[task_id] = task
        self._running_task = task

        # 执行任务
        try:
            logger.info(f"开始执行爬取任务：{task_id}")
            task.status = TaskStatus.RUNNING
            task.start_time = datetime.now(UTC8)

            # 触发开始回调
            for callback in self._on_start_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(task)
                    else:
                        callback(task)
                except Exception as e:
                    logger.error(f"回调执行失败：{e}")

            # 获取爬虫
            spider = self._spiders.get(options.spider_name)
            if not spider:
                raise ValueError(f"爬虫未注册：{options.spider_name}")

            # 重置停止信号
            self._stop_event.clear()

            # 执行爬取
            start_time = time.time()
            result = await self._run_spider(spider, options, task)
            end_time = time.time()

            # 记录指标
            crawl_result = CrawlResult(
                spider_name=options.spider_name,
                success=result.get("success", False),
                start_time=start_time,
                end_time=end_time,
                pages_crawled=result.get("pages_crawled", 0),
                items_extracted=result.get("items_extracted", 0),
                items_stored=result.get("items_stored", 0),
                error_message=result.get("error"),
            )
            record_crawl(crawl_result)

            # 更新任务状态
            task.status = TaskStatus.COMPLETED if result.get("success") else TaskStatus.FAILED
            task.result = result
            task.end_time = datetime.now(UTC8)

            # 发送告警（如果失败）
            if not result.get("success"):
                alert = Alert(
                    title="爬取任务失败",
                    content=f"任务 {task_id} 执行失败",
                    level=AlertLevel.ERROR,
                    spider_name=options.spider_name,
                    error_message=result.get("error"),
                    extra_data=result
                )
                self._alert_manager.send_sync(alert)

            logger.info(f"爬取任务完成：{task_id}, 状态：{task.status.value}")

        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            task.end_time = datetime.now(UTC8)
            logger.info(f"爬取任务已取消：{task_id}")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.end_time = datetime.now(UTC8)
            logger.error(f"爬取任务失败：{task_id}, 错误：{e}")

        finally:
            self._running_task = None

            # 触发完成回调
            for callback in self._on_complete_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(task)
                    else:
                        callback(task)
                except Exception as e:
                    logger.error(f"回调执行失败：{e}")

        return task

    async def _run_spider(
        self,
        spider: Any,
        options: CrawlOptions,
        task: CrawlTask
    ) -> Dict[str, Any]:
        """
        执行爬虫

        Args:
            spider: 爬虫实例
            options: 爬取选项
            task: 爬取任务

        Returns:
            爬取结果
        """
        try:
            # 调用爬虫的 run 方法
            result = await spider.run(
                crawl_all=options.crawl_all,
                target_category_idx=options.category_idx if not options.crawl_all else None,
                limit=options.limit,
                crawl_detail=options.crawl_detail,
            )

            return {
                "success": True,
                "message": "爬取完成",
                "pages_crawled": getattr(spider, '_pages_crawled', 0),
                "items_extracted": getattr(spider, '_items_extracted', 0),
                "items_stored": getattr(spider, '_items_stored', 0),
            }

        except Exception as e:
            logger.error(f"爬虫执行失败：{e}")
            return {
                "success": False,
                "error": str(e),
            }

    async def stop(self) -> bool:
        """
        停止当前运行的任务

        Returns:
            是否成功发送停止信号
        """
        if not self._running_task or self._running_task.status != TaskStatus.RUNNING:
            logger.warning("没有运行中的任务")
            return False

        self._stop_event.set()
        logger.info("已发送停止信号")
        return True

    def get_task(self, task_id: str) -> Optional[CrawlTask]:
        """获取任务状态"""
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[CrawlTask]:
        """获取所有任务"""
        return list(self._tasks.values())

    def get_running_task(self) -> Optional[CrawlTask]:
        """获取运行中的任务"""
        return self._running_task

    def should_stop(self) -> bool:
        """检查是否应该停止"""
        return self._stop_event.is_set()

    async def execute_all(self, options_template: CrawlOptions) -> List[CrawlTask]:
        """
        并行执行所有爬虫

        Args:
            options_template: 爬取选项模板

        Returns:
            任务结果列表
        """
        if not self._semaphore:
            self.set_max_concurrent(self._max_concurrent)

        tasks = []
        for name in self._spiders:
            options = CrawlOptions(
                spider_name=name,
                crawl_all=options_template.crawl_all,
                limit=options_template.limit,
                crawl_detail=options_template.crawl_detail,
                category_idx=options_template.category_idx,
                **options_template.extra
            )
            tasks.append(self.execute(options))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


# ==================== 全局实例 ====================

_orchestrator: Optional[SpiderOrchestrator] = None


def get_orchestrator() -> SpiderOrchestrator:
    """获取全局爬虫编排器实例"""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = SpiderOrchestrator()
    return _orchestrator


async def execute_crawl(options: CrawlOptions) -> CrawlTask:
    """执行爬取任务"""
    return await get_orchestrator().execute(options)


def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """获取任务状态"""
    task = get_orchestrator().get_task(task_id)
    return task.to_dict() if task else None
