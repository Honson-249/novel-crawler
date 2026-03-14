"""
服务层 - 业务服务和爬虫编排

使用延迟导入避免循环依赖
"""

from typing import Any


def __getattr__(name: str) -> Any:
    """延迟导入，避免循环依赖"""
    if name in ("TaskService", "get_task_service"):
        from .task_service import TaskService, get_task_service
        return locals()[name]
    elif name in ("BookService", "get_book_service"):
        from .book_service import BookService, get_book_service
        return locals()[name]
    elif name in ("ChapterService", "get_chapter_service"):
        from .chapter_service import ChapterService, get_chapter_service
        return locals()[name]
    elif name in ("SpiderOrchestrator", "get_orchestrator", "execute_crawl",
                  "get_task_status", "CrawlOptions", "CrawlTask", "TaskStatus"):
        from .orchestrator import (
            SpiderOrchestrator,
            get_orchestrator,
            execute_crawl,
            get_task_status,
            CrawlOptions,
            CrawlTask,
            TaskStatus,
        )
        return locals()[name]
    elif name in ("HealthChecker", "get_health_checker", "check_health",
                  "get_health_status", "HealthStatus", "HealthCheckResult"):
        from .health_check import (
            HealthChecker,
            get_health_checker,
            check_health,
            get_health_status,
            HealthStatus,
            HealthCheckResult,
        )
        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# 显式导出列表
__all__ = [
    # Task Service
    "TaskService",
    "get_task_service",
    # Book Service
    "BookService",
    "get_book_service",
    # Chapter Service
    "ChapterService",
    "get_chapter_service",
    # Orchestrator
    "SpiderOrchestrator",
    "get_orchestrator",
    "execute_crawl",
    "get_task_status",
    "CrawlOptions",
    "CrawlTask",
    "TaskStatus",
    # Health Check
    "HealthChecker",
    "get_health_checker",
    "check_health",
    "get_health_status",
    "HealthStatus",
    "HealthCheckResult",
]
