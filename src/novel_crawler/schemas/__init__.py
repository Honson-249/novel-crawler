"""
Pydantic 模型 / Schemas
"""

from .task import (
    TaskStatus,
    CrawlRequest,
    CrawlResponse,
    ScheduleRequest,
    ScheduleResponse,
)

from .book import (
    BookBase,
    BookCreate,
    BookData,
    BookDetail,
    BookListResponse,
)

from .stats import (
    CategoryStats,
    SummaryStats,
)


__all__ = [
    # Task
    "TaskStatus",
    "CrawlRequest",
    "CrawlResponse",
    "ScheduleRequest",
    "ScheduleResponse",
    # Book
    "BookBase",
    "BookCreate",
    "BookData",
    "BookDetail",
    "BookListResponse",
    # Stats
    "CategoryStats",
    "SummaryStats",
]
