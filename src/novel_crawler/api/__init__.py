"""
API 路由模块
"""

from .tasks import router as tasks_router
from .books import router as books_router
from .stats import router as stats_router


__all__ = [
    "tasks_router",
    "books_router",
    "stats_router",
]
