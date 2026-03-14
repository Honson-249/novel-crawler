"""
统计信息 API 路由
"""

from fastapi import APIRouter, Depends

from src.novel_crawler.schemas.stats import CategoryStats, SummaryStats
from src.novel_crawler.services.book_service import BookService, get_book_service
from src.novel_crawler.tools.cache_manager import get_cache_stats

router = APIRouter(prefix="/stats", tags=["统计信息"])


def get_service() -> BookService:
    """依赖注入：获取书籍服务"""
    return get_book_service()


@router.get("/categories", response_model=list[CategoryStats], summary="获取分类统计")
async def get_category_stats(service: BookService = Depends(get_service)):
    """
    获取各分类的书籍数量统计

    返回每个分类的：
    - 分类名称
    - 书籍数量
    - 最新批次日期
    """
    stats = service.get_category_stats()
    return [CategoryStats(**s) for s in stats]


@router.get("/summary", response_model=SummaryStats, summary="获取汇总统计")
async def get_summary_stats(service: BookService = Depends(get_service)):
    """
    获取数据汇总统计

    返回：
    - 总书籍数
    - 总记录数
    - 最新批次日期
    - 分类数量
    """
    stats = service.get_summary_stats()
    return SummaryStats(**stats)


@router.get("/cache", summary="获取缓存统计")
async def get_redis_cache_stats():
    """
    获取 Redis 缓存统计

    返回：
    - 总缓存数量
    - 连载中数量
    - 已完结数量
    """
    return get_cache_stats()
