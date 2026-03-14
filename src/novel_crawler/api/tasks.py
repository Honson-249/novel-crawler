"""
任务管理 API 路由
"""

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from src.novel_crawler.schemas.task import (
    TaskStatus,
    CrawlRequest,
    CrawlResponse,
    ScheduleRequest,
    ScheduleResponse,
)
from src.novel_crawler.services.task_service import TaskService, get_task_service

router = APIRouter(prefix="/task", tags=["任务管理"])


def get_service() -> TaskService:
    """依赖注入：获取任务服务"""
    return get_task_service()


@router.get("/status", response_model=TaskStatus, summary="获取任务状态")
async def get_task_status(service: TaskService = Depends(get_service)):
    """
    获取当前任务状态信息

    返回调度器运行状态、下次执行时间、上次执行时间和状态
    """
    status = service.get_status()
    return TaskStatus(**status)


@router.post("/start", response_model=CrawlResponse, summary="启动爬取任务")
async def start_task(
    request: CrawlRequest,
    service: TaskService = Depends(get_service)
):
    """
    立即开始爬取任务

    - **crawl_all**: 是否爬取所有分类
    - **limit**: 每个分类爬取的书籍数量 (1-100)
    - **detail**: 是否爬取详情页（获取章节列表）
    - **category_idx**: 分类索引（不爬取所有时使用）
    """
    if service.crawl_task_running:
        raise HTTPException(status_code=400, detail="爬取任务正在运行中")

    # 异步执行爬取任务
    import asyncio
    asyncio.create_task(
        service.execute_crawl(
            crawl_all=request.crawl_all,
            limit=request.limit,
            detail=request.detail,
            category_idx=request.category_idx
        )
    )

    return CrawlResponse(
        status="accepted",
        message="爬取任务已启动",
    )


@router.post("/stop", response_model=CrawlResponse, summary="停止爬取任务")
async def stop_task(service: TaskService = Depends(get_service)):
    """停止当前运行中的爬取任务"""
    stopped = await service.stop_current_task()

    if stopped:
        return CrawlResponse(status="success", message="已发送停止信号")
    return CrawlResponse(status="info", message="当前没有运行中的任务")


@router.post("/schedule", response_model=ScheduleResponse, summary="设置定时任务")
async def update_schedule(
    request: ScheduleRequest,
    service: TaskService = Depends(get_service)
):
    """
    设置每日定时爬取任务

    - **hour**: 执行时间 - 小时 (0-23)
    - **minute**: 执行时间 - 分钟 (0-59)
    - **crawl_all**: 是否爬取所有分类
    - **limit**: 每个分类爬取的书籍数量
    - **detail**: 是否爬取详情页
    """
    # 添加定时任务
    service.add_daily_job(
        crawl_all=request.crawl_all,
        limit=request.limit,
        detail=request.detail,
        hour=request.hour,
        minute=request.minute
    )

    # 重新调度
    next_run = service.reschedule_job('daily_crawl', request.hour, request.minute)

    return ScheduleResponse(
        status="success",
        message=f"定时任务已更新为每天 {request.hour:02d}:{request.minute:02d} 执行",
        next_run_time=next_run
    )


@router.delete("/schedule", response_model=CrawlResponse, summary="删除定时任务")
async def remove_schedule(service: TaskService = Depends(get_service)):
    """删除每日定时爬取任务"""
    removed = service.remove_job('daily_crawl')

    if removed:
        return CrawlResponse(status="success", message="定时任务已删除")
    return CrawlResponse(status="error", message="删除失败")
