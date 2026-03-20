#!/usr/bin/env python3
"""
小说爬取服务 - FastAPI Web 服务入口

标准项目结构:
- src/novel_crawler/ - 主包
- src/novel_crawler/api/ - API 路由
- src/novel_crawler/schemas/ - Pydantic 模型
- src/novel_crawler/services/ - 业务服务层
"""

import sys
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

# 修复 Windows 上 Python 3.12+ 与 Playwright 的兼容性问题
# Playwright 需要使用 SelectorEventLoop 来支持子进程
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.api import tasks_router, books_router, stats_router
from src.novel_crawler.tools.cache_manager import load_books_from_db_to_cache, get_cache_stats
from cli.scheduler import FanqieScheduler, ReelShortScheduler, DramaShortScheduler, MultiSiteScheduler


# ==================== 应用生命周期管理 ====================

# 全局多站点调度器实例
_multi_site_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """
    应用生命周期管理

    - 启动时：初始化服务、启动定时任务、预热缓存
    - 关闭时：清理资源、关闭调度器
    """
    global _multi_site_scheduler

    # 启动时执行
    app_version = app.version
    logger.info(f"Novel Crawler 服务启动中... [版本 v{app_version}]")

    # ==================== 启动多站点定时任务 ====================
    # 创建并启动多站点调度器
    _multi_site_scheduler = MultiSiteScheduler(
        sites=["fanqie", "reelshort", "dramashort"],
        fanqie_config={
            "hour": 0,
            "minute": 0,
            "limit": 30,
        },
        reelshort_config={
            "hour": 0,
            "minute": 10,
            "translate": True,
            "translate_workers": 20,
            "translate_llm_batch": 1,
        },
        dramashort_config={
            "hour": 0,
            "minute": 10,
            "translate": True,
            "translate_workers": 20,
            "translate_llm_batch": 1,
        },
    )

    # 在后台启动调度器（非阻塞）
    asyncio.create_task(_multi_site_scheduler.start())
    logger.info("多站点定时任务已启动：")
    logger.info("  - 番茄小说：每天 00:00 爬取榜单（不爬章节）")
    logger.info("  - ReelShort：每天 00:10 全量爬取 + 自动翻译")
    logger.info("  - DramaShorts：每天 00:10 全量爬取 + 自动翻译")

    # ==================== 预热缓存 ====================
    # 检查并预热缓存（如果 Redis 缓存为空，从数据库加载最近一天的数据）
    try:
        stats = get_cache_stats()
        if stats["total_count"] == 0:
            logger.info("Redis 缓存为空，正在从数据库加载最近一天的书籍数据...")
            loaded_count = load_books_from_db_to_cache(force_load=False)
            if loaded_count > 0:
                logger.info(f"缓存预热完成：成功加载 {loaded_count} 本书籍")
            else:
                logger.warning("缓存预热：没有找到可加载的书籍数据")
        else:
            logger.info(f"Redis 缓存已有 {stats['total_count']} 条记录，跳过预热")
    except Exception as e:
        logger.warning(f"缓存预热失败（不影响服务运行）: {e}")

    yield

    # 关闭时执行
    logger.info("Novel Crawler 服务关闭中...")
    if _multi_site_scheduler:
        await _multi_site_scheduler.shutdown()
    logger.info("多站点定时任务已停止")


# ==================== FastAPI 应用 ====================

app = FastAPI(
    title="小说爬取服务 API",
    description="多站点爬取服务 - 已集成番茄小说、ReelShort、DramaShorts，提供定时任务管理和数据查询接口",
    version="1.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# ==================== 中间件配置 ====================

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== 全局异常处理 ====================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """全局异常处理器"""
    logger.error(f"全局异常：{exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc),
            "path": str(request.url.path),
        }
    )


# ==================== 注册路由 ====================

# 健康检查
@app.get("/", tags=["基础"])
async def root():
    """根路径"""
    return {
        "name": "Novel Crawler API",
        "version": "1.1.0",
        "status": "running",
        "integrated_sites": ["fanqie", "reelshort", "dramashort"],
        "docs": "/docs",
    }


@app.get("/health", tags=["基础"])
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
    }


# 注册 API 路由
app.include_router(tasks_router, prefix="/api/v1")
app.include_router(books_router, prefix="/api/v1")
app.include_router(stats_router, prefix="/api/v1")


# ==================== 启动命令 ====================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # 关闭热重载，避免与 Playwright 子进程冲突
        log_level="info",
        # 在 uvicorn 启动日志中显示版本号
        server_header=False,  # 隐藏默认服务器头
    )
