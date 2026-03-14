"""
任务服务 - 负责爬虫任务调度和管理
"""

import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.novel_crawler.spiders.fanqie import FanqieSpider as HumanSimulatedSpider


class TaskService:
    """任务服务"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.crawl_task_running = False
        self.last_run_time: Optional[datetime] = None
        self.last_run_status: Optional[str] = None
        self._stop_event = asyncio.Event()

    async def execute_crawl(
        self,
        crawl_all: bool = True,
        limit: int = 30,
        detail: bool = False,
        category_idx: int = 0,
        auto: bool = True,
        detail_limit: int = 0
    ) -> Dict[str, Any]:
        """
        执行爬取任务（支持 auto 模式：两轮完整爬取 + 查缺补漏）

        Args:
            crawl_all: 是否爬取所有分类
            limit: 每个分类爬取数量
            detail: 是否爬取详情
            category_idx: 分类索引
            auto: 是否启用自动两阶段 + 第二轮重检（默认 True）
            detail_limit: 补充详情时最多爬取多少本，0 表示全部

        Returns:
            执行结果
        """
        if self.crawl_task_running:
            logger.warning("爬取任务正在运行中，无法重复执行")
            return {"status": "error", "message": "爬取任务正在运行中"}

        self.crawl_task_running = True
        self.last_run_time = datetime.now()

        try:
            logger.info("开始执行爬取任务")
            spider = HumanSimulatedSpider()

            if auto:
                # 自动两阶段爬取 + 第二轮重检（查缺补漏）
                logger.info("【第一轮】阶段 1: 快速爬取榜单页")
                await spider.run(
                    crawl_all=crawl_all,
                    target_category_idx=category_idx if not crawl_all else None,
                    limit=limit,
                    crawl_detail=False,
                    crawl_detail_later=False,
                    skip_crawled=True
                )

                logger.info("【第一轮】阶段 2: 补充爬取详情页")
                result_phase2 = await spider.crawl_missing_details(limit=detail_limit)
                first_round_remaining = result_phase2.get('remaining', 0)

                # 第二轮：完整重跑两阶段（应对榜单页遗漏 + 详情页遗漏）
                if first_round_remaining > 0:
                    logger.info(f"【第二轮】检测到 {first_round_remaining} 本书缺失，开始完整重跑两阶段（查缺补漏）")

                    # 第二轮 - 阶段 1: 重新爬取榜单页（强制重爬所有分类）
                    logger.info("【第二轮】阶段 1: 重新爬取榜单页（强制重爬，查缺补漏）")
                    await spider.run(
                        crawl_all=crawl_all,
                        target_category_idx=category_idx if not crawl_all else None,
                        limit=limit,
                        crawl_detail=False,
                        crawl_detail_later=False,
                        skip_crawled=False
                    )

                    # 第二轮 - 阶段 2: 再次补充详情页
                    logger.info("【第二轮】阶段 2: 再次补充爬取详情页")
                    result_phase4 = await spider.crawl_missing_details(limit=detail_limit)

                    if result_phase4.get('remaining', 0) > 0:
                        logger.warning(f"[注意] 两轮爬取后仍有 {result_phase4['remaining']} 本书无法获取章节数据")
                        logger.warning(f"[建议] 可能是新书无历史数据且爬取持续失败，请人工检查")
                    else:
                        logger.info("[OK] 两轮爬取完成，所有书籍章节数据完整")
                else:
                    logger.info("[OK] 第一轮已完成，所有书籍章节数据完整")
            else:
                # 普通爬取
                await spider.run(
                    crawl_all=crawl_all,
                    target_category_idx=category_idx if not crawl_all else None,
                    limit=limit,
                    crawl_detail=detail
                )

            self.last_run_status = "success"
            logger.info("爬取任务完成")
            return {"status": "success", "message": "爬取任务完成"}
        except Exception as e:
            self.last_run_status = f"error: {str(e)}"
            logger.error(f"爬取任务失败：{e}")
            return {"status": "error", "message": str(e)}
        finally:
            self.crawl_task_running = False

    def add_daily_job(
        self,
        crawl_all: bool = True,
        limit: int = 30,
        detail: bool = False,
        hour: int = 0,
        minute: int = 0,
        auto: bool = True,
        detail_limit: int = 0
    ) -> None:
        """
        添加每日定时任务

        Args:
            crawl_all: 是否爬取所有分类
            limit: 每个分类爬取数量
            detail: 是否爬取详情
            hour: 执行小时
            minute: 执行分钟
            auto: 是否启用自动两阶段 + 第二轮重检（默认 True）
            detail_limit: 补充详情时最多爬取多少本，0 表示全部
        """
        async def job_wrapper():
            await self.execute_crawl(crawl_all, limit, detail, 0, auto, detail_limit)

        self.scheduler.add_job(
            lambda: asyncio.create_task(job_wrapper()),
            trigger=CronTrigger(hour=hour, minute=minute, second=0),
            id='daily_crawl',
            name='每日爬取任务',
            replace_existing=True
        )
        logger.info(f"已添加每日 {hour:02d}:{minute:02d} 爬取任务（auto 模式：{auto}）")

    def reschedule_job(
        self,
        job_id: str,
        hour: int,
        minute: int
    ) -> Optional[str]:
        """
        重新调度任务

        Args:
            job_id: 任务 ID
            hour: 执行小时
            minute: 执行分钟

        Returns:
            下次执行时间
        """
        try:
            self.scheduler.reschedule_job(
                job_id,
                trigger=CronTrigger(hour=hour, minute=minute, second=0)
            )
            job = self.scheduler.get_job(job_id)
            return job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job else None
        except Exception as e:
            logger.error(f"重新调度任务失败：{e}")
            return None

    def remove_job(self, job_id: str) -> bool:
        """移除任务"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"已移除任务：{job_id}")
            return True
        except Exception as e:
            logger.error(f"移除任务失败：{e}")
            return False

    def get_job_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取任务信息"""
        job = self.scheduler.get_job(job_id)
        if not job:
            return None

        return {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None,
        }

    def start_scheduler(self) -> None:
        """启动调度器"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("调度器已启动")

    def shutdown_scheduler(self) -> None:
        """关闭调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("调度器已关闭")

    def get_status(self) -> Dict[str, Any]:
        """获取任务状态"""
        job = self.scheduler.get_job('daily_crawl')
        return {
            "scheduler_running": self.scheduler.running,
            "next_run_time": job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job and job.next_run_time else None,
            "last_run_time": self.last_run_time.strftime('%Y-%m-%d %H:%M:%S') if self.last_run_time else None,
            "last_run_status": self.last_run_status,
        }

    async def stop_current_task(self) -> bool:
        """停止当前任务"""
        if self.crawl_task_running:
            self._stop_event.set()
            logger.info("已发送停止信号")
            return True
        return False


# 全局任务服务实例
_task_service: Optional[TaskService] = None


def get_task_service() -> TaskService:
    """获取任务服务实例"""
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
