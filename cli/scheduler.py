#!/usr/bin/env python3
"""
番茄小说爬虫 - 定时任务调度器
支持每天凌晨 0 点自动爬取最新数据
"""
import argparse
import asyncio
import sys
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.spiders.fanqie.spider import FanqieSpider
from src.novel_crawler.config import LOG_CONFIG
from loguru import logger

# 配置日志
logger.remove()
logger.add(
    LOG_CONFIG["log_file"],
    format=LOG_CONFIG["format"],
    level=LOG_CONFIG["level"],
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    encoding=LOG_CONFIG["encoding"],
)
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=LOG_CONFIG["level"],
    colorize=False,
)


class CrawlScheduler:
    """爬取任务调度器"""

    def __init__(self, crawl_all: bool = True, limit: int = 30, detail: bool = False,
                 crawl_detail_later: bool = False, auto: bool = False,
                 hour: int = 0, minute: int = 0, detail_limit: int = 0,
                 retry_interval: int = 2):
        self.crawl_all = crawl_all
        self.limit = limit
        self.detail = detail
        self.crawl_detail_later = crawl_detail_later
        self.auto = auto  # 自动两阶段爬取
        self.detail_limit = detail_limit  # 补充详情时的数量限制
        self.hour = hour
        self.minute = minute
        self.retry_interval = retry_interval  # 空白页重试间隔（小时）
        self.scheduler = None
        self.is_running = False

    async def crawl_job(self):
        """爬取任务"""
        log_prefix = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        logger.info(f"{log_prefix} 开始执行定时爬取任务")

        spider = None
        try:
            spider = FanqieSpider()

            if self.auto:
                # 自动两阶段爬取 + 第二轮完整重检（查缺补漏）
                logger.info(f"{log_prefix} 【第一轮】阶段 1: 快速爬取榜单页")
                await spider.run(
                    crawl_all=self.crawl_all,
                    target_category_idx=0,
                    limit=self.limit,
                    crawl_detail=False,
                    crawl_detail_later=False,
                    skip_crawled=True  # 第一轮：跳过已爬取的分类
                )

                logger.info(f"{log_prefix} 【第一轮】阶段 2: 补充爬取详情页")
                result_phase2 = await spider.crawl_missing_details(limit=self.detail_limit)
                first_round_remaining = result_phase2.get('remaining', 0)

                # 检查是否触发空白页保护
                if spider.blank_page_detected:
                    logger.warning(f"{log_prefix} [空白页保护] 触发降级模式，剩余 {first_round_remaining} 本小说未爬取")
                    logger.warning(f"{log_prefix} [提示] 将在 {self.retry_interval} 小时后自动重试")
                    return

                # 第二轮：完整重跑两阶段（应对榜单页遗漏 + 详情页遗漏）
                if first_round_remaining > 0:
                    logger.info(f"{log_prefix} 【第二轮】检测到 {first_round_remaining} 本书缺失，开始完整重跑两阶段（查缺补漏）")

                    # 第二轮 - 阶段 1: 重新爬取榜单页（强制重爬所有分类，补充遗漏）
                    logger.info(f"{log_prefix} 【第二轮】阶段 1: 重新爬取榜单页（强制重爬，查缺补漏）")
                    await spider.run(
                        crawl_all=self.crawl_all,
                        target_category_idx=0,
                        limit=self.limit,
                        crawl_detail=False,
                        crawl_detail_later=False,
                        skip_crawled=False  # 第二轮：强制重爬所有分类，补充遗漏的书籍
                    )

                    # 第二轮 - 阶段 2: 再次补充详情页
                    logger.info(f"{log_prefix} 【第二轮】阶段 2: 再次补充爬取详情页")
                    result_phase4 = await spider.crawl_missing_details(limit=self.detail_limit)

                    # 检查是否触发空白页保护
                    if spider.blank_page_detected:
                        logger.warning(f"{log_prefix} [空白页保护] 触发降级模式，剩余 {result_phase4.get('remaining', 0)} 本小说未爬取")
                        logger.warning(f"{log_prefix} [提示] 将在 {self.retry_interval} 小时后自动重试")
                        return

                    if result_phase4.get('remaining', 0) > 0:
                        logger.warning(f"{log_prefix} [注意] 两轮爬取后仍有 {result_phase4['remaining']} 本书无法获取章节数据")
                        logger.warning(f"{log_prefix} [建议] 可能是新书无历史数据且爬取持续失败，请人工检查")
                    else:
                        logger.info(f"{log_prefix} 【OK】两轮爬取完成，所有书籍章节数据完整")
                else:
                    logger.info(f"{log_prefix} 【OK】第一轮已完成，所有书籍章节数据完整")
            else:
                # 普通爬取
                await spider.run(
                    crawl_all=self.crawl_all,
                    target_category_idx=0,
                    limit=self.limit,
                    crawl_detail=self.detail,
                    crawl_detail_later=self.crawl_detail_later
                )

            logger.info(f"{log_prefix} 爬取任务完成")
        except asyncio.CancelledError:
            logger.warning(f"{log_prefix} 任务被取消，正在清理资源...")
            if spider:
                try:
                    await spider.close()
                except Exception:
                    pass
            raise
        except KeyboardInterrupt:
            logger.warning(f"{log_prefix} 任务被用户中断，正在清理资源...")
            if spider:
                try:
                    await spider.close()
                except Exception:
                    pass
            raise
        except Exception as e:
            logger.error(f"{log_prefix} 爬取任务失败：{e}")
            import traceback
            logger.error(traceback.format_exc())
            if spider:
                try:
                    await spider.close()
                except Exception:
                    pass
            raise

    async def start(self):
        """启动调度器"""
        # 创建调度器（必须在异步上下文中）
        self.scheduler = AsyncIOScheduler()

        # 添加定时任务 - 每天在指定时间执行
        self.scheduler.add_job(
            self.crawl_job,
            trigger=CronTrigger(hour=self.hour, minute=self.minute, second=0),
            id='daily_crawl',
            name='每日爬取任务',
            replace_existing=True
        )

        # 添加空白页重试任务 - 每 2 小时执行一次
        self.scheduler.add_job(
            self.retry_crawl_job,
            trigger='interval',
            hours=self.retry_interval,
            id='retry_crawl',
            name='空白页重试任务',
            replace_existing=True
        )

        # 启动调度器
        self.scheduler.start()
        self.is_running = True

        log_msg = f"调度器已启动 - 每天 {self.hour:02d}:{self.minute:02d} 自动爬取，每{self.retry_interval}小时重试"
        logger.info(log_msg)
        logger.info(f"下次每日任务执行时间：{self.scheduler.get_job('daily_crawl').next_run_time}")
        logger.info(f"下次重试任务执行时间：{self.scheduler.get_job('retry_crawl').next_run_time}")

        # 保持运行
        try:
            while self.is_running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            await self.shutdown()

    async def retry_crawl_job(self):
        """空白页重试任务 - 每 2 小时执行一次"""
        log_prefix = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        logger.info(f"{log_prefix} 开始执行空白页重试任务")

        spider = None
        try:
            spider = FanqieSpider()

            # 只补充爬取缺失的详情页
            logger.info(f"{log_prefix} 补充爬取缺失章节的书籍")
            result = await spider.crawl_missing_details(limit=self.detail_limit)

            # 检查是否仍有缺失
            remaining = result.get('remaining', 0)
            if remaining > 0:
                if spider.blank_page_detected:
                    logger.warning(f"{log_prefix} [空白页保护] 仍有 {remaining} 本书无法获取，将在 {self.retry_interval} 小时后继续重试")
                else:
                    logger.warning(f"{log_prefix} [注意] 仍有 {remaining} 本书缺少章节数据（可能是新书无历史数据）")
            else:
                logger.info(f"{log_prefix} [OK] 所有书籍章节数据完整，恢复正常爬取模式")

        except Exception as e:
            logger.error(f"{log_prefix} 重试任务失败：{e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if spider:
                try:
                    await spider.close()
                except Exception:
                    pass

    async def shutdown(self):
        """关闭调度器"""
        logger.info("正在关闭调度器...")
        self.is_running = False
        if self.scheduler:
            self.scheduler.shutdown()
        logger.info("调度器已关闭")

    async def run_now(self):
        """立即执行一次爬取任务"""
        logger.info("立即执行爬取任务")
        await self.crawl_job()


async def async_main():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='番茄小说爬虫 - 定时任务调度器')
    parser.add_argument('--once', action='store_true', help='立即执行一次，不启动定时任务')
    parser.add_argument('--no-all', action='store_true', help='不爬取所有分类（只爬取第一个分类）')
    parser.add_argument('--limit', type=int, default=30, help='每个分类爬取的书籍数量（默认 30）')
    parser.add_argument('--no-detail', action='store_true', help='不爬取详情页（默认爬取详情）')
    parser.add_argument('--later', action='store_true', help='稍后爬取详情（先快速爬取榜单页入库）')
    parser.add_argument('--auto', action='store_true', help='自动两阶段爬取 + 第二轮重检（先榜单页，后自动补充详情，最后查缺补漏）')
    parser.add_argument('--detail-limit', type=int, default=0, help='补充详情时最多爬取多少本，0 表示全部')
    parser.add_argument('--hour', type=int, default=0, help='定时任务执行时间 - 小时（0-23，默认 0 点）')
    parser.add_argument('--minute', type=int, default=0, help='定时任务执行时间 - 分钟（0-59，默认 0 分）')
    parser.add_argument('--retry-interval', type=int, default=2, help='空白页重试间隔（小时，默认 2 小时）')

    args = parser.parse_args()

    # 创建调度器
    scheduler = CrawlScheduler(
        crawl_all=not args.no_all,
        limit=args.limit,
        detail=not args.no_detail,  # 默认爬取详情
        crawl_detail_later=args.later,
        auto=True,  # 默认启用自动两阶段 + 第二轮重检
        detail_limit=args.detail_limit,
        hour=args.hour,
        minute=args.minute,
        retry_interval=args.retry_interval
    )
        detail_limit=args.detail_limit,
        hour=args.hour,
        minute=args.minute
    )

    if args.once:
        # 立即执行一次
        await scheduler.run_now()
    else:
        # 启动定时任务
        await scheduler.start()


def main():
    """主函数"""
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
