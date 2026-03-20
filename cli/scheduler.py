#!/usr/bin/env python3
"""
多站点爬虫 - 定时任务调度器

支持站点：
- fanqie：番茄小说（每天 00:00 爬取榜单，不爬取章节）
- reelshort：ReelShort 短剧（每天 00:10 爬取全量，自动翻译）
- dramashort：DramaShorts 短剧（每天 00:10 爬取全量，自动翻译）

使用方法：
    python -m cli.scheduler                          # 启动所有站点定时任务
    python -m cli.scheduler --sites fanqie           # 只启动番茄小说
    python -m cli.scheduler --sites reelshort        # 只启动 ReelShort
    python -m cli.scheduler --sites dramashort       # 只启动 DramaShorts
    python -m cli.scheduler --sites fanqie,reelshort # 启动指定站点
"""
import argparse
import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG
from loguru import logger

# 配置日志
logger.remove()

def add_site_prefix(record):
    """为日志记录添加 site 前缀（如果不存在则设为空字符串）"""
    record["extra"].setdefault("site", "")
    return True

logger.add(
    LOG_CONFIG["log_file"],
    format=LOG_CONFIG["format"],
    level=LOG_CONFIG["level"],
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    encoding=LOG_CONFIG["encoding"],
    filter=add_site_prefix,
)
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra[site]:<12}{message}",
    level=LOG_CONFIG["level"],
    colorize=False,
    filter=add_site_prefix,
)


class FanqieScheduler:
    """
    番茄小说定时任务调度器

    爬取策略：
    - 默认只爬取榜单信息，不爬取章节详情
    - 每天凌晨 0 点自动执行
    """

    def __init__(
        self,
        crawl_all: bool = True,
        limit: int = 30,
        hour: int = 0,
        minute: int = 0,
    ):
        self.crawl_all = crawl_all
        self.limit = limit
        self.hour = hour
        self.minute = minute
        self.scheduler = None
        self.is_running = False

    async def crawl_job(self):
        """爬取榜单任务"""
        with logger.contextualize(site="[Fanqie] "):
            logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行番茄小说榜单爬取任务")

            from src.novel_crawler.spiders.fanqie.spider import FanqieSpider

            spider = None
            try:
                spider = FanqieSpider()
                # 只爬取榜单页，不获取章节列表
                await spider.run(
                    crawl_all=self.crawl_all,
                    target_category_idx=0,
                    limit=self.limit,
                    crawl_detail=False,       # 不爬取详情页
                    crawl_detail_later=False, # 不稍后爬取详情
                    skip_crawled=True,
                )
                logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 番茄小说榜单爬取任务完成")
            except Exception as e:
                logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 番茄小说爬取任务失败：{e}")
                import traceback
                logger.error(traceback.format_exc())
                raise
            finally:
                if spider:
                    try:
                        await spider.close()
                    except Exception:
                        pass

    async def start(self):
        """启动调度器"""
        self.scheduler = AsyncIOScheduler()

        # 添加定时任务 - 每天在指定时间执行
        self.scheduler.add_job(
            self.crawl_job,
            trigger=CronTrigger(hour=self.hour, minute=self.minute, second=0),
            id='fanqie_daily_crawl',
            name='番茄小说每日榜单爬取',
            replace_existing=True,
        )

        self.scheduler.start()
        self.is_running = True

        logger.info(f"番茄小说调度器已启动 - 每天 {self.hour:02d}:{self.minute:02d} 自动爬取榜单")
        logger.info(f"下次任务执行时间：{self.scheduler.get_job('fanqie_daily_crawl').next_run_time}")

        try:
            while self.is_running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            await self.shutdown()

    async def shutdown(self):
        """关闭调度器"""
        logger.info("正在关闭番茄小说调度器...")
        self.is_running = False
        if self.scheduler:
            self.scheduler.shutdown()
        logger.info("番茄小说调度器已关闭")

    async def run_now(self):
        """立即执行一次爬取任务"""
        logger.info("立即执行番茄小说榜单爬取任务")
        await self.crawl_job()


class ReelShortScheduler:
    """
    ReelShort 短剧定时任务调度器

    爬取策略：
    - 每天凌晨 00:10 自动爬取所有语言
    - 每爬取完成一个 Tab 自动触发翻译
    - 支持并发爬取多个语言
    """

    def __init__(
        self,
        languages: Optional[List[str]] = None,
        workers: int = 1,
        translate: bool = True,
        translate_workers: int = 20,
        translate_llm_batch: int = 1,
        hour: int = 0,
        minute: int = 10,
    ):
        self.languages = languages
        self.workers = workers
        self.translate = translate
        self.translate_workers = translate_workers
        self.translate_llm_batch = translate_llm_batch
        self.hour = hour
        self.minute = minute
        self.scheduler = None
        self.is_running = False

    async def crawl_job(self):
        """爬取任务"""
        with logger.contextualize(site="[ReelShort] "):
            logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行 ReelShort 爬取任务")

            from src.novel_crawler.spiders.reelshort.spider import ReelShortSpider

            spider = None
            try:
                spider = ReelShortSpider()
                result = await spider.run(
                    languages=self.languages,
                    crawl_detail=True,
                    workers=self.workers,
                    translate=self.translate,
                    translate_workers=self.translate_workers,
                    translate_llm_batch=self.translate_llm_batch,
                )
                logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ReelShort 爬取任务完成")
                logger.info(f"  - 标签维表写入：{result.get('tags', 0)} 条")
                logger.info(f"  - 榜单明细写入：{result.get('dramas', 0)} 条")
                logger.info(f"  - 详情更新：{result.get('details', 0)} 条")
            except Exception as e:
                logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ReelShort 爬取任务失败：{e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

    async def start(self):
        """启动调度器"""
        self.scheduler = AsyncIOScheduler()

        # 添加定时任务 - 每天 00:10 执行
        self.scheduler.add_job(
            self.crawl_job,
            trigger=CronTrigger(hour=self.hour, minute=self.minute, second=0),
            id='reelshort_daily_crawl',
            name='ReelShort 每日全量爬取',
            replace_existing=True,
        )

        self.scheduler.start()
        self.is_running = True

        logger.info(
            f"ReelShort 调度器已启动 - 每天 {self.hour:02d}:{self.minute:02d} 自动爬取"
            f"（translate={self.translate}, workers={self.workers}）"
        )
        logger.info(f"下次任务执行时间：{self.scheduler.get_job('reelshort_daily_crawl').next_run_time}")

        try:
            while self.is_running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            await self.shutdown()

    async def shutdown(self):
        """关闭调度器"""
        logger.info("正在关闭 ReelShort 调度器...")
        self.is_running = False
        if self.scheduler:
            self.scheduler.shutdown()
        logger.info("ReelShort 调度器已关闭")

    async def run_now(self):
        """立即执行一次爬取任务"""
        logger.info("立即执行 ReelShort 爬取任务")
        await self.crawl_job()


class DramaShortScheduler:
    """
    DramaShorts 短剧定时任务调度器

    爬取策略：
    - 每天凌晨 00:10 自动爬取所有语言
    - 每爬取完成一个语言自动触发翻译
    """

    def __init__(
        self,
        languages: Optional[List[str]] = None,
        translate: bool = True,
        translate_workers: int = 20,
        translate_llm_batch: int = 1,
        hour: int = 0,
        minute: int = 10,
    ):
        self.languages = languages
        self.translate = translate
        self.translate_workers = translate_workers
        self.translate_llm_batch = translate_llm_batch
        self.hour = hour
        self.minute = minute
        self.scheduler = None
        self.is_running = False

    async def crawl_job(self):
        """爬取任务"""
        with logger.contextualize(site="[DramaShort] "):
            logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行 DramaShorts 爬取任务")

            from src.novel_crawler.spiders.dramashort.spider import DramaShortSpider

            spider = None
            try:
                spider = DramaShortSpider()
                result = await spider.run(
                    languages=self.languages,
                    crawl_detail=True,
                    translate=self.translate,
                    translate_workers=self.translate_workers,
                    translate_llm_batch=self.translate_llm_batch,
                )
                logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DramaShorts 爬取任务完成")
                logger.info(f"  - 榜单明细写入：{result.get('dramas', 0)} 条")
                logger.info(f"  - 详情更新：{result.get('details', 0)} 条")
            except Exception as e:
                logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DramaShorts 爬取任务失败：{e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

    async def start(self):
        """启动调度器"""
        self.scheduler = AsyncIOScheduler()

        # 添加定时任务 - 每天 00:10 执行
        self.scheduler.add_job(
            self.crawl_job,
            trigger=CronTrigger(hour=self.hour, minute=self.minute, second=0),
            id='dramashort_daily_crawl',
            name='DramaShorts 每日全量爬取',
            replace_existing=True,
        )

        self.scheduler.start()
        self.is_running = True

        logger.info(
            f"DramaShorts 调度器已启动 - 每天 {self.hour:02d}:{self.minute:02d} 自动爬取"
            f"（translate={self.translate}）"
        )
        logger.info(f"下次任务执行时间：{self.scheduler.get_job('dramashort_daily_crawl').next_run_time}")

        try:
            while self.is_running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            await self.shutdown()

    async def shutdown(self):
        """关闭调度器"""
        logger.info("正在关闭 DramaShorts 调度器...")
        self.is_running = False
        if self.scheduler:
            self.scheduler.shutdown()
        logger.info("DramaShorts 调度器已关闭")

    async def run_now(self):
        """立即执行一次爬取任务"""
        logger.info("立即执行 DramaShorts 爬取任务")
        await self.crawl_job()


class MultiSiteScheduler:
    """
    多站点统一调度器

    管理所有站点的定时任务：
    - 番茄小说：每天 00:00 爬取榜单
    - ReelShort：每天 00:10 爬取全量 + 自动翻译
    - DramaShorts：每天 00:10 爬取全量 + 自动翻译
    """

    def __init__(
        self,
        sites: Optional[List[str]] = None,
        fanqie_config: Optional[dict] = None,
        reelshort_config: Optional[dict] = None,
        dramashort_config: Optional[dict] = None,
    ):
        self.sites = sites or ["fanqie", "reelshort", "dramashort"]
        self.schedulers = []

        # 创建各站点调度器
        if "fanqie" in self.sites:
            cfg = fanqie_config or {}
            self.schedulers.append(FanqieScheduler(**cfg))
            logger.info("已创建番茄小说调度器")

        if "reelshort" in self.sites:
            cfg = reelshort_config or {}
            # 默认配置
            cfg.setdefault("hour", 0)
            cfg.setdefault("minute", 10)
            cfg.setdefault("translate", True)
            cfg.setdefault("translate_workers", 20)
            cfg.setdefault("translate_llm_batch", 1)
            self.schedulers.append(ReelShortScheduler(**cfg))
            logger.info("已创建 ReelShort 调度器")

        if "dramashort" in self.sites:
            cfg = dramashort_config or {}
            # 默认配置
            cfg.setdefault("hour", 0)
            cfg.setdefault("minute", 10)
            cfg.setdefault("translate", True)
            cfg.setdefault("translate_workers", 20)
            cfg.setdefault("translate_llm_batch", 1)
            self.schedulers.append(DramaShortScheduler(**cfg))
            logger.info("已创建 DramaShorts 调度器")

    async def start(self):
        """启动所有调度器（并发运行）"""
        logger.info("=" * 60)
        logger.info(f"多站点调度器启动 - 管理站点：{self.sites}")
        logger.info("=" * 60)

        # 并发启动所有调度器
        tasks = [asyncio.create_task(sched.start()) for sched in self.schedulers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self):
        """关闭所有调度器"""
        logger.info("正在关闭所有调度器...")
        for sched in self.schedulers:
            await sched.shutdown()
        logger.info("所有调度器已关闭")

    async def run_now(self, site: Optional[str] = None):
        """立即执行指定站点或所有站点的爬取任务"""
        if site:
            for sched in self.schedulers:
                if type(sched).__name__.lower().startswith(site.lower()):
                    await sched.run_now()
                    break
        else:
            # 执行所有站点 - 并发启动
            await asyncio.gather(
                *[sched.run_now() for sched in self.schedulers],
                return_exceptions=True
            )


async def async_main():
    """异步主函数"""
    parser = argparse.ArgumentParser(
        description='多站点爬虫 - 定时任务调度器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s                                 启动所有站点定时任务
  %(prog)s --sites fanqie                  只启动番茄小说
  %(prog)s --sites reelshort               只启动 ReelShort
  %(prog)s --sites dramashort              只启动 DramaShorts
  %(prog)s --sites fanqie,reelshort        启动指定站点
  %(prog)s --once                          立即执行一次所有站点爬取
  %(prog)s --once --sites fanqie           立即执行指定站点爬取

  %(prog)s --fanqie-hour 0 --fanqie-minute 0     设置番茄小说爬取时间
  %(prog)s --drama-hour 0 --drama-minute 10      设置短剧爬取时间
  %(prog)s --workers 3                           短剧并发 Worker 数
  %(prog)s --translate-workers 20                翻译并发数
        """,
    )

    parser.add_argument(
        '--sites', type=str, default=None,
        help='要启动的站点列表，逗号分隔（默认：fanqie,reelshort,dramashort）'
    )
    parser.add_argument(
        '--once', action='store_true',
        help='立即执行一次爬取任务，不启动定时任务'
    )

    # 番茄小说配置
    parser.add_argument('--fanqie-hour', type=int, default=0, help='番茄小说爬取时间 - 小时（默认 0）')
    parser.add_argument('--fanqie-minute', type=int, default=0, help='番茄小说爬取时间 - 分钟（默认 0）')
    parser.add_argument('--fanqie-limit', type=int, default=30, help='番茄小说每个分类爬取数量（默认 30）')

    # 短剧配置（ReelShort 和 DramaShorts 共用）
    parser.add_argument('--drama-hour', type=int, default=0, help='短剧爬取时间 - 小时（默认 0）')
    parser.add_argument('--drama-minute', type=int, default=10, help='短剧爬取时间 - 分钟（默认 10）')
    parser.add_argument('--workers', type=int, default=1, help='短剧并发 Worker 数（默认 1）')
    parser.add_argument('--translate-workers', type=int, default=20, help='翻译并发数（默认 20）')
    parser.add_argument('--llm-batch', type=int, default=1, help='每次 LLM 请求条数（默认 1）')

    # 单独配置
    parser.add_argument('--reelshort-languages', type=str, default=None, help='ReelShort 语言列表，逗号分隔')
    parser.add_argument('--dramashort-languages', type=str, default=None, help='DramaShorts 语言列表，逗号分隔')
    parser.add_argument('--no-translate', action='store_true', help='禁用自动翻译')

    args = parser.parse_args()

    # 解析站点列表
    sites = None
    if args.sites:
        sites = [s.strip().lower() for s in args.sites.split(',')]

    # 解析语言列表
    reelshort_languages = None
    if args.reelshort_languages:
        reelshort_languages = [l.strip() for l in args.reelshort_languages.split(',') if l.strip()]

    dramashort_languages = None
    if args.dramashort_languages:
        dramashort_languages = [l.strip() for l in args.dramashort_languages.split(',') if l.strip()]

    # 创建调度器
    scheduler = MultiSiteScheduler(
        sites=sites,
        fanqie_config={
            "hour": args.fanqie_hour,
            "minute": args.fanqie_minute,
            "limit": args.fanqie_limit,
        },
        reelshort_config={
            "languages": reelshort_languages,
            "workers": args.workers,
            "translate": not args.no_translate,
            "translate_workers": args.translate_workers,
            "translate_llm_batch": args.llm_batch,
            "hour": args.drama_hour,
            "minute": args.drama_minute,
        },
        dramashort_config={
            "languages": dramashort_languages,
            "translate": not args.no_translate,
            "translate_workers": args.translate_workers,
            "translate_llm_batch": args.llm_batch,
            "hour": args.drama_hour,
            "minute": args.drama_minute,
        },
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
