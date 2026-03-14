#!/usr/bin/env python3
"""
番茄小说爬虫 - 统一命令行入口
"""
import argparse
import asyncio
import inspect
import sys
from pathlib import Path
from loguru import logger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.spiders.fanqie.spider import FanqieSpider
from src.novel_crawler.tools.export import export_data
from src.novel_crawler.tools.stats import quick_stats
from src.novel_crawler.tools.verify import verify_book_fields
from src.novel_crawler.tools.cache_manager import load_books_from_db_to_cache


async def crawl_books(args):
    """爬取书籍"""
    spider = FanqieSpider()
    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=args.detail,
        crawl_detail_later=args.later,
        skip_crawled=True  # 默认跳过已爬取的分类
    )


async def crawl_auto(args):
    """自动两阶段爬取 - 先快速爬取榜单页，然后自动补充详情"""
    spider = FanqieSpider()

    # 阶段 1: 快速爬取榜单页
    logger.info("=" * 60)
    logger.info("阶段 1: 快速爬取榜单页")
    logger.info("=" * 60)

    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=False,  # 不爬取详情
        crawl_detail_later=False,
        skip_crawled=True  # 跳过已爬取的分类
    )

    # 阶段 2: 补充爬取详情页
    logger.info("\n" + "=" * 60)
    logger.info("阶段 2: 补充爬取详情页")
    logger.info("=" * 60)

    await spider.crawl_missing_details(limit=args.detail_limit)


async def crawl_double(args):
    """两轮完整爬取 - 第一轮 + 第二轮查缺补漏"""
    spider = FanqieSpider()

    # ===== 第一轮 =====
    logger.info("\n" + "=" * 60)
    logger.info("【第一轮】阶段 1: 快速爬取榜单页")
    logger.info("=" * 60)

    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=False,
        crawl_detail_later=False,
        skip_crawled=True
    )

    logger.info("\n" + "=" * 60)
    logger.info("【第一轮】阶段 2: 补充爬取详情页")
    logger.info("=" * 60)

    result_phase2 = await spider.crawl_missing_details(limit=args.detail_limit)
    first_round_remaining = result_phase2.get('remaining', 0)

    # ===== 第二轮（如果需要）=====
    if first_round_remaining > 0:
        logger.info("\n" + "=" * 60)
        logger.info(f"【第二轮】检测到 {first_round_remaining} 本书缺失，开始完整重跑（查缺补漏）")
        logger.info("=" * 60)

        # 第二轮 - 阶段 1: 重新爬取榜单页（强制重爬）
        logger.info("\n" + "=" * 60)
        logger.info("【第二轮】阶段 1: 重新爬取榜单页（强制重爬，查缺补漏）")
        logger.info("=" * 60)

        await spider.run(
            crawl_all=args.all,
            target_category_idx=args.idx,
            limit=args.limit,
            crawl_detail=False,
            crawl_detail_later=False,
            skip_crawled=False  # 强制重爬所有分类
        )

        # 第二轮 - 阶段 2: 再次补充详情页
        logger.info("\n" + "=" * 60)
        logger.info("【第二轮】阶段 2: 再次补充爬取详情页")
        logger.info("=" * 60)

        result_phase4 = await spider.crawl_missing_details(limit=args.detail_limit)

        if result_phase4.get('remaining', 0) > 0:
            logger.warning(f"\n[注意] 两轮爬取后仍有 {result_phase4['remaining']} 本书无法获取章节数据")
            logger.warning("[建议] 可能是新书无历史数据且爬取持续失败，请人工检查")
        else:
            logger.info("\n[OK] 两轮爬取完成，所有书籍章节数据完整")
    else:
        logger.info("\n[OK] 第一轮已完成，所有书籍章节数据完整")


async def refill_missing_details(args):
    """补充爬取缺失章节的书籍"""
    spider = FanqieSpider()
    # 如果指定了日期，使用指定的日期；否则使用当前日期
    if args.date:
        spider.batch_date = args.date
    await spider.crawl_missing_details(limit=args.limit)


def cmd_export(args):
    """导出数据"""
    export_data()


def cmd_stats(args):
    """快速统计"""
    asyncio.run(quick_stats())


def cmd_verify(args):
    """验证数据"""
    verify_book_fields()


def cmd_warm_cache(args):
    """预热缓存 - 从数据库加载书籍数据到 Redis 缓存"""
    loaded_count = load_books_from_db_to_cache(
        batch_date=args.date,
        force_load=args.force
    )
    print(f"成功加载 {loaded_count} 本书到缓存")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='番茄小说爬虫',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s crawl --all                  爬取所有分类
  %(prog)s crawl --idx 0 --limit 30     爬取第一个分类的前 30 本书
  %(prog)s crawl --all --detail         爬取所有分类并获取章节列表
  %(prog)s crawl --all --later          先快速爬取榜单页入库，稍后再补充详情
  %(prog)s crawl-auto --all             自动两阶段爬取（先榜单页，后自动补充详情）
  %(prog)s crawl-double --all           两轮完整爬取（第一轮 + 第二轮查缺补漏）
  %(prog)s refill --limit 100           补充爬取 100 本缺失章节的书籍
  %(prog)s export                       导出数据到 CSV/JSON
  %(prog)s stats                        快速统计分类数量
  %(prog)s verify                       验证数据完整性
  %(prog)s warm-cache                   预热缓存（从数据库加载最新书籍到 Redis）
  %(prog)s warm-cache --force           强制预热缓存（即使缓存不为空）
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # crawl 命令
    crawl_parser = subparsers.add_parser('crawl', help='爬取书籍')
    crawl_parser.add_argument('--all', action='store_true', help='爬取所有分类')
    crawl_parser.add_argument('--idx', type=int, default=0, help='目标分类索引（默认 0）')
    crawl_parser.add_argument('--limit', type=int, default=30, help='每个分类爬取的书籍数量（默认 30）')
    crawl_parser.add_argument('--detail', action='store_true', help='爬取详情页（获取章节列表）')
    crawl_parser.add_argument('--later', action='store_true', help='稍后爬取详情（先快速爬取榜单页入库）')
    crawl_parser.set_defaults(func=crawl_books)

    # crawl-auto 命令（自动两阶段爬取）
    crawl_auto_parser = subparsers.add_parser('crawl-auto', help='自动两阶段爬取 - 先快速爬取榜单页，然后自动补充详情')
    crawl_auto_parser.add_argument('--all', action='store_true', help='爬取所有分类')
    crawl_auto_parser.add_argument('--idx', type=int, default=0, help='目标分类索引（默认 0）')
    crawl_auto_parser.add_argument('--limit', type=int, default=30, help='每个分类爬取的书籍数量（默认 30）')
    crawl_auto_parser.add_argument('--detail-limit', type=int, default=0, help='补充详情时最多爬取多少本，0 表示全部')
    crawl_auto_parser.set_defaults(func=crawl_auto)

    # crawl-double 命令（两轮完整爬取）
    crawl_double_parser = subparsers.add_parser('crawl-double', help='两轮完整爬取 - 第一轮 + 第二轮查缺补漏')
    crawl_double_parser.add_argument('--all', action='store_true', help='爬取所有分类')
    crawl_double_parser.add_argument('--idx', type=int, default=0, help='目标分类索引（默认 0）')
    crawl_double_parser.add_argument('--limit', type=int, default=30, help='每个分类爬取的书籍数量（默认 30）')
    crawl_double_parser.add_argument('--detail-limit', type=int, default=0, help='补充详情时最多爬取多少本，0 表示全部')
    crawl_double_parser.set_defaults(func=crawl_double)

    # export 命令
    export_parser = subparsers.add_parser('export', help='导出数据到 CSV/JSON')
    export_parser.set_defaults(func=cmd_export)

    # stats 命令
    stats_parser = subparsers.add_parser('stats', help='快速统计分类数量')
    stats_parser.set_defaults(func=cmd_stats)

    # verify 命令
    verify_parser = subparsers.add_parser('verify', help='验证数据完整性')
    verify_parser.set_defaults(func=cmd_verify)

    # refill 命令
    refill_parser = subparsers.add_parser('refill', help='补充爬取缺失章节的书籍')
    refill_parser.add_argument('--limit', type=int, default=0, help='最多爬取多少本，0 表示全部')
    refill_parser.add_argument('--date', type=str, default=None, help='指定批次日期（格式：YYYY-MM-DD），默认为最新批次')
    refill_parser.set_defaults(func=refill_missing_details)

    # warm-cache 命令
    warm_cache_parser = subparsers.add_parser('warm-cache', help='预热缓存 - 从数据库加载书籍数据到 Redis 缓存')
    warm_cache_parser.add_argument('--date', type=str, default=None, help='指定批次日期（格式：YYYY-MM-DD），默认为昨天')
    warm_cache_parser.add_argument('--force', action='store_true', help='强制加载（即使缓存不为空）')
    warm_cache_parser.set_defaults(func=cmd_warm_cache)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if inspect.iscoroutinefunction(args.func):
        asyncio.run(args.func(args))
    else:
        args.func(args)


if __name__ == '__main__':
    main()
