
#!/usr/bin/env python3
"""
多站点爬虫 - 统一命令行入口

支持站点：
- fanqie：番茄小说
- reelshort：ReelShort 短剧
- dramashort：DramaShorts 短剧
"""
import argparse
import asyncio
import inspect
import sys
from pathlib import Path
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.spiders.fanqie.spider import FanqieSpider
from src.novel_crawler.tools.export import export_data
from src.novel_crawler.tools.stats import quick_stats
from src.novel_crawler.tools.verify import verify_book_fields
from src.novel_crawler.tools.cache_manager import load_books_from_db_to_cache


async def crawl_books(args):
    spider = FanqieSpider()
    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=args.detail,
        crawl_detail_later=args.later,
        skip_crawled=True,
    )


async def crawl_auto(args):
    spider = FanqieSpider()
    logger.info("=" * 60)
    logger.info("阶段 1: 快速爬取榜单页")
    logger.info("=" * 60)
    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=False,
        crawl_detail_later=False,
        skip_crawled=True,
    )
    logger.info("\n" + "=" * 60)
    logger.info("阶段 2: 补充爬取详情页")
    logger.info("=" * 60)
    await spider.crawl_missing_details(limit=args.detail_limit)


async def crawl_double(args):
    spider = FanqieSpider()
    logger.info("\n" + "=" * 60)
    logger.info("【第一轮】阶段 1: 快速爬取榜单页")
    logger.info("=" * 60)
    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=False,
        crawl_detail_later=False,
        skip_crawled=True,
    )
    logger.info("\n" + "=" * 60)
    logger.info("【第一轮】阶段 2: 补充爬取详情页")
    logger.info("=" * 60)
    result_phase2 = await spider.crawl_missing_details(limit=args.detail_limit)
    first_round_remaining = result_phase2.get("remaining", 0)

    if first_round_remaining > 0:
        logger.info("\n" + "=" * 60)
        logger.info(f"【第二轮】检测到 {first_round_remaining} 本书缺失，开始完整重跑（查缺补漏）")
        logger.info("=" * 60)
        logger.info("\n" + "=" * 60)
        logger.info("【第二轮】阶段 1: 重新爬取榜单页（强制重爬，查缺补漏）")
        logger.info("=" * 60)
        await spider.run(
            crawl_all=args.all,
            target_category_idx=args.idx,
            limit=args.limit,
            crawl_detail=False,
            crawl_detail_later=False,
            skip_crawled=False,
        )
        logger.info("\n" + "=" * 60)
        logger.info("【第二轮】阶段 2: 再次补充爬取详情页")
        logger.info("=" * 60)
        result_phase4 = await spider.crawl_missing_details(limit=args.detail_limit)
        if result_phase4.get("remaining", 0) > 0:
            logger.warning(f"\n[注意] 两轮爬取后仍有 {result_phase4['remaining']} 本书无法获取章节数据")
        else:
            logger.info("\n[OK] 两轮爬取完成，所有书籍章节数据完整")
    else:
        logger.info("\n[OK] 第一轮已完成，所有书籍章节数据完整")


async def refill_missing_details(args):
    spider = FanqieSpider()
    if args.date:
        spider.batch_date = args.date
    await spider.crawl_missing_details(limit=args.limit)


def cmd_export(args):
    export_data()


def cmd_stats(args):
    asyncio.run(quick_stats())


def cmd_verify(args):
    verify_book_fields()


def cmd_warm_cache(args):
    loaded_count = load_books_from_db_to_cache(
        batch_date=args.date,
        force_load=args.force,
    )
    print(f"成功加载 {loaded_count} 本书到缓存")


# reelshort-classify 命令已废弃：标签分类现在在爬取过程中直接完成


async def crawl_reelshort(args):
    from src.novel_crawler.spiders.reelshort.spider import ReelShortSpider
    languages = None
    if args.languages:
        languages = [lang.strip() for lang in args.languages.split(",") if lang.strip()]
    spider = ReelShortSpider()
    result = await spider.run(
        languages=languages,
        crawl_detail=not args.no_detail,
        workers=args.workers,
    )
    print(f"\n[完成] ReelShort 爬取结果：")
    print(f"  - 榜单明细：{result.get('dramas', 0)} 条")
    print(f"  - 详情更新：{result.get('details', 0)} 条")


async def cmd_reelshort_translate(args):
    from src.novel_crawler.services.reelshort_translate_service import run_translate

    # --language 支持逗号分隔多语言，如 "en,th,vi"
    raw_lang = args.language or ""
    languages = [l.strip() for l in raw_lang.split(",") if l.strip()] or [None]

    total_all = translated_all = reused_all = skipped_all = 0
    for lang in languages:
        stats = await run_translate(
            batch_date=args.date or None,
            language=lang,
            workers=args.workers,
            llm_batch=args.llm_batch,
        )
        total_all += stats["total"]
        translated_all += stats["translated"]
        reused_all += stats.get("reused", 0)
        skipped_all += stats["skipped"]

    print(f"\n[完成] ReelShort 翻译结果：")
    print(f"  - 待翻译总计：{total_all} 条")
    print(f"  - 成功写入：{translated_all} 条（含跨批次复用 {reused_all} 条）")
    print(f"  - 跳过/失败：{skipped_all} 条")


async def cmd_reelshort_translate_tags(args):
    from src.novel_crawler.services.reelshort_tag_translate_service import translate_tags
    count = await translate_tags(
        language=args.language or None,
        batch_size=args.batch_size,
        workers=args.workers,
        force=args.force,
    )
    print(f"\n[完成] ReelShort 标签翻译：成功写入 {count} 条 tag_name_zh")


async def crawl_dramashort(args):
    from src.novel_crawler.spiders.dramashort.spider import DramaShortSpider
    languages = None
    if args.languages:
        languages = [lang.strip() for lang in args.languages.split(",") if lang.strip()]
    spider = DramaShortSpider()
    result = await spider.run(
        languages=languages,
        crawl_detail=not args.no_detail,
    )
    print(f"\n[完成] DramaShorts 爬取结果：")
    print(f"  - 榜单明细写入：{result.get('dramas', 0)} 条")
    print(f"  - 详情更新：{result.get('details', 0)} 条")


async def cmd_dramashort_translate(args):
    from src.novel_crawler.services.dramashort_translate_service import run_translate
    stats = await run_translate(
        batch_date=args.date or None,
        language=args.language or None,
        workers=args.workers,
        llm_batch=args.llm_batch,
    )
    print(f"\n[完成] DramaShorts 翻译结果：")
    print(f"  - 待翻译总计：{stats['total']} 条")
    print(f"  - 成功写入：{stats['translated']} 条（含跨批次复用 {stats.get('reused', 0)} 条）")
    print(f"  - 跳过/失败：{stats['skipped']} 条")


def cmd_export_drama_zh(args):
    from scripts.export_drama_zh import (
        export_reelshort, export_reelshort_raw,
        export_dramashort, export_dramashort_raw,
        get_utc8_today,
    )
    from datetime import datetime

    batch_date = args.date or get_utc8_today()
    try:
        datetime.strptime(batch_date, "%Y-%m-%d")
    except ValueError:
        print(f"[错误] 日期格式错误：{batch_date}，应为 YYYY-MM-DD")
        return

    language = args.language or None
    site = args.site or "all"
    raw = getattr(args, "raw", False)
    total = 0

    if site in ("reelshort", "all"):
        fn = export_reelshort_raw if raw else export_reelshort
        stats = fn(batch_date, language)
        if stats:
            subtotal = sum(stats.values())
            total += subtotal
            print(f"\n[ReelShort] 共导出 {subtotal} 条")
            for lang, cnt in stats.items():
                print(f"  - [{lang}] {cnt} 条")

    if site in ("dramashort", "all"):
        fn = export_dramashort_raw if raw else export_dramashort
        stats = fn(batch_date, language)
        if stats:
            subtotal = sum(stats.values())
            total += subtotal
            print(f"\n[DramaShort] 共导出 {subtotal} 条")
            for lang, cnt in stats.items():
                print(f"  - [{lang}] {cnt} 条")

    print(f"\n[完成] 合计导出 {total} 条")


def main():
    parser = argparse.ArgumentParser(
        description="多站点爬虫（番茄小说 / ReelShort / DramaShorts）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例（番茄小说）:
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

使用示例（ReelShort）:
  %(prog)s reelshort                                        爬取全部语言的 ReelShort 短剧数据（含详情）
  %(prog)s reelshort --languages en                         仅爬取英文数据
  %(prog)s reelshort --no-detail                            仅爬取列表页，不爬取详情页
  %(prog)s reelshort --workers 3                            3 个语言并发爬取
  %(prog)s reelshort-translate                              翻译今天全部语言数据为简体中文
  %(prog)s reelshort-translate --date 2026-03-19            翻译指定日期的数据
  %(prog)s reelshort-translate --language en                仅翻译英文数据
  %(prog)s reelshort-translate --workers 20 --llm-batch 1   20并发 每次1条（默认）

使用示例（DramaShorts）:
  %(prog)s dramashort                                        爬取 DramaShorts 全部榜单数据（含详情）
  %(prog)s dramashort --no-detail                            仅爬取榜单列表，不爬取详情页
  %(prog)s dramashort-translate                              翻译今天全部语言数据为简体中文
  %(prog)s dramashort-translate --date 2026-03-19            翻译指定日期的数据
  %(prog)s dramashort-translate --language en                仅翻译英文数据
  %(prog)s dramashort-translate --workers 20 --llm-batch 1   20并发 每次1条（默认）

使用示例（导出）:
  %(prog)s export-drama                                      导出今日全部站点中文翻译数据（CSV，按语言分文件）
  %(prog)s export-drama --date 2026-03-19                    导出指定日期中文翻译数据
  %(prog)s export-drama --site reelshort                     仅导出 ReelShort 中文翻译数据
  %(prog)s export-drama --site dramashort                    仅导出 DramaShort 中文翻译数据
  %(prog)s export-drama --date 2026-03-19 --language en      导出指定日期英文数据
  %(prog)s export-drama --raw                                导出今日全部站点原文数据
  %(prog)s export-drama --raw --date 2026-03-19              导出指定日期原文数据
  %(prog)s export-drama --raw --site reelshort               仅导出 ReelShort 原文数据
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # crawl
    crawl_parser = subparsers.add_parser("crawl", help="爬取书籍")
    crawl_parser.add_argument("--all", action="store_true", help="爬取所有分类")
    crawl_parser.add_argument("--idx", type=int, default=0, help="目标分类索引（默认 0）")
    crawl_parser.add_argument("--limit", type=int, default=30, help="每个分类爬取的书籍数量（默认 30）")
    crawl_parser.add_argument("--detail", action="store_true", help="爬取详情页（获取章节列表）")
    crawl_parser.add_argument("--later", action="store_true", help="稍后爬取详情（先快速爬取榜单页入库）")
    crawl_parser.set_defaults(func=crawl_books)

    # crawl-auto
    crawl_auto_parser = subparsers.add_parser("crawl-auto", help="自动两阶段爬取")
    crawl_auto_parser.add_argument("--all", action="store_true")
    crawl_auto_parser.add_argument("--idx", type=int, default=0)
    crawl_auto_parser.add_argument("--limit", type=int, default=30)
    crawl_auto_parser.add_argument("--detail-limit", type=int, default=0)
    crawl_auto_parser.set_defaults(func=crawl_auto)

    # crawl-double
    crawl_double_parser = subparsers.add_parser("crawl-double", help="两轮完整爬取")
    crawl_double_parser.add_argument("--all", action="store_true")
    crawl_double_parser.add_argument("--idx", type=int, default=0)
    crawl_double_parser.add_argument("--limit", type=int, default=30)
    crawl_double_parser.add_argument("--detail-limit", type=int, default=0)
    crawl_double_parser.set_defaults(func=crawl_double)

    # export
    export_parser = subparsers.add_parser("export", help="导出数据到 CSV/JSON")
    export_parser.set_defaults(func=cmd_export)

    # stats
    stats_parser = subparsers.add_parser("stats", help="快速统计分类数量")
    stats_parser.set_defaults(func=cmd_stats)

    # verify
    verify_parser = subparsers.add_parser("verify", help="验证数据完整性")
    verify_parser.set_defaults(func=cmd_verify)

    # refill
    refill_parser = subparsers.add_parser("refill", help="补充爬取缺失章节的书籍")
    refill_parser.add_argument("--limit", type=int, default=0)
    refill_parser.add_argument("--date", type=str, default=None)
    refill_parser.set_defaults(func=refill_missing_details)

    # warm-cache
    warm_cache_parser = subparsers.add_parser("warm-cache", help="预热缓存")
    warm_cache_parser.add_argument("--date", type=str, default=None)
    warm_cache_parser.add_argument("--force", action="store_true")
    warm_cache_parser.set_defaults(func=cmd_warm_cache)

    # reelshort
    reelshort_parser = subparsers.add_parser("reelshort", help="爬取 ReelShort 短剧数据（CSV 存储）")
    reelshort_parser.add_argument("--languages", type=str, default=None)
    reelshort_parser.add_argument("--no-detail", action="store_true")
    reelshort_parser.add_argument("--workers", type=int, default=1)
    reelshort_parser.set_defaults(func=crawl_reelshort)

    # reelshort-translate
    reelshort_translate_parser = subparsers.add_parser(
        "reelshort-translate",
        help="将 reelshort_drama 表中的文本字段翻译为简体中文，存储到 reelshort_drama_zh 表",
    )
    reelshort_translate_parser.add_argument("--date", type=str, default=None)
    reelshort_translate_parser.add_argument("--language", type=str, default=None,
        help="语言代码，支持逗号分隔多语言，如 en,th,vi；不填则翻译全部语言")
    reelshort_translate_parser.add_argument("--workers", type=int, default=20,
        help="并发 LLM 请求数（默认 20）")
    reelshort_translate_parser.add_argument("--llm-batch", type=int, default=1,
        dest="llm_batch", help="每次 LLM 请求合并的记录数（默认 1）")
    reelshort_translate_parser.set_defaults(func=cmd_reelshort_translate)

    # reelshort-translate-tags
    reelshort_translate_tags_parser = subparsers.add_parser(
        "reelshort-translate-tags",
        help="翻译 reelshort_tags 表中的标签名，写入 tag_name_zh 字段",
    )
    reelshort_translate_tags_parser.add_argument("--language", type=str, default=None,
        help="指定语言代码，不填则翻译全部语言")
    reelshort_translate_tags_parser.add_argument("--workers", type=int, default=5,
        help="并发 LLM 请求数（默认 5）")
    reelshort_translate_tags_parser.add_argument("--batch-size", type=int, default=30,
        dest="batch_size", help="每次 LLM 请求翻译的标签数（默认 30）")
    reelshort_translate_tags_parser.add_argument("--force", action="store_true",
        help="强制重新翻译已有 tag_name_zh 的记录")
    reelshort_translate_tags_parser.set_defaults(func=cmd_reelshort_translate_tags)

    # dramashort
    dramashort_parser = subparsers.add_parser("dramashort", help="爬取 DramaShorts 短剧数据")
    dramashort_parser.add_argument("--languages", type=str, default=None)
    dramashort_parser.add_argument("--no-detail", action="store_true")
    dramashort_parser.set_defaults(func=crawl_dramashort)

    # dramashort-translate
    dramashort_translate_parser = subparsers.add_parser(
        "dramashort-translate",
        help="将 dramashort_drama 表中的文本字段翻译为简体中文，存储到 dramashort_drama_zh 表",
    )
    dramashort_translate_parser.add_argument("--date", type=str, default=None)
    dramashort_translate_parser.add_argument("--language", type=str, default=None)
    dramashort_translate_parser.add_argument("--workers", type=int, default=20,
        help="并发 LLM 请求数（默认 20）")
    dramashort_translate_parser.add_argument("--llm-batch", type=int, default=1,
        dest="llm_batch", help="每次 LLM 请求合并的记录数（默认 1）")
    dramashort_translate_parser.set_defaults(func=cmd_dramashort_translate)

    # export-drama
    export_drama_parser = subparsers.add_parser(
        "export-drama",
        help="导出 reelshort_drama_zh / dramashort_drama_zh 数据到 CSV（按语言分文件）",
    )
    export_drama_parser.add_argument(
        "--date", type=str, default=None,
        help="批次日期，格式 YYYY-MM-DD（默认今日 UTC+8）",
    )
    export_drama_parser.add_argument(
        "--site", type=str, choices=["reelshort", "dramashort", "all"], default="all",
        help="导出站点：reelshort / dramashort / all（默认 all）",
    )
    export_drama_parser.add_argument(
        "--language", type=str, default=None,
        help="指定语言代码（如 en、英语），不填则导出全部语言",
    )
    export_drama_parser.add_argument(
        "--raw", action="store_true",
        help="导出原文表（reelshort_drama / dramashort_drama），默认导出中文翻译表",
    )
    export_drama_parser.set_defaults(func=cmd_export_drama_zh)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if inspect.iscoroutinefunction(args.func):
        asyncio.run(args.func(args))
    else:
        args.func(args)


if __name__ == "__main__":
    main()
