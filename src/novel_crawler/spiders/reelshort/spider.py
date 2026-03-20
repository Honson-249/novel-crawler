"""
ReelShort 爬虫主模块

爬取路径：
  1. 遍历语言列表（如 en、pt）
  2. 用 _next/data API（httpx）获取各 Tab 入口页标签列表，写入 DB（reelshort_tags）
  3. 用 _next/data API（httpx）直接翻页爬取 Tab 总列表（无需遍历子 tag）
  4. 用 _next/data API（httpx）获取详情（标签、简介），category_id 直接分类

数据写入：
  - reelshort_tags 表：各 Tab 下的子分类标签维表
  - reelshort_drama 表：榜单明细（含详情页数据，标签已分类）
    唯一键：(batch_date, language, board_name, detail_url)，无 sub_category

性能说明：
  - 全程使用 httpx HTTP 请求，无需 Playwright，速度快 10 倍以上
  - 详情缓存：同语言内同一 book_id 只请求一次
  - 断点续爬：记录已爬页码，重启时从上次中断的页码继续
"""
import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG, LOG_DIR
from src.novel_crawler.config.database import db_manager, get_utc8_date
from src.novel_crawler.dao.reelshort_dao import get_reelshort_dao
from .api_client import ReelShortApiClient
from .config import ReelShortConfig
from .page_parser import ReelShortPageParser

logger.remove()
logger.add(
    LOG_DIR / "reelshort_{time:YYYY-MM-DD}.log",
    format=LOG_CONFIG["format"],
    level=LOG_CONFIG["level"],
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    encoding="utf-8",
)
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=LOG_CONFIG["level"],
    colorize=False,
)


class ReelShortSpider:
    """
    ReelShort 短剧爬虫

    爬取策略：
    - 全程使用 httpx 调用 _next/data API，无需 Playwright
    - Tab 入口页 API 返回完整标签列表（含 id 和 text）
    - 列表页 API 支持分页遍历，详情页 API 含标签分类（category_id）
    - 同语言内同一 book_id 只请求一次详情（内存缓存 + DB 兜底）
    - 每个 Tab 爬取完成后自动异步触发翻译（translate=True 时）
    """

    def __init__(self):
        self.config = ReelShortConfig()
        self.page_parser = ReelShortPageParser()
        self.dao = get_reelshort_dao(db_manager())
        self.batch_date = get_utc8_date()

        # 动态发现的 Tab 列表（run() 时填充）
        self._discovered_tabs: List[Dict[str, str]] = []

        # 详情页缓存：{language:book_id → detail_data}，同语言内跨 Tab 复用
        self._detail_cache: Dict[str, Dict[str, Any]] = {}

        # 详情页缓存并发锁
        self._detail_cache_lock = asyncio.Lock()

        # 后台翻译任务集合（fire-and-forget，run 结束时等待）
        self._translate_tasks: List[asyncio.Task] = []

    # ==================== 主入口 ====================

    async def run(
        self,
        languages: Optional[List[str]] = None,
        crawl_detail: bool = True,
        workers: int = 1,
        translate: bool = False,
        translate_workers: int = 5,
        translate_llm_batch: int = 5,
    ) -> Dict[str, Any]:
        """
        主爬取入口

        Args:
            languages: 要爬取的语言列表，None 表示使用 config.default_languages 全量爬取
            crawl_detail: 是否同步爬取详情页（默认 True）
            workers: 最大并发语言数
            translate: 每个 Tab 爬完后自动异步触发翻译（默认 False）
            translate_workers: 翻译并发请求数（默认 5）
            translate_llm_batch: 每次 LLM 请求合并的记录数（默认 5）

        Returns:
            统计字典：{"tags": 标签数, "dramas": 剧集数, "details": 详情更新数}
        """
        logger.info("=" * 60)
        logger.info("ReelShort 爬虫启动")
        logger.info("=" * 60)

        target_languages = languages or self.config.default_languages

        self._discovered_tabs = [
            {"tab_name": name, "tab_slug": slug}
            for name, slug in self.config.tab_slugs.items()
        ]

        logger.info(f"目标语言（共 {len(target_languages)} 种）：{target_languages}")
        logger.info(f"Tab 列表：{[t['tab_slug'] for t in self._discovered_tabs]}")
        logger.info(f"并发 Worker 数：{workers}")
        if translate:
            logger.info(f"自动翻译：开启（workers={translate_workers}, llm_batch={translate_llm_batch}）")

        total_tags = 0
        total_dramas = 0
        total_details = 0
        self._translate_tasks = []

        semaphore = asyncio.Semaphore(workers)

        async def _run_with_sem(lang: str):
            async with semaphore:
                return await self._crawl_language_worker(
                    lang, crawl_detail, translate, translate_workers, translate_llm_batch
                )

        results = await asyncio.gather(
            *[_run_with_sem(lang) for lang in target_languages],
            return_exceptions=True,
        )
        for lang, result in zip(target_languages, results):
            if isinstance(result, Exception):
                logger.error(f"[{lang}] 爬取失败：{result}")
            else:
                total_tags += result.get("tags", 0)
                total_dramas += result.get("dramas", 0)
                total_details += result.get("details", 0)

        # 等待所有后台翻译任务完成
        if self._translate_tasks:
            logger.info(f"[翻译] 等待 {len(self._translate_tasks)} 个后台翻译任务完成...")
            await asyncio.gather(*self._translate_tasks, return_exceptions=True)
            logger.info("[翻译] 所有后台翻译任务完成")

        logger.info("\n" + "=" * 60)
        logger.info("[OK] ReelShort 爬取完成")
        logger.info(f"  - 标签维表：{total_tags} 条")
        logger.info(f"  - 榜单明细：{total_dramas} 条")
        logger.info(f"  - 详情更新：{total_details} 条")
        logger.info("=" * 60)

        return {
            "tags": total_tags,
            "dramas": total_dramas,
            "details": total_details,
        }

    # ==================== 语言级 Worker ====================

    async def _crawl_language_worker(
        self,
        language: str,
        crawl_detail: bool,
        translate: bool = False,
        translate_workers: int = 5,
        translate_llm_batch: int = 5,
    ) -> Dict[str, int]:
        """
        单语言爬取 Worker，持有独立的 httpx 连接池，多 Worker 并发安全
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始爬取语言：{language}")
        logger.info(f"{'='*60}")

        try:
            return await self._crawl_language(
                language, crawl_detail, translate, translate_workers, translate_llm_batch
            )
        except Exception as e:
            logger.error(f"[{language}] Worker 异常：{e}")
            raise
        finally:
            async with self._detail_cache_lock:
                cleared = len(self._detail_cache)
                self._detail_cache.clear()
            if cleared:
                logger.info(f"[{language}] 已清理详情缓存 {cleared} 条")

    # ==================== 语言级爬取 ====================

    async def _crawl_language(
        self,
        language: str,
        crawl_detail: bool,
        translate: bool = False,
        translate_workers: int = 5,
        translate_llm_batch: int = 5,
    ) -> Dict[str, int]:
        """
        处理单个语言的完整爬取（全程使用 httpx API，无需 Playwright）
        每个 Tab 爬完后，若 translate=True 则异步触发该语言的翻译任务
        """
        total_tags = 0
        total_dramas = 0
        total_details = 0

        async with ReelShortApiClient(
            delay_min=self.config.page_delay_min,
            delay_max=self.config.page_delay_max,
        ) as api_client:
            # 第一阶段：通过 API 获取各 Tab 的子分类标签列表并写入 DB
            tab_tag_map: Dict[str, List[Dict[str, str]]] = {}
            logger.info(f"  [{language}] 第一阶段：获取 Tab 标签列表（API）")
            for tab_info in self._discovered_tabs:
                tab_name = tab_info["tab_name"]
                tab_slug = tab_info["tab_slug"]

                data = await api_client.fetch_tab_index(language, tab_slug)
                if data is None:
                    logger.warning(f"  [{language}] Tab '{tab_name}' 入口页 API 失败，跳过")
                    tab_tag_map[tab_name] = []
                    continue

                tag_items = self.page_parser.parse_api_tab_index(data, tab_slug, tab_name, language)
                tab_tag_map[tab_name] = tag_items
                total_tags += len(tag_items)
                logger.info(f"  [{language}] Tab '{tab_name}' 共 {len(tag_items)} 个子分类标签")

                records = [
                    {"language": language, "tab_name": tab_name, "tag_name": item["tag_name"]}
                    for item in tag_items
                ]
                inserted = self.dao.insert_tags_batch(records, self.batch_date)
                logger.info(f"  [DB] reelshort_tags 写入 {inserted} 条（Tab: {tab_name}）")

            # 第二阶段：按 Tab 总列表翻页爬取（不再遍历子 tag）
            logger.info(f"  [{language}] 第二阶段：爬取 Tab 总列表和详情（API）")
            for tab_info in self._discovered_tabs:
                tab_name = tab_info["tab_name"]
                tab_slug = tab_info["tab_slug"]

                logger.info(f"\n  [{language}] Tab: {tab_name}")

                start_page = self.dao.find_last_crawled_page(self.batch_date, language, tab_name)
                if start_page > 1:
                    logger.info(f"  [{language}][{tab_name}] 检测到已爬 {start_page - 1} 页，从第 {start_page} 页续爬")

                dramas, details_count = await self._crawl_tab_total_list(
                    tab_name, tab_slug, language, api_client, crawl_detail, start_page
                )
                total_dramas += dramas
                total_details += details_count

                # Tab 爬完后，异步触发翻译（fire-and-forget）
                if translate:
                    self._fire_translate(language, translate_workers, translate_llm_batch, tab_name)

        return {
            "tags": total_tags,
            "dramas": total_dramas,
            "details": total_details,
        }

    def _fire_translate(
        self,
        language: str,
        workers: int,
        llm_batch: int,
        tab_name: str,
    ) -> None:
        """在后台异步触发翻译任务（fire-and-forget，不阻塞爬虫）"""
        from src.novel_crawler.services.reelshort_translate_service import run_translate

        async def _task():
            logger.info(f"[翻译] [{language}][{tab_name}] Tab 爬取完成，开始后台翻译...")
            try:
                stats = await run_translate(
                    batch_date=self.batch_date,
                    language=language,
                    workers=workers,
                    llm_batch=llm_batch,
                )
                logger.info(
                    f"[翻译] [{language}][{tab_name}] 完成："
                    f"写入 {stats['translated']} 条（复用 {stats['reused']} 条）"
                )
            except Exception as e:
                logger.error(f"[翻译] [{language}][{tab_name}] 翻译任务失败：{e}")

        task = asyncio.create_task(_task())
        self._translate_tasks.append(task)

    # ==================== Tab 总列表爬取（API）====================

    async def _crawl_tab_total_list(
        self,
        tab_name: str,
        tab_slug: str,
        language: str,
        api_client: ReelShortApiClient,
        crawl_detail: bool,
        start_page: int = 1,
    ) -> tuple:
        """
        通过 Tab 总列表 API 翻页爬取该 Tab 下所有剧集

        不再按子 tag 遍历，直接请求 Tab 级总列表，每页写入一次 DB。
        断点续爬：start_page > 1 时从指定页码开始，之前的页已爬完。

        Returns:
            (total_dramas, total_details) 元组
        """
        ctx = f"[{language}][{tab_name}]"
        total_dramas = 0
        total_details = 0

        # 先获取第一页（或续爬起始页）以拿到总页数
        data = await api_client.fetch_tab_total_list(language, tab_slug, page=start_page)
        if data is None:
            logger.error(f"  {ctx} 第 {start_page} 页 API 请求失败，跳过该 Tab")
            return 0, 0

        dramas, total_pages = self.page_parser.parse_api_list_page(data, language)
        logger.info(f"  {ctx} 共 {total_pages} 页，第 {start_page} 页 {len(dramas)} 部剧集")

        if dramas:
            d, det = await self._process_page_dramas(
                dramas, tab_name, language, api_client, crawl_detail, start_page
            )
            total_dramas += d
            total_details += det

        for page_num in range(start_page + 1, total_pages + 1):
            data = await api_client.fetch_tab_total_list(language, tab_slug, page=page_num)
            if data is None:
                logger.error(f"  {ctx} 第 {page_num} 页 API 请求失败，跳过")
                continue
            page_dramas, _ = self.page_parser.parse_api_list_page(data, language)
            logger.info(f"  {ctx} 第 {page_num}/{total_pages} 页 {len(page_dramas)} 部剧集")
            if page_dramas:
                d, det = await self._process_page_dramas(
                    page_dramas, tab_name, language, api_client, crawl_detail, page_num
                )
                total_dramas += d
                total_details += det

        logger.info(f"  {ctx} 完成，共 {total_dramas} 部剧集，{total_details} 条详情")
        return total_dramas, total_details

    # ==================== 单页剧集处理（含详情）====================

    async def _process_page_dramas(
        self,
        dramas: List[Dict[str, Any]],
        tab_name: str,
        language: str,
        api_client: ReelShortApiClient,
        crawl_detail: bool,
        page_num: int,
    ) -> tuple:
        """
        处理单页剧集列表：爬取详情（可选）并写入 DB

        Returns:
            (dramas_count, details_count) 元组
        """
        details_count = 0
        ctx = f"[{language}][{tab_name}] 第{page_num}页"

        if crawl_detail:
            for idx, drama in enumerate(dramas, 1):
                book_id = drama.get("book_id", "")
                detail_url = drama.get("detail_url", "")
                title = drama.get("series_title", "")
                logger.info(f"    {ctx} [{idx}/{len(dramas)}] {title}")

                if not book_id:
                    logger.warning(f"      缺少 book_id，跳过：{title}")
                    continue

                # 内存缓存（同语言内跨 Tab 复用）
                cache_key = f"{language}:{book_id}"
                async with self._detail_cache_lock:
                    cached = self._detail_cache.get(cache_key)

                if cached:
                    detail = cached
                    logger.debug(f"      [内存缓存命中] {title}")
                else:
                    db_detail = self.dao.find_detail_by_url(detail_url, language)
                    if db_detail:
                        detail = db_detail
                        async with self._detail_cache_lock:
                            self._detail_cache[cache_key] = detail
                        logger.debug(f"      [DB 缓存命中] {title}")
                    else:
                        # 用 book_id 请求，api_client 内部自动跟随 __N_REDIRECT 获取正确 slug
                        api_data = await api_client.fetch_drama_detail(language, book_id)
                        if api_data:
                            detail = self.page_parser.parse_api_drama_detail(api_data)
                            # 详情 API 的 synopsis 优先；若为空则用列表页已有的 synopsis 兜底
                            if not detail.get("synopsis") and drama.get("synopsis"):
                                detail["synopsis"] = drama["synopsis"]
                            if detail.get("synopsis") or detail.get("tag_list"):
                                async with self._detail_cache_lock:
                                    self._detail_cache[cache_key] = detail
                        else:
                            # 详情 API 失败（如该语言无此剧），用列表页数据兜底
                            detail = {
                                "tag_list": [], "actors_tags": [], "actresses_tags": [],
                                "identity_tags": [], "story_beat_tags": [], "genre_tags": [],
                                "synopsis": drama.get("synopsis", ""),
                                "play_count_raw": "", "play_count": None,
                                "favorite_count_raw": "", "favorite_count": None,
                            }

                merged = self._merge_drama_data(drama, detail, tab_name, language)
                saved = self.dao.insert_drama_batch([merged], self.batch_date)
                logger.debug(f"      [DB] 写入 {saved} 条")
                details_count += 1
        else:
            records = [self._merge_drama_data(d, {}, tab_name, language) for d in dramas]
            inserted = self.dao.insert_drama_batch(records, self.batch_date)
            logger.info(f"    {ctx} [DB] 写入 {inserted} 条（无详情）")

        return len(dramas), details_count

    def _merge_drama_data(
        self,
        list_data: Dict[str, Any],
        detail_data: Dict[str, Any],
        tab_name: str,
        language: str,
    ) -> Dict[str, Any]:
        """合并 API 列表页数据和详情页数据，输出 DB 记录格式（无 sub_category）"""
        import json

        def to_json(lst):
            return json.dumps(lst, ensure_ascii=False) if lst else None

        play_count = detail_data.get("play_count") or list_data.get("play_count")
        favorite_count = detail_data.get("favorite_count") or list_data.get("favorite_count")
        play_count_raw = detail_data.get("play_count_raw") or list_data.get("play_count_raw") or ""
        favorite_count_raw = detail_data.get("favorite_count_raw") or list_data.get("favorite_count_raw") or ""
        tag_list = detail_data.get("tag_list", [])

        return {
            "batch_date": self.batch_date,
            "language": language,
            "board_name": tab_name,
            "detail_url": list_data.get("detail_url", ""),
            "series_title": list_data.get("series_title", ""),
            "t_book_id": list_data.get("t_book_id"),
            "play_count_raw": play_count_raw,
            "play_count": play_count,
            "favorite_count_raw": favorite_count_raw,
            "favorite_count": favorite_count,
            "tag_list_json": to_json(tag_list),
            "actors_tags": to_json(detail_data.get("actors_tags")),
            "actresses_tags": to_json(detail_data.get("actresses_tags")),
            "identity_tags": to_json(detail_data.get("identity_tags")),
            "story_beat_tags": to_json(detail_data.get("story_beat_tags")),
            "genre_tags": to_json(detail_data.get("genre_tags")),
            # 详情页 synopsis 优先，列表页 synopsis 兜底（列表页已包含 special_desc）
            "synopsis": detail_data.get("synopsis") or list_data.get("synopsis", ""),
        }

