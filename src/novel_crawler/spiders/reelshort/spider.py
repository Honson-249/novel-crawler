"""
ReelShort 爬虫主模块

爬取路径：
  1. 遍历语言列表（如 en、pt）
  2. 用 _next/data API（httpx）获取各 Tab 入口页标签列表 → 缓存在内存
  3. 用 _next/data API（httpx）直接翻页爬取 Tab 总列表
  4. 用 _next/data API（httpx）获取详情（标签、简介）
  5. 利用标签缓存做分类 → 写入 CSV 文件

数据写入：
  - CSV 文件：/data/reelshort/{batch_date}/{language}.csv
  - 字段：batch_date, language, detail_url, series_title, play_count, favorite_count,
         synopsis, tag_list_json, identity_tags, story_beat_tags, genre_tags
  - 同一天运行会覆盖已有文件

性能说明：
  - 全程使用 httpx HTTP 请求，无需 Playwright，速度快 10 倍以上
  - 标签缓存：每语言爬完后立即清理，节省内存
  - 详情缓存：同语言内同一 book_id 只请求一次
"""
import asyncio
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG, LOG_DIR
from src.novel_crawler.config.database import get_utc8_date
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
    ReelShort 短剧爬虫（CSV 存储版本）

    爬取策略：
    - 全程使用 httpx 调用 _next/data API，无需 Playwright
    - Tab 入口页 API 返回完整标签列表（含 id 和 text）
    - 列表页 API 支持分页遍历，详情页 API 含标签分类（category_id）
    - 标签参照集仅缓存在内存中，每语言爬完后立即清理
    - 同语言内同一 book_id 只请求一次详情（内存缓存）
    - 数据直接写入 CSV 文件，不依赖数据库
    """

    def __init__(self):
        self.config = ReelShortConfig()
        self.page_parser = ReelShortPageParser()
        self.batch_date = get_utc8_date()

        # 动态发现的 Tab 列表（run() 时填充）
        self._discovered_tabs: List[Dict[str, str]] = []

        # 详情页缓存：{language:book_id → detail_data}，同语言内跨 Tab 复用
        self._detail_cache: Dict[str, Dict[str, Any]] = {}

        # 详情页缓存并发锁
        self._detail_cache_lock = asyncio.Lock()

        # 标签参照集缓存：{language: {tab_name: {tag_name_set}}}
        # 用于标签分类：Actors/Actresses/Identities/Story Beats
        self._tag_reference_cache: Dict[str, Dict[str, Set[str]]] = {}

        # 标签缓存并发锁
        self._tag_cache_lock = asyncio.Lock()

        # 已爬取的 detail_url 集合（同语言内去重，避免重复爬取）
        # key: language, value: Set[detail_url]
        self._crawled_urls: Dict[str, Set[str]] = {}

        # 已爬取 URL 锁
        self._crawled_urls_lock = asyncio.Lock()

    # ==================== 主入口 ====================

    async def run(
        self,
        languages: Optional[List[str]] = None,
        crawl_detail: bool = True,
        workers: int = 1,
    ) -> Dict[str, Any]:
        """
        主爬取入口

        Args:
            languages: 要爬取的语言列表，None 表示使用 config.default_languages 全量爬取
            crawl_detail: 是否同步爬取详情页（默认 True）
            workers: 最大并发语言数

        Returns:
            统计字典：{"dramas": 剧集数, "details": 详情更新数}
        """
        logger.info("=" * 60)
        logger.info("ReelShort 爬虫启动（CSV 存储版本）")
        logger.info("=" * 60)

        target_languages = languages or self.config.default_languages

        self._discovered_tabs = [
            {"tab_name": name, "tab_slug": slug}
            for name, slug in self.config.tab_slugs.items()
        ]

        logger.info(f"目标语言（共 {len(target_languages)} 种）：{target_languages}")
        logger.info(f"Tab 列表：{[t['tab_slug'] for t in self._discovered_tabs]}")
        logger.info(f"并发 Worker 数：{workers}")

        total_dramas = 0
        total_details = 0

        semaphore = asyncio.Semaphore(workers)

        async def _run_with_sem(lang: str):
            async with semaphore:
                return await self._crawl_language_worker(lang, crawl_detail)

        results = await asyncio.gather(
            *[_run_with_sem(lang) for lang in target_languages],
            return_exceptions=True,
        )
        for lang, result in zip(target_languages, results):
            if isinstance(result, Exception):
                logger.error(f"[{lang}] 爬取失败：{result}")
            else:
                total_dramas += result.get("dramas", 0)
                total_details += result.get("details", 0)

        logger.info("\n" + "=" * 60)
        logger.info("[OK] ReelShort 爬取完成")
        logger.info(f"  - 榜单明细：{total_dramas} 条")
        logger.info(f"  - 详情更新：{total_details} 条")
        logger.info("=" * 60)

        return {
            "dramas": total_dramas,
            "details": total_details,
        }

    # ==================== 语言级 Worker ====================

    async def _crawl_language_worker(
        self,
        language: str,
        crawl_detail: bool,
    ) -> Dict[str, int]:
        """
        单语言爬取 Worker，持有独立的 httpx 连接池，多 Worker 并发安全
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始爬取语言：{language}")
        logger.info(f"{'='*60}")

        try:
            return await self._crawl_language(language, crawl_detail)
        except Exception as e:
            logger.error(f"[{language}] Worker 异常：{e}")
            raise
        finally:
            # 清理该语言的标签缓存（方案 A：每语言完成后立即清理）
            async with self._tag_cache_lock:
                self._tag_reference_cache.pop(language, None)
                logger.info(f"[{language}] 已清理标签缓存")

            # 清理已爬取 URL 缓存
            async with self._crawled_urls_lock:
                crawled_count = len(self._crawled_urls.get(language, set()))
                self._crawled_urls.pop(language, None)
                logger.info(f"[{language}] 已清理已爬取 URL 缓存 ({crawled_count} 条)")

            # 清理详情缓存
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
    ) -> Dict[str, int]:
        """
        处理单个语言的完整爬取（全程使用 httpx API，数据写入 CSV）
        """
        total_dramas = 0
        total_details = 0

        async with ReelShortApiClient(
            delay_min=self.config.page_delay_min,
            delay_max=self.config.page_delay_max,
        ) as api_client:
            # 第一阶段：获取各 Tab 的子分类标签列表并缓存到内存
            tab_tag_map: Dict[str, List[Dict[str, str]]] = {}
            logger.info(f"  [{language}] 第一阶段：获取 Tab 标签列表（API）→ 缓存到内存")
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
                logger.info(f"  [{language}] Tab '{tab_name}' 共 {len(tag_items)} 个子分类标签")

                # 将标签存入缓存（用于后续分类）
                async with self._tag_cache_lock:
                    if language not in self._tag_reference_cache:
                        self._tag_reference_cache[language] = {}
                    # 提取 tag_name 集合
                    self._tag_reference_cache[language][tab_name] = {
                        item["tag_name"] for item in tag_items
                    }

            # 第二阶段：按 Tab 总列表翻页爬取
            logger.info(f"  [{language}] 第二阶段：爬取 Tab 总列表和详情（API）→ 每 Tab 写入 CSV")

            # 初始化已爬取 URL 集合（同语言内去重）
            async with self._crawled_urls_lock:
                self._crawled_urls[language] = set()

            # CSV 文件路径（每 Tab 追加写入）
            # 使用相对路径：项目根目录下的 data/reelshort 目录
            csv_dir = Path("data") / "reelshort" / self.batch_date
            csv_dir.mkdir(parents=True, exist_ok=True)  # 自动创建 data/reelshort/{日期} 所有层级

            # 爬取过程中使用 crawling_ 前缀，完成后重命名为正式文件
            temp_csv_path = csv_dir / f"crawling_{language}.csv"
            final_csv_path = csv_dir / f"{language}.csv"

            # 删除旧文件（如果存在），确保每次运行都是全新覆盖
            if temp_csv_path.exists():
                temp_csv_path.unlink()
                logger.info(f"  [{language}] 已删除旧的临时 CSV 文件")
            if final_csv_path.exists():
                final_csv_path.unlink()
                logger.info(f"  [{language}] 已删除旧的正式 CSV 文件，准备覆盖")

            # 标记是否首次写入（用于决定是否写 header）
            is_first_write = True

            # 已写入 CSV 的 URL 集合（跨 Tab 去重）
            written_urls: Set[str] = set()

            for tab_info in self._discovered_tabs:
                tab_name = tab_info["tab_name"]
                tab_slug = tab_info["tab_slug"]

                logger.info(f"\n  [{language}] Tab: {tab_name}")

                tab_dramas, details_count = await self._crawl_tab_total_list(
                    tab_name, tab_slug, language, api_client, crawl_detail
                )

                # 去重：跨 Tab 重复的剧集剔除
                unique_dramas = []
                cross_tab_duplicates = 0
                for drama in tab_dramas:
                    url = drama.get("detail_url")
                    if url and url in written_urls:
                        cross_tab_duplicates += 1
                        continue
                    written_urls.add(url)
                    unique_dramas.append(drama)

                if cross_tab_duplicates > 0:
                    logger.info(f"  [{language}][{tab_name}] 跨 Tab 去重：移除 {cross_tab_duplicates} 条")

                # 本 Tab 新增的剧集数（去重后）
                total_dramas += len(unique_dramas)
                total_details += details_count

                # 写入 CSV（追加模式）
                logger.info(f"  [{language}][{tab_name}] 写入 CSV：{len(unique_dramas)} 条记录")
                self._write_to_csv(unique_dramas, temp_csv_path, append=not is_first_write)
                is_first_write = False

            # 所有 Tab 完成后，重命名为正式文件
            if temp_csv_path.exists():
                temp_csv_path.rename(final_csv_path)
                logger.info(f"  [{language}] CSV 写入完成，重命名为 → {final_csv_path}")
            else:
                logger.warning(f"  [{language}] CSV 临时文件不存在，可能写入失败")

        return {
            "dramas": total_dramas,
            "details": total_details,
        }

    # ==================== Tab 总列表爬取（API）====================

    async def _crawl_tab_total_list(
        self,
        tab_name: str,
        tab_slug: str,
        language: str,
        api_client: ReelShortApiClient,
        crawl_detail: bool,
    ) -> tuple:
        """
        通过 Tab 总列表 API 翻页爬取该 Tab 下所有剧集

        Returns:
            (dramas_list, details_count) 元组
        """
        ctx = f"[{language}][{tab_name}]"
        all_dramas: List[Dict[str, Any]] = []
        total_details = 0

        # 先获取第一页以拿到总页数
        data = await api_client.fetch_tab_total_list(language, tab_slug, page=1)
        if data is None:
            logger.error(f"  {ctx} 第 1 页 API 请求失败，跳过该 Tab")
            return [], 0

        page_dramas, total_pages = self.page_parser.parse_api_list_page(data, language)
        logger.info(f"  {ctx} 共 {total_pages} 页，第 1 页 {len(page_dramas)} 部剧集")

        for drama in page_dramas:
            processed = await self._process_drama(
                drama, tab_name, language, api_client, crawl_detail
            )
            if processed:
                all_dramas.append(processed)
                total_details += 1

        for page_num in range(2, total_pages + 1):
            data = await api_client.fetch_tab_total_list(language, tab_slug, page=page_num)
            if data is None:
                logger.error(f"  {ctx} 第 {page_num} 页 API 请求失败，跳过")
                continue
            page_dramas, _ = self.page_parser.parse_api_list_page(data, language)
            logger.info(f"  {ctx} 第 {page_num}/{total_pages} 页 {len(page_dramas)} 部剧集")

            for drama in page_dramas:
                processed = await self._process_drama(
                    drama, tab_name, language, api_client, crawl_detail
                )
                if processed:
                    all_dramas.append(processed)
                    total_details += 1

        logger.info(f"  {ctx} 完成，共 {len(all_dramas)} 部剧集，{total_details} 条详情")
        return all_dramas, total_details

    # ==================== 单部剧集处理（含详情）====================

    async def _process_drama(
        self,
        drama: Dict[str, Any],
        tab_name: str,
        language: str,
        api_client: ReelShortApiClient,
        crawl_detail: bool,
    ) -> Optional[Dict[str, Any]]:
        """
        处理单部剧集：爬取详情（可选）并做标签分类

        Returns:
            处理后的剧集数据字典
        """
        book_id = drama.get("book_id", "")
        detail_url = drama.get("detail_url", "")
        title = drama.get("series_title", "")

        if not book_id:
            logger.warning(f"      缺少 book_id，跳过：{title}")
            return None

        # 实时去重：检查该 detail_url 是否已经爬取过（同语言内）
        async with self._crawled_urls_lock:
            if detail_url in self._crawled_urls.get(language, set()):
                logger.debug(f"      [URL 已爬取] 跳过：{title} - {detail_url}")
                return None

        # 内存缓存（同语言内跨 Tab 复用）
        cache_key = f"{language}:{book_id}"
        async with self._detail_cache_lock:
            cached = self._detail_cache.get(cache_key)

        if cached:
            detail = cached
            logger.debug(f"      [内存缓存命中] {title}")
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
                    "tag_list": [], "synopsis": drama.get("synopsis", ""),
                    "play_count_raw": "", "play_count": None,
                    "favorite_count_raw": "", "favorite_count": None,
                }

        # 标记该 URL 已爬取
        async with self._crawled_urls_lock:
            if language not in self._crawled_urls:
                self._crawled_urls[language] = set()
            self._crawled_urls[language].add(detail_url)

        return self._merge_and_classify_drama(drama, detail, tab_name, language)

    def _merge_and_classify_drama(
        self,
        list_data: Dict[str, Any],
        detail_data: Dict[str, Any],
        tab_name: str,
        language: str,
    ) -> Dict[str, Any]:
        """
        合并 API 列表页数据和详情页数据，并利用标签缓存做分类

        输出字段：
        - batch_date, language, detail_url, series_title
        - play_count, favorite_count, synopsis
        - identity_tags, story_beat_tags, genre_tags
        """
        play_count = detail_data.get("play_count") or list_data.get("play_count")
        favorite_count = detail_data.get("favorite_count") or list_data.get("favorite_count")
        tag_list = detail_data.get("tag_list", [])

        # 标签分类
        classified = self._classify_tags(tag_list, language)

        return {
            "batch_date": self.batch_date,
            "language": language,
            "detail_url": list_data.get("detail_url", ""),
            "series_title": list_data.get("series_title", ""),
            "play_count": play_count,
            "favorite_count": favorite_count,
            "synopsis": detail_data.get("synopsis") or list_data.get("synopsis", ""),
            "identity_tags": json.dumps(classified["identity_tags"], ensure_ascii=False),
            "story_beat_tags": json.dumps(classified["story_beat_tags"], ensure_ascii=False),
            "genre_tags": json.dumps(classified["genre_tags"], ensure_ascii=False),
        }

    def _classify_tags(
        self,
        tag_list: List[str],
        language: str,
    ) -> Dict[str, List[str]]:
        """
        利用标签缓存对 tag_list 进行分类

        分类逻辑：
        - 在 Actors 参照集中 → 忽略（不要 actors_tags）
        - 在 Actresses 参照集中 → 忽略（不要 actresses_tags）
        - 在 Identities 参照集中 → identity_tags
        - 在 Story Beats 参照集中 → story_beat_tags
        - 其他 → genre_tags

        Returns:
            {identity_tags: [], story_beat_tags: [], genre_tags: []}
        """
        # 获取该语言的标签参照集
        lang_refs = self._tag_reference_cache.get(language, {})
        actors_ref = lang_refs.get("Actors", set())
        actresses_ref = lang_refs.get("Actresses", set())
        identity_ref = lang_refs.get("Identities", set())
        story_beat_ref = lang_refs.get("Story Beats", set())

        identity_tags = []
        story_beat_tags = []
        genre_tags = []

        for tag in tag_list:
            if tag in actors_ref or tag in actresses_ref:
                # 演员名字标签，忽略
                continue
            elif tag in identity_ref:
                identity_tags.append(tag)
            elif tag in story_beat_ref:
                story_beat_tags.append(tag)
            else:
                # 不在 Identities/Story Beats 中的都归为 genre_tags
                genre_tags.append(tag)

        return {
            "identity_tags": identity_tags,
            "story_beat_tags": story_beat_tags,
            "genre_tags": genre_tags,
        }

    def _write_to_csv(
        self,
        dramas: List[Dict[str, Any]],
        csv_path: Path,
        append: bool = False,
    ) -> None:
        """
        将剧集数据写入 CSV 文件

        CSV 字段（共 9 列）：
        - batch_date, language, detail_url, series_title
        - play_count, favorite_count, synopsis
        - identity_tags, story_beat_tags, genre_tags

        Args:
            append: True 为追加模式，False 为覆盖模式（写入 header）
        """
        if not dramas:
            logger.warning(f"  没有数据可写入 CSV")
            return

        fieldnames = [
            "batch_date", "language", "detail_url", "series_title",
            "play_count", "favorite_count", "synopsis",
            "identity_tags", "story_beat_tags", "genre_tags",
        ]

        mode = 'a' if append else 'w'
        with open(csv_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not append:
                writer.writeheader()
            writer.writerows(dramas)

        logger.info(f"  CSV 文件已写入：{csv_path}（{len(dramas)} 条记录）")
