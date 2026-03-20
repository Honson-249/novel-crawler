"""
DramaShorts 爬虫主模块

爬取路径：
  1. 访问主页，解析所有榜单（含 top banner 轮播）
  2. 对每个榜单，获取剧集列表（board_name、board_order、series_title、detail_url、play_count_raw）
  3. 对每部剧集，访问详情页获取 synopsis
  4. 数据清洗后写入 dramashort_drama 表

数据写入：
  - dramashort_drama 表：榜单明细（含详情页 synopsis）

并发说明：
  - 当前为单浏览器实例顺序爬取
  - 详情页缓存：内存缓存 → DB 兜底 → 实际爬取（避免重复访问）
"""
import asyncio
import random
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG, LOG_DIR
from src.novel_crawler.config.database import db_manager, get_utc8_date
from src.novel_crawler.dao.dramashort_dao import get_dramashort_dao
from src.novel_crawler.pipeline.dramashort_clean import clean_record

from .config import DramaShortConfig
from .page_parser import DramaShortPageParser

from src.novel_crawler.spiders.fanqie.browser_manager import BrowserManager
from src.novel_crawler.spiders.fanqie.human_simulator import HumanSimulator

logger.remove()
logger.add(
    LOG_DIR / "dramashort_{time:YYYY-MM-DD}.log",
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


class DramaShortSpider:
    """
    DramaShorts 短剧爬虫

    爬取策略：
    - 通过 Playwright 加载主页，解析所有榜单及剧集列表
    - 对每部剧集访问详情页获取 synopsis
    - 支持内存缓存 + DB 缓存，避免重复爬取详情页
    - 每个语言爬取完成后可自动异步触发翻译（translate=True 时）
    """

    def __init__(self):
        self.config = DramaShortConfig()
        self.page_parser = DramaShortPageParser()
        _db = db_manager()
        _db.init_database()
        self.dao = get_dramashort_dao(_db)
        self.batch_date = get_utc8_date()

        # 详情页内存缓存：{detail_url: detail_dict}
        self._detail_cache: Dict[str, Dict[str, Any]] = {}
        # Next.js build ID（从主页 HTML 中提取，用于构造 JSON API URL）
        self._build_id: Optional[str] = None

        # 后台翻译任务集合（fire-and-forget，run 结束时等待）
        self._translate_tasks: List[asyncio.Task] = []

    # ==================== 主入口 ====================

    async def run(
        self,
        languages: Optional[List[str]] = None,
        crawl_detail: bool = True,
        translate: bool = False,
        translate_workers: int = 5,
        translate_llm_batch: int = 5,
    ) -> Dict[str, Any]:
        """
        主爬取入口

        Args:
            languages: 要爬取的语言列表，None 表示使用 config.default_languages
            crawl_detail: 是否同步爬取详情页获取 synopsis（默认 True）
            translate: 每个语言爬取完成后自动异步触发翻译（默认 False）
            translate_workers: 翻译并发请求数（默认 5）
            translate_llm_batch: 每次 LLM 请求合并的记录数（默认 5）

        Returns:
            统计字典：{"dramas": 剧集数, "details": 详情更新数}
        """
        logger.info("=" * 60)
        logger.info("DramaShorts 爬虫启动")
        logger.info("=" * 60)

        target_languages = languages or self.config.default_languages
        logger.info(f"目标语言（共 {len(target_languages)} 种）：{target_languages}")
        if translate:
            logger.info(f"自动翻译：开启（workers={translate_workers}, llm_batch={translate_llm_batch}）")

        total_dramas = 0
        total_details = 0
        self._translate_tasks = []

        for language in target_languages:
            try:
                result = await self._crawl_language(language, crawl_detail)
                total_dramas += result.get("dramas", 0)
                total_details += result.get("details", 0)

                # 语言爬完后异步触发翻译（fire-and-forget）
                if translate:
                    self._fire_translate(language, translate_workers, translate_llm_batch)

            except Exception as e:
                logger.error(f"[{language}] 爬取失败：{e}")

        # 等待所有后台翻译任务完成
        if self._translate_tasks:
            logger.info(f"[DS翻译] 等待 {len(self._translate_tasks)} 个后台翻译任务完成...")
            await asyncio.gather(*self._translate_tasks, return_exceptions=True)
            logger.info("[DS翻译] 所有后台翻译任务完成")

        logger.info("\n" + "=" * 60)
        logger.info("[OK] DramaShorts 爬取完成")
        logger.info(f"  - 榜单明细：{total_dramas} 条")
        logger.info(f"  - 详情更新：{total_details} 条")
        logger.info("=" * 60)

        return {
            "dramas": total_dramas,
            "details": total_details,
        }

    def _fire_translate(self, language: str, workers: int, llm_batch: int) -> None:
        """在后台异步触发翻译任务（fire-and-forget，不阻塞爬虫）"""
        from src.novel_crawler.services.dramashort_translate_service import run_translate

        async def _task():
            logger.info(f"[DS翻译] [{language}] 语言爬取完成，开始后台翻译...")
            try:
                stats = await run_translate(
                    batch_date=self.batch_date,
                    language=language,
                    workers=workers,
                    llm_batch=llm_batch,
                )
                logger.info(
                    f"[DS翻译] [{language}] 完成："
                    f"写入 {stats['translated']} 条（复用 {stats['reused']} 条）"
                )
            except Exception as e:
                logger.error(f"[DS翻译] [{language}] 翻译任务失败：{e}")

        task = asyncio.create_task(_task())
        self._translate_tasks.append(task)

    # ==================== 语言级爬取 ====================

    async def _crawl_language(
        self,
        language: str,
        crawl_detail: bool,
    ) -> Dict[str, int]:
        """
        处理单个语言的完整爬取

        优先通过 HTTP 请求主页 __NEXT_DATA__ 获取所有榜单数据（含 synopsis），
        无需启动 Playwright。仅当 HTTP 方式失败时才回退至 Playwright。

        Args:
            language: 语言代码，如 "en"
            crawl_detail: 是否爬取详情页（HTTP 模式下 synopsis 已内嵌，此参数保留兼容）

        Returns:
            该语言下的统计数据
        """
        logger.info("\n" + "=" * 60)
        logger.info(f"开始爬取语言：{language}")
        logger.info("=" * 60)

        home_url = self.config.home_url(language)

        # 优先：HTTP 请求主页，从 __NEXT_DATA__ 解析
        boards = await self._fetch_home_via_api(home_url, language)

        if not boards:
            # 兜底：Playwright 渲染后解析
            logger.warning(f"[{language}] HTTP 方式失败，回退至 Playwright")
            user_data_dir = BASE_DIR / f"browser_data_dramashort_{language}"
            browser_manager = BrowserManager(self.config, user_data_dir)
            human_simulator = HumanSimulator(None)
            try:
                page = await browser_manager.init_browser()
                human_simulator.set_page(page)
                boards = await self._fetch_home(home_url, browser_manager, human_simulator)
            finally:
                await browser_manager.close()
                self._detail_cache.clear()

        if not boards:
            logger.warning(f"[{language}] 主页未解析到任何榜单，退出")
            return {"dramas": 0, "details": 0}

        logger.info(f"[{language}] 共解析到 {len(boards)} 个榜单")

        total_dramas = 0

        for board in boards:
            board_name = board["board_name"]
            board_order = board["board_order"]
            dramas = board["dramas"]

            logger.info(f"\n[{language}] 榜单：{board_name}（order={board_order}），共 {len(dramas)} 部剧集")

            if not dramas:
                continue

            saved = self._save_dramas(dramas, board_name, board_order, language)
            total_dramas += saved

        return {"dramas": total_dramas, "details": total_dramas}

    # ==================== 主页爬取（HTTP API）====================

    async def _fetch_home_via_api(
        self,
        home_url: str,
        language: str,
    ) -> List[Dict[str, Any]]:
        """
        通过 HTTP 请求主页，从 __NEXT_DATA__ 解析所有榜单及剧集（含 synopsis）

        Next.js SSG 将完整数据内嵌在 HTML 的 __NEXT_DATA__ script 标签中，
        一次请求即可获取所有榜单、剧集标题、detail_url、播放量、收藏量和 synopsis。

        Args:
            home_url: 主页 URL
            language: 语言代码

        Returns:
            榜单列表，失败返回空列表
        """
        headers = {
            "User-Agent": random.choice(self.config.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    home_url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"主页 HTTP 请求返回 {resp.status}：{home_url}")
                        return []
                    html = await resp.text()

            next_data = self.page_parser.extract_next_data(html)
            if not next_data:
                logger.warning("未能从主页提取 __NEXT_DATA__")
                return []

            # 同时提取 build_id 供详情 API 备用
            self._build_id = self._extract_build_id(html)

            boards = self.page_parser.parse_home_from_json(
                next_data, self.config.block_id_to_board_name
            )
            if boards:
                logger.info(f"[HTTP] 主页解析成功，共 {len(boards)} 个榜单")
            return boards

        except Exception as e:
            logger.warning(f"主页 HTTP 请求失败：{e}")
            return []

    # ==================== 主页爬取（Playwright 兜底）====================

    async def _fetch_home(
        self,
        home_url: str,
        browser_manager: BrowserManager,
        human_simulator: HumanSimulator,
    ) -> List[Dict[str, Any]]:
        """
        加载主页并解析所有榜单

        Args:
            home_url: 主页 URL
            browser_manager: 浏览器管理器
            human_simulator: 人类模拟器

        Returns:
            榜单列表
        """
        logger.info(f"访问主页：{home_url}")

        html = await self._goto_with_retry(home_url, browser_manager, human_simulator)
        if html is None:
            logger.error("主页加载失败")
            return []

        # _goto_with_retry 已等待 domcontentloaded，此处再等待 h2 榜单标题渲染完成
        page = browser_manager.page
        try:
            await page.wait_for_selector("h2", timeout=15000)
            logger.info("主页 h2 榜单标题已渲染")
        except Exception:
            logger.warning("等待 h2 榜单标题超时，尝试继续解析")

        await asyncio.sleep(random.uniform(1.5, 2.5))
        html = await page.content()

        # 提取 Next.js build ID（用于后续 JSON API 请求）
        self._build_id = self._extract_build_id(html)
        if self._build_id:
            logger.info(f"Next.js build ID：{self._build_id}")
        else:
            logger.warning("未能提取 Next.js build ID，详情将回退至 Playwright 爬取")

        # 先解析普通榜单
        boards = self.page_parser.parse_home(html)

        # 遍历 banner 所有 slide，补充完整的 top banner 数据
        banner_dramas = await self._collect_banner_slides(page)
        if banner_dramas:
            # 替换或插入 top banner 榜单（始终排在第一位）
            if boards and boards[0]["board_name"] == "top banner":
                boards[0]["dramas"] = banner_dramas
            else:
                boards.insert(0, {
                    "board_name": "top banner",
                    "board_order": 1,
                    "dramas": banner_dramas,
                })
                # 重新编号其余榜单
                for i, board in enumerate(boards[1:], 2):
                    board["board_order"] = i
            logger.info(f"top banner 共收集到 {len(banner_dramas)} 部剧集")

        return boards

    async def _collect_banner_slides(self, page) -> List[Dict[str, Any]]:
        """
        通过点击 banner 的 dot 分页按钮，逐一收集所有 slide 的剧集信息

        banner 结构：
          - dot 按钮：aria-label="Go to slide N"（共 N 个）
          - 当前展示内容：DiscoverCarousel_content__* 下的 h3 标题 + Watch Now 链接

        Args:
            page: Playwright page 对象

        Returns:
            banner 剧集列表
        """
        dramas = []
        seen_urls = set()

        try:
            # 找所有 dot 按钮（aria-label 匹配 "Go to slide N"）
            dot_selector = '[class*="DiscoverCarousel"] button[aria-label^="Go to slide"]'
            dots = await page.query_selector_all(dot_selector)
            if not dots:
                logger.warning("未找到 banner dot 按钮，跳过 banner 遍历")
                return dramas

            logger.info(f"banner 共 {len(dots)} 个 slide，开始逐一收集")

            active_cls_pattern = "DiscoverCarousel_dots__item--active"

            for i, dot in enumerate(dots):
                try:
                    await dot.click()

                    # 等待该 dot 变为 active 状态，确认切换完成（最多等 2 秒）
                    try:
                        await page.wait_for_function(
                            f"""(dot) => dot.className.includes('{active_cls_pattern}')""",
                            arg=dot,
                            timeout=2000,
                        )
                    except Exception:
                        pass

                    # 从当前展示的 content 区域提取标题和链接
                    title_elem = await page.query_selector('[class*="DiscoverCarousel_title"]')
                    link_elem = await page.query_selector('[class*="DiscoverCarousel_content"] a[href*="/shorts/"]')

                    if not link_elem:
                        logger.debug(f"  slide {i+1}: 未找到 /shorts/ 链接，跳过")
                        continue

                    href = await link_elem.get_attribute("href") or ""
                    detail_url = href if href.startswith("http") else f"https://dramashorts.io{href}"

                    if detail_url in seen_urls:
                        logger.debug(f"  slide {i+1}: URL 重复，跳过")
                        continue
                    seen_urls.add(detail_url)

                    series_title = ""
                    if title_elem:
                        series_title = (await title_elem.inner_text()).strip()

                    logger.debug(f"  slide {i+1}: {series_title} → {detail_url}")
                    dramas.append({
                        "series_title": series_title,
                        "detail_url": detail_url,
                        "play_count_raw": "",
                        "favorite_count_raw": "",
                    })

                except Exception as e:
                    logger.warning(f"  slide {i+1} 收集失败：{e}")
                    continue

        except Exception as e:
            logger.error(f"banner slide 遍历失败：{e}")

        return dramas

    # ==================== 详情页批量爬取 ====================

    async def _crawl_details_batch(
        self,
        dramas: List[Dict[str, Any]],
        board_name: str,
        board_order: int,
        language: str,
        browser_manager: BrowserManager,
        human_simulator: HumanSimulator,
    ) -> int:
        """
        批量爬取剧集详情页，获取 synopsis

        三层缓存：内存缓存 → DB 兜底 → 实际爬取

        Args:
            dramas: 剧集基础信息列表
            board_name: 所属榜单名称
            board_order: 榜单排序
            language: 当前语言
            browser_manager: 浏览器管理器
            human_simulator: 人类模拟器

        Returns:
            成功处理的剧集数量
        """
        success_count = 0
        ctx = f"[{language}][{board_name}]"

        for idx, drama in enumerate(dramas, 1):
            detail_url = drama.get("detail_url", "")
            title = drama.get("series_title", "")
            logger.info(f"  {ctx} [{idx}/{len(dramas)}] {title}")

            detail: Dict[str, Any] = {}
            need_delay = False

            # 层1：内存缓存
            if detail_url in self._detail_cache:
                detail = self._detail_cache[detail_url]
                logger.debug(f"    [内存缓存命中] {title}")
            else:
                # 层2：DB 缓存
                db_detail = self.dao.find_by_url(detail_url)
                if db_detail:
                    detail = db_detail
                    self._detail_cache[detail_url] = detail
                    logger.debug(f"    [DB 缓存命中] {title}")
                else:
                    # 层3：优先通过 Next.js JSON API 获取，失败则回退 Playwright
                    if self._build_id:
                        detail = await self._fetch_detail_via_api(detail_url, language)
                    if not detail.get("synopsis"):
                        detail = await self._fetch_detail(
                            detail_url, browser_manager, human_simulator
                        )
                    if detail.get("synopsis"):
                        self._detail_cache[detail_url] = detail
                    need_delay = True

            # 构建完整记录并清洗写入
            record = self._build_record(drama, detail, board_name, board_order, language)
            cleaned = clean_record(record)
            saved = self.dao.insert_batch([cleaned], self.batch_date)
            logger.debug(f"    [DB] 写入 {saved} 条")

            success_count += 1

            if need_delay:
                await asyncio.sleep(random.uniform(
                    self.config.base_delay_min,
                    self.config.base_delay_max,
                ))

        return success_count

    async def _fetch_detail(
        self,
        detail_url: str,
        browser_manager: BrowserManager,
        human_simulator: HumanSimulator,
    ) -> Dict[str, Any]:
        """
        通过 Playwright 访问详情页，作为 JSON API 失败时的兜底方案

        Args:
            detail_url: 详情页完整 URL
            browser_manager: 浏览器管理器
            human_simulator: 人类模拟器

        Returns:
            详情数据字典，至少包含 {"synopsis": str}
        """
        empty_result = {"synopsis": ""}

        if not detail_url:
            return empty_result

        html = await self._goto_with_retry(
            detail_url, browser_manager, human_simulator,
            timeout=self.config.detail_timeout
        )
        if html is None:
            logger.error(f"    详情页加载失败（已达最大重试次数）：{detail_url}")
            return empty_result

        await asyncio.sleep(random.uniform(1.0, 2.0))
        await human_simulator.simulate_human_reading()

        detail = self.page_parser.parse_detail(html)

        if not detail.get("synopsis"):
            logger.warning(f"    详情页 synopsis 为空：{detail_url}")

        return detail

    # ==================== 仅保存列表数据（不爬详情）====================

    def _save_dramas(
        self,
        dramas: List[Dict[str, Any]],
        board_name: str,
        board_order: int,
        language: str,
    ) -> int:
        """
        仅保存剧集列表基础数据（无详情页 synopsis）

        Args:
            dramas: 剧集基础信息列表
            board_name: 所属榜单名称
            board_order: 榜单排序
            language: 当前语言

        Returns:
            成功写入的记录数
        """
        records = []
        for drama in dramas:
            record = self._build_record(drama, {}, board_name, board_order, language)
            cleaned = clean_record(record)
            records.append(cleaned)

        if records:
            inserted = self.dao.insert_batch(records, self.batch_date)
            logger.info(f"  [DB] dramashort_drama 写入 {inserted} 条")
            return inserted
        return 0

    # ==================== 数据构建 ====================

    def _build_record(
        self,
        drama: Dict[str, Any],
        detail: Dict[str, Any],
        board_name: str,
        board_order: int,
        language: str,
    ) -> Dict[str, Any]:
        """
        构建完整的剧集记录（合并列表页数据和详情页数据）

        列表页提供：series_title、detail_url、play_count_raw、favorite_count_raw
        详情页（JSON API）提供：synopsis、play_count、favorite_count、likes_count、
                               episodes_count、score、genre

        Args:
            drama: 列表页抓取的基础数据
            detail: 详情数据字典（来自 JSON API 或 Playwright）
            board_name: 所属榜单名称
            board_order: 榜单排序
            language: 当前语言

        Returns:
            完整记录字典
        """
        # drama 中的字段来自 __NEXT_DATA__（HTTP 模式），synopsis 兜底取 detail
        synopsis = drama.get("synopsis") or detail.get("synopsis", "")
        return {
            "batch_date": self.batch_date,
            "language": language,
            "board_name": board_name,
            "board_order": board_order,
            "detail_url": drama.get("detail_url", ""),
            "series_title": drama.get("series_title", ""),
            "play_count": drama.get("play_count"),
            "favorite_count": drama.get("favorite_count"),
            "likes_count": drama.get("likes_count"),
            "episodes_count": drama.get("episodes_count"),
            "score": drama.get("score"),
            "synopsis": synopsis,
        }

    # ==================== Next.js JSON API 详情获取 ====================

    def _extract_build_id(self, html: str) -> Optional[str]:
        """
        从主页 HTML 中提取 Next.js build ID

        Next.js 在 <script id="__NEXT_DATA__"> 标签中嵌入构建元数据，
        其中 buildId 字段即为 /_next/data/ 路径中的版本标识。

        Args:
            html: 主页 HTML 字符串

        Returns:
            build ID 字符串，提取失败返回 None
        """
        import json as _json

        # 优先从 __NEXT_DATA__ script 标签提取（最可靠）
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            try:
                data = _json.loads(match.group(1))
                build_id = data.get("buildId")
                if build_id:
                    return build_id
            except Exception:
                pass

        # 兜底：从 /_next/static/{buildId}/ 路径中提取
        match = re.search(r'/_next/static/([^/]+)/_buildManifest', html)
        if match:
            return match.group(1)

        return None

    async def _fetch_detail_via_api(
        self,
        detail_url: str,
        language: str,
    ) -> Dict[str, Any]:
        """
        通过 Next.js SSG JSON API 获取剧集详情，无需 Playwright

        API URL 格式：/_next/data/{buildId}/{lang}/shorts/{uuid}.json
        返回的 JSON 包含完整的 movieDetails，含 description、viewsCount、
        favoritesCount、likesCount、episodesCount、score、genre 等字段。

        Args:
            detail_url: 剧集详情页 URL，如 https://dramashorts.io/shorts/{uuid}
            language: 语言代码，如 "en"

        Returns:
            详情数据字典，提取失败返回空字典
        """
        empty = {}

        if not self._build_id or not detail_url:
            return empty

        # 从 detail_url 中提取 UUID
        match = re.search(r'/shorts/([0-9a-f-]{36})', detail_url)
        if not match:
            logger.warning(f"    无法从 URL 提取 UUID：{detail_url}")
            return empty

        short_id = match.group(1)
        api_url = self.config.next_data_url(self._build_id, language, short_id)

        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": detail_url,
            "User-Agent": random.choice(self.config.user_agents),
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.warning(f"    JSON API 返回 {resp.status}：{api_url}")
                        return empty

                    data = await resp.json(content_type=None)

            movie = data.get("pageProps", {}).get("movieDetails", {}).get("movie", {})
            if not movie:
                logger.warning(f"    JSON API 响应中未找到 movie 数据：{api_url}")
                return empty

            synopsis = movie.get("description", "")
            logger.debug(f"    [JSON API] 获取成功：{movie.get('title', '')}")
            return {"synopsis": synopsis}

        except Exception as e:
            logger.warning(f"    JSON API 请求失败（{api_url}）：{e}")
            return empty

    # ==================== 通用页面加载（含 403 重试）====================

    async def _goto_with_retry(
        self,
        url: str,
        browser_manager: BrowserManager,
        human_simulator: HumanSimulator,
        timeout: int = None,
        retry_count: int = 0,
    ) -> Optional[str]:
        """
        加载页面并检测 403 封锁，遇到封锁时刷新指纹重试

        Args:
            url: 目标 URL
            browser_manager: 浏览器管理器
            human_simulator: 人类模拟器
            timeout: 超时毫秒数，默认使用 config.timeout
            retry_count: 当前重试次数

        Returns:
            页面 HTML，失败返回 None
        """
        if timeout is None:
            timeout = self.config.timeout

        page = browser_manager.page
        try:
            # 使用 domcontentloaded 替代 networkidle，避免 CSR 网站持续请求导致超时
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            html = await page.content()

            # 检测 403 封锁
            if (
                "403 ERROR" in html
                or "Request blocked" in html
                or ("cloudfront" in html.lower() and "403" in html)
            ):
                logger.warning(f"  检测到 403 封锁：{url}（第 {retry_count + 1} 次）")
                if retry_count < self.config.max_retries:
                    wait = random.uniform(5.0, 10.0) * (retry_count + 1)
                    logger.info(f"  等待 {wait:.1f}s 后刷新指纹重试...")
                    await asyncio.sleep(wait)
                    await browser_manager.refresh_fingerprint()
                    human_simulator.set_page(browser_manager.page)
                    return await self._goto_with_retry(
                        url, browser_manager, human_simulator, timeout, retry_count + 1
                    )
                logger.error(f"  403 封锁，已达最大重试次数，放弃：{url}")
                return None

            return html

        except Exception as e:
            logger.error(f"  页面加载失败：{url}，错误：{e}")
            if retry_count < self.config.max_retries:
                wait = random.uniform(3.0, 6.0)
                logger.info(f"  等待 {wait:.1f}s 后重试...")
                await asyncio.sleep(wait)
                await browser_manager.refresh_fingerprint()
                human_simulator.set_page(browser_manager.page)
                return await self._goto_with_retry(
                    url, browser_manager, human_simulator, timeout, retry_count + 1
                )
            return None
