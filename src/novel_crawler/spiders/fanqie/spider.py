#!/usr/bin/env python3
"""
番茄小说爬虫 - Playwright 人类模拟版
重构版本：采用模块化、面向对象设计

模块划分:
- config.py: 配置管理
- browser_manager.py: 浏览器生命周期管理
- human_simulator.py: 人类行为模拟
- page_parser.py: 页面解析
- data_processor.py: 数据处理和缓存逻辑
- spider.py: 主爬虫类 (协调各组件)
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

from loguru import logger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# 导入配置和模块
from src.novel_crawler.config import LOG_CONFIG
from src.novel_crawler.pipeline.font_mapper import get_mapper
from src.novel_crawler.config.database import db_manager, get_utc8_date
from src.novel_crawler.services.chapter_service import get_chapter_service
from src.novel_crawler.dao.fanqie_rank_dao import get_fanqie_rank_dao
from src.novel_crawler.tools.cache_manager import get_book_cache, set_book_cache, get_utc8_now_str, load_books_from_db_to_cache

from .config import SpiderConfig
from .browser_manager import BrowserManager
from .human_simulator import HumanSimulator
from .page_parser import PageParser
from .data_processor import DataProcessor
from bs4 import BeautifulSoup
import random

# ==================== 日志配置 ====================
# 配置日志
logger.remove()
logger.add(
    LOG_CONFIG["log_file"],
    format=LOG_CONFIG["format"],
    level=LOG_CONFIG["level"],
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    encoding="utf-8",
)
# 控制台输出简化格式
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=LOG_CONFIG["level"],
    colorize=False,
)


class FanqieSpider:
    """番茄小说爬虫 - 主爬虫类"""

    def __init__(self):
        # 初始化各组件
        self.config = SpiderConfig()
        self.font_mapper = get_mapper()

        # 浏览器管理器
        user_data_dir = BASE_DIR / "browser_data"
        self.browser_manager = BrowserManager(self.config, user_data_dir)

        # 人类行为模拟器 (初始时不设置 page，在 init_browser 后设置)
        self.human_simulator = HumanSimulator(None)

        # 页面解析器
        self.page_parser = PageParser(self.font_mapper)

        # Service 层和 DAO 层
        self.chapter_service = get_chapter_service()
        self.fanqie_rank_dao = get_fanqie_rank_dao(db_manager())

        # 数据处理器
        self.data_processor = DataProcessor(
            db_manager=db_manager(),
            cache_manager=self._create_cache_manager(),
            font_mapper=self.font_mapper,
            chapter_service=self.chapter_service,
            fanqie_rank_dao=self.fanqie_rank_dao
        )

        # 状态
        self.batch_date = get_utc8_date()

    def _create_cache_manager(self):
        """创建缓存管理器包装类"""
        class CacheManagerWrapper:
            @staticmethod
            def get_book_cache(book_id: str):
                return get_book_cache(book_id)

            @staticmethod
            def set_book_cache(book_id: str, status: str, last_crawl_time: str = None, book_update_time: str = None):
                set_book_cache(book_id, status, last_crawl_time, book_update_time)

        return CacheManagerWrapper()

    async def init_browser(self):
        """初始化浏览器"""
        page = await self.browser_manager.init_browser()
        self.human_simulator.set_page(page)

    async def close(self):
        """关闭浏览器"""
        await self.browser_manager.close()

    async def check_blank_page(self) -> bool:
        """检查页面是否为空白页（被封锁）"""
        try:
            page = self.browser_manager.page

            # 检查页面标题
            title = await page.title()
            logger.debug(f"页面标题：{title[:80] if title else 'None'}...")

            # 检查页面是否为空标题
            if not title or len(title.strip()) < 3:
                logger.warning("页面标题为空或过短")
                return True

            # 检查页面内容
            content = await page.content()
            logger.debug(f"页面内容长度：{len(content)}")

            # 检查页面内容是否过短（可能是空白页）
            if len(content) < 3000:
                logger.warning(f"页面内容过短：{len(content)} 字节")
                return True

            # 检查是否有章节列表区域
            chapter_area = await page.query_selector('.chapter-list, [class*="chapter"]')
            if not chapter_area:
                logger.warning("未找到章节列表区域")
                # 保存页面截图和 HTML 以便调试
                await page.screenshot(path='debug/no_chapter_page.png')
                with open('debug/no_chapter_page.html', 'w', encoding='utf-8') as f:
                    f.write(content[:10000])
                return True

            return False
        except Exception as e:
            logger.error(f"检查页面失败：{e}")
            return True

    async def crawl_rank_home(self) -> List[Dict[str, str]]:
        """爬取榜单首页，获取分类列表"""
        logger.info("\n" + "=" * 60)
        logger.info("【步骤 1】访问榜单首页")
        logger.info("=" * 60)

        page = self.browser_manager.page
        logger.info(f"访问：{self.config.rank_url}")

        await page.goto(self.config.rank_url, wait_until="networkidle", timeout=self.config.timeout)

        # 模拟人类阅读
        await self.human_simulator.simulate_human_reading()

        # 获取页面 HTML
        html = await page.content()
        logger.info(f"榜单页加载完成，HTML 长度：{len(html)}")

        # 解析分类
        categories = self.page_parser.parse_rank_categories(html)
        logger.info(f"发现 {len(categories)} 个分类")

        return categories

    async def crawl_category_books(self, category: Dict, limit: int = 30) -> List[Dict]:
        """爬取分类榜单下的书籍 - 快速模式"""
        cat_name = category['cat_name']
        cat_url = f"{self.config.base_url}/rank/{category['gender_id']}_{category['board_type']}_{category['cat_id']}"

        logger.info(f"\n爬取分类：{cat_name}")
        logger.info(f"URL: {cat_url}")

        # 一级目录：性别 + 榜单类型（男频阅读榜、女频新书榜等）
        gender = self.config.genders.get(category['gender_id'], "未知")
        board_type = self.config.board_types.get(category['board_type'], "")
        board_name = f"{gender}{board_type}"

        page = self.browser_manager.page

        # 访问分类页
        await page.goto(cat_url, wait_until="networkidle", timeout=self.config.timeout)
        await asyncio.sleep(0.1)

        all_books = []

        # 快速滚动到底部加载所有书籍
        await self.human_simulator.quick_scroll_to_bottom()

        # 获取页面 HTML
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # 解析书籍列表
        book_items = soup.find_all(class_="rank-book-item")
        logger.info(f"  找到 {len(book_items)} 本书")

        for item in book_items[:limit]:
            book = await self.page_parser.parse_book_item(item, category, board_name, self.batch_date)
            if book:
                all_books.append(book)

        logger.info(f"分类 {cat_name} 完成，共 {len(all_books)} 本书")
        return all_books

    async def crawl_book_detail(self, book: Dict, retry_count: int = 0) -> Dict:
        """爬取书籍详情页 - 获取章节列表和状态（带人类模拟）"""
        book_id = book.get("book_id")
        if not book_id:
            return book

        # 添加 enter_from=Rank 参数，模拟从排行榜进入
        detail_url = f"{self.config.base_url}/page/{book_id}?enter_from=Rank"
        # 过滤书名中的特殊字符，避免 Windows 控制台编码问题
        book_title_safe = book['book_title'].encode('gbk', 'ignore').decode('gbk', 'ignore')
        logger.info(f"  [详情] {book_title_safe}")

        page = self.browser_manager.page

        try:
            # 先访问首页建立信任（简化版）
            logger.debug("先访问首页...")
            await asyncio.sleep(random.uniform(0.5, 1.0))
            await page.goto(self.config.base_url, wait_until="load", timeout=self.config.timeout)
            await asyncio.sleep(random.uniform(1.0, 2.0))

            # 访问详情页
            await asyncio.sleep(random.uniform(self.config.base_delay_min, self.config.base_delay_max))
            logger.debug(f"访问详情页：{detail_url}")
            await page.goto(detail_url, wait_until="load", timeout=self.config.detail_timeout)
            await asyncio.sleep(random.uniform(2.0, 3.0))

            # 检查页面 URL 是否被重定向
            current_url = page.url
            logger.debug(f"当前 URL: {current_url}")
            if str(book_id) not in current_url:
                logger.warning(f"页面被重定向：{current_url}")
                if retry_count < self.config.max_retries:
                    await self.browser_manager.refresh_fingerprint()
                    logger.info(f"    [重试] 第 {retry_count + 1} 次重试...")
                    await asyncio.sleep(random.uniform(1, 2))
                    return await self.crawl_book_detail(book, retry_count + 1)

            # 检查是否为空白页
            is_blank = await self.check_blank_page()
            if is_blank:
                logger.warning(f"    [警告] 检测到空白页，可能是反爬拦截")
                if retry_count < self.config.max_retries:
                    await self.browser_manager.refresh_fingerprint()
                    logger.info(f"    [重试] 第 {retry_count + 1} 次重试...")
                    await asyncio.sleep(random.uniform(1, 2))
                    return await self.crawl_book_detail(book, retry_count + 1)
                else:
                    logger.error(f"    [ERR] 重试{self.config.max_retries}次后仍为空白页，跳过")
                    return book

            # 快速滚动到底部加载所有章节
            await self.human_simulator.human_scroll_to_bottom()

            # 模拟人类浏览行为 - 鼠标移动浏览章节
            await self.human_simulator.simulate_mouse_browse()
            await asyncio.sleep(random.uniform(0.5, 1.0))

            # 再次检查页面是否有效
            is_blank = await self.check_blank_page()
            if is_blank:
                logger.warning(f"    [警告] 滚动后检测到空白页")
                if retry_count < self.config.max_retries:
                    await self.browser_manager.refresh_fingerprint()
                    logger.info(f"    [重试] 第 {retry_count + 1} 次重试...")
                    await asyncio.sleep(random.uniform(1, 2))
                    return await self.crawl_book_detail(book, retry_count + 1)

            # 获取页面 HTML
            html = await page.content()

            # 解析详情页
            detail_data = self.page_parser.parse_book_detail(html)

            # 更新书籍数据
            book["book_status"] = detail_data["book_status"]
            if detail_data["chapter_list"]:
                book["chapter_list_json"] = json.dumps(detail_data["chapter_list"], ensure_ascii=False)
            # 如果详情页有更精确的更新时间，使用详情页的时间
            if detail_data["detail_update_time"]:
                book["book_update_time"] = detail_data["detail_update_time"]

            logger.info(f"    [OK] 详情获取完成：状态={detail_data['book_status']}, "
                        f"章节={len(detail_data['chapter_list'])}, "
                        f"更新时间={detail_data['detail_update_time'] or 'N/A'}")

        except Exception as e:
            logger.error(f"    [ERR] 详情获取失败：{e}")
            if retry_count < self.config.max_retries:
                logger.info(f"    [重试] 异常后第 {retry_count + 1} 次重试...")
                await self.browser_manager.refresh_fingerprint()
                await asyncio.sleep(random.uniform(1, 2))
                return await self.crawl_book_detail(book, retry_count + 1)

        return book

    async def _crawl_details_batch(self, books: List[Dict]) -> Tuple[int, int, int, int]:
        """
        批量爬取书籍详情

        Returns:
            (details_count, skipped_count, cached_count, reused_count)
        """
        # 使用数据处理器批量处理
        result = self.data_processor.process_batch_books(books, self.batch_date)
        books_to_crawl = result['books_to_crawl']
        skipped_count = result['skipped_count']
        cached_count = result['cached_count']
        reused_count = result['reused_count']

        if not books_to_crawl:
            logger.info("[OK] 本批次所有书籍详情已爬取过或已缓存，无需重复爬取")
            return (0, skipped_count, cached_count, reused_count)

        logger.info(f"\n开始爬取书籍详情...（共 {len(books_to_crawl)} 本需要爬取）")

        details_count = 0

        for idx, book in enumerate(books_to_crawl, 1):
            book_title_safe = book['book_title'].encode('gbk', 'ignore').decode('gbk', 'ignore')
            book_id = book.get('book_id')
            logger.info(f"[{idx}/{len(books_to_crawl)}] {book_title_safe}")

            # 爬取详情
            result = await self.crawl_book_detail(book)

            # 如果有章节数据，保存到数据库并设置缓存
            if result and result.get('chapter_list_json'):
                success = self.data_processor.save_book_detail(
                    book_id=book_id,
                    batch_date=self.batch_date,
                    book_status=result.get('book_status', '连载中'),
                    chapter_list_json=result['chapter_list_json']
                )
                if success:
                    # 设置缓存 - 保留原有的 book_update_time
                    cache_data = get_book_cache(book_id)
                    book_update_time = cache_data.get('book_update_time') if cache_data else None
                    set_book_cache(book_id, result.get('book_status', '连载中'), get_utc8_now_str(), book_update_time)
                    details_count += 1
                    logger.info(" [OK] 已更新详情并缓存")
            else:
                # 爬取失败，尝试从数据库查询历史数据作为降级方案
                db_status = self.chapter_service.get_book_status(book_id)
                if db_status:
                    if db_status == '已完结':
                        reused = self.chapter_service.copy_chapters_from_history_by_status(
                            book_id=book_id,
                            batch_date=self.batch_date,
                            book_status='已完结'
                        )
                    else:
                        reused = self.chapter_service.reuse_chapters_if_unchanged(
                            book_id=book_id,
                            batch_date=self.batch_date
                        )

                    if reused:
                        logger.info(f"[降级] {book_title_safe} - 爬取失败，从数据库获取历史数据（状态：{db_status}）")
                    else:
                        logger.warning(f"[失败] {book_title_safe} - 爬取失败且数据库中无历史数据")
                else:
                    logger.warning(f"[失败] {book_title_safe} - 爬取失败且数据库中无此书记录")

            # 随机延迟
            await asyncio.sleep(random.uniform(0.5, 1))

        return (details_count, skipped_count, cached_count, reused_count)

    async def run(self, crawl_all: bool = False, target_category_idx: int = 0,
                  limit: int = 30, crawl_detail: bool = False,
                  crawl_detail_later: bool = False, skip_crawled: bool = True) -> Dict[str, Any]:
        """
        运行爬虫

        Args:
            crawl_all: 是否爬取所有分类
            target_category_idx: 目标分类索引（crawl_all=False 时使用）
            limit: 每个分类爬取的书籍数量
            crawl_detail: 是否爬取详情页（获取章节列表）
            crawl_detail_later: 是否稍后爬取详情（先快速爬取榜单页入库，稍后再补充详情）
            skip_crawled: 是否跳过已爬取的分类（第一轮 True，第二轮补漏时 False）

        Returns:
            统计字典：{"books": 基础数据数，"details": 详情更新数，"skipped": 跳过数，"cached": 缓存命中数，"reused": 复用数}
        """
        logger.info("=" * 60)
        logger.info("开始爬取 - Playwright 人类模拟版")
        logger.info("=" * 60)

        # 先从数据库加载昨天的数据到缓存
        load_books_from_db_to_cache()

        await self.init_browser()

        try:
            # 爬取榜单首页，获取分类列表
            categories = await self.crawl_rank_home()

            if not categories:
                logger.error("未发现任何分类")
                return {"books": 0, "details": 0, "skipped": 0, "cached": 0, "reused": 0}

            logger.info(f"\n共发现 {len(categories)} 个分类")

            # 确定要爬取的分类列表
            if crawl_all:
                target_categories = categories
                logger.info(f"将爬取所有 {len(categories)} 个分类")
            else:
                if target_category_idx >= len(categories):
                    logger.error(f"分类索引 {target_category_idx} 超出范围")
                    return {"books": 0, "details": 0, "skipped": 0, "cached": 0, "reused": 0}
                target_categories = [categories[target_category_idx]]
                logger.info(f"目标分类：{target_categories[0]['cat_name']}")

            # 爬取每个分类
            total_books = 0
            total_details = 0
            skipped_details = 0
            cached_details = 0
            total_reused = 0
            skipped_categories = 0

            for cat_idx, category in enumerate(target_categories, 1):
                # 检查该分类是否已爬取（同时检查数量是否达到 limit）
                # skip_crawled=False 时强制重爬（用于第二轮补漏）
                if skip_crawled and self.data_processor.check_category_crawled(category, self.batch_date, limit):
                    cat_name = category.get('cat_name', '未知')
                    logger.info(f"[跳过] 分类 '{cat_name}' 今日已爬取（已达 {limit} 本），跳过")
                    skipped_categories += 1
                    continue

                logger.info(f"\n{'='*60}")
                logger.info(f"[{cat_idx}/{len(target_categories)}] 爬取分类：{category['cat_name']}")
                logger.info(f"{'='*60}")

                # 爬取分类下的书籍
                books = await self.crawl_category_books(category, limit)

                if not books:
                    logger.warning(f"分类 {category['cat_name']} 未爬取到任何书籍")
                    continue

                # 保存基础数据到数据库（重复的 book_id 会被 UPDATE 覆盖）
                self._save_category_data(books)
                total_books += len(books)

                # 爬取详情页
                if crawl_detail:
                    details_count, skipped_count, cached_count, reused_count = await self._crawl_details_batch(books)
                    total_details += details_count
                    skipped_details += skipped_count
                    cached_details += cached_count
                    total_reused += reused_count

                # 分类间短暂延迟
                if cat_idx < len(target_categories):
                    await asyncio.sleep(0.5)

            logger.info("\n" + "=" * 60)
            logger.info("[OK] 全部完成")
            if skipped_categories > 0:
                logger.info(f"  - 跳过分类：{skipped_categories} 个 (今日已爬取)")
            logger.info(f"  - 基础数据：{total_books} 本")
            if crawl_detail:
                logger.info(f"  - 详情更新：{total_details} 本")
                logger.info(f"  - 跳过已爬：{skipped_details} 本")
                logger.info(f"  - 缓存命中：{cached_details} 本")
                logger.info(f"  - 复用历史：{total_reused} 本")
            elif crawl_detail_later:
                logger.info("  - 详情页将在后续任务中补充（使用 crawl-missing 命令）")
            logger.info("=" * 60)

            return {"books": total_books, "details": total_details, "skipped": skipped_details, "cached": cached_details, "reused": total_reused}

        finally:
            await self.close()

    def _save_category_data(self, books: List[Dict]):
        """保存单个分类的数据"""
        # 保存到数据库
        inserted = self.data_processor.save_books_to_db(books, self.batch_date)
        logger.info(f"[DB] 成功入库 {inserted} 条记录")

        # 将每本书的更新时间存入 Redis 缓存
        for book in books:
            book_id = book.get('book_id')
            book_update_time = book.get('book_update_time')
            book_status = book.get('book_status', '连载中')
            if book_id:
                # Phase 1 只设置 book_status 和 book_update_time，不设置 last_crawl_time
                # last_crawl_time 只在 Phase 2 真正爬取详情页后才更新
                self.data_processor.cache_manager.set_book_cache(book_id, book_status, book_update_time=book_update_time)

    async def crawl_missing_details(self, limit: int = 0) -> Dict[str, int]:
        """
        补充爬取缺失章节的书籍

        Args:
            limit: 最多爬取多少本，0 表示全部

        Returns:
            统计字典：{"reused": 复用数，"crawled": 爬取数，"success": 成功数，"remaining": 剩余缺失数}
        """
        logger.info("=" * 60)
        logger.info("开始补充爬取缺失章节的书籍")
        logger.info("=" * 60)

        try:
            # 获取缺少章节的书籍 ID
            books_need_chapters = self.data_processor.get_books_without_chapters(self.batch_date)
            initial_count = len(books_need_chapters)
            logger.info(f"发现 {initial_count} 本书缺少章节数据")

            if not books_need_chapters:
                logger.info("[OK] 所有书籍都已有章节数据")
                return {"reused": 0, "crawled": 0, "success": 0, "remaining": 0}

            # 尝试从历史批次复用章节数据
            reused_count = 0
            books_to_crawl = []

            for book in books_need_chapters:
                book_id = book['book_id']
                book_title = book['book_title']

                # 尝试从历史批次复制章节数据（要求历史数据有有效的章节列表）
                if self.data_processor.reuse_chapters_from_history(book_id, self.batch_date):
                    reused_count += 1
                    book_title_safe = book_title.encode('gbk', 'ignore').decode('gbk', 'ignore')
                    logger.info(f" [复用] {book_title_safe} (从历史批次复制)")
                else:
                    books_to_crawl.append(book)

            logger.info(f"[复用] 从历史批次复用 {reused_count} 本书的章节数据")
            if reused_count > 0 and len(books_to_crawl) == 0:
                logger.info("[提示] 所有书籍都复用了历史数据")
            logger.info(f"需要爬取 {len(books_to_crawl)} 本书的详情")

            # 限制数量
            if limit > 0:
                books_to_crawl = books_to_crawl[:limit]

            if not books_to_crawl:
                logger.info("[OK] 无需爬取，所有书籍都已通过复用或已有章节数据")
                # 重新检查剩余缺失数量
                remaining = self.data_processor.get_books_without_chapters(self.batch_date)
                return {"reused": reused_count, "crawled": 0, "success": 0, "remaining": len(remaining)}

            await self.init_browser()

            try:
                # 爬取详情
                success_count = 0
                reused_count_from_crawl = 0

                for idx, book in enumerate(books_to_crawl, 1):
                    book_title_safe = book['book_title'].encode('gbk', 'ignore').decode('gbk', 'ignore')
                    logger.info(f"[{idx}/{len(books_to_crawl)}] {book_title_safe}")

                    result = await self.crawl_book_detail(book)

                    if result and result.get('chapter_list_json'):
                        # 保存到数据库（单条立即更新）
                        success = self.data_processor.save_book_detail(
                            book_id=book['book_id'],
                            batch_date=self.batch_date,
                            book_status=result.get('book_status', '连载中'),
                            chapter_list_json=result['chapter_list_json']
                        )
                        if success:
                            # 更新缓存 - 保留原有的 book_update_time
                            cache_data = get_book_cache(book['book_id'])
                            book_update_time = cache_data.get('book_update_time') if cache_data else None
                            set_book_cache(book['book_id'], result.get('book_status', '连载中'), get_utc8_now_str(), book_update_time)
                            success_count += 1
                            logger.info(" [OK] 已更新详情并缓存")
                        else:
                            logger.warning(f"[DB 失败] {book_title_safe} - 数据库更新失败")
                    else:
                        # 爬取失败，尝试从历史数据复用
                        db_status = self.chapter_service.get_book_status(book['book_id'])
                        if db_status:
                            if db_status == '已完结':
                                reused = self.chapter_service.copy_chapters_from_history_by_status(
                                    book_id=book['book_id'],
                                    batch_date=self.batch_date,
                                    book_status='已完结'
                                )
                            else:
                                reused = self.chapter_service.reuse_chapters_if_unchanged(
                                    book_id=book['book_id'],
                                    batch_date=self.batch_date
                                )
                            if reused:
                                reused_count_from_crawl += 1
                                logger.info(f" [复用] 爬取失败，从数据库复用历史数据")
                            else:
                                logger.warning(f"[失败] {book_title_safe} - 爬取失败且无历史数据")
                        else:
                            logger.warning(f"[失败] {book_title_safe} - 爬取失败且数据库中无此书记录")

                    # 随机延迟
                    await asyncio.sleep(random.uniform(1, 2))

                # 重新检查剩余缺失数量
                remaining_books = self.data_processor.get_books_without_chapters(self.batch_date)
                remaining_count = len(remaining_books)

                logger.info("=" * 60)
                logger.info(f"[OK] 补充爬取完成：成功 {success_count}/{len(books_to_crawl)} 本")
                if reused_count_from_crawl > 0:
                    logger.info(f"[复用] 从数据库复用 {reused_count_from_crawl} 本书的历史数据")
                if remaining_count > 0:
                    logger.info(f"[注意] 仍有 {remaining_count} 本书缺少章节数据（可能是爬取失败且无历史数据）")
                else:
                    logger.info("[OK] 所有书籍章节数据完整")
                logger.info("=" * 60)

                return {"reused": reused_count, "crawled": len(books_to_crawl), "success": success_count, "remaining": remaining_count}

            except Exception as e:
                logger.error(f"爬取详情页时发生异常：{e}")
                import traceback
                logger.error(traceback.format_exc())
                raise
            finally:
                await self.close()

        except Exception as e:
            logger.error(f"crawl_missing_details 发生异常：{e}")
            import traceback
            logger.error(traceback.format_exc())
            raise


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='番茄小说爬虫')
    parser.add_argument('--all', action='store_true', help='爬取所有分类')
    parser.add_argument('--idx', type=int, default=0, help='目标分类索引（默认 0）')
    parser.add_argument('--limit', type=int, default=30, help='每个分类爬取的书籍数量（默认 30）')
    parser.add_argument('--detail', action='store_true', help='爬取详情页（获取章节列表）')
    parser.add_argument('--later', action='store_true', help='稍后爬取详情（先快速爬取榜单页入库）')

    args = parser.parse_args()

    spider = FanqieSpider()
    await spider.run(
        crawl_all=args.all,
        target_category_idx=args.idx,
        limit=args.limit,
        crawl_detail=args.detail,
        crawl_detail_later=args.later
    )


if __name__ == "__main__":
    asyncio.run(main())
