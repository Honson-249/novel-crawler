#!/usr/bin/env python3
"""
数据处理模块 - 负责缓存管理、数据库操作和数据存储
"""
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from loguru import logger


class DataProcessor:
    """数据处理器 - 负责缓存管理、数据库操作和数据存储"""

    def __init__(self, db_manager, cache_manager, font_mapper, chapter_service=None, fanqie_rank_dao=None):
        self.db_manager = db_manager
        self.cache_manager = cache_manager
        self.font_mapper = font_mapper
        self.chapter_service = chapter_service
        self.fanqie_rank_dao = fanqie_rank_dao

    def check_book_needs_crawl(self, book: Dict, batch_date: str) -> Tuple[bool, str]:
        """
        检查书籍是否需要爬取详情

        Returns:
            (needs_crawl, reason): 是否需要爬取及原因
        """
        book_id = book.get('book_id')
        book_title = book.get('book_title', '未知书名')

        # 1. 检查 Redis 缓存
        cache_data = self.cache_manager.get_book_cache(book_id)

        if cache_data:
            cache_status = cache_data.get('book_status')
            last_crawl_time = cache_data.get('last_crawl_time')
            book_update_time = cache_data.get('book_update_time')

            # 情况 1: 已完结，直接从数据库获取目录项
            if cache_status == '已完结' and self.chapter_service:
                reused = self.chapter_service.copy_chapters_from_history_by_status(
                    book_id=book_id,
                    batch_date=batch_date,
                    book_status='已完结'
                )
                if reused:
                    return False, f"已完结，从数据库复制目录"

            # 情况 2: 连载中，检查缓存时间是否晚于榜单更新时间
            if cache_status == '连载中' and last_crawl_time and book_update_time and self.chapter_service:
                # 比较缓存爬取时间与榜单更新时间
                last_crawl_dt = self._parse_time(last_crawl_time)
                book_update_dt = self._parse_time(book_update_time)

                if last_crawl_dt and book_update_dt and last_crawl_dt >= book_update_dt:
                    reused = self.chapter_service.reuse_chapters_if_unchanged(
                        book_id=book_id,
                        batch_date=batch_date
                    )
                    if reused:
                        return False, f"未更新 (缓存：{last_crawl_time}, 榜单：{book_update_time})"

        # 情况 3: 检查数据库
        if book_id and self.chapter_service:
            db_status = self.chapter_service.get_book_status(book_id)

            if db_status == '已完结':
                reused = self.chapter_service.copy_chapters_from_history_by_status(
                    book_id=book_id,
                    batch_date=batch_date,
                    book_status='已完结'
                )
                if reused:
                    # 更新缓存
                    self.cache_manager.set_book_cache(book_id, '已完结')
                    return False, f"已完结，从数据库复制目录"

        return True, "需要爬取详情页"

    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """
        解析时间字符串为 datetime 对象

        Args:
            time_str: 时间字符串，支持 "YYYY-MM-DD HH:MM" 或 "YYYY-MM-DD" 格式

        Returns:
            datetime 对象，解析失败返回 None
        """
        if not time_str:
            return None

        try:
            # 尝试带时间的格式
            if ' ' in time_str and ':' in time_str:
                return datetime.strptime(time_str, "%Y-%m-%d %H:%M")
            # 只有日期的格式
            elif ' ' in time_str:
                return datetime.strptime(time_str, "%Y-%m-%d")
            else:
                return datetime.strptime(time_str, "%Y-%m-%d")
        except ValueError:
            logger.warning(f"时间格式解析失败：{time_str}")
            return None

    def process_batch_books(self, books: List[Dict], batch_date: str) -> Dict[str, Any]:
        """
        批量处理书籍，判断哪些需要爬取

        Returns:
            包含 books_to_crawl 和各种计数的字典
        """
        skipped_count = 0
        cached_count = 0
        reused_count = 0
        books_to_crawl = []

        for book in books:
            book_id = book.get('book_id')
            book_title = book.get('book_title', '未知书名')

            needs_crawl, reason = self.check_book_needs_crawl(book, batch_date)

            if not needs_crawl:
                if '已完结' in reason or '从数据库' in reason:
                    reused_count += 1
                else:
                    skipped_count += 1
                logger.info(f"[缓存] {book_title} - {reason}")
            else:
                books_to_crawl.append(book)
                if '缓存状态' in reason:
                    cached_count += 1

        if cached_count > 0:
            logger.info(f"[缓存] 共 {cached_count} 本书命中缓存")
        if reused_count > 0:
            logger.info(f"[复用] 共 {reused_count} 本书复用历史数据")
        if skipped_count > 0:
            logger.info(f"[跳过] 跳过 {skipped_count} 本今日已爬取的书籍")

        return {
            'books_to_crawl': books_to_crawl,
            'skipped_count': skipped_count,
            'cached_count': cached_count,
            'reused_count': reused_count,
        }

    def save_book_detail(self, book_id: str, batch_date: str, book_status: str,
                         chapter_list_json: str) -> bool:
        """保存书籍详情到数据库并设置缓存"""
        if not self.fanqie_rank_dao:
            logger.error("fanqie_rank_dao 未初始化")
            return False
        success = self.fanqie_rank_dao.update_detail(
            book_id=book_id,
            batch_date=batch_date,
            book_status=book_status,
            chapter_list_json=chapter_list_json
        )
        return success

    def save_books_to_db(self, books: List[Dict], batch_date: str) -> int:
        """批量保存书籍到数据库"""
        if not self.fanqie_rank_dao:
            logger.error("fanqie_rank_dao 未初始化")
            return 0
        # 确保表已创建
        self.db_manager.init_database()
        inserted = self.fanqie_rank_dao.insert_batch(books, batch_date)
        return inserted

    def save_books_to_json(self, books: List[Dict], category: Dict, cat_idx: int) -> str:
        """保存书籍数据到 JSON 文件"""
        safe_cat_name = re.sub(r'[^\w\u4e00-\u9fff]', '_', category['cat_name'])
        output_file = f"data/books_{cat_idx:03d}_{safe_cat_name}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(books, f, ensure_ascii=False, indent=2)

        return output_file

    def get_books_without_chapters(self, batch_date: str) -> List[Dict]:
        """获取缺少章节的书籍列表"""
        if not self.chapter_service:
            logger.error("chapter_service 未初始化")
            return []
        books_need_chapters = self.chapter_service.get_books_without_chapters(batch_date)

        if not books_need_chapters:
            return []

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(books_need_chapters))
        cursor.execute(f'''
            SELECT book_id, book_title, detail_url
            FROM fanqie_ranks
            WHERE batch_date = %s AND book_id IN ({placeholders})
        ''', [batch_date] + list(books_need_chapters))

        result = []
        for row in cursor.fetchall():
            result.append({
                'book_id': row[0],
                'book_title': row[1],
                'detail_url': row[2],
            })

        conn.close()
        return result

    def reuse_chapters_from_history(self, book_id: str, batch_date: str) -> bool:
        """
        尝试从历史批次复用章节数据

        Args:
            book_id: 书籍 ID
            batch_date: 目标批次日期

        Returns:
            bool: 是否成功复用
        """
        if not self.chapter_service:
            logger.error("chapter_service 未初始化")
            return False

        # 从 Redis 缓存获取数据
        cache_data = self.cache_manager.get_book_cache(book_id)
        if not cache_data:
            logger.debug(f"书籍 {book_id} 无缓存数据，不复用")
            return False

        # 获取榜单更新时间（Phase 1 爬取榜单页时存入缓存）
        book_update_time = cache_data.get('book_update_time')
        if not book_update_time:
            # 缓存中没有榜单更新时间，说明是第一次爬取，需要爬取详情
            logger.debug(f"书籍 {book_id} 缓存中无榜单更新时间，需要爬取详情")
            return False

        # 获取上次爬取详情页的时间
        last_crawl_time = cache_data.get('last_crawl_time')
        if not last_crawl_time:
            # 没有爬取时间，需要爬取详情
            logger.debug(f"书籍 {book_id} 缓存中无爬取时间，需要爬取详情")
            return False

        # 比较时间
        last_crawl_dt = self._parse_time(last_crawl_time)
        book_update_dt = self._parse_time(book_update_time)

        if not last_crawl_dt or not book_update_dt:
            logger.debug(f"书籍 {book_id} 时间解析失败，不复用")
            return False

        if last_crawl_dt < book_update_dt:
            # 缓存时间早于榜单更新时间，说明书在榜单更新后可能又有新章节
            logger.debug(f"书籍 {book_id} 缓存时间 ({last_crawl_time}) 早于榜单更新 ({book_update_time})，不复用")
            return False
        else:
            # 缓存时间 >= 榜单更新时间，可以复用
            logger.debug(f"书籍 {book_id} 缓存时间 ({last_crawl_time}) >= 榜单更新 ({book_update_time})，可复用")

        return self.chapter_service.copy_chapters_from_history(book_id, batch_date)

    def check_category_crawled(self, category: Dict, batch_date: str, limit: int = 30) -> bool:
        """
        检查分类是否已爬取（通过检查数据库中该分类是否有数据）

        Args:
            category: 分类信息字典
            batch_date: 批次日期
            limit: 每个分类爬取的书籍数量限制（默认 30）

        Returns:
            bool: 如果该分类已爬取且数量足够则返回 True，否则返回 False
        """
        if not self.fanqie_rank_dao:
            logger.error("fanqie_rank_dao 未初始化")
            return False

        # 构建分类标识
        gender_id = category.get('gender_id', '')
        board_type = category.get('board_type', '')
        cat_name = category.get('cat_name', '')

        # 一级目录：性别 + 榜单类型（如"男频阅读榜"）
        genders = {"0": "女频", "1": "男频"}
        board_types = {"1": "新书榜", "2": "阅读榜", "3": "完本榜", "4": "热读榜"}
        board_name = f"{genders.get(gender_id, '未知')}{board_types.get(board_type, '')}"

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        # 同时检查 board_name 和 sub_category，并统计数量
        cursor.execute('''
            SELECT COUNT(*) FROM fanqie_ranks
            WHERE batch_date = %s AND board_name = %s AND sub_category = %s
        ''', (batch_date, board_name, cat_name))

        count = cursor.fetchone()[0]
        conn.close()

        # 数量达到 limit 才认为已爬取
        if count >= limit:
            return True
        elif count > 0:
            logger.debug(f"分类 '{board_name}-{cat_name}' 已爬取但数量不足 ({count}/{limit})")
            return False
        return False
