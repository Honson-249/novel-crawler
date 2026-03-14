"""
章节服务 - 章节数据业务逻辑

职责:
- 章节复用策略
- 章节数据查询
- 不包含 SQL 执行
"""

from typing import Optional, List, Dict, Any
from loguru import logger

from src.novel_crawler.dao import FanqieRankDAO
from src.novel_crawler.config.database import DatabaseManager


class ChapterService:
    """章节服务"""

    def __init__(self, db_manager: DatabaseManager = None):
        """
        初始化章节服务

        Args:
            db_manager: DatabaseManager 实例
        """
        self.db_manager = db_manager or DatabaseManager()
        self.dao = FanqieRankDAO(self.db_manager)

    def copy_chapters_from_history(self, book_id: str, batch_date: str) -> bool:
        """
        从历史批次复制章节数据到当前批次

        Args:
            book_id: 书籍 ID
            batch_date: 目标批次日期

        Returns:
            是否成功复制
        """
        import json

        # DAO 层获取历史数据
        chapter_data = self.dao.find_latest_chapter_data(book_id)
        if not chapter_data:
            logger.debug(f"书籍 {book_id} 无历史章节数据")
            return False

        # 再次检查章节数据有效性
        try:
            chapter_list = json.loads(chapter_data["chapter_list_json"]) if isinstance(chapter_data["chapter_list_json"], str) else chapter_data["chapter_list_json"]
            if not chapter_list or not isinstance(chapter_list, list) or len(chapter_list) == 0:
                logger.debug(f"书籍 {book_id} 历史章节数据为空数组")
                return False
            logger.debug(f"书籍 {book_id} 历史章节数：{len(chapter_list)}, 状态：{chapter_data['book_status']}")
        except (json.JSONDecodeError, TypeError) as e:
            logger.debug(f"书籍 {book_id} 章节数据 JSON 解析失败：{e}")
            return False

        # DAO 层更新当前批次
        result = self.dao.update_detail(
            book_id=book_id,
            batch_date=batch_date,
            book_status=chapter_data["book_status"],
            chapter_list_json=chapter_data["chapter_list_json"]
        )
        if result:
            logger.info(f"书籍 {book_id} 成功从历史复制 {len(chapter_list)} 个章节")
        return result

    def reuse_chapters_if_unchanged(self, book_id: str, batch_date: str) -> bool:
        """
        复用历史章节数据（用于榜单更新时间未变化的情况）

        Args:
            book_id: 书籍 ID
            batch_date: 目标批次日期

        Returns:
            是否成功复用
        """
        chapter_data = self.dao.find_latest_chapter_data(book_id)
        if not chapter_data:
            return False

        logger.debug(f"复用历史章节数据：{book_id}")
        return self.dao.update_detail(
            book_id=book_id,
            batch_date=batch_date,
            book_status=chapter_data["book_status"],
            chapter_list_json=chapter_data["chapter_list_json"]
        )

    def copy_chapters_from_history_by_status(self, book_id: str, batch_date: str,
                                              book_status: str) -> bool:
        """
        根据书籍状态从历史批次复制章节数据

        Args:
            book_id: 书籍 ID
            batch_date: 目标批次日期
            book_status: 书籍状态

        Returns:
            是否成功复制
        """
        chapter_data = self.dao.find_latest_chapter_data(book_id)
        if not chapter_data:
            return False

        # 优先使用历史数据中的状态
        status = chapter_data["book_status"] or book_status

        logger.debug(f"从历史复制章节数据：{book_id}, status={status}")
        return self.dao.update_detail(
            book_id=book_id,
            batch_date=batch_date,
            book_status=status,
            chapter_list_json=chapter_data["chapter_list_json"]
        )

    def get_books_without_chapters(self, batch_date: str) -> List[str]:
        """
        获取缺少章节的书籍 ID 列表

        Args:
            batch_date: 批次日期

        Returns:
            书籍 ID 列表
        """
        return self.dao.find_books_without_chapters(batch_date)

    def get_books_with_chapters(self, batch_date: str) -> List[str]:
        """
        获取已有章节的书籍 ID 列表

        Args:
            batch_date: 批次日期

        Returns:
            书籍 ID 列表
        """
        return self.dao.find_books_with_chapters(batch_date)

    def get_book_status(self, book_id: str) -> Optional[str]:
        """
        获取书籍状态

        Args:
            book_id: 书籍 ID

        Returns:
            书籍状态
        """
        return self.dao.find_book_status(book_id)

    def get_book_detail(self, book_id: str, batch_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        获取书籍详情

        Args:
            book_id: 书籍 ID
            batch_date: 可选的批次日期

        Returns:
            书籍详情字典
        """
        return self.dao.find_book_by_id(book_id, batch_date)


# 全局章节服务实例
_chapter_service: Optional[ChapterService] = None


def get_chapter_service(db_manager: DatabaseManager = None) -> ChapterService:
    """
    获取章节服务实例

    Args:
        db_manager: DatabaseManager 实例

    Returns:
        ChapterService 实例
    """
    global _chapter_service
    if _chapter_service is None:
        _chapter_service = ChapterService(db_manager)
    return _chapter_service
