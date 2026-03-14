"""
书籍服务 - 负责书籍数据查询和统计

重构后：
- 使用 DAO 层执行 SQL
- Service 层专注于业务逻辑和数据转换
"""

from typing import Optional, List, Dict, Any
from src.novel_crawler.config.database import DatabaseManager
from src.novel_crawler.dao import BookDAO, get_book_dao


class BookService:
    """书籍服务"""

    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        self._book_dao: Optional[BookDAO] = None

    @property
    def book_dao(self) -> BookDAO:
        """懒加载 BookDAO"""
        if self._book_dao is None:
            self._book_dao = get_book_dao(self.db_manager)
        return self._book_dao

    def get_book_list(
        self,
        page: int = 1,
        page_size: int = 20,
        board_name: Optional[str] = None,
        sub_category: Optional[str] = None,
        book_title: Optional[str] = None,
        batch_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取书籍列表

        Args:
            page: 页码
            page_size: 每页数量
            board_name: 榜单名称筛选
            sub_category: 分类名称筛选
            book_title: 书名搜索
            batch_date: 批次日期筛选

        Returns:
            包含 total 和 data 的字典
        """
        return self.book_dao.find_book_list(
            page=page,
            page_size=page_size,
            board_name=board_name,
            sub_category=sub_category,
            book_title=book_title,
            batch_date=batch_date
        )

    def get_book_detail(self, book_id: str) -> Optional[Dict[str, Any]]:
        """
        获取书籍详情

        Args:
            book_id: 书籍 ID

        Returns:
            书籍详情，不存在返回 None
        """
        return self.book_dao.find_book_detail(book_id)

    def get_category_stats(self) -> List[Dict[str, Any]]:
        """获取分类统计"""
        return self.book_dao.count_by_category()

    def get_summary_stats(self) -> Dict[str, Any]:
        """获取汇总统计"""
        return self.book_dao.count_summary()


# 全局书籍服务实例
_book_service: Optional[BookService] = None


def get_book_service(db_manager: DatabaseManager = None) -> BookService:
    """获取书籍服务实例"""
    global _book_service
    if _book_service is None:
        _book_service = BookService(db_manager)
    return _book_service
