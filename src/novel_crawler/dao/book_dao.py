"""
书籍数据访问对象

职责:
- 纯 SQL 执行
- 不包含业务逻辑
- 返回原始数据 (dict/list)
"""

from typing import List, Dict, Any, Optional
from loguru import logger


class BookDAO:
    """书籍 DAO"""

    def __init__(self, db_manager):
        """
        初始化 DAO

        Args:
            db_manager: DatabaseManager 实例
        """
        self.db_manager = db_manager

    def find_book_list(
        self,
        page: int = 1,
        page_size: int = 20,
        board_name: Optional[str] = None,
        sub_category: Optional[str] = None,
        book_title: Optional[str] = None,
        batch_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分页查询书籍列表

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
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            # 构建查询条件
            conditions = []
            params = []

            if board_name:
                conditions.append("board_name = %s")
                params.append(board_name)
            if sub_category:
                conditions.append("sub_category = %s")
                params.append(sub_category)
            if book_title:
                conditions.append("book_title LIKE %s")
                params.append(f"%{book_title}%")
            if batch_date:
                conditions.append("batch_date = %s")
                params.append(batch_date)

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # 查询总数
            count_sql = f"SELECT COUNT(*) FROM fanqie_ranks WHERE {where_clause}"
            cursor.execute(count_sql, params)
            total = cursor.fetchone()[0]

            # 分页查询
            offset = (page - 1) * page_size
            query_sql = f"""
                SELECT id, batch_date, board_name, sub_category, rank_num,
                       book_id, book_title, author_name, metric_name,
                       metric_value_raw, metric_value, book_status,
                       synopsis, cover_url, detail_url
                FROM fanqie_ranks
                WHERE {where_clause}
                ORDER BY batch_date DESC, board_name, rank_num
                LIMIT %s OFFSET %s
            """
            cursor.execute(query_sql, params + [page_size, offset])
            rows = cursor.fetchall()

            # 转换为字典列表
            books = []
            for row in rows:
                books.append({
                    "id": row[0],
                    "batch_date": str(row[1]),
                    "board_name": row[2],
                    "sub_category": row[3],
                    "rank_num": row[4],
                    "book_id": row[5],
                    "book_title": row[6],
                    "author_name": row[7],
                    "metric_name": row[8],
                    "metric_value_raw": row[9],
                    "metric_value": row[10],
                    "book_status": row[11],
                    "synopsis": row[12],
                    "cover_url": row[13],
                    "detail_url": row[14],
                })

            return {
                "total": total,
                "page": page,
                "page_size": page_size,
                "data": books,
            }

        finally:
            conn.close()

    def find_book_detail(self, book_id: str) -> Optional[Dict[str, Any]]:
        """
        获取书籍详情（最新批次）

        Args:
            book_id: 书籍 ID

        Returns:
            书籍详情，不存在返回 None
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT id, batch_date, board_name, sub_category, rank_num,
                       book_id, book_title, author_name, metric_name,
                       metric_value_raw, metric_value, book_status,
                       synopsis, chapter_list_json, cover_url, detail_url
                FROM fanqie_ranks
                WHERE book_id = %s
                ORDER BY batch_date DESC
                LIMIT 1
            """, (book_id,))

            row = cursor.fetchone()
            if not row:
                return None

            # 转换为字典
            columns = [desc[0] for desc in cursor.description]
            book_dict = dict(zip(columns, row))

            # 解析章节列表 JSON
            import json
            chapter_list = None
            if row[13]:  # chapter_list_json 索引
                try:
                    chapter_list = json.loads(row[13])
                except (json.JSONDecodeError, TypeError):
                    chapter_list = row[13]

            book_dict["chapter_list"] = chapter_list

            # 将 date 对象转换为字符串
            if book_dict.get("batch_date"):
                book_dict["batch_date"] = str(book_dict["batch_date"])

            return book_dict

        finally:
            conn.close()

    def count_by_category(self) -> List[Dict[str, Any]]:
        """
        获取分类统计

        Returns:
            分类统计列表
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT sub_category,
                       COUNT(*) as book_count,
                       MAX(batch_date) as latest_batch_date
                FROM fanqie_ranks
                GROUP BY sub_category
                ORDER BY book_count DESC
            """)

            rows = cursor.fetchall()
            return [
                {
                    "sub_category": row[0],
                    "book_count": row[1],
                    "latest_batch_date": str(row[2]),
                }
                for row in rows
            ]

        finally:
            conn.close()

    def count_summary(self) -> Dict[str, Any]:
        """
        获取汇总统计

        Returns:
            汇总统计字典
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            # 总书籍数
            cursor.execute("SELECT COUNT(DISTINCT book_id) FROM fanqie_ranks")
            total_books = cursor.fetchone()[0]

            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM fanqie_ranks")
            total_records = cursor.fetchone()[0]

            # 最新批次
            cursor.execute("SELECT MAX(batch_date) FROM fanqie_ranks")
            latest_batch = cursor.fetchone()[0]

            # 分类数量
            cursor.execute("SELECT COUNT(DISTINCT sub_category) FROM fanqie_ranks")
            category_count = cursor.fetchone()[0]

            return {
                "total_books": total_books,
                "total_records": total_records,
                "latest_batch_date": str(latest_batch) if latest_batch else None,
                "category_count": category_count,
            }

        finally:
            conn.close()


# 全局 BookDAO 实例
_book_dao: Optional[BookDAO] = None


def get_book_dao(db_manager) -> BookDAO:
    """
    获取 BookDAO 实例

    Args:
        db_manager: DatabaseManager 实例

    Returns:
        BookDAO 实例
    """
    global _book_dao
    if _book_dao is None:
        _book_dao = BookDAO(db_manager)
    return _book_dao
