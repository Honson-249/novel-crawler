"""
番茄小说榜单数据访问对象

职责:
- 纯 SQL 执行
- 不包含业务逻辑
- 返回原始数据 (dict/list)
"""

from typing import List, Dict, Any, Optional
from loguru import logger


class FanqieRankDAO:
    """番茄小说榜单 DAO"""

    def __init__(self, db_manager):
        """
        初始化 DAO

        Args:
            db_manager: DatabaseManager 实例
        """
        self.db_manager = db_manager

    def insert_batch(self, records: List[Dict[str, Any]], batch_date: str) -> int:
        """
        批量插入榜单数据

        Args:
            records: 记录列表
            batch_date: 批次日期

        Returns:
            成功插入的记录数
        """
        if not records:
            logger.warning("没有数据需要插入")
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        inserted = 0

        try:
            for record in records:
                chapter_json = self._normalize_chapter_json(record.get("chapter_list_json"))

                cursor.execute("""
                    INSERT INTO fanqie_ranks
                    (batch_date, board_name, sub_category, rank_num, book_id,
                     book_title, author_name, metric_name, metric_value_raw,
                     metric_value, tags, book_status, synopsis, chapter_list_json,
                     cover_url, detail_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    rank_num = VALUES(rank_num),
                    book_title = VALUES(book_title),
                    author_name = VALUES(author_name),
                    metric_name = VALUES(metric_name),
                    metric_value_raw = VALUES(metric_value_raw),
                    metric_value = VALUES(metric_value),
                    book_status = VALUES(book_status),
                    synopsis = VALUES(synopsis),
                    updated_at = CURRENT_TIMESTAMP
                """, (
                    batch_date,
                    record.get("board_name"),
                    record.get("sub_category"),
                    record.get("rank_num"),
                    record.get("book_id"),
                    record.get("book_title"),
                    record.get("author_name"),
                    record.get("metric_name"),
                    record.get("metric_value_raw"),
                    record.get("metric_value"),
                    record.get("tags"),
                    record.get("book_status"),
                    record.get("synopsis"),
                    chapter_json,
                    record.get("cover_url"),
                    record.get("detail_url"),
                ))
                inserted += 1

            conn.commit()
            logger.info(f"[DAO] 成功插入 {inserted}/{len(records)} 条记录")
            return inserted

        except Exception as e:
            logger.error(f"[DAO] 批量插入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def update_detail(self, book_id: str, batch_date: str, book_status: str,
                      chapter_list_json: str) -> bool:
        """
        更新书籍详情

        Args:
            book_id: 书籍 ID
            batch_date: 批次日期
            book_status: 书籍状态
            chapter_list_json: 章节列表 JSON

        Returns:
            是否更新成功
        """
        if not book_id:
            return False

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            chapter_json = self._normalize_chapter_json(chapter_list_json)

            cursor.execute("""
                UPDATE fanqie_ranks
                SET book_status = %s, chapter_list_json = %s, updated_at = CURRENT_TIMESTAMP
                WHERE book_id = %s AND batch_date = %s
            """, (book_status, chapter_json, book_id, batch_date))

            conn.commit()
            updated = cursor.rowcount > 0
            return updated

        except Exception as e:
            logger.error(f"[DAO] 更新书籍详情失败：{e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def update_detail_batch(self, updates: List[Dict[str, Any]], batch_date: str) -> int:
        """
        批量更新书籍详情

        Args:
            updates: 更新列表，每项包含 book_id, book_status, chapter_list_json
            batch_date: 批次日期

        Returns:
            成功更新的记录数
        """
        if not updates:
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        updated_count = 0

        try:
            for update in updates:
                chapter_json = self._normalize_chapter_json(update.get('chapter_list_json'))

                cursor.execute("""
                    UPDATE fanqie_ranks
                    SET book_status = %s, chapter_list_json = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE book_id = %s AND batch_date = %s
                """, (
                    update.get('book_status', '连载中'),
                    chapter_json,
                    update.get('book_id'),
                    batch_date
                ))
                updated_count += 1

            conn.commit()
            logger.info(f"[DAO] 批量更新 {updated_count} 条记录")
            return updated_count

        except Exception as e:
            logger.error(f"[DAO] 批量更新失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_books_without_chapters(self, batch_date: str) -> List[str]:
        """
        获取缺少章节数据的书籍 ID 列表

        Args:
            batch_date: 批次日期

        Returns:
            书籍 ID 列表
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT book_id FROM fanqie_ranks
                WHERE batch_date = %s AND (chapter_list_json IS NULL OR chapter_list_json = '')
            """, (batch_date,))

            rows = cursor.fetchall()
            return [row[0] for row in rows if row[0]]

        except Exception as e:
            logger.error(f"[DAO] 查询缺少章节的书籍失败：{e}")
            return []
        finally:
            conn.close()

    def find_books_with_chapters(self, batch_date: str) -> List[str]:
        """
        获取已有章节数据的书籍 ID 列表

        Args:
            batch_date: 批次日期

        Returns:
            书籍 ID 列表
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT book_id FROM fanqie_ranks
                WHERE batch_date = %s AND chapter_list_json IS NOT NULL AND chapter_list_json != ''
            """, (batch_date,))

            rows = cursor.fetchall()
            return [row[0] for row in rows if row[0]]

        except Exception as e:
            logger.error(f"[DAO] 查询已有章节的书籍失败：{e}")
            return []
        finally:
            conn.close()

    def find_latest_chapter_data(self, book_id: str) -> Optional[Dict[str, Any]]:
        """
        获取某本书的最新章节数据

        Args:
            book_id: 书籍 ID

        Returns:
            包含 chapter_list_json 和 book_status 的字典，不存在返回 None
        """
        import json

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT chapter_list_json, book_status FROM fanqie_ranks
                WHERE book_id = %s AND chapter_list_json IS NOT NULL AND chapter_list_json != ''
                ORDER BY batch_date DESC
                LIMIT 1
            """, (book_id,))

            row = cursor.fetchone()
            if not row:
                return None

            # 检查章节数据是否是有效的非空数组
            try:
                chapter_data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                # 如果是空数组或不是列表，认为无效
                if not chapter_data or not isinstance(chapter_data, list) or len(chapter_data) == 0:
                    logger.debug(f"书籍 {book_id} 历史章节数据为空或格式无效")
                    return None
            except (json.JSONDecodeError, TypeError):
                logger.debug(f"书籍 {book_id} 章节数据 JSON 解析失败")
                return None

            return {
                "chapter_list_json": row[0],
                "book_status": row[1] or '连载中'
            }

        except Exception as e:
            logger.error(f"[DAO] 查询历史章节数据失败：{e}")
            return None
        finally:
            conn.close()

    def find_book_status(self, book_id: str) -> Optional[str]:
        """
        获取书籍状态

        Args:
            book_id: 书籍 ID

        Returns:
            book_status 字符串，不存在返回 None
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT book_status FROM fanqie_ranks
                WHERE book_id = %s
                ORDER BY batch_date DESC
                LIMIT 1
            """, (book_id,))

            row = cursor.fetchone()
            return row[0] if row else None

        except Exception as e:
            logger.error(f"[DAO] 获取书籍状态失败：{e}")
            return None
        finally:
            conn.close()

    def find_book_by_id(self, book_id: str, batch_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        根据 book_id 查询书籍信息

        Args:
            book_id: 书籍 ID
            batch_date: 可选的批次日期，不传则返回最新一条

        Returns:
            书籍记录字典，不存在返回 None
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            if batch_date:
                cursor.execute("""
                    SELECT * FROM fanqie_ranks
                    WHERE book_id = %s AND batch_date = %s
                    LIMIT 1
                """, (book_id, batch_date))
            else:
                cursor.execute("""
                    SELECT * FROM fanqie_ranks
                    WHERE book_id = %s
                    ORDER BY batch_date DESC
                    LIMIT 1
                """, (book_id,))

            row = cursor.fetchone()
            if not row:
                return None

            # 转换为字典（根据列名）
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))

        except Exception as e:
            logger.error(f"[DAO] 查询书籍失败：{e}")
            return None
        finally:
            conn.close()

    def _normalize_chapter_json(self, data: Any) -> Optional[str]:
        """
        标准化章节 JSON 为字符串

        Args:
            data: 可能是 list、dict 或 str

        Returns:
            JSON 字符串或 None
        """
        import json

        if not data:
            return None

        if isinstance(data, str):
            return data if data.strip() else None

        if isinstance(data, (list, dict)):
            return json.dumps(data, ensure_ascii=False)

        return None


# 全局 DAO 实例
_fanqie_rank_dao: Optional[FanqieRankDAO] = None


def get_fanqie_rank_dao(db_manager) -> FanqieRankDAO:
    """
    获取 FanqieRankDAO 实例

    Args:
        db_manager: DatabaseManager 实例

    Returns:
        FanqieRankDAO 实例
    """
    global _fanqie_rank_dao
    if _fanqie_rank_dao is None:
        _fanqie_rank_dao = FanqieRankDAO(db_manager)
    return _fanqie_rank_dao
