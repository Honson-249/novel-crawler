"""
DramaShorts 数据访问对象

职责：
- 纯 SQL 执行，不包含业务逻辑
- 操作 dramashort_drama（榜单明细）表
- 返回原始数据（dict/list）
"""
from typing import Any, Dict, List, Optional

from loguru import logger


class DramaShortDAO:
    """DramaShorts 数据访问对象"""

    def __init__(self, db_manager):
        """
        初始化 DAO

        Args:
            db_manager: DatabaseManager 实例
        """
        self.db_manager = db_manager

    # ==================== dramashort_drama 操作 ====================

    def insert_batch(self, records: List[Dict[str, Any]], batch_date: str) -> int:
        """
        批量插入榜单明细（幂等）

        唯一键：(batch_date, language, board_name, detail_url)
        重复时更新播放量、收藏量、简介等字段。

        Args:
            records: 剧集记录列表
            batch_date: 批次日期

        Returns:
            成功写入的记录数
        """
        if not records:
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        inserted = 0

        try:
            for record in records:
                cursor.execute(
                    """
                    INSERT INTO dramashort_drama
                        (batch_date, language, board_name, board_order, detail_url,
                         series_title, play_count_raw, play_count,
                         favorite_count_raw, favorite_count,
                         likes_count_raw, likes_count,
                         episodes_count, score, synopsis)
                    VALUES
                        (%s, %s, %s, %s, %s,
                         %s, %s, %s,
                         %s, %s,
                         %s, %s,
                         %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        board_order         = VALUES(board_order),
                        series_title        = VALUES(series_title),
                        play_count_raw      = VALUES(play_count_raw),
                        play_count          = VALUES(play_count),
                        favorite_count_raw  = VALUES(favorite_count_raw),
                        favorite_count      = VALUES(favorite_count),
                        likes_count_raw     = VALUES(likes_count_raw),
                        likes_count         = VALUES(likes_count),
                        episodes_count      = VALUES(episodes_count),
                        score               = VALUES(score),
                        synopsis            = VALUES(synopsis),
                        updated_at          = CURRENT_TIMESTAMP
                    """,
                    (
                        record.get("batch_date", batch_date),
                        record.get("language", "en"),
                        record.get("board_name", ""),
                        record.get("board_order"),
                        record.get("detail_url", ""),
                        record.get("series_title", ""),
                        record.get("play_count_raw", ""),
                        record.get("play_count"),
                        record.get("favorite_count_raw", ""),
                        record.get("favorite_count"),
                        record.get("likes_count_raw", ""),
                        record.get("likes_count"),
                        record.get("episodes_count"),
                        record.get("score"),
                        record.get("synopsis", ""),
                    ),
                )
                inserted += 1

            conn.commit()
            logger.debug(f"[DAO] dramashort_drama 写入 {inserted} 条")
            return inserted

        except Exception as e:
            logger.error(f"[DAO] dramashort_drama 批量插入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_by_url(self, detail_url: str) -> Optional[Dict[str, Any]]:
        """
        按 detail_url 查询已存在的记录（DB 缓存兜底）

        synopsis 非空则视为有效详情，可直接复用，无需重新爬取。

        Args:
            detail_url: 剧集详情页 URL

        Returns:
            包含 synopsis 的字典，未找到则返回 None
        """
        if not detail_url:
            return None

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT synopsis, play_count_raw, play_count,
                       favorite_count_raw, favorite_count
                FROM dramashort_drama
                WHERE detail_url = %s
                  AND synopsis IS NOT NULL
                  AND synopsis != ''
                LIMIT 1
                """,
                (detail_url,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            synopsis, play_count_raw, play_count, favorite_count_raw, favorite_count = row
            return {
                "synopsis": synopsis or "",
                "play_count_raw": play_count_raw or "",
                "play_count": play_count,
                "favorite_count_raw": favorite_count_raw or "",
                "favorite_count": favorite_count,
            }

        except Exception as e:
            logger.error(f"[DAO] 查询 detail_url 详情失败（{detail_url}）：{e}")
            return None
        finally:
            conn.close()

    def find_without_synopsis(self, batch_date: str, language: str) -> List[Dict[str, Any]]:
        """
        查询尚未补充简介的剧集记录（synopsis 为空）

        用途：支持两阶段爬取——先保存列表页数据，后补充详情。

        Args:
            batch_date: 批次日期
            language: 语言代码

        Returns:
            记录列表，每项包含 id、detail_url、series_title 等
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT id, detail_url, series_title, board_name, board_order
                FROM dramashort_drama
                WHERE batch_date = %s
                  AND language = %s
                  AND (synopsis IS NULL OR synopsis = '')
                """,
                (batch_date, language),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"[DAO] 查询未补充简介的记录失败：{e}")
            return []
        finally:
            conn.close()

    def update_synopsis(self, record_id: int, synopsis: str) -> bool:
        """
        更新单条剧集的简介数据

        Args:
            record_id: 记录主键 ID
            synopsis: 剧情简介

        Returns:
            是否更新成功
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE dramashort_drama
                SET synopsis   = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (synopsis, record_id),
            )
            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"[DAO] 更新剧集简介失败（id={record_id}）：{e}")
            conn.rollback()
            return False
        finally:
            conn.close()


    # ==================== dramashort_drama_zh 操作 ====================

    def find_dramas_for_translate(
        self, batch_date: str, language: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        查询待翻译记录（在 dramashort_drama 中但不在 dramashort_drama_zh 中）

        Args:
            batch_date: 批次日期
            language: 语言代码，None 表示全部语言

        Returns:
            待翻译记录列表
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            if language:
                cursor.execute(
                    """
                    SELECT d.id, d.batch_date, d.language, d.board_name, d.board_order,
                           d.detail_url, d.series_title,
                           d.play_count_raw, d.play_count,
                           d.favorite_count_raw, d.favorite_count,
                           d.likes_count_raw, d.likes_count,
                           d.episodes_count, d.score, d.synopsis
                    FROM dramashort_drama d
                    LEFT JOIN dramashort_drama_zh z ON z.source_id = d.id
                    WHERE d.batch_date = %s
                      AND d.language = %s
                      AND z.source_id IS NULL
                    ORDER BY d.id
                    """,
                    (batch_date, language),
                )
            else:
                cursor.execute(
                    """
                    SELECT d.id, d.batch_date, d.language, d.board_name, d.board_order,
                           d.detail_url, d.series_title,
                           d.play_count_raw, d.play_count,
                           d.favorite_count_raw, d.favorite_count,
                           d.likes_count_raw, d.likes_count,
                           d.episodes_count, d.score, d.synopsis
                    FROM dramashort_drama d
                    LEFT JOIN dramashort_drama_zh z ON z.source_id = d.id
                    WHERE d.batch_date = %s
                      AND z.source_id IS NULL
                    ORDER BY d.language, d.id
                    """,
                    (batch_date,),
                )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"[DAO] 查询待翻译记录失败：{e}")
            return []
        finally:
            conn.close()

    def insert_drama_zh_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        批量写入翻译结果到 dramashort_drama_zh（幂等）

        Args:
            records: 翻译后的记录列表，每项必须含 source_id

        Returns:
            成功写入的记录数
        """
        if not records:
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            params = [
                (
                    r["source_id"],
                    r.get("batch_date", ""),
                    r.get("language", ""),
                    r.get("board_name", ""),
                    r.get("board_order"),
                    r.get("detail_url", ""),
                    r.get("series_title", ""),
                    r.get("play_count_raw", ""),
                    r.get("play_count"),
                    r.get("favorite_count_raw", ""),
                    r.get("favorite_count"),
                    r.get("likes_count_raw", ""),
                    r.get("likes_count"),
                    r.get("episodes_count"),
                    r.get("score"),
                    r.get("synopsis", ""),
                )
                for r in records
            ]
            cursor.executemany(
                """
                INSERT INTO dramashort_drama_zh
                    (source_id, batch_date, language, board_name, board_order,
                     detail_url, series_title,
                     play_count_raw, play_count,
                     favorite_count_raw, favorite_count,
                     likes_count_raw, likes_count,
                     episodes_count, score, synopsis,
                     translated_at)
                VALUES
                    (%s, %s, %s, %s, %s,
                     %s, %s,
                     %s, %s,
                     %s, %s,
                     %s, %s,
                     %s, %s, %s,
                     CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    series_title        = VALUES(series_title),
                    synopsis            = VALUES(synopsis),
                    board_name          = VALUES(board_name),
                    translated_at       = CURRENT_TIMESTAMP,
                    updated_at          = CURRENT_TIMESTAMP
                """,
                params,
            )
            conn.commit()
            written = len(records)
            logger.debug(f"[DAO] dramashort_drama_zh 写入 {written} 条")
            return written
        except Exception as e:
            logger.error(f"[DAO] dramashort_drama_zh 批量写入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_translated_by_url(self, language: str) -> dict:
        """
        查询指定语言已翻译记录，按 detail_url 索引（用于跨批次复用）

        Args:
            language: 语言代码

        Returns:
            {detail_url: {series_title, synopsis, board_name}} 字典
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT detail_url, series_title, synopsis
                FROM dramashort_drama_zh
                WHERE language = %s
                  AND detail_url IS NOT NULL
                  AND detail_url != ''
                """,
                (language,),
            )
            rows = cursor.fetchall()
            return {
                row[0]: {
                    "series_title": row[1],
                    "synopsis": row[2],
                }
                for row in rows
                if row[0]
            }
        except Exception as e:
            logger.error(f"[DAO] 查询已翻译 URL 缓存失败：{e}")
            return {}
        finally:
            conn.close()


# ==================== 全局单例 ====================

_dramashort_dao: Optional[DramaShortDAO] = None


def get_dramashort_dao(db_manager) -> DramaShortDAO:
    """
    获取 DramaShortDAO 单例实例

    Args:
        db_manager: DatabaseManager 实例

    Returns:
        DramaShortDAO 实例
    """
    global _dramashort_dao
    if _dramashort_dao is None:
        _dramashort_dao = DramaShortDAO(db_manager)
    return _dramashort_dao
