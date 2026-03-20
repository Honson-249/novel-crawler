"""
ReelShort 数据访问对象

职责：
- 纯 SQL 执行，不包含业务逻辑
- 操作 reelshort_drama（榜单明细）和 reelshort_tags（标签维表）两张表
- 返回原始数据（dict/list）
"""
import json
from typing import Any, Dict, List, Optional, Set

from loguru import logger


class ReelShortDAO:
    """ReelShort 数据访问对象"""

    def __init__(self, db_manager):
        """
        初始化 DAO

        Args:
            db_manager: DatabaseManager 实例
        """
        self.db_manager = db_manager

    # ==================== reelshort_tags 操作 ====================

    def insert_tags_batch(self, records: List[Dict[str, Any]], batch_date: str) -> int:
        """
        批量插入标签维表数据（幂等）

        唯一键：(language, tab_name, tag_name)，不含 batch_date。
        同一标签重复爬取时只更新 updated_date，不新增记录。

        Args:
            records: 标签记录列表，每项包含 {language, tab_name, tag_name}
            batch_date: 本次爬取日期（写入 first_seen_date / updated_date）

        Returns:
            实际新增的记录数（不含已存在的更新）
        """
        if not records:
            return 0

        # 应用层去重：同一批次内 (language, tab_name, tag_name) 只保留一条
        seen = set()
        unique_records = []
        for record in records:
            key = (record.get("language", ""), record.get("tab_name", ""), record.get("tag_name", ""))
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.executemany(
                """
                INSERT INTO reelshort_tags
                    (batch_date, language, tab_name, tag_name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    batch_date = VALUES(batch_date)
                """,
                [
                    (
                        batch_date,
                        r.get("language", "en"),
                        r.get("tab_name", ""),
                        r.get("tag_name", ""),
                    )
                    for r in unique_records
                ],
            )
            # rowcount：新增行计 1，ON DUPLICATE KEY UPDATE 计 2，未变化计 0
            # 用 len(unique_records) 表示本次处理数更直观
            conn.commit()
            new_count = cursor.rowcount
            logger.debug(f"[DAO] reelshort_tags 处理 {len(unique_records)} 条，rowcount={new_count}")
            return len(unique_records)

        except Exception as e:
            logger.error(f"[DAO] reelshort_tags 批量插入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_tags_by_language(
        self, batch_date: str, language: str
    ) -> Dict[str, Set[str]]:
        """
        按语言查询标签维表，返回各 Tab 的标签集合

        用途：从 DB 恢复标签参照集（用于重启后的标签交叉比对）
        维表不含 batch_date，直接按语言查全量。

        Args:
            batch_date: 保留参数（兼容调用方），实际不用于查询
            language: 语言代码

        Returns:
            {tab_name: {tag_name, ...}} 结构
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT tab_name, tag_name
                FROM reelshort_tags
                WHERE language = %s
                """,
                (language,),
            )
            rows = cursor.fetchall()

            result: Dict[str, Set[str]] = {}
            for tab_name, tag_name in rows:
                if tab_name not in result:
                    result[tab_name] = set()
                result[tab_name].add(tag_name)

            return result

        except Exception as e:
            logger.error(f"[DAO] 查询 reelshort_tags 失败：{e}")
            return {}
        finally:
            conn.close()

    def find_tags_without_zh(self, language: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        查询尚未翻译（tag_name_zh 为空）的标签记录

        Args:
            language: 语言代码，为 None 时查全部语言

        Returns:
            [{id, language, tab_name, tag_name}, ...]
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            if language:
                cursor.execute(
                    """
                    SELECT id, language, tab_name, tag_name
                    FROM reelshort_tags
                    WHERE (tag_name_zh IS NULL OR tag_name_zh = '')
                      AND language = %s
                    ORDER BY language, tab_name, id
                    """,
                    (language,),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, language, tab_name, tag_name
                    FROM reelshort_tags
                    WHERE tag_name_zh IS NULL OR tag_name_zh = ''
                    ORDER BY language, tab_name, id
                    """
                )
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"[DAO] 查询未翻译标签失败：{e}")
            return []
        finally:
            conn.close()

    def update_tag_zh_batch(self, updates: List[Dict[str, Any]]) -> int:
        """
        批量更新 reelshort_tags.tag_name_zh

        Args:
            updates: [{id, tag_name_zh}, ...]

        Returns:
            更新条数
        """
        if not updates:
            return 0
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(
                "UPDATE reelshort_tags SET tag_name_zh = %s WHERE id = %s",
                [(r["tag_name_zh"], r["id"]) for r in updates],
            )
            conn.commit()
            return len(updates)
        except Exception as e:
            logger.error(f"[DAO] 更新 tag_name_zh 失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_tag_zh_map(self, language: str) -> Dict[str, str]:
        """
        加载指定语言的标签翻译对照表

        Returns:
            {tag_name: tag_name_zh}，tag_name_zh 为空的不包含
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT tag_name, tag_name_zh
                FROM reelshort_tags
                WHERE language = %s
                  AND tag_name_zh IS NOT NULL
                  AND tag_name_zh != ''
                """,
                (language,),
            )
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"[DAO] 加载标签翻译对照表失败：{e}")
            return {}
        finally:
            conn.close()

    # ==================== reelshort_drama 操作 ====================

    def insert_drama_batch(self, records: List[Dict[str, Any]], batch_date: str) -> int:
        """
        批量插入榜单明细（幂等）

        唯一键：(batch_date, language, board_name, detail_url)
        重复时更新播放量、收藏量、标签、简介等字段。

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
                tag_list_json = self._to_json(record.get("tag_list_json"))
                actors_tags = self._to_json(record.get("actors_tags"))
                actresses_tags = self._to_json(record.get("actresses_tags"))
                identity_tags = self._to_json(record.get("identity_tags"))
                story_beat_tags = self._to_json(record.get("story_beat_tags"))
                genre_tags = self._to_json(record.get("genre_tags"))

                cursor.execute(
                    """
                    INSERT INTO reelshort_drama
                        (batch_date, language, board_name, detail_url,
                         series_title, t_book_id, play_count_raw, play_count,
                         favorite_count_raw, favorite_count,
                         tag_list_json, actors_tags, actresses_tags,
                         identity_tags, story_beat_tags, genre_tags, synopsis)
                    VALUES
                        (%s, %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s,
                         %s, %s, %s,
                         %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        series_title        = VALUES(series_title),
                        t_book_id           = COALESCE(VALUES(t_book_id), t_book_id),
                        play_count_raw      = VALUES(play_count_raw),
                        play_count          = VALUES(play_count),
                        favorite_count_raw  = VALUES(favorite_count_raw),
                        favorite_count      = VALUES(favorite_count),
                        tag_list_json       = COALESCE(VALUES(tag_list_json),   tag_list_json),
                        actors_tags         = COALESCE(VALUES(actors_tags),     actors_tags),
                        actresses_tags      = COALESCE(VALUES(actresses_tags),  actresses_tags),
                        identity_tags       = COALESCE(VALUES(identity_tags),   identity_tags),
                        story_beat_tags     = COALESCE(VALUES(story_beat_tags), story_beat_tags),
                        genre_tags          = COALESCE(VALUES(genre_tags),      genre_tags),
                        synopsis            = COALESCE(NULLIF(VALUES(synopsis), ''), synopsis),
                        updated_at          = CURRENT_TIMESTAMP
                    """,
                    (
                        record.get("batch_date", batch_date),
                        record.get("language", "en"),
                        record.get("board_name", ""),
                        record.get("detail_url", ""),
                        record.get("series_title", ""),
                        record.get("t_book_id"),
                        record.get("play_count_raw", ""),
                        record.get("play_count"),
                        record.get("favorite_count_raw", ""),
                        record.get("favorite_count"),
                        tag_list_json,
                        actors_tags,
                        actresses_tags,
                        identity_tags,
                        story_beat_tags,
                        genre_tags,
                        record.get("synopsis", ""),
                    ),
                )
                inserted += 1

            conn.commit()
            logger.debug(f"[DAO] reelshort_drama 写入 {inserted} 条")
            return inserted

        except Exception as e:
            logger.error(f"[DAO] reelshort_drama 批量插入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def find_detail_by_url(self, detail_url: str, language: str) -> Optional[Dict[str, Any]]:
        """
        按 detail_url + language 查询已存在的详情数据

        不同语言的详情页内容（简介、标签名称）不同，不可跨语言复用。
        同一语言内，同一部剧出现在多个 Tab/sub_category 下时可复用。

        Args:
            detail_url: 剧集详情页 URL
            language: 语言代码

        Returns:
            包含 {tag_list, synopsis, play_count_raw, play_count,
            favorite_count_raw, favorite_count} 的字典，未找到则返回 None
        """
        if not detail_url:
            return None

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT tag_list_json, synopsis, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       actors_tags, actresses_tags, identity_tags,
                       story_beat_tags, genre_tags
                FROM reelshort_drama
                WHERE detail_url = %s
                  AND language = %s
                  AND synopsis IS NOT NULL
                  AND synopsis != ''
                LIMIT 1
                """,
                (detail_url, language),
            )
            row = cursor.fetchone()
            if not row:
                return None

            (tag_list_json, synopsis, play_count_raw, play_count,
             favorite_count_raw, favorite_count,
             actors_tags_json, actresses_tags_json, identity_tags_json,
             story_beat_tags_json, genre_tags_json) = row

            def _parse_json_list(s):
                try:
                    return json.loads(s) if s else []
                except (json.JSONDecodeError, TypeError):
                    return []

            return {
                "tag_list": _parse_json_list(tag_list_json),
                "synopsis": synopsis or "",
                "play_count_raw": play_count_raw or "",
                "play_count": play_count,
                "favorite_count_raw": favorite_count_raw or "",
                "favorite_count": favorite_count,
                "actors_tags": _parse_json_list(actors_tags_json),
                "actresses_tags": _parse_json_list(actresses_tags_json),
                "identity_tags": _parse_json_list(identity_tags_json),
                "story_beat_tags": _parse_json_list(story_beat_tags_json),
                "genre_tags": _parse_json_list(genre_tags_json),
            }

        except Exception as e:
            logger.error(f"[DAO] 查询 detail_url 详情失败（{detail_url}）：{e}")
            return None
        finally:
            conn.close()

    def find_last_crawled_page(
        self, batch_date: str, language: str, tab_name: str, page_size: int = 20
    ) -> int:
        """
        查询当天该语言、Tab 下已爬取的最大页码，用于断点续爬

        通过统计当天该 Tab 的记录数推算已爬页数：
          已爬页数 = ceil(已有记录数 / page_size)
        重启时从 已爬页数 + 1 开始继续，跳过已完成的页。

        Args:
            batch_date: 批次日期（当天）
            language: 语言代码
            tab_name: Tab 名称（board_name）
            page_size: 每页剧集数（ReelShort 默认 20）

        Returns:
            下次应从第几页开始爬取（1 表示从头开始）
        """
        import math

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) FROM reelshort_drama
                WHERE batch_date = %s AND language = %s AND board_name = %s
                """,
                (batch_date, language, tab_name),
            )
            row = cursor.fetchone()
            count = row[0] if row else 0
            if count == 0:
                return 1
            crawled_pages = math.ceil(count / page_size)
            # 最后一页可能未爬完（中断），从最后一页重新爬以确保完整性
            return max(1, crawled_pages)
        except Exception as e:
            logger.error(f"[DAO] 查询已爬页码失败：{e}")
            return 1
        finally:
            conn.close()

    def find_dramas_without_detail(
        self, batch_date: str, language: str
    ) -> List[Dict[str, Any]]:
        """
        查询尚未补充详情的剧集记录（tag_list_json 为空）

        用途：支持两阶段爬取——先保存列表页数据，后补充详情。

        Args:
            batch_date: 批次日期
            language: 语言代码

        Returns:
            记录列表，每项包含 detail_url、series_title 等
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT id, detail_url, series_title, board_name
                FROM reelshort_drama
                WHERE batch_date = %s
                  AND language = %s
                  AND (tag_list_json IS NULL OR tag_list_json = '' OR tag_list_json = '[]')
                """,
                (batch_date, language),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"[DAO] 查询未补充详情的记录失败：{e}")
            return []
        finally:
            conn.close()

    def update_drama_detail(
        self,
        record_id: int,
        tag_list_json: Optional[str],
        actors_tags: Optional[str],
        actresses_tags: Optional[str],
        identity_tags: Optional[str],
        story_beat_tags: Optional[str],
        genre_tags: Optional[str],
        synopsis: str,
        play_count_raw: str,
        play_count: Optional[int],
        favorite_count_raw: str,
        favorite_count: Optional[int],
    ) -> bool:
        """
        更新单条剧集的详情数据

        Args:
            record_id: 记录主键 ID
            tag_list_json: 全量标签 JSON 字符串
            actors_tags: 演员标签 JSON 字符串
            actresses_tags: 女演员标签 JSON 字符串
            identity_tags: 身份标签 JSON 字符串
            story_beat_tags: 故事节拍标签 JSON 字符串
            genre_tags: 题材标签 JSON 字符串
            synopsis: 剧情简介
            play_count_raw: 播放量原始值
            play_count: 播放量数值
            favorite_count_raw: 收藏量原始值
            favorite_count: 收藏量数值

        Returns:
            是否更新成功
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE reelshort_drama
                SET tag_list_json      = %s,
                    actors_tags        = %s,
                    actresses_tags     = %s,
                    identity_tags      = %s,
                    story_beat_tags    = %s,
                    genre_tags         = %s,
                    synopsis           = %s,
                    play_count_raw     = %s,
                    play_count         = %s,
                    favorite_count_raw = %s,
                    favorite_count     = %s,
                    updated_at         = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    tag_list_json,
                    actors_tags,
                    actresses_tags,
                    identity_tags,
                    story_beat_tags,
                    genre_tags,
                    synopsis,
                    play_count_raw,
                    play_count,
                    favorite_count_raw,
                    favorite_count,
                    record_id,
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"[DAO] 更新剧集详情失败（id={record_id}）：{e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    # ==================== 标签分类后处理 ====================

    def find_dramas_for_classify(
        self, batch_date: str, language: str
    ) -> List[Dict[str, Any]]:
        """
        查询当天指定语言下所有有 tag_list_json 的剧集记录（用于后处理分类）

        Args:
            batch_date: 批次日期
            language: 语言代码

        Returns:
            记录列表，每项包含 id、tag_list_json
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, tag_list_json
                FROM reelshort_drama
                WHERE batch_date = %s
                  AND language = %s
                  AND tag_list_json IS NOT NULL
                  AND tag_list_json != ''
                  AND tag_list_json != '[]'
                """,
                (batch_date, language),
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"[DAO] 查询待分类剧集失败：{e}")
            return []
        finally:
            conn.close()

    def batch_update_tag_classify(
        self, updates: List[Dict[str, Any]]
    ) -> int:
        """
        批量更新剧集的标签分类字段

        Args:
            updates: 更新列表，每项包含
                {id, actors_tags, actresses_tags, identity_tags, story_beat_tags, genre_tags}

        Returns:
            成功更新的记录数
        """
        if not updates:
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(
                """
                UPDATE reelshort_drama
                SET actors_tags     = %s,
                    actresses_tags  = %s,
                    identity_tags   = %s,
                    story_beat_tags = %s,
                    genre_tags      = %s,
                    updated_at      = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                [
                    (
                        u.get("actors_tags"),
                        u.get("actresses_tags"),
                        u.get("identity_tags"),
                        u.get("story_beat_tags"),
                        u.get("genre_tags"),
                        u["id"],
                    )
                    for u in updates
                ],
            )
            conn.commit()
            count = cursor.rowcount
            logger.debug(f"[DAO] 标签分类更新 {count} 条")
            return count
        except Exception as e:
            logger.error(f"[DAO] 批量更新标签分类失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    # ==================== reelshort_drama_zh 操作 ====================

    def find_translated_by_url(
        self, language: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        查询某语言下已有翻译结果的 detail_url 映射（跨批次复用）

        Args:
            language: 语言代码

        Returns:
            {detail_url: {series_title, tag_list_json,
                          actors_tags, actresses_tags, identity_tags,
                          story_beat_tags, genre_tags, synopsis}} 字典
            注意：board_name 是榜单名，每条记录不同，不纳入缓存
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT detail_url, series_title,
                       tag_list_json, actors_tags, actresses_tags, identity_tags,
                       story_beat_tags, genre_tags, synopsis
                FROM reelshort_drama_zh
                WHERE language = %s AND detail_url IS NOT NULL
                ORDER BY translated_at DESC
                """,
                (language,),
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            result: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                row_dict = dict(zip(columns, row))
                url = row_dict.pop("detail_url")
                if url and url not in result:
                    result[url] = row_dict
            logger.debug(f"[DAO] 加载已翻译 URL 缓存：language={language}，共 {len(result)} 条")
            return result

        except Exception as e:
            logger.error(f"[DAO] 查询已翻译 URL 缓存失败：{e}")
            return {}
        finally:
            conn.close()

    def find_dramas_for_translate(
        self, batch_date: str, language: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        查询需要翻译的 reelshort_drama 记录（排除已存在于 reelshort_drama_zh 的记录）

        Args:
            batch_date: 批次日期
            language: 语言代码，为 None 时查询所有语言

        Returns:
            待翻译的记录列表（dict）
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            if language:
                cursor.execute(
                    """
                    SELECT d.id, d.batch_date, d.language, d.board_name,
                           d.detail_url, d.series_title, d.play_count_raw, d.play_count,
                           d.favorite_count_raw, d.favorite_count, d.tag_list_json,
                           d.actors_tags, d.actresses_tags, d.identity_tags,
                           d.story_beat_tags, d.genre_tags, d.synopsis
                    FROM reelshort_drama d
                    LEFT JOIN reelshort_drama_zh z ON z.source_id = d.id
                    WHERE d.batch_date = %s AND d.language = %s AND z.source_id IS NULL
                    ORDER BY d.id
                    """,
                    (batch_date, language),
                )
            else:
                cursor.execute(
                    """
                    SELECT d.id, d.batch_date, d.language, d.board_name,
                           d.detail_url, d.series_title, d.play_count_raw, d.play_count,
                           d.favorite_count_raw, d.favorite_count, d.tag_list_json,
                           d.actors_tags, d.actresses_tags, d.identity_tags,
                           d.story_beat_tags, d.genre_tags, d.synopsis
                    FROM reelshort_drama d
                    LEFT JOIN reelshort_drama_zh z ON z.source_id = d.id
                    WHERE d.batch_date = %s AND z.source_id IS NULL
                    ORDER BY d.id
                    """,
                    (batch_date,),
                )

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"[DAO] 查询待翻译记录失败：{e}")
            return []
        finally:
            conn.close()

    # 语言代码 → 简体中文名称映射（与 ReelShortConfig.language_names 保持一致）
    _LANGUAGE_NAMES: Dict[str, str] = {
        "en":    "英语",
        "pt":    "葡萄牙语",
        "es":    "西班牙语",
        "de":    "德语",
        "fr":    "法语",
        "ja":    "日语",
        "ko":    "韩语",
        "th":    "泰语",
        "ru":    "俄语",
        "id":    "印度尼西亚语",
        "zh-TW": "繁体中文",
        "ar":    "阿拉伯语",
        "pl":    "波兰语",
        "it":    "意大利语",
        "tr":    "土耳其语",
        "ro":    "罗马尼亚语",
        "cs":    "捷克语",
        "bg":    "保加利亚语",
        "vi":    "越南语",
    }

    def insert_drama_zh_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        批量写入翻译结果到 reelshort_drama_zh

        language 字段写入简体中文名称（如 "英语"），方便直接阅读。

        Args:
            records: 翻译结果列表，每项需包含 source_id 及所有字段

        Returns:
            成功写入的记录数
        """
        if not records:
            return 0

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            cursor.executemany(
                """
                INSERT INTO reelshort_drama_zh
                    (source_id, batch_date, language, board_name,
                     detail_url, series_title, play_count_raw, play_count,
                     favorite_count_raw, favorite_count, tag_list_json,
                     actors_tags, actresses_tags, identity_tags,
                     story_beat_tags, genre_tags, synopsis, translated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    board_name = VALUES(board_name),
                    series_title = VALUES(series_title),
                    tag_list_json = VALUES(tag_list_json),
                    actors_tags = VALUES(actors_tags),
                    actresses_tags = VALUES(actresses_tags),
                    identity_tags = VALUES(identity_tags),
                    story_beat_tags = VALUES(story_beat_tags),
                    genre_tags = VALUES(genre_tags),
                    synopsis = VALUES(synopsis),
                    translated_at = NOW(),
                    updated_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        r["source_id"],
                        r["batch_date"],
                        self._LANGUAGE_NAMES.get(r["language"], r["language"]),
                        r.get("board_name"),
                        r.get("detail_url"),
                        r.get("series_title"),
                        r.get("play_count_raw"),
                        r.get("play_count"),
                        r.get("favorite_count_raw"),
                        r.get("favorite_count"),
                        r.get("tag_list_json"),
                        r.get("actors_tags"),
                        r.get("actresses_tags"),
                        r.get("identity_tags"),
                        r.get("story_beat_tags"),
                        r.get("genre_tags"),
                        r.get("synopsis"),
                    )
                    for r in records
                ],
            )
            conn.commit()
            count = len(records)
            logger.info(f"[DAO] reelshort_drama_zh 写入 {count} 条")
            return count

        except Exception as e:
            logger.error(f"[DAO] reelshort_drama_zh 批量写入失败：{e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    # ==================== 私有工具方法 ====================

    def _to_json(self, data: Any) -> Optional[str]:
        """
        将 list/dict/str/None 统一转为 JSON 字符串

        Args:
            data: 输入数据

        Returns:
            JSON 字符串或 None
        """
        if data is None:
            return None
        if isinstance(data, str):
            return data if data.strip() else None
        if isinstance(data, (list, dict)):
            return json.dumps(data, ensure_ascii=False)
        return None


# ==================== 全局单例 ====================

_reelshort_dao: Optional[ReelShortDAO] = None


def get_reelshort_dao(db_manager) -> ReelShortDAO:
    """
    获取 ReelShortDAO 单例实例

    Args:
        db_manager: DatabaseManager 实例

    Returns:
        ReelShortDAO 实例
    """
    global _reelshort_dao
    if _reelshort_dao is None:
        _reelshort_dao = ReelShortDAO(db_manager)
    return _reelshort_dao
