#!/usr/bin/env python3
"""
短剧中文翻译数据导出脚本

支持导出：
- reelshort_drama_zh（ReelShort 中文翻译表）
- dramashort_drama_zh（DramaShorts 中文翻译表）

导出格式：CSV（按语言分文件）
"""
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from src.novel_crawler.config.database import db_manager

# 导出目录
EXPORT_DIR = Path(__file__).resolve().parent.parent / "exported_drama_zh"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# 语言代码 → 文件名后缀映射
LANGUAGE_FILENAMES = {
    "英语": "en",
    "葡萄牙语": "pt",
    "西班牙语": "es",
    "德语": "de",
    "法语": "fr",
    "日语": "ja",
    "韩语": "ko",
    "泰语": "th",
    "俄语": "ru",
    "印度尼西亚语": "id",
    "繁体中文": "zh-tw",
    "阿拉伯语": "ar",
    "波兰语": "pl",
    "意大利语": "it",
    "土耳其语": "tr",
    "罗马尼亚语": "ro",
    "捷克语": "cs",
    "保加利亚语": "bg",
    "越南语": "vi",
    "en": "en",
    "pt": "pt",
    "es": "es",
    "de": "de",
    "fr": "fr",
    "ja": "ja",
    "ko": "ko",
    "th": "th",
    "ru": "ru",
    "id": "id",
    "zh-TW": "zh-tw",
    "zh-tw": "zh-tw",
    "ar": "ar",
    "pl": "pl",
    "it": "it",
    "tr": "tr",
    "ro": "ro",
    "cs": "cs",
    "bg": "bg",
    "vi": "vi",
}

# reelshort_drama_zh 导出字段
REELSHORT_ZH_FIELDS = [
    "id", "batch_date", "language", "board_name", "board_order",
    "detail_url", "series_title", "play_count_raw", "play_count",
    "favorite_count_raw", "favorite_count", "tag_list",
    "actors_tags", "actresses_tags", "identity_tags",
    "story_beat_tags", "genre_tags", "synopsis",
]

# reelshort_drama 原文导出字段
REELSHORT_RAW_FIELDS = [
    "id", "batch_date", "language", "board_name", "detail_url",
    "series_title", "t_book_id", "play_count_raw", "play_count",
    "favorite_count_raw", "favorite_count",
    "tag_list_json", "actors_tags", "actresses_tags",
    "identity_tags", "story_beat_tags", "genre_tags", "synopsis",
]

# dramashort_drama_zh 导出字段
DRAMASHORT_ZH_FIELDS = [
    "id", "batch_date", "language", "board_name", "board_order",
    "detail_url", "series_title", "play_count_raw", "play_count",
    "favorite_count_raw", "favorite_count", "likes_count_raw", "likes_count",
    "episodes_count", "score", "synopsis",
]

# dramashort_drama 原文导出字段
DRAMASHORT_RAW_FIELDS = [
    "id", "batch_date", "language", "board_name", "board_order",
    "detail_url", "series_title", "play_count_raw", "play_count",
    "favorite_count_raw", "favorite_count", "likes_count_raw", "likes_count",
    "episodes_count", "score", "synopsis",
]


def get_utc8_today() -> str:
    """获取 UTC+8 时区的今天日期"""
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _parse_json_list(json_str: Optional[str]) -> str:
    """解析 JSON 列表为逗号分隔字符串"""
    if not json_str:
        return ""
    try:
        lst = json.loads(json_str)
        if isinstance(lst, list):
            return ", ".join(str(item) for item in lst)
    except (json.JSONDecodeError, TypeError):
        pass
    return str(json_str)


def export_reelshort(
    batch_date: str,
    language: Optional[str] = None,
) -> Dict[str, int]:
    """
    导出 ReelShort 中文翻译数据

    Args:
        batch_date: 批次日期 YYYY-MM-DD
        language: 语言名称（如"英语"），None 表示全部

    Returns:
        {language: count} 统计字典
    """
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        if language:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count, tag_list_json,
                       actors_tags, actresses_tags, identity_tags,
                       story_beat_tags, genre_tags, synopsis
                FROM reelshort_drama_zh
                WHERE batch_date = %s AND language = %s
                ORDER BY board_name, id
                """,
                (batch_date, language),
            )
        else:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count, tag_list_json,
                       actors_tags, actresses_tags, identity_tags,
                       story_beat_tags, genre_tags, synopsis
                FROM reelshort_drama_zh
                WHERE batch_date = %s
                ORDER BY language, board_name, id
                """,
                (batch_date,),
            )

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        # 按语言分组导出
        by_language: Dict[str, List[Dict]] = {}
        for rec in records:
            lang = rec.get("language", "unknown")
            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(rec)

        stats = {}
        for lang, lang_records in by_language.items():
            filename_slug = LANGUAGE_FILENAMES.get(lang, lang.lower().replace(" ", "_"))
            csv_path = EXPORT_DIR / f"reelshort_zh_{batch_date}_{filename_slug}.csv"

            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=REELSHORT_ZH_FIELDS, extrasaction="ignore")
                writer.writeheader()
                for rec in lang_records:
                    row = dict(rec)
                    row["tag_list"] = _parse_json_list(rec.get("tag_list_json"))
                    row["actors_tags"] = _parse_json_list(rec.get("actors_tags"))
                    row["actresses_tags"] = _parse_json_list(rec.get("actresses_tags"))
                    row["identity_tags"] = _parse_json_list(rec.get("identity_tags"))
                    row["story_beat_tags"] = _parse_json_list(rec.get("story_beat_tags"))
                    row["genre_tags"] = _parse_json_list(rec.get("genre_tags"))
                    writer.writerow(row)

            stats[lang] = len(lang_records)
            logger.info(f"导出 ReelShort [{lang}] {len(lang_records)} 条 → {csv_path}")

        return stats

    except Exception as e:
        logger.error(f"导出 ReelShort 数据失败：{e}")
        return {}
    finally:
        conn.close()


def export_reelshort_raw(
    batch_date: str,
    language: Optional[str] = None,
) -> Dict[str, int]:
    """
    导出 ReelShort 原文数据

    Args:
        batch_date: 批次日期 YYYY-MM-DD
        language: 语言代码（如"en"），None 表示全部

    Returns:
        {language: count} 统计字典
    """
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        if language:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, detail_url,
                       series_title, t_book_id, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       tag_list_json, actors_tags, actresses_tags,
                       identity_tags, story_beat_tags, genre_tags, synopsis
                FROM reelshort_drama
                WHERE batch_date = %s AND language = %s
                ORDER BY board_name, id
                """,
                (batch_date, language),
            )
        else:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, detail_url,
                       series_title, t_book_id, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       tag_list_json, actors_tags, actresses_tags,
                       identity_tags, story_beat_tags, genre_tags, synopsis
                FROM reelshort_drama
                WHERE batch_date = %s
                ORDER BY language, board_name, id
                """,
                (batch_date,),
            )

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        # 按语言分组导出
        by_language: Dict[str, List[Dict]] = {}
        for rec in records:
            lang = rec.get("language", "unknown")
            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(rec)

        stats = {}
        for lang, lang_records in by_language.items():
            filename_slug = LANGUAGE_FILENAMES.get(lang, lang.lower().replace(" ", "_"))
            csv_path = EXPORT_DIR / f"reelshort_raw_{batch_date}_{filename_slug}.csv"

            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=REELSHORT_RAW_FIELDS, extrasaction="ignore")
                writer.writeheader()
                for rec in lang_records:
                    writer.writerow(rec)

            stats[lang] = len(lang_records)
            logger.info(f"导出 ReelShort 原文 [{lang}] {len(lang_records)} 条 → {csv_path}")

        return stats

    except Exception as e:
        logger.error(f"导出 ReelShort 原文数据失败：{e}")
        return {}
    finally:
        conn.close()


def export_dramashort(
    batch_date: str,
    language: Optional[str] = None,
) -> Dict[str, int]:
    """
    导出 DramaShorts 中文翻译数据

    Args:
        batch_date: 批次日期 YYYY-MM-DD
        language: 语言名称（如"英语"），None 表示全部

    Returns:
        {language: count} 统计字典
    """
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        if language:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       likes_count_raw, likes_count,
                       episodes_count, score, synopsis
                FROM dramashort_drama_zh
                WHERE batch_date = %s AND language = %s
                ORDER BY board_name, id
                """,
                (batch_date, language),
            )
        else:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       likes_count_raw, likes_count,
                       episodes_count, score, synopsis
                FROM dramashort_drama_zh
                WHERE batch_date = %s
                ORDER BY language, board_name, id
                """,
                (batch_date,),
            )

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        # 按语言分组导出
        by_language: Dict[str, List[Dict]] = {}
        for rec in records:
            lang = rec.get("language", "unknown")
            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(rec)

        stats = {}
        for lang, lang_records in by_language.items():
            filename_slug = LANGUAGE_FILENAMES.get(lang, lang.lower().replace(" ", "_"))
            csv_path = EXPORT_DIR / f"dramashort_zh_{batch_date}_{filename_slug}.csv"

            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=DRAMASHORT_ZH_FIELDS, extrasaction="ignore")
                writer.writeheader()
                for rec in lang_records:
                    writer.writerow(rec)

            stats[lang] = len(lang_records)
            logger.info(f"导出 DramaShort [{lang}] {len(lang_records)} 条 → {csv_path}")

        return stats

    except Exception as e:
        logger.error(f"导出 DramaShort 数据失败：{e}")
        return {}
    finally:
        conn.close()


def export_dramashort_raw(
    batch_date: str,
    language: Optional[str] = None,
) -> Dict[str, int]:
    """
    导出 DramaShorts 原文数据

    Args:
        batch_date: 批次日期 YYYY-MM-DD
        language: 语言代码（如"en"），None 表示全部

    Returns:
        {language: count} 统计字典
    """
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        if language:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       likes_count_raw, likes_count,
                       episodes_count, score, synopsis
                FROM dramashort_drama
                WHERE batch_date = %s AND language = %s
                ORDER BY board_name, id
                """,
                (batch_date, language),
            )
        else:
            cursor.execute(
                """
                SELECT id, batch_date, language, board_name, board_order,
                       detail_url, series_title, play_count_raw, play_count,
                       favorite_count_raw, favorite_count,
                       likes_count_raw, likes_count,
                       episodes_count, score, synopsis
                FROM dramashort_drama
                WHERE batch_date = %s
                ORDER BY language, board_name, id
                """,
                (batch_date,),
            )

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        # 按语言分组导出
        by_language: Dict[str, List[Dict]] = {}
        for rec in records:
            lang = rec.get("language", "unknown")
            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(rec)

        stats = {}
        for lang, lang_records in by_language.items():
            filename_slug = LANGUAGE_FILENAMES.get(lang, lang.lower().replace(" ", "_"))
            csv_path = EXPORT_DIR / f"dramashort_raw_{batch_date}_{filename_slug}.csv"

            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=DRAMASHORT_RAW_FIELDS, extrasaction="ignore")
                writer.writeheader()
                for rec in lang_records:
                    writer.writerow(rec)

            stats[lang] = len(lang_records)
            logger.info(f"导出 DramaShort 原文 [{lang}] {len(lang_records)} 条 → {csv_path}")

        return stats

    except Exception as e:
        logger.error(f"导出 DramaShort 原文数据失败：{e}")
        return {}
    finally:
        conn.close()


if __name__ == "__main__":
    # 测试导出
    today = get_utc8_today()
    print(f"导出日期：{today}")

    print("\n=== 导出 ReelShort 中文翻译数据 ===")
    stats = export_reelshort(today)
    for lang, count in stats.items():
        print(f"  [{lang}] {count} 条")

    print("\n=== 导出 DramaShorts 中文翻译数据 ===")
    stats = export_dramashort(today)
    for lang, count in stats.items():
        print(f"  [{lang}] {count} 条")
