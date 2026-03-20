"""
ReelShort 标签分类后处理服务

职责：
- 爬取完成后，以当天 reelshort_tags 表中的全量标签为参照集
- 对 reelshort_drama 表中当天有 tag_list_json 的记录，批量填充
  actors_tags / actresses_tags / identity_tags / story_beat_tags / genre_tags

使用场景：
- 爬取完成后手动触发：python -m cli.main reelshort-classify --date 2026-03-18
- 支持指定语言（--language en）或全量处理（不指定语言则处理所有语言）
- 幂等：重复执行会覆盖已有分类结果
"""
import json
from datetime import date
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from src.novel_crawler.config.database import get_db_manager
from src.novel_crawler.dao.reelshort_dao import get_reelshort_dao
from src.novel_crawler.pipeline.reelshort_clean import classify_tags

# 每批次更新的记录数
BATCH_SIZE = 500


def run_classify(
    batch_date: Optional[str] = None,
    language: Optional[str] = None,
) -> Dict[str, int]:
    """
    执行标签分类后处理

    Args:
        batch_date: 批次日期（YYYY-MM-DD），默认为今天
        language: 指定语言代码（如 "en"），None 表示处理所有语言

    Returns:
        {language: 更新条数} 的统计字典
    """
    if batch_date is None:
        batch_date = date.today().isoformat()

    db_manager = get_db_manager()
    dao = get_reelshort_dao(db_manager)

    # 确定要处理的语言列表
    if language:
        languages = [language]
    else:
        languages = _get_languages_for_date(dao, batch_date)

    if not languages:
        logger.warning(f"[classify] {batch_date} 无可处理的语言数据")
        return {}

    logger.info(f"[classify] 开始处理 {batch_date}，语言：{languages}")
    stats: Dict[str, int] = {}

    for lang in languages:
        count = _classify_language(dao, batch_date, lang)
        stats[lang] = count
        logger.info(f"[classify] [{lang}] 完成，更新 {count} 条")

    total = sum(stats.values())
    logger.info(f"[classify] 全部完成，共更新 {total} 条")
    return stats


def _classify_language(dao, batch_date: str, language: str) -> int:
    """
    处理单个语言的标签分类

    Args:
        dao: ReelShortDAO 实例
        batch_date: 批次日期
        language: 语言代码

    Returns:
        更新条数
    """
    # 从 DB 读取当天该语言的全量标签参照集
    tab_refs: Dict[str, Set[str]] = dao.find_tags_by_language(batch_date, language)

    if not tab_refs:
        logger.warning(f"[classify] [{language}] reelshort_tags 中无数据，跳过")
        return 0

    actors_ref = tab_refs.get("Actors", set())
    actresses_ref = tab_refs.get("Actresses", set())
    identity_ref = tab_refs.get("Identities", set())
    story_beat_ref = tab_refs.get("Story Beats", set())

    logger.info(
        f"[classify] [{language}] 参照集：Actors={len(actors_ref)}, "
        f"Actresses={len(actresses_ref)}, Identities={len(identity_ref)}, "
        f"Story Beats={len(story_beat_ref)}"
    )

    # 查询当天所有有 tag_list_json 的剧集
    dramas = dao.find_dramas_for_classify(batch_date, language)
    if not dramas:
        logger.info(f"[classify] [{language}] 无待分类剧集")
        return 0

    logger.info(f"[classify] [{language}] 共 {len(dramas)} 条待分类")

    # 批量构建更新列表
    updates: List[Dict[str, Any]] = []
    for drama in dramas:
        try:
            tag_list = json.loads(drama["tag_list_json"])
        except (json.JSONDecodeError, TypeError):
            tag_list = []

        classified = classify_tags(tag_list, actors_ref, actresses_ref, identity_ref, story_beat_ref)

        updates.append({
            "id": drama["id"],
            "actors_tags": json.dumps(classified["actors_tags"], ensure_ascii=False) if classified["actors_tags"] else None,
            "actresses_tags": json.dumps(classified["actresses_tags"], ensure_ascii=False) if classified["actresses_tags"] else None,
            "identity_tags": json.dumps(classified["identity_tags"], ensure_ascii=False) if classified["identity_tags"] else None,
            "story_beat_tags": json.dumps(classified["story_beat_tags"], ensure_ascii=False) if classified["story_beat_tags"] else None,
            "genre_tags": json.dumps(classified["genre_tags"], ensure_ascii=False) if classified["genre_tags"] else None,
        })

    # 分批写入
    total_updated = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i: i + BATCH_SIZE]
        updated = dao.batch_update_tag_classify(batch)
        total_updated += updated
        logger.debug(f"[classify] [{language}] 批次 {i // BATCH_SIZE + 1}：更新 {updated} 条")

    return total_updated


def _get_languages_for_date(dao, batch_date: str) -> List[str]:
    """
    查询当天 reelshort_tags 中有数据的语言列表

    Args:
        dao: ReelShortDAO 实例
        batch_date: 批次日期

    Returns:
        语言代码列表
    """
    conn = dao.db_manager.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT DISTINCT language FROM reelshort_tags WHERE batch_date = %s",
            (batch_date,),
        )
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"[classify] 查询语言列表失败：{e}")
        return []
    finally:
        conn.close()
