"""
ReelShort 翻译服务

职责：
- 从 reelshort_drama 查询待翻译记录
- 每 LLM_BATCH_SIZE 条记录合并为一次 LLM 请求（默认 5 条/次）
- 将翻译结果写入 reelshort_drama_zh 表
- 支持按 batch_date / language 过滤，支持并发控制

翻译字段（每批一次请求全部翻译）：
- board_name, sub_category, series_title, synopsis（纯文本）
- tag_list_json, actors_tags, actresses_tags, identity_tags,
  story_beat_tags, genre_tags（JSON 数组，模型直接翻译数组内每项）

直接复制字段（不翻译）：
- detail_url, play_count_raw, play_count, favorite_count_raw, favorite_count

去重优化：
1. 同剧跨批次复用：同语言下相同 detail_url 已有翻译时直接复用，不调用 LLM
2. 标签内存缓存：同一次任务内相同标签字符串只翻译一次，命中缓存的标签不进入 LLM payload
"""
import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from loguru import logger

from src.novel_crawler.config.database import db_manager
from src.novel_crawler.dao.reelshort_dao import get_reelshort_dao
from src.novel_crawler.llm.client import LLMClient

# UTC+8
UTC8 = timezone(timedelta(hours=8))

# 每次 LLM 请求合并的记录数（默认值，可通过 run_translate 参数覆盖）
LLM_BATCH_SIZE = 1

# 纯文本翻译字段（board_name 单独通过 board_cache 处理，不放入每条记录的 LLM payload）
_TEXT_FIELDS = ["series_title", "synopsis"]

# 需要翻译的 JSON 数组标签字段（不含 tag_list_json，它是其他5个字段的并集，翻译后重新拼合）
_JSON_TAG_FIELDS = [
    "actors_tags",
    "actresses_tags",
    "identity_tags",
    "story_beat_tags",
    "genre_tags",
]

# tag_list_json 是上面 5 个字段的并集，不单独翻译，翻译后从 5 个字段重新拼合
_TAG_LIST_COMPOSED_FIELDS = _JSON_TAG_FIELDS

_ALL_TRANSLATE_FIELDS = _TEXT_FIELDS + _JSON_TAG_FIELDS + ["tag_list_json"]

# 可跨记录复用的字段（同一部剧在不同榜单/批次中这些字段不变）
# board_name 是榜单名，每条记录不同，不能复用
_REUSABLE_FIELDS = ["series_title", "synopsis"] + _JSON_TAG_FIELDS + ["tag_list_json"]


def _parse_json_tags(json_str: Optional[str]) -> Optional[List[str]]:
    """解析 JSON 标签数组，失败返回 None"""
    if not json_str or not json_str.strip():
        return None
    try:
        tags = json.loads(json_str)
        if isinstance(tags, list):
            return [str(t) for t in tags]
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _build_record_payload(record: Dict, tag_cache: Dict[str, str]) -> Dict:
    """
    构建单条记录发给 LLM 的字段 dict

    - 纯文本字段直接放入
    - JSON 标签字段（5个分类字段）：只放入未被 tag_cache 覆盖的新标签
    - tag_list_json 不发给 LLM，翻译后从 5 个字段重新拼合
    """
    payload: Dict = {}

    for field in _TEXT_FIELDS:
        val = record.get(field) or ""
        if val.strip():
            payload[field] = val

    for field in _JSON_TAG_FIELDS:
        tags = _parse_json_tags(record.get(field))
        if not tags:
            continue
        new_tags = [t for t in tags if t not in tag_cache]
        if new_tags:
            payload[field] = json.dumps(new_tags, ensure_ascii=False)

    return payload


def _apply_translation(
    record: Dict,
    translated: Dict,
    tag_cache: Dict[str, str],
) -> Dict:
    """
    将 LLM 翻译结果合并回 record，并更新 tag_cache。
    tag_list_json 不单独翻译，翻译完成后从 5 个分类字段重新拼合（去重保序）。
    """
    result = dict(record)

    for field in _TEXT_FIELDS:
        if field in translated and translated[field]:
            result[field] = translated[field]

    for field in _JSON_TAG_FIELDS:
        tags = _parse_json_tags(record.get(field))
        if not tags:
            continue

        new_translations: Dict[str, str] = {}
        if field in translated and translated[field]:
            raw = translated[field]
            try:
                new_tags_translated = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(new_tags_translated, list):
                    new_tags_original = [t for t in tags if t not in tag_cache]
                    for orig, trans in zip(new_tags_original, new_tags_translated):
                        new_translations[orig] = str(trans)
                        tag_cache[orig] = str(trans)
            except (json.JSONDecodeError, TypeError):
                pass

        final_tags = []
        seen_tags = set()
        for tag in tags:
            if tag in tag_cache:
                translated_tag = tag_cache[tag]
            elif tag in new_translations:
                translated_tag = new_translations[tag]
            else:
                translated_tag = tag
            if translated_tag not in seen_tags:
                final_tags.append(translated_tag)
                seen_tags.add(translated_tag)

        result[field] = json.dumps(final_tags, ensure_ascii=False)

    # tag_list_json 从 5 个分类字段重新拼合（去重保序），不依赖 LLM 单独翻译
    combined = []
    seen_combined: set = set()
    for field in _TAG_LIST_COMPOSED_FIELDS:
        tags = _parse_json_tags(result.get(field))
        if tags:
            for t in tags:
                if t not in seen_combined:
                    combined.append(t)
                    seen_combined.add(t)
    if combined:
        result["tag_list_json"] = json.dumps(combined, ensure_ascii=False)

    return result


class _TranslateContext:
    """单次翻译任务上下文"""

    def __init__(self, url_cache: Dict[str, Dict], board_cache: Dict[str, str]):
        self.url_cache = url_cache              # {detail_url -> 已翻译字段}
        self.tag_cache: Dict[str, str] = {}     # {原始标签 -> 翻译结果}
        self.board_cache = board_cache          # {原始 board_name -> 翻译结果}（预填充，并发安全）
        self.url_hit = 0
        self.api_calls = 0
        self.tag_cache_hit = 0


async def _prefill_board_cache(
    records: List[Dict], client: LLMClient
) -> Dict[str, str]:
    """
    串行预翻译所有唯一的 board_name，返回完整的 board_cache。
    在并发翻译开始前调用，避免并发写缓存导致的不一致。
    """
    unique_boards = list({r.get("board_name") or "" for r in records} - {""})
    if not unique_boards:
        return {}

    board_cache: Dict[str, str] = {}
    payloads = [{"board_name": b} for b in unique_boards]
    # 一次请求翻译所有唯一 board_name
    translated = await client.translate_records_batch(payloads)
    for orig, trans in zip(unique_boards, translated):
        if trans.get("board_name"):
            board_cache[orig] = trans["board_name"]
    logger.info(f"[翻译] 预翻译 board_name：{len(board_cache)} 个唯一榜单名")
    return board_cache


async def _translate_group(
    group: List[Dict], client: LLMClient, ctx: _TranslateContext
) -> List[Tuple[Dict, bool]]:
    """
    将一组记录（最多 LLM_BATCH_SIZE 条）合并为一次 LLM 请求翻译

    复用规则：
    - series_title / synopsis / tag 字段可跨批次复用（同剧内容不变）
    - board_name 是榜单名，每条记录不同，始终需要翻译

    Returns:
        [(翻译后的 dict, is_reused), ...]
    """
    results: List[Tuple[Dict, bool]] = []
    need_translate: List[Tuple[int, Dict]] = []  # 只收集需要全量翻译的记录

    for i, record in enumerate(group):
        r = dict(record)
        r["source_id"] = record["id"]
        detail_url = record.get("detail_url") or ""

        if detail_url and detail_url in ctx.url_cache:
            cached = ctx.url_cache[detail_url]
            for field in _REUSABLE_FIELDS:
                r[field] = cached.get(field, record.get(field))
            ctx.url_hit += 1
            results.append((r, True))
            # URL 缓存命中：board_name 由 board_cache 处理，无需 LLM
        else:
            results.append((r, False))
            need_translate.append((i, record))  # 全量翻译

    # 所有记录先统一从 board_cache 填充 board_name
    for i, record in enumerate(group):
        r, reused = results[i]
        orig_board = record.get("board_name") or ""
        if orig_board and orig_board in ctx.board_cache:
            r["board_name"] = ctx.board_cache[orig_board]
        results[i] = (r, reused)

    if not need_translate:
        return results

    # 统计标签缓存命中
    for _, record in need_translate:
        for field in _JSON_TAG_FIELDS:
            tags = _parse_json_tags(record.get(field))
            if tags:
                ctx.tag_cache_hit += sum(1 for t in tags if t in ctx.tag_cache)

    # 构建 payload：全量翻译（不含 board_name，board_name 已由 board_cache 处理）
    payloads = [_build_record_payload(rec, ctx.tag_cache) for _, rec in need_translate]

    translated_list = await client.translate_records_batch(payloads)
    ctx.api_calls += 1

    # 翻译 API 失败，跳过整组
    if not translated_list:
        logger.warning(f"[翻译] LLM 返回空结果，跳过本组 {len(need_translate)} 条记录")
        for group_idx, _ in need_translate:
            results[group_idx] = (None, False)
        return results

    # 将翻译结果写回
    for pos, (group_idx, record) in enumerate(need_translate):
        translated = translated_list[pos] if pos < len(translated_list) else {}
        if not translated:
            logger.warning(f"[翻译] 记录 id={record['id']} 翻译结果缺失，跳过")
            results[group_idx] = (None, False)
            continue

        r, reused = results[group_idx]
        if r is None:
            r = dict(record)
            r["source_id"] = record["id"]

        r = _apply_translation(r, translated, ctx.tag_cache)
        # 新翻译的记录写入 URL 缓存（只缓存可复用字段，不含 board_name）
        detail_url = record.get("detail_url") or ""
        if detail_url:
            ctx.url_cache[detail_url] = {
                field: r[field] for field in _REUSABLE_FIELDS
            }

        results[group_idx] = (r, reused)

    return results


async def run_translate(
    batch_date: Optional[str] = None,
    language: Optional[str] = None,
    workers: int = 5,
    llm_batch: int = LLM_BATCH_SIZE,
) -> Dict[str, int]:
    """
    执行翻译任务

    Args:
        batch_date: 批次日期（YYYY-MM-DD），默认为今天（UTC+8）
        language: 语言代码，为 None 时处理所有语言
        workers: 并发 LLM 请求数
        llm_batch: 每次 LLM 请求合并的记录数（默认 5）

    Returns:
        统计字典 {"total", "translated", "reused", "skipped"}
    """
    if batch_date is None:
        batch_date = datetime.now(UTC8).strftime("%Y-%m-%d")

    logger.info(
        f"[翻译] 开始翻译 batch_date={batch_date} "
        f"language={language or '全部'} workers={workers} "
        f"llm_batch={llm_batch}条/次"
    )

    dao = get_reelshort_dao(db_manager())
    client = LLMClient(max_concurrency=workers)

    records = dao.find_dramas_for_translate(batch_date, language)
    total = len(records)

    if total == 0:
        logger.info("[翻译] 没有需要翻译的记录（可能已全部翻译）")
        return {"total": 0, "translated": 0, "reused": 0, "skipped": 0}

    logger.info(f"[翻译] 共 {total} 条待翻译记录，每次 LLM 请求处理 {llm_batch} 条")

    by_language: Dict[str, List[Dict]] = defaultdict(list)
    for r in records:
        by_language[r["language"]].append(r)

    translated_count = 0
    reused_count = 0
    skipped_count = 0

    # 并发处理的外层批次大小 = workers * llm_batch
    outer_batch_size = workers * llm_batch

    for lang, lang_records in by_language.items():
        logger.info(f"[翻译] 语言 [{lang}]：{len(lang_records)} 条")

        url_cache = dao.find_translated_by_url(lang)
        board_cache = await _prefill_board_cache(lang_records, client)
        ctx = _TranslateContext(url_cache, board_cache)

        # 从 reelshort_tags 预加载已翻译的标签对照表，优先于 LLM 翻译
        tag_zh_map = dao.find_tag_zh_map(lang)
        if tag_zh_map:
            ctx.tag_cache.update(tag_zh_map)
            logger.info(f"[翻译] [{lang}] 从标签表预加载 {len(tag_zh_map)} 个标签译名")

        for i in range(0, len(lang_records), outer_batch_size):
            outer_batch = lang_records[i: i + outer_batch_size]

            # 将外层批次切成 llm_batch 大小的小组，并发发起
            groups = [
                outer_batch[j: j + llm_batch]
                for j in range(0, len(outer_batch), llm_batch)
            ]

            logger.info(
                f"[翻译] [{lang}] 第 {i + 1}~{min(i + outer_batch_size, len(lang_records))} 条"
                f"（{len(groups)} 个并发请求，每请求 {llm_batch} 条）"
            )

            try:
                group_results = await asyncio.gather(
                    *[_translate_group(g, client, ctx) for g in groups]
                )

                # 过滤掉翻译失败的记录（None 表示跳过）
                all_translated = [
                    r for group_result in group_results
                    for r, _ in group_result if r is not None
                ]
                written = dao.insert_drama_zh_batch(all_translated)
                batch_reused = sum(
                    1 for group_result in group_results
                    for r, reused in group_result if r is not None and reused
                )
                translated_count += written
                reused_count += batch_reused
                skipped_count += len(outer_batch) - written

            except Exception as e:
                logger.error(f"[翻译] [{lang}] 批次处理失败：{e}")
                skipped_count += len(outer_batch)

        logger.info(
            f"[翻译] [{lang}] 完成：URL复用={ctx.url_hit}，"
            f"标签缓存命中={ctx.tag_cache_hit}，LLM请求={ctx.api_calls}"
        )

    logger.info(
        f"[翻译] 全部完成：总计 {total} 条，"
        f"写入 {translated_count} 条（含复用 {reused_count} 条），"
        f"跳过 {skipped_count} 条"
    )
    return {
        "total": total,
        "translated": translated_count,
        "reused": reused_count,
        "skipped": skipped_count,
    }
