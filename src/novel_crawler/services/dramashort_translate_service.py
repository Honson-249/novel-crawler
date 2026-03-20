"""
DramaShorts 翻译服务

职责：
- 从 dramashort_drama 查询待翻译记录
- 每 LLM_BATCH_SIZE 条记录合并为一次 LLM 请求（默认 5 条/次）
- 将翻译结果写入 dramashort_drama_zh 表
- 支持按 batch_date / language 过滤，支持并发控制

翻译字段：
- board_name, series_title, synopsis（纯文本）

直接复制字段（不翻译）：
- detail_url, board_order, play_count_raw, play_count,
  favorite_count_raw, favorite_count, likes_count_raw, likes_count,
  episodes_count, score

去重优化：
1. 同剧跨批次复用：同语言下相同 detail_url 已有翻译时直接复用，不调用 LLM
"""
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from loguru import logger

from src.novel_crawler.config.database import db_manager
from src.novel_crawler.dao.dramashort_dao import get_dramashort_dao
from src.novel_crawler.llm.client import LLMClient

UTC8 = timezone(timedelta(hours=8))

LLM_BATCH_SIZE = 1

# board_name 单独通过 board_cache 处理，不放入每条记录的 LLM payload
_TEXT_FIELDS = ["series_title", "synopsis"]

# series_title / synopsis 可跨批次复用；board_name 是榜单名，每条不同，不能复用
_REUSABLE_FIELDS = ["series_title", "synopsis"]


async def _translate_group(
    group: List[Dict],
    client: LLMClient,
    url_cache: Dict[str, Dict],
    board_cache: Dict[str, str],
    api_calls_ref: List[int],
    url_hit_ref: List[int],
) -> List[Tuple[Dict, bool]]:
    """
    将一组记录（最多 LLM_BATCH_SIZE 条）合并为一次 LLM 请求翻译

    复用规则：
    - series_title / synopsis 可跨批次复用（同剧内容不变）
    - board_name 是榜单名，每条记录不同，始终需要翻译

    Returns:
        [(翻译后的 dict, is_reused), ...]
    """
    results: List[Tuple[Dict, bool]] = []
    # (group_index, record, is_url_hit)
    need_translate: List[Tuple[int, Dict, bool]] = []

    for i, record in enumerate(group):
        r = dict(record)
        r["source_id"] = record["id"]
        detail_url = record.get("detail_url") or ""

        if detail_url and detail_url in url_cache:
            cached = url_cache[detail_url]
            for field in _REUSABLE_FIELDS:
                r[field] = cached.get(field, record.get(field))
            url_hit_ref[0] += 1
            results.append((r, True))
            # URL 缓存命中：board_name 由 board_cache 处理，无需 LLM
        else:
            results.append((r, False))
            need_translate.append((i, record, False))  # 全量翻译

    if not need_translate:
        # 所有记录都命中 URL 缓存，board_name 直接从 board_cache 填充
        for i, record in enumerate(group):
            r, reused = results[i]
            orig_board = record.get("board_name") or ""
            if orig_board and orig_board in board_cache:
                r["board_name"] = board_cache[orig_board]
            results[i] = (r, reused)
        return results

    payloads = []
    for _, rec, _ in need_translate:
        item = {}
        for field in _TEXT_FIELDS:
            val = rec.get(field) or ""
            if val.strip():
                item[field] = val
        payloads.append(item)

    filtered_payloads = [p for p in payloads if p]
    if filtered_payloads:
        translated_list = await client.translate_records_batch(filtered_payloads)
        api_calls_ref[0] += 1
    else:
        translated_list = []

    # 翻译 API 失败（返回空列表），跳过整组，不写入未翻译数据
    if filtered_payloads and not translated_list:
        logger.warning(f"[DS翻译] LLM 返回空结果，跳过本组 {len(need_translate)} 条记录")
        for group_idx, _, _ in need_translate:
            results[group_idx] = (None, False)
        return results

    translated_pos = 0
    for pos, (group_idx, record, is_hit) in enumerate(need_translate):
        r, reused = results[group_idx]
        if r is None:
            r = dict(record)
            r["source_id"] = record["id"]

        # board_name 统一从 board_cache 取（预翻译阶段已填充）
        orig_board = record.get("board_name") or ""
        if orig_board and orig_board in board_cache:
            r["board_name"] = board_cache[orig_board]

        if payloads[pos]:
            translated = translated_list[translated_pos] if translated_pos < len(translated_list) else {}
            translated_pos += 1
            if not translated and not is_hit:
                logger.warning(f"[DS翻译] 记录 id={record['id']} 翻译结果缺失，跳过")
                results[group_idx] = (None, False)
                continue
            if not is_hit:
                for field in _TEXT_FIELDS:
                    if field in translated and translated[field]:
                        r[field] = translated[field]
                # 新翻译的记录写入 URL 缓存（只缓存可复用字段，不含 board_name）
                detail_url = record.get("detail_url") or ""
                if detail_url:
                    url_cache[detail_url] = {field: r.get(field) for field in _REUSABLE_FIELDS}

        results[group_idx] = (r, reused)

    return results


async def run_translate(
    batch_date: Optional[str] = None,
    language: Optional[str] = None,
    workers: int = 5,
    llm_batch: int = LLM_BATCH_SIZE,
) -> Dict[str, int]:
    """
    执行 DramaShorts 翻译任务

    Args:
        batch_date: 批次日期（YYYY-MM-DD），默认为今天（UTC+8）
        language: 语言代码，None 时处理所有语言
        workers: 并发 LLM 请求数
        llm_batch: 每次 LLM 请求合并的记录数（默认 5）

    Returns:
        统计字典 {"total", "translated", "reused", "skipped"}
    """
    if batch_date is None:
        batch_date = datetime.now(UTC8).strftime("%Y-%m-%d")

    logger.info(
        f"[DS翻译] 开始翻译 batch_date={batch_date} "
        f"language={language or '全部'} workers={workers} "
        f"llm_batch={llm_batch}条/次"
    )

    dao = get_dramashort_dao(db_manager())
    client = LLMClient(max_concurrency=workers)

    records = dao.find_dramas_for_translate(batch_date, language)
    total = len(records)

    if total == 0:
        logger.info("[DS翻译] 没有需要翻译的记录（可能已全部翻译）")
        return {"total": 0, "translated": 0, "reused": 0, "skipped": 0}

    logger.info(f"[DS翻译] 共 {total} 条待翻译记录，每次 LLM 请求处理 {llm_batch} 条")

    by_language: Dict[str, List[Dict]] = defaultdict(list)
    for r in records:
        by_language[r["language"]].append(r)

    translated_count = 0
    reused_count = 0
    skipped_count = 0

    outer_batch_size = workers * llm_batch

    for lang, lang_records in by_language.items():
        logger.info(f"[DS翻译] 语言 [{lang}]：{len(lang_records)} 条")

        url_cache = dao.find_translated_by_url(lang)
        # 预翻译所有唯一 board_name，避免并发写缓存导致不一致
        unique_boards = list({r.get("board_name") or "" for r in lang_records} - {""})
        board_cache: Dict[str, str] = {}
        if unique_boards:
            board_payloads = [{"board_name": b} for b in unique_boards]
            board_translated = await client.translate_records_batch(board_payloads)
            for orig, trans in zip(unique_boards, board_translated):
                if trans.get("board_name"):
                    board_cache[orig] = trans["board_name"]
            logger.info(f"[DS翻译] 预翻译 board_name：{len(board_cache)} 个唯一榜单名")
        api_calls_ref = [0]
        url_hit_ref = [0]

        for i in range(0, len(lang_records), outer_batch_size):
            outer_batch = lang_records[i: i + outer_batch_size]
            groups = [
                outer_batch[j: j + llm_batch]
                for j in range(0, len(outer_batch), llm_batch)
            ]

            logger.info(
                f"[DS翻译] [{lang}] 第 {i + 1}~{min(i + outer_batch_size, len(lang_records))} 条"
                f"（{len(groups)} 个并发请求，每请求 {llm_batch} 条）"
            )

            try:
                group_results = await asyncio.gather(
                    *[
                        _translate_group(g, client, url_cache, board_cache, api_calls_ref, url_hit_ref)
                        for g in groups
                    ]
                )

                # 过滤掉翻译失败的记录（None 表示跳过）
                all_translated = [r for group_result in group_results for r, _ in group_result if r is not None]
                written = dao.insert_drama_zh_batch(all_translated)
                batch_reused = sum(
                    1 for group_result in group_results for r, reused in group_result if r is not None and reused
                )
                translated_count += written
                reused_count += batch_reused
                skipped_count += len(outer_batch) - written

            except Exception as e:
                logger.error(f"[DS翻译] [{lang}] 批次处理失败：{e}")
                skipped_count += len(outer_batch)

        logger.info(
            f"[DS翻译] [{lang}] 完成：URL复用={url_hit_ref[0]}，LLM请求={api_calls_ref[0]}"
        )

    logger.info(
        f"[DS翻译] 全部完成：总计 {total} 条，"
        f"写入 {translated_count} 条（含复用 {reused_count} 条），"
        f"跳过 {skipped_count} 条"
    )
    return {
        "total": total,
        "translated": translated_count,
        "reused": reused_count,
        "skipped": skipped_count,
    }
