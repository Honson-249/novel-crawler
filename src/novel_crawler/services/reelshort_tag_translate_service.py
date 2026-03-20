"""
ReelShort 标签翻译服务

职责：
- 从 reelshort_tags 查询尚未翻译（tag_name_zh 为空）的标签
- 按语言分批调用 LLM，将标签名翻译为简体中文
- 将翻译结果写回 reelshort_tags.tag_name_zh

特点：
- 标签名通常较短（演员名、剧情类型等），每批可翻译更多条
- 同一次运行内已翻译的标签不重复请求（内存缓存）
- 幂等：已有 tag_name_zh 的记录不会被覆盖（除非 force=True）
"""
import asyncio
from typing import Dict, List, Optional

from loguru import logger

from src.novel_crawler.config.database import db_manager
from src.novel_crawler.dao.reelshort_dao import get_reelshort_dao
from src.novel_crawler.llm.client import LLMClient

# 每批翻译的标签数量（标签短，可以多放一些）
_TAG_BATCH_SIZE = 30

_TAG_SYSTEM_PROMPT = (
    "你是影视内容翻译专家，专注于短剧内容的中文本地化。"
    "用户会给你一个 JSON 数组，每个元素包含 _idx（索引）和 tag（标签原文）。"
    "请将每个 tag 翻译为简体中文，严格遵守以下规则：\n"
    "1. 演员/女演员名（人名）：音译为中文，如 John Smith → 约翰·史密斯\n"
    "2. 剧情类型/身份/故事节拍等标签：意译为简体中文，保持简洁\n"
    "3. 空字符串或 null 保持原样\n"
    "4. _idx 字段原样保留，不翻译\n"
    "5. 只返回翻译后的 JSON 数组，不要任何解释或 markdown 代码块\n"
    "6. 输出格式：[{\"_idx\": 0, \"tag\": \"译文\"}, ...]"
)


async def translate_tags(
    language: Optional[str] = None,
    batch_size: int = _TAG_BATCH_SIZE,
    workers: int = 3,
    force: bool = False,
) -> int:
    """
    翻译 reelshort_tags 表中尚未翻译的标签

    Args:
        language: 指定语言，None 表示全部语言
        batch_size: 每次 LLM 请求翻译的标签数
        workers: LLM 并发数
        force: True 时重新翻译已有 tag_name_zh 的记录

    Returns:
        成功翻译的标签数
    """
    dao = get_reelshort_dao(db_manager())
    llm = LLMClient(max_concurrency=workers)

    if force:
        # force 模式：查全部（含已翻译）
        conn = db_manager().get_connection()
        cur = conn.cursor()
        try:
            if language:
                cur.execute(
                    "SELECT id, language, tab_name, tag_name FROM reelshort_tags WHERE language = %s ORDER BY language, tab_name, id",
                    (language,),
                )
            else:
                cur.execute(
                    "SELECT id, language, tab_name, tag_name FROM reelshort_tags ORDER BY language, tab_name, id"
                )
            columns = [d[0] for d in cur.description]
            records = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    else:
        records = dao.find_tags_without_zh(language)

    if not records:
        logger.info("[标签翻译] 没有需要翻译的标签")
        return 0

    logger.info(f"[标签翻译] 共 {len(records)} 条标签待翻译，batch_size={batch_size}")

    # 按 batch_size 分批，并发翻译
    total_updated = 0
    batches = [records[i : i + batch_size] for i in range(0, len(records), batch_size)]

    semaphore = asyncio.Semaphore(workers)

    async def _translate_batch(batch: List[Dict]) -> List[Dict]:
        payload = [{"_idx": i, "tag": r["tag_name"]} for i, r in enumerate(batch)]
        async with semaphore:
            results = await llm._call_translate_with_prompt(payload, _TAG_SYSTEM_PROMPT)
        # 按 _idx 对齐
        idx_map = {item.get("_idx"): item.get("tag", "") for item in results if "_idx" in item}
        updates = []
        for i, rec in enumerate(batch):
            zh = idx_map.get(i)
            if zh and zh.strip():
                updates.append({"id": rec["id"], "tag_name_zh": zh.strip()})
        return updates

    total_batches = len(batches)
    total_updated = 0

    # 每次只并发 workers 个批次，翻译完立即入库，避免一次性堆积过多协程
    for i in range(0, total_batches, workers):
        chunk = batches[i: i + workers]
        chunk_results = await asyncio.gather(
            *[_translate_batch(b) for b in chunk], return_exceptions=True
        )
        chunk_updates: List[Dict] = []
        for res in chunk_results:
            if isinstance(res, Exception):
                logger.error(f"[标签翻译] 批次失败：{res}")
            else:
                chunk_updates.extend(res)

        if chunk_updates:
            written = dao.update_tag_zh_batch(chunk_updates)
            total_updated += written

        done = min(i + workers, total_batches)
        logger.info(
            f"[标签翻译] 进度 {done}/{total_batches} 批，本轮入库 {len(chunk_updates)} 条，累计 {total_updated} 条"
        )

    return total_updated
