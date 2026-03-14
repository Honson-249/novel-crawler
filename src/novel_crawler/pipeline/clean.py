"""
数据清洗模块
"""
import re
from typing import Dict, Any, Optional
from loguru import logger


def clean_text(text: Optional[str]) -> Optional[str]:
    """清洗文本"""
    if not text:
        return None
    return text.strip()


def parse_heat_value(heat_display: Optional[str]) -> Optional[int]:
    """
    解析热度值
    
    Args:
        heat_display: 热度显示（如：45.2 万）
    
    Returns:
        热度值（如：452000）
    """
    if not heat_display:
        return None
    
    match = re.search(r"([\d.]+) 万", heat_display)
    if match:
        return int(float(match.group(1)) * 10000)
    
    match = re.search(r"(\d+)", heat_display)
    if match:
        return int(match.group(1))
    
    return None


def parse_tags(tags: Optional[str]) -> Optional[str]:
    """解析标签"""
    if not tags:
        return None
    return tags.strip()


def clean_fanqie_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    清洗番茄小说单条记录
    
    Args:
        record: 原始记录
    
    Returns:
        清洗后的记录
    """
    import json
    
    # 处理 chapter_list_json 为 JSON 字符串
    chapter_raw = record.get("chapter_list_json")
    chapter_json = None
    if chapter_raw:
        if isinstance(chapter_raw, list):
            chapter_json = json.dumps(chapter_raw, ensure_ascii=False)
        elif isinstance(chapter_raw, str):
            chapter_json = chapter_raw
    
    # 解析 metric_value（如果原始记录没有但 metric_value_raw 有）
    metric_value = record.get("metric_value")
    if not metric_value and record.get("metric_value_raw"):
        metric_value = parse_metric_value(record.get("metric_value_raw"))
    
    return {
        # 数据库字段（旧字段名）
        "batch_date": None,  # 入库时填充
        "board_name": clean_text(record.get("board_name")),
        "sub_category": clean_text(record.get("sub_category")),
        "rank_position": record.get("rank_position"),
        "category": record.get("category", "all"),
        "book_id": clean_text(record.get("book_id")),
        "book_name": clean_text(record.get("book_name")),
        "author": clean_text(record.get("author")),
        "heat_display": clean_text(record.get("metric_value_raw")),  # 映射到 heat_display
        "heat_value": parse_heat_value(record.get("metric_value_raw")),  # 映射到 heat_value
        "metric_name": clean_text(record.get("metric_name")),
        "metric_value_raw": clean_text(record.get("metric_value_raw")),
        "metric_value": metric_value,
        "book_status": clean_text(record.get("book_status")),
        "synopsis": clean_text(record.get("synopsis")),
        "chapter_list_json": chapter_json,
        # 辅助字段
        "cover_url": clean_text(record.get("cover_url")),
        "detail_url": clean_text(record.get("detail_url")),
        "crawl_level": clean_text(record.get("crawl_level")),
    }


def parse_metric_value(metric_value_raw: Optional[str]) -> Optional[int]:
    """
    解析指标值（按照 OCR 规范）
    
    Args:
        metric_value_raw: 原始指标值（如 "在读：42.3 万"）
    
    Returns:
        数值（如 423000）
    """
    if not metric_value_raw:
        return None
    
    # 提取数字部分
    match = re.search(r"([\d.]+) 万", metric_value_raw)
    if match:
        return int(float(match.group(1)) * 10000)
    
    match = re.search(r"[:：]\s*(\d+)", metric_value_raw)
    if match:
        return int(match.group(1))
    
    return None


def clean_batch_records(records: list, source: str) -> list:
    """
    批量清洗记录
    
    Args:
        records: 原始记录列表
        source: 数据源（fanqie 或 hongguo）
    
    Returns:
        清洗后的记录列表
    """
    cleaned_records = []
    clean_func = clean_fanqie_record if source == "fanqie" else None
    
    if not clean_func:
        return records
    
    for record in records:
        try:
            cleaned = clean_func(record)
            cleaned_records.append(cleaned)
        except Exception as e:
            logger.error(f"清洗记录失败：{record}, 错误：{e}")
    
    logger.info(f"清洗完成：{len(cleaned_records)}/{len(records)} 条记录")
    return cleaned_records
