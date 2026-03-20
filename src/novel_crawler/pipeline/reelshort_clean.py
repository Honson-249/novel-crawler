"""
ReelShort 数据清洗模块

负责：
1. 播放量 / 收藏量数值清洗（K/M 单位换算）
2. 标签交叉比对分类（基于 Tab 参照集将 tag_list 拆分为五类子标签）
3. 完整的剧集记录清洗入口
"""
import json
import re
from typing import Any, Dict, List, Optional, Set

from loguru import logger


# ==================== 数值清洗 ====================

def parse_count_value(raw: str) -> Optional[int]:
    """
    清洗播放量 / 收藏量原始文案为整数

    换算规则：
    - K / k = × 1,000
    - M / m = × 1,000,000

    示例：
    - "22.5M"   → 22500000
    - "112.9M"  → 112900000
    - "251.9k"  → 251900
    - "1.1M"    → 1100000
    - "154.2k"  → 154200
    - "13.1M"   → 13100000

    Args:
        raw: 原始文案，如 "22.5M"、"251.9k"

    Returns:
        整数数值，无法解析时返回 None
    """
    if not raw:
        return None

    raw = raw.strip()

    # 匹配 数字 + 可选小数 + 可选单位
    match = re.search(r"([\d]+(?:\.[\d]+)?)\s*([kKmM]?)", raw)
    if not match:
        return None

    value_str, unit = match.group(1), match.group(2).upper()

    try:
        value = float(value_str)
    except ValueError:
        return None

    if unit == "K":
        return int(value * 1_000)
    elif unit == "M":
        return int(value * 1_000_000)
    else:
        return int(value)


# ==================== 标签交叉比对 ====================

def classify_tags(
    tag_list: List[str],
    actors_ref: Set[str],
    actresses_ref: Set[str],
    identity_ref: Set[str],
    story_beat_ref: Set[str],
) -> Dict[str, List[str]]:
    """
    将剧集标签列表按 Tab 参照集分类

    分类逻辑（按优先级顺序匹配）：
    - tag ∈ actors_ref       → actors_tags
    - tag ∈ actresses_ref    → actresses_tags
    - tag ∈ identity_ref     → identity_tags
    - tag ∈ story_beat_ref   → story_beat_tags
    - 以上均不匹配           → genre_tags

    注：同一 tag 可能属于多个参照集（如演员名同时出现在 Actors 和 Actresses Tab），
    实际发现时会按优先级放入第一个匹配的类别。

    Args:
        tag_list: 详情页全量标签列表
        actors_ref: Actors Tab 的标签参照集
        actresses_ref: Actresses Tab 的标签参照集
        identity_ref: Identities Tab 的标签参照集
        story_beat_ref: Story Beats Tab 的标签参照集

    Returns:
        {
            "actors_tags": [...],
            "actresses_tags": [...],
            "identity_tags": [...],
            "story_beat_tags": [...],
            "genre_tags": [...],
        }
    """
    result: Dict[str, List[str]] = {
        "actors_tags": [],
        "actresses_tags": [],
        "identity_tags": [],
        "story_beat_tags": [],
        "genre_tags": [],
    }

    for tag in tag_list:
        if not tag:
            continue
        tag_lower = tag.lower()

        if actors_ref and tag_lower in {t.lower() for t in actors_ref}:
            result["actors_tags"].append(tag)
        elif actresses_ref and tag_lower in {t.lower() for t in actresses_ref}:
            result["actresses_tags"].append(tag)
        elif identity_ref and tag_lower in {t.lower() for t in identity_ref}:
            result["identity_tags"].append(tag)
        elif story_beat_ref and tag_lower in {t.lower() for t in story_beat_ref}:
            result["story_beat_tags"].append(tag)
        else:
            result["genre_tags"].append(tag)

    return result


# ==================== 完整记录清洗 ====================

def clean_drama_record(
    record: Dict[str, Any],
) -> Dict[str, Any]:
    """
    清洗单条剧集记录

    步骤：
    1. 清洗播放量 / 收藏量（字符串 → 整数）
    2. tag_list 序列化为 JSON 字符串
    3. 各字段标准化（strip、None 处理）

    注：actors_tags / actresses_tags / identity_tags / story_beat_tags / genre_tags
    由后处理命令 reelshort-classify 基于当天全量 reelshort_tags 参照集批量填充。

    Args:
        record: 原始合并记录（来自 spider._merge_drama_data）

    Returns:
        清洗后的记录，字段与 reelshort_drama 表对应
    """
    # 播放量 / 收藏量清洗
    play_count_raw = _clean_str(record.get("play_count_raw"))
    favorite_count_raw = _clean_str(record.get("favorite_count_raw"))
    play_count = parse_count_value(play_count_raw) if play_count_raw else None
    favorite_count = parse_count_value(favorite_count_raw) if favorite_count_raw else None

    # 标签列表
    tag_list = record.get("tag_list") or []
    if isinstance(tag_list, str):
        try:
            tag_list = json.loads(tag_list)
        except Exception:
            tag_list = []

    # tag_list 序列化（分类字段由后处理批量填充，此处不计算）
    tag_list_json = json.dumps(tag_list, ensure_ascii=False) if tag_list else None

    return {
        "batch_date": record.get("batch_date"),
        "language": _clean_str(record.get("language")) or "en",
        "board_name": _clean_str(record.get("board_name")),
        "sub_category": _clean_str(record.get("sub_category")),
        "detail_url": _clean_str(record.get("detail_url")),
        "series_title": _clean_str(record.get("series_title")),
        "play_count_raw": play_count_raw,
        "play_count": play_count,
        "favorite_count_raw": favorite_count_raw,
        "favorite_count": favorite_count,
        "tag_list_json": tag_list_json,
        "actors_tags": None,
        "actresses_tags": None,
        "identity_tags": None,
        "story_beat_tags": None,
        "genre_tags": None,
        "synopsis": _clean_str(record.get("synopsis")),
    }


# ==================== 私有工具 ====================

def _clean_str(value: Any) -> Optional[str]:
    """
    清洗字符串：strip，空字符串返回 None

    Args:
        value: 输入值

    Returns:
        清洗后的字符串，空值返回 None
    """
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
