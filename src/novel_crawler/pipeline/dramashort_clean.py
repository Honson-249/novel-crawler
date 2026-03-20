"""
DramaShorts 数据清洗模块

负责：
1. 播放量 / 收藏量数值清洗（K/M 单位换算）
2. 整数计数值格式化为 K/M 缩写（用于 _raw 字段）
3. 完整的剧集记录清洗入口
"""
from typing import Any, Dict, Optional


def format_count(value: Optional[int]) -> str:
    """
    将整数计数值格式化为 K/M 缩写形式

    规则：
    - value >= 1,000,000：除以 1,000,000，保留一位小数，加 M 后缀
    - value >= 1,000：除以 1,000，保留一位小数，加 K 后缀
    - 其他：直接转字符串
    - 小数点后为 0 时省略（如 2.0M → 2M）

    示例：
    - 36048405 → "36M"
    - 1500000  → "1.5M"
    - 251900   → "251.9K"
    - 154200   → "154.2K"
    - 500      → "500"

    Args:
        value: 整数计数值

    Returns:
        格式化后的字符串，如 "36M"、"251.9K"
    """
    if value is None:
        return ""
    try:
        v = int(value)
    except (TypeError, ValueError):
        return ""

    if v >= 1_000_000:
        formatted = v / 1_000_000
        return f"{formatted:.1f}M".replace(".0M", "M")
    elif v >= 1_000:
        formatted = v / 1_000
        return f"{formatted:.1f}K".replace(".0K", "K")
    else:
        return str(v)


def _clean_str(value: Any) -> Optional[str]:
    """
    清洗字符串字段：strip 并将空字符串转为 None

    Args:
        value: 输入值

    Returns:
        清洗后的字符串或 None
    """
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    完整记录清洗入口

    处理内容：
    - play_count / favorite_count / likes_count：直接使用整数原值（保留精度）
    - *_raw 字段：由对应整数生成 K/M 缩写（仅用于展示）
    - 字符串字段 strip 处理

    Args:
        record: 原始剧集记录字典

    Returns:
        清洗后的记录字典
    """
    # 整数字段直接使用，不经过 raw 二次转换（避免精度丢失）
    play_count = record.get("play_count")
    favorite_count = record.get("favorite_count")

    # _raw 字段由整数生成 K/M 缩写，仅用于展示
    play_count_raw = format_count(play_count)
    favorite_count_raw = format_count(favorite_count)
    likes_count_raw = format_count(record.get("likes_count"))

    raw_score = record.get("score")
    score: Optional[float] = None
    if raw_score is not None:
        try:
            score = round(float(raw_score), 2)
        except (TypeError, ValueError):
            pass

    return {
        "batch_date": record.get("batch_date"),
        "language": _clean_str(record.get("language")) or "en",
        "board_name": _clean_str(record.get("board_name")) or "",
        "board_order": record.get("board_order"),
        "detail_url": _clean_str(record.get("detail_url")) or "",
        "series_title": _clean_str(record.get("series_title")) or "",
        "play_count_raw": play_count_raw,
        "play_count": play_count,
        "favorite_count_raw": favorite_count_raw,
        "favorite_count": favorite_count,
        "likes_count_raw": likes_count_raw,
        "likes_count": record.get("likes_count"),
        "episodes_count": record.get("episodes_count"),
        "score": score,
        "synopsis": _clean_str(record.get("synopsis")) or "",
    }
