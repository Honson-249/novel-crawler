#!/usr/bin/env python3
"""
指标收集模块
- 爬取成功率
- 爬取耗时
- 页面数量
- 数据量统计
"""
import time
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path
from loguru import logger
from dataclasses import dataclass, field, asdict


# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


@dataclass
class CrawlResult:
    """爬取结果数据类"""
    spider_name: str
    success: bool
    start_time: float
    end_time: float
    pages_crawled: int = 0
    items_extracted: int = 0
    items_stored: int = 0
    error_message: Optional[str] = None
    extra_metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """爬取耗时（秒）"""
        return self.end_time - self.start_time

    @property
    def duration_ms(self) -> float:
        """爬取耗时（毫秒）"""
        return (self.end_time - self.start_time) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            **asdict(self),
            "duration": self.duration,
            "duration_ms": self.duration_ms,
        }


class MetricsCollector:
    """
    指标收集器
    - 单例模式
    - 内存存储（后续可扩展到 Redis/Prometheus）
    - 支持指标导出
    """
    _instance: Optional['MetricsCollector'] = None

    def __new__(cls) -> 'MetricsCollector':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # 指标存储
        self._counters: Dict[str, int] = {
            "crawl_success_total": 0,
            "crawl_failure_total": 0,
            "pages_crawled_total": 0,
            "items_extracted_total": 0,
            "items_stored_total": 0,
        }

        # 耗时统计（最近 N 次）
        self._durations: List[float] = []
        self._max_durations_size = 100

        # 最近爬取结果
        self._recent_results: List[CrawlResult] = []
        self._max_recent_size = 50

        # 按爬虫名称统计
        self._spider_stats: Dict[str, Dict[str, Any]] = {}

        # 指标文件路径
        self._metrics_file = Path(__file__).parent.parent.parent / "data" / "metrics.json"
        self._metrics_file.parent.mkdir(parents=True, exist_ok=True)

    def record(self, result: CrawlResult) -> None:
        """
        记录一次爬取结果

        Args:
            result: 爬取结果
        """
        # 更新计数器
        if result.success:
            self._counters["crawl_success_total"] += 1
        else:
            self._counters["crawl_failure_total"] += 1

        self._counters["pages_crawled_total"] += result.pages_crawled
        self._counters["items_extracted_total"] += result.items_extracted
        self._counters["items_stored_total"] += result.items_stored

        # 记录耗时
        self._durations.append(result.duration)
        if len(self._durations) > self._max_durations_size:
            self._durations.pop(0)

        # 记录最近结果
        self._recent_results.append(result)
        if len(self._recent_results) > self._max_recent_size:
            self._recent_results.pop(0)

        # 按蜘蛛统计
        spider_name = result.spider_name
        if spider_name not in self._spider_stats:
            self._spider_stats[spider_name] = {
                "success": 0,
                "failure": 0,
                "total_duration": 0,
                "total_pages": 0,
                "total_items": 0,
            }

        stats = self._spider_stats[spider_name]
        if result.success:
            stats["success"] += 1
        else:
            stats["failure"] += 1
        stats["total_duration"] += result.duration
        stats["total_pages"] += result.pages_crawled
        stats["total_items"] += result.items_extracted

        # 记录日志
        log_level = "info" if result.success else "error"
        msg = (
            f"[指标] {spider_name} - "
            f"状态：{'成功' if result.success else '失败'} | "
            f"耗时：{result.duration_ms:.0f}ms | "
            f"页面：{result.pages_crawled} | "
            f"数据：{result.items_extracted}"
        )
        if result.error_message:
            msg += f" | 错误：{result.error_message}"

        getattr(logger, log_level)(msg)

        # 定期保存到文件
        self._save_metrics()

    def _save_metrics(self) -> None:
        """保存指标到文件"""
        try:
            metrics_data = {
                "updated_at": datetime.now(UTC8).isoformat(),
                "counters": self._counters.copy(),
                "durations": {
                    "recent": self._durations[-10:],  # 最近 10 次
                    "avg": sum(self._durations) / len(self._durations) if self._durations else 0,
                    "min": min(self._durations) if self._durations else 0,
                    "max": max(self._durations) if self._durations else 0,
                },
                "spider_stats": self._spider_stats,
            }

            with open(self._metrics_file, 'w', encoding='utf-8') as f:
                json.dump(metrics_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"保存指标失败：{e}")

    def get_summary(self) -> Dict[str, Any]:
        """
        获取指标汇总

        Returns:
            指标汇总字典
        """
        total = self._counters["crawl_success_total"] + self._counters["crawl_failure_total"]
        success_rate = (
            self._counters["crawl_success_total"] / total * 100
            if total > 0 else 0
        )

        return {
            "updated_at": datetime.now(UTC8).isoformat(),
            "counters": self._counters.copy(),
            "success_rate": f"{success_rate:.2f}%",
            "durations": {
                "avg_seconds": sum(self._durations) / len(self._durations) if self._durations else 0,
                "min_seconds": min(self._durations) if self._durations else 0,
                "max_seconds": max(self._durations) if self._durations else 0,
            },
            "spider_stats": self._spider_stats,
            "recent_results": [r.to_dict() for r in self._recent_results[-5:]],
        }

    def get_spider_stats(self, spider_name: str) -> Optional[Dict[str, Any]]:
        """获取指定爬虫的统计"""
        stats = self._spider_stats.get(spider_name)
        if not stats:
            return None

        total = stats["success"] + stats["failure"]
        return {
            "spider_name": spider_name,
            "success": stats["success"],
            "failure": stats["failure"],
            "success_rate": f"{stats['success'] / total * 100:.2f}%" if total > 0 else "0%",
            "avg_duration": stats["total_duration"] / total if total > 0 else 0,
            "total_pages": stats["total_pages"],
            "total_items": stats["total_items"],
        }

    def get_counter(self, name: str) -> int:
        """获取计数器值"""
        return self._counters.get(name, 0)

    def reset(self) -> None:
        """重置所有指标"""
        logger.info("重置所有指标")
        self._counters = {k: 0 for k in self._counters}
        self._durations.clear()
        self._recent_results.clear()
        self._spider_stats.clear()
        self._save_metrics()

    def load_from_file(self) -> bool:
        """从文件加载指标"""
        if not self._metrics_file.exists():
            return False

        try:
            with open(self._metrics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self._counters.update(data.get("counters", {}))
            self._durations.extend(data.get("durations", {}).get("recent", []))
            logger.info(f"从文件加载指标：{self._metrics_file}")
            return True
        except Exception as e:
            logger.error(f"加载指标失败：{e}")
            return False


# ==================== 全局实例 ====================

_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """获取全局指标收集器实例"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
        _metrics_collector.load_from_file()
    return _metrics_collector


def record_crawl(result: CrawlResult) -> None:
    """记录爬取结果"""
    get_metrics_collector().record(result)


def get_metrics_summary() -> Dict[str, Any]:
    """获取指标汇总"""
    return get_metrics_collector().get_summary()
