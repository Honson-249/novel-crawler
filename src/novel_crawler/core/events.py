#!/usr/bin/env python3
"""
事件定义
爬虫生命周期中的各种事件
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Optional


# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


class EventType(Enum):
    """事件类型"""
    # 爬虫生命周期
    SPIDER_INITIALIZED = "spider_initialized"
    SPIDER_STARTED = "spider_started"
    SPIDER_COMPLETED = "spider_completed"
    SPIDER_FAILED = "spider_failed"
    SPIDER_CLEANED_UP = "spider_cleaned_up"

    # 页面级别
    PAGE_FETCH_STARTED = "page_fetch_started"
    PAGE_FETCH_COMPLETED = "page_fetch_completed"
    PAGE_FETCH_FAILED = "page_fetch_failed"

    # 数据级别
    ITEM_EXTRACTED = "item_extracted"
    ITEM_VALIDATED = "item_validated"
    ITEM_STORED = "item_stored"

    # 错误级别
    ERROR_OCCURRED = "error_occurred"
    RETRY_STARTED = "retry_started"
    RETRY_COMPLETED = "retry_completed"

    # 自定义
    CUSTOM = "custom"


@dataclass
class Event:
    """
    事件基类

    Attributes:
        event_type: 事件类型
        spider_name: 爬虫名称
        timestamp: 事件时间戳
        data: 事件数据
    """
    event_type: EventType
    spider_name: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC8))
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "event_type": self.event_type.value,
            "spider_name": self.spider_name,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
        }


# ==================== 具体事件类 ====================

@dataclass
class SpiderInitializedEvent(Event):
    """爬虫初始化完成事件"""
    def __post_init__(self):
        self.event_type = EventType.SPIDER_INITIALIZED


@dataclass
class SpiderStartedEvent(Event):
    """爬虫开始事件"""
    def __post_init__(self):
        self.event_type = EventType.SPIDER_STARTED


@dataclass
class SpiderCompletedEvent(Event):
    """爬虫完成事件"""
    pages_crawled: int = 0
    items_extracted: int = 0
    items_stored: int = 0
    duration_seconds: float = 0.0

    def __post_init__(self):
        self.event_type = EventType.SPIDER_COMPLETED
        self.data = {
            "pages_crawled": self.pages_crawled,
            "items_extracted": self.items_extracted,
            "items_stored": self.items_stored,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class SpiderFailedEvent(Event):
    """爬虫失败事件"""
    error_message: str = ""
    error_type: str = ""

    def __post_init__(self):
        self.event_type = EventType.SPIDER_FAILED
        self.data = {
            "error_message": self.error_message,
            "error_type": self.error_type,
        }


@dataclass
class PageFetchStartedEvent(Event):
    """页面抓取开始事件"""
    url: str = ""

    def __post_init__(self):
        self.event_type = EventType.PAGE_FETCH_STARTED
        self.data["url"] = self.url


@dataclass
class PageFetchCompletedEvent(Event):
    """页面抓取完成事件"""
    url: str = ""
    status_code: int = 200
    response_time_ms: float = 0.0

    def __post_init__(self):
        self.event_type = EventType.PAGE_FETCH_COMPLETED
        self.data = {
            "url": self.url,
            "status_code": self.status_code,
            "response_time_ms": self.response_time_ms,
        }


@dataclass
class PageFetchFailedEvent(Event):
    """页面抓取失败事件"""
    url: str = ""
    error_message: str = ""
    status_code: Optional[int] = None

    def __post_init__(self):
        self.event_type = EventType.PAGE_FETCH_FAILED
        self.data = {
            "url": self.url,
            "error_message": self.error_message,
            "status_code": self.status_code,
        }


@dataclass
class ItemExtractedEvent(Event):
    """数据项提取事件"""
    item_id: str = ""
    item_type: str = ""
    item_data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.event_type = EventType.ITEM_EXTRACTED
        self.data = {
            "item_id": self.item_id,
            "item_type": self.item_type,
        }


@dataclass
class ItemStoredEvent(Event):
    """数据项存储事件"""
    item_id: str = ""
    storage_type: str = ""

    def __post_init__(self):
        self.event_type = EventType.ITEM_STORED
        self.data = {
            "item_id": self.item_id,
            "storage_type": self.storage_type,
        }


@dataclass
class ErrorOccurredEvent(Event):
    """错误发生事件"""
    error_message: str = ""
    error_type: str = ""
    traceback: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.event_type = EventType.ERROR_OCCURRED
        self.data = {
            "error_message": self.error_message,
            "error_type": self.error_type,
            "traceback": self.traceback,
            **self.context,
        }


# ==================== 事件总线（简单实现） ====================

class EventBus:
    """
    简单事件总线
    用于发布/订阅事件
    """

    def __init__(self):
        self._subscribers: Dict[EventType, list] = {}

    def subscribe(self, event_type: EventType, callback: callable) -> None:
        """
        订阅事件

        Args:
            event_type: 事件类型
            callback: 回调函数
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: EventType, callback: callable) -> None:
        """取消订阅"""
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(callback)
            except ValueError:
                pass

    def publish(self, event: Event) -> None:
        """
        发布事件

        Args:
            event: 事件对象
        """
        callbacks = self._subscribers.get(event.event_type, [])
        for callback in callbacks:
            try:
                callback(event)
            except Exception as e:
                # 避免回调异常影响事件发布
                from loguru import logger
                logger.error(f"事件回调失败 [{event.event_type.value}]: {e}")

    def clear(self) -> None:
        """清空所有订阅"""
        self._subscribers.clear()


# 全局事件总线实例
_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """获取全局事件总线实例"""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus


def publish_event(event: Event) -> None:
    """发布事件"""
    get_event_bus().publish(event)


__all__ = [
    "EventType",
    "Event",
    "SpiderInitializedEvent",
    "SpiderStartedEvent",
    "SpiderCompletedEvent",
    "SpiderFailedEvent",
    "PageFetchStartedEvent",
    "PageFetchCompletedEvent",
    "PageFetchFailedEvent",
    "ItemExtractedEvent",
    "ItemStoredEvent",
    "ErrorOccurredEvent",
    "EventBus",
    "get_event_bus",
    "publish_event",
]
