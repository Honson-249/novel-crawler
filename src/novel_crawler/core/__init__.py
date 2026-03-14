#!/usr/bin/env python3
"""
核心抽象层模块 - 导出
"""
from .base_spider import (
    SpiderConfig,
    CrawlStatistics,
    CrawlResult,
    BaseSpider,
    BaseParser,
)

from .base_parser import (
    BasePageParser,
)

from .events import (
    EventType,
    Event,
    SpiderInitializedEvent,
    SpiderStartedEvent,
    SpiderCompletedEvent,
    SpiderFailedEvent,
    PageFetchStartedEvent,
    PageFetchCompletedEvent,
    PageFetchFailedEvent,
    ItemExtractedEvent,
    ItemStoredEvent,
    ErrorOccurredEvent,
    EventBus,
    get_event_bus,
    publish_event,
)


__all__ = [
    # Base Spider
    "SpiderConfig",
    "CrawlStatistics",
    "CrawlResult",
    "BaseSpider",
    "BaseParser",
    # Base Parser
    "BasePageParser",
    # Events
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
