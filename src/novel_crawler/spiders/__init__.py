"""
爬虫模块 - 人类行为模拟爬虫

结构:
- src/novel_crawler/spiders/fanqie/ - 番茄小说爬虫实现
- src/novel_crawler/spiders/[future_sites]/ - 未来可扩展其他站点
"""

from .fanqie.spider import FanqieSpider
from .fanqie.browser_manager import BrowserManager
from .fanqie.human_simulator import HumanSimulator
from .fanqie.page_parser import PageParser
from .fanqie.data_processor import DataProcessor
from .fanqie.config import SpiderConfig as FanqieSpiderConfig

# 别名：保持向后兼容
HumanSimulatedSpider = FanqieSpider


__all__ = [
    "FanqieSpider",
    "HumanSimulatedSpider",  # 向后兼容别名
    "BrowserManager",
    "HumanSimulator",
    "PageParser",
    "DataProcessor",
    "FanqieSpiderConfig",
]
