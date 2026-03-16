#!/usr/bin/env python3
"""
爬虫抽象基类
定义爬虫的标准接口和通用功能
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from loguru import logger


# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


@dataclass
class SpiderConfig:
    """爬虫配置基类"""
    site_name: str = "unknown"
    base_url: str = ""
    timeout: int = 30
    retry_times: int = 3
    delay_range: Tuple[int, int] = (1, 3)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlStatistics:
    """爬取统计信息"""
    pages_crawled: int = 0
    items_extracted: int = 0
    items_stored: int = 0
    errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        """爬取耗时"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_minutes(self) -> float:
        """爬取耗时（分钟）"""
        return self.duration_seconds / 60.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pages_crawled": self.pages_crawled,
            "items_extracted": self.items_extracted,
            "items_stored": self.items_stored,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
            "duration_minutes": self.duration_minutes,
        }


@dataclass
class CrawlResult:
    """爬取结果"""
    success: bool
    statistics: CrawlStatistics
    error_message: Optional[str] = None
    data: List[Dict[str, Any]] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "statistics": self.statistics.to_dict(),
            "error_message": self.error_message,
            "item_count": len(self.data),
            "extra": self.extra,
        }


class BaseSpider(ABC):
    """
    爬虫抽象基类

    使用方法:
    1. 继承此类
    2. 实现抽象方法
    3. 可选：重写钩子方法

    示例:
        class FanqieSpider(BaseSpider):
            async def _do_crawl(self) -> CrawlResult:
                # 实现爬取逻辑
                pass
    """

    def __init__(self, config: Optional[SpiderConfig] = None):
        self.config = config or SpiderConfig()
        self.stats = CrawlStatistics()
        self._stop_requested = False

    # ==================== 抽象方法（必须实现） ====================

    @abstractmethod
    async def _do_crawl(self) -> CrawlResult:
        """
        执行爬取（核心逻辑）

        Returns:
            CrawlResult: 爬取结果
        """
        pass

    @abstractmethod
    async def _initialize(self) -> None:
        """
        初始化爬虫（如：启动浏览器、加载配置等）
        """
        pass

    @abstractmethod
    async def _cleanup(self) -> None:
        """
        清理资源（如：关闭浏览器、释放连接等）
        """
        pass

    # ==================== 钩子方法（可选实现） ====================

    async def _before_crawl(self) -> None:
        """爬取前钩子（如：健康检查、登录等）"""
        pass

    async def _after_crawl(self, result: CrawlResult) -> None:
        """
        爬取后钩子（如：数据统计、通知等）

        Args:
            result: 爬取结果
        """
        pass

    async def _on_error(self, error: Exception) -> None:
        """
        错误处理钩子

        Args:
            error: 异常对象
        """
        logger.error(f"爬取过程中发生错误：{error}")

    # ==================== 公共方法 ====================

    async def run(self, **kwargs) -> CrawlResult:
        """
        运行爬虫（统一入口）

        Args:
            **kwargs: 额外参数

        Returns:
            CrawlResult: 爬取结果
        """
        logger.info(f"开始爬虫：{self.config.site_name}")
        self._stop_requested = False
        self.stats = CrawlStatistics()
        self.stats.start_time = datetime.now(UTC8)

        try:
            # 初始化
            await self._initialize()
            logger.debug("爬虫初始化完成")

            # 爬取前钩子
            await self._before_crawl()

            # 执行爬取
            result = await self._do_crawl()

            # 爬取后钩子
            await self._after_crawl(result)

            # 更新统计
            self.stats.end_time = datetime.now(UTC8)
            self.stats.errors = 0 if result.success else 1

            logger.info(
                f"爬虫完成：{self.config.site_name}, "
                f"页面：{self.stats.pages_crawled}, "
                f"数据：{self.stats.items_extracted}, "
                f"耗时：{self.stats.duration_minutes:.2f}分钟"
            )

            return result

        except asyncio.CancelledError:
            logger.warning(f"爬虫被取消：{self.config.site_name}")
            self.stats.end_time = datetime.now(UTC8)
            return CrawlResult(
                success=False,
                statistics=self.stats,
                error_message="爬虫被取消"
            )

        except Exception as e:
            logger.error(f"爬虫失败：{self.config.site_name}, 错误：{e}")
            await self._on_error(e)
            self.stats.end_time = datetime.now(UTC8)
            self.stats.errors += 1

            return CrawlResult(
                success=False,
                statistics=self.stats,
                error_message=str(e)
            )

        finally:
            # 清理资源
            await self._cleanup()
            logger.debug("爬虫资源已清理")

    def request_stop(self) -> None:
        """请求停止爬虫"""
        self._stop_requested = True
        logger.info(f"收到停止请求：{self.config.site_name}")

    def should_stop(self) -> bool:
        """检查是否应该停止"""
        return self._stop_requested

    def get_statistics(self) -> Dict[str, Any]:
        """获取爬取统计"""
        return self.stats.to_dict()

    # ==================== 工具方法 ====================

    def update_stats(
        self,
        pages: int = 0,
        items: int = 0,
        stored: int = 0
    ) -> None:
        """
        更新统计信息

        Args:
            pages: 新增页面数
            items: 新增数据项数
            stored: 新增存储项数
        """
        self.stats.pages_crawled += pages
        self.stats.items_extracted += items
        self.stats.items_stored += stored

    def log_progress(
        self,
        message: str,
        level: str = "info",
        **context
    ) -> None:
        """
        记录进度日志

        Args:
            message: 日志消息
            level: 日志级别
            **context: 上下文信息
        """
        context_str = " | ".join(f"{k}={v}" for k, v in context.items())
        log_msg = f"[{self.config.site_name}] {message}"
        if context_str:
            log_msg += f" ({context_str})"

        getattr(logger, level.lower(), logger.info)(log_msg)


# ==================== 解析器基类 ====================

class BaseParser(ABC):
    """
    页面解析器抽象基类

    用于统一不同站点的页面解析逻辑
    """

    @abstractmethod
    def parse(self, html: str, **kwargs) -> Any:
        """
        解析页面

        Args:
            html: HTML 内容
            **kwargs: 额外参数

        Returns:
            解析结果
        """
        pass

    @abstractmethod
    def parse_list(self, html: str) -> List[Dict[str, Any]]:
        """
        解析列表页

        Args:
            html: HTML 内容

        Returns:
            数据列表
        """
        pass

    @abstractmethod
    def parse_detail(self, html: str, item_id: str) -> Dict[str, Any]:
        """
        解析详情页

        Args:
            html: HTML 内容
            item_id: 数据项 ID

        Returns:
            单个数据项
        """
        pass


__all__ = [
    "SpiderConfig",
    "CrawlStatistics",
    "CrawlResult",
    "BaseSpider",
    "BaseParser",
]
