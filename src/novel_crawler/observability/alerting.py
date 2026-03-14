#!/usr/bin/env python3
"""
告警通知模块
- 支持多渠道通知（飞书、钉钉、企业微信）
- 支持告警阈值配置
- 支持告警频率限制
"""
import time
import json
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

import httpx
from loguru import logger


# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """告警渠道"""
    FEISHU = "feishu"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"


@dataclass
class Alert:
    """告警数据类"""
    title: str
    content: str
    level: AlertLevel
    channel: Optional[AlertChannel] = None
    spider_name: Optional[str] = None
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None
    extra_data: Dict[str, Any] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(UTC8)
        if self.extra_data is None:
            self.extra_data = {}


class AlertManager:
    """
    告警管理器
    - 单例模式
    - 支持多渠道
    - 支持频率限制
    """
    _instance: Optional['AlertManager'] = None

    def __new__(cls) -> 'AlertManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # 配置
        self.enabled = False
        self.webhooks: Dict[str, str] = {}
        self.channels: List[AlertChannel] = []

        # 频率限制（相同标题的告警在指定时间内只发送一次）
        self.rate_limit_seconds = 300  # 5 分钟
        self._last_sent: Dict[str, datetime] = {}

        # 失败率阈值
        self.failure_rate_threshold = 0.1  # 10%
        self._failure_count = 0
        self._total_count = 0
        self._window_start: Optional[datetime] = None
        self._window_seconds = 3600  # 1 小时窗口

    def configure(
        self,
        enabled: bool = False,
        feishu_webhook: Optional[str] = None,
        dingtalk_webhook: Optional[str] = None,
        wechat_webhook: Optional[str] = None,
        channels: Optional[List[str]] = None,
        failure_rate_threshold: float = 0.1,
    ) -> None:
        """
        配置告警管理器

        Args:
            enabled: 是否启用
            feishu_webhook: 飞书 Webhook
            dingtalk_webhook: 钉钉 Webhook
            wechat_webhook: 企业微信 Webhook
            channels: 启用的渠道列表
            failure_rate_threshold: 失败率阈值
        """
        self.enabled = enabled
        self.failure_rate_threshold = failure_rate_threshold

        if feishu_webhook:
            self.webhooks[AlertChannel.FEISHU.value] = feishu_webhook
        if dingtalk_webhook:
            self.webhooks[AlertChannel.DINGTALK.value] = dingtalk_webhook
        if wechat_webhook:
            self.webhooks[AlertChannel.WECHAT.value] = wechat_webhook

        if channels:
            self.channels = [AlertChannel(c) for c in channels]
        else:
            self.channels = [
                AlertChannel(c) for c in self.webhooks.keys()
                if c in [ch.value for ch in AlertChannel]
            ]

        logger.info(
            f"告警配置完成 - 启用：{enabled}, "
            f"渠道：{[c.value for c in self.channels]}, "
            f"失败率阈值：{failure_rate_threshold:.1%}"
        )

    async def send(self, alert: Alert) -> bool:
        """
        发送告警

        Args:
            alert: 告警对象

        Returns:
            是否发送成功
        """
        if not self.enabled:
            logger.debug(f"告警已禁用：{alert.title}")
            return False

        # 检查频率限制
        if not self._check_rate_limit(alert.title):
            logger.debug(f"告警频率限制：{alert.title}")
            return False

        # 发送到所有配置的渠道
        success = False
        for channel in self.channels:
            if channel.value in self.webhooks:
                result = await self._send_to_channel(channel, alert)
                success = success or result

        if success:
            self._last_sent[alert.title] = datetime.now(UTC8)

        return success

    def send_sync(self, alert: Alert) -> bool:
        """同步发送告警"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.send(alert))

    async def _send_to_channel(
        self,
        channel: AlertChannel,
        alert: Alert
    ) -> bool:
        """
        发送到指定渠道

        Args:
            channel: 渠道
            alert: 告警对象

        Returns:
            是否发送成功
        """
        webhook = self.webhooks.get(channel.value)
        if not webhook:
            logger.error(f"渠道 {channel.value} 的 Webhook 未配置")
            return False

        try:
            if channel == AlertChannel.FEISHU:
                return await self._send_feishu(webhook, alert)
            elif channel == AlertChannel.DINGTALK:
                return await self._send_dingtalk(webhook, alert)
            elif channel == AlertChannel.WECHAT:
                return await self._send_wechat(webhook, alert)
        except Exception as e:
            logger.error(f"发送告警失败 ({channel.value}): {e}")
            return False

        return False

    async def _send_feishu(self, webhook: str, alert: Alert) -> bool:
        """发送飞书告警"""
        content = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"{self._get_emoji(alert.level)} {alert.title}"
                    },
                    "template": self._get_feishu_color(alert.level)
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": self._format_content(alert)
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"时间：{alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                            }
                        ]
                    }
                ]
            }
        }

        return await self._post_webhook(webhook, content)

    async def _send_dingtalk(self, webhook: str, alert: Alert) -> bool:
        """发送钉钉告警"""
        content = {
            "msgtype": "markdown",
            "markdown": {
                "title": alert.title,
                "text": self._format_content(alert, markdown=True)
            }
        }

        return await self._post_webhook(webhook, content)

    async def _send_wechat(self, webhook: str, alert: Alert) -> bool:
        """发送企业微信告警"""
        content = {
            "msgtype": "markdown",
            "markdown": {
                "content": self._format_content(alert, markdown=True)
            }
        }

        return await self._post_webhook(webhook, content)

    async def _post_webhook(self, webhook: str, content: Dict[str, Any]) -> bool:
        """发送 HTTP POST 请求到 Webhook"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(webhook, json=content)
                response.raise_for_status()

                # 检查响应
                result = response.json()
                if isinstance(result, dict):
                    # 不同渠道的成功标识可能不同
                    if result.get("code") == 0 or result.get("errcode") == 0 or result.get("StatusCode") == 0:
                        return True
                    if "ok" in str(result).lower():
                        return True

                return True  # 默认认为成功
            except Exception as e:
                logger.error(f"发送 Webhook 失败：{e}")
                return False

    def _check_rate_limit(self, title: str) -> bool:
        """检查频率限制"""
        now = datetime.now(UTC8)
        last_sent = self._last_sent.get(title)

        if last_sent and (now - last_sent).total_seconds() < self.rate_limit_seconds:
            return False

        return True

    def _get_emoji(self, level: AlertLevel) -> str:
        """获取告警级别对应的 Emoji"""
        emojis = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "❌",
            AlertLevel.CRITICAL: "🚨",
        }
        return emojis.get(level, "📢")

    def _get_feishu_color(self, level: AlertLevel) -> str:
        """获取飞书卡片颜色"""
        colors = {
            AlertLevel.INFO: "blue",
            AlertLevel.WARNING: "orange",
            AlertLevel.ERROR: "red",
            AlertLevel.CRITICAL: "purple",
        }
        return colors.get(level, "gray")

    def _format_content(self, alert: Alert, markdown: bool = False) -> str:
        """格式化告警内容"""
        lines = [
            f"**告警**: {alert.title}",
            f"**内容**: {alert.content}",
        ]

        if alert.spider_name:
            lines.append(f"**爬虫**: {alert.spider_name}")

        if alert.error_message:
            lines.append(f"**错误**: {alert.error_message}")

        if alert.extra_data:
            for k, v in alert.extra_data.items():
                lines.append(f"**{k}**: {v}")

        if markdown:
            return "\n\n".join(lines)
        return "\n".join(lines)

    def record_result(self, success: bool, spider_name: str = "") -> None:
        """
        记录爬取结果用于失败率统计

        Args:
            success: 是否成功
            spider_name: 爬虫名称
        """
        now = datetime.now(UTC8)

        # 重置时间窗口
        if self._window_start is None or (now - self._window_start).total_seconds() > self._window_seconds:
            self._window_start = now
            self._failure_count = 0
            self._total_count = 0

        self._total_count += 1
        if not success:
            self._failure_count += 1

        # 检查失败率
        if self._total_count >= 10:  # 至少 10 次才判断
            failure_rate = self._failure_count / self._total_count
            if failure_rate >= self.failure_rate_threshold:
                alert = Alert(
                    title="爬取失败率过高",
                    content=f"当前失败率：{failure_rate:.1%} (阈值：{self.failure_rate_threshold:.1%})",
                    level=AlertLevel.WARNING,
                    spider_name=spider_name,
                    extra_data={
                        "failure_count": self._failure_count,
                        "total_count": self._total_count,
                        "window": f"{self._window_seconds / 60}分钟",
                    }
                )
                self.send_sync(alert)
                # 重置计数避免重复告警
                self._failure_count = 0
                self._total_count = 0


# ==================== 全局实例 ====================

_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """获取全局告警管理器实例"""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def send_alert(title: str, content: str, level: AlertLevel = AlertLevel.INFO) -> bool:
    """快速发送告警"""
    alert = Alert(title=title, content=content, level=level)
    return get_alert_manager().send_sync(alert)


def configure_alerts(
    enabled: bool = False,
    feishu_webhook: Optional[str] = None,
    dingtalk_webhook: Optional[str] = None,
    wechat_webhook: Optional[str] = None,
    **kwargs
) -> None:
    """配置告警"""
    get_alert_manager().configure(
        enabled=enabled,
        feishu_webhook=feishu_webhook,
        dingtalk_webhook=dingtalk_webhook,
        wechat_webhook=wechat_webhook,
        **kwargs
    )
