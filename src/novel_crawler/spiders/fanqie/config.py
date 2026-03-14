#!/usr/bin/env python3
"""
爬虫配置模块 - 集中管理所有配置项
"""
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class SpiderConfig:
    """爬虫配置类"""

    # 基础 URL
    base_url: str = "https://fanqienovel.com"
    rank_url: str = "https://fanqienovel.com/rank"

    # 浏览器配置
    headless: bool = True
    window_size: tuple = (1920, 1080)
    timeout: int = 60000
    detail_timeout: int = 90000

    # 重试配置
    max_retries: int = 3
    base_delay_min: float = 1.0  # 原来 3.0
    base_delay_max: float = 2.0  # 原来 5.0

    # 人类行为模拟配置
    scroll_min: int = 100
    scroll_max: int = 500
    delay_min: float = 0.5  # 原来 1.0
    delay_max: float = 1.5  # 原来 3.0

    # 性别和榜单类型映射
    genders: Dict[str, str] = field(default_factory=lambda: {
        "0": "女频",
        "1": "男频"
    })
    board_types: Dict[str, str] = field(default_factory=lambda: {
        "1": "新书榜",
        "2": "阅读榜",
        "3": "完本榜",
        "4": "热读榜"
    })

    # User Agent 列表
    user_agents: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    ])

    # 视口选项
    viewport_widths: List[int] = field(default_factory=lambda: [1920, 1366, 1440, 1536, 1280])
    viewport_heights: List[int] = field(default_factory=lambda: [1080, 768, 900, 864, 720])
    device_scales: List[float] = field(default_factory=lambda: [1, 1.25, 1.5, 2])

    # HTTP 请求头
    extra_http_headers: Dict[str, str] = field(default_factory=lambda: {
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })

    # 浏览器启动参数
    browser_args: List[str] = field(default_factory=lambda: [
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--disable-background-networking',
        '--disable-default-apps',
        '--disable-extensions',
        '--disable-sync',
        '--disable-translate',
        '--metrics-recording-only',
        '--no-first-run',
        '--safebrowsing-disable-auto-update',
        '--disable-features=ImprovedCookieControls',
        '--disable-features=LeakDetector',
        '--disable-features=MediaRouter',
        '--mute-audio',
        '--no-default-browser-check',
        '--window-position=0,0',
        '--window-size=1920,1080',
    ])
