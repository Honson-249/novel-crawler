"""
DramaShorts 爬虫配置模块
"""
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class DramaShortConfig:
    """DramaShorts 爬虫专属配置"""

    # ==================== URL 配置 ====================
    base_url: str = "https://dramashorts.io"

    # ==================== 浏览器配置 ====================
    headless: bool = True
    timeout: int = 60000         # 页面加载超时（ms）
    detail_timeout: int = 90000  # 详情页超时（ms）
    max_retries: int = 3

    # ==================== 延迟配置 ====================
    base_delay_min: float = 1.0
    base_delay_max: float = 2.5

    # ==================== 语言配置 ====================
    # 语言代码 → URL 路径前缀（英文默认无前缀）
    # 目前仅爬取英文主页，后续可扩展多语言
    default_languages: List[str] = field(default_factory=lambda: ["en"])

    # ==================== 榜单 ID 映射 ====================
    # __NEXT_DATA__ 中的 block id → 页面显示的 board_name
    # best_match 对应顶部轮播 banner，无 h2 标题
    block_id_to_board_name: Dict[str, str] = field(default_factory=lambda: {
        "best_match":       "top banner",
        "plus_subscription": "DramaShorts Plus",
        "only_on_drama":    "Only On Drama",
        "top_trending":     "Top Trending",
        "must_watch_next":  "Must Watch Next",
        "popular_now":      "Popular Now",
        "audience_favorite": "Audience Favorite",
    })

    lang_url_prefix: Dict[str, str] = field(default_factory=lambda: {
        "en": "",   # English：https://dramashorts.io/
    })

    # ==================== 视口配置 ====================
    # 使用桌面端视口，主页为桌面布局
    viewport_widths: List[int] = field(default_factory=lambda: [1920, 1440, 1280])
    viewport_heights: List[int] = field(default_factory=lambda: [1080, 900, 800])
    device_scales: List[float] = field(default_factory=lambda: [1.0])

    # ==================== User-Agent 池 ====================
    user_agents: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ])

    # ==================== 浏览器启动参数（反爬伪装）====================
    browser_args: List[str] = field(default_factory=lambda: [
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        "--disable-notifications",
        "--disable-web-security",
        "--allow-running-insecure-content",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-gpu",
        "--window-size=1920,1080",
        "--start-maximized",
        "--lang=en-US",
    ])

    # ==================== HTTP 请求头 ====================
    extra_http_headers: Dict[str, str] = field(default_factory=lambda: {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })

    def home_url(self, language: str = "en") -> str:
        """
        构建主页 URL

        Args:
            language: 语言代码，如 "en"

        Returns:
            完整主页 URL
        """
        prefix = self.lang_url_prefix.get(language, language)
        if prefix:
            return f"{self.base_url}/{prefix}"
        return self.base_url

    def detail_url(self, short_id: str) -> str:
        """
        构建剧集详情页 URL

        Args:
            short_id: 剧集 ID（UUID 格式）

        Returns:
            完整详情页 URL
        """
        return f"{self.base_url}/shorts/{short_id}"

    def next_data_url(self, build_id: str, language: str, short_id: str) -> str:
        """
        构建 Next.js SSG 数据接口 URL

        Next.js 将页面数据预渲染为 JSON，可直接 HTTP 请求，无需 Playwright。
        URL 格式：/_next/data/{buildId}/{lang}/shorts/{uuid}.json

        Args:
            build_id: Next.js 构建 ID（从主页 HTML 中提取）
            language: 语言代码，如 "en"
            short_id: 剧集 UUID

        Returns:
            完整的 Next.js 数据接口 URL
        """
        return f"{self.base_url}/_next/data/{build_id}/{language}/shorts/{short_id}.json"
