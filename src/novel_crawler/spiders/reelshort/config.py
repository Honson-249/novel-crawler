"""
ReelShort 爬虫配置模块
"""
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ReelShortConfig:
    """ReelShort 爬虫专属配置"""

    # ==================== URL 配置 ====================
    base_url: str = "https://www.reelshort.com"

    # Tab slug 兜底默认值（爬虫启动时会从页面动态发现，此处仅作备用）
    # key 为英文显示名称，value 为 URL slug（固定英文，不随语言变化）
    tab_slugs: Dict[str, str] = field(default_factory=lambda: {
        "Actors": "movie-actors",
        "Actresses": "movie-actresses",
        "Identities": "movie-identities",
        "Story Beats": "story-beats",
    })

    # ==================== 浏览器配置 ====================
    headless: bool = True
    timeout: int = 60000        # 页面加载超时（ms）
    detail_timeout: int = 90000 # 详情页超时（ms）
    max_retries: int = 3

    # ==================== 延迟配置 ====================
    # 所有 API 请求（列表页、详情页）统一使用此延迟，在 _get_json 每次请求前触发
    page_delay_min: float = 0.3
    page_delay_max: float = 0.8

    # ==================== 语言配置 ====================
    # 语言代码 → URL 前缀（英文默认无前缀，其他语言加前缀）
    # 英文用空字符串表示无前缀
    # 动态发现失败时使用此处的完整兜底列表
    default_languages: List[str] = field(default_factory=lambda: [
        "en",    # 英语
        "pt",    # 葡萄牙语
        "es",    # 西班牙语
        "de",    # 德语
        "fr",    # 法语
        "ja",    # 日语
        "ko",    # 韩语
        "th",    # 泰语
        "ru",    # 俄语
        "id",    # 印度尼西亚语
        "zh-TW", # 繁体中文
        "ar",    # 阿拉伯语
        "pl",    # 波兰语
        "it",    # 意大利语
        "tr",    # 土耳其语
        "ro",    # 罗马尼亚语
        "cs",    # 捷克语
        "bg",    # 保加利亚语
        "vi",    # 越南语
    ])

    # 语言代码 → 简体中文名称（用于翻译结果表 language 字段展示）
    language_names: Dict[str, str] = field(default_factory=lambda: {
        "en":    "英语",
        "pt":    "葡萄牙语",
        "es":    "西班牙语",
        "de":    "德语",
        "fr":    "法语",
        "ja":    "日语",
        "ko":    "韩语",
        "th":    "泰语",
        "ru":    "俄语",
        "id":    "印度尼西亚语",
        "zh-TW": "繁体中文",
        "ar":    "阿拉伯语",
        "pl":    "波兰语",
        "it":    "意大利语",
        "tr":    "土耳其语",
        "ro":    "罗马尼亚语",
        "cs":    "捷克语",
        "bg":    "保加利亚语",
        "vi":    "越南语",
    })

    # 语言代码到 URL 路径前缀的兜底映射（爬虫启动时会从页面动态发现并更新此映射）
    # 英文（en）无前缀，其他语言使用对应代码作为前缀
    # 动态发现失败时使用此处的默认值
    lang_url_prefix: Dict[str, str] = field(default_factory=lambda: {
        "en":    "",       # English：https://www.reelshort.com/tags/...
        "pt":    "pt",     # Português
        "es":    "es",     # Español
        "de":    "de",     # Deutsch
        "fr":    "fr",     # Français
        "ja":    "ja",     # 日本語
        "ko":    "ko",     # 한국어
        "th":    "th",     # ภาษาไทย
        "ru":    "ru",     # Русск
        "id":    "id",     # Bahasa Indonesia
        "zh-TW": "zh-TW",  # 繁體中文
        "ar":    "ar",     # العربية
        "pl":    "pl",     # Polski
        "it":    "it",     # Italiano
        "tr":    "tr",     # Türkçe
        "ro":    "ro",     # Română
        "cs":    "cs",     # Čeština
        "bg":    "bg",     # български
        "vi":    "vi",     # Tiếng Việt
    })

    # ==================== 视口配置 ====================
    # ReelShort 标签页面使用移动端布局（展开按钮在 md:hidden，仅移动端可见）
    # 使用移动端视口宽度（< 768px）确保展开按钮可见可点击
    viewport_widths: List[int] = field(default_factory=lambda: [390, 414, 375, 360])
    viewport_heights: List[int] = field(default_factory=lambda: [844, 896, 812, 780])
    device_scales: List[float] = field(default_factory=lambda: [2.0, 3.0])

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

    def tab_index_url(self, tab_name: str, language: str = "en") -> str:
        """
        构建 Tab 入口页 URL

        格式：
        - 英文：https://www.reelshort.com/tags/movie-actors
        - 其他：https://www.reelshort.com/pt/tags/movie-actors

        Args:
            tab_name: Tab 名称，如 "Actors"
            language: 语言代码，如 "en"、"pt"

        Returns:
            完整 URL
        """
        slug = self.tab_slugs.get(tab_name, "")
        prefix = self.lang_url_prefix.get(language, language)
        if prefix:
            return f"{self.base_url}/{prefix}/tags/{slug}"
        return f"{self.base_url}/tags/{slug}"

    def tag_page_url(self, tab_name: str, tag_slug: str, language: str = "en", page: int = 1) -> str:
        """
        构建子分类标签剧集列表页 URL

        格式：
        - 第 1 页：https://www.reelshort.com/tags/story-beats/age-gap-movies-{id}
        - 第 N 页：https://www.reelshort.com/tags/story-beats/age-gap-movies-{id}/{page}

        Args:
            tab_name: Tab 名称，如 "Story Beats"
            tag_slug: 标签完整 slug，如 "age-gap-movies-676d210e4582b53a14081aec"
            language: 语言代码
            page: 页码（从 1 开始）

        Returns:
            完整 URL
        """
        tab_slug = self.tab_slugs.get(tab_name, "")
        prefix = self.lang_url_prefix.get(language, language)
        base = f"{self.base_url}/{prefix}/tags/{tab_slug}/{tag_slug}" if prefix else \
               f"{self.base_url}/tags/{tab_slug}/{tag_slug}"
        if page > 1:
            return f"{base}/{page}"
        return base
