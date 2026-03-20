"""
DramaShorts 页面解析模块

页面结构（基于实际 HTML 分析）：

主页结构：
  - Top Banner（轮播）：div.DiscoverCarousel_wrapper__* → div.DiscoverCarousel_content__* → h3（标题）
  - 普通榜单：div.DiscoverCard_wrapper__* → header.DiscoverCard_header__*（h2 榜单名）
                                          → div.DiscoverCard_list__* → a.MovieCard_wrapper__*（h4 剧集标题）

剧集链接格式：/shorts/{uuid}
播放量格式：链接文本中包含，如 "Exclusive36MThe Billionaire's Maid"

详情页结构（CSR 渲染，需 Playwright）：
  - 简介区域：通常在 "Plot of {title}" 标题下方的段落
"""
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from loguru import logger


class DramaShortPageParser:
    """DramaShorts 页面解析器"""

    BASE_URL = "https://dramashorts.io"

    # ==================== 主页 JSON 解析（__NEXT_DATA__）====================

    def extract_next_data(self, html: str) -> Optional[Dict[str, Any]]:
        """
        从主页 HTML 中提取 __NEXT_DATA__ JSON 数据

        Next.js 将完整的页面数据嵌入 <script id="__NEXT_DATA__"> 标签，
        包含所有榜单及剧集的完整信息（含 synopsis），无需额外请求。

        Args:
            html: 主页 HTML 字符串

        Returns:
            解析后的 JSON 字典，提取失败返回 None
        """
        m = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not m:
            return None
        try:
            return json.loads(m.group(1))
        except Exception as e:
            logger.warning(f"__NEXT_DATA__ JSON 解析失败：{e}")
            return None

    def parse_home_from_json(
        self,
        next_data: Dict[str, Any],
        block_id_map: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """
        从 __NEXT_DATA__ 解析所有榜单及剧集列表

        数据结构：
          pageProps.discover → list of blocks
          每个 block：{"id": "top_trending", "type": "...", "data": {"movies": [...]}}
          每部 movie：{"id": uuid, "title": ..., "description": synopsis,
                       "viewsCount": ..., "favoritesCount": ...}

        Args:
            next_data: __NEXT_DATA__ 完整 JSON
            block_id_map: block_id → board_name 映射（来自 config）

        Returns:
            榜单列表，格式与 parse_home 一致
        """
        discover = next_data.get("props", {}).get("pageProps", {}).get("discover", [])
        if not discover:
            logger.warning("__NEXT_DATA__ 中未找到 discover 数据")
            return []

        boards = []
        board_order = 1

        for block in discover:
            block_id = block.get("id", "")
            board_name = block_id_map.get(block_id, block_id)
            movies = block.get("data", {}).get("movies", [])

            dramas = []
            for movie in movies:
                movie_id = movie.get("id", "")
                if not movie_id:
                    continue
                detail_url = f"{self.BASE_URL}/shorts/{movie_id}"
                dramas.append({
                    "series_title": movie.get("title", ""),
                    "detail_url": detail_url,
                    "play_count": movie.get("viewsCount"),
                    "favorite_count": movie.get("favoritesCount"),
                    "likes_count": movie.get("likesCount"),
                    "episodes_count": movie.get("episodesCount"),
                    "score": movie.get("score"),
                    "synopsis": movie.get("description", ""),
                })

            boards.append({
                "board_name": board_name,
                "board_order": board_order,
                "dramas": dramas,
            })
            logger.info(f"榜单 '{board_name}'（order={board_order}）解析到 {len(dramas)} 部剧集")
            board_order += 1

        return boards

    # ==================== 主页 HTML 解析（Playwright 兜底）====================

    def parse_home(self, html: str) -> List[Dict[str, Any]]:
        """
        解析主页，提取所有榜单及其剧集列表

        返回结构：
        [
            {
                "board_name": "top banner",
                "board_order": 1,
                "dramas": [{"series_title": ..., "detail_url": ..., "play_count_raw": ...}, ...]
            },
            {
                "board_name": "DramaShorts Plus",
                "board_order": 2,
                ...
            },
            ...
        ]

        Args:
            html: 主页 HTML

        Returns:
            榜单列表，每项包含 board_name、board_order、dramas
        """
        soup = BeautifulSoup(html, "lxml")
        boards = []
        board_order = 1

        # 解析 top banner（轮播区域，在所有 H2 榜单之前）
        banner_dramas = self._parse_banner(soup)
        if banner_dramas:
            boards.append({
                "board_name": "top banner",
                "board_order": board_order,
                "dramas": banner_dramas,
            })
            board_order += 1
            logger.info(f"top banner 解析到 {len(banner_dramas)} 部剧集")

        # 解析所有 H2 普通榜单
        for h2 in soup.find_all("h2"):
            board_name = h2.get_text(strip=True)
            if not board_name:
                continue

            # 榜单容器：h2 的祖父节点（DiscoverCard_wrapper）
            container = h2.parent.parent if h2.parent else None
            if container is None:
                continue

            dramas = self._parse_board_dramas(container)
            boards.append({
                "board_name": board_name,
                "board_order": board_order,
                "dramas": dramas,
            })
            logger.info(f"榜单 '{board_name}'（order={board_order}）解析到 {len(dramas)} 部剧集")
            board_order += 1

        return boards

    def _parse_banner(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        解析 top banner 轮播区域

        DOM 结构：
          div.DiscoverCarousel_wrapper__* → div.DiscoverCarousel_content__* → h3（标题）
          同一 wrapper 下有多个 content 项（轮播幻灯片）

        注意：静态 HTML 中可能只有当前展示的幻灯片，
        Playwright 渲染后会有完整的轮播项列表。

        Args:
            soup: 主页 BeautifulSoup 对象

        Returns:
            banner 剧集列表
        """
        dramas = []
        seen_urls = set()

        # 通过 class 名称模式匹配轮播容器
        carousel_wrapper = soup.find(
            class_=re.compile(r"DiscoverCarousel_wrapper", re.I)
        )
        if carousel_wrapper is None:
            # 兜底：查找所有 h3 标题（banner 使用 h3，普通榜单使用 h4）
            for h3 in soup.find_all("h3"):
                drama = self._parse_banner_item(h3, seen_urls)
                if drama:
                    dramas.append(drama)
            return dramas

        # 找轮播内容区域
        content_items = carousel_wrapper.find_all(
            class_=re.compile(r"DiscoverCarousel_content", re.I)
        )
        if not content_items:
            # 兜底：直接找 h3
            for h3 in carousel_wrapper.find_all("h3"):
                drama = self._parse_banner_item(h3, seen_urls)
                if drama:
                    dramas.append(drama)
            return dramas

        for item in content_items:
            h3 = item.find("h3")
            if h3 is None:
                continue
            drama = self._parse_banner_item_from_container(item, seen_urls)
            if drama:
                dramas.append(drama)

        return dramas

    def _parse_banner_item(self, h3_elem, seen_urls: set) -> Optional[Dict[str, Any]]:
        """
        从 h3 元素解析 banner 剧集项

        Args:
            h3_elem: h3 BeautifulSoup 元素
            seen_urls: 已处理 URL 集合（去重）

        Returns:
            剧集信息字典或 None
        """
        series_title = h3_elem.get_text(strip=True)
        if not series_title:
            return None

        # 向上找包含 shorts 链接的容器
        container = h3_elem.parent
        for _ in range(4):
            if container is None:
                break
            link = container.find("a", href=re.compile(r"/shorts/"))
            if link:
                href = link.get("href", "")
                detail_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                if detail_url in seen_urls:
                    return None
                seen_urls.add(detail_url)
                return {
                    "series_title": series_title,
                    "detail_url": detail_url,
                    "play_count_raw": "",
                    "favorite_count_raw": "",
                }
            container = container.parent

        return None

    def _parse_banner_item_from_container(
        self, container, seen_urls: set
    ) -> Optional[Dict[str, Any]]:
        """
        从轮播内容容器解析 banner 剧集项

        Args:
            container: 轮播内容容器节点
            seen_urls: 已处理 URL 集合（去重）

        Returns:
            剧集信息字典或 None
        """
        h3 = container.find("h3")
        series_title = h3.get_text(strip=True) if h3 else ""

        link = container.find("a", href=re.compile(r"/shorts/"))
        if not link:
            return None

        href = link.get("href", "")
        detail_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

        if detail_url in seen_urls:
            return None
        seen_urls.add(detail_url)

        return {
            "series_title": series_title,
            "detail_url": detail_url,
            "play_count_raw": "",
            "favorite_count_raw": "",
        }

    def _parse_board_dramas(self, container) -> List[Dict[str, Any]]:
        """
        解析普通榜单容器中的剧集列表

        DOM 结构：
          div.DiscoverCard_list__* → a.MovieCard_wrapper__*（每部剧集）
            → h4（标题）
            → 文本包含播放量，如 "Exclusive36MThe Billionaire's Maid"

        Args:
            container: 榜单容器节点（DiscoverCard_wrapper）

        Returns:
            剧集列表
        """
        dramas = []
        seen_urls = set()

        # 找所有指向 /shorts/ 的链接（每个链接对应一部剧集）
        movie_links = container.find_all("a", href=re.compile(r"/shorts/"))

        for link in movie_links:
            href = link.get("href", "")
            if not href:
                continue

            detail_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
            if detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)

            drama = self._parse_movie_card(link, detail_url)
            if drama:
                dramas.append(drama)

        return dramas

    def _parse_movie_card(self, link_elem, detail_url: str) -> Optional[Dict[str, Any]]:
        """
        从剧集链接元素解析基础信息

        链接文本格式（基于实际 HTML）：
        - "Exclusive36MThe Billionaire's Maid"  → 有 Exclusive 标记
        - "306.6KBlind Date with My Boss"        → 无 Exclusive 标记
        - "Watch Now"                             → banner 的 Watch Now 按钮，跳过

        Args:
            link_elem: a 标签 BeautifulSoup 元素
            detail_url: 已提取的详情页 URL

        Returns:
            剧集基础信息字典或 None
        """
        try:
            # 提取标题（优先找 h4，其次 h3，最后从文本提取）
            series_title = ""
            for tag in ["h4", "h3", "h2"]:
                elem = link_elem.find(tag)
                if elem:
                    series_title = elem.get_text(strip=True)
                    break

            if not series_title:
                return None

            # 从链接文本中提取播放量
            link_text = link_elem.get_text(strip=True)
            play_count_raw = self._extract_count_from_text(link_text, series_title)

            return {
                "series_title": series_title,
                "detail_url": detail_url,
                "play_count_raw": play_count_raw,
                "favorite_count_raw": "",
            }
        except Exception as e:
            logger.debug(f"解析剧集卡片失败：{e}")
            return None

    def _extract_count_from_text(self, text: str, title: str) -> str:
        """
        从链接文本中提取播放量原始值

        文本格式示例：
        - "Exclusive36MThe Billionaire's Maid" → "36M"
        - "306.6KBlind Date with My Boss"      → "306.6K"
        - "Exclusive4.5MLove You / Hate You"   → "4.5M"

        Args:
            text: 链接完整文本
            title: 剧集标题（用于从文本中剔除）

        Returns:
            播放量原始值，如 "36M"、"306.6K"，未找到返回空字符串
        """
        # 去掉 "Exclusive" 前缀和标题后缀，剩余部分即为播放量
        cleaned = text.replace("Exclusive", "").replace(title, "").strip()

        # 匹配 K/M 格式数值
        match = re.search(r"(\d+(?:\.\d+)?[kKmM])", cleaned)
        if match:
            return match.group(1)

        # 兜底：从原始文本中直接匹配
        match = re.search(r"(\d+(?:\.\d+)?[kKmM])", text)
        if match:
            return match.group(1)

        return ""

    # ==================== 详情页解析 ====================

    def parse_detail(self, html: str) -> Dict[str, Any]:
        """
        解析剧集详情页，获取剧情简介

        详情页为 CSR 渲染，需要 Playwright 加载后再解析。

        Args:
            html: 详情页 HTML（Playwright 渲染后）

        Returns:
            {
                "synopsis": str,  # 剧情简介
            }
        """
        soup = BeautifulSoup(html, "lxml")

        return {
            "synopsis": self._parse_synopsis(soup),
        }

    def _parse_synopsis(self, soup: BeautifulSoup) -> str:
        """
        解析剧情简介

        策略（按优先级）：
        1. 找 "Plot of" 文本节点，取其后最长段落
        2. 查找常见简介容器 class（synopsis/description/intro/plot/summary）
        3. 从正文中提取最长段落（>30 字符）

        Args:
            soup: 详情页 BeautifulSoup 对象

        Returns:
            剧情简介文本
        """
        # 移除展开/折叠按钮文字（避免混入简介）
        for btn in soup.find_all(string=re.compile(r"^\s*(More|Less)\s*$", re.I)):
            if btn.parent:
                btn.parent.decompose()

        # 方法一：找 "Plot of" 文本节点
        for elem in soup.find_all(string=re.compile(r"Plot of", re.I)):
            parent = elem.parent
            if parent:
                candidates = []
                next_sib = parent.find_next_sibling()
                if next_sib:
                    t = self._clean_synopsis(next_sib.get_text(strip=True))
                    if len(t) > 30:
                        candidates.append(t)
                grandparent = parent.parent
                if grandparent:
                    for p in grandparent.find_all("p"):
                        t = self._clean_synopsis(p.get_text(strip=True))
                        if len(t) > 30:
                            candidates.append(t)
                if candidates:
                    return max(candidates, key=len)[:3000]

        # 方法二：查找常见简介容器 class
        for selector in [
            '[class*="synopsis"]',
            '[class*="description"]',
            '[class*="intro"]',
            '[class*="plot"]',
            '[class*="summary"]',
            '[class*="overview"]',
        ]:
            elem = soup.select_one(selector)
            if elem:
                text = self._clean_synopsis(elem.get_text(strip=True))
                if len(text) > 30:
                    return text[:3000]

        # 方法三：从正文中提取最长段落
        paragraphs = soup.find_all("p")
        longest = ""
        for p in paragraphs:
            text = self._clean_synopsis(p.get_text(strip=True))
            if len(text) > len(longest):
                longest = text
        if len(longest) > 30:
            return longest[:3000]

        return ""

    @staticmethod
    def _clean_synopsis(text: str) -> str:
        """去除简介末尾的省略号（页面折叠截断产生的 ... 或 …）"""
        return re.sub(r"[\u2026.]{1,3}\s*$", "", text).strip()
