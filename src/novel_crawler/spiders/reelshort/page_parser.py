"""
ReelShort 页面解析模块

基于真实 URL 结构：
- Tab 入口页：{base}/{lang}/tags/{tab-slug}
  例：https://www.reelshort.com/tags/movie-actors
      https://www.reelshort.com/pt/tags/movie-actors

- Tab 入口页包含该 Tab 下所有子分类标签的链接列表
  例：<a href="/tags/story-beats/age-gap-movies-676d210e4582b53a14081aec">Age Gap</a>

- 子分类标签剧集列表页：{base}/{lang}/tags/{tab-slug}/{tag-slug}/{page}
  例：https://www.reelshort.com/tags/story-beats/age-gap-movies-676d210e4582b53a14081aec
      https://www.reelshort.com/tags/story-beats/age-gap-movies-676d210e4582b53a14081aec/2

- 剧集详情页：{base}/{lang}/movie/{movie-id}
"""
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger


class ReelShortPageParser:
    """ReelShort 页面解析器"""

    BASE_URL = "https://www.reelshort.com"

    # ==================== Tab 入口页解析（获取子分类标签列表）====================

    def parse_tab_index(self, html: str, tab_slug: str, language: str = "en") -> List[Dict[str, str]]:
        """
        解析 Tab 入口页，提取该 Tab 下所有子分类标签

        页面结构：Tab 入口页顶部有所有子分类标签的链接列表
        链接格式：/tags/{tab-slug}/{tag-name}-movies-{id}
                  /{lang}/tags/{tab-slug}/{tag-name}-movies-{id}

        Args:
            html: Tab 入口页 HTML
            tab_slug: Tab 的 URL slug，如 "story-beats"
            language: 语言代码，用于构建完整 URL

        Returns:
            标签列表，每项包含：
            {
                "tag_name": str,   # 标签显示名称，如 "Age Gap"
                "tag_slug": str,   # 标签完整 slug，如 "age-gap-movies-676d..."
                "tag_url": str,    # 标签完整 URL
            }
        """
        soup = BeautifulSoup(html, "lxml")
        tags = []
        seen_slugs = set()

        # 构建匹配模式：/tags/{tab-slug}/ 或 /{lang}/tags/{tab-slug}/
        # 匹配子分类标签链接（路径中包含 tab-slug，且后面还有 tag-slug）
        pattern = re.compile(rf"/tags/{re.escape(tab_slug)}/([^/\s\"']+)")

        all_links = soup.find_all("a", href=True)
        for link in all_links:
            href = link.get("href", "")
            match = pattern.search(href)
            if not match:
                continue

            tag_slug = match.group(1)
            # 过滤掉纯数字（分页链接，如 /tags/story-beats/2）
            if tag_slug.isdigit():
                continue
            # 过滤掉已处理的重复项
            if tag_slug in seen_slugs:
                continue
            seen_slugs.add(tag_slug)

            tag_name = link.get_text(strip=True)
            if not tag_name:
                continue

            # 构建完整 URL
            if href.startswith("http"):
                tag_url = href
            else:
                tag_url = f"{self.BASE_URL}{href}"

            tags.append({
                "tag_name": tag_name,
                "tag_slug": tag_slug,
                "tag_url": tag_url,
            })

        logger.info(f"Tab '{tab_slug}' 解析到 {len(tags)} 个子分类标签")
        return tags

    def parse_total_pages(self, html: str) -> int:
        """
        解析列表页的总页数

        分页链接格式：
        - /tags/story-beats/2
        - /tags/story-beats/age-gap-movies-{id}/2
        - 最后一页链接通常是最大页码

        Args:
            html: 列表页 HTML

        Returns:
            总页数（至少为 1）
        """
        soup = BeautifulSoup(html, "lxml")

        # 查找所有分页链接，提取最大页码
        max_page = 1

        # 分页链接通常是以数字结尾的路径
        page_pattern = re.compile(r"/(\d+)$")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            match = page_pattern.search(href)
            if match:
                page_num = int(match.group(1))
                if page_num > max_page:
                    max_page = page_num

        return max_page

    # ==================== 剧集列表页解析 ====================

    def parse_drama_list(self, html: str) -> List[Dict[str, Any]]:
        """
        解析子分类标签剧集列表页，提取每部剧集的基础信息

        页面结构（基于实际抓取内容）：
        - 每部剧集有标题（h2）、播放量、收藏量、简介
        - 剧集链接格式：/movie/{movie-id} 或 /{lang}/movie/{movie-id}

        Args:
            html: 剧集列表页 HTML

        Returns:
            剧集列表，每项包含：
            {
                "series_title": str,
                "detail_url": str,
                "play_count_raw": str,
                "favorite_count_raw": str,
            }
        """
        soup = BeautifulSoup(html, "lxml")
        dramas = []
        seen_urls = set()

        # 基于实际页面结构：剧集链接指向 /movie/ 路径
        movie_link_pattern = re.compile(r"/movie/")

        # 找到所有指向剧集详情页的链接
        movie_links = soup.find_all("a", href=movie_link_pattern)

        for link in movie_links:
            href = link.get("href", "")
            if not href:
                continue

            detail_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

            # 去重
            if detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)

            # 向上找最近的包含完整剧集信息的容器
            container = self._find_drama_container(link)
            if container is None:
                container = link.parent

            drama = self._parse_drama_item(container, detail_url)
            if drama:
                dramas.append(drama)

        logger.debug(f"解析到 {len(dramas)} 部剧集")
        return dramas

    def _find_drama_container(self, link_elem) -> Optional[Any]:
        """
        从链接元素向上查找包含完整剧集信息的容器节点

        策略：向上最多 5 层，找到包含 h2/h3 标题的节点
        """
        node = link_elem
        for _ in range(5):
            node = node.parent
            if node is None:
                break
            if node.find(["h2", "h3"]):
                return node
        return link_elem.parent

    def _parse_drama_item(self, container, detail_url: str) -> Optional[Dict[str, Any]]:
        """
        从剧集容器节点提取基础信息

        Args:
            container: 包含剧集信息的 BeautifulSoup 节点
            detail_url: 已提取的详情页 URL

        Returns:
            剧集基础信息字典
        """
        try:
            # 提取标题（h2 > h3 > 其他）
            series_title = ""
            for tag in ["h2", "h3", "h4"]:
                elem = container.find(tag)
                if elem:
                    series_title = elem.get_text(strip=True)
                    break

            # 提取播放量和收藏量（从文本中找 K/M 数值）
            play_count_raw, favorite_count_raw = self._extract_counts(container)

            return {
                "series_title": series_title,
                "detail_url": detail_url,
                "play_count_raw": play_count_raw,
                "favorite_count_raw": favorite_count_raw,
            }
        except Exception as e:
            logger.debug(f"解析剧集项失败：{e}")
            return None

    def _extract_counts(self, container) -> Tuple[str, str]:
        """
        从容器文本中提取播放量和收藏量

        规律（基于实际页面）：
        - 每部剧集的播放量和收藏量各出现两次（一次在卡片，一次在详情区）
        - 格式如：13.1M / 154.2k

        Returns:
            (play_count_raw, favorite_count_raw) 元组
        """
        text = container.get_text(" ", strip=True)
        # 匹配所有 K/M 格式数值
        matches = re.findall(r"\b(\d+(?:\.\d+)?[kKmM])\b", text)

        # 去重保序（同一数值出现多次只取一次）
        seen = []
        for m in matches:
            if m not in seen:
                seen.append(m)

        play_count_raw = seen[0] if len(seen) >= 1 else ""
        favorite_count_raw = seen[1] if len(seen) >= 2 else ""
        return play_count_raw, favorite_count_raw

    # ==================== 详情页解析 ====================

    def parse_drama_detail(self, html: str) -> Dict[str, Any]:
        """
        解析剧集详情页，获取全量标签和剧情简介

        Args:
            html: 详情页 HTML

        Returns:
            {
                "tag_list": List[str],     # 详情页标签区所有标签
                "synopsis": str,            # 剧情简介
                "play_count_raw": str,      # 详情页播放量（补充）
                "favorite_count_raw": str,  # 详情页收藏量（补充）
            }
        """
        soup = BeautifulSoup(html, "lxml")

        result = {
            "tag_list": [],
            "synopsis": "",
            "play_count_raw": "",
            "favorite_count_raw": "",
        }

        result["tag_list"] = self._parse_detail_tags(soup)
        result["synopsis"] = self._parse_synopsis(soup)
        result["play_count_raw"], result["favorite_count_raw"] = self._parse_detail_counts(soup)

        return result

    def _parse_detail_tags(self, soup: BeautifulSoup) -> List[str]:
        """
        解析详情页标签区域

        详情页标签链接格式：
        - /tags/story-beats/age-gap-movies-{id}
        - /tags/movie-identities/billionaire-movies-{id}
        - /tags/movie-actors/{actor-name}-movies-{id}
        """
        tags = []
        seen = set()

        # 方法一：查找所有指向 /tags/ 路径的链接（这些就是标签）
        tag_pattern = re.compile(r"/tags/[^/]+/[^/\s\"']+")
        for link in soup.find_all("a", href=tag_pattern):
            href = link.get("href", "")
            # 排除纯 Tab 入口链接（路径只有两段，如 /tags/movie-actors）
            parts = [p for p in href.split("/") if p and p not in ("tags",)]
            # 去掉语言前缀（如 "pt"）
            if parts and len(parts[0]) <= 3 and not parts[0].startswith("movie") and not parts[0].startswith("story"):
                parts = parts[1:]

            if len(parts) < 2:
                continue  # 只有 tab-slug，没有 tag-slug，跳过

            tag_name = link.get_text(strip=True)
            if tag_name and tag_name not in seen:
                seen.add(tag_name)
                tags.append(tag_name)

        # 方法二：查找 class 中含 tag/chip/badge 的元素（兜底）
        if not tags:
            logger.debug("详情页未找到 /tags/ 链接，使用 class 兜底解析")
            for elem in soup.find_all(["a", "span"], class_=re.compile(r"tag|chip|badge|genre|label", re.I)):
                text = elem.get_text(strip=True)
                if text and 1 < len(text) < 80 and text not in seen:
                    seen.add(text)
                    tags.append(text)

        return tags

    @staticmethod
    def _clean_synopsis(text: str) -> str:
        """去除简介末尾的省略号（页面折叠截断产生的 ... 或 …）"""
        return re.sub(r'[\u2026.]{1,3}\s*$', '', text).strip()

    def _parse_synopsis(self, soup: BeautifulSoup) -> str:
        """
        解析剧情简介

        基于实际页面：简介通常在 "Plot of {title}" 标题下方的段落。
        页面简介区域末尾有折叠展开按钮（文字为 "More" / "Less"），
        提取前先移除这些按钮元素，避免按钮文字混入简介内容。
        """
        # 移除简介区域内的展开/折叠按钮（文字为 "More" 或 "Less"）
        for btn in soup.find_all(string=re.compile(r"^\s*(More|Less)\s*$", re.I)):
            if btn.parent:
                btn.parent.decompose()

        # 方法一：找 "Plot of" 文本节点，取其后区域中最长的段落
        # 优先取最长段落，避免命中顶部被 CSS 截断的短版简介
        for elem in soup.find_all(string=re.compile(r"Plot of", re.I)):
            parent = elem.parent
            if parent:
                candidates = []
                # 下一个兄弟节点
                next_sib = parent.find_next_sibling()
                if next_sib:
                    t = self._clean_synopsis(next_sib.get_text(strip=True))
                    if len(t) > 30:
                        candidates.append(t)
                # 祖父节点下所有段落
                grandparent = parent.parent
                if grandparent:
                    for p in grandparent.find_all("p"):
                        t = self._clean_synopsis(p.get_text(strip=True))
                        if len(t) > 30:
                            candidates.append(t)
                if candidates:
                    return max(candidates, key=len)[:3000]

        # 方法二：查找常见简介容器
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

    def _parse_detail_counts(self, soup: BeautifulSoup) -> Tuple[str, str]:
        """
        解析详情页播放量和收藏量

        Returns:
            (play_count_raw, favorite_count_raw) 元组
        """
        # 查找包含 K/M 数值的元素
        all_text = soup.get_text(" ", strip=True)
        matches = re.findall(r"\b(\d+(?:\.\d+)?[kKmM])\b", all_text)

        seen = []
        for m in matches:
            if m not in seen:
                seen.append(m)

        play_count_raw = seen[0] if len(seen) >= 1 else ""
        favorite_count_raw = seen[1] if len(seen) >= 2 else ""
        return play_count_raw, favorite_count_raw

    # ==================== API JSON 解析（_next/data 接口）====================

    # tab-slug → tab_name 映射，用于标签分类
    TAB_SLUG_TO_NAME = {
        "movie-actors": "Actors",
        "movie-actresses": "Actresses",
        "movie-identities": "Identities",
        "story-beats": "Story Beats",
    }

    def parse_api_tab_index(
        self, data: Dict[str, Any], tab_slug: str, tab_name: str, language: str
    ) -> List[Dict[str, str]]:
        """
        解析 Tab 入口页 API 数据，提取该 Tab 下所有子分类标签

        API 返回的 tags 字段按 category_id 分组，每项包含 id 和 text。
        slug 规律：{text转连字符小写}-movies-{id}

        Args:
            data: API 返回的 pageProps 字典
            tab_slug: Tab URL slug，如 movie-actors
            tab_name: Tab 显示名称，如 Actors
            language: 语言代码

        Returns:
            标签列表，每项包含 tag_name、tag_slug、tag_url
        """
        # category_id → tab_slug 映射（用于确认取哪个分组）
        cat_id_map = {
            "movie-actors": "1001",
            "movie-actresses": "1005",
            "movie-identities": "1020",
            "story-beats": "1022",
        }
        cat_id = cat_id_map.get(tab_slug)
        if not cat_id:
            logger.warning(f"未知 tab_slug：{tab_slug}，无法确定 category_id")
            return []

        tags_by_cat = data.get("tags", {})
        raw_tags = tags_by_cat.get(cat_id, [])

        if not raw_tags:
            logger.warning(f"Tab '{tab_name}' API 返回标签为空（category_id={cat_id}）")
            return []

        result = []
        lang_prefix = f"/{language}"
        for t in raw_tags:
            tag_id = t.get("id", "")
            tag_text = t.get("text", "").strip()
            if not tag_id or not tag_text:
                continue

            # 构造 slug：text 转小写连字符 + -movies- + id
            slug_text = re.sub(r"[^\w\s-]", "", tag_text.lower()).strip()
            slug_text = re.sub(r"[\s_]+", "-", slug_text)
            slug_text = re.sub(r"-+", "-", slug_text)
            tag_slug = f"{slug_text}-movies-{tag_id}"
            tag_url = f"{self.BASE_URL}{lang_prefix}/tags/{tab_slug}/{tag_slug}"

            result.append({
                "tag_name": tag_text,
                "tag_slug": tag_slug,
                "tag_url": tag_url,
            })

        logger.info(f"Tab '{tab_name}' API 解析到 {len(result)} 个子分类标签")
        return result

    def parse_api_list_page(
        self, data: Dict[str, Any], language: str
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        解析列表页 API 数据（_next/data/.../tags/.../...json）

        Args:
            data: API 返回的 pageProps 字典
            language: 语言代码

        Returns:
            (dramas, total_pages) 元组
            dramas 每项包含 detail_url、series_title、play_count_raw、
                           play_count、favorite_count_raw、favorite_count
        """
        tag_books = data.get("tagBooks", {})
        books = tag_books.get("books", [])
        total_pages = data.get("totalPage", 1)

        dramas = []
        for book in books:
            book_id = book.get("book_id", "")
            title = book.get("book_title", "")
            if not book_id or not title:
                continue

            # detail_url 用 book_id 构造，作为数据库唯一标识
            # 详情 API 请求时直接用 book_id，由 api_client 内部跟随 __N_REDIRECT 获取正确 slug
            detail_url = f"{self.BASE_URL}/{language}/movie/{book_id}"

            read_count = book.get("read_count") or 0
            collect_count = book.get("collect_count") or 0
            synopsis = (book.get("special_desc") or "").strip()

            # t_book_id：平台内置全局排序序号，直接存原始值（如 200000000000000016）
            t_book_id = book.get("t_book_id")

            dramas.append({
                "detail_url": detail_url,
                "book_id": book_id,
                "t_book_id": t_book_id,
                "series_title": title,
                "play_count_raw": self._format_count(read_count) if read_count else "",
                "play_count": read_count if read_count else None,
                "favorite_count_raw": self._format_count(collect_count) if collect_count else "",
                "favorite_count": collect_count if collect_count else None,
                "synopsis": synopsis,
            })

        logger.debug(f"API 列表页解析到 {len(dramas)} 部剧集，共 {total_pages} 页")
        return dramas, total_pages

    def parse_api_drama_detail(
        self, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        解析详情页 API 数据（_next/data/.../movie/...json 的 pageProps.data）

        tag_list 里每项含 category_id，映射关系：
          1001 → Actors
          1005 → Actresses
          1020 → Identities
          1022 / 1010001 → Story Beats
          其余 → genre_tags

        Args:
            data: API 返回的 pageProps.data 字典

        Returns:
            与 parse_drama_detail 格式兼容的字典
        """
        tag_list_raw = data.get("tag_list", [])
        synopsis = (data.get("special_desc") or "").strip()
        read_count = data.get("read_count") or 0
        collect_count = data.get("collect_count") or 0

        # 提取标签文本列表
        tag_list = [t.get("text", "") for t in tag_list_raw if t.get("text")]

        # 按 category_id 分类
        actors_tags, actresses_tags, identity_tags, story_beat_tags, genre_tags = [], [], [], [], []
        for t in tag_list_raw:
            text = t.get("text", "")
            if not text:
                continue
            cat = str(t.get("category_id", ""))
            if cat == "1001":
                actors_tags.append(text)
            elif cat == "1005":
                actresses_tags.append(text)
            elif cat == "1020":
                identity_tags.append(text)
            elif cat in ("1022", "1010001"):
                story_beat_tags.append(text)
            else:
                genre_tags.append(text)

        return {
            "tag_list": tag_list,
            "actors_tags": actors_tags,
            "actresses_tags": actresses_tags,
            "identity_tags": identity_tags,
            "story_beat_tags": story_beat_tags,
            "genre_tags": genre_tags,
            "synopsis": synopsis,
            "play_count_raw": self._format_count(read_count) if read_count else "",
            "play_count": read_count if read_count else None,
            "favorite_count_raw": self._format_count(collect_count) if collect_count else "",
            "favorite_count": collect_count if collect_count else None,
        }

    @staticmethod
    def _format_count(n: int) -> str:
        """
        将整数播放量/收藏量格式化为带单位的字符串

        规则：
          >= 1,000,000 → 保留一位小数 + M（如 1982591 → 1.9M）
          >= 1,000     → 保留一位小数 + K（如 28203 → 28.2K）
          < 1,000      → 直接返回数字字符串

        小数部分若为 .0 则省略（如 2000000 → 2M，而非 2.0M）。
        """
        if n >= 1_000_000:
            val = round(n / 1_000_000, 1)
            return f"{val:g}M"
        if n >= 1_000:
            val = round(n / 1_000, 1)
            return f"{val:g}K"
        return str(n)

