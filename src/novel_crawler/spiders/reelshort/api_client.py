"""
ReelShort Next.js API 客户端

ReelShort 基于 Next.js SSR，所有页面数据均可通过 /_next/data/{build_id}/... 接口以
JSON 形式获取，无需 Playwright 渲染，速度快 10 倍以上，且不触发 CloudFront 限流。

接口格式：
  列表页：/_next/data/{build_id}/{lang}/tags/{tab-slug}/{tag-slug}/{page}.json
  详情页：/_next/data/{build_id}/{lang}/movie/{slug}.json?slug={slug}

build_id 从首页 HTML 的 <script id="__NEXT_DATA__"> 提取，每次部署会变，启动时获取一次。
"""
import re
import json
import asyncio
import random
from typing import Any, Dict, List, Optional, Tuple

import httpx
from loguru import logger


# 请求头，模拟浏览器
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.reelshort.com/",
}

BASE_URL = "https://www.reelshort.com"


class ReelShortApiClient:
    """
    ReelShort Next.js 数据 API 客户端

    使用 httpx 异步 HTTP 客户端，复用连接池，支持自动重试。
    build_id 懒加载，首次请求时自动从首页获取。
    """

    def __init__(self, delay_min: float = 0.3, delay_max: float = 0.8):
        """
        Args:
            delay_min: 请求间最小延迟（秒）
            delay_max: 请求间最大延迟（秒）
        """
        self._build_id: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None
        self.delay_min = delay_min
        self.delay_max = delay_max
        # slug 缓存：{language:book_id → real_slug}
        # 同一部剧在不同 Tab 下重复出现时，直接用缓存 slug，省掉第一次 book_id 请求
        self._slug_cache: Dict[str, str] = {}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            headers=_HEADERS,
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def get_build_id(self) -> str:
        """
        从首页 HTML 提取 Next.js build_id

        build_id 存在于 <script id="__NEXT_DATA__"> 的 JSON 中：
        {"props":...,"buildId":"dc4ea06",...}
        """
        if self._build_id:
            return self._build_id

        logger.info("[API] 获取 Next.js build_id...")
        resp = await self._client.get(BASE_URL + "/", headers={**_HEADERS, "Accept": "text/html"})
        resp.raise_for_status()

        match = re.search(r'"buildId"\s*:\s*"([^"]+)"', resp.text)
        if not match:
            raise RuntimeError("无法从首页提取 build_id，页面结构可能已变更")

        self._build_id = match.group(1)
        logger.info(f"[API] build_id = {self._build_id}")
        return self._build_id

    async def fetch_tab_index(
        self,
        language: str,
        tab_slug: str,
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        获取 Tab 入口页数据，包含该 Tab 下所有子分类标签列表

        返回的 pageProps.tags 字段按 category_id 分组，包含完整标签信息。

        Args:
            language: 语言代码
            tab_slug: Tab slug，如 movie-actors

        Returns:
            pageProps 字典，包含 tags 字段
        """
        build_id = await self.get_build_id()
        lang_prefix = f"/{language}"
        url = f"{BASE_URL}/_next/data/{build_id}{lang_prefix}/tags/{tab_slug}.json?slug={tab_slug}"
        return await self._get_json(url, max_retries)

    async def fetch_tab_total_list(
        self,
        language: str,
        tab_slug: str,
        page: int = 1,
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        获取 Tab 总列表分页数据（不按子 tag 过滤，直接返回该 Tab 下全量剧集）

        URL 格式：
          第 1 页：/{lang}/tags/{tab_slug}.json?slug={tab_slug}
          第 N 页：/{lang}/tags/{tab_slug}/{N}.json?slug={tab_slug}&slug={N}

        返回结构与 fetch_tag_list_page 完全一致（tagBooks.books、totalPage 等），
        books 中每项包含 t_book_id 字段，可用于还原平台内置排序。

        Args:
            language: 语言代码
            tab_slug: Tab slug，如 story-beats
            page: 页码（从 1 开始）
            max_retries: 最大重试次数

        Returns:
            pageProps 字典，包含 tagBooks.books、total、totalPage 等
        """
        build_id = await self.get_build_id()
        lang_prefix = f"/{language}"

        if page == 1:
            path = f"{lang_prefix}/tags/{tab_slug}"
            query = f"slug={tab_slug}"
        else:
            path = f"{lang_prefix}/tags/{tab_slug}/{page}"
            query = f"slug={tab_slug}&slug={page}"

        url = f"{BASE_URL}/_next/data/{build_id}{path}.json?{query}"
        return await self._get_json(url, max_retries)

    async def fetch_tag_list_page(
        self,
        language: str,
        tab_slug: str,
        tag_slug: str,
        page: int = 1,
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        获取标签列表页数据

        Args:
            language: 语言代码（所有语言含 en 均带 /{language}/ 前缀）
            tab_slug: Tab slug，如 movie-identities
            tag_slug: 标签 slug，如 yerno-movies-676d210e4582b53a14081ab1
            page: 页码（从 1 开始）
            max_retries: 最大重试次数

        Returns:
            pageProps 字典，包含 tagBooks.books、total、totalPage 等
        """
        build_id = await self.get_build_id()
        lang_prefix = f"/{language}"

        if page == 1:
            path = f"{lang_prefix}/tags/{tab_slug}/{tag_slug}"
        else:
            path = f"{lang_prefix}/tags/{tab_slug}/{tag_slug}/{page}"

        # query 参数：slug 数组
        slug_parts = [tab_slug, tag_slug]
        if page > 1:
            slug_parts.append(str(page))
        query = "&".join(f"slug={s}" for s in slug_parts)

        url = f"{BASE_URL}/_next/data/{build_id}{path}.json?{query}"
        return await self._get_json(url, max_retries)

    async def fetch_drama_detail(
        self,
        language: str,
        book_id: str,
        max_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """
        获取剧集详情页数据

        直接用 book_id 请求，服务端会通过 __N_REDIRECT 返回正确的完整 slug，
        再跟随重定向用正确 slug 请求一次，避免自行构造 slug 的各种语言差异问题。

        Args:
            language: 语言代码
            book_id: 剧集唯一 ID（24位十六进制）
            max_retries: 最大重试次数

        Returns:
            pageProps.data 字典，包含 tag_list、special_desc、read_count 等
        """
        build_id = await self.get_build_id()
        lang_prefix = f"/{language}"
        cache_key = f"{language}:{book_id}"

        # 优先使用缓存的 real_slug，跳过第一次 book_id 请求
        cached_slug = self._slug_cache.get(cache_key)
        if cached_slug:
            logger.debug(f"[API] slug 缓存命中：{book_id} → {cached_slug}")
            url = f"{BASE_URL}/_next/data/{build_id}{lang_prefix}/movie/{cached_slug}.json?slug={cached_slug}"
            data = await self._get_json(url, max_retries)
            if data and "data" in data:
                return data.get("data")
            # 缓存 slug 失效（build_id 更新等），清除后走正常流程
            logger.debug(f"[API] 缓存 slug 请求失败，清除缓存重试：{cached_slug}")
            del self._slug_cache[cache_key]

        # 第一次：用 book_id 请求，预期服务端返回 __N_REDIRECT 指向正确 slug
        url = f"{BASE_URL}/_next/data/{build_id}{lang_prefix}/movie/{book_id}.json?slug={book_id}"
        data = await self._get_json(url, max_retries)
        if data is None:
            return None

        # 直接命中（极少数情况，book_id 本身就是有效 slug）
        if "data" in data:
            return data.get("data")

        # 跟随 __N_REDIRECT，用服务端给出的正确 slug 重新请求
        if "__N_REDIRECT" in data:
            redirect_path = data["__N_REDIRECT"]  # 如 /en/movie/the-ceo-s-contract-wife-{id}
            if "/movie/" not in redirect_path:
                logger.debug(f"[API] 重定向路径无 /movie/，跳过：{redirect_path}")
                return None

            real_slug = redirect_path.rstrip("/").split("/movie/")[-1]
            # 提取重定向目标语言（路径首段，如 /en/movie/... 中的 en）
            path_parts = redirect_path.strip("/").split("/")
            real_lang = path_parts[0] if len(path_parts) >= 2 and path_parts[1] == "movie" else language

            # 大小写不敏感比较（zh-TW 和 zh-tw 视为同一语言）
            if real_lang.lower() != language.lower():
                # 重定向到不同语言，说明该语言下此剧不存在
                logger.debug(f"[API] 该语言无此剧，重定向至 {real_lang}：{redirect_path}")
                return None
            # 统一用 real_lang 构造 URL（保持和服务端一致的大小写，如 zh-TW）
            # language 参数可能是 zh-tw，但服务端重定向给的是 zh-TW

            # 缓存 real_slug，同一部剧下次直接用
            self._slug_cache[cache_key] = real_slug
            logger.debug(f"[API] 跟随重定向并缓存：{book_id} → {real_slug}")
            url2 = f"{BASE_URL}/_next/data/{build_id}/{real_lang}/movie/{real_slug}.json?slug={real_slug}"
            data2 = await self._get_json(url2, max_retries)
            if data2 and "data" in data2:
                return data2.get("data")

        return None

    async def _get_json(
        self,
        url: str,
        max_retries: int = 3,
        retry_count: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """
        发送 GET 请求并解析 JSON，自动重试

        遇到 403/429 时等待更长时间再重试；build_id 过期（404）时重新获取。
        """
        try:
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            resp = await self._client.get(url)

            if resp.status_code == 404:
                # build_id 可能已过期，重新获取后重试一次
                if retry_count == 0:
                    logger.warning(f"[API] 404，尝试刷新 build_id 重试：{url}")
                    self._build_id = None
                    await self.get_build_id()
                    # 用新 build_id 重建 URL
                    new_url = re.sub(
                        r"/_next/data/[^/]+/",
                        f"/_next/data/{self._build_id}/",
                        url,
                    )
                    return await self._get_json(new_url, max_retries, retry_count + 1)
                logger.error(f"[API] 404 持续，放弃：{url}")
                return None

            if resp.status_code in (403, 429):
                if retry_count < max_retries:
                    wait = random.uniform(5.0, 10.0) * (retry_count + 1)
                    logger.warning(f"[API] {resp.status_code} 限流，等待 {wait:.1f}s 重试（第 {retry_count + 1} 次）")
                    await asyncio.sleep(wait)
                    return await self._get_json(url, max_retries, retry_count + 1)
                logger.error(f"[API] {resp.status_code} 已达最大重试次数，放弃：{url}")
                return None

            resp.raise_for_status()
            data = resp.json()
            return data.get("pageProps", data)

        except httpx.TimeoutException:
            if retry_count < max_retries:
                wait = random.uniform(2.0, 4.0)
                logger.warning(f"[API] 超时，等待 {wait:.1f}s 重试（第 {retry_count + 1} 次）：{url}")
                await asyncio.sleep(wait)
                return await self._get_json(url, max_retries, retry_count + 1)
            logger.error(f"[API] 超时已达最大重试次数，放弃：{url}")
            return None

        except Exception as e:
            logger.error(f"[API] 请求失败：{url}，错误：{e}")
            return None

    @staticmethod
    def extract_book_id_from_url(detail_url: str) -> Optional[str]:
        """
        从详情页 URL 提取 book_id（URL 最后一段，24位十六进制）

        示例：
          https://www.reelshort.com/es/movie/la-venganza-del-yerno-64475486a7476038d06b044a
          → 64475486a7476038d06b044a
          https://www.reelshort.com/es/movie/64475486a7476038d06b044a
          → 64475486a7476038d06b044a
        """
        match = re.search(r"([0-9a-f]{24})(?:[/?#]|$)", detail_url)
        return match.group(1) if match else None
