#!/usr/bin/env python3
"""
解析器基类
提供通用的页面解析功能和工具
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup, Tag
from loguru import logger


class BasePageParser(ABC):
    """
    页面解析器基类

    提供通用的 HTML 解析方法和工具
    """

    def __init__(self):
        self._soup: Optional[BeautifulSoup] = None

    def set_html(self, html: str) -> None:
        """设置 HTML 内容"""
        self._soup = BeautifulSoup(html, 'lxml')

    def get_soup(self) -> Optional[BeautifulSoup]:
        """获取 BeautifulSoup 对象"""
        return self._soup

    # ==================== 抽象方法（必须实现） ====================

    @abstractmethod
    def parse_list_page(self) -> List[Dict[str, Any]]:
        """
        解析列表页

        Returns:
            数据列表
        """
        pass

    @abstractmethod
    def parse_detail_page(self, item_id: str) -> Dict[str, Any]:
        """
        解析详情页

        Args:
            item_id: 数据项 ID

        Returns:
            单个数据项
        """
        pass

    # ==================== 通用工具方法 ====================

    def extract_text(
        self,
        selector: str,
        default: str = "",
        strip: bool = True
    ) -> str:
        """
        提取文本

        Args:
            selector: CSS 选择器
            default: 默认值
            strip: 是否去除首尾空格

        Returns:
            文本内容
        """
        if not self._soup:
            return default

        try:
            element = self._soup.select_one(selector)
            if element:
                text = element.get_text()
                return text.strip() if strip else text
            return default
        except Exception as e:
            logger.debug(f"提取文本失败 [{selector}]: {e}")
            return default

    def extract_attr(
        self,
        selector: str,
        attr_name: str,
        default: str = ""
    ) -> str:
        """
        提取属性

        Args:
            selector: CSS 选择器
            attr_name: 属性名
            default: 默认值

        Returns:
            属性值
        """
        if not self._soup:
            return default

        try:
            element = self._soup.select_one(selector)
            if element and element.has_attr(attr_name):
                value = element[attr_name]
                return value if isinstance(value, str) else value[0]
            return default
        except Exception as e:
            logger.debug(f"提取属性失败 [{selector}@{attr_name}]: {e}")
            return default

    def extract_all_text(
        self,
        selector: str,
        default: List[str] = None
    ) -> List[str]:
        """
        提取多个文本

        Args:
            selector: CSS 选择器
            default: 默认列表

        Returns:
            文本列表
        """
        if not self._soup:
            return default or []

        try:
            elements = self._soup.select(selector)
            return [el.get_text().strip() for el in elements if el.get_text().strip()]
        except Exception as e:
            logger.debug(f"提取文本列表失败 [{selector}]: {e}")
            return default or []

    def extract_all_items(
        self,
        container_selector: str,
        field_selectors: Dict[str, str]
    ) -> List[Dict[str, str]]:
        """
        提取多个数据项

        Args:
            container_selector: 容器选择器
            field_selectors: 字段选择器映射

        Returns:
            数据项列表
        """
        if not self._soup:
            return []

        results = []

        try:
            containers = self._soup.select(container_selector)
            for container in containers:
                item = {}
                for field, selector in field_selectors.items():
                    # 支持相对选择器
                    if selector.startswith('.'):
                        el = container.select_one(selector)
                    else:
                        el = self._soup.select_one(f"{selector}")

                    if el:
                        item[field] = el.get_text().strip()
                    else:
                        item[field] = ""

                if item:
                    results.append(item)

        except Exception as e:
            logger.debug(f"提取数据项失败：{e}")

        return results

    def extract_links(
        self,
        selector: str = "a",
        text_filter: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        提取链接

        Args:
            selector: CSS 选择器
            text_filter: 文本过滤

        Returns:
            链接列表 [{text, href}]
        """
        if not self._soup:
            return []

        results = []

        try:
            elements = self._soup.select(selector)
            for el in elements:
                href = el.get("href", "")
                text = el.get_text().strip()

                if text_filter and text_filter not in text:
                    continue

                if href and text:
                    results.append({"text": text, "href": href})

        except Exception as e:
            logger.debug(f"提取链接失败：{e}")

        return results

    def clean_html(self, html: str) -> str:
        """
        清理 HTML（移除脚本、样式等）

        Args:
            html: HTML 内容

        Returns:
            清理后的 HTML
        """
        try:
            soup = BeautifulSoup(html, 'lxml')

            # 移除不需要的标签
            for tag in soup(["script", "style", "noscript", "iframe"]):
                tag.decompose()

            return str(soup)
        except Exception as e:
            logger.error(f"清理 HTML 失败：{e}")
            return html

    def normalize_text(self, text: str) -> str:
        """
        规范化文本（移除多余空白、统一换行等）

        Args:
            text: 原始文本

        Returns:
            规范化后的文本
        """
        if not text:
            return ""

        import re
        # 移除多余空白
        text = re.sub(r'\s+', ' ', text)
        # 统一换行
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()


__all__ = [
    "BasePageParser",
]
