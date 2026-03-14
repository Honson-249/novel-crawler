#!/usr/bin/env python3
"""
页面解析模块 - 负责解析 HTML 页面内容
"""
import re
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
from loguru import logger


class PageParser:
    """页面解析器 - 负责解析 HTML 页面内容"""

    def __init__(self, font_mapper=None):
        self.font_mapper = font_mapper

    def set_font_mapper(self, font_mapper):
        """设置字体映射器"""
        self.font_mapper = font_mapper

    def parse_rank_categories(self, html: str) -> List[Dict[str, str]]:
        """解析榜单首页，获取分类列表"""
        soup = BeautifulSoup(html, "lxml")

        categories = []
        rank_links = soup.find_all("a", href=re.compile(r"/rank/\d+_\d+_\d+"))

        for link in rank_links:
            href = link.get("href", "")
            match = re.search(r"/rank/(\d+)_(\d+)_(\d+)", href)
            if match:
                gender_id, board_type, cat_id = match.groups()
                cat_name_raw = link.get_text(strip=True)

                # 使用字体映射解码分类名称
                cat_name = self.font_mapper.decode_text(cat_name_raw) if self.font_mapper else cat_name_raw

                if cat_name and len(cat_name) < 20:
                    categories.append({
                        "gender_id": gender_id,
                        "board_type": board_type,
                        "cat_id": cat_id,
                        "cat_name": cat_name,
                        "href": href,
                    })

        return categories

    async def parse_book_item(self, item, category: Dict, board_name: str, batch_date: str) -> Optional[Dict[str, Any]]:
        """解析单个书籍项"""
        try:
            # 排名 - 从 .book-item-index > h1 获取
            rank_num = 0
            rank_container = item.find(class_="book-item-index")
            if rank_container:
                rank_elem = rank_container.find("h1")
                if rank_elem:
                    rank_text = rank_elem.get_text(strip=True).strip()
                    match = re.search(r'\d+', rank_text)
                    if match:
                        rank_num = int(match.group())

            # 书名
            book_name_elem = item.find(class_="title")
            book_title_raw = book_name_elem.get_text(strip=True) if book_name_elem else ""
            book_title = self.font_mapper.decode_text(book_title_raw) if self.font_mapper else book_title_raw

            # 作者
            author_elem = item.find(class_="author")
            author_raw = author_elem.get_text(strip=True) if author_elem else ""
            author_name = self.font_mapper.decode_text(author_raw) if self.font_mapper else author_raw

            # 热度/指标
            footer_elem = item.find(class_="book-item-footer")
            metric_name = "在读人数"
            metric_value_raw = ""
            metric_value = 0

            if footer_elem:
                footer_text = footer_elem.get_text(strip=True)
                # 使用 font_mapper 解码文本（处理字体加密）
                footer_text_decoded = self.font_mapper.decode_text(footer_text) if self.font_mapper else footer_text

                if '热力' in footer_text_decoded:
                    metric_name = "热力值"
                elif '在读' in footer_text_decoded:
                    metric_name = "在读人数"

                # 提取完整的数值（包括万/亿单位）
                match = re.search(r"(\d+(?:\.\d+)?)\s*([万亿])?", footer_text_decoded)
                if match:
                    value_str = match.group(1)
                    unit = match.group(2) or ""
                    metric_value_raw = value_str + unit  # 保存完整的原始值，如 "50 万"

                    # 转换为实际数值
                    if unit == '亿':
                        metric_value = int(float(value_str) * 100000000)
                    elif unit == '万':
                        metric_value = int(float(value_str) * 10000)
                    else:
                        metric_value = int(float(value_str))

            # Book ID
            link = item.find("a", href=True)
            book_id = ""
            detail_url = ""
            if link:
                href = link["href"]
                match = re.search(r"/page/(\d+)", href)
                if match:
                    book_id = match.group(1)
                    detail_url = f"https://fanqienovel.com/page/{book_id}"

            # 封面
            img = item.find("img")
            cover_url = img.get("src") or img.get("data-src") or "" if img else ""

            # 简介 - 从榜单页的 .desc.abstract 获取
            synopsis = ""
            desc_elem = item.find(class_="desc")
            if desc_elem:
                synopsis_raw = desc_elem.get_text(strip=True)[:500]
                synopsis = self.font_mapper.decode_text(synopsis_raw) if self.font_mapper else synopsis_raw

            # 书籍更新时间 - 从榜单页右侧单独的时间元素获取
            book_update_time = None
            book_status = "连载中"

            # 尝试从 item 中查找时间格式的元素
            # 优先查找 class 包含 "update" 或 "time" 的元素
            for class_name in ['update-time', 'update', 'time', 'gmt']:
                time_elem = item.find(class_=lambda c: c and class_name in c.lower() if c else False)
                if time_elem:
                    text = time_elem.get_text(strip=True)
                    text = self.font_mapper.decode_text(text) if self.font_mapper else text
                    if re.match(r'\d{4}-\d{2}-\d{2}', text):
                        book_update_time = text
                        break

            # 如果没找到，尝试从所有 span 元素中提取
            if not book_update_time:
                for span in item.find_all('span'):
                    text = span.get_text(strip=True)
                    text = self.font_mapper.decode_text(text) if self.font_mapper else text
                    if re.match(r'^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2})?$', text):
                        book_update_time = text
                        break

            # 如果还是没找到，尝试从整个 item 文本中提取最后一个时间格式
            if not book_update_time:
                item_text = item.get_text(strip=True)
                item_text = self.font_mapper.decode_text(item_text) if self.font_mapper else item_text
                time_matches = re.findall(r'\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?', item_text)
                if time_matches:
                    # 取最后一个时间（通常是更新时间）
                    book_update_time = time_matches[-1]

            # 提取状态：完结/连载中
            item_text = item.get_text(strip=True)
            item_text = self.font_mapper.decode_text(item_text) if self.font_mapper else item_text
            if '完结' in item_text or '(完)' in item_text:
                book_status = "已完结"

            # 调试日志
            if not book_update_time:
                logger.warning(f"书籍 {book_id} ({book_title}) 未解析到更新时间")
            else:
                logger.debug(f"书籍 {book_id} 更新时间：{book_update_time}")

            return {
                "batch_date": batch_date,
                "board_name": board_name,
                "sub_category": category['cat_name'],
                "rank_num": rank_num,
                "book_id": book_id,
                "book_title": book_title,
                "author_name": author_name,
                "metric_name": metric_name,
                "metric_value_raw": metric_value_raw,
                "metric_value": metric_value,
                "book_status": book_status,
                "synopsis": synopsis,
                "chapter_list_json": None,
                "cover_url": cover_url,
                "detail_url": detail_url,
                "tags": None,
                "book_update_time": book_update_time,
            }

        except Exception as e:
            logger.error(f"解析书籍失败：{e}")
            return None

    def parse_book_detail(self, html: str) -> Dict[str, Any]:
        """解析书籍详情页，获取章节列表和状态"""
        soup = BeautifulSoup(html, "lxml")
        result = {
            "book_status": "连载中",
            "chapter_list": [],
            "detail_update_time": None,
        }

        # 解析状态
        for elem in soup.find_all(["span", "div"]):
            text = elem.get_text(strip=True)
            if "已完结" in text:
                result["book_status"] = "已完结"
                break
            elif "连载中" in text:
                result["book_status"] = "连载中"
                break

        # 解析最后更新时间
        for elem in soup.find_all(["span", "div"]):
            text = elem.get_text(strip=True)
            time_match = re.search(
                r'(?:最后更新 | 更新 | 更新时间)[：:]\s*(\d{4}-\d{2}-\d{2})\s*(\d{2}:\d{2})?',
                text
            )
            if time_match:
                date_part = time_match.group(1)
                time_part = time_match.group(2)
                if time_part:
                    result["detail_update_time"] = f"{date_part} {time_part}"
                else:
                    result["detail_update_time"] = date_part
                break

        # 解析章节列表
        chapter_area = soup.find(class_=re.compile(r'chapter.*list|chapter.*wrap|scroll.*wrap', re.I))
        if chapter_area:
            chapter_elems = chapter_area.find_all("a")
        else:
            chapter_elems = soup.select('.chapter-list a, [class*="chapter"] a, [class*="Chapter"] a')

        for chap in chapter_elems:
            text = chap.get_text(strip=True)
            if text and len(text) < 150:
                result["chapter_list"].append(text)

        return result
