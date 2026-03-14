#!/usr/bin/env python3
"""测试单本小说爬取并入库 - MySQL 版本"""

import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import sys
import re
from pathlib import Path

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from pipeline.font_mapper import get_mapper
from pipeline.storage import db_manager


class BookDatabaseTester:
    """单本小说数据库测试器 - MySQL 版本"""

    def __init__(self):
        self.font_mapper = get_mapper()
        self.base_url = "https://fanqienovel.com"
        self.browser = None
        self.batch_date = datetime.now().strftime("%Y-%m-%d")

    async def init_browser(self):
        """初始化浏览器"""
        p = await async_playwright().start()
        self.browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        return context

    def _clean_metric_value(self, raw_value: str) -> int:
        """清洗热度值：删除前缀，提取数字并依据单位转化为具体数值"""
        if not raw_value:
            return 0

        # 提取数字和单位
        match = re.search(r'([\d.]+)\s*(万 | 亿)?', raw_value)
        if not match:
            return 0

        value = float(match.group(1))
        unit = match.group(2) or ''

        if unit == '亿':
            return int(value * 100000000)
        elif unit == '万':
            return int(value * 10000)
        else:
            return int(value)

    def _extract_initial_state(self, html: str) -> dict:
        """从榜单页提取 __INITIAL_STATE__ 中的书籍数据"""
        result = {}
        try:
            soup = BeautifulSoup(html, "lxml")
            for script in soup.find_all("script"):
                text = script.get_text()
                if "__INITIAL_STATE__" in text:
                    book_ids = re.findall(r'"bookId"[:\s]*["\']?(\d+)["\']?', text)
                    abstracts = re.findall(r'"abstract"[:\s]*"((?:[^"\\]|\\.)*)"', text)
                    word_numbers = re.findall(r'"wordNumber"[:\s]*"?(\d+)"?', text)

                    for i, book_id in enumerate(book_ids):
                        abstract = abstracts[i] if i < len(abstracts) else None
                        word_num = word_numbers[i] if i < len(word_numbers) else None
                        if abstract:
                            abstract = self.font_mapper.decode_text(abstract.replace('\\n', '\n'))
                        result[book_id] = {
                            "abstract": abstract,
                            "wordNumber": word_num,
                        }
                    break
        except Exception as e:
            print(f"提取 __INITIAL_STATE__ 失败：{e}")
        return result

    async def test_and_store_book(self):
        """测试爬取第一本小说并存入 MySQL 数据库"""
        print("=" * 60)
        print("开始测试单本小说爬取并入库 (MySQL)")
        print("=" * 60)

        context = await self.init_browser()
        page = await context.new_page()

        # 测试榜单：男频玄幻脑洞阅读榜
        board_name = "男频阅读榜"
        sub_category = "玄幻脑洞"
        test_url = "https://fanqienovel.com/rank/1_1_257"

        print(f"\n访问榜单页：{test_url}")
        print(f"榜单名称：{board_name}")
        print(f"子分类：{sub_category}")

        await page.goto(test_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)

        html = await page.content()
        print(f"榜单页 HTML 长度：{len(html)} 字符")

        # 提取 __INITIAL_STATE__ 数据
        initial_state = self._extract_initial_state(html)

        # 解析榜单页
        soup = BeautifulSoup(html, "lxml")
        book_items = soup.find_all(class_="rank-book-item")
        print(f"找到书籍项数：{len(book_items)}")

        if not book_items:
            print("未找到任何书籍")
            await self.browser.close()
            return None

        # 测试第一本
        item = book_items[0]

        # 提取排名（去除前导零）
        rank_elem = item.find(class_="num")
        rank_num_raw = rank_elem.get_text(strip=True) if rank_elem else "1"
        rank_num = int(rank_num_raw.lstrip('0') or '0')

        # 书名
        book_name_elem = item.find(class_="title")
        book_name_raw = book_name_elem.get_text(strip=True) if book_name_elem else ""
        book_name = self.font_mapper.decode_text(book_name_raw)

        # 作者
        author_elem = item.find(class_="author")
        author_raw = author_elem.get_text(strip=True) if author_elem else ""
        author = self.font_mapper.decode_text(author_raw)

        # 热度指标
        footer_elem = item.find(class_="book-item-footer")
        metric_name = "在读人数"  # 默认
        metric_value_raw = ""

        if footer_elem:
            footer_text = footer_elem.get_text(strip=True)
            if '热力' in footer_text:
                metric_name = "热力值"
            elif '在读' in footer_text:
                metric_name = "在读人数"
            else:
                metric_name = "热度"

            # 提取完整的数值（包括万/亿单位）
            heat_match = re.search(r"([\d.]+)\s*(万 | 亿)?", footer_text)
            if heat_match:
                value_str = heat_match.group(1)
                unit = heat_match.group(2) or ""
                metric_value_raw = value_str + unit  # 保存完整的原始值，如 "50 万"

        # Book ID
        link = item.find("a", href=True)
        book_id = ""
        if link:
            href = link["href"]
            match = re.search(r"/page/(\d+)", href)
            if match:
                book_id = match.group(1)

        # 封面 URL
        img = item.find("img")
        cover_url = ""
        if img:
            cover_url = img.get("src") or img.get("data-src") or ""

        # 详情页 URL
        detail_url = f"{self.base_url}/page/{book_id}" if book_id else ""

        # 从 __INITIAL_STATE__ 获取简介
        synopsis = ""
        if book_id and book_id in initial_state:
            synopsis = initial_state[book_id].get("abstract") or ""

        print(f"\n第一本小说数据:")
        print(f"  排名：{rank_num}")
        print(f"  书名：{book_name}")
        print(f"  作者：{author}")
        print(f"  指标：{metric_name}")
        print(f"  原始值：{metric_value_raw}")

        # 访问详情页获取更多信息
        if book_id:
            print(f"\n访问详情页：{detail_url}")
            await page.goto(detail_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)

            detail_html = await page.content()
            print(f"详情页 HTML 长度：{len(detail_html)} 字符")

            # 解析详情页
            detail_soup = BeautifulSoup(detail_html, "lxml")

            # 状态
            book_status = "连载中"  # 默认
            for elem in detail_soup.find_all(["span", "div"]):
                text = elem.get_text(strip=True)
                if "已完结" in text:
                    book_status = "已完结"
                    break
                elif "连载中" in text:
                    book_status = "连载中"
                    break

            # 如果没有从榜单页获取简介，尝试从详情页获取
            if not synopsis:
                for elem in detail_soup.find_all(["div", "p"]):
                    text = elem.get_text(strip=True)
                    if len(text) > 50 and len(text) < 500:
                        synopsis = text[:300]
                        break

            # 章节列表
            chapter_list = []
            chapter_selectors = ['[class*="chapter"] a', '[class*="Chapter"] a', '[class*="dir"] a']
            for sel in chapter_selectors:
                chapter_elems = detail_soup.select(sel)
                for chap in chapter_elems[:30]:  # 取前 30 章
                    text = chap.get_text(strip=True)
                    if text and len(text) < 100:
                        chapter_list.append(text)
                if chapter_list:
                    break

            print(f"\n详情页数据:")
            print(f"  状态：{book_status}")
            print(f"  章节数：{len(chapter_list)}")
            if synopsis:
                print(f"  简介：{synopsis[:100]}...")

            # 构建符合 OCR 规范的记录
            metric_value = self._clean_metric_value(metric_value_raw)

            record = {
                "batch_date": self.batch_date,
                "board_name": board_name,
                "sub_category": sub_category,
                "rank_num": rank_num,
                "book_title": book_name,
                "author_name": author,
                "metric_name": metric_name,
                "metric_value_raw": metric_value_raw,
                "metric_value": metric_value,
                "book_status": book_status,
                "synopsis": synopsis,
                "chapter_list_json": json.dumps(chapter_list, ensure_ascii=False) if chapter_list else None,
                "book_id": book_id,
                "cover_url": cover_url,
                "detail_url": detail_url,
                "tags": None,
            }

            # 打印完整记录
            print("\n" + "=" * 60)
            print("完整数据记录:")
            print("=" * 60)
            for key, value in record.items():
                if key == "chapter_list_json" and value:
                    chapters = json.loads(value)
                    print(f"  {key}: {len(chapters)} 个章节")
                elif key == "synopsis" and value:
                    print(f"  {key}: {value[:100]}...")
                elif isinstance(value, str) and value and len(value) > 50:
                    print(f"  {key}: {value[:50]}...")
                else:
                    print(f"  {key}: {value}")

            # 入库
            print("\n" + "=" * 60)
            print("数据入库 (MySQL)...")
            print("=" * 60)

            # 创建表
            db_manager().create_tables()

            # 插入数据
            inserted = db_manager().insert_fanqie_batch([record], self.batch_date)
            print(f"成功插入/更新 {inserted} 条记录")

            # 保存测试数据到 JSON
            with open("data/test_book_mysql.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            print(f"数据已保存：data/test_book_mysql.json")

        await page.close()
        await self.browser.close()

        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)

        return record


async def main():
    tester = BookDatabaseTester()
    await tester.test_and_store_book()


if __name__ == "__main__":
    asyncio.run(main())
