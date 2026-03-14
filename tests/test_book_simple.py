#!/usr/bin/env python3
"""简化版单本小说测试 - 无 emoji"""

import asyncio
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import sys
from pathlib import Path

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from pipeline.font_mapper import get_mapper


class BookTester:
    def __init__(self):
        self.font_mapper = get_mapper()
        self.base_url = "https://fanqienovel.com"
        self.browser = None

    async def init_browser(self):
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

    async def test_book(self):
        print("=" * 60)
        print("开始测试单本小说爬取")
        print("=" * 60)

        context = await self.init_browser()
        page = await context.new_page()

        # 测试书籍：从男频玄幻脑洞阅读榜获取第一本
        test_url = "https://fanqienovel.com/rank/1_1_257"

        print(f"\n访问榜单页：{test_url}")
        await page.goto(test_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)

        html = await page.content()
        print(f"榜单页 HTML 长度：{len(html)} 字符")

        # 解析
        soup = BeautifulSoup(html, "lxml")
        book_items = soup.find_all(class_="rank-book-item")
        print(f"找到书籍项数：{len(book_items)}")

        if not book_items:
            print("未找到任何书籍")
            await self.browser.close()
            return

        # 测试第一本
        item = book_items[0]

        # 书名
        book_name_elem = item.find(class_="title")
        book_title_raw = book_name_elem.get_text(strip=True) if book_name_elem else ""
        book_title = self.font_mapper.decode_text(book_title_raw)

        # 作者
        author_elem = item.find(class_="author")
        author_raw = author_elem.get_text(strip=True) if author_elem else ""
        author = self.font_mapper.decode_text(author_raw)

        # 热度
        footer_elem = item.find(class_="book-item-footer")
        heat_display = ""
        if footer_elem:
            footer_text = footer_elem.get_text(strip=True)
            import re
            heat_match = re.search(r"([\d.]+万|\d+)", footer_text)
            if heat_match:
                heat_display = heat_match.group(1)

        # Book ID
        link = item.find("a", href=True)
        book_id = ""
        if link:
            href = link["href"]
            import re
            match = re.search(r"/page/(\d+)", href)
            if match:
                book_id = match.group(1)

        print(f"\n第一本小说数据:")
        print(f"  书名：{book_title}")
        print(f"  作者：{author}")
        print(f"  热度：{heat_display}")
        print(f"  Book ID: {book_id}")

        # 访问详情页
        if book_id:
            detail_url = f"{self.base_url}/page/{book_id}"
            print(f"\n访问详情页：{detail_url}")
            await page.goto(detail_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)

            detail_html = await page.content()
            print(f"详情页 HTML 长度：{len(detail_html)} 字符")

            # 解析详情页
            detail_soup = BeautifulSoup(detail_html, "lxml")

            # 简介
            synopsis = ""
            for elem in detail_soup.find_all(["div", "p"]):
                text = elem.get_text(strip=True)
                if len(text) > 50 and len(text) < 500:
                    synopsis = text[:200] + "..." if len(text) > 200 else text
                    break

            # 状态
            book_status = ""
            for elem in detail_soup.find_all(["span", "div"]):
                text = elem.get_text(strip=True)
                if "连载" in text or "完结" in text:
                    book_status = "已完结" if "完结" in text else "连载中"
                    break

            print(f"\n详情页数据:")
            print(f"  状态：{book_status}")
            print(f"  简介：{synopsis[:100]}..." if synopsis else "  简介：未找到")

            # 保存数据
            book_data = {
                "book_id": book_id,
                "book_title": book_title,
                "author": author,
                "heat_display": heat_display,
                "book_status": book_status,
                "synopsis": synopsis,
                "detail_url": detail_url,
            }

            with open("data/test_book_simple.json", "w", encoding="utf-8") as f:
                json.dump(book_data, f, ensure_ascii=False, indent=2)
            print(f"\n数据已保存：data/test_book_simple.json")

        await page.close()
        await self.browser.close()

        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)


async def main():
    tester = BookTester()
    await tester.test_book()


if __name__ == "__main__":
    asyncio.run(main())
