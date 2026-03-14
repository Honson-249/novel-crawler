#!/usr/bin/env python3
"""测试单本小说数据获取 - 验证所有 OCR 规范字段"""

import asyncio
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import sys
from pathlib import Path

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from pipeline.font_mapper import get_mapper


class SingleBookTester:
    """单本小说测试器"""
    
    def __init__(self):
        self.font_mapper = get_mapper()
        self.base_url = "https://fanqienovel.com"
        self.browser = None
        self.context = None
    
    async def init_browser(self):
        """初始化浏览器"""
        p = await async_playwright().start()
        self.browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        
        # 注入指纹隐藏
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh;q=0.9', 'en;q=0.8']});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
            delete navigator.__proto__.webdriver;
            window.chrome = { runtime: {}, app: { isInstalled: false } };
            window.navigator.chrome = { runtime: {} };
        """)
        
        print("✅ 浏览器初始化完成")
    
    async def close_browser(self):
        """关闭浏览器"""
        if self.browser:
            await self.browser.close()
            print("✅ 浏览器已关闭")
    
    def _extract_initial_state(self, html: str) -> dict:
        """从榜单页提取 __INITIAL_STATE__"""
        result = {}
        try:
            soup = BeautifulSoup(html, "lxml")
            for script in soup.find_all("script"):
                text = script.get_text()
                if "__INITIAL_STATE__" in text:
                    import re
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
            print(f"⚠️ 提取 __INITIAL_STATE__ 失败：{e}")
        return result
    
    def _parse_rank_page(self, html: str) -> list:
        """解析榜单页"""
        books = []
        initial_state = self._extract_initial_state(html)
        
        try:
            soup = BeautifulSoup(html, "lxml")
            book_items = soup.find_all(class_="rank-book-item")
            
            if not book_items:
                book_items = soup.find_all(class_=re.compile(r"rank-book-item|book-item", re.I))
            
            print(f"📚 找到 {len(book_items)} 个书籍项")
            
            for idx, item in enumerate(book_items[:1], 1):  # 只取第一本
                try:
                    # book_title
                    book_name_elem = item.find(class_="title")
                    book_title = book_name_elem.get_text(strip=True) if book_name_elem else None
                    book_title = self.font_mapper.decode_text(book_title) if book_title else None
                    
                    # author_name
                    author_elem = item.find(class_="author")
                    author_name = author_elem.get_text(strip=True) if author_elem else None
                    author_name = self.font_mapper.decode_text(author_name) if author_name else None
                    
                    # metric_*
                    footer_elem = item.find(class_="book-item-footer")
                    metric_name = None
                    metric_value_raw = None
                    metric_value = None
                    
                    if footer_elem:
                        footer_text = footer_elem.get_text(strip=True)
                        if '在读' in footer_text:
                            metric_name = "在读人数"
                        elif '热力' in footer_text:
                            metric_name = "热力值"
                        else:
                            metric_name = "热度"
                        
                        import re
                        heat_match = re.search(r"([\d.]+ 万 |\d+)", footer_text)
                        if heat_match:
                            metric_value_raw = heat_match.group(0)
                            value_str = heat_match.group(1)
                            if '万' in value_str:
                                metric_value = int(float(value_str.replace('万', '')) * 10000)
                            else:
                                metric_value = int(value_str)
                    
                    # book_id
                    book_id = None
                    link = item.find("a", href=re.compile(r"/page/|/book/"))
                    if link and link.get("href"):
                        href = link["href"]
                        match = re.search(r"/page/(\d+)", href)
                        if match:
                            book_id = match.group(1)
                    
                    # cover_url
                    cover_url = None
                    img = item.find("img")
                    if img:
                        cover_url = img.get("src") or img.get("data-src")
                    
                    # 从 __INITIAL_STATE__ 补充 synopsis
                    synopsis = None
                    if book_id and book_id in initial_state:
                        synopsis = initial_state[book_id].get("abstract")
                    
                    book_data = {
                        "board_name": "测试榜单",
                        "sub_category": "测试分类",
                        "rank_num": idx,
                        "book_title": book_title,
                        "author_name": author_name,
                        "metric_name": metric_name,
                        "metric_value_raw": metric_value_raw,
                        "metric_value": metric_value,
                        "book_status": "连载中",  # 榜单默认
                        "synopsis": synopsis,  # 从榜单页提取
                        "chapter_list_json": None,  # 需要详情页
                        "book_id": book_id,
                        "cover_url": cover_url,
                        "detail_url": f"{self.base_url}/page/{book_id}",
                    }
                    
                    books.append(book_data)
                    
                except Exception as e:
                    print(f"⚠️ 解析书籍项失败：{e}")
            
        except Exception as e:
            print(f"⚠️ 解析页面失败：{e}")
        
        return books
    
    def _parse_detail_page(self, html: str, book_id: str) -> dict:
        """解析详情页"""
        result = {"book_id": book_id}
        
        try:
            soup = BeautifulSoup(html, "lxml")
            
            # synopsis
            synopsis_selectors = ['[class*="intro"]', '[class*="desc"]', '[class*="synopsis"]']
            for sel in synopsis_selectors:
                elem = soup.select_one(sel)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 20:
                        result["synopsis"] = text
                        print(f"✅ 找到简介：{text[:50]}...")
                        break
            
            # book_status
            status_keywords = ["连载中", "已完结", "完结", "连载"]
            for elem in soup.find_all(["span", "div"]):
                text = elem.get_text(strip=True)
                for status in status_keywords:
                    if status in text:
                        result["book_status"] = "已完结" if "完结" in status else "连载中"
                        print(f"✅ 找到状态：{result['book_status']}")
                        break
                if result.get("book_status"):
                    break
            
            # chapter_list_json
            chapter_list = []
            chapter_selectors = ['[class*="chapter"] a', '[class*="Chapter"] a', '[class*="dir"] a']
            for sel in chapter_selectors:
                chapter_elems = soup.select(sel)
                for chap in chapter_elems[:50]:
                    text = chap.get_text(strip=True)
                    if text and len(text) < 100 and any(c.isdigit() for c in text):
                        chapter_list.append(text)
                if chapter_list:
                    break
            
            if chapter_list:
                result["chapter_list_json"] = json.dumps(chapter_list[:10], ensure_ascii=False)  # 只取前 10 章
                print(f"✅ 找到章节：{len(chapter_list)} 个（已取前 10 个）")
            
        except Exception as e:
            print(f"⚠️ 解析详情页失败：{e}")
        
        return result
    
    async def test_single_book(self):
        """测试单本小说数据获取"""
        print("\n" + "="*60)
        print("🧪 开始测试单本小说数据获取")
        print("="*60)
        
        await self.init_browser()
        
        # 测试书籍：从榜单页获取第一本
        test_url = "https://fanqienovel.com/rank/1_1_257"  # 男频玄幻脑洞阅读榜
        
        print(f"\n📖 访问榜单页：{test_url}")
        page = await self.context.new_page()
        
        try:
            await page.goto(test_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)  # 等待加载
            
            html = await page.content()
            print(f"✅ 榜单页 HTML 长度：{len(html)} 字符")
            
            # 解析榜单页
            books = self._parse_rank_page(html)
            
            if not books:
                print("❌ 未找到任何书籍")
                return
            
            # 测试第一本书
            book = books[0]
            print(f"\n📚 测试书籍：{book['book_title']} by {book['author_name']}")
            print(f"   Book ID: {book['book_id']}")
            
            # 访问详情页
            if book['book_id']:
                print(f"\n🔗 访问详情页：{book['detail_url']}")
                await page.goto(book['detail_url'], wait_until="networkidle", timeout=30000)
                await asyncio.sleep(4)  # 等待详情加载
                
                detail_html = await page.content()
                print(f"✅ 详情页 HTML 长度：{len(detail_html)} 字符")
                
                # 解析详情页
                detail_data = self._parse_detail_page(detail_html, book['book_id'])
                
                # 合并数据
                book.update(detail_data)
                
                # 打印完整数据
                print("\n" + "="*60)
                print("📊 完整数据字段验证")
                print("="*60)
                
                ocr_fields = [
                    "batch_date", "board_name", "sub_category", "rank_num",
                    "book_title", "author_name", "metric_name", "metric_value_raw",
                    "metric_value", "book_status", "synopsis", "chapter_list_json",
                    "book_id", "cover_url", "detail_url"
                ]
                
                for field in ocr_fields:
                    value = book.get(field)
                    status = "✅" if value else "❌"
                    
                    if field == "chapter_list_json" and value:
                        chapters = json.loads(value)
                        print(f"{status} {field}: {len(chapters)} 个章节")
                    elif field == "synopsis" and value:
                        print(f"{status} {field}: {value[:50]}...")
                    elif isinstance(value, str) and value and len(value) > 50:
                        print(f"{status} {field}: {value[:50]}...")
                    else:
                        print(f"{status} {field}: {value}")
                
                # 统计
                total_fields = len(ocr_fields)
                filled_fields = sum(1 for f in ocr_fields if book.get(f))
                print(f"\n📈 字段填充率：{filled_fields}/{total_fields} ({100*filled_fields/total_fields:.1f}%)")
                
                # 保存测试数据
                with open("data/test_single_book.json", "w", encoding="utf-8") as f:
                    json.dump(book, f, ensure_ascii=False, indent=2)
                print(f"\n💾 测试数据已保存：data/test_single_book.json")
            
        except Exception as e:
            print(f"❌ 测试失败：{e}")
            import traceback
            traceback.print_exc()
        
        finally:
            await page.close()
            await self.close_browser()
        
        print("\n" + "="*60)
        print("✅ 测试完成")
        print("="*60)


def main():
    """主函数"""
    tester = SingleBookTester()
    asyncio.run(tester.test_single_book())
    return 0


if __name__ == "__main__":
    exit(main())
