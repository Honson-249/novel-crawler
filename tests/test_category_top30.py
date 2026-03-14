#!/usr/bin/env python3
"""测试单个分类能否获取 Top 30 本书"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from pipeline.font_mapper import get_mapper


async def test_category_top30():
    """测试单个分类 Top 30 书籍"""
    print("="*60)
    print("📚 测试单个分类 Top 30 书籍获取")
    print("="*60)
    
    font_mapper = get_mapper()
    
    # 测试分类：男频玄幻脑洞阅读榜
    test_url = "https://fanqienovel.com/rank/1_1_257"
    category_name = "男频 - 玄幻脑洞 - 阅读榜"
    
    print(f"\n📖 测试分类：{category_name}")
    print(f"URL: {test_url}")
    print()
    
    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        all_books = []
        
        # 爬取 3 页（每页 10 本，共 30 本）
        for page_num in range(1, 4):
            url = f"{test_url}?page={page_num}" if page_num > 1 else test_url
            print(f"\n【第 {page_num} 页】{url}")
            print("-"*60)
            
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)
            
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            
            book_items = soup.find_all(class_="rank-book-item")
            if not book_items:
                book_items = soup.find_all(class_=re.compile(r"rank-book-item|book-item", re.I))
            
            print(f"找到 {len(book_items)} 个书籍项")
            
            if not book_items:
                print("❌ 未找到书籍，停止")
                break
            
            # 解析每本书
            for idx, item in enumerate(book_items, 1):
                try:
                    # 书名
                    title_elem = item.find(class_="title")
                    title_raw = title_elem.get_text(strip=True) if title_elem else ""
                    title = font_mapper.decode_text(title_raw)
                    
                    # 作者
                    author_elem = item.find(class_="author")
                    author_raw = author_elem.get_text(strip=True) if author_elem else ""
                    author = font_mapper.decode_text(author_raw)
                    
                    # 排名
                    rank = (page_num - 1) * 10 + idx
                    
                    # 指标值（footer）
                    footer_elem = item.find(class_="book-item-footer")
                    metric = "N/A"
                    if footer_elem:
                        footer_raw = footer_elem.get_text(strip=True)
                        footer_text = font_mapper.decode_text(footer_raw)
                        
                        # 提取指标
                        heat_match = re.search(r"在读：([\d.]+ 万)", footer_text)
                        if heat_match:
                            metric = heat_match.group(1)
                        else:
                            metric = footer_text[:30]
                    
                    book_info = {
                        "rank": rank,
                        "title": title,
                        "author": author,
                        "metric": metric
                    }
                    all_books.append(book_info)
                    
                    # 显示前 10 本和最后几本
                    if rank <= 10 or rank > 27:
                        print(f"  {rank:2d}. 《{title[:20]}》by {author[:8]} - {metric}")
                    elif rank == 11:
                        print(f"  ... (中间省略) ...")
                    
                except Exception as e:
                    print(f"  ❌ 解析失败：{e}")
            
            await asyncio.sleep(2)  # 页间延迟
        
        # 统计
        print("\n" + "="*60)
        print("📊 统计结果")
        print("="*60)
        print(f"目标书籍数：30 本")
        print(f"实际获取：{len(all_books)} 本")
        print(f"完成率：{100*len(all_books)/30:.1f}%")
        
        if len(all_books) >= 27:
            print("\n✅ 成功获取 Top 30 本书！")
        else:
            print(f"\n⚠️ 只获取到 {len(all_books)} 本，未达 30 本")
        
        # 保存结果
        import json
        with open("data/test_category_top30.json", "w", encoding="utf-8") as f:
            json.dump({
                "category": category_name,
                "url": test_url,
                "total_books": len(all_books),
                "books": all_books
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 数据已保存：data/test_category_top30.json")
        
        await browser.close()
        
    except Exception as e:
        print(f"❌ 测试失败：{e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("✅ 测试完成")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(test_category_top30())
