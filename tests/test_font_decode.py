#!/usr/bin/env python3
"""测试字体解码后的指标值"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from pipeline.font_mapper import get_mapper

async def test_font_decode():
    print("="*60)
    print("📊 测试字体解码后的指标值")
    print("="*60)
    
    font_mapper = get_mapper()
    
    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        
        await page.goto("https://fanqienovel.com/rank/1_1_257", wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        
        book_items = soup.find_all(class_="rank-book-item")[:5]
        
        print(f"\n找到 {len(book_items)} 个书籍项\n")
        
        for idx, item in enumerate(book_items, 1):
            print(f"【第 {idx} 本】")
            
            # 书名（解码）
            title_elem = item.find(class_="title")
            title_raw = title_elem.get_text(strip=True) if title_elem else ""
            title_decoded = font_mapper.decode_text(title_raw)
            print(f"  书名：{title_decoded}")
            
            # footer（指标值）
            footer_elem = item.find(class_="book-item-footer")
            if footer_elem:
                footer_raw = footer_elem.get_text(strip=True)
                footer_decoded = font_mapper.decode_text(footer_raw)
                print(f"  footer 原始：{footer_raw}")
                print(f"  footer 解码：{footer_decoded}")
                
                # 解析指标
                metric_name = "在读人数" if '在读' in footer_decoded else "热度"
                
                # 提取数值（解码后）
                heat_match = re.search(r'在读：([\d.]+ 万)', footer_decoded)
                if heat_match:
                    raw = heat_match.group(0)  # 在读：50.7 万
                    value_str = heat_match.group(1)  # 50.7 万
                    
                    if '万' in value_str:
                        calc = int(float(value_str.replace('万', '')) * 10000)
                    else:
                        calc = int(value_str)
                    
                    print(f"  ✅ metric_name: {metric_name}")
                    print(f"  ✅ metric_value_raw: {raw}")
                    print(f"  ✅ metric_value: {calc:,}")
                else:
                    print(f"  ❌ 无法解析")
            else:
                print(f"  ❌ 未找到 footer")
            
            print()
        
        await browser.close()
        
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    
    print("="*60)

if __name__ == "__main__":
    asyncio.run(test_font_decode())
