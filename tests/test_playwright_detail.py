#!/usr/bin/env python3
"""测试 Playwright 访问番茄小说详情页（带完整浏览器）"""

from playwright.sync_api import sync_playwright
import time

def test_fanqie_with_playwright():
    """使用 Playwright 完整浏览器访问详情页"""
    
    test_books = [
        "7294988302923067675",
    ]
    
    with sync_playwright() as p:
        # 启动浏览器（有头模式，可以看到行为）
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        for book_id in test_books:
            url = f"https://fanqienovel.com/page/{book_id}"
            print(f"\n{'='*60}")
            print(f"测试书籍：{book_id}")
            print(f"URL: {url}")
            print(f"{'='*60}")
            
            try:
                # 访问页面
                print("正在加载页面...")
                response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                print(f"响应状态码：{response.status}")
                
                # 等待一会让 JS 执行
                print("等待页面渲染...")
                time.sleep(5)
                
                # 获取页面 HTML
                html = page.content()
                print(f"页面 HTML 长度：{len(html)} 字符")
                
                # 检查是否有内容
                if len(html) < 2000:
                    print("❌ 页面内容为空或过短，可能被反爬拦截")
                    print(f"HTML 前 500 字符：{html[:500]}")
                else:
                    print("✅ 页面有内容")
                    
                    # 尝试查找简介
                    try:
                        # 查找各种可能的简介选择器
                        selectors = [
                            ".book-detail .desc",
                            ".desc",
                            "[class*='desc']",
                            ".book-detail__desc",
                        ]
                        
                        for selector in selectors:
                            try:
                                elem = page.query_selector(selector)
                                if elem:
                                    text = elem.inner_text()
                                    if text and len(text) > 10:
                                        print(f"✅ 找到简介 (selector: {selector}): {text[:100]}...")
                                        break
                            except:
                                continue
                        else:
                            print("⚠️ 未找到简介元素")
                            
                    except Exception as e:
                        print(f"⚠️ 查找元素失败：{e}")
                    
                    # 保存 HTML
                    with open(f"/root/.openclaw/workspace/feature-crawler/playwright_{book_id}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"HTML 已保存到：playwright_{book_id}.html")
                    
            except Exception as e:
                print(f"❌ 错误：{e}")
        
        browser.close()

if __name__ == "__main__":
    test_fanqie_with_playwright()
