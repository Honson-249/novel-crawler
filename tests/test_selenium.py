#!/usr/bin/env python3
"""测试 Selenium + webdriver-manager 访问番茄小说详情页"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def test_fanqie_detail():
    """测试访问番茄小说详情页"""
    
    # 测试书籍 ID（从榜单中获取的真实书籍）
    test_books = [
        "7294988302923067675",  # 一本热门书
        "7294988302923067676",
    ]
    
    # 配置 Chrome 选项
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 无头模式
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # 用户代理
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    results = []
    
    for book_id in test_books:
        url = f"https://fanqienovel.com/page/{book_id}"
        print(f"\n{'='*60}")
        print(f"测试书籍：{book_id}")
        print(f"URL: {url}")
        print(f"{'='*60}")
        
        try:
            # 初始化 driver
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            
            # 访问页面
            print("正在加载页面...")
            driver.get(url)
            
            # 等待页面加载
            time.sleep(3)
            
            # 获取页面 HTML
            html = driver.page_source
            print(f"页面 HTML 长度：{len(html)} 字符")
            
            # 检查是否有内容
            if len(html) < 1000:
                print("❌ 页面内容为空或过短，可能被反爬拦截")
                results.append({"book_id": book_id, "success": False, "reason": "empty_page"})
            else:
                # 尝试查找简介
                try:
                    # 查找简介元素
                    synopsis_elem = driver.find_element(By.CSS_SELECTOR, ".book-detail .desc")
                    synopsis = synopsis_elem.text if synopsis_elem else "未找到简介"
                    print(f"✅ 成功获取简介：{synopsis[:100]}...")
                    
                    # 查找书名
                    title_elem = driver.find_element(By.CSS_SELECTOR, ".book-detail .title")
                    title = title_elem.text if title_elem else "未找到书名"
                    print(f"✅ 书名：{title}")
                    
                    results.append({
                        "book_id": book_id,
                        "success": True,
                        "title": title,
                        "synopsis": synopsis[:200]
                    })
                except Exception as e:
                    print(f"⚠️ 页面有内容但未找到简介元素：{e}")
                    # 保存 HTML 到文件以便分析
                    with open(f"/root/.openclaw/workspace/feature-crawler/test_{book_id}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"HTML 已保存到：test_{book_id}.html")
                    results.append({"book_id": book_id, "success": "partial", "reason": "no_synopsis_element"})
            
            driver.quit()
            
        except Exception as e:
            print(f"❌ 错误：{e}")
            results.append({"book_id": book_id, "success": False, "reason": str(e)})
    
    # 打印总结
    print(f"\n{'='*60}")
    print("测试总结")
    print(f"{'='*60}")
    for r in results:
        status = "✅ 成功" if r["success"] == True else "⚠️ 部分成功" if r["success"] == "partial" else "❌ 失败"
        print(f"{r['book_id']}: {status} - {r.get('reason', r.get('title', ''))}")

if __name__ == "__main__":
    test_fanqie_detail()
