#!/usr/bin/env python3
"""快速统计工具 - 统计番茄小说分类和书籍数量"""

import asyncio
from pathlib import Path
import sys
import json
import re
from loguru import logger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG

# 配置日志
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=LOG_CONFIG["level"],
    colorize=False,
)

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


async def quick_stats():
    """快速统计"""
    logger.info("\n" + "=" * 60)
    logger.info("Stats - 番茄小说平台快速统计")
    logger.info("=" * 60)

    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        # 访问榜单首页
        logger.info("\n[1] 访问榜单首页...")
        await page.goto("https://fanqienovel.com/rank", wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # 提取所有分类链接
        category_links = soup.find_all("a", href=re.compile(r"/rank/\d+_\d+_\d+"))

        # 去重和筛选
        categories = set()
        for link in category_links:
            href = link.get("href", "")
            match = re.search(r"/rank/(\d+)_(\d+)_(\d+)", href)
            if match:
                categories.add(match.group(0))

        logger.info(f"\n[OK] 发现分类数：{len(categories)} 个")

        # 分类列表
        logger.info("\n分类列表:")
        for cat in sorted(categories):
            logger.info(f"  - {cat}")

        # 估算书籍总数
        # 每个分类前 3 页 = 30 本书
        books_per_category = 30
        estimated_total = len(categories) * books_per_category

        logger.info(f"\n估算统计:")
        logger.info(f"  每分类书籍数：{books_per_category} 本（前 3 页）")
        logger.info(f"  总书籍数（估算）：{estimated_total} 本")
        logger.info(f"  去重后估算：{int(estimated_total * 0.6):,} 本（考虑重复上榜）")

        # 保存结果
        result = {
            "category_count": len(categories),
            "books_per_category": books_per_category,
            "estimated_total": estimated_total,
            "estimated_unique": int(estimated_total * 0.6),
            "categories": sorted(list(categories))
        }

        with open("data/quick_stats.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logger.info(f"\n[OK] 统计已保存：data/quick_stats.json")

        await browser.close()

    except Exception as e:
        logger.error(f"\n[ERR] 统计失败：{e}")

    logger.info("\n" + "=" * 60)
    logger.info("Complete - 统计完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(quick_stats())
