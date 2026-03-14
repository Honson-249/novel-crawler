#!/usr/bin/env python3
"""
人类行为模拟模块 - 模拟真实用户操作行为
"""
import asyncio
import random
from typing import Optional

from playwright.async_api import Page
from loguru import logger


class HumanSimulator:
    """人类行为模拟器 - 模拟真实用户的浏览行为"""

    def __init__(self, page: Page):
        self.page = page

    def set_page(self, page: Page):
        """设置页面对象"""
        self.page = page

    async def random_scroll(self, min_px: int = 100, max_px: int = 500):
        """随机滚动页面"""
        scroll_amount = random.randint(min_px, max_px)
        await self.page.evaluate(f"window.scrollBy(0, {scroll_amount})")

    async def random_delay(self, min_sec: float = 1.0, max_sec: float = 3.0):
        """随机延迟"""
        delay = random.uniform(min_sec, max_sec)
        await asyncio.sleep(delay)

    async def simulate_human_reading(self, page: Optional[Page] = None):
        """模拟真实用户阅读行为"""
        if page is None:
            page = self.page

        # 1. 初始停顿（模拟加载后浏览）
        await self.random_delay(0.3, 0.6)

        # 2. 随机滚动 1-2 次
        scroll_count = random.randint(1, 2)
        for _ in range(scroll_count):
            await self.random_scroll(200, 600)
            await self.random_delay(0.5, 1.0)

        # 3. 返回顶部
        await page.evaluate("window.scrollTo(0, 0)")
        await self.random_delay(0.3, 0.8)

        # 4. 再次向下滚动
        await self.random_scroll(300, 800)
        await self.random_delay(0.3, 0.8)

    async def simulate_mouse_browse(self):
        """模拟鼠标浏览章节列表的行为"""
        # 随机停留 0.3-0.5 秒，模拟浏览章节
        await self.random_delay(0.3, 0.5)

        # 模拟鼠标在章节列表区域移动
        viewport_height = await self.page.evaluate("window.innerHeight")
        scroll_y = await self.page.evaluate("window.scrollY")

        # 在页面底部（章节列表区域）模拟几次鼠标移动
        for _ in range(random.randint(2, 4)):
            target_x = random.randint(100, 600)
            target_y = scroll_y + viewport_height - random.randint(100, 400)
            await self.page.mouse.move(target_x, target_y)
            await asyncio.sleep(random.uniform(0.05, 0.15))

        # 滚动回顶部，模拟继续阅读
        await self.page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.2)

    async def simulate_mouse_movement(self, target_element=None):
        """模拟真实鼠标移动轨迹"""
        try:
            if target_element:
                box = await target_element.bounding_box()
                if not box:
                    return
                target_x = box['x'] + box['width'] / 2
                target_y = box['y'] + box['height'] / 2
            else:
                target_x = random.randint(100, 800)
                target_y = random.randint(100, 600)

            # 分步移动（模拟真实鼠标轨迹）
            steps = random.randint(3, 6)
            for i in range(steps):
                x = target_x * (i / steps) + random.randint(-30, 30)
                y = target_y * (i / steps) + random.randint(-30, 30)
                await self.page.mouse.move(x, y)
                await self.random_delay(0.03, 0.1)

            # 最终定位
            await self.page.mouse.move(target_x, target_y)
            await self.random_delay(0.2, 0.5)

        except Exception as e:
            logger.error(f"鼠标移动模拟失败：{e}")

    async def quick_scroll_to_bottom(self, repeat: int = 2, delay: float = 0.2):
        """快速滚动到底部加载内容"""
        for _ in range(repeat):
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(delay)

    async def human_scroll_to_bottom(self, delay_min: float = 2.0, delay_max: float = 3.0):
        """模拟人类滚动到底部的行为（带延迟）"""
        await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(delay_min, delay_max))
        await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(delay_min, delay_max))
