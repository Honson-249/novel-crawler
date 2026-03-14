#!/usr/bin/env python3
"""
爬虫流程集成测试
"""
import pytest
import asyncio
from src.spiders.fanqie.spider import HumanSimulatedSpider


@pytest.mark.integration
@pytest.mark.slow
class TestSpiderFlow:
    """爬虫流程集成测试"""

    @pytest.fixture
    def spider(self):
        """创建爬虫实例"""
        return HumanSimulatedSpider()

    def test_spider_initialization(self, spider):
        """测试爬虫初始化"""
        assert spider is not None
        assert spider.config is not None

    @pytest.mark.asyncio
    async def test_spider_run_single_category(self, spider):
        """测试爬取单个分类"""
        # 只爬取第一个分类的前 3 本书
        result = await spider.run(
            crawl_all=False,
            target_category_idx=0,
            limit=3,
            crawl_detail=False
        )

        assert result.success is True
        assert result.statistics.pages_crawled > 0
        assert result.statistics.items_extracted > 0

    @pytest.mark.asyncio
    async def test_spider_with_cache(self, spider):
        """测试缓存机制"""
        # 第一次爬取
        result1 = await spider.run(
            crawl_all=False,
            target_category_idx=0,
            limit=3,
            crawl_detail=False
        )

        # 第二次爬取（应该使用缓存）
        result2 = await spider.run(
            crawl_all=False,
            target_category_idx=0,
            limit=3,
            crawl_detail=False
        )

        assert result1.success is True
        assert result2.success is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
