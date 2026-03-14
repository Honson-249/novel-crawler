#!/usr/bin/env python3
"""
端到端测试 - 完整爬取流程
"""
import pytest
import asyncio
from pathlib import Path


@pytest.mark.e2e
@pytest.mark.slow
class TestFullCrawl:
    """完整爬取流程端到端测试"""

    @pytest.mark.asyncio
    async def test_full_crawl_and_export(self):
        """测试完整爬取和导出流程"""
        # 1. 爬取
        from src.spiders.fanqie.spider import HumanSimulatedSpider

        spider = HumanSimulatedSpider()
        result = await spider.run(
            crawl_all=False,
            target_category_idx=0,
            limit=5,
            crawl_detail=False
        )

        assert result.success is True
        assert result.statistics.items_extracted > 0

        # 2. 验证数据在数据库中
        from src.pipeline.storage import db_manager
        from src.pipeline.validator import DataValidator

        conn = db_manager().get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM fanqie_ranks
            WHERE board_name = '男频新书榜' OR board_name = '女频新书榜'
        """)
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0

        # 3. 验证数据质量
        validator = DataValidator()
        validation_result = validator.validate()

        # 验证应该通过（至少部分通过）
        assert validation_result.get("total_records", 0) > 0


@pytest.mark.e2e
@pytest.mark.slow
class TestWebAPI:
    """Web API 端到端测试"""

    def test_health_endpoint(self):
        """测试健康检查端点"""
        import httpx

        try:
            response = httpx.get("http://localhost:8000/health", timeout=5)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
        except httpx.ConnectError:
            pytest.skip("Web API 服务未启动")

    def test_books_endpoint(self):
        """测试书籍列表端点"""
        import httpx

        try:
            response = httpx.get(
                "http://localhost:8000/books?page=1&page_size=5",
                timeout=5
            )
            assert response.status_code == 200
            data = response.json()
            assert "total" in data
            assert "data" in data
        except httpx.ConnectError:
            pytest.skip("Web API 服务未启动")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e"])
