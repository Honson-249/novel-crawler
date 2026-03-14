#!/usr/bin/env python3
"""
健康检查集成测试
"""
import pytest
import asyncio
from src.service.health_check import (
    HealthChecker,
    get_health_checker,
    check_health,
    HealthStatus,
)


@pytest.mark.integration
class TestHealthCheck:
    """健康检查集成测试"""

    @pytest.fixture
    def health_checker(self):
        """创建健康检查器实例"""
        return HealthChecker()

    def test_health_checker_singleton(self):
        """测试单例模式"""
        checker1 = get_health_checker()
        checker2 = get_health_checker()
        assert checker1 is checker2

    @pytest.mark.asyncio
    async def test_health_check(self, health_checker):
        """测试健康检查"""
        result = await health_checker.check(use_cache=False)

        assert result.status in [
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
            HealthStatus.UNHEALTHY
        ]
        assert result.timestamp is not None
        assert isinstance(result.checks, dict)

    @pytest.mark.asyncio
    async def test_database_check(self, health_checker):
        """测试数据库检查"""
        result = await health_checker.check(use_cache=False)

        # 检查数据库检查结果
        if "database" in result.checks:
            db_ok = result.checks["database"]
            details = result.details.get("database", {})
            assert isinstance(db_ok, bool)

    @pytest.mark.asyncio
    async def test_redis_check(self, health_checker):
        """测试 Redis 检查"""
        result = await health_checker.check(use_cache=False)

        # 检查 Redis 检查结果
        if "redis" in result.checks:
            redis_ok = result.checks["redis"]
            details = result.details.get("redis", {})
            assert isinstance(redis_ok, bool)

    @pytest.mark.asyncio
    async def test_browser_check(self, health_checker):
        """测试浏览器检查"""
        result = await health_checker.check(use_cache=False)

        # 检查浏览器检查结果
        if "browser" in result.checks:
            browser_ok = result.checks["browser"]
            details = result.details.get("browser", {})
            assert isinstance(browser_ok, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
