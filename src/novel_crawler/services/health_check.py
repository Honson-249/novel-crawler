#!/usr/bin/env python3
"""
健康检查服务
- 数据库连接检查
- Redis 连接检查
- 浏览器环境检查
- 服务健康状态
"""
import asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path
from loguru import logger

# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))


class HealthStatus:
    """健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class HealthCheckResult:
    """健康检查结果"""

    def __init__(
        self,
        status: str = HealthStatus.HEALTHY,
        checks: Optional[Dict[str, bool]] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.status = status
        self.checks = checks or {}
        self.details = details or {}
        self.timestamp = datetime.now(UTC8)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
            "checks": self.checks,
            "details": self.details,
        }


class HealthChecker:
    """
    健康检查器
    - 单例模式
    - 支持多项检查
    - 缓存检查结果
    """
    _instance: Optional['HealthChecker'] = None

    def __new__(cls) -> 'HealthChecker':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # 检查函数注册表
        self._checks: Dict[str, callable] = {}

        # 缓存检查结果
        self._cache: Dict[str, Tuple[HealthCheckResult, datetime]] = {}
        self._cache_ttl = 30  # 缓存 TTL（秒）

        # 注册默认检查
        self._register_default_checks()

    def _register_default_checks(self):
        """注册默认检查项"""
        self.register_check("database", self._check_database)
        self.register_check("redis", self._check_redis)
        self.register_check("browser", self._check_browser)
        self.register_check("disk_space", self._check_disk_space)

    def register_check(self, name: str, check_func: callable) -> None:
        """
        注册检查项

        Args:
            name: 检查项名称
            check_func: 检查函数
        """
        self._checks[name] = check_func
        logger.debug(f"注册健康检查项：{name}")

    async def check(self, use_cache: bool = True) -> HealthCheckResult:
        """
        执行所有检查

        Args:
            use_cache: 是否使用缓存结果

        Returns:
            健康检查结果
        """
        # 检查缓存
        if use_cache:
            cached = self._get_cached_result()
            if cached:
                return cached

        # 执行所有检查
        results = {}
        details = {}

        for name, check_func in self._checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()

                results[name] = result[0]
                details[name] = result[1] if len(result) > 1 else {}
            except Exception as e:
                logger.error(f"健康检查失败 [{name}]: {e}")
                results[name] = False
                details[name] = {"error": str(e)}

        # 计算总体状态
        all_passed = all(results.values())
        any_passed = any(results.values())

        if all_passed:
            status = HealthStatus.HEALTHY
        elif any_passed:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY

        result = HealthCheckResult(
            status=status,
            checks=results,
            details=details
        )

        # 缓存结果
        self._cache_result(result)

        return result

    def _get_cached_result(self) -> Optional[HealthCheckResult]:
        """获取缓存结果"""
        if not self._cache:
            return None

        result, timestamp = self._cache
        if (datetime.now(UTC8) - timestamp).total_seconds() < self._cache_ttl:
            return result

        return None

    def _cache_result(self, result: HealthCheckResult) -> None:
        """缓存结果"""
        self._cache = (result, datetime.now(UTC8))

    async def _check_database(self) -> Tuple[bool, Dict[str, Any]]:
        """检查数据库连接"""
        try:
            from src.novel_crawler.config.config_loader import get_database_config

            config = get_database_config()

            # 尝试连接
            import pymysql
            conn = pymysql.connect(
                **config.connection_params,
                connect_timeout=5
            )
            conn.close()

            return True, {"host": config.host, "database": config.database}
        except ImportError:
            return True, {"message": "pymysql 未安装"}
        except Exception as e:
            return False, {"error": str(e)}

    async def _check_redis(self) -> Tuple[bool, Dict[str, Any]]:
        """检查 Redis 连接"""
        try:
            from src.novel_crawler.config.config_loader import get_redis_config
            import redis

            config = get_redis_config()
            client = redis.Redis(
                **config.connection_params,
                socket_connect_timeout=5
            )
            client.ping()

            return True, {"host": config.host, "port": config.port}
        except ImportError:
            return True, {"message": "redis 未安装"}
        except Exception as e:
            return False, {"error": str(e)}

    async def _check_browser(self) -> Tuple[bool, Dict[str, Any]]:
        """检查浏览器环境"""
        try:
            from playwright.async_api import async_playwright

            # 简单启动浏览器测试
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=True,
                timeout=10000
            )
            await browser.close()
            await playwright.stop()

            return True, {"browser": "chromium"}
        except Exception as e:
            return False, {"error": str(e)}

    async def _check_disk_space(self) -> Tuple[bool, Dict[str, Any]]:
        """检查磁盘空间"""
        try:
            import shutil

            # 检查项目目录磁盘空间
            project_dir = Path(__file__).parent.parent.parent
            usage = shutil.disk_usage(project_dir)
            free_gb = usage.free / (1024 ** 3)

            # 小于 1GB 认为空间不足
            if free_gb < 1:
                return False, {"free_gb": free_gb, "threshold": 1}

            return True, {
                "free_gb": round(free_gb, 2),
                "total_gb": round(usage.total / (1024 ** 3), 2),
                "used_percent": round(usage.used / usage.total * 100, 1)
            }
        except Exception as e:
            return False, {"error": str(e)}

    def get_status_summary(self) -> Dict[str, Any]:
        """获取健康状态摘要（同步版本）"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        result = loop.run_until_complete(self.check())
        return result.to_dict()


# ==================== 全局实例 ====================

_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """获取全局健康检查器实例"""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


async def check_health() -> HealthCheckResult:
    """检查健康状态"""
    return await get_health_checker().check()


def get_health_status() -> Dict[str, Any]:
    """获取健康状态（同步）"""
    return get_health_checker().get_status_summary()
