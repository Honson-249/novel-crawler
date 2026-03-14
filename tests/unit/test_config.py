#!/usr/bin/env python3
"""
配置模块单元测试
"""
import os
import pytest
from src.config.config_loader import (
    ConfigLoader,
    DatabaseConfig,
    RedisConfig,
    SpiderConfig,
    get_config_loader,
    get_database_config,
    get_redis_config,
)


class TestDatabaseConfig:
    """数据库配置测试类"""

    def test_default_values(self):
        """测试默认值"""
        config = DatabaseConfig()
        assert config.port == 3306
        assert config.charset == "utf8mb4"
        assert config.use_unicode is True

    def test_connection_params(self):
        """测试连接参数"""
        config = DatabaseConfig(
            host="localhost",
            user="test",
            password="test123",
            database="testdb"
        )
        params = config.connection_params
        assert params["host"] == "localhost"
        assert params["user"] == "test"
        assert params["password"] == "test123"
        assert params["database"] == "testdb"
        assert "init_command" in params


class TestRedisConfig:
    """Redis 配置测试类"""

    def test_default_values(self):
        """测试默认值"""
        config = RedisConfig()
        assert config.port == 6379
        assert config.db == 0
        assert config.password is None

    def test_connection_params_without_password(self):
        """测试无密码连接参数"""
        config = RedisConfig(host="localhost")
        params = config.connection_params
        assert params["host"] == "localhost"
        assert params["port"] == 6379
        assert "password" not in params

    def test_connection_params_with_password(self):
        """测试有密码连接参数"""
        config = RedisConfig(host="localhost", password="secret")
        params = config.connection_params
        assert params["password"] == "secret"


class TestSpiderConfig:
    """爬虫配置测试类"""

    def test_default_values(self):
        """测试默认值"""
        config = SpiderConfig()
        assert config.timeout == 30
        assert config.retry_times == 3
        assert config.headless is False

    def test_site_name(self):
        """测试站点名称"""
        config = SpiderConfig(site_name="test_site")
        assert config.site_name == "test_site"


class TestConfigLoader:
    """配置加载器测试类"""

    def test_singleton(self):
        """测试单例模式"""
        loader1 = ConfigLoader()
        loader2 = ConfigLoader()
        assert loader1 is loader2

    def test_get_config_loader(self):
        """测试全局配置加载器"""
        loader1 = get_config_loader()
        loader2 = get_config_loader()
        assert loader1 is loader2

    def test_get_database_config(self):
        """测试获取数据库配置"""
        config = get_database_config()
        assert isinstance(config, DatabaseConfig)

    def test_get_redis_config(self):
        """测试获取 Redis 配置"""
        config = get_redis_config()
        assert isinstance(config, RedisConfig)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
