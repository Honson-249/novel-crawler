#!/usr/bin/env python3
"""
配置加载器 - 统一配置管理
支持多环境、环境变量注入、热重载
"""
import os
import json
from typing import Any, Dict, Optional
from pathlib import Path
from loguru import logger
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ==================== 配置模型 ====================

class DatabaseConfig(BaseSettings):
    """数据库配置"""
    model_config = SettingsConfigDict(env_prefix="MYSQL_", extra="ignore")

    host: str = Field(default="localhost", description="MySQL 主机")
    port: int = Field(default=3306, description="MySQL 端口")
    user: str = Field(default="root", description="MySQL 用户")
    password: str = Field(default="", description="MySQL 密码")
    database: str = Field(default="novel_db", description="数据库名称")
    charset: str = Field(default="utf8mb4", description="字符集")
    use_unicode: bool = Field(default=True, description="使用 Unicode")

    @property
    def connection_params(self) -> Dict[str, Any]:
        """获取 pymysql 连接参数"""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
            "use_unicode": self.use_unicode,
            "init_command": 'SET time_zone = "+08:00"'
        }


class RedisConfig(BaseSettings):
    """Redis 配置"""
    model_config = SettingsConfigDict(env_prefix="REDIS_", extra="ignore")

    host: str = Field(default="localhost", description="Redis 主机")
    port: int = Field(default=6379, description="Redis 端口")
    db: int = Field(default=0, description="Redis 数据库")
    password: Optional[str] = Field(default=None, description="Redis 密码")

    @property
    def connection_params(self) -> Dict[str, Any]:
        """获取 redis 连接参数"""
        params = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        if self.password:
            params["password"] = self.password
            params["decode_responses"] = True
        return params


class SpiderConfig(BaseSettings):
    """爬虫通用配置"""
    model_config = SettingsConfigDict(env_prefix="SPIDER_", extra="ignore")

    site_name: str = Field(default="fanqie", description="站点名称")
    base_url: str = Field(default="https://fanqienovel.com", description="站点基础 URL")
    rank_url: str = Field(default="https://fanqienovel.com/rank", description="榜单 URL")
    timeout: int = Field(default=30, description="请求超时时间 (秒)")
    retry_times: int = Field(default=3, description="重试次数")
    delay_min: int = Field(default=1, description="最小延迟 (秒)")
    delay_max: int = Field(default=3, description="最大延迟 (秒)")
    headless: bool = Field(default=False, description="浏览器无头模式")
    slow_mo: int = Field(default=100, description="浏览器操作延迟 (ms)")


class FanqieConfig(BaseModel):
    """番茄小说站点配置"""
    base_url: str = "https://fanqienovel.com"
    rank_url: str = "https://fanqienovel.com/rank"
    timeout: int = 30
    retry_times: int = 3
    # 性别和榜单类型（用于解析，不需要手动配置分类）
    genders: dict[str, str] = {"0": "女频", "1": "男频"}
    board_types: dict[str, str] = {"1": "新书榜", "2": "阅读榜", "3": "完本榜", "4": "热读榜"}


# ==================== 多环境配置 ====================

class EnvDatabaseConfig(BaseModel):
    """单环境数据库配置"""
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"
    use_unicode: bool = True


class MultiEnvDatabaseConfig(BaseModel):
    """多环境数据库配置"""
    local: EnvDatabaseConfig
    dev: EnvDatabaseConfig
    test: EnvDatabaseConfig
    prod: EnvDatabaseConfig


# 默认多环境数据库配置
DEFAULT_MULTI_ENV_DB_CONFIG = MultiEnvDatabaseConfig(
    local=EnvDatabaseConfig(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="novel_db",
    ),
    dev=EnvDatabaseConfig(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="novel_db",
    ),
    test=EnvDatabaseConfig(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="fanqie_test",
    ),
    prod=EnvDatabaseConfig(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="novel_db",
    ),
)


class EnvRedisConfig(BaseModel):
    """单环境 Redis 配置"""
    host: str
    port: int
    db: int
    password: Optional[str] = None


class MultiEnvRedisConfig(BaseModel):
    """多环境 Redis 配置"""
    local: EnvRedisConfig
    dev: EnvRedisConfig
    test: EnvRedisConfig
    prod: EnvRedisConfig


# 默认多环境 Redis 配置
DEFAULT_MULTI_ENV_REDIS_CONFIG = MultiEnvRedisConfig(
    local=EnvRedisConfig(host="localhost", port=6379, db=0),
    dev=EnvRedisConfig(host="localhost", port=6379, db=1),
    test=EnvRedisConfig(host="localhost", port=6379, db=2),
    prod=EnvRedisConfig(host="localhost", port=6379, db=1),
)


class LogConfig(BaseSettings):
    """日志配置"""
    model_config = SettingsConfigDict(env_prefix="LOG_", extra="ignore")

    level: str = Field(default="INFO", description="日志级别")
    format: str = Field(
        default="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        description="日志格式"
    )
    rotation: str = Field(default="00:00", description="日志切分时间")
    retention: str = Field(default="7 days", description="日志保留时间")
    encoding: str = Field(default="utf-8", description="日志编码")


class AlertConfig(BaseModel):
    """告警配置"""
    enabled: bool = Field(default=False, description="是否启用告警")
    channels: list[str] = Field(default_factory=list, description="告警渠道")
    feishu_webhook: Optional[str] = Field(default=None, description="飞书 Webhook")
    dingtalk_webhook: Optional[str] = Field(default=None, description="钉钉 Webhook")
    wechat_webhook: Optional[str] = Field(default=None, description="企业微信 Webhook")
    min_failure_rate: float = Field(default=0.1, description="最小失败率阈值")


# ==================== 配置加载器 ====================

class ConfigLoader:
    """
    统一配置加载器
    - 单例模式
    - 支持多环境
    - 支持环境变量覆盖
    - 支持热重载
    """
    _instance: Optional['ConfigLoader'] = None
    _config_cache: Dict[str, Any] = {}

    def __new__(cls) -> 'ConfigLoader':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self._current_env = os.getenv("APP_ENV", "local")
        self._config_dir = Path(__file__).parent
        self._env_file = self._config_dir.parent.parent.parent / ".env"
        # 多环境配置
        self._multi_env_db_config = DEFAULT_MULTI_ENV_DB_CONFIG
        self._multi_env_redis_config = DEFAULT_MULTI_ENV_REDIS_CONFIG

    def load(self, env: Optional[str] = None) -> Dict[str, Any]:
        """
        加载配置

        Args:
            env: 环境名称，默认使用 APP_ENV 环境变量

        Returns:
            配置字典
        """
        if env:
            self._current_env = env

        logger.info(f"加载配置 - 环境：{self._current_env}")

        # 加载环境变量文件
        self._load_env_file()

        # 构建配置
        config = {
            "env": self._current_env,
            "database": self.get_database_config(),
            "redis": self.get_redis_config(),
            "spider": self.get_spider_config(),
            "log": self.get_log_config(),
            "alert": self.get_alert_config(),
            "fanqie": self.get_fanqie_config(),
        }

        self._config_cache = config
        logger.info("配置加载完成")
        return config

    def _load_env_file(self):
        """加载 .env 文件"""
        if self._env_file.exists():
            from dotenv import load_dotenv
            load_dotenv(self._env_file)
            logger.info(f"已加载环境变量文件：{self._env_file}")

    def get_database_config(self) -> DatabaseConfig:
        """获取数据库配置（根据当前环境）"""
        if "database" in self._config_cache:
            return self._config_cache["database"]

        # 从多环境配置中获取当前环境的配置
        env_config = getattr(self._multi_env_db_config, self._current_env, self._multi_env_db_config.local)

        # 使用环境变量覆盖
        config = DatabaseConfig(
            host=os.getenv("MYSQL_HOST", env_config.host),
            port=int(os.getenv("MYSQL_PORT", str(env_config.port))),
            user=os.getenv("MYSQL_USER", env_config.user),
            password=os.getenv("MYSQL_PASSWORD", env_config.password),
            database=os.getenv("MYSQL_DATABASE", env_config.database),
        )
        self._config_cache["database"] = config
        return config

    def get_redis_config(self) -> RedisConfig:
        """获取 Redis 配置（根据当前环境）"""
        if "redis" in self._config_cache:
            return self._config_cache["redis"]

        # 从多环境配置中获取当前环境的配置
        env_config = getattr(self._multi_env_redis_config, self._current_env, self._multi_env_redis_config.local)

        # 使用环境变量覆盖
        config = RedisConfig(
            host=os.getenv("REDIS_HOST", env_config.host),
            port=int(os.getenv("REDIS_PORT", str(env_config.port))),
            db=int(os.getenv("REDIS_DB", str(env_config.db))),
            password=os.getenv("REDIS_PASSWORD", env_config.password),
        )
        self._config_cache["redis"] = config
        return config

    def get_spider_config(self) -> SpiderConfig:
        """获取爬虫配置"""
        if "spider" in self._config_cache:
            return self._config_cache["spider"]
        config = SpiderConfig()
        self._config_cache["spider"] = config
        return config

    def get_log_config(self) -> LogConfig:
        """获取日志配置"""
        if "log" in self._config_cache:
            return self._config_cache["log"]
        config = LogConfig()
        self._config_cache["log"] = config
        return config

    def get_alert_config(self) -> AlertConfig:
        """获取告警配置"""
        if "alert" in self._config_cache:
            return self._config_cache["alert"]

        # 从环境变量读取告警配置
        alert_config = AlertConfig(
            enabled=os.getenv("ALERT_ENABLED", "false").lower() == "true",
            feishu_webhook=os.getenv("ALERT_FEISHU_WEBHOOK"),
            dingtalk_webhook=os.getenv("ALERT_DINGTALK_WEBHOOK"),
            wechat_webhook=os.getenv("ALERT_WECHAT_WEBHOOK"),
        )
        self._config_cache["alert"] = alert_config
        return alert_config

    def get_fanqie_config(self) -> FanqieConfig:
        """获取番茄小说配置"""
        if "fanqie" in self._config_cache:
            return self._config_cache["fanqie"]
        config = FanqieConfig()
        self._config_cache["fanqie"] = config
        return config

    def reload(self):
        """重新加载配置（热重载）"""
        logger.info("重新加载配置...")
        self._config_cache.clear()
        return self.load()

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split(".")
        value = self._config_cache

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return default

            if value is None:
                return default

        return value


# ==================== 全局配置实例 ====================

# 延迟初始化，避免循环导入
_config_loader: Optional[ConfigLoader] = None


def get_config_loader() -> ConfigLoader:
    """获取全局配置加载器实例"""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
        _config_loader.load()
    return _config_loader


def get_database_config() -> DatabaseConfig:
    """获取数据库配置"""
    return get_config_loader().get_database_config()


def get_redis_config() -> RedisConfig:
    """获取 Redis 配置"""
    return get_config_loader().get_redis_config()


def get_spider_config() -> SpiderConfig:
    """获取爬虫配置"""
    return get_config_loader().get_spider_config()


def get_log_config() -> LogConfig:
    """获取日志配置"""
    return get_config_loader().get_log_config()


def get_fanqie_config() -> FanqieConfig:
    """获取番茄小说配置"""
    return get_config_loader().get_fanqie_config()


def get_alert_config() -> AlertConfig:
    """获取告警配置"""
    return get_config_loader().get_alert_config()


__all__ = [
    # 配置模型
    "DatabaseConfig",
    "RedisConfig",
    "SpiderConfig",
    "FanqieConfig",
    "LogConfig",
    "AlertConfig",
    # 配置加载器
    "ConfigLoader",
    "get_config_loader",
    # 便捷函数
    "get_database_config",
    "get_redis_config",
    "get_spider_config",
    "get_log_config",
    "get_fanqie_config",
]
