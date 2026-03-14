"""
配置中心模块

使用方式:
    from src.novel_crawler.config import get_database_config, get_redis_config
    或
    from src.novel_crawler.config.config_loader import get_config_loader
"""

from src.novel_crawler.config.config_loader import (
    get_config_loader,
    get_database_config,
    get_redis_config,
    get_spider_config,
    get_log_config,
    get_fanqie_config,
    ConfigLoader,
    DatabaseConfig,
    RedisConfig,
    SpiderConfig,
    FanqieConfig,
    LogConfig,
    AlertConfig,
)
from src.novel_crawler.config.secrets import get_secrets_manager, get_secret, get_required_secret
from src.novel_crawler.config.database import (
    DatabaseManager,
    get_db_manager,
    db_manager,
    get_utc8_date,
)

# 兼容性配置（替代 settings.py）
from src.novel_crawler.config.config_loader import get_log_config, get_redis_config, get_alert_config
from pathlib import Path

_log_config = get_log_config()
_config_dir = Path(__file__).parent

# 日志目录放在项目根目录下
LOG_DIR = _config_dir.parent.parent.parent / "logs"
LOG_CONFIG = {
    "level": _log_config.level,
    "format": _log_config.format,
    "rotation": _log_config.rotation,
    "retention": _log_config.retention,
    "encoding": _log_config.encoding,
    "log_file": LOG_DIR / "crawler_{time:YYYY-MM-DD}.log",
}

# Redis 配置
REDIS_CONFIG = get_redis_config().connection_params

# 告警配置
_alert_config = get_alert_config()
ALERT_CONFIG = {
    "enabled": _alert_config.enabled,
    "feishu_webhook": _alert_config.feishu_webhook,
}

# 数据校验配置
VALIDATOR_CONFIG = {
    "min_rows": 10,
    "max_empty_top1": True,
    "check_duplicate": True,
}

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
    # 密钥管理
    "get_secrets_manager",
    "get_secret",
    "get_required_secret",
    # 数据库管理
    "DatabaseManager",
    "get_db_manager",
    "db_manager",
    "get_utc8_date",
    # 兼容性配置（替代 settings.py）
    "LOG_CONFIG",
    "REDIS_CONFIG",
    "ALERT_CONFIG",
    "VALIDATOR_CONFIG",
]
