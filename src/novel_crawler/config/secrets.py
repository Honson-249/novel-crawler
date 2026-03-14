#!/usr/bin/env python3
"""
密钥管理模块
- 支持从环境变量读取敏感信息
- 支持从文件读取密钥
- 禁止硬编码密钥
"""
import os
from pathlib import Path
from typing import Optional
from loguru import logger


class SecretsManager:
    """
    密钥管理器
    - 单例模式
    - 支持多种密钥来源
    """
    _instance: Optional['SecretsManager'] = None

    def __new__(cls) -> 'SecretsManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self._secrets_dir = Path(__file__).parent.parent.parent / "secrets"
        self._cache = {}

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        获取密钥值

        优先级：
        1. 环境变量
        2. secrets 目录下的文件
        3. 默认值

        Args:
            key: 密钥名称
            default: 默认值

        Returns:
            密钥值
        """
        # 先从缓存读取
        if key in self._cache:
            return self._cache[key]

        # 1. 尝试从环境变量读取
        value = os.getenv(key)
        if value:
            self._cache[key] = value
            return value

        # 2. 尝试从文件读取
        file_path = self._secrets_dir / key
        if file_path.exists():
            try:
                value = file_path.read_text().strip()
                self._cache[key] = value
                return value
            except Exception as e:
                logger.error(f"读取密钥文件失败：{file_path}, 错误：{e}")

        # 3. 返回默认值
        if default:
            self._cache[key] = default
            return default

        logger.warning(f"未找到密钥：{key}")
        return None

    def get_required(self, key: str, description: str = "") -> str:
        """
        获取必需的密钥，如果不存在则抛出异常

        Args:
            key: 密钥名称
            description: 密钥描述（用于错误提示）

        Returns:
            密钥值

        Raises:
            ValueError: 密钥不存在
        """
        value = self.get(key)
        if not value:
            desc = f" ({description})" if description else ""
            raise ValueError(
                f"缺少必需的密钥：{key}{desc}\n"
                f"请设置环境变量 {key} 或在 secrets 目录下创建同名文件"
            )
        return value

    def get_mysql_password(self) -> str:
        """获取 MySQL 密码"""
        return self.get("MYSQL_PASSWORD", "")

    def get_redis_password(self) -> Optional[str]:
        """获取 Redis 密码"""
        return self.get("REDIS_PASSWORD")

    def get_alert_webhook(self, channel: str) -> Optional[str]:
        """
        获取告警 Webhook

        Args:
            channel: 渠道名称 (feishu/dingtalk/wechat)

        Returns:
            Webhook URL
        """
        key = f"ALERT_{channel.upper()}_WEBHOOK"
        return self.get(key)

    def reload(self):
        """重新加载密钥（清空缓存）"""
        logger.info("重新加载密钥...")
        self._cache.clear()


# ==================== 全局实例 ====================

_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """获取全局密钥管理器实例"""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager


def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """获取密钥"""
    return get_secrets_manager().get(key, default)


def get_required_secret(key: str, description: str = "") -> str:
    """获取必需的密钥"""
    return get_secrets_manager().get_required(key, description)
