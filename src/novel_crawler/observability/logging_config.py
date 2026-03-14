#!/usr/bin/env python3
"""
统一日志配置模块
- 结构化日志（JSON 格式）
- 多处理器支持（文件 + 控制台）
- 日志级别动态调整
"""
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from loguru import logger


class LoggingConfigurator:
    """
    日志配置器
    - 单例模式
    - 支持多环境配置
    - 支持动态日志级别
    """
    _instance: Optional['LoggingConfigurator'] = None
    _configured = False

    def __new__(cls) -> 'LoggingConfigurator':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self._log_dir = Path(__file__).parent.parent.parent / "logs"
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def configure(
        self,
        level: str = "INFO",
        format_str: Optional[str] = None,
        log_to_file: bool = True,
        log_to_console: bool = True,
        json_format: bool = False,
        rotation: str = "00:00",
        retention: str = "7 days",
    ) -> None:
        """
        配置日志

        Args:
            level: 日志级别
            format_str: 日志格式
            log_to_file: 是否输出到文件
            log_to_console: 是否输出到控制台
            json_format: 是否使用 JSON 格式
            rotation: 日志切分策略
            retention: 日志保留策略
        """
        if self._configured:
            logger.warning("日志已配置，忽略本次配置请求")
            return

        # 移除所有默认处理器
        logger.remove()

        # 控制台格式
        console_format = format_str or "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"

        # 文件格式（更详细）
        file_format = format_str or (
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
            "{name}:{function}:{line} | {extra} | {message}"
        )

        # JSON 格式处理器（用于日志收集系统）
        if json_format:
            logger.add(
                self._log_dir / "crawler.json",
                format="{message}",
                level=level,
                rotation=rotation,
                retention=retention,
                encoding="utf-8",
                serialize=True,  # JSON 格式
                enqueue=True,  # 线程安全
            )

        # 控制台处理器
        if log_to_console:
            logger.add(
                sys.stdout,
                format=console_format,
                level=level,
                colorize=not json_format,
                enqueue=True,
            )

        # 文件处理器 - 所有日志
        if log_to_file:
            logger.add(
                self._log_dir / "crawler_{time:YYYY-MM-DD}.log",
                format=file_format,
                level=level,
                rotation=rotation,
                retention=retention,
                encoding="utf-8",
                enqueue=True,
                backtrace=True,  # 显示完整异常堆栈
                diagnose=True,   # 显示局部变量
            )

            # 错误日志单独文件
            logger.add(
                self._log_dir / "error_{time:YYYY-MM-DD}.log",
                format=file_format,
                level="ERROR",
                rotation=rotation,
                retention=retention,
                encoding="utf-8",
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )

        self._configured = True
        logger.info(f"日志配置完成 - 级别：{level}, 目录：{self._log_dir}")

    def set_level(self, level: str) -> None:
        """
        动态设置日志级别

        Args:
            level: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        """
        # 注意：loguru 不支持直接修改已添加处理器的级别
        # 需要重新配置
        logger.warning("动态修改日志级别需要重新配置")
        self._configured = False
        self.configure(level=level)

    def get_log_dir(self) -> Path:
        """获取日志目录"""
        return self._log_dir

    def add_handler(
        self,
        sink: Any,
        level: str = "INFO",
        format_str: Optional[str] = None,
        **kwargs
    ) -> int:
        """
        添加自定义处理器

        Args:
            sink: 输出目标（文件路径/流/函数）
            level: 日志级别
            format_str: 日志格式
            **kwargs: 其他参数传递给 logger.add

        Returns:
            处理器 ID（可用于移除）
        """
        return logger.add(sink, level=level, format=format_str, **kwargs)

    def remove_handler(self, handler_id: int) -> None:
        """移除处理器"""
        logger.remove(handler_id)


# ==================== 全局实例 ====================

_logging_configurator: Optional[LoggingConfigurator] = None


def get_logging_configurator() -> LoggingConfigurator:
    """获取全局日志配置器实例"""
    global _logging_configurator
    if _logging_configurator is None:
        _logging_configurator = LoggingConfigurator()
    return _logging_configurator


def setup_logging(
    level: str = "INFO",
    log_to_file: bool = True,
    json_format: bool = False,
    **kwargs
) -> None:
    """
    快速设置日志

    Args:
        level: 日志级别
        log_to_file: 是否输出到文件
        json_format: 是否使用 JSON 格式
        **kwargs: 其他配置参数
    """
    configurator = get_logging_configurator()
    configurator.configure(level=level, log_to_file=log_to_file, json_format=json_format, **kwargs)


# ==================== 日志上下文 ====================

class LogContext:
    """
    日志上下文管理器
    用于在日志中添加追踪 ID 等上下文信息
    """

    def __init__(self, **context):
        self.context = context
        self._token = None

    def __enter__(self):
        # loguru 的 context 功能有限，这里简单记录
        logger.info(f"进入上下文：{self.context}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(f"上下文执行失败：{exc_val}")
        else:
            logger.info("上下文执行完成")
        return False


def log_with_context(message: str, context: Dict[str, Any], level: str = "INFO") -> None:
    """
    带上下文的日志

    Args:
        message: 日志消息
        context: 上下文信息
        level: 日志级别
    """
    context_str = " | ".join(f"{k}={v}" for k, v in context.items())
    getattr(logger, level.lower(), logger.info)(f"[{context_str}] {message}")
