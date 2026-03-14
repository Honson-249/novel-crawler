#!/usr/bin/env python3
"""
数据库初始化脚本

使用方法:
    python scripts/init_db.py
    python scripts/init_db.py --sql-file path/to/fullfanqie_ranks.sql
"""

import argparse
import sys
from pathlib import Path

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from loguru import logger
from src.novel_crawler.config.database import DatabaseManager


def main():
    parser = argparse.ArgumentParser(description='数据库初始化脚本')
    parser.add_argument(
        '--sql-file',
        type=str,
        default=None,
        help='SQL 文件路径（可选，不传则使用内建建表语句）'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='local',
        help='环境名称（local/dev/prod）'
    )

    args = parser.parse_args()

    # 配置日志
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )

    logger.info(f"开始数据库初始化 - 环境：{args.env}")

    # 创建数据库管理器
    db_manager = DatabaseManager()

    # 执行初始化
    if args.sql_file:
        sql_path = Path(args.sql_file)
        if not sql_path.exists():
            logger.error(f"SQL 文件不存在：{sql_path}")
            sys.exit(1)
        logger.info(f"使用 SQL 文件：{sql_path}")
        success = db_manager.init_database(str(sql_path))
    else:
        logger.info("使用内建建表语句")
        success = db_manager.init_database()

    if success:
        logger.info("数据库初始化完成")
        sys.exit(0)
    else:
        logger.error("数据库初始化失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
