"""
数据库连接管理模块

职责:
- 数据库连接管理（连接池）
- 数据库初始化（建表、加载 SQL 脚本）

注意：本模块不包含任何业务逻辑，业务逻辑已迁移至 Service 层和 DAO 层。
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from loguru import logger

from src.novel_crawler.config.config_loader import get_database_config

# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))

# 全局连接池（懒加载，首次调用 get_connection 时初始化）
_pool = None


def get_utc8_date() -> str:
    """获取 UTC+8 时区的日期字符串"""
    return datetime.now(UTC8).strftime("%Y-%m-%d")


class DatabaseManager:
    """数据库管理器 - MySQL"""

    def __init__(self, config: Optional[dict] = None):
        """
        初始化数据库管理器

        Args:
            config: 数据库配置字典，不传则使用默认配置
        """
        self._config = config
        self._connection = None

    @property
    def config(self) -> dict:
        """获取数据库配置"""
        if self._config is None:
            db_config = get_database_config()
            self._config = db_config.connection_params
        return self._config

    def get_connection(self):
        """从连接池获取数据库连接"""
        global _pool
        if _pool is None:
            _pool = self._create_pool()
        return _pool.connection()

    def _create_pool(self):
        """创建数据库连接池"""
        try:
            import pymysql
            from dbutils.pooled_db import PooledDB
            pool = PooledDB(
                creator=pymysql,
                maxconnections=10,
                mincached=2,
                maxcached=5,
                blocking=True,
                **self.config,
            )
            logger.info("数据库连接池初始化完成")
            return pool
        except ImportError as e:
            logger.error(f"缺少依赖，请安装：pip install pymysql dbutils ({e})")
            raise

    def init_database(self, sql_file: Optional[str] = None) -> bool:
        """
        初始化数据库（执行建表 SQL）

        Args:
            sql_file: SQL 文件路径，不传则使用内建建表语句

        Returns:
            是否成功
        """
        if sql_file:
            return self._execute_sql_file(sql_file)
        else:
            return self._create_tables()

    def _execute_sql_file(self, sql_file: str) -> bool:
        """执行 SQL 文件"""
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            conn = self.get_connection()
            cursor = conn.cursor()
            statements = sql_content.split(';')

            for stmt in statements:
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    cursor.execute(stmt)

            conn.commit()
            conn.close()
            logger.info(f"执行 SQL 文件完成：{sql_file}")
            return True

        except Exception as e:
            logger.error(f"执行 SQL 文件失败：{e}")
            return False

    def _create_tables(self) -> bool:
        """创建数据表（内建 SQL）"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fanqie_ranks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_date DATE NOT NULL,
                board_name VARCHAR(50),
                sub_category VARCHAR(50),
                rank_num INT,
                book_id VARCHAR(50),
                book_title VARCHAR(200),
                author_name VARCHAR(100),
                metric_name VARCHAR(50),
                metric_value_raw VARCHAR(50),
                metric_value BIGINT,
                tags TEXT,
                book_status VARCHAR(20),
                synopsis TEXT,
                chapter_list_json LONGTEXT,
                cover_url TEXT,
                detail_url TEXT,
                book_update_time VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_record (batch_date, board_name, sub_category, rank_num),
                INDEX idx_batch (batch_date),
                INDEX idx_board (board_name),
                INDEX idx_category (sub_category),
                INDEX idx_book_id (book_id),
                INDEX idx_book_batch (book_id, batch_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                ALTER TABLE fanqie_ranks
                MODIFY COLUMN chapter_list_json LONGTEXT
            """)

            # 为存量数据库补充联合索引（幂等，索引已存在时忽略错误）
            try:
                cursor.execute("""
                    ALTER TABLE fanqie_ranks
                    ADD INDEX idx_book_batch (book_id, batch_date)
                """)
            except Exception:
                pass  # 索引已存在，忽略

            # ==================== ReelShort 相关表 ====================

            # ReelShort 标签维表
            # 唯一键为 (language, tab_name, tag_name)，不含 batch_date
            # 同一天重复爬取不新增记录，只更新 batch_date（最近爬取日期）
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS reelshort_tags (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_date DATE NOT NULL COMMENT '最近爬取日期',
                language VARCHAR(50) NOT NULL,
                tab_name VARCHAR(100) NOT NULL,
                tag_name VARCHAR(500) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_tag (language, tab_name, tag_name(200)),
                INDEX idx_lang_tab (language, tab_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # ReelShort 榜单明细表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS reelshort_drama (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_date DATE NOT NULL,
                language VARCHAR(50) NOT NULL,
                board_name VARCHAR(100),
                sub_category VARCHAR(200),
                detail_url TEXT,
                series_title VARCHAR(500),
                play_count_raw VARCHAR(50),
                play_count BIGINT,
                favorite_count_raw VARCHAR(50),
                favorite_count BIGINT,
                tag_list_json TEXT,
                actors_tags TEXT,
                actresses_tags TEXT,
                identity_tags TEXT,
                story_beat_tags TEXT,
                genre_tags TEXT,
                synopsis TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_record (batch_date, language, board_name(50), sub_category(100), detail_url(200)),
                INDEX idx_batch (batch_date),
                INDEX idx_language (language),
                INDEX idx_board (board_name),
                INDEX idx_sub_category (sub_category(100))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # ==================== DramaShorts 相关表 ====================

            # DramaShorts 榜单明细表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dramashort_drama (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_date DATE NOT NULL,
                language VARCHAR(50) NOT NULL,
                board_name VARCHAR(100),
                board_order INT,
                detail_url TEXT,
                series_title VARCHAR(500),
                play_count_raw VARCHAR(50),
                play_count BIGINT,
                favorite_count_raw VARCHAR(50),
                favorite_count BIGINT,
                synopsis TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_record (batch_date, language, board_name(50), detail_url(200)),
                INDEX idx_batch (batch_date),
                INDEX idx_language (language),
                INDEX idx_board (board_name),
                INDEX idx_board_order (board_order)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            conn.commit()
            logger.info("MySQL 数据表创建完成")
            return True

        except Exception as e:
            logger.error(f"创建数据表失败：{e}")
            conn.rollback()
            return False
        finally:
            conn.close()


# 全局 DatabaseManager 实例（单例）
_database_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """获取数据库管理器实例"""
    global _database_manager
    if _database_manager is None:
        _database_manager = DatabaseManager()
    return _database_manager


def db_manager() -> DatabaseManager:
    """获取数据库管理器实例（别名）"""
    return get_db_manager()
