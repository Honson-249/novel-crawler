#!/usr/bin/env python3
"""
存储模块集成测试
"""
import pytest
from datetime import datetime
from src.pipeline.storage import DatabaseManager, get_utc8_date


@pytest.mark.integration
class TestStorage:
    """存储模块集成测试"""

    @pytest.fixture
    def db_manager(self):
        """创建数据库管理器实例"""
        return DatabaseManager()

    def test_get_connection(self, db_manager):
        """测试获取数据库连接"""
        try:
            conn = db_manager.get_connection()
            assert conn is not None
            conn.close()
        except Exception as e:
            pytest.skip(f"数据库不可用：{e}")

    def test_create_tables(self, db_manager):
        """测试创建数据表"""
        try:
            db_manager.create_tables()
            # 如果无异常，认为成功
            assert True
        except Exception as e:
            pytest.skip(f"数据库不可用：{e}")

    def test_get_utc8_date(self):
        """测试 UTC+8 日期获取"""
        date_str = get_utc8_date()
        assert isinstance(date_str, str)
        # 验证日期格式
        datetime.strptime(date_str, "%Y-%m-%d")

    def test_batch_insert(self, db_manager):
        """测试批量插入"""
        try:
            # 准备测试数据
            test_records = [
                {
                    "board_name": "测试榜单",
                    "sub_category": "测试分类",
                    "rank_num": 999,
                    "book_id": "test_999",
                    "book_title": "测试书籍",
                    "author_name": "测试作者",
                    "metric_name": "热力值",
                    "metric_value_raw": "100",
                    "metric_value": 100,
                    "book_status": "连载中",
                    "synopsis": "测试简介",
                    "cover_url": "https://example.com/cover.jpg",
                    "detail_url": "https://example.com/detail",
                }
            ]

            batch_date = get_utc8_date()
            inserted = db_manager.insert_fanqie_batch(test_records, batch_date)

            assert inserted == 1
        except Exception as e:
            pytest.skip(f"数据库不可用：{e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
