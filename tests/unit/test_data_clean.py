#!/usr/bin/env python3
"""
数据清洗模块单元测试
"""
import pytest
from src.pipeline.clean import clean_text, parse_metric_value


class TestTextCleaning:
    """文本清洗测试类"""

    def test_clean_empty_string(self):
        """测试空字符串清洗"""
        assert clean_text("") == ""
        assert clean_text(None) == ""

    def test_clean_whitespace(self):
        """测试空白字符清洗"""
        assert clean_text("  hello  ") == "hello"
        assert clean_text("\t\nhello\r\n") == "hello"

    def test_clean_multiple_spaces(self):
        """测试多个空格清洗"""
        text = "hello   world"
        result = clean_text(text)
        # 应该保留单个空格
        assert "  " not in result

    def test_clean_html_entities(self):
        """测试 HTML 实体清洗"""
        text = "hello &nbsp; world &amp; test"
        result = clean_text(text)
        assert "&nbsp;" not in result
        assert "&amp;" not in result


class TestMetricValueParsing:
    """热度值解析测试类"""

    def test_parse_plain_number(self):
        """测试纯数字解析"""
        assert parse_metric_value("100") == 100
        assert parse_metric_value("12345") == 12345

    def test_parse_wan_unit(self):
        """测试万字单位解析"""
        assert parse_metric_value("1 万") == 10000
        assert parse_metric_value("1.5 万") == 15000
        assert parse_metric_value("10.5 万") == 105000

    def test_parse_invalid_input(self):
        """测试无效输入"""
        assert parse_metric_value("") == 0
        assert parse_metric_value(None) == 0
        assert parse_metric_value("abc") == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
