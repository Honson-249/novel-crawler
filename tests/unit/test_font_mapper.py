#!/usr/bin/env python3
"""
字体映射器单元测试
"""
import pytest
from src.pipeline.font_mapper import FontMapper, get_mapper, decode_text


class TestFontMapper:
    """字体映射器测试类"""

    @pytest.fixture
    def mapper(self):
        """创建映射器实例"""
        return FontMapper()

    def test_decode_empty_string(self, mapper):
        """测试空字符串解码"""
        assert mapper.decode_text("") == ""
        assert mapper.decode_text(None) is None

    def test_decode_normal_text(self, mapper):
        """测试普通文本解码（无特殊字符）"""
        text = "Hello World"
        assert mapper.decode_text(text) == text

    def test_decode_chinese_text(self, mapper):
        """测试中文文本解码"""
        text = "测试文本"
        # 如果没有映射，应该返回原文本
        result = mapper.decode_text(text)
        assert isinstance(result, str)

    def test_mapper_singleton(self):
        """测试单例模式"""
        mapper1 = get_mapper()
        mapper2 = get_mapper()
        assert mapper1 is mapper2

    def test_decode_text_function(self):
        """测试 decode_text 函数"""
        result = decode_text("test")
        assert isinstance(result, str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
