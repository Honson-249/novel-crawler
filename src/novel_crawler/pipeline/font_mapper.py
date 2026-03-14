"""
字体反爬映射模块
使用 OCR 生成的映射表解码番茄小说的字体反爬
"""
import json
import os
from typing import Dict, Optional
from loguru import logger


class FontMapper:
    """字体映射器"""
    
    def __init__(self, mapping_path: Optional[str] = None):
        self.mappings: Dict[str, str] = {}
        
        # 默认映射表路径
        if mapping_path is None:
            mapping_path = os.path.join(os.path.dirname(__file__), 'ocr_mapping.json')
        
        self.load_mapping(mapping_path)
    
    def load_mapping(self, mapping_path: str):
        """加载 OCR 映射表"""
        if not os.path.exists(mapping_path):
            logger.warning(f"映射表文件不存在：{mapping_path}")
            return
        
        try:
            with open(mapping_path, 'r', encoding='utf-8') as f:
                self.mappings = json.load(f)
            logger.info(f"加载字体映射表：{len(self.mappings)} 个字符映射")
        except Exception as e:
            logger.error(f"加载映射表失败：{e}")
    
    def decode_text(self, text: str) -> str:
        """
        解码包含字体反爬字符的文本
        
        Args:
            text: 原始文本（包含私有区字符）
        
        Returns:
            解码后的文本
        """
        if not text:
            return text
        
        result = []
        for char in text:
            if char in self.mappings:
                result.append(self.mappings[char])
            else:
                code = ord(char)
                if 0xE000 <= code <= 0xF8FF:
                    logger.debug(f"无映射的私有区字符：U+{code:04X} ({char})")
                else:
                    result.append(char)
        
        return ''.join(result)


# 全局映射器实例
_mapper: Optional[FontMapper] = None


def get_mapper() -> FontMapper:
    """获取全局映射器实例"""
    global _mapper
    if _mapper is None:
        _mapper = FontMapper()
    return _mapper


def decode_text(text: str) -> str:
    """解码文本"""
    return get_mapper().decode_text(text)
