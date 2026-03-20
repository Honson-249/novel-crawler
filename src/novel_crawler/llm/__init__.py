"""
大模型 API 模块

提供与 OpenAI 兼容接口的大模型调用能力，用于文本翻译等任务。
"""
from src.novel_crawler.llm.client import LLMClient, get_llm_client

__all__ = ["LLMClient", "get_llm_client"]
