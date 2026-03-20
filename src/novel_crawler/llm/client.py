"""
大模型 API 客户端

职责：
- 封装与 OpenAI 兼容接口的 HTTP 通信
- 支持多条记录合并为一次 LLM 请求（默认 5 条/次）
- 使用 asyncio.Semaphore 控制并发，避免超出 API 限速
- 空值 / None 直接透传，不消耗 API 调用
"""
import asyncio
import json
import re
import traceback
from typing import Dict, List, Optional

import httpx
from loguru import logger

from src.novel_crawler.config.config_loader import get_llm_config

# 系统 prompt
_SYSTEM_PROMPT = (
    "你是影视内容翻译专家，专注于短剧内容的中文本地化。"
    "用户会给你一个 JSON 数组，每个元素包含一条短剧的各类文本字段（含 _idx 索引）。"
    "请将每个元素中所有字段的值翻译为简体中文，严格遵守以下规则：\n"
    "1. 标题、标签类字段（board_name/series_title/tags）：保持简洁准确\n"
    "2. 简介类字段（synopsis）：保持原文风格和情感，译文必须是连续的单行文本\n"
    "3. 数组类字段（值为 JSON 数组字符串）：翻译数组中每个元素，保持 JSON 数组格式\n"
    "4. 空字符串或 null 保持原样\n"
    "5. _idx 字段原样保留，不翻译\n"
    "6. 译文中如需使用双引号，必须用中文引号（\u201c\u201d 或 『』）替代，严禁使用英文双引号\n"
    "7. 【重要】所有字段值严禁包含真实换行符（\\n 或 \\r），所有内容必须在同一行内输出\n"
    "8. 【重要】输出必须是合法的单行 JSON 数组，数组中每个元素之间用逗号分隔，整体用 [ ] 包裹\n"
    "9. 只返回翻译后的 JSON 数组，不要任何解释、前缀或 markdown 代码块"
)


class LLMClient:
    """大模型 API 客户端（OpenAI 兼容接口）"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        max_concurrency: Optional[int] = None,
        timeout: Optional[int] = None,
    ):
        config = get_llm_config()
        self._api_key = api_key or config.api_key
        self._base_url = (base_url or config.base_url).rstrip("/")
        self._model = model or config.model
        self._timeout = timeout or config.timeout
        self._semaphore = asyncio.Semaphore(max_concurrency or config.max_concurrency)

    # ==================== 核心接口：多条记录一次翻译 ====================

    async def translate_records_batch(
        self, records: List[Dict[str, Optional[str]]]
    ) -> List[Dict[str, Optional[str]]]:
        """
        将多条记录的待翻译字段合并为一次 LLM 请求

        Args:
            records: 列表，每项为 {字段名: 原始值}

        Returns:
            与输入等长的翻译结果列表，顺序一致
        """
        if not records:
            return records

        # 过滤空值，给每条加上 _idx 用于对齐
        # synopsis 中的英文双引号替换为单引号，避免放入 JSON 后破坏格式
        payload_list = []
        for i, rec in enumerate(records):
            item = {"_idx": i}
            for k, v in rec.items():
                if v and str(v).strip():
                    cleaned = str(v).replace("\r", "").replace("\n", " ")
                    if k == "synopsis":
                        cleaned = cleaned.replace('"', "'")
                    item[k] = cleaned
            payload_list.append(item)

        async with self._semaphore:
            translated_list = await self._call_translate_batch(payload_list)

        # 按 _idx 对齐，合并回原始记录
        translated_map: Dict[int, Dict] = {}
        for item in translated_list:
            idx = item.get("_idx")
            if idx is not None:
                translated_map[int(idx)] = item

        results = []
        for i, rec in enumerate(records):
            merged = dict(rec)
            trans = translated_map.get(i, {})
            for k, v in trans.items():
                if k != "_idx" and v is not None:
                    merged[k] = v
            results.append(merged)

        return results

    # ==================== 兼容接口（供标签缓存单项翻译使用）====================

    async def translate_to_zh(self, text: str) -> str:
        """单段文本翻译（用于标签缓存的单项翻译）"""
        if not text or not text.strip():
            return text
        results = await self.translate_records_batch([{"text": text}])
        return results[0].get("text") or text

    # ==================== 内部实现 ====================

    async def _call_translate_with_prompt(
        self, payload_list: List[Dict], system_prompt: str
    ) -> List[Dict]:
        """
        使用自定义 system_prompt 调用 LLM API 翻译一批记录。
        若输出被截断（finish_reason=length）且批次多于 1 条，自动拆成单条重试。
        """
        user_content = json.dumps(payload_list, ensure_ascii=False)
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.1,
            "enable_thinking": False,
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self._base_url}/chat/completions"

        raw = ""
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                choice = data["choices"][0]
                raw = choice["message"]["content"].strip()
                finish_reason = choice.get("finish_reason", "")

                if raw.startswith("```"):
                    raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

                if finish_reason == "length" and len(payload_list) > 1:
                    logger.warning(
                        f"[LLM] 输出被截断（finish_reason=length），"
                        f"将 {len(payload_list)} 条拆分为逐条重试"
                    )
                    results = []
                    for item in payload_list:
                        single = await self._call_translate_with_prompt([item], system_prompt)
                        results.extend(single)
                    return results

                translated = json.loads(raw)
                if not isinstance(translated, list):
                    translated = [translated]
                return translated

        except httpx.HTTPStatusError as e:
            logger.error(f"[LLM] API 错误 {e.response.status_code}：{e.response.text[:200]}")
            return []
        except (httpx.RequestError, KeyError) as e:
            logger.error(f"[LLM] 请求失败：{type(e).__name__}: {e}")
            return []
        except json.JSONDecodeError as e:
            fixed = self._try_fix_json(raw)
            if fixed is not None:
                return fixed
            logger.error(f"[LLM] JSON 解析失败：{e}，原始内容：{raw[:300]}")
            if len(payload_list) > 1:
                results = []
                for item in payload_list:
                    single = await self._call_translate_with_prompt([item], system_prompt)
                    results.extend(single)
                return results
            return []

    async def _call_translate_batch(
        self, payload_list: List[Dict]
    ) -> List[Dict]:
        """
        调用 LLM API，一次翻译多条记录。
        若输出被截断（finish_reason=length）且批次多于 1 条，自动拆成单条重试。
        """
        return await self._call_translate_with_prompt(payload_list, _SYSTEM_PROMPT)

    @staticmethod
    def _try_fix_json(raw: str) -> Optional[List[Dict]]:
        """
        尝试修复模型返回的不合法 JSON。

        根本原因：模型翻译 synopsis 时保留了原文的英文双引号（如 "新婚挑战"），
        未转义导致 JSON 字符串提前结束。内容本身是完整的，只是格式非法。

        修复策略：
        1. json-repair 库（能处理未转义引号等各种格式问题，优先使用）
        2. 补全末尾（兜底，处理极少数 json-repair 也失败的情况）
        """
        # 策略1：json-repair（优先）
        try:
            import json_repair  # type: ignore
            result = json_repair.loads(raw)
            if isinstance(result, list) and result:
                return result
        except ImportError:
            pass
        except Exception:
            pass

        # 策略2：补全末尾兜底
        for suffix in ['"}]', "'}]", "']", "]"]:
            try:
                result = json.loads(raw.rstrip() + suffix)
                if isinstance(result, list) and result:
                    return result
            except (json.JSONDecodeError, ValueError):
                continue

        return None


# 全局单例
_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """获取全局 LLMClient 单例"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client
