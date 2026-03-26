#!/usr/bin/env python3
"""
ReelShort CSV 数据下载 API

提供 HTTP 接口供数据服务拉取 ReelShort 爬取的 CSV 文件。

接口：
  GET /api/v1/reelshort/csv/{language}              - 下载今天的 CSV
  GET /api/v1/reelshort/csv/{batch_date}/{language} - 下载指定日期的 CSV

安全：
  需要 X-API-Key 请求头进行验证（如果配置了 REELSHORT_API_KEY）
"""
import os
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.responses import FileResponse

router = APIRouter(prefix="/reelshort", tags=["ReelShort 数据"])

# ==================== 配置 ====================

# 从环境变量读取 API Key
API_KEY = os.getenv("REELSHORT_API_KEY", "")

# CSV 数据根目录（相对路径，相对于项目根目录）
CSV_BASE_DIR = Path("data/reelshort")


# ==================== 依赖项 ====================

async def verify_api_key(x_api_key: str | None = Header(None, alias="X-API-Key")):
    """
    验证 API Key

    - 如果环境变量未配置 API_KEY，则跳过验证（方便本地开发）
    - 如果配置了 API_KEY，则必须提供正确的 X-API-Key 请求头

    Returns:
        API Key 字符串（验证通过时）

    Raises:
        HTTPException: 401 验证失败
    """
    if not API_KEY:
        return None  # 未配置 API Key，跳过验证

    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key


# ==================== 工具函数 ====================

def get_today_str() -> str:
    """获取今天的日期字符串（YYYY-MM-DD）"""
    return datetime.now().strftime("%Y-%m-%d")


def is_valid_date(date_str: str) -> bool:
    """验证日期字符串格式"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# ==================== API 接口 ====================

@router.get(
    "/csv/{language}",
    dependencies=[Depends(verify_api_key)],
    summary="下载今天的 CSV 文件",
    response_class=FileResponse,
)
async def download_csv_today(language: str):
    """
    下载今天爬取的 ReelShort CSV 文件

    **路径参数:**
    - `language`: 语言代码，如 en, pt, es, zh-TW 等

    **返回:**
    - 200: CSV 文件内容（Content-Type: text/csv）
    - 404: 文件不存在（今日数据尚未爬取完成）
    - 401: API Key 验证失败

    **示例:**
    ```bash
    curl -H "X-API-Key: your-key" \\
         http://localhost:8000/api/v1/reelshort/csv/en \\
         -o reelshort_en.csv
    ```
    """
    batch_date = get_today_str()
    return await _download_csv(batch_date, language)


@router.get(
    "/csv/{batch_date}/{language}",
    dependencies=[Depends(verify_api_key)],
    summary="下载指定日期的 CSV 文件",
    response_class=FileResponse,
)
async def download_csv_by_date(batch_date: str, language: str):
    """
    下载指定日期爬取的 ReelShort CSV 文件

    **路径参数:**
    - `batch_date`: 批次日期，格式 YYYY-MM-DD
    - `language`: 语言代码，如 en, pt, es, zh-TW 等

    **返回:**
    - 200: CSV 文件内容
    - 400: 日期格式错误
    - 404: 文件不存在
    - 401: API Key 验证失败

    **示例:**
    ```bash
    curl -H "X-API-Key: your-key" \\
         http://localhost:8000/api/v1/reelshort/csv/2026-03-24/en \\
         -o reelshort_2026-03-24_en.csv
    ```
    """
    if not is_valid_date(batch_date):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {batch_date}. Use YYYY-MM-DD"
        )

    return await _download_csv(batch_date, language)


async def _download_csv(batch_date: str, language: str):
    """
    内部函数：处理 CSV 文件下载逻辑

    文件查找顺序：
    1. {language}.csv - 已完成，可下载
    2. crawling_{language}.csv - 正在爬取中（返回 404）
    """
    file_dir = CSV_BASE_DIR / batch_date

    # 优先查找已完成的文件（无 crawlong 前缀）
    csv_path = file_dir / f"{language}.csv"

    if csv_path.exists():
        return FileResponse(
            path=csv_path,
            media_type="text/csv",
            filename=f"reelshort_{batch_date}_{language}.csv",
        )

    # 检查是否正在爬取中
    crawling_path = file_dir / f"crawling_{language}.csv"
    if crawling_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Data for {language} is still being crawled. Try again later."
        )

    # 文件不存在
    raise HTTPException(
        status_code=404,
        detail=f"CSV file not found: {batch_date}/{language}.csv"
    )
