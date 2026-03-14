"""
任务相关的 Pydantic 模型
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TaskStatus(BaseModel):
    """任务状态响应"""
    scheduler_running: bool = Field(..., description="调度器是否运行中")
    next_run_time: Optional[str] = Field(None, description="下次执行时间")
    last_run_time: Optional[str] = Field(None, description="上次执行时间")
    last_run_status: Optional[str] = Field(None, description="上次执行状态")


class CrawlRequest(BaseModel):
    """爬取请求"""
    crawl_all: bool = Field(True, description="是否爬取所有分类")
    limit: int = Field(30, ge=1, le=100, description="每个分类爬取的书籍数量")
    detail: bool = Field(False, description="是否爬取详情页")
    category_idx: int = Field(0, ge=0, description="分类索引（不爬取所有时使用）")


class CrawlResponse(BaseModel):
    """爬取响应"""
    status: str = Field(..., description="状态")
    message: str = Field(..., description="消息")
    task_id: Optional[str] = Field(None, description="任务 ID")


class ScheduleRequest(BaseModel):
    """定时任务配置请求"""
    hour: int = Field(0, ge=0, le=23, description="执行时间 - 小时")
    minute: int = Field(0, ge=0, le=59, description="执行时间 - 分钟")
    crawl_all: bool = Field(True, description="是否爬取所有分类")
    limit: int = Field(30, ge=1, le=100, description="每个分类爬取的书籍数量")
    detail: bool = Field(False, description="是否爬取详情页")


class ScheduleResponse(BaseModel):
    """定时任务配置响应"""
    status: str = Field(..., description="状态")
    message: str = Field(..., description="消息")
    next_run_time: Optional[str] = Field(None, description="下次执行时间")
