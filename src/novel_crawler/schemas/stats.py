"""
统计相关的 Pydantic 模型
"""

from pydantic import BaseModel, Field


class CategoryStats(BaseModel):
    """分类统计"""
    sub_category: str = Field(..., description="分类名称")
    book_count: int = Field(..., description="书籍数量")
    latest_batch_date: str = Field(..., description="最新批次日期")


class SummaryStats(BaseModel):
    """汇总统计"""
    total_books: int = Field(..., description="总书籍数")
    total_records: int = Field(..., description="总记录数")
    latest_batch_date: str = Field(None, description="最新批次日期")
    category_count: int = Field(..., description="分类数量")
