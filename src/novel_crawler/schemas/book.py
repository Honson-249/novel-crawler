"""
书籍相关的 Pydantic 模型
"""

from pydantic import BaseModel, Field
from typing import Optional, List


class BookBase(BaseModel):
    """书籍基础信息"""
    book_id: str = Field(..., description="书籍 ID")
    book_title: str = Field(..., description="书名")
    author_name: str = Field(..., description="作者")
    book_status: str = Field(..., description="书籍状态")
    synopsis: Optional[str] = Field(None, description="简介")
    cover_url: Optional[str] = Field(None, description="封面 URL")
    detail_url: str = Field(..., description="详情页 URL")


class BookCreate(BookBase):
    """创建书籍请求"""
    pass


class BookData(BookBase):
    """书籍数据（榜单记录）"""
    id: int = Field(..., description="记录 ID")
    batch_date: str = Field(..., description="批次日期")
    board_name: str = Field(..., description="榜单名称")
    sub_category: str = Field(..., description="细分分类")
    rank_num: int = Field(..., description="排名")
    metric_name: str = Field(..., description="指标名称")
    metric_value_raw: str = Field(..., description="原始显示值")
    metric_value: int = Field(..., description="转换后数值")

    model_config = {"from_attributes": True}


class BookDetail(BookData):
    """书籍详情（包含章节列表）"""
    chapter_list: Optional[List[str]] = Field(None, description="章节列表")


class BookListResponse(BaseModel):
    """书籍列表响应"""
    total: int = Field(..., description="总数")
    page: int = Field(..., description="页码")
    page_size: int = Field(..., description="每页数量")
    data: List[BookData] = Field(..., description="书籍列表")
