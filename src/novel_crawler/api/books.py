"""
书籍数据 API 路由
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from src.novel_crawler.schemas.book import BookData, BookDetail, BookListResponse
from src.novel_crawler.services.book_service import BookService, get_book_service

router = APIRouter(prefix="/books", tags=["书籍数据"])


def get_service() -> BookService:
    """依赖注入：获取书籍服务"""
    return get_book_service()


@router.get("", response_model=BookListResponse, summary="获取书籍列表")
async def get_book_list(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    board_name: Optional[str] = Query(None, description="榜单名称"),
    sub_category: Optional[str] = Query(None, description="分类名称"),
    book_title: Optional[str] = Query(None, description="书名搜索"),
    batch_date: Optional[str] = Query(None, description="批次日期"),
    service: BookService = Depends(get_service)
):
    """
    获取书籍列表，支持分页和筛选

    - **page**: 页码，默认 1
    - **page_size**: 每页数量，默认 20，最大 100
    - **board_name**: 榜单名称筛选
    - **sub_category**: 分类名称筛选
    - **book_title**: 书名搜索（支持模糊匹配）
    - **batch_date**: 批次日期筛选（格式：YYYY-MM-DD）
    """
    result = service.get_book_list(
        page=page,
        page_size=page_size,
        board_name=board_name,
        sub_category=sub_category,
        book_title=book_title,
        batch_date=batch_date
    )

    return BookListResponse(**result)


@router.get("/{book_id}", response_model=BookDetail, summary="获取书籍详情")
async def get_book_detail(
    book_id: str,
    service: BookService = Depends(get_service)
):
    """
    获取指定书籍的详细信息，包含章节列表

    - **book_id**: 书籍 ID
    """
    book = service.get_book_detail(book_id)

    if not book:
        raise HTTPException(status_code=404, detail="书籍不存在")

    return BookDetail(**book)
