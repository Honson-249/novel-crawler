"""
DAO 层 - 数据访问对象

职责：
- 纯 SQL 执行
- 不包含业务逻辑
- 返回原始数据（dict/list）
"""

from .fanqie_rank_dao import FanqieRankDAO, get_fanqie_rank_dao
from .book_dao import BookDAO, get_book_dao


__all__ = [
    "FanqieRankDAO",
    "get_fanqie_rank_dao",
    "BookDAO",
    "get_book_dao",
]
