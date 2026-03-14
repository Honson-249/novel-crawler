"""
缓存管理模块 - 用于缓存书籍状态和更新时间
使用 Redis 作为缓存后端，缓存有效期 7 天

缓存结构:
- key: book_status:{book_id}
- value: {"last_crawl_time": "2026-03-13 10:30", "book_status": "连载中", "book_update_time": "2026-03-13"}
- TTL: 7 天

注意：缓存只用于判断是否需要爬取详情页获取章节列表，榜单页的元数据（书名、作者、排名等）每次都要实时获取
"""
import json
import redis
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from loguru import logger

# Redis 配置从 settings 导入
from src.novel_crawler.config import REDIS_CONFIG

# 数据库管理器导入
from src.novel_crawler.config.database import db_manager

# 缓存有效期（7 天）
CACHE_TTL_HOURS = 24 * 7

# UTC+8 时区
UTC8 = timezone(timedelta(hours=8))

# Redis 客户端（懒加载）
_redis_client: Optional[redis.Redis] = None


def get_redis_client() -> redis.Redis:
    """获取 Redis 客户端连接（单例模式）"""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_CONFIG["host"],
            port=REDIS_CONFIG["port"],
            db=REDIS_CONFIG["db"],
            password=REDIS_CONFIG.get("password"),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        # 测试连接
        try:
            _redis_client.ping()
            logger.info(f"Redis 连接成功：{REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}, db={REDIS_CONFIG['db']}")
        except redis.ConnectionError as e:
            logger.error(f"Redis 连接失败：{e}")
            raise
    return _redis_client


def get_utc8_now() -> datetime:
    """获取 UTC+8 时区的时间"""
    return datetime.now(UTC8)


def get_utc8_now_str() -> str:
    """获取 UTC+8 时区的时间字符串（精确到分钟）"""
    return get_utc8_now().strftime("%Y-%m-%d %H:%M")


def _get_cache_key(book_id: str) -> str:
    """生成缓存键"""
    return f"book_status:{book_id}"


def get_book_cache(book_id: str) -> Optional[Dict[str, Any]]:
    """
    从缓存获取书籍状态信息

    Args:
        book_id: 书籍 ID

    Returns:
        如果缓存存在且未过期，返回 {"last_crawl_time": str, "book_status": str}；否则返回 None
    """
    try:
        client = get_redis_client()
        cache_key = _get_cache_key(book_id)

        data = client.get(cache_key)
        if data:
            logger.debug(f"缓存命中：book_id={book_id}")
            return json.loads(data)
        else:
            logger.debug(f"缓存未命中：book_id={book_id}")
            return None
    except redis.RedisError as e:
        logger.error(f"获取缓存失败：{e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"解析缓存数据失败：{e}")
        return None


def set_book_cache(book_id: str, book_status: str, last_crawl_time: str = None, book_update_time: str = None):
    """
    设置书籍缓存

    Args:
        book_id: 书籍 ID
        book_status: 书籍状态（连载中/已完结）
        last_crawl_time: 爬取时间字符串，为 None 时不设置（保留原有值或留空）
        book_update_time: 榜单页上的更新时间（可选）
    """
    try:
        client = get_redis_client()
        cache_key = _get_cache_key(book_id)

        # 先获取现有缓存（如果有）
        existing_data = client.get(cache_key)
        if existing_data:
            cache_data = json.loads(existing_data)
        else:
            cache_data = {}

        # 更新状态
        cache_data["book_status"] = book_status

        # 只有传入时才更新 last_crawl_time
        if last_crawl_time:
            cache_data["last_crawl_time"] = last_crawl_time

        # 如果有榜单更新时间，也存入缓存
        if book_update_time:
            cache_data["book_update_time"] = book_update_time

        # 计算过期时间（秒）
        ttl_seconds = CACHE_TTL_HOURS * 3600

        # 存储 JSON 数据
        client.setex(
            cache_key,
            ttl_seconds,
            json.dumps(cache_data, ensure_ascii=False)
        )

        logger.debug(f"已设置缓存：book_id={book_id}, status={book_status}, crawl_time={cache_data.get('last_crawl_time', 'N/A')}, update_time={book_update_time or 'N/A'}, 有效期={CACHE_TTL_HOURS}小时")
    except (redis.RedisError, json.JSONDecodeError) as e:
        logger.error(f"设置缓存失败：{e}")


def update_book_status(book_id: str, book_status: str) -> bool:
    """
    仅更新书籍状态（保留原有爬取时间）

    Args:
        book_id: 书籍 ID
        book_status: 书籍状态

    Returns:
        是否更新成功
    """
    try:
        client = get_redis_client()
        cache_key = _get_cache_key(book_id)

        # 先获取现有缓存
        data = client.get(cache_key)
        if data:
            cache_data = json.loads(data)
            cache_data["book_status"] = book_status
            # 保留原有时间
            client.setex(
                cache_key,
                CACHE_TTL_HOURS * 3600,
                json.dumps(cache_data, ensure_ascii=False)
            )
            logger.debug(f"已更新书籍状态：book_id={book_id}, status={book_status}")
            return True
        else:
            # 如果没有缓存，创建新缓存
            set_book_cache(book_id, book_status)
            return True
    except (redis.RedisError, json.JSONDecodeError) as e:
        logger.error(f"更新书籍状态失败：{e}")
        return False


def delete_book_cache(book_id: str) -> bool:
    """
    删除指定书籍缓存

    Args:
        book_id: 书籍 ID

    Returns:
        是否删除成功
    """
    try:
        client = get_redis_client()
        cache_key = _get_cache_key(book_id)
        result = client.delete(cache_key)
        logger.debug(f"已删除缓存：book_id={book_id}")
        return result > 0
    except redis.RedisError as e:
        logger.error(f"删除缓存失败：{e}")
        return False


def clear_all_cache() -> int:
    """
    清空所有书籍缓存

    Returns:
        清理的键数量
    """
    try:
        client = get_redis_client()
        # 查找所有 book_status:* 键
        keys = client.keys("book_status:*")
        if keys:
            count = client.delete(*keys)
            logger.info(f"已清空所有缓存：{count}条记录")
            return count
        return 0
    except redis.RedisError as e:
        logger.error(f"清空缓存失败：{e}")
        return 0


def get_cache_stats() -> Dict[str, Any]:
    """
    获取缓存统计信息

    Returns:
        {"total_count": int, "ongoing_count": int, "completed_count": int}
    """
    try:
        client = get_redis_client()
        keys = client.keys("book_status:*")

        total_count = len(keys)
        ongoing_count = 0
        completed_count = 0

        for key in keys:
            data = client.get(key)
            if data:
                cache_data = json.loads(data)
                if cache_data.get("book_status") == "连载中":
                    ongoing_count += 1
                elif cache_data.get("book_status") == "已完结":
                    completed_count += 1

        return {
            "total_count": total_count,
            "ongoing_count": ongoing_count,
            "completed_count": completed_count,
        }
    except (redis.RedisError, json.JSONDecodeError) as e:
        logger.error(f"获取缓存统计失败：{e}")
        return {"total_count": 0, "ongoing_count": 0, "completed_count": 0}


def load_books_from_db_to_cache(batch_date: str = None, force_load: bool = False) -> int:
    """
    从数据库加载书籍数据到缓存中

    Args:
        batch_date: 批次日期，格式 YYYY-MM-DD，默认为数据库中最近的日期
        force_load: 是否强制加载，默认 False（只在缓存为空时加载）

    Returns:
        成功加载到缓存的记录数
    """
    # 如果没有传入日期，先查询数据库中最近的批次日期
    if batch_date is None:
        batch_date = get_latest_batch_date_from_db()
        if not batch_date:
            logger.warning("数据库中没有找到任何书籍数据")
            return 0

    # 检查缓存是否为空
    if not force_load:
        stats = get_cache_stats()
        if stats["total_count"] > 0:
            logger.info(f"缓存中已有 {stats['total_count']} 条记录，跳过加载")
            return 0

    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        # 查询指定日期的所有书籍，获取 book_id、book_status 和 updated_at
        # 只加载有章节数据的记录（chapter_list_json 不为空）
        cursor.execute("""
            SELECT DISTINCT book_id, book_status, updated_at
            FROM fanqie_ranks
            WHERE batch_date = %s
              AND book_id IS NOT NULL AND book_id != ''
              AND chapter_list_json IS NOT NULL AND chapter_list_json != ''
        """, (batch_date,))
        logger.info(f"开始从数据库加载 {batch_date} 的书籍数据到缓存...")

        rows = cursor.fetchall()
        if not rows:
            logger.warning(f"数据库中没有找到 {batch_date} 的书籍数据")
            return 0

        loaded_count = 0
        for row in rows:
            book_id = row[0]
            book_status = row[1] or '连载中'
            updated_at = row[2]

            # 使用数据库中的 updated_at 作为爬取时间
            if updated_at:
                # 格式化为 "YYYY-MM-DD HH:MM" 格式
                crawl_time = updated_at.strftime("%Y-%m-%d %H:%M")
            else:
                # 如果没有 updated_at，使用批次日期的 0 点作为备用
                crawl_time = f"{batch_date} 00:00"

            set_book_cache(book_id, book_status, crawl_time)
            loaded_count += 1

        logger.info(f"成功从数据库加载 {loaded_count}/{len(rows)} 本书到缓存")
        return loaded_count

    except Exception as e:
        logger.error(f"从数据库加载缓存失败：{e}")
        return 0
    finally:
        conn.close()


def get_latest_batch_date_from_db() -> Optional[str]:
    """
    获取数据库中最近的批次日期

    Returns:
        最近的批次日期字符串（YYYY-MM-DD），没有数据返回 None
    """
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT MAX(batch_date)
            FROM fanqie_ranks
            WHERE batch_date IS NOT NULL
        """)
        result = cursor.fetchone()[0]
        if result:
            # 如果是 date 对象，转换为字符串
            return str(result)
        return None
    except Exception as e:
        logger.error(f"查询最近批次日期失败：{e}")
        return None
    finally:
        conn.close()
