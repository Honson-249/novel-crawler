#!/usr/bin/env python3
"""
迁移脚本 - 修改唯一约束为 (batch_date, board_name, sub_category, rank_num)
并清理现有的重复数据
"""
import sys
from pathlib import Path
from loguru import logger

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config import LOG_CONFIG

# 配置日志
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level=LOG_CONFIG["level"],
    colorize=False,
)

from src.novel_crawler.config.database import db_manager


def migrate_unique_constraint():
    """迁移唯一约束并清理重复数据"""
    logger.info("=" * 60)
    logger.info("开始迁移唯一约束")
    logger.info("=" * 60)

    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        # 1. 查找重复数据（同一个 batch_date + board_name + sub_category + rank_num 有多条记录）
        logger.info("\n[1] 查找重复数据...")
        cursor.execute("""
            SELECT batch_date, board_name, sub_category, rank_num, COUNT(*) as cnt,
                   GROUP_CONCAT(book_id ORDER BY id) as book_ids,
                   GROUP_CONCAT(book_title ORDER BY id) as titles,
                   GROUP_CONCAT(author_name ORDER BY id) as authors,
                   MIN(id) as min_id, MAX(id) as max_id
            FROM fanqie_ranks
            GROUP BY batch_date, board_name, sub_category, rank_num
            HAVING cnt > 1
            ORDER BY batch_date DESC, board_name, sub_category, rank_num
        """)
        duplicates = cursor.fetchall()

        if duplicates:
            logger.warning(f"发现 {len(duplicates)} 组重复数据:")
            for dup in duplicates:
                logger.warning(f"  {dup[0]} | {dup[1]} | {dup[2]} | rank={dup[3]} -> {dup[4]}条")
                logger.warning(f"    book_ids: {dup[5]}")
                logger.warning(f"    书名：{dup[6]}")
                logger.warning(f"    作者：{dup[7]}")
                logger.warning(f"    记录 ID 范围：{dup[8]} - {dup[9]}")
        else:
            logger.info("  未发现重复数据")

        # 2. 清理重复数据，只保留每组中 id 最小的一条
        if duplicates:
            logger.info("\n[2] 清理重复数据...")
            cursor.execute("""
                DELETE r1 FROM fanqie_ranks r1
                INNER JOIN (
                    SELECT batch_date, board_name, sub_category, rank_num, MIN(id) as min_id
                    FROM fanqie_ranks
                    GROUP BY batch_date, board_name, sub_category, rank_num
                    HAVING COUNT(*) > 1
                ) r2
                ON r1.batch_date = r2.batch_date
                AND r1.board_name = r2.board_name
                AND r1.sub_category = r2.sub_category
                AND r1.rank_num = r2.rank_num
                AND r1.id > r2.min_id
            """)
            deleted = cursor.rowcount
            conn.commit()
            logger.info(f"  已删除 {deleted} 条重复记录")

        # 3. 删除旧的唯一约束
        logger.info("\n[3] 删除旧的唯一约束...")
        try:
            cursor.execute("ALTER TABLE fanqie_ranks DROP INDEX unique_record")
            logger.info("  已删除旧约束：unique_record (batch_date, sub_category, book_id)")
        except Exception as e:
            logger.warning(f"  删除旧约束失败（可能不存在）: {e}")

        # 4. 添加新的唯一约束
        logger.info("\n[4] 添加新的唯一约束...")
        cursor.execute("""
            ALTER TABLE fanqie_ranks
            ADD UNIQUE KEY unique_record (batch_date, board_name, sub_category, rank_num)
        """)
        logger.info("  已添加新约束：unique_record (batch_date, board_name, sub_category, rank_num)")

        # 5. 添加 book_id 索引（用于缓存查询）
        logger.info("\n[5] 添加 book_id 索引...")
        try:
            cursor.execute("CREATE INDEX idx_book_id ON fanqie_ranks (book_id)")
            logger.info("  已添加索引：idx_book_id")
        except Exception as e:
            logger.warning(f"  添加索引失败（可能已存在）: {e}")

        conn.commit()
        logger.info("\n" + "=" * 60)
        logger.info("迁移完成！")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"迁移失败：{e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate_unique_constraint()
