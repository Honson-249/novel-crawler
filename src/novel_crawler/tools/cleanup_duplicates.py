#!/usr/bin/env python3
"""
清理重复数据脚本 - 不使用 JSON_LENGTH
处理策略：
1. 优先保留有章节数据的记录
2. 如果都有章节数据，保留章节数据更大的（用 LENGTH 代理）
3. 如果长度相同，保留 ID 较小的
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


def cleanup_duplicates():
    """清理重复数据"""
    logger.info("=" * 60)
    logger.info("开始清理重复数据")
    logger.info("=" * 60)

    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        # 1. 查找所有重复的组合
        logger.info("\n[1] 查找重复数据...")
        cursor.execute("""
            SELECT batch_date, board_name, sub_category, rank_num, COUNT(*) as cnt,
                   GROUP_CONCAT(id ORDER BY id) as ids,
                   GROUP_CONCAT(book_id ORDER BY id) as book_ids,
                   GROUP_CONCAT(chapter_list_json IS NOT NULL AND chapter_list_json != '' ORDER BY id) as has_chapters,
                   GROUP_CONCAT(LENGTH(chapter_list_json) ORDER BY id) as chapter_lengths,
                   MIN(id) as min_id, MAX(id) as max_id
            FROM fanqie_ranks
            GROUP BY batch_date, board_name, sub_category, rank_num
            HAVING cnt > 1
            ORDER BY batch_date DESC, board_name, sub_category, rank_num
        """)
        duplicates = cursor.fetchall()

        if not duplicates:
            logger.info("  未发现重复数据")
            return

        logger.warning(f"发现 {len(duplicates)} 组重复数据:")
        for dup in duplicates:
            batch_date, board, sub_cat, rank, cnt, ids, book_ids, has_chapters, lengths, min_id, max_id = dup
            logger.warning(f"  [{batch_date}] {board} | {sub_cat} | 排名{rank}: {cnt}条")
            logger.warning(f"    IDs: {ids}")
            logger.warning(f"    Books: {book_ids}")
            logger.warning(f"    Has chapters: {has_chapters}")
            logger.warning(f"    Chapter lengths: {lengths}")

        # 2. 对每组重复数据，确定要删除的 ID
        logger.info("\n[2] 确定要删除的记录...")
        ids_to_delete = []

        for dup in duplicates:
            batch_date, board, sub_cat, rank, cnt, ids, book_ids, has_chapters, lengths, min_id, max_id = dup

            id_list = [int(x) for x in ids.split(',')]
            has_chapter_list = [x == '1' for x in has_chapters.split(',')]
            length_list = [int(x) if x else 0 for x in lengths.split(',')]

            # 组合排序键：(有章节，章节长度，-ID) - 降序排列，取第一个保留
            # 有章节的排前面，章节长度大的排前面，ID 小的排前面
            records = list(zip(id_list, has_chapter_list, length_list))
            # 排序：优先有章节 (True > False)，然后章节长度大的，然后 ID 小的
            # 我们要保留最好的，删除其他所有
            records.sort(key=lambda x: (x[1], x[2], -x[0]), reverse=True)

            # 第一个保留，其余删除
            to_keep = records[0][0]
            to_delete = [r[0] for r in records[1:]]
            ids_to_delete.extend(to_delete)

            logger.info(f"  保留 ID={to_keep}, 删除 IDs={to_delete}")

        # 3. 执行删除
        if ids_to_delete:
            logger.info(f"\n[3] 执行删除，共 {len(ids_to_delete)} 条记录...")

            # 分批删除，避免 SQL 过长
            batch_size = 100
            for i in range(0, len(ids_to_delete), batch_size):
                batch_ids = ids_to_delete[i:i+batch_size]
                placeholders = ','.join(['%s'] * len(batch_ids))
                cursor.execute(f"""
                    DELETE FROM fanqie_ranks
                    WHERE id IN ({placeholders})
                """, batch_ids)
                conn.commit()
                logger.info(f"  已删除 {len(batch_ids)} 条记录")

            logger.info(f"\n总计删除 {len(ids_to_delete)} 条重复记录")
        else:
            logger.info("  无需删除记录")

        # 4. 删除旧的唯一约束
        logger.info("\n[4] 删除旧的唯一约束...")
        try:
            cursor.execute("ALTER TABLE fanqie_ranks DROP INDEX unique_record")
            logger.info("  已删除旧约束：unique_record")
        except Exception as e:
            logger.warning(f"  删除旧约束失败（可能不存在）: {e}")

        # 5. 添加新的唯一约束
        logger.info("\n[5] 添加新的唯一约束...")
        cursor.execute("""
            ALTER TABLE fanqie_ranks
            ADD UNIQUE KEY unique_record (batch_date, board_name, sub_category, rank_num)
        """)
        logger.info("  已添加新约束：unique_record (batch_date, board_name, sub_category, rank_num)")

        # 6. 确保 book_id 索引存在
        logger.info("\n[6] 添加 book_id 索引...")
        try:
            cursor.execute("CREATE INDEX idx_book_id ON fanqie_ranks (book_id)")
            logger.info("  已添加索引：idx_book_id")
        except Exception as e:
            logger.warning(f"  添加索引失败（可能已存在）: {e}")

        conn.commit()
        logger.info("\n" + "=" * 60)
        logger.info("清理完成！")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"操作失败：{e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    cleanup_duplicates()
