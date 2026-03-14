#!/usr/bin/env python3
"""数据验证工具 - 验证书籍字段和指标格式"""

import sys
import json
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


def verify_book_fields():
    """验证书籍字段完整性"""
    logger.info("\n" + "=" * 60)
    logger.info("Books - 书籍字段验证")
    logger.info("=" * 60)

    conn = db_manager().get_connection()
    cur = conn.cursor()

    # 查询所有数据
    cur.execute('SELECT * FROM fanqie_ranks ORDER BY batch_date DESC LIMIT 100')
    rows = cur.fetchall()

    # 获取列名
    columns = [desc[0] for desc in cur.description]

    # 转换为字典列表
    rows = [dict(zip(columns, row)) for row in rows]

    if not rows:
        logger.warning("Database - 数据库中没有数据")
        conn.close()
        return

    # 字段统计
    fields_to_check = [
        'book_title', 'author_name', 'metric_value',
        'synopsis', 'chapter_list_json', 'book_status'
    ]

    logger.info(f"\n检查最近 {len(rows)} 条记录...\n")

    for field in fields_to_check:
        filled = sum(1 for row in rows if row.get(field) and str(row[field]).strip())
        empty = len(rows) - filled
        pct = 100 * filled / len(rows)
        status = "[OK]" if pct >= 90 else "[WARN]" if pct >= 50 else "[ERR]"
        logger.info(f"  {status} {field}: {filled}/{len(rows)} ({pct:.1f}%) - 空 {empty}")

    # 检查 metric_value 解析
    logger.info("\nMetric Value - 解析检查:")
    with_raw = sum(1 for row in rows if row.get('metric_value_raw'))
    with_value = sum(1 for row in rows if row.get('metric_value') is not None)
    logger.info(f"  有 metric_value_raw: {with_raw}/{len(rows)}")
    logger.info(f"  有 metric_value: {with_value}/{len(rows)}")

    # 检查章节列表
    logger.info("\nChapters - 章节列表检查:")
    with_chapters = sum(1 for row in rows if row.get('chapter_list_json'))
    if with_chapters:
        total_chapters = 0
        for row in rows:
            if row.get('chapter_list_json'):
                try:
                    chapters = json.loads(row['chapter_list_json'])
                    total_chapters += len(chapters)
                except:
                    pass
        avg_chapters = total_chapters / with_chapters if with_chapters else 0
        logger.info(f"  有章节数据：{with_chapters}/{len(rows)}")
        logger.info(f"  平均每本章节数：{avg_chapters:.1f}")

    # 检查重复记录
    logger.info("\nDuplicates - 重复记录检查:")
    cur.execute('''
        SELECT batch_date, sub_category, book_id, COUNT(*) as cnt
        FROM fanqie_ranks
        GROUP BY batch_date, sub_category, book_id
        HAVING cnt > 1
        LIMIT 10
    ''')
    duplicates = cur.fetchall()
    if duplicates:
        logger.warning(f"  [WARN] 发现 {len(duplicates)} 条重复记录:")
        for dup in duplicates[:5]:
            logger.warning(f"    - {dup['batch_date']} | {dup['sub_category']} | {dup['book_id']}")
    else:
        logger.info("  [OK] 未发现重复记录")

    conn.close()
    logger.info("\n" + "=" * 60)
    logger.info("Verification Complete - 验证完成")
    logger.info("=" * 60)


if __name__ == '__main__':
    verify_book_fields()
