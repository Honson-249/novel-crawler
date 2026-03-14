#!/usr/bin/env python3
"""数据导出工具 - 导出 fanqie_ranks 数据到 CSV 和 JSON"""

import sys
import csv
import json
from datetime import datetime
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


def export_data():
    """导出数据到 CSV 和 JSON"""
    conn = db_manager().get_connection()
    conn.row_factory = lambda cursor, row: {desc[0]: row[idx] for idx, desc in enumerate(cursor.description)}
    cur = conn.cursor()

    # 查询所有数据
    cur.execute('SELECT * FROM fanqie_ranks ORDER BY board_name, rank_num')
    rows = cur.fetchall()

    # 获取列名
    columns = [
        'id', 'batch_date', 'board_name', 'sub_category', 'rank_num',
        'book_id', 'book_title', 'author_name', 'metric_name',
        'metric_value_raw', 'metric_value', 'tags', 'book_status',
        'synopsis', 'chapter_list_json', 'cover_url', 'detail_url',
        'created_at', 'updated_at'
    ]

    timestamp = datetime.now().strftime('%Y-%m-%d')

    # 导出 CSV (UTF-8-SIG with BOM for Excel)
    csv_file = f'data/fanqie_ranks_{timestamp}.csv'
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.info(f'[OK] CSV 导出：{csv_file} ({len(rows)} 条记录)')

    # 导出 JSON
    json_file = f'data/fanqie_ranks_{timestamp}.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    logger.info(f'[OK] JSON 导出：{json_file} ({len(rows)} 条记录)')

    # 统计 synopsis 和 metric_value
    with_synopsis = sum(1 for row in rows if row.get('synopsis') and row['synopsis'].strip())
    with_metric = sum(1 for row in rows if row.get('metric_value') is not None)

    logger.info(f'Stats - 有 synopsis 的记录：{with_synopsis}/{len(rows)} ({100*with_synopsis/len(rows):.1f}%)')
    logger.info(f'Stats - 有 metric_value 的记录：{with_metric}/{len(rows)} ({100*with_metric/len(rows):.1f}%)')

    conn.close()


if __name__ == '__main__':
    export_data()
