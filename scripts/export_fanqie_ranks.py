# -*- coding: utf-8 -*-
"""
导出番茄榜单数据为 CSV 文件
用法:
    python scripts/export_fanqie_ranks.py --date 2026-03-16    # 导出指定日期数据
    python scripts/export_fanqie_ranks.py                       # 导出今天数据
"""
import csv
import logging
import os
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from src.novel_crawler.config.database import DatabaseManager

# 导出目录：D:\dev\projects\crawler-data\番茄小说排行榜
EXPORT_DIR = Path("D:/dev/projects/crawler-data/番茄小说排行榜")


# 导出目录：D:\dev\projects\crawler-data\番茄小说排行榜
EXPORT_DIR = Path("D:/dev/projects/crawler-data/番茄小说排行榜")
# 日志目录（项目根目录下）
LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"


def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"export_fanqie_{date.today().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_date_arg():
    """解析 --date 参数，返回 date 对象"""
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            date_str = sys.argv[i + 1]
            try:
                parts = date_str.split("-")
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
            except (ValueError, IndexError):
                logging.error("无效的日期格式：%s，请使用 YYYY-MM-DD", date_str)
                sys.exit(1)
    return date.today()


def export_fanqie_ranks(batch_date: date):
    """从 fanqie_ranks 表导出数据为 CSV"""
    logger = logging.getLogger(__name__)
    conn = None
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        with conn.cursor() as cur:
            sql = """
                SELECT id, book_id, batch_date, board_name, sub_category,
                       rank_num, book_title, author_name, book_status,
                       synopsis, metric_name, metric_value_raw, metric_value,
                       chapter_list_json, detail_url
                FROM `fanqie_ranks`
                WHERE `batch_date` = %s
                ORDER BY `board_name`, `sub_category`, `rank_num`
            """
            cur.execute(sql, (batch_date,))
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]

            if not rows:
                logger.warning("没有找到 %s 的数据", batch_date)
                return

            total_count = len(rows)
            logger.info("查询到 %d 条记录", total_count)

            # 导出数据为 CSV
            EXPORT_DIR.mkdir(parents=True, exist_ok=True)
            csv_filename = EXPORT_DIR / f"番茄小说榜单_{batch_date.strftime('%Y%m%d')}.csv"

            fieldnames = [
                "id", "book_id", "batch_date", "board_name", "sub_category",
                "rank_num", "book_title", "author_name", "book_status",
                "synopsis", "metric_name", "metric_value_raw", "metric_value",
                "chapter_list_json", "detail_url"
            ]

            # 需要检查的必填字段（不能为空）
            required_fields = {"book_id", "batch_date", "board_name", "sub_category",
                               "rank_num", "book_title", "author_name"}

            normal_count = 0
            anomaly_count = 0

            with open(csv_filename, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    row_dict = dict(zip(column_names, row))
                    # 检查是否有缺失的必填字段
                    has_missing = any(
                        row_dict.get(field) is None or row_dict.get(field) == ""
                        for field in required_fields
                    )
                    if has_missing:
                        anomaly_count += 1
                    else:
                        normal_count += 1
                    writer.writerow(row_dict)

            logger.info("已导出到：%s", csv_filename)
            logger.info("数据统计：总计 %d 条，正常 %d 条，异常 %d 条",
                       total_count, normal_count, anomaly_count)

    except Exception as e:
        logger.error("导出失败：%s", e)
        raise
    finally:
        if conn:
            conn.close()


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    batch_date = parse_date_arg()
    logger.info("开始导出番茄榜单数据，日期：%s", batch_date)

    export_fanqie_ranks(batch_date)

    logger.info("导出完成")


if __name__ == "__main__":
    main()
