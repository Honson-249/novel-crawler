#!/usr/bin/env python3
"""
查询数据库中的重复数据
"""
import sys
import io
from pathlib import Path
import csv

# 设置 stdout 为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.novel_crawler.config.database import db_manager


def check_duplicates():
    """查询并输出重复数据"""
    conn = db_manager().get_connection()
    cursor = conn.cursor()

    try:
        # 1. 按 (batch_date, board_name, sub_category, rank_num) 查询重复
        print("=" * 100)
        print("【重复数据查询】同一个 batch_date + board_name + sub_category + rank_num 的重复数据")
        print("=" * 100)
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
            print(f"\n发现 {len(duplicates)} 组重复数据:\n")
            for dup in duplicates:
                batch_date, board_name, sub_cat, rank_num, cnt, book_ids, titles, authors, min_id, max_id = dup
                print(f"[{batch_date}] {board_name} | {sub_cat} - 排名 {rank_num} ({cnt}条)")
                print(f"  book_ids: {book_ids}")
                print(f"  书名：{titles}")
                print(f"  作者：{authors}")
                print(f"  记录 ID 范围：{min_id} - {max_id}")
                print("")
        else:
            print("  未发现重复数据")

        # 保存到 CSV 文件
        if duplicates:
            csv_file = BASE_DIR / "data" / "duplicates_report.csv"
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['batch_date', 'board_name', 'sub_category', 'rank_num', 'count', 'book_ids', 'titles', 'authors', 'min_id', 'max_id'])
                for dup in duplicates:
                    writer.writerow(dup)
            print(f"\n详细报告已保存到：{csv_file}")

        # 2. 统计概览
        print("\n" + "=" * 100)
        print("【统计概览】")
        print("=" * 100)

        cursor.execute("SELECT COUNT(*) FROM fanqie_ranks")
        total = cursor.fetchone()[0]
        print(f"  总记录数：{total}")

        cursor.execute("SELECT COUNT(DISTINCT book_id) FROM fanqie_ranks WHERE book_id IS NOT NULL AND book_id != ''")
        unique_books = cursor.fetchone()[0]
        print(f"  不重复书籍数：{unique_books}")

        cursor.execute("SELECT COUNT(DISTINCT CONCAT(batch_date, '-', board_name, '-', sub_category, '-', rank_num)) FROM fanqie_ranks")
        unique_ranks = cursor.fetchone()[0]
        print(f"  不重复排名组合数：{unique_ranks}")

        cursor.execute("SELECT batch_date, COUNT(*) as cnt FROM fanqie_ranks GROUP BY batch_date ORDER BY batch_date DESC")
        by_date = cursor.fetchall()
        print(f"\n  按日期统计:")
        for row in by_date:
            print(f"    {row[0]}: {row[1]} 条")

    finally:
        conn.close()


if __name__ == "__main__":
    check_duplicates()
