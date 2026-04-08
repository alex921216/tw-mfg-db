"""
資料驗證工具

對 tmdb.db 進行統計檢查、抽樣驗證、以及交叉驗證。

用法:
  python3 -m scripts.verify_data                  # 完整統計
  python3 -m scripts.verify_data --sample 50      # 抽樣 50 筆
  python3 -m scripts.verify_data --translation-only  # 只檢查翻譯品質
"""

from __future__ import annotations

import argparse
import random
import re
import sqlite3
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 路徑常數
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 中文字元偵測
# ---------------------------------------------------------------------------
CJK_RE = re.compile(r'[\u4e00-\u9fff]')

# ---------------------------------------------------------------------------
# 日期格式驗證
# ---------------------------------------------------------------------------
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


# ---------------------------------------------------------------------------
# DB 工具
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """建立並回傳 SQLite 連線。"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """檢查資料表是否有某欄位。"""
    cursor = conn.execute(f'PRAGMA table_info({table})')
    return any(row['name'] == column for row in cursor.fetchall())


# ---------------------------------------------------------------------------
# 統計檢查
# ---------------------------------------------------------------------------

def check_statistics(conn: sqlite3.Connection) -> None:
    """印出總筆數、縣市分佈、產業分佈、翻譯狀態分佈。"""
    total = conn.execute('SELECT COUNT(*) FROM factories').fetchone()[0]
    print('=== 統計摘要 ===')
    print(f'總筆數: {total:,}')

    # 各縣市分佈（前 10 大）
    print('\n--- 各縣市分佈（前 10 大）---')
    rows = conn.execute(
        """
        SELECT city_en, COUNT(*) as cnt
        FROM factories
        WHERE city_en IS NOT NULL AND city_en != ''
        GROUP BY city_en
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()
    for i, row in enumerate(rows, 1):
        pct = row['cnt'] / total * 100
        print(f'  {i:2}. {row["city_en"]:<25} {row["cnt"]:>7,}  ({pct:.1f}%)')

    # 各產業分佈（前 10 大）
    print('\n--- 各產業分佈（前 10 大）---')
    rows = conn.execute(
        """
        SELECT industry_en, COUNT(*) as cnt
        FROM factories
        WHERE industry_en IS NOT NULL AND industry_en != ''
        GROUP BY industry_en
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()
    for i, row in enumerate(rows, 1):
        pct = row['cnt'] / total * 100
        print(f'  {i:2}. {row["industry_en"]:<35} {row["cnt"]:>7,}  ({pct:.1f}%)')

    # 翻譯狀態分佈
    print('\n--- 翻譯狀態分佈 ---')
    if has_column(conn, 'factories', 'translation_status'):
        rows = conn.execute(
            """
            SELECT COALESCE(translation_status, 'pending') as status, COUNT(*) as cnt
            FROM factories
            GROUP BY status
            ORDER BY cnt DESC
            """
        ).fetchall()
        for row in rows:
            pct = row['cnt'] / total * 100
            print(f'  {row["status"]:<10} {row["cnt"]:>7,}  ({pct:.1f}%)')
    else:
        # translation_status 欄位尚未建立，以 name_en 是否含中文判斷
        cjk_count = sum(
            1 for row in conn.execute('SELECT name_en FROM factories').fetchall()
            if CJK_RE.search(row['name_en'] or '')
        )
        complete_count = total - cjk_count
        print(f'  已完整英文  {complete_count:>7,}  ({complete_count / total * 100:.1f}%)')
        print(f'  含中文待譯  {cjk_count:>7,}  ({cjk_count / total * 100:.1f}%)')


# ---------------------------------------------------------------------------
# 抽樣驗證
# ---------------------------------------------------------------------------

def check_sample(conn: sqlite3.Connection, n: int = 20) -> None:
    """隨機抽取 N 筆，顯示中英對照表格。"""
    all_ids = [row['id'] for row in conn.execute('SELECT id FROM factories').fetchall()]
    sample_ids = random.sample(all_ids, min(n, len(all_ids)))

    rows = conn.execute(
        f"""
        SELECT id, name_zh, name_en, industry_en, city_en
        FROM factories
        WHERE id IN ({','.join('?' * len(sample_ids))})
        ORDER BY id
        """,
        sample_ids
    ).fetchall()

    print(f'\n=== 隨機抽樣 {len(rows)} 筆 ===')
    header = f'{"#":>3}  {"中文名稱":<22}  {"英文翻譯":<40}  {"產業":<30}  {"城市"}'
    print(header)
    print('-' * len(header))

    for i, row in enumerate(rows, 1):
        name_zh = (row['name_zh'] or '')[:22]
        name_en = (row['name_en'] or '')[:40]
        industry = (row['industry_en'] or '-')[:30]
        city = row['city_en'] or '-'
        has_cjk = ' [含中文]' if CJK_RE.search(row['name_en'] or '') else ''
        print(f'{i:>3}  {name_zh:<22}  {name_en:<40}  {industry:<30}  {city}{has_cjk}')


# ---------------------------------------------------------------------------
# 翻譯品質抽樣
# ---------------------------------------------------------------------------

def check_translation_only(conn: sqlite3.Connection, n: int = 20) -> None:
    """從已翻譯完成的記錄中抽樣，確認翻譯品質。"""
    if has_column(conn, 'factories', 'translation_status'):
        rows = conn.execute(
            """
            SELECT id, name_zh, name_en, industry_en, city_en
            FROM factories
            WHERE translation_status = 'done'
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (n,)
        ).fetchall()
    else:
        # fallback：從不含中文的記錄抽樣
        candidates = conn.execute(
            'SELECT id, name_zh, name_en, industry_en, city_en FROM factories'
        ).fetchall()
        done = [r for r in candidates if not CJK_RE.search(r['name_en'] or '')]
        rows = random.sample(done, min(n, len(done)))

    if not rows:
        print('尚無已完成翻譯的記錄。')
        return

    print(f'\n=== 翻譯品質抽樣（已完成，{len(rows)} 筆）===')
    header = f'{"#":>3}  {"中文名稱":<22}  {"英文翻譯":<40}  {"城市"}'
    print(header)
    print('-' * len(header))

    for i, row in enumerate(rows, 1):
        name_zh = (row['name_zh'] or '')[:22]
        name_en = (row['name_en'] or '')[:40]
        city = row['city_en'] or '-'
        print(f'{i:>3}  {name_zh:<22}  {name_en:<40}  {city}')


# ---------------------------------------------------------------------------
# 交叉驗證
# ---------------------------------------------------------------------------

def cross_validate(conn: sqlite3.Connection) -> None:
    """檢查資料完整性：唯一性、必要欄位、日期格式。"""
    print('\n=== 交叉驗證 ===')
    issues_found = 0

    # 1. 統一編號唯一性
    dup_rows = conn.execute(
        """
        SELECT tax_id, COUNT(*) as cnt
        FROM factories
        WHERE tax_id IS NOT NULL AND tax_id != ''
        GROUP BY tax_id
        HAVING cnt > 1
        """
    ).fetchall()
    if dup_rows:
        print(f'[WARN] 重複統一編號: {len(dup_rows)} 個 tax_id 有重複記錄')
        for row in dup_rows[:5]:
            print(f'       tax_id={row["tax_id"]} 出現 {row["cnt"]} 次')
        if len(dup_rows) > 5:
            print(f'       ... 以及 {len(dup_rows) - 5} 個更多')
        issues_found += len(dup_rows)
    else:
        print('[OK]   統一編號唯一性：無重複')

    # 2. 必要欄位空值
    for col in ('name_zh', 'tax_id'):
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM factories WHERE {col} IS NULL OR {col} = ''"
        ).fetchone()[0]
        if null_count:
            print(f'[WARN] 空值欄位 {col}: {null_count:,} 筆')
            issues_found += null_count
        else:
            print(f'[OK]   必要欄位 {col}：無空值')

    # 3. 日期格式
    date_rows = conn.execute(
        "SELECT registration_date FROM factories WHERE registration_date IS NOT NULL AND registration_date != ''"
    ).fetchall()
    bad_dates = [r['registration_date'] for r in date_rows if not DATE_RE.match(r['registration_date'])]
    if bad_dates:
        print(f'[WARN] 日期格式不正確: {len(bad_dates):,} 筆（預期 YYYY-MM-DD）')
        for d in bad_dates[:3]:
            print(f'       {d!r}')
        issues_found += len(bad_dates)
    else:
        print('[OK]   日期格式：全部符合 YYYY-MM-DD')

    # 4. 翻譯錯誤記錄數
    if has_column(conn, 'factories', 'translation_status'):
        err_count = conn.execute(
            "SELECT COUNT(*) FROM factories WHERE translation_status = 'error'"
        ).fetchone()[0]
        if err_count:
            print(f'[WARN] 翻譯錯誤記錄: {err_count:,} 筆（可用 --retry-errors 重試）')
            issues_found += err_count
        else:
            print('[OK]   無翻譯錯誤記錄')

    if issues_found == 0:
        print('\n所有檢查通過，無問題。')
    else:
        print(f'\n共發現 {issues_found} 個問題，請檢閱上方 [WARN] 訊息。')


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='tw-mfg-db 資料驗證工具')
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        metavar='N',
        help='隨機抽樣 N 筆（預設 20 筆）',
    )
    parser.add_argument(
        '--translation-only',
        action='store_true',
        help='只進行翻譯品質抽樣檢查',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = get_connection()

    if args.translation_only:
        n = args.sample if args.sample else 20
        check_translation_only(conn, n=n)
        conn.close()
        return

    if args.sample is not None:
        check_sample(conn, n=args.sample)
    else:
        # 完整檢查
        check_statistics(conn)
        check_sample(conn, n=20)
        cross_validate(conn)

    conn.close()


if __name__ == '__main__':
    main()
