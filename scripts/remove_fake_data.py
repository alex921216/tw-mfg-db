"""
remove_fake_data.py — 移除所有模擬/虛假資料，只保留來自真實資料來源的欄位

真實資料來源：
  - 上市公司 (is_listed=1 / stock_id IS NOT NULL)：phone, fax, email, website 來自 TWSE/TPEX OpenAPI
  - capital_amount, paid_in_capital：GCIS API
  - 工廠名稱、地址、產業：MOEA 工廠登記
  - supply_chain_links：MOPS 年報 PDF 爬取 (234 筆)

清除項目：
  1. phone, fax — 非上市公司的（模擬區碼生成）
  2. email, website — 非上市公司的（模擬生成）
  3. certifications_en — 全清（全部為模擬的 ISO 認證）
  4. government_records — 全清（3,970 筆真實 raw data 與 11,188 筆模擬資料無法區分）
  5. company_profile_en — 重新生成（移除認證/獎項敘述後）
"""

import sqlite3
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'


def print_stats(cur, label: str) -> None:
  print(f'\n=== {label} ===')
  queries = [
    ('factories 總筆數',             'SELECT COUNT(*) FROM factories'),
    ('phone IS NOT NULL',            'SELECT COUNT(*) FROM factories WHERE phone IS NOT NULL'),
    ('fax IS NOT NULL',              'SELECT COUNT(*) FROM factories WHERE fax IS NOT NULL'),
    ('email IS NOT NULL',            'SELECT COUNT(*) FROM factories WHERE email IS NOT NULL'),
    ('website IS NOT NULL',          'SELECT COUNT(*) FROM factories WHERE website IS NOT NULL'),
    ('certifications_en IS NOT NULL','SELECT COUNT(*) FROM factories WHERE certifications_en IS NOT NULL'),
    ('government_records 總筆數',    'SELECT COUNT(*) FROM government_records'),
    ('supply_chain_links 總筆數',    'SELECT COUNT(*) FROM supply_chain_links'),
  ]
  for desc, sql in queries:
    cnt = cur.execute(sql).fetchone()[0]
    print(f'  {desc:<35} {cnt:>10,}')


def main() -> None:
  if not DB_PATH.exists():
    print(f'[ERROR] 找不到資料庫：{DB_PATH}')
    raise SystemExit(1)

  print(f'[INFO] 連接資料庫：{DB_PATH}')
  conn = sqlite3.connect(str(DB_PATH))
  conn.execute('PRAGMA journal_mode=WAL')
  cur = conn.cursor()

  # ── BEFORE 統計 ────────────────────────────────────────────────────────────
  print_stats(cur, 'BEFORE')

  # ── Step 1：清除非上市公司的假電話/傳真 ──────────────────────────────────
  print('\n[Step 1] 清除非上市公司的 phone / fax...')
  r = cur.execute(
    "UPDATE factories SET phone = NULL WHERE stock_id IS NULL AND (is_listed IS NULL OR is_listed != 1)"
  )
  print(f'  phone 清除 {r.rowcount:,} 筆')
  r = cur.execute(
    "UPDATE factories SET fax = NULL WHERE stock_id IS NULL AND (is_listed IS NULL OR is_listed != 1)"
  )
  print(f'  fax 清除 {r.rowcount:,} 筆')

  # ── Step 2：清除非上市公司的假 email/website ──────────────────────────────
  print('\n[Step 2] 清除非上市公司的 email / website...')
  r = cur.execute(
    "UPDATE factories SET email = NULL WHERE stock_id IS NULL AND (is_listed IS NULL OR is_listed != 1)"
  )
  print(f'  email 清除 {r.rowcount:,} 筆')
  r = cur.execute(
    "UPDATE factories SET website = NULL WHERE stock_id IS NULL AND (is_listed IS NULL OR is_listed != 1)"
  )
  print(f'  website 清除 {r.rowcount:,} 筆')

  # ── Step 3：清除所有模擬認證 ─────────────────────────────────────────────
  print('\n[Step 3] 清除所有 certifications_en（全為模擬資料）...')
  r = cur.execute("UPDATE factories SET certifications_en = NULL")
  print(f'  certifications_en 清除 {r.rowcount:,} 筆')

  # ── Step 4：清除所有 government_records ──────────────────────────────────
  # 原始 3,970 筆 raw data 與 11,188 筆模擬資料混合後無標記可區分
  print('\n[Step 4] 清除 government_records（無法區分真偽，全清）...')
  r = cur.execute("DELETE FROM government_records")
  print(f'  刪除 {r.rowcount:,} 筆')

  conn.commit()
  print('\n[INFO] 已 commit。')

  # ── AFTER 統計 ─────────────────────────────────────────────────────────────
  print_stats(cur, 'AFTER')

  conn.close()
  print('\n[INFO] remove_fake_data.py 完成。')
  print('[NEXT] 請執行：python scripts/generate_company_profiles.py')
  print('[NEXT] 然後執行：rm -f data/tmdb.db.gz && gzip -k data/tmdb.db')


if __name__ == '__main__':
  main()
