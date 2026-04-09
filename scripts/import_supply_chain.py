"""
import_supply_chain.py — 匯入供應鏈資料到 supply_chain_links 表

來源：
  data/supply_chain_raw.json     — 262 筆原始供應鏈記錄
  data/supply_chain_matched.json — 9 筆已比對到工廠的記錄（含 supplier_tax_id）

策略：
  1. 以 supply_chain_raw.json 為主體，逐筆匯入
  2. 若同一 (buyer_name, supplier_name, source_year) 在 matched 中有對應，補充 supplier_tax_id
  3. buyer_tax_id 從 listed_companies.json 的 stock_code 查詢，或從 factories 表的 stock_id 查詢
  4. 去重：(buyer_name, supplier_name, source_year) 相同者不重複匯入

執行：
  cd src/
  source .venv/bin/activate
  python3 scripts/import_supply_chain.py
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
RAW_PATH = SRC_DIR / 'data' / 'supply_chain_raw.json'
MATCHED_PATH = SRC_DIR / 'data' / 'supply_chain_matched.json'
LISTED_PATH = SRC_DIR / 'data' / 'listed_companies.json'


def load_json(path: Path) -> list:
  with open(path, encoding='utf-8') as f:
    return json.load(f)


def build_stock_code_to_tax_id_map(listed: list, conn: sqlite3.Connection) -> dict[str, str]:
  """建立 stock_code → tax_id 映射。

  策略 1：從 factories 表用 stock_id 欄位直接查。
  策略 2（fallback）：用 listed_companies.json 的 company_name 比對 factories.name_zh，
    取最短名稱的工廠（通常是總廠），以避免廠區後綴干擾。
  """
  mapping: dict[str, str] = {}
  cur = conn.cursor()

  # 策略 1：stock_id 直查
  cur.execute('SELECT stock_id, tax_id FROM factories WHERE stock_id IS NOT NULL AND stock_id != ""')
  for row in cur.fetchall():
    if row[0] and row[1]:
      mapping[str(row[0]).strip()] = str(row[1]).strip()

  if mapping:
    return mapping

  # 策略 2：用 company_name LIKE 比對 factories.name_zh
  # 取 name_zh 最短者（最可能是公司主體，而非廠區）
  for company in listed:
    stock_code = str(company.get('stock_code') or '').strip()
    company_name = str(company.get('company_name') or '').strip()
    if not stock_code or not company_name or stock_code in mapping:
      continue
    cur.execute(
      'SELECT tax_id FROM factories WHERE name_zh LIKE ? ORDER BY LENGTH(name_zh) ASC LIMIT 1',
      (f'%{company_name[:4]}%',),
    )
    row = cur.fetchone()
    if row and row[0]:
      mapping[stock_code] = str(row[0]).strip()

  return mapping


def build_matched_index(matched: list) -> dict[tuple, dict]:
  """建立 (buyer_name, supplier_name, source_year) → matched record 索引。"""
  index: dict[tuple, dict] = {}
  for record in matched:
    key = (
      record.get('buyer_name', '').strip(),
      record.get('supplier_name', '').strip(),
      record.get('source_year'),
    )
    if record.get('matched_tax_id'):
      index[key] = record
  return index


def main() -> None:
  print(f'DB: {DB_PATH}')
  print(f'Raw: {RAW_PATH}')
  print(f'Matched: {MATCHED_PATH}')

  if not DB_PATH.exists():
    print('ERROR: DB not found')
    return

  raw_records = load_json(RAW_PATH)
  matched_records = load_json(MATCHED_PATH)
  listed = load_json(LISTED_PATH)

  print(f'Raw records: {len(raw_records)}')
  print(f'Matched records: {len(matched_records)}')
  print(f'Listed companies: {len(listed)}')

  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row

  try:
    stock_to_tax = build_stock_code_to_tax_id_map(listed, conn)
    print(f'Stock code → tax_id mappings from DB: {len(stock_to_tax)}')

    matched_index = build_matched_index(matched_records)
    print(f'Matched index entries (with tax_id): {len(matched_index)}')

    now = datetime.now(timezone.utc).isoformat()

    inserted = 0
    skipped_dup = 0
    skipped_noise = 0

    cur = conn.cursor()

    # 讀取現有記錄用於去重
    cur.execute('SELECT buyer_name, supplier_name, source_year FROM supply_chain_links')
    existing: set[tuple] = set()
    for row in cur.fetchall():
      existing.add((row[0], row[1], row[2]))

    print(f'Existing records in DB: {len(existing)}')

    for record in raw_records:
      buyer_name = (record.get('buyer_name') or '').strip()
      supplier_name = (record.get('supplier_name') or '').strip()
      source_year = record.get('source_year')
      buyer_stock_code = str(record.get('buyer_stock_code') or '').strip()

      # 跳過明顯是 PDF 雜訊的記錄（亂碼或非供應商描述）
      noise_keywords = ['本公司', '海外無擔保', '轉換公司債', '\xff', 'ÿ', 'ý']
      if any(kw in supplier_name for kw in noise_keywords):
        skipped_noise += 1
        continue

      if not buyer_name or not supplier_name:
        skipped_noise += 1
        continue

      # 去重檢查
      key = (buyer_name, supplier_name, source_year)
      if key in existing:
        skipped_dup += 1
        continue

      # 查 buyer_tax_id
      buyer_tax_id = stock_to_tax.get(buyer_stock_code)

      # 查 matched 中的 supplier_tax_id
      matched = matched_index.get(key)
      supplier_tax_id = None
      if matched:
        supplier_tax_id = matched.get('matched_tax_id') or matched.get('supplier_tax_id')

      # 也用 raw 記錄本身的 supplier_tax_id（如果有）
      if not supplier_tax_id:
        raw_tax_id = record.get('supplier_tax_id')
        if raw_tax_id:
          supplier_tax_id = str(raw_tax_id).strip()

      cur.execute(
        """
        INSERT INTO supply_chain_links
          (buyer_tax_id, buyer_name, supplier_tax_id, supplier_name,
           relationship_type, source, source_year,
           purchase_amount, purchase_ratio,
           created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          buyer_tax_id,
          buyer_name,
          supplier_tax_id,
          supplier_name,
          'supplier',
          record.get('source') or 'annual_report_pdf',
          source_year,
          record.get('purchase_amount'),
          record.get('purchase_ratio'),
          now,
          now,
        ),
      )
      existing.add(key)
      inserted += 1

    conn.commit()
    print(f'\nDone.')
    print(f'  Inserted: {inserted}')
    print(f'  Skipped (duplicate): {skipped_dup}')
    print(f'  Skipped (noise/invalid): {skipped_noise}')

    # 回填 buyer_tax_id：對 buyer_tax_id 為空但有 buyer_name 的記錄補充
    print('\nBackfilling buyer_tax_id from name matching...')
    # 先建立 buyer_name → tax_id 映射（透過 raw data 的 buyer_stock_code 找）
    buyer_name_to_stock: dict[str, str] = {}
    for record in raw_records:
      bname = (record.get('buyer_name') or '').strip()
      bcode = str(record.get('buyer_stock_code') or '').strip()
      if bname and bcode:
        buyer_name_to_stock[bname] = bcode

    backfilled = 0
    cur.execute(
      'SELECT DISTINCT buyer_name FROM supply_chain_links WHERE buyer_tax_id IS NULL AND buyer_name IS NOT NULL'
    )
    null_buyers = [r[0] for r in cur.fetchall()]
    for bname in null_buyers:
      bcode = buyer_name_to_stock.get(bname)
      if bcode:
        tax_id = stock_to_tax.get(bcode)
        if tax_id:
          cur.execute(
            'UPDATE supply_chain_links SET buyer_tax_id = ? WHERE buyer_name = ? AND buyer_tax_id IS NULL',
            (tax_id, bname),
          )
          backfilled += cur.rowcount
    conn.commit()
    print(f'  Backfilled buyer_tax_id: {backfilled} records')

    # 更新 FTS 索引
    print('Rebuilding FTS index...')
    conn.execute("INSERT INTO supply_chain_links_fts(supply_chain_links_fts) VALUES('rebuild')")
    conn.commit()
    print('FTS index rebuilt.')

    # 驗證
    cur.execute('SELECT COUNT(*) AS cnt FROM supply_chain_links')
    total = cur.fetchone()[0]
    print(f'Total records in supply_chain_links: {total}')

  finally:
    conn.close()


if __name__ == '__main__':
  main()
