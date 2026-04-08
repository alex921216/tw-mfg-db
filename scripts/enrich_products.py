"""
enrich_products.py — 產品代碼英文化並寫入 DB，重建 FTS5 索引

執行：
  cd /Users/alex/Desktop/forge-internal-master/projects/tw-mfg-db/src
  source .venv/bin/activate
  python3 -m scripts.enrich_products
"""

import json
import re
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
DATA_PATH = SRC_DIR / 'data' / 'factories_translated.json'
PRODUCT_CODES_PATH = SRC_DIR / 'translations' / 'product_codes.json'


# ---------------------------------------------------------------------------
# 解析 products_zh 字串 → 代碼清單
# ---------------------------------------------------------------------------

def parse_products_zh(raw: str) -> list[str]:
  """
  將 '089其他食品' 或 '259其他金屬製品、293通用機械設備' 解析成代碼/名稱 pair list。
  回傳每個 item 的完整原文（如 ['089其他食品', '293通用機械設備']）。
  """
  if not raw or not raw.strip():
    return []
  items = re.split(r'、(?=\d)', raw)
  result = []
  for item in items:
    item = item.strip()
    if item:
      result.append(item)
  return result


def item_to_code(item: str) -> str:
  """從 '089其他食品' 取出代碼 '089'（支援 3~7 位數）。"""
  m = re.match(r'^(\d+)', item)
  return m.group(1) if m else ''


# ---------------------------------------------------------------------------
# 翻譯邏輯
# ---------------------------------------------------------------------------

def translate_products(products_zh: str, code_map: dict[str, str]) -> tuple[str, str]:
  """
  回傳 (products_zh_clean, products_en)。
  - products_zh_clean: 用於存回 DB 的中文原文（可能有多個產品，以「、」分隔）
  - products_en: 英文翻譯，多個產品以 ', ' 分隔
  """
  if not products_zh or not products_zh.strip():
    return ('', '')

  items = parse_products_zh(products_zh)
  en_parts = []

  for item in items:
    code = item_to_code(item)
    if not code:
      continue

    # 優先完整代碼比對，fallback 到前 3 位
    en_name = code_map.get(code)
    if not en_name and len(code) > 3:
      en_name = code_map.get(code[:3])
    if not en_name:
      # 最後 fallback：掃描所有鍵找前綴最長匹配
      for key_len in range(len(code) - 1, 2, -1):
        en_name = code_map.get(code[:key_len])
        if en_name:
          break

    if en_name and en_name not in en_parts:
      en_parts.append(en_name)

  products_en = ', '.join(en_parts)
  return (products_zh.strip(), products_en)


# ---------------------------------------------------------------------------
# DB 操作
# ---------------------------------------------------------------------------

def add_columns_if_missing(conn: sqlite3.Connection) -> None:
  """新增 products_zh / products_en 欄位（若尚未存在）。"""
  cur = conn.cursor()
  cur.execute('PRAGMA table_info(factories)')
  existing_cols = {row[1] for row in cur.fetchall()}

  if 'products_zh' not in existing_cols:
    print('  Adding column: products_zh')
    conn.execute('ALTER TABLE factories ADD COLUMN products_zh TEXT')

  if 'products_en' not in existing_cols:
    print('  Adding column: products_en')
    conn.execute('ALTER TABLE factories ADD COLUMN products_en TEXT')

  conn.commit()


def bulk_update_products(
  conn: sqlite3.Connection,
  updates: list[tuple[str, str, str]],  # (products_zh, products_en, tax_id)
) -> None:
  """批次更新 products_zh / products_en。"""
  conn.executemany(
    'UPDATE factories SET products_zh = ?, products_en = ? WHERE tax_id = ?',
    updates,
  )
  conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
  """重建 FTS5 虛擬表，納入 products_en 欄位。"""
  print('  Dropping old FTS5 table...')
  conn.execute('DROP TABLE IF EXISTS factories_fts')

  print('  Creating new FTS5 table with products_en...')
  conn.execute("""
    CREATE VIRTUAL TABLE factories_fts USING fts5(
      name_en, industry_en, city_en, district_en, products_en,
      content='factories', content_rowid='id'
    )
  """)

  print('  Rebuilding FTS5 index...')
  conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
  conn.commit()
  print('  FTS5 rebuild complete.')


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
  print('=== enrich_products.py ===')

  # 1. 載入對照表
  print('\n[1/4] Loading product code map...')
  code_map: dict[str, str] = json.loads(PRODUCT_CODES_PATH.read_text(encoding='utf-8'))
  print(f'  Loaded {len(code_map)} code mappings.')

  # 2. 載入原始資料
  print('\n[2/4] Loading factories_translated.json...')
  raw_data: list[dict] = json.loads(DATA_PATH.read_text(encoding='utf-8'))
  print(f'  Loaded {len(raw_data)} factory records.')

  # 3. 建立 tax_id → products_zh 對照
  tax_id_products: dict[str, str] = {}
  for record in raw_data:
    tid = record.get('tax_id') or record.get('unified_business_no', '')
    pz = record.get('products_zh', '')
    if tid and pz:
      tax_id_products[str(tid)] = pz

  print(f'  Found products_zh for {len(tax_id_products)} factories.')

  # 4. 翻譯
  updates: list[tuple[str, str, str]] = []
  skipped = 0
  for tax_id, products_zh in tax_id_products.items():
    zh_clean, en = translate_products(products_zh, code_map)
    if zh_clean or en:
      updates.append((zh_clean, en, tax_id))
    else:
      skipped += 1

  print(f'  Prepared {len(updates)} updates, skipped {skipped}.')

  # 5. 寫入 DB
  print('\n[3/4] Updating database...')
  conn = sqlite3.connect(str(DB_PATH))
  try:
    add_columns_if_missing(conn)
    bulk_update_products(conn, updates)
    print(f'  Updated {len(updates)} rows.')

    # 驗證
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM factories WHERE products_en IS NOT NULL AND products_en != ''")
    filled = cur.fetchone()[0]
    print(f'  Rows with products_en: {filled}')

    # 6. 重建 FTS5
    print('\n[4/4] Rebuilding FTS5 index...')
    rebuild_fts(conn)

  finally:
    conn.close()

  print('\n=== Done ===')
  print('Verify with:')
  print('  Search "semiconductor" → should find 261 Semiconductors factories')
  print('  Search "PCB" → should find 263 PCB factories')
  print('  Search "machining" → should find metal processing factories')


if __name__ == '__main__':
  main()
