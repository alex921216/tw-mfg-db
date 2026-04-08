"""
enrich_certifications.py — 依產業推斷常見認證，寫入 DB，重建 FTS5 索引

注意：這些認證是「產業常見認證」的推斷，非個別工廠的確認資料。
前端顯示時須標示 "Common certifications for this industry type"。

執行：
  cd /Users/alex/Desktop/forge-internal-master/projects/tw-mfg-db/src
  source .venv/bin/activate
  python3 -m scripts.enrich_certifications            # 執行推斷並寫入 DB
  python3 -m scripts.enrich_certifications --status   # 只顯示目前統計

重建 FTS5 索引（certifications_en 已加入索引欄位）：
  索引會在每次執行時自動重建。
"""

import argparse
import json
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 產業 → 常見認證對照表
# ---------------------------------------------------------------------------

INDUSTRY_CERT_MAP: dict[str, list[str]] = {
  'Semiconductor Manufacturing': ['ISO 9001', 'ISO 14001', 'IECQ QC 080000'],
  'Electronic Components Manufacturing': ['ISO 9001', 'ISO 14001', 'IATF 16949'],
  'Computer, Electronic & Optical Products Manufacturing': ['ISO 9001', 'ISO 14001', 'ISO 27001'],
  'Motor Vehicles & Parts Manufacturing': ['ISO 9001', 'IATF 16949'],
  'Aircraft & Parts Manufacturing': ['ISO 9001', 'AS9100'],
  'Medical Devices & Supplies Manufacturing': ['ISO 13485', 'ISO 9001'],
  'Medical Devices & Supplies': ['ISO 13485', 'ISO 9001'],
  'Food Manufacturing': ['ISO 22000', 'HACCP'],
  'Beverages Manufacturing': ['ISO 22000', 'HACCP'],
  'Pharmaceutical Manufacturing': ['GMP', 'ISO 9001'],
  'Pharmaceuticals': ['GMP', 'ISO 9001'],
  'Chemical Manufacturing': ['ISO 9001', 'ISO 14001', 'OHSAS 18001'],
  'Plastic Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Rubber Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Basic Metal Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Fabricated Metal Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Industrial Machinery Manufacturing': ['ISO 9001'],
  'Electrical Equipment Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Furniture Manufacturing': ['ISO 9001'],
  'Textile Manufacturing': ['ISO 9001', 'OEKO-TEX Standard 100'],
  'Wearing Apparel Manufacturing': ['ISO 9001', 'OEKO-TEX Standard 100'],
  'Paper & Paper Products Manufacturing': ['ISO 9001', 'ISO 14001', 'FSC'],
  'Printing & Reproduction': ['ISO 9001'],
  'Leather & Footwear Manufacturing': ['ISO 9001'],
  'Wood Products Manufacturing': ['ISO 9001', 'FSC'],
}

# 前綴：標示這是推斷資料，非確認
TYPICAL_PREFIX = 'Typical: '

# ---------------------------------------------------------------------------
# 主要邏輯
# ---------------------------------------------------------------------------

def add_columns_if_missing(conn: sqlite3.Connection) -> None:
  """若欄位不存在則新增。"""
  cur = conn.cursor()
  cur.execute("PRAGMA table_info(factories)")
  existing_cols = {row[1] for row in cur.fetchall()}

  if 'certifications' not in existing_cols:
    print('  Adding column: certifications')
    conn.execute('ALTER TABLE factories ADD COLUMN certifications TEXT')

  if 'certifications_en' not in existing_cols:
    print('  Adding column: certifications_en')
    conn.execute('ALTER TABLE factories ADD COLUMN certifications_en TEXT')

  conn.commit()


def enrich_certifications(conn: sqlite3.Connection) -> dict:
  """依 industry_en 推斷認證，寫入 DB。回傳統計。"""
  cur = conn.cursor()
  cur.execute('SELECT id, industry_en FROM factories')
  rows = cur.fetchall()

  updated = 0
  skipped_no_industry = 0
  skipped_no_mapping = 0

  for row in rows:
    factory_id = row[0]
    industry_en = row[1] or ''

    if not industry_en.strip():
      skipped_no_industry += 1
      continue

    certs = INDUSTRY_CERT_MAP.get(industry_en)

    if not certs:
      skipped_no_mapping += 1
      continue

    certs_json = json.dumps(certs, ensure_ascii=False)
    certs_display = TYPICAL_PREFIX + ', '.join(certs)

    conn.execute(
      'UPDATE factories SET certifications = ?, certifications_en = ? WHERE id = ?',
      (certs_json, certs_display, factory_id),
    )
    updated += 1

  conn.commit()

  return {
    'updated': updated,
    'skipped_no_industry': skipped_no_industry,
    'skipped_no_mapping': skipped_no_mapping,
    'total': len(rows),
  }


def rebuild_fts5(conn: sqlite3.Connection) -> None:
  """重建 FTS5 索引，加入 certifications_en 欄位。"""
  print('  Dropping existing FTS5 table...')
  conn.execute('DROP TABLE IF EXISTS factories_fts')

  print('  Creating new FTS5 table with certifications_en...')
  conn.execute("""
    CREATE VIRTUAL TABLE factories_fts USING fts5(
      name_en, industry_en, city_en, district_en, products_en, certifications_en,
      content='factories', content_rowid='id'
    )
  """)

  print('  Rebuilding FTS5 index...')
  conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
  conn.commit()
  print('  FTS5 index rebuilt successfully.')


def show_status(conn: sqlite3.Connection) -> None:
  """顯示目前認證欄位的填充狀態。"""
  cur = conn.cursor()

  cur.execute('SELECT COUNT(*) FROM factories')
  total = cur.fetchone()[0]

  cur.execute("SELECT COUNT(*) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''")
  with_certs = cur.fetchone()[0]

  cur.execute("SELECT COUNT(*) FROM factories WHERE certifications_en LIKE 'Typical:%'")
  typical = cur.fetchone()[0]

  print(f'\n=== Certifications Status ===')
  print(f'  Total factories:          {total:,}')
  print(f'  With certifications_en:   {with_certs:,} ({with_certs/total*100:.1f}%)')
  print(f'  Typical (inferred):       {typical:,}')
  print(f'  Without certifications:   {total - with_certs:,}')

  # 顯示已覆蓋的產業
  cur.execute("""
    SELECT industry_en, COUNT(*) AS cnt
    FROM factories
    WHERE certifications_en IS NOT NULL AND certifications_en != ''
    GROUP BY industry_en
    ORDER BY cnt DESC
    LIMIT 15
  """)
  rows = cur.fetchall()
  if rows:
    print(f'\n  Top industries with certifications:')
    for r in rows:
      print(f'    {r[0]}: {r[1]:,}')

  # FTS5 狀態
  cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='factories_fts'")
  fts_exists = cur.fetchone() is not None
  print(f'\n  FTS5 index exists: {fts_exists}')
  print()


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
  parser = argparse.ArgumentParser(
    description='Enrich factory records with inferred industry certifications.',
  )
  parser.add_argument(
    '--status',
    action='store_true',
    help='Only show current certification fill statistics, do not modify DB.',
  )
  args = parser.parse_args()

  if not DB_PATH.exists():
    print(f'ERROR: Database not found at {DB_PATH}')
    raise SystemExit(1)

  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row

  try:
    if args.status:
      show_status(conn)
      return

    print(f'Database: {DB_PATH}')
    print()

    print('[1/3] Ensuring certification columns exist...')
    add_columns_if_missing(conn)

    print('[2/3] Enriching certifications by industry...')
    stats = enrich_certifications(conn)
    print(f'  Updated:              {stats["updated"]:,}')
    print(f'  Skipped (no industry): {stats["skipped_no_industry"]:,}')
    print(f'  Skipped (no mapping):  {stats["skipped_no_mapping"]:,}')
    print(f'  Total processed:      {stats["total"]:,}')

    print('[3/3] Rebuilding FTS5 index...')
    rebuild_fts5(conn)

    print()
    print('Done. Run with --status to verify results.')

  finally:
    conn.close()


if __name__ == '__main__':
  main()
