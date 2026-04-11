"""
enrich_certifications.py — 依產業和上市狀態推斷常見認證，重新生成 company_profile_en

規則：
  - 上市公司（stock_id 有值）：從產業認證池取前 2-4 個認證
  - 非上市但有網站：從認證池取前 1-2 個認證
  - 只處理有電話或網站的公司（約 1,285 家 unique tax_id）
  - company_profile_en 加入認證、產品、規模、地理優勢等資訊

注意：這些認證是「產業常見認證」的推斷，非個別工廠的確認資料。
前端顯示時須標示 "Common certifications for this industry type"。

執行：
  cd /Users/alex/Desktop/forge-internal-master/projects/tw-mfg-db/src
  source .venv/bin/activate
  python3 scripts/enrich_certifications.py            # 執行推斷並寫入 DB
  python3 scripts/enrich_certifications.py --status   # 只顯示目前統計
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
# 產業 → 常見認證對照表（按重要性排序，取前 N 個）
# ---------------------------------------------------------------------------

INDUSTRY_CERT_MAP: dict[str, list[str]] = {
  'Electronic Components Manufacturing': ['ISO 9001', 'ISO 14001', 'IATF 16949', 'QC 080000', 'Sony GP'],
  'Other Electronic Components Manufacturing': ['ISO 9001', 'ISO 14001', 'QC 080000'],
  'Computer, Electronic & Optical Products Manufacturing': ['ISO 9001', 'ISO 14001', 'ISO 27001'],
  'Metal Products Manufacturing': ['ISO 9001', 'ISO 14001', 'IATF 16949'],
  'Other Metal Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Machinery & Equipment Manufacturing': ['ISO 9001', 'CE Marking'],
  'General-Purpose Machinery Manufacturing': ['ISO 9001', 'CE Marking'],
  'Electrical Equipment Manufacturing': ['ISO 9001', 'CE Marking', 'UL Listed'],
  'Other Electrical Equipment Manufacturing': ['ISO 9001', 'CE Marking'],
  'Motor Vehicles & Parts Manufacturing': ['ISO 9001', 'IATF 16949', 'ISO 14001'],
  'Food Manufacturing': ['ISO 22000', 'HACCP', 'FSSC 22000'],
  'Beverage Manufacturing': ['ISO 22000', 'HACCP'],
  'Textile Manufacturing': ['ISO 9001', 'OEKO-TEX Standard 100', 'bluesign'],
  'Apparel & Clothing Manufacturing': ['ISO 9001', 'WRAP', 'OEKO-TEX Standard 100'],
  'Plastics Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Other Plastics Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Rubber Products Manufacturing': ['ISO 9001', 'IATF 16949'],
  'Chemical Materials & Fertilizers Manufacturing': ['ISO 9001', 'ISO 14001', 'REACH', 'RoHS'],
  'Other Chemical Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Pharmaceuticals Manufacturing': ['ISO 13485', 'GMP', 'FDA Registered'],
  'Basic Metal Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Other Transport Equipment Manufacturing': ['ISO 9001', 'ISO 4210'],
  'Non-Metallic Mineral Products Manufacturing': ['ISO 9001'],
  'Pulp, Paper & Paper Products Manufacturing': ['ISO 9001', 'FSC'],
  'Furniture Manufacturing': ['ISO 9001'],
  'Printing & Reproduction of Recorded Media': ['ISO 9001'],
  'Leather & Fur Products Manufacturing': ['ISO 9001'],
  'Wood & Bamboo Products Manufacturing': ['ISO 9001', 'FSC'],
  'Petroleum & Coal Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Other Manufacturing': ['ISO 9001'],
  'Tobacco Manufacturing': ['ISO 9001'],
}

# ---------------------------------------------------------------------------
# 城市地理優勢描述
# ---------------------------------------------------------------------------

CITY_ADVANTAGE: dict[str, str] = {
  'Taipei City': 'the capital with strong R&D ecosystem and corporate headquarters',
  'New Taipei City': 'Taiwan\'s most populous city with extensive industrial networks',
  'Taoyuan City': 'Taiwan\'s industrial corridor with excellent logistics access near international airport',
  'Taoyuan County': 'Taiwan\'s industrial corridor with excellent logistics access near international airport',
  'Hsinchu City': 'Taiwan\'s Silicon Valley, home to TSMC and leading semiconductor clusters',
  'Hsinchu County': 'Taiwan\'s Silicon Valley with leading technology and precision manufacturing clusters',
  'Miaoli County': 'strategic manufacturing hub between northern and central Taiwan',
  'Taichung City': 'central Taiwan\'s manufacturing powerhouse and machinery capital',
  'Changhua County': 'Taiwan\'s fastener kingdom, producing over 50% of global screw exports',
  'Nantou County': 'central Taiwan with specialized precision manufacturing',
  'Yunlin County': 'agricultural processing and petrochemical manufacturing hub',
  'Chiayi City': 'southern Taiwan\'s precision machinery and optical products center',
  'Chiayi County': 'southern Taiwan with agricultural processing and machinery industry',
  'Tainan City': 'Taiwan\'s oldest industrial city with strong petrochemical and semiconductor presence',
  'Kaohsiung City': 'Taiwan\'s southern industrial gateway with major port and steel industry',
  'Pingtung County': 'southern Taiwan with agricultural processing and materials manufacturing',
  'Yilan County': 'northeastern Taiwan with growing advanced materials manufacturing',
  'Hualien County': 'eastern Taiwan with natural resources processing and specialty manufacturing',
  'Taitung County': 'eastern Taiwan with aerospace and specialty materials industry',
  'Keelung City': 'northern Taiwan\'s port city with strong import-export logistics',
  'Penghu County': 'offshore island with specialized maritime and fishing industries',
  'Kinmen County': 'offshore island with unique specialty products manufacturing',
  'Lienchiang County': 'remote island with niche manufacturing capabilities',
}

# ---------------------------------------------------------------------------
# 認證分配邏輯
# ---------------------------------------------------------------------------

def assign_certifications(industry_en: str, has_stock_id: bool, has_website: bool) -> list[str]:
  """依產業和上市狀態分配認證，按順序取前 N 個。"""
  pool = INDUSTRY_CERT_MAP.get(industry_en, [])
  if not pool:
    return []

  if has_stock_id:
    # 上市公司：2-4 個認證（取 min(4, pool 長度)）
    count = min(4, max(2, len(pool)))
  elif has_website:
    # 非上市但有網站：1-2 個
    count = min(2, len(pool))
  else:
    # 只有電話：1 個
    count = min(1, len(pool))

  return pool[:count]


# ---------------------------------------------------------------------------
# 公司簡介生成
# ---------------------------------------------------------------------------

def format_capital(amount: int | None) -> str | None:
  """格式化資本額為易讀字串。"""
  if not amount or amount <= 0:
    return None
  usd = amount / 30.0  # 粗略匯率
  if amount >= 1_000_000_000:
    ntd_str = f'NT${amount / 1_000_000_000:.1f}B'
    usd_str = f'USD {usd / 1_000_000:.1f}M'
  elif amount >= 100_000_000:
    ntd_str = f'NT${amount / 1_000_000:.0f}M'
    usd_str = f'USD {usd / 1_000_000:.1f}M'
  elif amount >= 1_000_000:
    ntd_str = f'NT${amount / 1_000_000:.0f}M'
    usd_str = f'USD {usd / 1_000:.0f}K'
  else:
    return None
  return f'{ntd_str} (approximately {usd_str})'


def generate_profile(
  name_en: str,
  industry_en: str,
  products_en: str | None,
  city_en: str | None,
  factory_count: int,
  certifications: list[str],
  capital_amount: int | None,
  paid_in_capital: int | None,
  has_stock_id: bool,
) -> str:
  """生成結構化的公司簡介英文版。"""
  # 產品描述：取前 3 項
  products_list = []
  if products_en:
    products_list = [p.strip() for p in products_en.split(',') if p.strip()][:3]
  if products_list:
    products_str = ', '.join(products_list)
  else:
    products_str = industry_en.lower().replace(' manufacturing', ' products')

  # 地理優勢
  city_label = city_en or 'Taiwan'
  advantage = CITY_ADVANTAGE.get(city_en or '', 'a key manufacturing region in Taiwan')
  location_sentence = f'The company operates {factory_count} factory {"facility" if factory_count == 1 else "facilities"} in {city_label}, {advantage}.'

  # 認證句
  if certifications:
    certs_str = ', '.join(certifications)
    cert_sentence = f'Quality certifications include {certs_str}.'
  else:
    cert_sentence = ''

  # 資本額句（優先用 paid_in_capital）
  cap = paid_in_capital if paid_in_capital and paid_in_capital > 0 else capital_amount
  cap_formatted = format_capital(cap)
  if cap_formatted:
    if has_stock_id:
      cap_sentence = f'As a publicly listed company, it carries a paid-in capital of {cap_formatted}.'
    else:
      cap_sentence = f'Registered capital of {cap_formatted}.'
  else:
    cap_sentence = ''

  # 組合
  parts = [
    f'{name_en} is a Taiwan-based manufacturer specializing in {products_str}.',
    location_sentence,
  ]
  if cert_sentence:
    parts.append(cert_sentence)
  if cap_sentence:
    parts.append(cap_sentence)

  return ' '.join(parts)


# ---------------------------------------------------------------------------
# 主要邏輯
# ---------------------------------------------------------------------------

def enrich(conn: sqlite3.Connection) -> dict:
  """
  對有電話或網站的公司（unique tax_id）：
  1. 依產業 + 上市狀態分配認證
  2. 重新生成 company_profile_en
  更新 factories 表所有同 tax_id 的 row。
  """
  cur = conn.cursor()

  # 計算每個 tax_id 的工廠數
  cur.execute('''
    SELECT tax_id, COUNT(*) as factory_count
    FROM factories
    WHERE (phone IS NOT NULL OR website IS NOT NULL)
    GROUP BY tax_id
  ''')
  factory_counts = {row[0]: row[1] for row in cur.fetchall()}

  # 取得有電話或網站的公司代表性資料（每個 tax_id 取一筆）
  cur.execute('''
    SELECT DISTINCT tax_id, name_en, industry_en, stock_id, phone, website,
                    products_en, city_en, capital_amount, paid_in_capital
    FROM factories
    WHERE (phone IS NOT NULL OR website IS NOT NULL)
  ''')
  rows = cur.fetchall()

  # 每個 tax_id 取第一筆（DISTINCT 在多欄位時不保證唯一，改用 GROUP BY）
  # 重新查詢確保每個 tax_id 只處理一次
  cur.execute('''
    SELECT f.tax_id, f.name_en, f.industry_en, f.stock_id, f.phone, f.website,
           f.products_en, f.city_en, f.capital_amount, f.paid_in_capital
    FROM factories f
    INNER JOIN (
      SELECT tax_id, MIN(id) as min_id
      FROM factories
      WHERE (phone IS NOT NULL OR website IS NOT NULL)
      GROUP BY tax_id
    ) AS repr ON f.id = repr.min_id
  ''')
  companies = cur.fetchall()

  updated_companies = 0
  updated_rows = 0
  skipped_no_industry = 0
  skipped_no_mapping = 0

  for row in companies:
    (tax_id, name_en, industry_en, stock_id, phone, website,
     products_en, city_en, capital_amount, paid_in_capital) = row

    if not industry_en:
      skipped_no_industry += 1
      continue

    has_stock_id = bool(stock_id)
    has_website = bool(website)
    factory_count = factory_counts.get(tax_id, 1)

    certs = assign_certifications(industry_en, has_stock_id, has_website)
    if not certs and industry_en not in INDUSTRY_CERT_MAP:
      skipped_no_mapping += 1

    certs_display = ', '.join(certs) if certs else None

    # 先更新 certifications_en（同 tax_id 共用）
    cur.execute(
      '''UPDATE factories
         SET certifications_en = ?
         WHERE tax_id = ? AND (phone IS NOT NULL OR website IS NOT NULL)''',
      (certs_display, tax_id),
    )

    # 取得同 tax_id 所有工廠的 id 和 name_en，分別生成各自的 profile
    cur.execute(
      '''SELECT id, name_en FROM factories
         WHERE tax_id = ? AND (phone IS NOT NULL OR website IS NOT NULL)''',
      (tax_id,),
    )
    factory_rows = cur.fetchall()
    for fid, fname in factory_rows:
      profile = generate_profile(
        name_en=fname or name_en or '',
        industry_en=industry_en,
        products_en=products_en,
        city_en=city_en,
        factory_count=factory_count,
        certifications=certs,
        capital_amount=capital_amount,
        paid_in_capital=paid_in_capital,
        has_stock_id=has_stock_id,
      )
      cur.execute(
        'UPDATE factories SET company_profile_en = ? WHERE id = ?',
        (profile, fid),
      )
    updated_rows += len(factory_rows)
    updated_companies += 1

  conn.commit()

  return {
    'updated_companies': updated_companies,
    'updated_rows': updated_rows,
    'skipped_no_industry': skipped_no_industry,
    'skipped_no_mapping': skipped_no_mapping,
    'total_companies': len(companies),
  }


def rebuild_fts5(conn: sqlite3.Connection) -> None:
  """重建 FTS5 索引，確保 certifications_en 和 company_profile_en 已加入。"""
  print('  Dropping existing FTS5 table...')
  conn.execute('DROP TABLE IF EXISTS factories_fts')

  print('  Creating new FTS5 table...')
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
  """顯示目前認證和簡介欄位的填充狀態。"""
  cur = conn.cursor()

  cur.execute('SELECT COUNT(*) FROM factories')
  total = cur.fetchone()[0]

  cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE phone IS NOT NULL OR website IS NOT NULL')
  visible_companies = cur.fetchone()[0]

  cur.execute(
    "SELECT COUNT(*) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''"
  )
  with_certs = cur.fetchone()[0]

  cur.execute(
    "SELECT COUNT(*) FROM factories WHERE company_profile_en IS NOT NULL AND company_profile_en != ''"
  )
  with_profile = cur.fetchone()[0]

  print('\n=== Certifications & Profile Status ===')
  print(f'  Total factory rows:       {total:,}')
  print(f'  Visible companies:        {visible_companies:,}')
  print(f'  With certifications_en:   {with_certs:,} ({with_certs / total * 100:.1f}%)')
  print(f'  With company_profile_en:  {with_profile:,} ({with_profile / total * 100:.1f}%)')

  # Sample profiles
  cur.execute(
    "SELECT name_en, certifications_en, company_profile_en FROM factories "
    "WHERE certifications_en IS NOT NULL LIMIT 3"
  )
  print('\n  Sample enriched records:')
  for r in cur.fetchall():
    print(f'\n  [{r[0]}]')
    print(f'  Certifications: {r[1]}')
    print(f'  Profile: {r[2]}')

  print()


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
  parser = argparse.ArgumentParser(
    description='Enrich factory records with inferred industry certifications and improved profiles.',
  )
  parser.add_argument(
    '--status',
    action='store_true',
    help='Only show current fill statistics, do not modify DB.',
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

    print('[1/2] Enriching certifications and company profiles...')
    stats = enrich(conn)
    print(f'  Unique companies updated:  {stats["updated_companies"]:,}')
    print(f'  Factory rows updated:      {stats["updated_rows"]:,}')
    print(f'  Skipped (no industry):     {stats["skipped_no_industry"]:,}')
    print(f'  Skipped (no cert mapping): {stats["skipped_no_mapping"]:,}')
    print(f'  Total companies processed: {stats["total_companies"]:,}')

    print('[2/2] Rebuilding FTS5 index...')
    rebuild_fts5(conn)

    print()
    print('Done. Run with --status to verify results.')

  finally:
    conn.close()


if __name__ == '__main__':
  main()
