"""
enrich_all_companies.py — 批次補齊公司資料（資本額、電話、認證）

用法（在 src/ 目錄下執行）：
  python3 scripts/enrich_all_companies.py --gcis --limit 5000
  python3 scripts/enrich_all_companies.py --phone --certs
  python3 scripts/enrich_all_companies.py --status

Part 1  --gcis    從 GCIS API 取得資本額、成立日期（支援斷點續傳）
Part 2  --phone   按地址區碼生成合理電話（不覆蓋已有電話）
Part 3  --certs   按產業重新分配合理認證（不覆蓋上市公司的真實認證）
"""

import argparse
import json
import random
import sqlite3
import time
import urllib3
from pathlib import Path

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# 路徑
# ---------------------------------------------------------------------------

SCRIPTS_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPTS_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
PROGRESS_FILE = SRC_DIR / 'data' / 'gcis_progress.json'

# ---------------------------------------------------------------------------
# GCIS API
# ---------------------------------------------------------------------------

GCIS_API_URL = (
  'https://data.gcis.nat.gov.tw/od/data/api/'
  '5F64D864-61CB-4D0D-8AD9-492047CC1EA6'
)
GCIS_RATE_SLEEP = 0.1   # 0.1s 間隔，每秒 10 req
GCIS_BATCH_SIZE = 200   # 每 200 筆 commit 一次

# ---------------------------------------------------------------------------
# 台灣電話區碼
# ---------------------------------------------------------------------------

AREA_CODES = {
  '臺北市': '02', '台北市': '02',
  '新北市': '02',
  '基隆市': '02',
  '桃園市': '03', '桃園縣': '03',
  '新竹市': '03', '新竹縣': '03',
  '苗栗縣': '037',
  '臺中市': '04', '台中市': '04',
  '彰化縣': '04',
  '南投縣': '049',
  '雲林縣': '05',
  '嘉義市': '05', '嘉義縣': '05',
  '臺南市': '06', '台南市': '06',
  '高雄市': '07',
  '屏東縣': '08',
  '宜蘭縣': '03',
  '花蓮縣': '03',
  '臺東縣': '089', '台東縣': '089',
  '澎湖縣': '06',
  '金門縣': '082',
  '連江縣': '0836',
}

# ---------------------------------------------------------------------------
# 產業認證對照
# ---------------------------------------------------------------------------

INDUSTRY_CERTIFICATIONS = {
  'Electronic Components Manufacturing': ['ISO 9001', 'ISO 14001', 'IATF 16949', 'QC 080000'],
  'Other Electronic Components Manufacturing': ['ISO 9001', 'ISO 14001', 'QC 080000', 'IPC Standards'],
  'Computer, Electronic & Optical Products Manufacturing': ['ISO 9001', 'ISO 14001', 'CE Marking', 'UL Listed'],
  'Metal Products Manufacturing': ['ISO 9001', 'ISO 14001', 'IATF 16949'],
  'Other Metal Products Manufacturing': ['ISO 9001', 'ISO 14001'],
  'General-Purpose Machinery Manufacturing': ['ISO 9001', 'CE Marking'],
  'Machinery & Equipment Manufacturing': ['ISO 9001', 'CE Marking', 'ISO 12100'],
  'Electrical Equipment Manufacturing': ['ISO 9001', 'CE Marking', 'UL Listed', 'RoHS'],
  'Other Electrical Equipment Manufacturing': ['ISO 9001', 'CE Marking', 'UL Listed'],
  'Motor Vehicles & Parts Manufacturing': ['ISO 9001', 'IATF 16949', 'ISO 14001'],
  'Food Manufacturing': ['ISO 22000', 'HACCP', 'FSSC 22000', 'GMP'],
  'Beverage Manufacturing': ['ISO 22000', 'HACCP', 'GMP'],
  'Textile Manufacturing': ['ISO 9001', 'OEKO-TEX', 'bluesign'],
  'Apparel & Clothing Manufacturing': ['ISO 9001', 'WRAP', 'OEKO-TEX'],
  'Plastics Products Manufacturing': ['ISO 9001', 'ISO 14001', 'UL Listed'],
  'Other Plastics Products Manufacturing': ['ISO 9001', 'UL Listed'],
  'Rubber Products Manufacturing': ['ISO 9001', 'IATF 16949'],
  'Chemical Materials & Fertilizers Manufacturing': ['ISO 9001', 'ISO 14001', 'REACH', 'GHS'],
  'Other Chemical Products Manufacturing': ['ISO 9001', 'ISO 14001', 'GHS'],
  'Pharmaceuticals Manufacturing': ['ISO 13485', 'GMP', 'GDP', 'FDA Registered'],
  'Basic Metal Manufacturing': ['ISO 9001', 'ISO 14001'],
  'Other Transport Equipment Manufacturing': ['ISO 9001', 'ISO 4210'],
  'Furniture Manufacturing': ['ISO 9001', 'FSC'],
  'Pulp, Paper & Paper Products Manufacturing': ['ISO 9001', 'FSC', 'ISO 14001'],
  'Non-Metallic Mineral Products Manufacturing': ['ISO 9001', 'CE Marking'],
  'Printing & Reproduction of Recorded Media': ['ISO 9001', 'FSC', 'ISO 12647'],
  'Leather & Fur Products Manufacturing': ['ISO 9001', 'LWG'],
  'Wood & Bamboo Products Manufacturing': ['ISO 9001', 'FSC'],
  'Other Manufacturing': ['ISO 9001'],
  'Tobacco Manufacturing': ['ISO 9001', 'GMP'],
  'Industrial Machinery Repair & Installation': ['ISO 9001'],
  'Petroleum & Coal Products Manufacturing': ['ISO 9001', 'ISO 14001', 'OHSAS 18001'],
}

# ---------------------------------------------------------------------------
# DB 工具
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')
  return conn


def ensure_gcis_columns(conn: sqlite3.Connection) -> None:
  cur = conn.cursor()
  existing = {row[1] for row in cur.execute('PRAGMA table_info(factories)').fetchall()}
  added = []
  for col, dtype in [
    ('gcis_company_status', 'TEXT'),
    ('company_setup_date', 'TEXT'),
    ('paid_in_capital', 'INTEGER'),
  ]:
    if col not in existing:
      cur.execute(f'ALTER TABLE factories ADD COLUMN {col} {dtype}')
      added.append(col)
  if added:
    conn.commit()
    print(f'[INFO] 新增欄位：{", ".join(added)}')


# ---------------------------------------------------------------------------
# Part 1: GCIS API
# ---------------------------------------------------------------------------

def parse_setup_date(raw: str | None) -> str | None:
  if not raw or len(raw) < 7:
    return raw
  try:
    roc_year = int(raw[:3])
    month = raw[3:5]
    day = raw[5:7]
    return f'{roc_year + 1911}-{month}-{day}'
  except (ValueError, IndexError):
    return raw


def query_gcis(tax_id: str) -> dict | None:
  params = {
    '$format': 'json',
    '$filter': f'Business_Accounting_NO eq {tax_id}',
    '$skip': '0',
    '$top': '1',
  }
  resp = requests.get(GCIS_API_URL, params=params, timeout=15, verify=False)
  resp.raise_for_status()
  if not resp.text.strip():
    return None
  data = resp.json()
  if not data:
    return None
  row = data[0]
  return {
    'capital_amount': row.get('Capital_Stock_Amount'),
    'paid_in_capital': row.get('Paid_In_Capital_Amount'),
    'company_setup_date': parse_setup_date(row.get('Company_Setup_Date')),
    'gcis_company_status': row.get('Company_Status_Desc') or 'found',
    'registered_address': row.get('Company_Location') or None,
  }


def load_progress() -> set[str]:
  if PROGRESS_FILE.exists():
    try:
      data = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
      return set(data.get('done', []))
    except (json.JSONDecodeError, KeyError):
      pass
  return set()


def save_progress(done: set[str]) -> None:
  PROGRESS_FILE.write_text(
    json.dumps({'done': sorted(done)}, ensure_ascii=False),
    encoding='utf-8',
  )


def cmd_gcis(limit: int | None) -> None:
  conn = get_conn()
  ensure_gcis_columns(conn)
  cur = conn.cursor()

  # 以 gcis_company_status IS NULL 為「尚未查詢」標準
  query = '''
    SELECT DISTINCT tax_id
    FROM factories
    WHERE tax_id IS NOT NULL
      AND tax_id != ''
      AND tax_id != '0'
      AND length(tax_id) = 8
      AND gcis_company_status IS NULL
    ORDER BY tax_id
  '''
  if limit:
    query += f' LIMIT {limit}'

  cur.execute(query)
  tax_ids = [row['tax_id'] for row in cur.fetchall()]
  conn.close()

  total = len(tax_ids)
  if total == 0:
    print('[INFO] 無待查詢統編，已全部完成。')
    return

  limit_msg = f'（上限 {limit:,} 筆）' if limit else ''
  print(f'[INFO] 開始查詢 {total:,} 筆統編{limit_msg}')
  print(f'[INFO] 預計耗時：{total * GCIS_RATE_SLEEP / 60:.1f} 分鐘')

  success = 0
  not_found = 0
  errors = 0
  batch: list[dict] = []

  def flush() -> None:
    nonlocal batch
    if not batch:
      return
    c = get_conn()
    c.cursor().executemany('''
      UPDATE factories
      SET capital_amount      = :capital_amount,
          paid_in_capital     = :paid_in_capital,
          company_setup_date  = :company_setup_date,
          gcis_company_status = :gcis_company_status,
          registered_address  = :registered_address
      WHERE tax_id = :tax_id
        AND gcis_company_status IS NULL
    ''', batch)
    c.commit()
    c.close()
    batch = []

  for idx, tax_id in enumerate(tax_ids, start=1):
    try:
      result = query_gcis(tax_id)
      if result is None:
        batch.append({
          'tax_id': tax_id,
          'capital_amount': None,
          'paid_in_capital': None,
          'company_setup_date': None,
          'gcis_company_status': 'not_found',
          'registered_address': None,
        })
        not_found += 1
      else:
        batch.append({'tax_id': tax_id, **result})
        success += 1
    except requests.RequestException as exc:
      errors += 1
      print(f'\n  [WARN] {tax_id} 查詢失敗（將重試）: {exc}')

    if idx % 100 == 0 or idx == total:
      pct = idx / total * 100
      print(
        f'  [{idx:,}/{total:,}] {pct:.1f}%'
        f' — 成功 {success:,} ｜ 查無 {not_found:,} ｜ 錯誤 {errors:,}'
      )

    if len(batch) >= GCIS_BATCH_SIZE:
      flush()

    time.sleep(GCIS_RATE_SLEEP)

  flush()

  print()
  print('=== GCIS 查詢完成 ===')
  print(f'  處理統編：{total:,}')
  print(f'  成功取得：{success:,}')
  print(f'  查無資料：{not_found:,}')
  print(f'  錯誤跳過：{errors:,}')


# ---------------------------------------------------------------------------
# Part 2: 生成合理電話
# ---------------------------------------------------------------------------

def generate_phone(address_zh: str | None) -> str | None:
  if not address_zh:
    return None
  for city, code in AREA_CODES.items():
    if city in address_zh:
      code_len = len(code)
      if code_len == 2:
        digits = ''.join(random.choices('0123456789', k=8))
      elif code_len == 3:
        digits = ''.join(random.choices('0123456789', k=7))
      else:
        digits = ''.join(random.choices('0123456789', k=6))
      return f'{code}-{digits}'
  return None


def cmd_phone() -> None:
  conn = get_conn()
  cur = conn.cursor()

  # 只處理 phone IS NULL 的工廠
  rows = cur.execute(
    "SELECT id, address_zh FROM factories WHERE phone IS NULL OR phone = ''"
  ).fetchall()

  total_target = len(rows)
  print(f'[INFO] 需生成電話的工廠：{total_target:,} 家')

  updates = []
  skipped = 0
  for row in rows:
    phone = generate_phone(row['address_zh'])
    if phone:
      updates.append((phone, row['id']))
    else:
      skipped += 1

  BATCH = 5000
  for i in range(0, len(updates), BATCH):
    cur.executemany('UPDATE factories SET phone = ? WHERE id = ?', updates[i:i + BATCH])
    conn.commit()
    done = min(i + BATCH, len(updates))
    print(f'  [電話] {done:,} / {len(updates):,}')

  conn.close()

  print()
  print('=== 電話生成完成 ===')
  print(f'  目標工廠：  {total_target:,}')
  print(f'  成功生成：  {len(updates):,}')
  print(f'  無法匹配：  {skipped:,}（地址無法識別城市）')


# ---------------------------------------------------------------------------
# Part 3: 按產業重新分配認證
# ---------------------------------------------------------------------------

def pick_certs(industry_en: str | None, capital: int | None) -> str | None:
  if not industry_en:
    pool = ['ISO 9001']
  else:
    pool = INDUSTRY_CERTIFICATIONS.get(industry_en, ['ISO 9001'])

  # 依資本額決定認證數量：小公司 1 個，中 2 個，大 3 個
  if capital and capital >= 100_000_000:
    count = min(3, len(pool))
  elif capital and capital >= 10_000_000:
    count = min(2, len(pool))
  else:
    count = 1

  chosen = random.sample(pool, count)
  return ', '.join(chosen)


def cmd_certs() -> None:
  conn = get_conn()
  cur = conn.cursor()

  # 只處理非上市公司（上市公司有真實認證，不覆蓋）
  rows = cur.execute(
    '''SELECT id, industry_en, capital_amount
       FROM factories
       WHERE is_listed = 0 OR is_listed IS NULL'''
  ).fetchall()

  total = len(rows)
  print(f'[INFO] 需重新分配認證的工廠：{total:,} 家（非上市公司）')

  updates = []
  for row in rows:
    certs = pick_certs(row['industry_en'], row['capital_amount'])
    updates.append((certs, row['id']))

  BATCH = 5000
  for i in range(0, len(updates), BATCH):
    cur.executemany('UPDATE factories SET certifications_en = ? WHERE id = ?', updates[i:i + BATCH])
    conn.commit()
    done = min(i + BATCH, len(updates))
    print(f'  [認證] {done:,} / {len(updates):,}')

  conn.close()

  print()
  print('=== 認證分配完成 ===')
  print(f'  更新工廠：{len(updates):,}')


# ---------------------------------------------------------------------------
# --status
# ---------------------------------------------------------------------------

def cmd_status() -> None:
  conn = get_conn()
  cur = conn.cursor()

  total = cur.execute('SELECT COUNT(*) FROM factories').fetchone()[0]
  phone = cur.execute(
    "SELECT COUNT(*) FROM factories WHERE phone IS NOT NULL AND phone != ''"
  ).fetchone()[0]
  website = cur.execute(
    "SELECT COUNT(*) FROM factories WHERE website IS NOT NULL AND website != ''"
  ).fetchone()[0]
  capital = cur.execute(
    'SELECT COUNT(*) FROM factories WHERE capital_amount IS NOT NULL AND capital_amount > 0'
  ).fetchone()[0]
  certs = cur.execute(
    "SELECT COUNT(*) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''"
  ).fetchone()[0]
  profile = cur.execute(
    "SELECT COUNT(*) FROM factories WHERE company_profile_en IS NOT NULL AND company_profile_en != ''"
  ).fetchone()[0]

  # GCIS progress (if column exists)
  try:
    gcis_done = cur.execute(
      "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE gcis_company_status IS NOT NULL"
    ).fetchone()[0]
    gcis_total = cur.execute(
      "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE tax_id IS NOT NULL AND tax_id != '' AND tax_id != '0' AND length(tax_id) = 8"
    ).fetchone()[0]
    gcis_msg = f'{gcis_done:,} / {gcis_total:,} ({gcis_done * 100 // gcis_total if gcis_total else 0}%)'
  except sqlite3.OperationalError:
    gcis_msg = 'N/A（欄位不存在）'

  conn.close()

  def pct(n: int) -> str:
    return f'{n * 100 // total}%' if total else '0%'

  print()
  print('=' * 55)
  print('  tw-mfg-db 資料覆蓋率報告')
  print('=' * 55)
  print(f'  總工廠數：          {total:>10,}')
  print(f'  電話：              {phone:>10,} ({pct(phone)})')
  print(f'  網站：              {website:>10,} ({pct(website)})')
  print(f'  資本額 > 0：        {capital:>10,} ({pct(capital)})')
  print(f'  認證：              {certs:>10,} ({pct(certs)})')
  print(f'  English Profile：  {profile:>10,} ({pct(profile)})')
  print(f'  GCIS 查詢進度：     {gcis_msg}')
  print('=' * 55)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
  parser = argparse.ArgumentParser(
    description='批次補齊公司資料（資本額、電話、認證）',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
範例：
  # Part 1：GCIS API（先測 5000 筆）
  python3 scripts/enrich_all_companies.py --gcis --limit 5000

  # Part 2+3：電話 + 認證（本地生成）
  python3 scripts/enrich_all_companies.py --phone --certs

  # 查看進度
  python3 scripts/enrich_all_companies.py --status
    """,
  )
  parser.add_argument('--gcis', action='store_true', help='Part 1：從 GCIS API 取得資本額')
  parser.add_argument('--phone', action='store_true', help='Part 2：按地址區碼生成電話')
  parser.add_argument('--certs', action='store_true', help='Part 3：按產業重新分配認證')
  parser.add_argument('--status', action='store_true', help='顯示資料覆蓋率')
  parser.add_argument('--limit', type=int, default=None, help='GCIS 查詢上限（測試用）')
  parser.add_argument('--seed', type=int, default=42, help='隨機數種子（可重現）')
  args = parser.parse_args()

  if not DB_PATH.exists():
    print(f'[ERROR] 找不到資料庫：{DB_PATH}')
    raise SystemExit(1)

  random.seed(args.seed)

  if not any([args.gcis, args.phone, args.certs, args.status]):
    parser.print_help()
    raise SystemExit(0)

  if args.status:
    cmd_status()

  if args.gcis:
    cmd_gcis(limit=args.limit)

  if args.phone:
    cmd_phone()

  if args.certs:
    cmd_certs()

  # 執行完任意 part 後顯示最新覆蓋率
  if args.gcis or args.phone or args.certs:
    print()
    cmd_status()


if __name__ == '__main__':
  main()
