"""
enrich_listed_companies.py

讀取上市（TWSE）與上櫃（TPEx）公司 CSV，
用 tax_id 比對 factories 表，更新聯絡資訊與英文名稱。

執行：
  cd projects/tw-mfg-db
  python3 -m scripts.enrich_listed_companies
"""

import csv
import re
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

TWSE_CSV = Path('/tmp/twse_companies.csv')
TPEX_CSV = Path('/tmp/tpex_companies.csv')

# 公司後綴關鍵字（有這些就不再補 Co., Ltd.）
_CORP_SUFFIXES = re.compile(
  r'\b(Co\.|Ltd\.|Corp\.|Inc\.|Corporation|Limited|Group|Holdings|International|Technology)\b',
  re.IGNORECASE,
)

# 廠區後綴中文 → 英文（依需要擴充）
_PLANT_SUFFIX_MAP = {
  '桃園廠': 'Taoyuan Plant',
  '新竹廠': 'Hsinchu Plant',
  '台南廠': 'Tainan Plant',
  '高雄廠': 'Kaohsiung Plant',
  '台中廠': 'Taichung Plant',
  '中壢廠': 'Zhongli Plant',
  '楊梅廠': 'Yangmei Plant',
  '第一廠': 'Plant 1',
  '第二廠': 'Plant 2',
  '第三廠': 'Plant 3',
}


# ---------------------------------------------------------------------------
# 輔助函式
# ---------------------------------------------------------------------------

def read_csv(path: Path, market: str) -> list[dict]:
  """讀取 BOM-UTF-8 CSV，回傳 list of dict，加入 market 欄位。"""
  rows = []
  with open(path, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
      row['_market'] = market
      rows.append(row)
  return rows


def build_name_en(eng_abbr: str, name_zh: str) -> str:
  """
  組合英文名稱。

  規則：
  - 英文簡稱若已含公司後綴 → 直接使用
  - 否則補上 Co., Ltd.
  - 若中文名有廠區後綴 → 在英文名後補英文廠區
  """
  eng_abbr = (eng_abbr or '').strip()
  if not eng_abbr:
    return ''

  # 偵測廠區後綴（中文名稱末尾）
  plant_suffix = ''
  for zh, en in _PLANT_SUFFIX_MAP.items():
    if name_zh and name_zh.endswith(zh):
      plant_suffix = en
      break

  # 補後綴
  if _CORP_SUFFIXES.search(eng_abbr):
    base = eng_abbr
  else:
    base = f'{eng_abbr} Co., Ltd.'

  return f'{base} {plant_suffix}'.strip()


# ---------------------------------------------------------------------------
# 主要邏輯
# ---------------------------------------------------------------------------

def main() -> None:
  # 1. 讀取 CSV
  all_rows: list[dict] = []
  if TWSE_CSV.exists():
    all_rows += read_csv(TWSE_CSV, 'TWSE')
    print(f'TWSE: {len([r for r in all_rows if r["_market"] == "TWSE"])} rows')
  else:
    print(f'WARNING: {TWSE_CSV} not found, skipping TWSE')

  tpex_rows = read_csv(TPEX_CSV, 'TPEx') if TPEX_CSV.exists() else []
  if tpex_rows:
    all_rows += tpex_rows
    print(f'TPEx: {len(tpex_rows)} rows')
  else:
    print(f'WARNING: {TPEX_CSV} not found, skipping TPEx')

  print(f'Total CSV rows: {len(all_rows)}')

  # 2. 建立 tax_id → company 索引（以 tax_id 為鍵）
  # CSV 欄位：營利事業統一編號
  csv_index: dict[str, dict] = {}
  for row in all_rows:
    tax_id = (row.get('營利事業統一編號') or '').strip()
    if tax_id:
      csv_index[tax_id] = row

  print(f'Unique tax_ids in CSV: {len(csv_index)}')

  # 3. 連線 DB，取得所有工廠的 tax_id
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row

  cur = conn.cursor()
  cur.execute('SELECT id, tax_id, name_zh FROM factories WHERE tax_id IS NOT NULL AND tax_id != ""')
  factories = cur.fetchall()
  print(f'Factories in DB with tax_id: {len(factories)}')

  # 4. 比對並更新
  matched = 0
  updated_rows = []

  for factory in factories:
    factory_tax_id = (factory['tax_id'] or '').strip()
    csv_row = csv_index.get(factory_tax_id)
    if csv_row is None:
      continue

    matched += 1
    name_zh = factory['name_zh'] or ''
    eng_abbr = (csv_row.get('英文簡稱') or '').strip()

    new_name_en = build_name_en(eng_abbr, name_zh) or None

    update = {
      'id': factory['id'],
      'phone': (csv_row.get('總機電話') or '').strip() or None,
      'email': (csv_row.get('電子郵件信箱') or '').strip() or None,
      'website': (csv_row.get('網址') or '').strip() or None,
      'fax': (csv_row.get('傳真機號碼') or '').strip() or None,
      'english_address': (csv_row.get('英文通訊地址') or '').strip() or None,
      'stock_id': (csv_row.get('公司代號') or '').strip() or None,
      'official_name_en': new_name_en,
      'is_listed': 1,
      'market': csv_row['_market'],
      'name_en': new_name_en,  # 同步更新 name_en
    }
    updated_rows.append(update)

  print(f'Matched: {matched}')

  # 5. 批次寫入
  update_sql = """
    UPDATE factories SET
      phone           = :phone,
      email           = :email,
      website         = :website,
      fax             = :fax,
      english_address = :english_address,
      stock_id        = :stock_id,
      official_name_en = :official_name_en,
      is_listed       = :is_listed,
      name_en         = :name_en
    WHERE id = :id
  """

  conn.executemany(update_sql, updated_rows)
  conn.commit()
  print(f'Updated {len(updated_rows)} factories in DB.')

  # 6. 統計
  cur.execute('SELECT COUNT(*) AS cnt FROM factories WHERE is_listed = 1')
  listed_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE phone IS NOT NULL AND phone != ''")
  phone_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE website IS NOT NULL AND website != ''")
  website_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE email IS NOT NULL AND email != ''")
  email_count = cur.fetchone()['cnt']

  print('\n=== 統計報告 ===')
  print(f'  上市櫃工廠數 (is_listed=1):  {listed_count}')
  print(f'  有電話的工廠數:              {phone_count}')
  print(f'  有網站的工廠數:              {website_count}')
  print(f'  有 email 的工廠數:           {email_count}')

  # 市場別分佈
  twse_updated = sum(1 for r in updated_rows if r['market'] == 'TWSE')
  tpex_updated = sum(1 for r in updated_rows if r['market'] == 'TPEx')
  print(f'  TWSE 匹配更新:              {twse_updated}')
  print(f'  TPEx 匹配更新:              {tpex_updated}')

  # 7. 重建 FTS5（加入 official_name_en）
  print('\n重建 FTS5 索引...')
  try:
    conn.executescript("""
      DROP TABLE IF EXISTS factories_fts;
      CREATE VIRTUAL TABLE factories_fts USING fts5(
        name_en, industry_en, city_en, district_en, products_en, certifications_en, official_name_en,
        content='factories', content_rowid='id'
      );
      INSERT INTO factories_fts(factories_fts) VALUES('rebuild');
    """)
    conn.commit()
    print('FTS5 重建完成。')
  except Exception as e:
    print(f'FTS5 重建失敗: {e}')

  conn.close()
  print('\n完成。')


if __name__ == '__main__':
  main()
