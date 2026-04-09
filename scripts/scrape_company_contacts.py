#!/usr/bin/env python3
"""
scrape_company_contacts.py

從 TWSE / TPEX 公開 API 爬取上市櫃公司聯絡資訊（電話、網站、email、傳真），
用 tax_id（營利事業統一編號）比對 factories 表並寫入 DB。

執行：
  cd /Users/alex/Desktop/forge-internal-master/projects/tw-mfg-db/src
  source .venv/bin/activate
  python3 scripts/scrape_company_contacts.py
"""

import json
import re
import sqlite3
import ssl
import time
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

SRC_DIR = Path(__file__).resolve().parent.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

TWSE_API = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
TPEX_API = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O'

REQUEST_DELAY = 1.5  # 秒，避免對政府網站造成負擔

# SSL workaround：台灣政府網站憑證有時無法驗證
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# 公司後綴關鍵字
_CORP_SUFFIXES = re.compile(
  r'\b(Co\.|Ltd\.|Corp\.|Inc\.|Corporation|Limited|Group|Holdings|International|Technology)\b',
  re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def fetch_json(url: str) -> list[dict] | None:
  """發送 GET 請求，回傳 JSON list；失敗回傳 None。"""
  try:
    req = urllib.request.Request(
      url,
      headers={
        'User-Agent': 'Mozilla/5.0 (compatible; tw-mfg-db/1.0)',
        'Accept': 'application/json',
      }
    )
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
      raw = resp.read()
      return json.loads(raw)
  except Exception as e:
    print(f'  ERROR fetching {url}: {e}')
    return None


def clean(value: str | None) -> str | None:
  """清除前後空白與全形空白，空字串回傳 None。"""
  if value is None:
    return None
  cleaned = value.strip().strip('\u3000').strip()
  return cleaned if cleaned and cleaned not in ('-', '－', '—') else None


def build_name_en(eng_abbr: str | None) -> str | None:
  """組合英文名稱，若無後綴則補 Co., Ltd.。"""
  eng_abbr = (eng_abbr or '').strip()
  if not eng_abbr:
    return None
  if _CORP_SUFFIXES.search(eng_abbr):
    return eng_abbr
  return f'{eng_abbr} Co., Ltd.'


# ---------------------------------------------------------------------------
# 資料來源解析
# ---------------------------------------------------------------------------

def parse_twse(records: list[dict]) -> list[dict]:
  """將 TWSE JSON records 轉為統一 dict 格式。"""
  result = []
  for r in records:
    tax_id = clean(r.get('營利事業統一編號'))
    if not tax_id:
      continue
    result.append({
      'tax_id': tax_id,
      'stock_id': clean(r.get('公司代號')),
      'market': 'TWSE',
      'phone': clean(r.get('總機電話')),
      'fax': clean(r.get('傳真機號碼')),
      'email': clean(r.get('電子郵件信箱')),
      'website': clean(r.get('網址')),
      'english_address': clean(r.get('英文通訊地址')),
      'official_name_en': build_name_en(r.get('英文簡稱')),
      'capital_amount': _parse_int(r.get('實收資本額')),
    })
  return result


def parse_tpex(records: list[dict]) -> list[dict]:
  """將 TPEX JSON records 轉為統一 dict 格式。"""
  result = []
  for r in records:
    tax_id = clean(r.get('UnifiedBusinessNo.'))
    if not tax_id:
      continue
    result.append({
      'tax_id': tax_id,
      'stock_id': clean(r.get('SecuritiesCompanyCode')),
      'market': 'TPEx',
      'phone': clean(r.get('Telephone')),
      'fax': clean(r.get('Fax')),
      'email': clean(r.get('EmailAddress')),
      'website': clean(r.get('WebAddress')),
      'english_address': clean(r.get('Address')),
      'official_name_en': build_name_en(r.get('Symbol')),
      'capital_amount': _parse_int(r.get('Paidin.Capital.NTDollars')),
    })
  return result


def _parse_int(value) -> int | None:
  try:
    return int(str(value).strip().replace(',', ''))
  except Exception:
    return None


# ---------------------------------------------------------------------------
# DB 更新
# ---------------------------------------------------------------------------

UPDATE_SQL = """
  UPDATE factories SET
    phone            = COALESCE(phone,            :phone),
    fax              = COALESCE(fax,              :fax),
    email            = COALESCE(email,            :email),
    website          = COALESCE(website,          :website),
    english_address  = COALESCE(english_address,  :english_address),
    stock_id         = COALESCE(stock_id,         :stock_id),
    official_name_en = COALESCE(official_name_en, :official_name_en),
    capital_amount   = COALESCE(capital_amount,   :capital_amount),
    is_listed        = 1
  WHERE tax_id = :tax_id
"""


def update_db(conn: sqlite3.Connection, companies: list[dict]) -> dict:
  """批次更新 factories 表，回傳統計資訊。"""
  stats = {'twse': 0, 'tpex': 0, 'total_matched': 0}

  for company in companies:
    before = conn.total_changes
    conn.execute(UPDATE_SQL, company)
    if conn.total_changes > before:
      stats['total_matched'] += 1
      if company['market'] == 'TWSE':
        stats['twse'] += 1
      else:
        stats['tpex'] += 1

  conn.commit()
  return stats


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

def main() -> None:
  print('=== scrape_company_contacts.py ===\n')

  # 1. 取得 TWSE 資料
  print(f'[1/4] 下載 TWSE 上市公司資料 ({TWSE_API})')
  twse_raw = fetch_json(TWSE_API)
  if twse_raw:
    print(f'  取得 {len(twse_raw)} 筆')
    twse_companies = parse_twse(twse_raw)
    print(f'  有效 tax_id: {len(twse_companies)} 筆')
  else:
    print('  TWSE API 不可達，跳過')
    twse_companies = []

  time.sleep(REQUEST_DELAY)

  # 2. 取得 TPEX 資料
  print(f'\n[2/4] 下載 TPEX 上櫃公司資料 ({TPEX_API})')
  tpex_raw = fetch_json(TPEX_API)
  if tpex_raw:
    print(f'  取得 {len(tpex_raw)} 筆')
    tpex_companies = parse_tpex(tpex_raw)
    print(f'  有效 tax_id: {len(tpex_companies)} 筆')
  else:
    print('  TPEX API 不可達，跳過')
    tpex_companies = []

  all_companies = twse_companies + tpex_companies
  print(f'\n  合計: {len(all_companies)} 筆')

  if not all_companies:
    print('\nERROR: 無任何資料可更新，結束。')
    return

  # 3. 連線 DB 並更新
  print(f'\n[3/4] 更新 DB ({DB_PATH})')
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row

  stats = update_db(conn, all_companies)
  print(f'  TWSE 匹配更新: {stats["twse"]}')
  print(f'  TPEx 匹配更新: {stats["tpex"]}')
  print(f'  合計匹配更新:  {stats["total_matched"]}')

  # 4. 統計報告
  print('\n[4/4] 統計報告')
  cur = conn.cursor()

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE is_listed = 1")
  listed_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE phone IS NOT NULL AND phone != ''")
  phone_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE website IS NOT NULL AND website != ''")
  website_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE email IS NOT NULL AND email != ''")
  email_count = cur.fetchone()['cnt']

  cur.execute("SELECT COUNT(*) AS cnt FROM factories WHERE fax IS NOT NULL AND fax != ''")
  fax_count = cur.fetchone()['cnt']

  print(f'  上市櫃工廠數 (is_listed=1): {listed_count:,}')
  print(f'  有電話的工廠數:             {phone_count:,}')
  print(f'  有網站的工廠數:             {website_count:,}')
  print(f'  有 email 的工廠數:          {email_count:,}')
  print(f'  有傳真的工廠數:             {fax_count:,}')

  # 樣本資料展示
  cur.execute("""
    SELECT name_zh, phone, website, email, stock_id
    FROM factories
    WHERE is_listed = 1 AND phone IS NOT NULL
    LIMIT 5
  """)
  rows = cur.fetchall()
  if rows:
    print('\n  樣本資料（前 5 筆）:')
    for row in rows:
      print(f'    [{row["stock_id"]}] {row["name_zh"]} | {row["phone"]} | {row["website"]}')

  conn.close()
  print('\n完成。')


if __name__ == '__main__':
  main()
