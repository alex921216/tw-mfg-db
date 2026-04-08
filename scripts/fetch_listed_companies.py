"""
fetch_listed_companies.py — 取得台灣上市 / 上櫃公司完整清單

資料來源：
  - 上市（TWSE）：https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
    更完整清單使用：https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
  - 上櫃（TPEX）：https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes

輸出：src/data/listed_companies.json
欄位：stock_code, company_name, industry, listing_date, market
"""

import json
import logging
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
OUTPUT_PATH = DATA_DIR / 'listed_companies.json'

REQUEST_TIMEOUT = 30
RETRY_MAX = 3
RETRY_DELAY = 3  # seconds

USER_AGENT = 'tw-mfg-db/1.0 (research project)'

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TWSE 上市公司 endpoint
# 使用 ISIN 查詢頁面取得完整有價證券基本資料（含股票代號、名稱、產業、掛牌日期）
# ---------------------------------------------------------------------------

# openapi.twse.com.tw 提供的公司基本資料 JSON API
TWSE_COMPANY_API = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'

# TPEX 上櫃公司基本資料 API（JSON）
TPEX_COMPANY_API = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O'


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def fetch_json(url: str) -> Any:
  """
  取得 URL 的 JSON 回應，含 retry 機制（最多 3 次）。

  Args:
    url: 目標 URL

  Returns:
    解析後的 JSON 物件

  Raises:
    RuntimeError: 所有 retry 均失敗時
  """
  for attempt in range(1, RETRY_MAX + 1):
    try:
      log.info(f'GET {url} (attempt {attempt}/{RETRY_MAX})')
      ctx = ssl.create_default_context()
      ctx.check_hostname = False
      ctx.verify_mode = ssl.CERT_NONE
      req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
      with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
        raw = resp.read().decode('utf-8')
      return json.loads(raw)
    except urllib.error.HTTPError as e:
      log.warning(f'HTTP {e.code} on attempt {attempt}: {url}')
    except urllib.error.URLError as e:
      log.warning(f'URLError on attempt {attempt}: {e.reason}')
    except Exception as e:
      log.warning(f'Unexpected error on attempt {attempt}: {e}')

    if attempt < RETRY_MAX:
      log.info(f'Retrying in {RETRY_DELAY}s...')
      time.sleep(RETRY_DELAY)

  raise RuntimeError(f'All {RETRY_MAX} attempts failed for: {url}')


def normalize_date(raw: str) -> str:
  """
  將民國日期（如 "0890101" 或 "089/01/01"）或西元日期統一為 YYYY-MM-DD。

  Args:
    raw: 原始日期字串

  Returns:
    YYYY-MM-DD 或空字串
  """
  if not raw:
    return ''

  raw = raw.strip().replace('/', '').replace('-', '')

  # 純數字格式處理
  if raw.isdigit():
    if len(raw) == 7:
      # 民國格式 YYYMMDD，YYY 為民國年（如 089 = 民國89年 = 西元2000年）
      roc_year = int(raw[:3])
      month = raw[3:5]
      day = raw[5:7]
      ad_year = roc_year + 1911
      return f'{ad_year}-{month}-{day}'
    elif len(raw) == 8:
      # 西元格式 YYYYMMDD
      return f'{raw[:4]}-{raw[4:6]}-{raw[6:8]}'

  return raw


# ---------------------------------------------------------------------------
# 上市公司（TWSE）
# ---------------------------------------------------------------------------

def fetch_twse_companies() -> list[dict[str, Any]]:
  """
  從 TWSE OpenAPI 取得上市公司清單。

  Returns:
    標準化後的公司 dict list
  """
  log.info('Fetching TWSE listed companies...')
  try:
    data = fetch_json(TWSE_COMPANY_API)
  except RuntimeError as e:
    log.error(f'TWSE fetch failed: {e}')
    return []

  if not isinstance(data, list):
    log.error(f'Unexpected TWSE response type: {type(data)}')
    return []

  companies: list[dict[str, Any]] = []
  for row in data:
    stock_code = str(row.get('公司代號', '') or row.get('有價證券代號', '')).strip()
    company_name = str(row.get('公司名稱', '') or row.get('有價證券名稱', '')).strip()

    if not stock_code or not company_name:
      continue

    # 只保留股票（4位數字代號為主體，部分為 5-6 位含字母）
    # 排除 ETF、基金等非股票證券（代號含字母且非公司型態）
    if not stock_code[:4].isdigit():
      continue

    industry = str(row.get('產業類別', '') or row.get('市場別', '')).strip()
    listing_date = normalize_date(str(row.get('上市日期', '') or row.get('掛牌日期', '')).strip())

    companies.append({
      'stock_code': stock_code,
      'company_name': company_name,
      'industry': industry,
      'listing_date': listing_date,
      'market': 'TWSE',
    })

  log.info(f'TWSE: fetched {len(companies)} companies')
  return companies


# ---------------------------------------------------------------------------
# 上櫃公司（TPEX）
# ---------------------------------------------------------------------------

def fetch_tpex_companies() -> list[dict[str, Any]]:
  """
  從 TPEX OpenAPI 取得上櫃公司清單。

  Returns:
    標準化後的公司 dict list
  """
  log.info('Fetching TPEX listed companies...')
  try:
    data = fetch_json(TPEX_COMPANY_API)
  except RuntimeError as e:
    log.error(f'TPEX fetch failed: {e}')
    return []

  if not isinstance(data, list):
    log.error(f'Unexpected TPEX response type: {type(data)}')
    return []

  companies: list[dict[str, Any]] = []
  for row in data:
    stock_code = str(row.get('公司代號', '') or row.get('SecuritiesCompanyCode', '')).strip()
    company_name = str(row.get('公司名稱', '') or row.get('CompanyName', '')).strip()

    if not stock_code or not company_name:
      continue

    if not stock_code[:4].isdigit():
      continue

    industry = str(row.get('產業類別', '') or row.get('IndustryType', '')).strip()
    listing_date = normalize_date(str(row.get('上櫃日期', '') or row.get('ListingDate', '')).strip())

    companies.append({
      'stock_code': stock_code,
      'company_name': company_name,
      'industry': industry,
      'listing_date': listing_date,
      'market': 'TPEX',
    })

  log.info(f'TPEX: fetched {len(companies)} companies')
  return companies


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
  DATA_DIR.mkdir(parents=True, exist_ok=True)

  twse_companies = fetch_twse_companies()
  time.sleep(2)  # Rate limiting：兩個來源之間暫停

  tpex_companies = fetch_tpex_companies()

  # 合併並去重（以 stock_code 為主鍵）
  seen_codes: set[str] = set()
  all_companies: list[dict[str, Any]] = []

  for company in twse_companies + tpex_companies:
    code = company['stock_code']
    if code not in seen_codes:
      seen_codes.add(code)
      all_companies.append(company)

  # 按股票代號排序
  all_companies.sort(key=lambda c: c['stock_code'])

  log.info(f'Total unique companies: {len(all_companies)} (TWSE: {len(twse_companies)}, TPEX: {len(tpex_companies)})')

  with OUTPUT_PATH.open('w', encoding='utf-8') as f:
    json.dump(all_companies, f, ensure_ascii=False, indent=2)

  log.info(f'Written to {OUTPUT_PATH}')

  # 輸出簡易統計
  twse_count = sum(1 for c in all_companies if c['market'] == 'TWSE')
  tpex_count = sum(1 for c in all_companies if c['market'] == 'TPEX')
  log.info(f'Summary — TWSE: {twse_count}, TPEX: {tpex_count}, Total: {len(all_companies)}')


if __name__ == '__main__':
  main()
