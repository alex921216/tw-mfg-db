"""
enrich_gcis.py — 商工行政資料 API 批次查詢，補充工廠資本額等資訊

用法：
  python3 -m scripts.enrich_gcis              # 完整查詢
  python3 -m scripts.enrich_gcis --limit 200  # 測試 200 筆
  python3 -m scripts.enrich_gcis --status     # 顯示進度

API 來源：
  https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6
"""

import argparse
import sqlite3
import time
import urllib3
from pathlib import Path

import requests

# 台灣政府 API 憑證 Missing Subject Key Identifier，無法通過標準 SSL 驗證
# 停用驗證並抑制警告（僅針對此特定可信來源）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SCRIPTS_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPTS_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 常數
# ---------------------------------------------------------------------------

GCIS_API_URL = (
  'https://data.gcis.nat.gov.tw/od/data/api/'
  '5F64D864-61CB-4D0D-8AD9-492047CC1EA6'
)
RATE_LIMIT_SLEEP = 0.5   # 每次請求間隔 0.5s → 最多 2 req/s
BATCH_SIZE = 100          # 每 100 筆 commit 一次
PROGRESS_INTERVAL = 100   # 每 100 筆印出進度

# ---------------------------------------------------------------------------
# DB 工具
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')
  return conn


def parse_setup_date(raw: str | None) -> str | None:
  """將民國年日期（如 '0680718'）轉換為西元 ISO 格式（如 '1979-07-18'）。"""
  if not raw or len(raw) < 7:
    return raw
  try:
    # 格式：YYYMMDD（民國年 3 位 + 月 2 位 + 日 2 位）
    roc_year = int(raw[:3])
    month = raw[3:5]
    day = raw[5:7]
    ad_year = roc_year + 1911
    return f'{ad_year}-{month}-{day}'
  except (ValueError, IndexError):
    return raw

# ---------------------------------------------------------------------------
# API 查詢
# ---------------------------------------------------------------------------

def query_gcis(tax_id: str) -> dict | None:
  """
  查詢商工行政 API。

  Returns:
    dict — 查詢到的公司資料
    None — 查無資料（獨資、合夥、外國公司等）
  Raises:
    requests.RequestException — 網路或 API 錯誤
  """
  params = {
    '$format': 'json',
    '$filter': f'Business_Accounting_NO eq {tax_id}',
    '$skip': '0',
    '$top': '1',
  }
  resp = requests.get(GCIS_API_URL, params=params, timeout=15, verify=False)
  resp.raise_for_status()

  # 部分統編（空的統編或格式不合的）API 回傳空 body
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

# ---------------------------------------------------------------------------
# 子命令：--status
# ---------------------------------------------------------------------------

def cmd_status() -> None:
  conn = get_conn()
  cur = conn.cursor()

  cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE tax_id IS NOT NULL AND tax_id != ""')
  total_unique = cur.fetchone()[0]

  cur.execute('''
    SELECT COUNT(DISTINCT tax_id)
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
      AND gcis_company_status IS NOT NULL
  ''')
  done = cur.fetchone()[0]

  cur.execute('''
    SELECT COUNT(DISTINCT tax_id)
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
      AND gcis_company_status = 'not_found'
  ''')
  not_found = cur.fetchone()[0]

  conn.close()

  remaining = total_unique - done
  pct = (done / total_unique * 100) if total_unique else 0

  print(f'=== GCIS 查詢進度 ===')
  print(f'  總唯一統編：{total_unique:,}')
  print(f'  已查詢：    {done:,} ({pct:.1f}%)')
  print(f'    其中查無：{not_found:,}')
  print(f'  尚未查詢：  {remaining:,}')

# ---------------------------------------------------------------------------
# 子命令：查詢（主流程）
# ---------------------------------------------------------------------------

def cmd_enrich(limit: int | None) -> None:
  conn = get_conn()
  cur = conn.cursor()

  # 取得尚未查詢的唯一 tax_id（gcis_company_status IS NULL 代表尚未處理）
  query = '''
    SELECT DISTINCT tax_id
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
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
    print('所有統編皆已查詢完畢，無需更新。')
    return

  limit_msg = f'（上限 {limit} 筆）' if limit else ''
  print(f'開始查詢 {total:,} 筆統編{limit_msg}...')

  success_count = 0
  not_found_count = 0
  error_count = 0
  batch_updates: list[dict] = []

  def flush_batch() -> None:
    if not batch_updates:
      return
    conn2 = get_conn()
    cur2 = conn2.cursor()
    cur2.executemany('''
      UPDATE factories
      SET capital_amount       = :capital_amount,
          paid_in_capital      = :paid_in_capital,
          company_setup_date   = :company_setup_date,
          gcis_company_status  = :gcis_company_status,
          registered_address   = :registered_address
      WHERE tax_id = :tax_id
        AND gcis_company_status IS NULL
    ''', batch_updates)
    conn2.commit()
    conn2.close()
    batch_updates.clear()

  for idx, tax_id in enumerate(tax_ids, start=1):
    try:
      result = query_gcis(tax_id)

      if result is None:
        batch_updates.append({
          'tax_id': tax_id,
          'capital_amount': None,
          'paid_in_capital': None,
          'company_setup_date': None,
          'gcis_company_status': 'not_found',
          'registered_address': None,
        })
        not_found_count += 1
      else:
        batch_updates.append({'tax_id': tax_id, **result})
        success_count += 1

    except requests.RequestException as e:
      # 網路錯誤：記錄但不標記，讓下次可以重試
      error_count += 1
      print(f'\n  [WARN] {tax_id} 查詢失敗（將重試）: {e}')

    # 進度輸出
    if idx % PROGRESS_INTERVAL == 0 or idx == total:
      pct = idx / total * 100
      print(
        f'  [{idx:,}/{total:,}] {pct:.1f}% — '
        f'成功 {success_count:,}｜查無 {not_found_count:,}｜錯誤 {error_count:,}'
      )

    # 批次 commit
    if len(batch_updates) >= BATCH_SIZE:
      flush_batch()

    time.sleep(RATE_LIMIT_SLEEP)

  # 最後一批
  flush_batch()

  print(f'\n=== 完成 ===')
  print(f'  處理：{total:,} 筆統編')
  print(f'  成功：{success_count:,}')
  print(f'  查無：{not_found_count:,}')
  print(f'  錯誤（未寫入）：{error_count:,}')

# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
  parser = argparse.ArgumentParser(
    description='商工行政 API 批次查詢，補充工廠資本額等資訊',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
範例：
  python3 -m scripts.enrich_gcis --status
  python3 -m scripts.enrich_gcis --limit 200
  python3 -m scripts.enrich_gcis
    """,
  )
  parser.add_argument(
    '--limit', type=int, default=None,
    help='最多查詢幾筆唯一統編（用於測試）',
  )
  parser.add_argument(
    '--status', action='store_true',
    help='顯示目前查詢進度並結束',
  )
  args = parser.parse_args()

  if not DB_PATH.exists():
    print(f'錯誤：找不到資料庫 {DB_PATH}')
    raise SystemExit(1)

  if args.status:
    cmd_status()
  else:
    cmd_enrich(limit=args.limit)


if __name__ == '__main__':
  main()
