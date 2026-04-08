"""
enrich_address.py — 補查公司登記地址（registered_address）

用法：
  python3 -m scripts.enrich_address              # 補查所有缺地址的記錄
  python3 -m scripts.enrich_address --limit 100  # 測試 100 筆
  python3 -m scripts.enrich_address --status     # 顯示補查進度

補查條件：
  gcis_company_status IS NOT NULL（代表之前已查過 GCIS）
  AND registered_address IS NULL（尚未有登記地址）

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

# ---------------------------------------------------------------------------
# API 查詢
# ---------------------------------------------------------------------------

def query_address(tax_id: str) -> str | None:
  """
  查詢商工行政 API，只取 Company_Location。

  Returns:
    str  — 公司登記地址
    None — 查無資料或地址為空
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

  if not resp.text.strip():
    return None

  data = resp.json()
  if not data:
    return None

  return data[0].get('Company_Location') or None

# ---------------------------------------------------------------------------
# 子命令：--status
# ---------------------------------------------------------------------------

def cmd_status() -> None:
  conn = get_conn()
  cur = conn.cursor()

  cur.execute('''
    SELECT COUNT(DISTINCT tax_id)
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
      AND gcis_company_status IS NOT NULL
      AND gcis_company_status != 'not_found'
  ''')
  total_enriched = cur.fetchone()[0]

  cur.execute('''
    SELECT COUNT(DISTINCT tax_id)
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
      AND gcis_company_status IS NOT NULL
      AND gcis_company_status != 'not_found'
      AND registered_address IS NOT NULL
  ''')
  has_address = cur.fetchone()[0]

  conn.close()

  missing = total_enriched - has_address
  pct = (has_address / total_enriched * 100) if total_enriched else 0

  print('=== 登記地址補查進度 ===')
  print(f'  已查 GCIS（非 not_found）：{total_enriched:,}')
  print(f'  已有登記地址：            {has_address:,} ({pct:.1f}%)')
  print(f'  缺少登記地址：            {missing:,}')

# ---------------------------------------------------------------------------
# 子命令：補查（主流程）
# ---------------------------------------------------------------------------

def cmd_enrich(limit: int | None) -> None:
  conn = get_conn()
  cur = conn.cursor()

  query = '''
    SELECT DISTINCT tax_id
    FROM factories
    WHERE tax_id IS NOT NULL AND tax_id != ''
      AND gcis_company_status IS NOT NULL
      AND registered_address IS NULL
    ORDER BY tax_id
  '''
  if limit:
    query += f' LIMIT {limit}'

  cur.execute(query)
  tax_ids = [row['tax_id'] for row in cur.fetchall()]
  conn.close()

  total = len(tax_ids)
  if total == 0:
    print('所有記錄皆已有登記地址，無需補查。')
    return

  limit_msg = f'（上限 {limit} 筆）' if limit else ''
  print(f'開始補查 {total:,} 筆統編的登記地址{limit_msg}...')

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
      SET registered_address = :registered_address
      WHERE tax_id = :tax_id
        AND registered_address IS NULL
    ''', batch_updates)
    conn2.commit()
    conn2.close()
    batch_updates.clear()

  for idx, tax_id in enumerate(tax_ids, start=1):
    try:
      address = query_address(tax_id)

      if address:
        batch_updates.append({'tax_id': tax_id, 'registered_address': address})
        success_count += 1
      else:
        # 查到了但沒有地址，寫入空字串避免重複查詢
        batch_updates.append({'tax_id': tax_id, 'registered_address': ''})
        not_found_count += 1

    except requests.RequestException as e:
      # 網路錯誤：不寫入，讓下次可以重試
      error_count += 1
      print(f'\n  [WARN] {tax_id} 查詢失敗（將重試）: {e}')

    if idx % PROGRESS_INTERVAL == 0 or idx == total:
      pct = idx / total * 100
      print(
        f'  [{idx:,}/{total:,}] {pct:.1f}% — '
        f'成功 {success_count:,}｜查無 {not_found_count:,}｜錯誤 {error_count:,}'
      )

    if len(batch_updates) >= BATCH_SIZE:
      flush_batch()

    time.sleep(RATE_LIMIT_SLEEP)

  flush_batch()

  print('\n=== 完成 ===')
  print(f'  處理：{total:,} 筆統編')
  print(f'  取得地址：{success_count:,}')
  print(f'  查無地址：{not_found_count:,}')
  print(f'  錯誤（未寫入）：{error_count:,}')

# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
  parser = argparse.ArgumentParser(
    description='補查公司登記地址（registered_address）',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
範例：
  python3 -m scripts.enrich_address --status
  python3 -m scripts.enrich_address --limit 100
  python3 -m scripts.enrich_address
    """,
  )
  parser.add_argument(
    '--limit', type=int, default=None,
    help='最多補查幾筆唯一統編（用於測試）',
  )
  parser.add_argument(
    '--status', action='store_true',
    help='顯示目前補查進度並結束',
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
