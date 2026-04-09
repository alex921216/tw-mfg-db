"""
scrape_findbiz_phones.py — 從 FINDBIZ 取得工廠電話號碼

執行方式（在 src/ 目錄下）：
  python scripts/scrape_findbiz_phones.py [--limit N] [--delay SECONDS]

選項：
  --limit N        只處理前 N 家沒有電話的工廠（預設 100，用於測試）
  --delay SECONDS  每次請求之間的延遲秒數（預設 0.5）
  --all            處理所有沒有電話的工廠（覆蓋 --limit）

FINDBIZ API：
  https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do?fhl=zh&queryStr={tax_id}

如果 API 不可達或被限流，腳本會印出統計後退出，不修改 DB。
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

try:
  import requests
  HAS_REQUESTS = True
except ImportError:
  HAS_REQUESTS = False

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

FINDBIZ_BASE = 'https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do'
HEADERS = {
  'User-Agent': 'Mozilla/5.0 (compatible; TMDB-Enricher/1.0)',
  'Accept': 'application/json, text/html',
  'Referer': 'https://findbiz.nat.gov.tw/',
}

REQUEST_TIMEOUT = 8  # seconds


def fetch_phone_from_findbiz(tax_id: str, session: 'requests.Session') -> str | None:
  """
  嘗試從 FINDBIZ 查詢公司電話號碼。

  FINDBIZ 的工廠查詢 (factoryList.do) 需要特定 POST session，
  此函式嘗試用 GET 方式查詢；如果回傳的 HTML 中包含電話，才採用。
  """
  try:
    resp = session.get(
      'https://findbiz.nat.gov.tw/fts/query/Factory/factoryList.do',
      params={'bizUniNum': tax_id, 'fhl': 'zh'},
      headers=HEADERS,
      timeout=REQUEST_TIMEOUT,
      allow_redirects=True,
    )

    if resp.status_code != 200:
      return None

    text = resp.text
    import re

    # 台灣電話格式，鄰近「電話」關鍵字才採用（避免抓到頁面通用電話）
    phone_pattern = re.compile(
      r'(?:電話|TEL|Tel)[^0-9]*(\(0\d{1,2}\)\s*\d{4}[\s-]?\d{4}|\d{2,3}[\s-]\d{3,4}[\s-]\d{4})'
    )
    matches = phone_pattern.findall(text)
    if matches:
      # 確認找到的電話不是頁面共用電話
      phone = matches[0].strip()
      if phone and '2412' not in phone and '1166' not in phone:
        return phone

  except Exception:
    return None

  return None


def main() -> None:
  parser = argparse.ArgumentParser(description='Scrape phone numbers from FINDBIZ')
  parser.add_argument('--limit', type=int, default=100, help='Number of factories to process')
  parser.add_argument('--delay', type=float, default=0.5, help='Delay between requests (seconds)')
  parser.add_argument('--all', action='store_true', help='Process all factories without phone')
  args = parser.parse_args()

  if not HAS_REQUESTS:
    print('[ERROR] requests 套件未安裝，請執行：pip install requests')
    sys.exit(1)

  if not DB_PATH.exists():
    print(f'[ERROR] 找不到資料庫：{DB_PATH}')
    sys.exit(1)

  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')
  cur = conn.cursor()

  # 找出沒有電話且有 tax_id 的工廠
  query = """
    SELECT id, tax_id, name_zh, city_en
    FROM factories
    WHERE (phone IS NULL OR phone = '')
      AND tax_id IS NOT NULL
      AND tax_id != ''
    ORDER BY is_listed DESC, hidden_champion_score DESC
  """
  if not args.all:
    query += f' LIMIT {args.limit}'

  factories = cur.execute(query).fetchall()
  total = len(factories)
  print(f'[INFO] 待處理工廠：{total:,} 家')

  if total == 0:
    print('[INFO] 所有工廠已有電話號碼')
    conn.close()
    return

  # 測試 FINDBIZ 是否可達
  session = requests.Session()
  print('[INFO] 測試 FINDBIZ 連線...')
  try:
    test_resp = session.get(
      FINDBIZ_BASE,
      params={'fhl': 'zh', 'queryStr': '22093672'},  # 台積電
      headers=HEADERS,
      timeout=REQUEST_TIMEOUT,
    )
    print(f'[INFO] FINDBIZ 回應狀態：{test_resp.status_code}')
    if test_resp.status_code != 200:
      print('[WARN] FINDBIZ API 回應異常，可能無法取得資料')
  except Exception as e:
    print(f'[WARN] 無法連接 FINDBIZ：{e}')
    print('[INFO] 跳過電話爬取，建議改用其他資料來源')
    conn.close()
    return

  # 開始爬取
  stats = {'success': 0, 'not_found': 0, 'error': 0}
  updates = []

  for i, factory in enumerate(factories):
    tax_id = factory['tax_id']
    name_zh = factory['name_zh']

    phone = fetch_phone_from_findbiz(tax_id, session)

    if phone:
      updates.append((phone, factory['id']))
      stats['success'] += 1
      print(f'[OK] {name_zh} ({tax_id}) → {phone}')
    else:
      stats['not_found'] += 1

    # 批次寫入
    if len(updates) >= 50:
      cur.executemany('UPDATE factories SET phone = ? WHERE id = ?', updates)
      conn.commit()
      updates.clear()

    # 進度
    if (i + 1) % 10 == 0:
      print(f'[INFO] 進度：{i+1}/{total} | 成功：{stats["success"]} | 未找到：{stats["not_found"]}')

    time.sleep(args.delay)

  # 寫入剩餘
  if updates:
    cur.executemany('UPDATE factories SET phone = ? WHERE id = ?', updates)
    conn.commit()

  conn.close()

  print()
  print('=' * 50)
  print('FINDBIZ 電話爬取完成')
  print('=' * 50)
  print(f'  處理工廠：  {total:>8,}')
  print(f'  成功取得：  {stats["success"]:>8,}')
  print(f'  未找到：    {stats["not_found"]:>8,}')
  print(f'  成功率：    {stats["success"]*100//total if total else 0:>7}%')
  print('=' * 50)
  print()
  print('[NOTE] 對於無電話的公司，Company Profile 已說明')
  print('       "Contact information available upon request."')


if __name__ == '__main__':
  main()
