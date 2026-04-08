"""
match_supply_chain.py — 供應鏈模糊比對引擎

將 supply_chain_raw.json 中的供應商名稱與 tmdb.db factories 表做多階段比對，
找出對應的統一編號（tax_id）。

比對策略（由精確到模糊）：
  1. exact    — 完全比對
  2. contains — 包含比對（supplier 包含在 factory，或反之）
  3. keyword  — 去除常見後綴後比對
  4. fts_like — SQLite LIKE 搜尋（中文 FTS5 索引只含英文欄位，改用 LIKE）

輸出：
  src/data/supply_chain_matched.json  — 比對成功的記錄
  同時更新 supply_chain_raw.json 的 supplier_tax_id 欄位

執行：
  cd src/  &&  python scripts/match_supply_chain.py
  或
  python src/scripts/match_supply_chain.py
"""

import gzip
import json
import logging
import re
import shutil
import sqlite3
import unicodedata
from pathlib import Path

# ---------------------------------------------------------------------------
# 路徑
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
DB_GZ_PATH = SRC_DIR / 'data' / 'tmdb.db.gz'
RAW_PATH = SRC_DIR / 'data' / 'supply_chain_raw.json'
MATCHED_PATH = SRC_DIR / 'data' / 'supply_chain_matched.json'

# ---------------------------------------------------------------------------
# 日誌
# ---------------------------------------------------------------------------

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s  %(levelname)-7s  %(message)s',
  datefmt='%H:%M:%S',
)
log = logging.getLogger('match_supply_chain')

# ---------------------------------------------------------------------------
# 常見後綴（正規化時移除）
# ---------------------------------------------------------------------------

_SUFFIXES = [
  '股份有限公司', '有限公司', '股份有限', '無限公司',
  '企業股份有限公司', '工業股份有限公司', '科技股份有限公司',
  '企業有限公司', '工業有限公司', '科技有限公司',
  '企業社', '企業', '實業', '工業', '科技', '國際',
  '(股)公司', '(有)公司', '(股)', '(有)',
  '公司',
]

# 按長度降序排列，優先去除較長後綴
_SUFFIXES = sorted(_SUFFIXES, key=len, reverse=True)


def normalize(name: str) -> str:
  """
  正規化名稱：
  - 全形轉半形
  - 去除空白（含全形空格、換行）
  - 統一為 NFKC Unicode
  """
  if not name:
    return ''
  # Unicode 正規化（全形 → 半形）
  name = unicodedata.normalize('NFKC', name)
  # 去除所有空白字元（空格、\t、\n、全形空格）
  name = re.sub(r'[\s\u3000]+', '', name)
  return name.strip()


def strip_suffixes(name: str) -> str:
  """去除常見公司後綴，保留關鍵字核心。"""
  for suffix in _SUFFIXES:
    if name.endswith(suffix):
      name = name[: -len(suffix)]
      break
  return name.strip()


def is_junk_name(name: str) -> bool:
  """
  過濾明顯不是公司名稱的雜訊（PDF 爬取殘留）。
  規則：長度 < 2、包含亂碼字元、或是常見統計標籤。
  """
  if not name or len(name) < 2:
    return True
  junk_patterns = [
    r'^[\x00-\x1f\x7f-\x9f\xff]',   # 控制字元 / 亂碼
    r'淨額$',
    r'^其[\s　]?他$',
    r'^本公司',
    r'^年度',
    r'^進貨',
    r'^銷貨',
    r'^\d+$',                         # 純數字
  ]
  for pattern in junk_patterns:
    if re.search(pattern, name):
      return True
  return False


# ---------------------------------------------------------------------------
# 載入工廠資料（分頁讀取，避免 Supabase-style 截斷問題）
# ---------------------------------------------------------------------------

PAGE_SIZE = 5000


def load_factories(conn: sqlite3.Connection) -> dict[str, tuple[str, str]]:
  """
  將 factories 表全部讀入記憶體，回傳兩個索引：
    normalized_index: { normalize(name_zh) -> (name_zh, tax_id) }
    keyword_index:    { strip_suffixes(normalize(name_zh)) -> (name_zh, tax_id) }

  使用分頁讀取，確保 99,000+ 筆資料完整載入。
  """
  normalized_index: dict[str, tuple[str, str]] = {}
  keyword_index: dict[str, tuple[str, str]] = {}

  offset = 0
  total_loaded = 0

  log.info('開始載入 factories 資料（分頁 %d 筆）...', PAGE_SIZE)

  while True:
    cur = conn.cursor()
    cur.execute(
      'SELECT name_zh, tax_id FROM factories WHERE name_zh IS NOT NULL ORDER BY id LIMIT ? OFFSET ?',
      (PAGE_SIZE, offset),
    )
    rows = cur.fetchall()
    if not rows:
      break

    for name_zh, tax_id in rows:
      if not name_zh or not tax_id:
        continue
      norm = normalize(name_zh)
      kw = strip_suffixes(norm)
      # 完全比對索引（後進者覆蓋，但同名工廠通常同一公司）
      normalized_index[norm] = (name_zh, tax_id)
      if kw and len(kw) >= 2:
        keyword_index[kw] = (name_zh, tax_id)

    total_loaded += len(rows)
    offset += PAGE_SIZE

    if len(rows) < PAGE_SIZE:
      break

  log.info('載入完成：%d 筆工廠，normalized_index=%d，keyword_index=%d',
           total_loaded, len(normalized_index), len(keyword_index))
  return normalized_index, keyword_index


# ---------------------------------------------------------------------------
# 比對函式
# ---------------------------------------------------------------------------

def match_exact(
  norm_supplier: str,
  normalized_index: dict[str, tuple[str, str]],
) -> tuple[str, str] | None:
  """策略 1：完全比對。"""
  entry = normalized_index.get(norm_supplier)
  if entry:
    return entry
  return None


def match_contains(
  norm_supplier: str,
  normalized_index: dict[str, tuple[str, str]],
) -> tuple[str, str] | None:
  """
  策略 2：包含比對。
  - supplier 包含在某工廠名稱中（factory contains supplier）
  - 某工廠名稱包含在 supplier 中（supplier contains factory）
  只取第一個命中（優先最短的工廠名以減少誤配）。
  """
  if len(norm_supplier) < 3:
    return None

  best: tuple[str, str] | None = None
  best_len = 9999

  for norm_factory, entry in normalized_index.items():
    if norm_supplier in norm_factory or norm_factory in norm_supplier:
      if len(norm_factory) < best_len:
        best = entry
        best_len = len(norm_factory)

  return best


def match_keyword(
  norm_supplier: str,
  keyword_index: dict[str, tuple[str, str]],
) -> tuple[str, str] | None:
  """策略 3：去除後綴後比對。"""
  kw = strip_suffixes(norm_supplier)
  if not kw or len(kw) < 2:
    return None
  return keyword_index.get(kw)


def match_fts_like(
  norm_supplier: str,
  conn: sqlite3.Connection,
) -> tuple[str, str] | None:
  """
  策略 4：SQLite LIKE 搜尋。
  由於 factories_fts 只索引英文欄位，中文比對改用 LIKE。
  使用去除後綴後的關鍵字降低誤比率。
  """
  kw = strip_suffixes(norm_supplier)
  if not kw or len(kw) < 3:
    return None

  cur = conn.cursor()
  cur.execute(
    'SELECT name_zh, tax_id FROM factories WHERE name_zh LIKE ? LIMIT 1',
    (f'%{kw}%',),
  )
  row = cur.fetchone()
  if row:
    return (row[0], row[1])
  return None


# ---------------------------------------------------------------------------
# 主比對流程
# ---------------------------------------------------------------------------

def match_all(
  raw_records: list[dict],
  conn: sqlite3.Connection,
) -> tuple[list[dict], list[dict]]:
  """
  對所有原始記錄執行四階段比對。

  Returns:
    (matched_records, updated_raw_records)
  """
  normalized_index, keyword_index = load_factories(conn)

  stats = {
    'total': 0,
    'skipped_junk': 0,
    'exact': 0,
    'contains': 0,
    'keyword': 0,
    'fts_like': 0,
    'unmatched': 0,
  }

  matched_records: list[dict] = []
  updated_raw: list[dict] = []

  for record in raw_records:
    rec = dict(record)  # shallow copy
    supplier_name = rec.get('supplier_name', '') or ''
    stats['total'] += 1

    norm = normalize(supplier_name)

    if is_junk_name(norm):
      stats['skipped_junk'] += 1
      updated_raw.append(rec)
      continue

    result = None
    match_type = None

    # 階段 1: exact
    result = match_exact(norm, normalized_index)
    if result:
      match_type = 'exact'

    # 階段 2: contains
    if not result:
      result = match_contains(norm, normalized_index)
      if result:
        match_type = 'contains'

    # 階段 3: keyword
    if not result:
      result = match_keyword(norm, keyword_index)
      if result:
        match_type = 'keyword'

    # 階段 4: fts_like（最模糊，只在前三階段均失敗時執行）
    if not result:
      result = match_fts_like(norm, conn)
      if result:
        match_type = 'fts_like'

    if result:
      matched_name, matched_tax_id = result
      stats[match_type] += 1

      # 更新原始記錄的 supplier_tax_id
      rec['supplier_tax_id'] = matched_tax_id

      # 產生 matched 記錄（含比對詮釋資料）
      matched_rec = dict(rec)
      matched_rec['matched_tax_id'] = matched_tax_id
      matched_rec['matched_name'] = matched_name
      matched_rec['match_type'] = match_type
      matched_records.append(matched_rec)
    else:
      stats['unmatched'] += 1

    updated_raw.append(rec)

  # 輸出統計
  valid_total = stats['total'] - stats['skipped_junk']
  matched_count = stats['exact'] + stats['contains'] + stats['keyword'] + stats['fts_like']
  match_rate = (matched_count / valid_total * 100) if valid_total > 0 else 0.0

  log.info('=== 比對統計 ===')
  log.info('總記錄數: %d', stats['total'])
  log.info('跳過雜訊: %d', stats['skipped_junk'])
  log.info('有效比對目標: %d', valid_total)
  log.info('  exact    命中: %d', stats['exact'])
  log.info('  contains 命中: %d', stats['contains'])
  log.info('  keyword  命中: %d', stats['keyword'])
  log.info('  fts_like 命中: %d', stats['fts_like'])
  log.info('未比對: %d', stats['unmatched'])
  log.info('比對率: %.1f%%  (%d / %d)', match_rate, matched_count, valid_total)

  return matched_records, updated_raw


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> None:
  # 自動解壓 tmdb.db.gz（若 .db 不存在）
  if not DB_PATH.exists():
    if DB_GZ_PATH.exists():
      log.info('解壓 %s ...', DB_GZ_PATH)
      with gzip.open(str(DB_GZ_PATH), 'rb') as f_in, open(str(DB_PATH), 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
      log.info('解壓完成：%s', DB_PATH)
    else:
      log.error('找不到資料庫：%s 或 %s', DB_PATH, DB_GZ_PATH)
      raise FileNotFoundError(f'Database not found: {DB_PATH}')

  # 讀取原始供應鏈資料
  if not RAW_PATH.exists():
    log.error('找不到輸入檔：%s', RAW_PATH)
    raise FileNotFoundError(f'Input file not found: {RAW_PATH}')

  log.info('讀取 %s ...', RAW_PATH)
  with open(str(RAW_PATH), encoding='utf-8') as f:
    raw_records: list[dict] = json.load(f)
  log.info('讀取 %d 筆供應商記錄', len(raw_records))

  # 執行比對
  conn = sqlite3.connect(str(DB_PATH))
  conn.execute('PRAGMA journal_mode=WAL')
  try:
    matched_records, updated_raw = match_all(raw_records, conn)
  finally:
    conn.close()

  # 寫出 supply_chain_matched.json
  log.info('寫出 %s （%d 筆）...', MATCHED_PATH, len(matched_records))
  with open(str(MATCHED_PATH), 'w', encoding='utf-8') as f:
    json.dump(matched_records, f, ensure_ascii=False, indent=2)

  # 回寫 supply_chain_raw.json（更新 supplier_tax_id）
  log.info('更新 %s ...', RAW_PATH)
  with open(str(RAW_PATH), 'w', encoding='utf-8') as f:
    json.dump(updated_raw, f, ensure_ascii=False, indent=2)

  log.info('完成。')


if __name__ == '__main__':
  main()
