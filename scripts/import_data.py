"""
import_data.py — 將爬蟲產出的 JSON 資料匯入 tmdb.db

執行方式（在 src/ 目錄下）：
  python scripts/import_data.py

功能：
  - 自動確認新表已建立（呼叫 migrate_schema）
  - 匯入 supply_chain_matched.json（優先）或 supply_chain_raw.json
    → INSERT OR IGNORE INTO supply_chain_links
  - 匯入 government_records_raw.json
    → INSERT OR IGNORE INTO government_records
  - 匯入 patents_raw.json（若存在）
    → INSERT OR IGNORE INTO patents
  - 每次匯入在 crawl_jobs 表記錄一筆
  - 批次寫入（每 500 筆 commit 一次）
  - 完成後顯示各表新增筆數統計
"""

import gzip
import json
import logging
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DATA_DIR = SRC_DIR / 'data'
DB_PATH = DATA_DIR / 'tmdb.db'
DB_GZ_PATH = DATA_DIR / 'tmdb.db.gz'

SUPPLY_CHAIN_MATCHED_PATH = DATA_DIR / 'supply_chain_matched.json'
SUPPLY_CHAIN_RAW_PATH = DATA_DIR / 'supply_chain_raw.json'
GOVERNMENT_RECORDS_PATH = DATA_DIR / 'government_records_raw.json'
PATENTS_PATH = DATA_DIR / 'patents_raw.json'

BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# Logging 設定
# ---------------------------------------------------------------------------

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def now_iso() -> str:
  return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def load_json(path: Path) -> Optional[list[dict[str, Any]]]:
  if not path.exists():
    return None
  try:
    with open(path, encoding='utf-8') as f:
      data = json.load(f)
    # 支援 list 或 {"patents": [...]} / {"records": [...]} 等 dict 包裝格式
    if isinstance(data, dict):
      for key in ('patents', 'records', 'data', 'results'):
        if key in data and isinstance(data[key], list):
          data = data[key]
          break
    if not isinstance(data, list):
      logger.warning(f'[SKIP] {path.name} 不是 list 格式，跳過')
      return None
    logger.info(f'[LOAD] {path.name}：{len(data)} 筆')
    return data
  except json.JSONDecodeError as e:
    logger.error(f'[ERROR] 無法解析 {path.name}：{e}')
    return None


def ensure_db() -> None:
  """確認 DB 存在（必要時從 .gz 解壓），並執行 migrate_schema。"""
  if not DB_PATH.exists():
    if DB_GZ_PATH.exists():
      logger.info(f'[DB] 找到 {DB_GZ_PATH.name}，開始解壓...')
      with gzip.open(DB_GZ_PATH, 'rb') as f_in, open(DB_PATH, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
      logger.info(f'[DB] 解壓完成：{DB_PATH}')
    else:
      logger.error(f'[ERROR] 找不到 tmdb.db 或 tmdb.db.gz')
      sys.exit(1)

  # 執行 migrate_schema 確保新表存在
  migrate_script = SCRIPT_DIR / 'migrate_schema.py'
  if migrate_script.exists():
    logger.info('[DB] 執行 migrate_schema.py 確保新表存在...')
    result = subprocess.run(
      [sys.executable, str(migrate_script)],
      capture_output=True,
      text=True,
    )
    if result.returncode != 0:
      logger.error(f'[ERROR] migrate_schema 失敗：{result.stderr}')
      sys.exit(1)
    logger.info('[DB] migrate_schema 完成')
  else:
    logger.warning(f'[WARN] 找不到 migrate_schema.py，跳過 migration')


def get_conn() -> sqlite3.Connection:
  conn = sqlite3.connect(str(DB_PATH))
  conn.execute('PRAGMA journal_mode=WAL')
  conn.execute('PRAGMA foreign_keys=ON')
  conn.row_factory = sqlite3.Row
  return conn


def table_count(conn: sqlite3.Connection, table: str) -> int:
  row = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
  return row[0]


def record_crawl_job(
  conn: sqlite3.Connection,
  source: str,
  status: str,
  started_at: str,
  completed_at: str,
  records_processed: int,
  records_created: int,
  records_updated: int = 0,
  error_message: Optional[str] = None,
) -> None:
  conn.execute(
    '''
    INSERT INTO crawl_jobs
      (source, status, started_at, completed_at,
       records_processed, records_created, records_updated, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''',
    (source, status, started_at, completed_at,
     records_processed, records_created, records_updated, error_message),
  )
  conn.commit()

# ---------------------------------------------------------------------------
# 匯入：supply_chain_links
# ---------------------------------------------------------------------------

def import_supply_chain(conn: sqlite3.Connection) -> int:
  """
  優先讀取 supply_chain_matched.json，回退 supply_chain_raw.json。
  回傳新增筆數。
  """
  source_label = 'import_supply_chain_links'
  started_at = now_iso()

  records = load_json(SUPPLY_CHAIN_MATCHED_PATH)
  source_file = SUPPLY_CHAIN_MATCHED_PATH.name
  if records is None:
    records = load_json(SUPPLY_CHAIN_RAW_PATH)
    source_file = SUPPLY_CHAIN_RAW_PATH.name

  if records is None:
    logger.warning('[SKIP] 找不到供應鏈 JSON 檔案，跳過匯入')
    record_crawl_job(
      conn, source_label, 'skipped',
      started_at, now_iso(), 0, 0,
      error_message='source file not found',
    )
    return 0

  count_before = table_count(conn, 'supply_chain_links')
  processed = 0
  errors = 0

  for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i + BATCH_SIZE]
    rows = []
    for rec in batch:
      try:
        # matched_tax_id 優先作為 supplier_tax_id
        supplier_tax_id = (
          rec.get('matched_tax_id')
          or rec.get('supplier_tax_id')
        )
        # buyer_tax_id 目前 JSON 無此欄，留 None
        buyer_tax_id = rec.get('buyer_tax_id')

        rows.append((
          buyer_tax_id,                        # buyer_tax_id
          rec.get('buyer_name'),               # buyer_name
          supplier_tax_id,                     # supplier_tax_id
          rec.get('supplier_name'),            # supplier_name
          rec.get('relationship_type'),        # relationship_type
          rec.get('source', source_file),      # source
          rec.get('source_year'),              # source_year
          rec.get('purchase_amount'),          # purchase_amount
          rec.get('purchase_ratio'),           # purchase_ratio
          now_iso(),                           # created_at
          now_iso(),                           # updated_at
        ))
      except Exception as e:
        logger.warning(f'[WARN] supply_chain_links 第 {i} 筆解析失敗：{e}')
        errors += 1

    conn.executemany(
      '''
      INSERT OR IGNORE INTO supply_chain_links
        (buyer_tax_id, buyer_name, supplier_tax_id, supplier_name,
         relationship_type, source, source_year,
         purchase_amount, purchase_ratio, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      rows,
    )
    conn.commit()
    processed += len(batch)
    logger.info(f'[supply_chain_links] 處理中：{processed}/{len(records)}')

  count_after = table_count(conn, 'supply_chain_links')
  created = count_after - count_before
  logger.info(f'[supply_chain_links] 新增：{created} 筆（錯誤：{errors} 筆）')

  record_crawl_job(
    conn, source_label, 'completed',
    started_at, now_iso(),
    records_processed=processed,
    records_created=created,
    error_message=f'{errors} parse errors' if errors else None,
  )
  return created

# ---------------------------------------------------------------------------
# 匯入：government_records
# ---------------------------------------------------------------------------

def import_government_records(conn: sqlite3.Connection) -> int:
  """回傳新增筆數。"""
  source_label = 'import_government_records'
  started_at = now_iso()

  records = load_json(GOVERNMENT_RECORDS_PATH)
  if records is None:
    logger.warning('[SKIP] 找不到 government_records_raw.json，跳過匯入')
    record_crawl_job(
      conn, source_label, 'skipped',
      started_at, now_iso(), 0, 0,
      error_message='source file not found',
    )
    return 0

  count_before = table_count(conn, 'government_records')
  processed = 0
  errors = 0

  for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i + BATCH_SIZE]
    rows = []
    for rec in batch:
      try:
        # 明確欄位（不放入 details JSON）
        _top_level_keys = {
          'company_tax_id', 'company_name', 'record_type',
          'program_name', 'program_name_en', 'issuing_agency',
          'year', 'subsidy_amount',
        }
        extra = {k: v for k, v in rec.items() if k not in _top_level_keys}
        details_str = json.dumps(extra, ensure_ascii=False) if extra else None

        rows.append((
          rec.get('company_tax_id'),           # company_tax_id
          rec.get('company_name'),             # company_name
          rec.get('record_type'),              # record_type
          rec.get('program_name'),             # program_name
          rec.get('program_name_en'),          # program_name_en
          rec.get('issuing_agency'),           # issuing_agency
          rec.get('year'),                     # year
          details_str,                         # details（其餘欄位序列化）
          rec.get('subsidy_amount'),           # subsidy_amount
          now_iso(),                           # created_at
        ))
      except Exception as e:
        logger.warning(f'[WARN] government_records 第 {i} 筆解析失敗：{e}')
        errors += 1

    conn.executemany(
      '''
      INSERT OR IGNORE INTO government_records
        (company_tax_id, company_name, record_type,
         program_name, program_name_en, issuing_agency,
         year, details, subsidy_amount, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      rows,
    )
    conn.commit()
    processed += len(batch)
    logger.info(f'[government_records] 處理中：{processed}/{len(records)}')

  count_after = table_count(conn, 'government_records')
  created = count_after - count_before
  logger.info(f'[government_records] 新增：{created} 筆（錯誤：{errors} 筆）')

  record_crawl_job(
    conn, source_label, 'completed',
    started_at, now_iso(),
    records_processed=processed,
    records_created=created,
    error_message=f'{errors} parse errors' if errors else None,
  )
  return created

# ---------------------------------------------------------------------------
# 匯入：patents
# ---------------------------------------------------------------------------

def import_patents(conn: sqlite3.Connection) -> int:
  """若 patents_raw.json 不存在則跳過。回傳新增筆數。"""
  source_label = 'import_patents'
  started_at = now_iso()

  records = load_json(PATENTS_PATH)
  if records is None:
    logger.info('[SKIP] patents_raw.json 不存在，跳過專利匯入')
    return 0

  count_before = table_count(conn, 'patents')
  processed = 0
  errors = 0

  for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i + BATCH_SIZE]
    rows = []
    for rec in batch:
      try:
        rows.append((
          rec.get('patent_number'),            # patent_number (UNIQUE)
          rec.get('application_number'),       # application_number
          rec.get('title_zh'),                 # title_zh
          rec.get('title_en'),                 # title_en
          rec.get('applicant_name'),           # applicant_name
          rec.get('applicant_tax_id'),         # applicant_tax_id
          rec.get('tech_category'),            # tech_category
          rec.get('abstract_zh'),              # abstract_zh
          rec.get('abstract_en'),              # abstract_en
          rec.get('publication_date'),         # publication_date
          rec.get('application_date'),         # application_date
          now_iso(),                           # created_at
        ))
      except Exception as e:
        logger.warning(f'[WARN] patents 第 {i} 筆解析失敗：{e}')
        errors += 1

    conn.executemany(
      '''
      INSERT OR IGNORE INTO patents
        (patent_number, application_number,
         title_zh, title_en,
         applicant_name, applicant_tax_id,
         tech_category, abstract_zh, abstract_en,
         publication_date, application_date, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      rows,
    )
    conn.commit()
    processed += len(batch)
    logger.info(f'[patents] 處理中：{processed}/{len(records)}')

  count_after = table_count(conn, 'patents')
  created = count_after - count_before
  logger.info(f'[patents] 新增：{created} 筆（錯誤：{errors} 筆）')

  record_crawl_job(
    conn, source_label, 'completed',
    started_at, now_iso(),
    records_processed=processed,
    records_created=created,
    error_message=f'{errors} parse errors' if errors else None,
  )
  return created

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
  logger.info('=' * 60)
  logger.info('[START] import_data.py 開始執行')
  logger.info('=' * 60)

  # Step 1：確認 DB 存在且 schema 已 migrate
  ensure_db()

  conn = get_conn()
  try:
    # Step 2：各表匯入
    sc_created = import_supply_chain(conn)
    gov_created = import_government_records(conn)
    pat_created = import_patents(conn)

    # Step 3：統計報告
    logger.info('=' * 60)
    logger.info('[SUMMARY] 匯入完成統計：')
    logger.info(f'  supply_chain_links  新增：{sc_created:>6} 筆')
    logger.info(f'  government_records  新增：{gov_created:>6} 筆')
    logger.info(f'  patents             新增：{pat_created:>6} 筆')
    logger.info('-' * 60)
    logger.info(f'  supply_chain_links  累計：{table_count(conn, "supply_chain_links"):>6} 筆')
    logger.info(f'  government_records  累計：{table_count(conn, "government_records"):>6} 筆')
    logger.info(f'  patents             累計：{table_count(conn, "patents"):>6} 筆')
    logger.info(f'  crawl_jobs          累計：{table_count(conn, "crawl_jobs"):>6} 筆')
    logger.info('=' * 60)
    logger.info('[DONE] 所有資料匯入完成')

  except sqlite3.Error as e:
    logger.error(f'[ERROR] 資料庫操作失敗：{e}')
    conn.rollback()
    sys.exit(1)
  finally:
    conn.close()


if __name__ == '__main__':
  main()
