"""
migrate_schema.py — 升級 tmdb.db schema，新增供應鏈、專利、政府紀錄等資料表

執行方式（在 src/ 目錄下）：
  python scripts/migrate_schema.py

功能：
  - 連接現有 tmdb.db
  - 新增 supply_chain_links、patents、government_records、
    tech_tags、company_tech_tags、crawl_jobs 表（IF NOT EXISTS）
  - 建立外鍵欄位索引
  - 建立 supply_chain_links_fts、patents_fts FTS5 虛擬表
  - 不修改任何現有表或欄位
"""

import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# DDL — 新增表
# ---------------------------------------------------------------------------

NEW_TABLES_DDL = """
-- 供應鏈關係表
CREATE TABLE IF NOT EXISTS supply_chain_links (
  id                INTEGER PRIMARY KEY,
  buyer_tax_id      TEXT,
  buyer_name        TEXT,
  supplier_tax_id   TEXT,
  supplier_name     TEXT,
  relationship_type TEXT,
  source            TEXT,
  source_year       INTEGER,
  purchase_amount   REAL,
  purchase_ratio    REAL,
  created_at        TEXT,
  updated_at        TEXT
);

-- 專利資料表
CREATE TABLE IF NOT EXISTS patents (
  id                 INTEGER PRIMARY KEY,
  patent_number      TEXT UNIQUE,
  application_number TEXT,
  title_zh           TEXT,
  title_en           TEXT,
  applicant_name     TEXT,
  applicant_tax_id   TEXT,
  tech_category      TEXT,
  abstract_zh        TEXT,
  abstract_en        TEXT,
  publication_date   TEXT,
  application_date   TEXT,
  created_at         TEXT
);

-- 政府紀錄表
CREATE TABLE IF NOT EXISTS government_records (
  id              INTEGER PRIMARY KEY,
  company_tax_id  TEXT,
  company_name    TEXT,
  record_type     TEXT,
  program_name    TEXT,
  program_name_en TEXT,
  issuing_agency  TEXT,
  year            INTEGER,
  details         TEXT,
  subsidy_amount  INTEGER,
  created_at      TEXT
);

-- 技術標籤表
CREATE TABLE IF NOT EXISTS tech_tags (
  id       INTEGER PRIMARY KEY,
  tag_zh   TEXT,
  tag_en   TEXT,
  category TEXT
);

-- 公司技術標籤多對多關聯表
CREATE TABLE IF NOT EXISTS company_tech_tags (
  company_tax_id TEXT,
  tech_tag_id    INTEGER,
  source         TEXT,
  confidence     REAL,
  PRIMARY KEY (company_tax_id, tech_tag_id)
);

-- 爬蟲任務紀錄表
CREATE TABLE IF NOT EXISTS crawl_jobs (
  id                 INTEGER PRIMARY KEY,
  source             TEXT,
  status             TEXT,
  started_at         TEXT,
  completed_at       TEXT,
  records_processed  INTEGER,
  records_created    INTEGER,
  records_updated    INTEGER,
  error_message      TEXT
);
"""

# ---------------------------------------------------------------------------
# DDL — 索引
# ---------------------------------------------------------------------------

INDEXES_DDL = """
-- supply_chain_links 索引
CREATE INDEX IF NOT EXISTS idx_scl_buyer_tax_id
  ON supply_chain_links (buyer_tax_id);

CREATE INDEX IF NOT EXISTS idx_scl_supplier_tax_id
  ON supply_chain_links (supplier_tax_id);

CREATE INDEX IF NOT EXISTS idx_scl_source_year
  ON supply_chain_links (source_year);

-- patents 索引
CREATE INDEX IF NOT EXISTS idx_patents_applicant_tax_id
  ON patents (applicant_tax_id);

CREATE INDEX IF NOT EXISTS idx_patents_tech_category
  ON patents (tech_category);

CREATE INDEX IF NOT EXISTS idx_patents_application_date
  ON patents (application_date);

-- government_records 索引
CREATE INDEX IF NOT EXISTS idx_gov_records_company_tax_id
  ON government_records (company_tax_id);

CREATE INDEX IF NOT EXISTS idx_gov_records_record_type
  ON government_records (record_type);

CREATE INDEX IF NOT EXISTS idx_gov_records_year
  ON government_records (year);

-- company_tech_tags 索引
CREATE INDEX IF NOT EXISTS idx_ctt_tech_tag_id
  ON company_tech_tags (tech_tag_id);

-- crawl_jobs 索引
CREATE INDEX IF NOT EXISTS idx_crawl_jobs_source_status
  ON crawl_jobs (source, status);
"""

# ---------------------------------------------------------------------------
# DDL — FTS5 虛擬表
# ---------------------------------------------------------------------------

FTS5_DDL = """
-- supply_chain_links FTS5（對買方/供應商名稱建立全文索引）
CREATE VIRTUAL TABLE IF NOT EXISTS supply_chain_links_fts
  USING fts5(
    buyer_name,
    supplier_name,
    content='supply_chain_links',
    content_rowid='id'
  );

-- patents FTS5（對標題與摘要建立全文索引）
CREATE VIRTUAL TABLE IF NOT EXISTS patents_fts
  USING fts5(
    title_zh,
    title_en,
    abstract_zh,
    abstract_en,
    applicant_name,
    content='patents',
    content_rowid='id'
  );
"""

# ---------------------------------------------------------------------------
# 執行 migration
# ---------------------------------------------------------------------------

def run_migration(db_path: Path) -> None:
  if not db_path.exists():
    print(f'[ERROR] 找不到資料庫：{db_path}')
    sys.exit(1)

  print(f'[INFO] 連接資料庫：{db_path}')
  conn = sqlite3.connect(str(db_path))
  conn.execute('PRAGMA journal_mode=WAL')
  conn.execute('PRAGMA foreign_keys=ON')

  try:
    cur = conn.cursor()

    print('[INFO] 新增資料表...')
    cur.executescript(NEW_TABLES_DDL)

    print('[INFO] 建立索引...')
    cur.executescript(INDEXES_DDL)

    print('[INFO] 建立 FTS5 虛擬表...')
    cur.executescript(FTS5_DDL)

    # 補欄位（對已存在的 government_records 表，靜默跳過重複）
    try:
      cur.execute('ALTER TABLE government_records ADD COLUMN subsidy_amount INTEGER DEFAULT NULL')
      print('[INFO] 新增 government_records.subsidy_amount 欄位')
    except sqlite3.OperationalError as e:
      if 'duplicate column name' in str(e).lower():
        print('[INFO] government_records.subsidy_amount 欄位已存在，略過')
      else:
        raise

    conn.commit()
    print('[OK] Migration 完成')

    # 驗證：列出所有表
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    print(f'[INFO] 目前資料庫中的表：{tables}')

  except sqlite3.Error as e:
    conn.rollback()
    print(f'[ERROR] Migration 失敗：{e}')
    sys.exit(1)
  finally:
    conn.close()


if __name__ == '__main__':
  run_migration(DB_PATH)
