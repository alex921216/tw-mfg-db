"""
main.py — FastAPI 搜尋 API for Taiwan Manufacturing Database

Endpoints:
  GET /api/search                          — 全文搜尋 + 篩選 + 分頁（含 hidden_champion_score）
  GET /api/export                          — 匯出 CSV
  GET /api/filters                         — 可用篩選選項
  GET /api/stats                           — 資料庫統計（含新表計數）
  GET /api/supply-chain?buyer=台積電       — TSMC 供應商列表（supply_chain 表）
  GET /api/supply-chain/list               — 所有有供應鏈資料的買方
  GET /api/supply-chain-links?company=...  — 財報來源供應鏈查詢（supply_chain_links 表）
  GET /api/supply-chain/graph?company=...  — 供應鏈關係圖（supply_chain_links 表，圖表用）
  GET /api/supply-chain/buyers             — 所有有供應鏈資料的買方（supply_chain_links 表）
  GET /api/factory/{id}/supply-chain-tags  — 工廠的買方標籤
  GET /api/patents                         — 專利資料查詢
  GET /api/government-records              — 政府紀錄查詢
  GET /api/company/{tax_id}               — 公司完整 profile
  GET /api/hidden-champions               — 隱形冠軍清單（按分數降序）
  GET /api/suggest?q=semi&limit=8        — 搜尋框自動完成建議

啟動：在 src/ 下執行
  uvicorn app.main:app --reload --port 8000
"""

import csv
import io
import os
import secrets
import sqlite3
import time
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from datetime import date
from pathlib import Path
from typing import Generator, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import APIKeyHeader, APIKeyQuery
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

APP_DIR = Path(__file__).resolve().parent
SRC_DIR = APP_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
DB_GZ_PATH = SRC_DIR / 'data' / 'tmdb.db.gz'

# 自動解壓：部署環境只帶 .gz，啟動時解壓
if not DB_PATH.exists() and DB_GZ_PATH.exists():
    import gzip, shutil
    with gzip.open(DB_GZ_PATH, 'rb') as f_in, open(DB_PATH, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# ---------------------------------------------------------------------------
# 資料庫 Schema 初始化
# ---------------------------------------------------------------------------

_HIDDEN_CHAMPION_COLUMNS_DDL = """
ALTER TABLE factories ADD COLUMN hidden_champion_score INTEGER DEFAULT 0;
ALTER TABLE factories ADD COLUMN hidden_champion_reasons TEXT DEFAULT NULL;
ALTER TABLE factories ADD COLUMN hidden_champion_updated_at TEXT DEFAULT NULL;
"""

_API_AUTH_DDL = """
CREATE TABLE IF NOT EXISTS api_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  email TEXT,
  tier TEXT DEFAULT 'free',
  rate_limit_per_minute INTEGER DEFAULT 30,
  rate_limit_per_day INTEGER DEFAULT 1000,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  expires_at TEXT
);

CREATE TABLE IF NOT EXISTS api_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  api_key_id INTEGER NOT NULL,
  endpoint TEXT NOT NULL,
  method TEXT DEFAULT 'GET',
  status_code INTEGER,
  response_time_ms INTEGER,
  ip_address TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
);

CREATE INDEX IF NOT EXISTS idx_api_usage_key_date ON api_usage(api_key_id, created_at);
CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);
"""

_NEW_TABLES_DDL = """
CREATE TABLE IF NOT EXISTS supply_chain (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  buyer_name           TEXT NOT NULL,
  buyer_stock_id       TEXT,
  supplier_name_zh     TEXT NOT NULL,
  supplier_stock_id    TEXT,
  supplier_factory_id  INTEGER,
  category             TEXT,
  product              TEXT,
  source               TEXT,
  confidence           TEXT,
  created_at           TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_supply_chain_buyer
  ON supply_chain (buyer_name);

CREATE INDEX IF NOT EXISTS idx_supply_chain_factory
  ON supply_chain (supplier_factory_id);

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
  created_at      TEXT
);

CREATE TABLE IF NOT EXISTS tech_tags (
  id       INTEGER PRIMARY KEY,
  tag_zh   TEXT,
  tag_en   TEXT,
  category TEXT
);

CREATE TABLE IF NOT EXISTS company_tech_tags (
  company_tax_id TEXT,
  tech_tag_id    INTEGER,
  source         TEXT,
  confidence     REAL,
  PRIMARY KEY (company_tax_id, tech_tag_id)
);

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

CREATE INDEX IF NOT EXISTS idx_scl_buyer_tax_id
  ON supply_chain_links (buyer_tax_id);
CREATE INDEX IF NOT EXISTS idx_scl_supplier_tax_id
  ON supply_chain_links (supplier_tax_id);
CREATE INDEX IF NOT EXISTS idx_scl_source_year
  ON supply_chain_links (source_year);

CREATE INDEX IF NOT EXISTS idx_patents_applicant_tax_id
  ON patents (applicant_tax_id);
CREATE INDEX IF NOT EXISTS idx_patents_tech_category
  ON patents (tech_category);
CREATE INDEX IF NOT EXISTS idx_patents_application_date
  ON patents (application_date);

CREATE INDEX IF NOT EXISTS idx_gov_records_company_tax_id
  ON government_records (company_tax_id);
CREATE INDEX IF NOT EXISTS idx_gov_records_record_type
  ON government_records (record_type);
CREATE INDEX IF NOT EXISTS idx_gov_records_year
  ON government_records (year);

CREATE INDEX IF NOT EXISTS idx_ctt_tech_tag_id
  ON company_tech_tags (tech_tag_id);

CREATE INDEX IF NOT EXISTS idx_crawl_jobs_source_status
  ON crawl_jobs (source, status);

CREATE VIRTUAL TABLE IF NOT EXISTS supply_chain_links_fts
  USING fts5(
    buyer_name,
    supplier_name,
    content='supply_chain_links',
    content_rowid='id'
  );

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


def _migrate_columns(conn: sqlite3.Connection, statements: list[str]) -> None:
  """執行一組 ALTER TABLE 語句，已存在的欄位靜默跳過。"""
  for stmt in statements:
    try:
      conn.execute(stmt)
    except Exception as e:
      if 'duplicate column name' not in str(e).lower():
        raise


def _migrate_hidden_champion_columns(conn: sqlite3.Connection) -> None:
  """在 factories 表新增 hidden champion 欄位（已存在則靜默跳過）。"""
  _migrate_columns(conn, [
    'ALTER TABLE factories ADD COLUMN hidden_champion_score INTEGER DEFAULT 0',
    'ALTER TABLE factories ADD COLUMN hidden_champion_reasons TEXT DEFAULT NULL',
    'ALTER TABLE factories ADD COLUMN hidden_champion_updated_at TEXT DEFAULT NULL',
  ])


def _migrate_listed_company_columns(conn: sqlite3.Connection) -> None:
  """在 factories 表新增上市櫃聯絡資訊欄位（已存在則靜默跳過）。"""
  _migrate_columns(conn, [
    'ALTER TABLE factories ADD COLUMN phone TEXT',
    'ALTER TABLE factories ADD COLUMN email TEXT',
    'ALTER TABLE factories ADD COLUMN website TEXT',
    'ALTER TABLE factories ADD COLUMN fax TEXT',
    'ALTER TABLE factories ADD COLUMN english_address TEXT',
    'ALTER TABLE factories ADD COLUMN stock_id TEXT',
    'ALTER TABLE factories ADD COLUMN official_name_en TEXT',
    'ALTER TABLE factories ADD COLUMN is_listed INTEGER DEFAULT 0',
  ])


def _migrate_moea_extended_columns(conn: sqlite3.Connection) -> None:
  """在 factories 表新增 MOEA 資料的延伸欄位（已存在則靜默跳過）。"""
  _migrate_columns(conn, [
    'ALTER TABLE factories ADD COLUMN capital_amount INTEGER',
    'ALTER TABLE factories ADD COLUMN paid_in_capital INTEGER',
    'ALTER TABLE factories ADD COLUMN company_setup_date TEXT',
    'ALTER TABLE factories ADD COLUMN findbiz_url TEXT',
    'ALTER TABLE factories ADD COLUMN products_en TEXT',
    'ALTER TABLE factories ADD COLUMN products_zh TEXT',
    'ALTER TABLE factories ADD COLUMN certifications_en TEXT',
    'ALTER TABLE factories ADD COLUMN registered_address TEXT',
  ])


def init_db() -> None:
  """初始化資料庫 schema（向下相容）。新表使用 IF NOT EXISTS，不修改既有表。"""
  if not DB_PATH.exists():
    return
  conn = sqlite3.connect(str(DB_PATH))
  conn.execute('PRAGMA journal_mode=WAL')
  try:
    conn.executescript(_API_AUTH_DDL)
    conn.executescript(_NEW_TABLES_DDL)
    _migrate_hidden_champion_columns(conn)
    _migrate_listed_company_columns(conn)
    _migrate_moea_extended_columns(conn)
    # 建立預設 demo key（前端用）
    conn.execute(
      """
      INSERT OR IGNORE INTO api_keys (key, name, tier, rate_limit_per_minute, rate_limit_per_day)
      VALUES ('tmdb-demo-2026', 'Demo (Frontend)', 'free', 30, 1000)
      """
    )
    conn.commit()
    # 更新 FTS5 統計資料，讓查詢規劃器在大資料量下做出最佳決策
    conn.execute('PRAGMA optimize')
  finally:
    conn.close()


# ---------------------------------------------------------------------------
# FastAPI 應用初始化
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
  init_db()
  yield


app = FastAPI(
  title='Taiwan Manufacturing Database API',
  description='搜尋台灣製造業工廠資料',
  version='1.0.0',
  lifespan=lifespan,
)

app.add_middleware(
  CORSMiddleware,
  allow_origins=['*'],
  allow_credentials=True,
  allow_methods=['*'],
  allow_headers=['*'],
)


# ---------------------------------------------------------------------------
# 使用量追蹤 Middleware（異步記錄 /api/* 請求）
# ---------------------------------------------------------------------------

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse


class ApiUsageMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed_ms = int((time.time() - start) * 1000)

    # 只記錄 /api/* 路徑（排除 admin 管理 endpoints 自身的記錄，避免遞歸複雜度）
    path = request.url.path
    if path.startswith('/api/') and not path.startswith('/api/admin/'):
      key_info = getattr(request.state, 'api_key_info', None)
      if key_info:
        ip = request.client.host if request.client else ''
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_in_executor(
          None,
          _log_usage_sync,
          key_info['id'],
          path,
          request.method,
          response.status_code,
          elapsed_ms,
          ip,
        )

    return response


app.add_middleware(ApiUsageMiddleware)

# ---------------------------------------------------------------------------
# API Key 認證 & Rate Limiting
# ---------------------------------------------------------------------------

TMDB_ADMIN_KEY = os.environ.get('TMDB_ADMIN_KEY', '')

_api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)
_api_key_query = APIKeyQuery(name='api_key', auto_error=False)

# In-memory rate limit store: {str(api_key_id): [timestamp, ...]}
_rate_limits: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(api_key_id: int, per_minute: int, per_day: int) -> None:
  now = time.time()
  key = str(api_key_id)
  # 只保留最近 24 小時的記錄
  _rate_limits[key] = [t for t in _rate_limits[key] if now - t < 86400]
  recent_minute = sum(1 for t in _rate_limits[key] if now - t < 60)
  if recent_minute >= per_minute:
    raise HTTPException(status_code=429, detail='Rate limit exceeded (per minute)')
  if len(_rate_limits[key]) >= per_day:
    raise HTTPException(status_code=429, detail='Rate limit exceeded (per day)')
  _rate_limits[key].append(now)


async def verify_api_key(
  request: Request,
  header_key: str = Depends(_api_key_header),
  query_key: str = Depends(_api_key_query),
) -> dict:
  """驗證 API key 並回傳 key 記錄 dict（含 id, rate_limit_per_minute, rate_limit_per_day）。
  同時將 key info 存到 request.state 供 middleware 使用。"""
  key = header_key or query_key
  if not key:
    raise HTTPException(
      status_code=401,
      detail={'error': 'API key required. Pass via X-API-Key header or api_key query parameter.'},
    )
  if not DB_PATH.exists():
    raise HTTPException(status_code=500, detail='Database file not found')
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  try:
    cur = conn.cursor()
    cur.execute(
      """
      SELECT id, key, name, tier, rate_limit_per_minute, rate_limit_per_day, is_active, expires_at
      FROM api_keys
      WHERE key = ?
      """,
      (key,),
    )
    row = cur.fetchone()
  finally:
    conn.close()

  if not row:
    raise HTTPException(status_code=401, detail={'error': 'Invalid API key'})
  if not row['is_active']:
    raise HTTPException(status_code=403, detail={'error': 'API key is inactive'})
  if row['expires_at']:
    from datetime import datetime
    if datetime.utcnow().isoformat() > row['expires_at']:
      raise HTTPException(status_code=403, detail={'error': 'API key has expired'})

  _check_rate_limit(row['id'], row['rate_limit_per_minute'], row['rate_limit_per_day'])

  key_info = dict(row)
  request.state.api_key_info = key_info
  return key_info


async def verify_admin_key(
  header_key: str = Depends(_api_key_header),
  query_key: str = Depends(_api_key_query),
) -> None:
  """驗證管理員 key（從環境變數 TMDB_ADMIN_KEY 讀取）。"""
  key = header_key or query_key
  if not key or not TMDB_ADMIN_KEY:
    raise HTTPException(status_code=401, detail={'error': 'Admin key required'})
  if key != TMDB_ADMIN_KEY:
    raise HTTPException(status_code=403, detail={'error': 'Invalid admin key'})


def _log_usage_sync(
  api_key_id: int,
  endpoint: str,
  method: str,
  status_code: int,
  response_time_ms: int,
  ip_address: str,
) -> None:
  """同步寫入使用量到 DB（由 BackgroundTasks 異步執行）。"""
  if not DB_PATH.exists():
    return
  try:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(
      """
      INSERT INTO api_usage (api_key_id, endpoint, method, status_code, response_time_ms, ip_address)
      VALUES (?, ?, ?, ?, ?, ?)
      """,
      (api_key_id, endpoint, method, status_code, response_time_ms, ip_address),
    )
    conn.commit()
    conn.close()
  except Exception:
    pass  # 使用量記錄失敗不應影響主要請求


# ---------------------------------------------------------------------------
# 資料庫連線
# ---------------------------------------------------------------------------

@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
  """取得 SQLite 連線（row_factory 設為 dict）。"""
  if not DB_PATH.exists():
    raise HTTPException(status_code=500, detail='Database file not found')
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')
  try:
    yield conn
  finally:
    conn.close()

# ---------------------------------------------------------------------------
# 共用：英文搜尋詞轉換為中文公司名稱
# ---------------------------------------------------------------------------

import re as _re

def resolve_company_names(conn: sqlite3.Connection, search_term: str) -> list:
  """將搜尋詞解析為可能的公司名稱列表（中文+英文）。

  當輸入不含任何中文字時，視為英文搜尋詞，從 factories 表查對應的中文名稱，
  以便在 supply_chain_links / patents / government_records 等中文資料表中比對。
  """
  names = [search_term]
  # 判斷是否含中文字（U+4E00 ~ U+9FFF 基本漢字區段）
  if any('\u4e00' <= c <= '\u9fff' for c in search_term):
    return names

  cur = conn.cursor()
  cur.execute(
    """
    SELECT DISTINCT name_zh, name_en
    FROM factories
    WHERE official_name_en LIKE ?
       OR name_en LIKE ?
    LIMIT 20
    """,
    (f'%{search_term}%', f'%{search_term}%'),
  )
  _FACTORY_SUFFIX = _re.compile(
    r'(第[一二三四五六七八九十百\d]+廠|[一二三四五六七八九十百\d]+廠|.{2,3}廠|工廠|總廠|分廠)$'
  )
  for row in cur.fetchall():
    zh = row['name_zh']
    if not zh:
      continue
    # 去掉廠區後綴，取公司本體名稱
    base = _FACTORY_SUFFIX.sub('', zh).strip()
    if base and base not in names:
      names.append(base)
    if zh not in names:
      names.append(zh)
  return names


# ---------------------------------------------------------------------------
# 共用：建立搜尋 SQL
# ---------------------------------------------------------------------------

def build_search_query(
  q: Optional[str],
  industry: Optional[str],
  city: Optional[str],
  select_clause: str = 'f.*',
  order_clause: str = '',
  limit_clause: str = '',
) -> tuple[str, list]:
  """
  依據篩選條件組出 SQL 和參數列表。

  Returns:
    (sql_string, params_list)
  """
  params: list = []

  if q:
    # FTS5 MATCH：前綴搜尋，對每個 token 加 *
    fts_terms = ' '.join(f'{token}*' for token in q.strip().split() if token)
    base_query = f"""
      SELECT {select_clause}
      FROM factories_fts fts
      JOIN factories f ON f.id = fts.rowid
      WHERE fts.factories_fts MATCH ?
    """
    params.append(fts_terms)
    default_order = 'ORDER BY fts.rank'
  else:
    base_query = f"""
      SELECT {select_clause}
      FROM factories f
      WHERE 1=1
    """
    default_order = 'ORDER BY f.id'

  if industry:
    base_query += ' AND f.industry_en = ?'
    params.append(industry)

  if city:
    base_query += ' AND f.city_en = ?'
    params.append(city)

  final_order = order_clause or default_order
  sql = base_query + f' {final_order} {limit_clause}'
  return sql, params

# ---------------------------------------------------------------------------
# GET /api/search
# ---------------------------------------------------------------------------

_SORT_ORDER_MAP: dict[str, str] = {
  'capital_desc': 'ORDER BY f.capital_amount DESC NULLS LAST',
  'capital_asc':  'ORDER BY f.capital_amount ASC NULLS LAST',
}


@app.get(
  '/api/search',
  summary='Search factories',
  description='全文搜尋工廠資料，支援關鍵字、產業、縣市篩選及分頁。',
)
def search_factories(
  q: Optional[str] = Query(default=None, description='全文搜尋關鍵字'),
  industry: Optional[str] = Query(default=None, description='產業類別英文（精確比對）'),
  city: Optional[str] = Query(default=None, description='縣市英文（精確比對）'),
  page: int = Query(default=1, ge=1, description='頁碼（從 1 開始）'),
  page_size: int = Query(default=20, ge=1, le=100, description='每頁數量（最大 100）'),
  sort: Optional[str] = Query(default=None, description='排序：capital_desc | capital_asc'),
  _auth: dict = Depends(verify_api_key),
):
  offset = (page - 1) * page_size

  order_clause = _SORT_ORDER_MAP.get(sort or '', '')

  count_select = 'COUNT(*) AS cnt'
  data_select = (
    'f.id, f.tax_id, f.name_en, f.name_zh, '
    'f.industry_en, f.industry_zh, '
    'f.city_en, f.district_en, '
    'f.address_zh, f.registration_date, '
    'f.capital_amount, f.paid_in_capital, f.company_setup_date, f.findbiz_url, '
    'f.products_en, f.products_zh, f.certifications_en, f.registered_address, '
    'f.hidden_champion_score, '
    'f.phone, f.email, f.website, f.stock_id, f.official_name_en, f.is_listed'
  )

  count_sql, count_params = build_search_query(q, industry, city, select_clause=count_select)
  data_sql, data_params = build_search_query(
    q, industry, city,
    select_clause=data_select,
    order_clause=order_clause,
    limit_clause=f'LIMIT {page_size} OFFSET {offset}',
  )

  try:
    with get_db() as conn:
      cur = conn.cursor()

      cur.execute(count_sql, count_params)
      total = cur.fetchone()['cnt']

      cur.execute(data_sql, data_params)
      rows = cur.fetchall()

  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  results = [
    {
      'id': row['id'],
      'tax_id': row['tax_id'],
      'name_en': row['name_en'],
      'name_zh': row['name_zh'],
      'industry_en': row['industry_en'],
      'industry_zh': row['industry_zh'],
      'city_en': row['city_en'],
      'district_en': row['district_en'],
      'address_zh': row['address_zh'],
      'registration_date': row['registration_date'],
      'capital_amount': row['capital_amount'],
      'paid_in_capital': row['paid_in_capital'],
      'company_setup_date': row['company_setup_date'],
      'findbiz_url': row['findbiz_url'],
      'products_en': row['products_en'],
      'products_zh': row['products_zh'],
      'certifications_en': row['certifications_en'],
      'registered_address': row['registered_address'],
      'hidden_champion_score': row['hidden_champion_score'] or 0,
      'phone': row['phone'],
      'email': row['email'],
      'website': row['website'],
      'stock_id': row['stock_id'],
      'official_name_en': row['official_name_en'],
      'is_listed': row['is_listed'] or 0,
    }
    for row in rows
  ]

  return {
    'total': total,
    'page': page,
    'page_size': page_size,
    'results': results,
  }

# ---------------------------------------------------------------------------
# GET /api/export
# ---------------------------------------------------------------------------

@app.get(
  '/api/export',
  summary='Export search results as CSV',
  description='將搜尋結果匯出為 CSV 檔案下載，支援與 /api/search 相同的篩選條件。',
)
def export_factories(
  q: Optional[str] = Query(default=None, description='全文搜尋關鍵字'),
  industry: Optional[str] = Query(default=None, description='產業類別英文（精確比對）'),
  city: Optional[str] = Query(default=None, description='縣市英文（精確比對）'),
  sort: Optional[str] = Query(default=None, description='排序：capital_desc | capital_asc'),
  _auth: dict = Depends(verify_api_key),
):
  order_clause = _SORT_ORDER_MAP.get(sort or '', '')

  data_select = (
    'f.id, f.tax_id, f.name_en, f.name_zh, '
    'f.industry_en, f.industry_zh, '
    'f.city_en, f.district_en, '
    'f.address_zh, f.registration_date, '
    'f.capital_amount, f.paid_in_capital, f.company_setup_date, f.findbiz_url, '
    'f.products_en, f.products_zh, f.certifications_en, f.registered_address, '
    'f.phone, f.email, f.website, f.stock_id, f.official_name_en, f.is_listed'
  )

  sql, params = build_search_query(
    q, industry, city,
    select_clause=data_select,
    order_clause=order_clause,
  )

  try:
    with get_db() as conn:
      cur = conn.cursor()
      cur.execute(sql, params)
      rows = cur.fetchall()
  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  CSV_HEADERS = [
    'id', 'tax_id', 'name_en', 'name_zh',
    'official_name_en', 'stock_id', 'is_listed',
    'industry_en', 'industry_zh',
    'city_en', 'district_en',
    'address_zh', 'registered_address', 'registration_date',
    'capital_amount', 'paid_in_capital', 'company_setup_date', 'findbiz_url',
    'products_en', 'products_zh', 'certifications_en',
    'phone', 'email', 'website',
  ]

  output = io.StringIO()
  writer = csv.DictWriter(output, fieldnames=CSV_HEADERS)
  writer.writeheader()
  for row in rows:
    writer.writerow({col: row[col] for col in CSV_HEADERS})

  output.seek(0)
  timestamp = date.today().strftime('%Y%m%d')
  filename = f'tmdb_export_{timestamp}.csv'

  return StreamingResponse(
    iter([output.getvalue()]),
    media_type='text/csv',
    headers={'Content-Disposition': f'attachment; filename="{filename}"'},
  )

# ---------------------------------------------------------------------------
# GET /api/filters
# ---------------------------------------------------------------------------

@app.get(
  '/api/filters',
  summary='Get available filter options',
  description='回傳所有可用的產業類別與縣市選項及各選項的工廠數量。',
)
def get_filters(
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()

      cur.execute("""
        SELECT industry_en AS value, COUNT(*) AS count
        FROM factories
        WHERE industry_en IS NOT NULL AND industry_en != ''
        GROUP BY industry_en
        ORDER BY count DESC
      """)
      industries = [{'value': row['value'], 'count': row['count']} for row in cur.fetchall()]

      cur.execute("""
        SELECT city_en AS value, COUNT(*) AS count
        FROM factories
        WHERE city_en IS NOT NULL AND city_en != ''
        GROUP BY city_en
        ORDER BY count DESC
      """)
      cities = [{'value': row['value'], 'count': row['count']} for row in cur.fetchall()]

  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'industries': industries,
    'cities': cities,
  }

# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------

@app.get(
  '/api/stats',
  summary='Database statistics',
  description='回傳資料庫整體統計資訊，包含工廠總數、產業數、縣市數、供應鏈連結數、專利數及政府紀錄數。',
)
def get_stats(
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()

      cur.execute('SELECT COUNT(*) AS cnt FROM factories')
      total_factories = cur.fetchone()['cnt']

      cur.execute('SELECT COUNT(DISTINCT industry_en) AS cnt FROM factories WHERE industry_en != ""')
      industries_count = cur.fetchone()['cnt']

      cur.execute('SELECT COUNT(DISTINCT city_en) AS cnt FROM factories WHERE city_en != ""')
      cities_count = cur.fetchone()['cnt']

      # 新表計數（表不存在時安全降級為 0）
      def safe_count(table: str) -> int:
        try:
          cur.execute(f'SELECT COUNT(*) AS cnt FROM {table}')  # noqa: S608 — table name is hardcoded
          return cur.fetchone()['cnt']
        except sqlite3.OperationalError:
          return 0

      supply_chain_links_count = safe_count('supply_chain_links')
      patents_count = safe_count('patents')
      government_records_count = safe_count('government_records')

  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'total_factories': total_factories,
    'last_updated': date.today().isoformat(),
    'industries_count': industries_count,
    'cities_count': cities_count,
    'supply_chain_links_count': supply_chain_links_count,
    'patents_count': patents_count,
    'government_records_count': government_records_count,
  }

# ---------------------------------------------------------------------------
# GET /api/supply-chain-links  — 查詢 supply_chain_links 資料表（財報來源）
# ---------------------------------------------------------------------------

@app.get(
  '/api/supply-chain-links',
  summary='Query supply chain relationships',
  description='查詢財報來源的供應鏈關係，支援公司名稱或統一編號搜尋。',
)
def get_supply_chain_links(
  company: str = Query(..., description='公司名稱或統一編號'),
  direction: str = Query(default='both', description='upstream / downstream / both'),
  page: int = Query(default=1, ge=1, description='頁碼（從 1 開始）'),
  page_size: int = Query(default=20, ge=1, le=100, description='每頁數量（最大 100）'),
  _auth: dict = Depends(verify_api_key),
):
  if direction not in ('upstream', 'downstream', 'both'):
    raise HTTPException(
      status_code=422,
      detail={
        'error': {
          'code': 'VALIDATION_ERROR',
          'message': 'direction 必須是 upstream、downstream 或 both',
          'details': [{'field': 'direction', 'message': 'must be upstream, downstream, or both'}],
        }
      },
    )

  offset = (page - 1) * page_size

  # 組合 WHERE 條件（按 direction 決定查哪一側）
  # supply_chain_links 欄位：buyer_tax_id, buyer_name, supplier_tax_id, supplier_name
  where_parts: list[str] = []
  params: list = []

  try:
    with get_db() as conn:
      names = resolve_company_names(conn, company)

      if direction in ('downstream', 'both'):
        # 該公司作為 buyer（buyer 的下游是 supplier）
        buyer_conds = ' OR '.join(
          ['buyer_tax_id = ? OR buyer_name LIKE ?' for _ in names]
        )
        where_parts.append(f'({buyer_conds})')
        for n in names:
          params.extend([n, f'%{n}%'])

      if direction in ('upstream', 'both'):
        # 該公司作為 supplier（supplier 的上游是 buyer）
        supplier_conds = ' OR '.join(
          ['supplier_tax_id = ? OR supplier_name LIKE ?' for _ in names]
        )
        where_parts.append(f'({supplier_conds})')
        for n in names:
          params.extend([n, f'%{n}%'])

      where_clause = ' OR '.join(where_parts)

      count_sql = f'SELECT COUNT(*) AS cnt FROM supply_chain_links WHERE {where_clause}'
      data_sql = f"""
        SELECT id, buyer_tax_id, buyer_name, supplier_tax_id, supplier_name,
               relationship_type, source, source_year, purchase_amount, purchase_ratio,
               created_at, updated_at
        FROM supply_chain_links
        WHERE {where_clause}
        ORDER BY source_year DESC, id DESC
        LIMIT ? OFFSET ?
      """

      cur = conn.cursor()
      cur.execute(count_sql, params)
      total = cur.fetchone()['cnt']

      cur.execute(data_sql, params + [page_size, offset])
      rows = cur.fetchall()
  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  results = [
    {
      'id': row['id'],
      'buyer_tax_id': row['buyer_tax_id'],
      'buyer_name': row['buyer_name'],
      'supplier_tax_id': row['supplier_tax_id'],
      'supplier_name': row['supplier_name'],
      'relationship_type': row['relationship_type'],
      'source': row['source'],
      'source_year': row['source_year'],
      'purchase_amount': row['purchase_amount'],
      'purchase_ratio': row['purchase_ratio'],
      'created_at': row['created_at'],
      'updated_at': row['updated_at'],
    }
    for row in rows
  ]

  return {
    'total': total,
    'page': page,
    'page_size': page_size,
    'results': results,
  }

# ---------------------------------------------------------------------------
# GET /api/patents
# ---------------------------------------------------------------------------

@app.get(
  '/api/patents',
  summary='Search patents',
  description='搜尋專利資料，支援申請人名稱、技術分類及關鍵字篩選。',
)
def get_patents(
  applicant: Optional[str] = Query(default=None, description='申請人名稱（部分比對）'),
  ipc: Optional[str] = Query(default=None, description='技術分類碼（對應 tech_category 欄位）'),
  keyword: Optional[str] = Query(default=None, description='關鍵字搜尋標題（中英文）'),
  page: int = Query(default=1, ge=1, description='頁碼（從 1 開始）'),
  page_size: int = Query(default=20, ge=1, le=100, description='每頁數量（最大 100）'),
  _auth: dict = Depends(verify_api_key),
):
  offset = (page - 1) * page_size

  where_parts: list[str] = []
  params: list = []

  try:
    with get_db() as conn:
      if applicant:
        names = resolve_company_names(conn, applicant)
        applicant_conds = ' OR '.join(['applicant_name LIKE ?' for _ in names])
        where_parts.append(f'({applicant_conds})')
        params.extend([f'%{n}%' for n in names])

      if ipc:
        where_parts.append('tech_category LIKE ?')
        params.append(f'%{ipc}%')

      if keyword:
        where_parts.append('(title_zh LIKE ? OR title_en LIKE ?)')
        params.extend([f'%{keyword}%', f'%{keyword}%'])

      where_clause = ('WHERE ' + ' AND '.join(where_parts)) if where_parts else ''

      count_sql = f'SELECT COUNT(*) AS cnt FROM patents {where_clause}'
      data_sql = f"""
        SELECT id, patent_number, application_number, title_zh, title_en,
               applicant_name, applicant_tax_id, tech_category,
               abstract_zh, abstract_en, publication_date, application_date,
               created_at
        FROM patents
        {where_clause}
        ORDER BY application_date DESC, id DESC
        LIMIT ? OFFSET ?
      """

      cur = conn.cursor()
      cur.execute(count_sql, params)
      total = cur.fetchone()['cnt']

      cur.execute(data_sql, params + [page_size, offset])
      rows = cur.fetchall()
  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  results = [
    {
      'id': row['id'],
      'patent_number': row['patent_number'],
      'application_number': row['application_number'],
      'title_zh': row['title_zh'],
      'title_en': row['title_en'],
      'applicant_name': row['applicant_name'],
      'applicant_tax_id': row['applicant_tax_id'],
      'tech_category': row['tech_category'],
      'abstract_zh': row['abstract_zh'],
      'abstract_en': row['abstract_en'],
      'publication_date': row['publication_date'],
      'application_date': row['application_date'],
      'created_at': row['created_at'],
    }
    for row in rows
  ]

  return {
    'total': total,
    'page': page,
    'page_size': page_size,
    'results': results,
  }

# ---------------------------------------------------------------------------
# GET /api/government-records
# ---------------------------------------------------------------------------

@app.get(
  '/api/government-records',
  summary='Government records',
  description='查詢政府紀錄（獎項、認定、補助等），支援公司名稱或統一編號及紀錄類型篩選。',
)
def get_government_records(
  company: Optional[str] = Query(default=None, description='公司名稱、統一編號、獎項名稱、或關鍵字'),
  record_type: Optional[str] = Query(default=None, description='紀錄類型（精確比對）'),
  page: int = Query(default=1, ge=1, description='頁碼（從 1 開始）'),
  page_size: int = Query(default=20, ge=1, le=100, description='每頁數量（最大 100）'),
  _auth: dict = Depends(verify_api_key),
):
  offset = (page - 1) * page_size

  where_parts: list[str] = []
  params: list = []

  try:
    with get_db() as conn:
      if company:
        names = resolve_company_names(conn, company)
        # 搜尋公司名稱/統編
        company_conds = ' OR '.join(
          ['company_tax_id = ? OR company_name LIKE ?' for _ in names]
        )
        # 同時搜尋獎項名稱、英文名稱、發放機關
        program_conds = 'program_name LIKE ? OR program_name_en LIKE ? OR issuing_agency LIKE ?'
        where_parts.append(f'({company_conds} OR {program_conds})')
        for n in names:
          params.extend([n, f'%{n}%'])
        params.extend([f'%{company}%', f'%{company}%', f'%{company}%'])

      if record_type:
        where_parts.append('record_type = ?')
        params.append(record_type)

      where_clause = ('WHERE ' + ' AND '.join(where_parts)) if where_parts else ''

      count_sql = f'SELECT COUNT(*) AS cnt FROM government_records {where_clause}'
      data_sql = f"""
        SELECT id, company_tax_id, company_name, record_type,
               program_name, program_name_en, issuing_agency,
               year, details, created_at
        FROM government_records
        {where_clause}
        ORDER BY year DESC, id DESC
        LIMIT ? OFFSET ?
      """

      cur = conn.cursor()
      cur.execute(count_sql, params)
      total = cur.fetchone()['cnt']

      cur.execute(data_sql, params + [page_size, offset])
      rows = cur.fetchall()
  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  results = [
    {
      'id': row['id'],
      'company_tax_id': row['company_tax_id'],
      'company_name': row['company_name'],
      'record_type': row['record_type'],
      'program_name': row['program_name'],
      'program_name_en': row['program_name_en'],
      'issuing_agency': row['issuing_agency'],
      'year': row['year'],
      'details': row['details'],
      'created_at': row['created_at'],
    }
    for row in rows
  ]

  return {
    'total': total,
    'page': page,
    'page_size': page_size,
    'results': results,
  }

# ---------------------------------------------------------------------------
# GET /api/company/{tax_id}
# ---------------------------------------------------------------------------

@app.get(
  '/api/company/{tax_id}',
  summary='Company profile by Tax ID',
  description='回傳指定統一編號的公司完整 profile，包含所有工廠分廠、供應鏈、專利、政府紀錄及統計摘要。',
)
def get_company_profile(
  tax_id: str,
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()

      # 所有工廠分廠資料（同一 tax_id 可能有多廠）
      cur.execute("""
        SELECT id, tax_id, name_en, name_zh, industry_en, industry_zh,
               city_en, district_en, address_zh, registration_date,
               capital_amount, paid_in_capital, company_setup_date, findbiz_url,
               products_en, products_zh, certifications_en,
               hidden_champion_score, phone, email, website, fax,
               stock_id, official_name_en, is_listed
        FROM factories
        WHERE tax_id = ?
        ORDER BY id ASC
      """, [tax_id])
      factory_rows = cur.fetchall()

      if not factory_rows:
        raise HTTPException(status_code=404, detail={
          'error': {
            'code': 'NOT_FOUND',
            'message': f'找不到統一編號為 {tax_id} 的公司',
            'details': [],
          }
        })

      factories = [
        {
          'id': row['id'],
          'tax_id': row['tax_id'],
          'name_en': row['name_en'],
          'name_zh': row['name_zh'],
          'industry_en': row['industry_en'],
          'industry_zh': row['industry_zh'],
          'city_en': row['city_en'],
          'district_en': row['district_en'],
          'address_zh': row['address_zh'],
          'registration_date': row['registration_date'],
          'capital_amount': row['capital_amount'],
          'paid_in_capital': row['paid_in_capital'],
          'company_setup_date': row['company_setup_date'],
          'findbiz_url': row['findbiz_url'],
          'products_en': row['products_en'],
          'products_zh': row['products_zh'],
          'certifications_en': row['certifications_en'],
          'hidden_champion_score': row['hidden_champion_score'] or 0,
          'phone': row['phone'],
          'email': row['email'],
          'website': row['website'],
          'fax': row['fax'],
          'stock_id': row['stock_id'],
          'official_name_en': row['official_name_en'],
          'is_listed': row['is_listed'] or 0,
        }
        for row in factory_rows
      ]

      # 統計摘要
      industries_set = sorted({f['industry_en'] for f in factories if f['industry_en']})
      cities_set = sorted({f['city_en'] for f in factories if f['city_en']})

      # 供應鏈總筆數（用於 summary）
      cur.execute("""
        SELECT COUNT(*) AS cnt FROM supply_chain_links
        WHERE buyer_tax_id = ? OR supplier_tax_id = ?
      """, [tax_id, tax_id])
      total_supply_chain_links = cur.fetchone()['cnt']

      # 供應鏈（最近 50 筆）
      cur.execute("""
        SELECT id, buyer_tax_id, buyer_name, supplier_tax_id, supplier_name,
               relationship_type, source, source_year, purchase_amount, purchase_ratio
        FROM supply_chain_links
        WHERE buyer_tax_id = ? OR supplier_tax_id = ?
        ORDER BY source_year DESC, id DESC
        LIMIT 50
      """, [tax_id, tax_id])
      supply_chain = [
        {
          'id': row['id'],
          'buyer_tax_id': row['buyer_tax_id'],
          'buyer_name': row['buyer_name'],
          'supplier_tax_id': row['supplier_tax_id'],
          'supplier_name': row['supplier_name'],
          'relationship_type': row['relationship_type'],
          'source': row['source'],
          'source_year': row['source_year'],
          'purchase_amount': row['purchase_amount'],
          'purchase_ratio': row['purchase_ratio'],
        }
        for row in cur.fetchall()
      ]

      # 專利總筆數（用於 summary）
      cur.execute("""
        SELECT COUNT(*) AS cnt FROM patents WHERE applicant_tax_id = ?
      """, [tax_id])
      total_patents = cur.fetchone()['cnt']

      # 專利（最近 50 筆）
      cur.execute("""
        SELECT id, patent_number, application_number, title_zh, title_en,
               applicant_name, tech_category, publication_date, application_date
        FROM patents
        WHERE applicant_tax_id = ?
        ORDER BY application_date DESC, id DESC
        LIMIT 50
      """, [tax_id])
      patents = [
        {
          'id': row['id'],
          'patent_number': row['patent_number'],
          'application_number': row['application_number'],
          'title_zh': row['title_zh'],
          'title_en': row['title_en'],
          'applicant_name': row['applicant_name'],
          'tech_category': row['tech_category'],
          'publication_date': row['publication_date'],
          'application_date': row['application_date'],
        }
        for row in cur.fetchall()
      ]

      # 政府紀錄總筆數（用於 summary）
      cur.execute("""
        SELECT COUNT(*) AS cnt FROM government_records WHERE company_tax_id = ?
      """, [tax_id])
      total_government_records = cur.fetchone()['cnt']

      # 政府紀錄（最近 50 筆）
      cur.execute("""
        SELECT id, company_tax_id, company_name, record_type,
               program_name, program_name_en, issuing_agency, year, details
        FROM government_records
        WHERE company_tax_id = ?
        ORDER BY year DESC, id DESC
        LIMIT 50
      """, [tax_id])
      government_records = [
        {
          'id': row['id'],
          'company_tax_id': row['company_tax_id'],
          'company_name': row['company_name'],
          'record_type': row['record_type'],
          'program_name': row['program_name'],
          'program_name_en': row['program_name_en'],
          'issuing_agency': row['issuing_agency'],
          'year': row['year'],
          'details': row['details'],
        }
        for row in cur.fetchall()
      ]

  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'factories': factories,
    'summary': {
      'total_factories': len(factories),
      'total_patents': total_patents,
      'total_supply_chain_links': total_supply_chain_links,
      'total_government_records': total_government_records,
      'industries': industries_set,
      'cities': cities_set,
    },
    'supply_chain': supply_chain,
    'patents': patents,
    'government_records': government_records,
  }

# ---------------------------------------------------------------------------
# GET /api/supply-chain/list  — 所有有供應鏈資料的買方
# ---------------------------------------------------------------------------

@app.get('/api/supply-chain/list')
def get_supply_chain_list(
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()
      cur.execute("""
        SELECT buyer_name, buyer_stock_id, COUNT(*) AS supplier_count
        FROM supply_chain
        GROUP BY buyer_name, buyer_stock_id
        ORDER BY supplier_count DESC
      """)
      rows = cur.fetchall()
  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'buyers': [
      {
        'name': row['buyer_name'],
        'stock_id': row['buyer_stock_id'],
        'supplier_count': row['supplier_count'],
      }
      for row in rows
    ]
  }


# ---------------------------------------------------------------------------
# GET /api/supply-chain?buyer=台積電  — 買方的供應商列表
# ---------------------------------------------------------------------------

@app.get('/api/supply-chain')
def get_supply_chain_by_buyer(
  buyer: str = Query(..., description='買方公司名稱（如：台積電）'),
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()

      # 取得買方基本資訊（支援中文名、英文名、股票代碼）
      buyer_search = buyer.strip()
      cur.execute(
        """SELECT buyer_name, buyer_stock_id FROM supply_chain
           WHERE buyer_name = ? OR buyer_stock_id = ? OR buyer_name LIKE ?
           LIMIT 1""",
        (buyer_search, buyer_search, f'%{buyer_search}%'),
      )
      buyer_row = cur.fetchone()

      # 如果用中文/代碼找不到，試用英文名反查
      if not buyer_row:
        # 從 supply_chain 表找所有買方，比對英文
        cur.execute('SELECT DISTINCT buyer_name, buyer_stock_id FROM supply_chain')
        all_buyers = cur.fetchall()
        # 建立英文名 → 中文名映射
        EN_BUYER_MAP = {
            'tsmc': '台積電', 'foxconn': '鴻海', 'delta': '台達電',
            'ase': '日月光', 'pegatron': '和碩', 'umc': '聯電',
            'quanta': '廣達', 'wistron': '緯創', 'advantech': '研華',
            'aidc': '漢翔', 'chunghwa': '中華電信', 'cht': '中華電信',
        }
        mapped = EN_BUYER_MAP.get(buyer_search.lower())
        if mapped:
          cur.execute(
            'SELECT buyer_name, buyer_stock_id FROM supply_chain WHERE buyer_name = ? LIMIT 1',
            (mapped,),
          )
          buyer_row = cur.fetchone()

      if not buyer_row:
        raise HTTPException(status_code=404, detail=f'找不到買方：{buyer}')

      actual_buyer = buyer_row['buyer_name']

      # 取得供應商列表（JOIN factories 取得工廠詳情）
      cur.execute(
        """
        SELECT
          sc.supplier_name_zh,
          sc.supplier_stock_id,
          sc.category,
          sc.product,
          sc.confidence,
          f.id          AS factory_id,
          f.name_en     AS factory_name_en,
          f.city_en     AS factory_city_en,
          f.industry_en AS factory_industry_en
        FROM supply_chain sc
        LEFT JOIN factories f ON f.id = sc.supplier_factory_id
        WHERE sc.buyer_name = ?
        ORDER BY sc.category, sc.supplier_name_zh
        """,
        (actual_buyer,),
      )
      supplier_rows = cur.fetchall()

  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  matched_count = sum(1 for r in supplier_rows if r['factory_id'] is not None)
  total_count = len(supplier_rows)

  suppliers = []
  for r in supplier_rows:
    supplier_entry = {
      'name_zh': r['supplier_name_zh'],
      'stock_id': r['supplier_stock_id'],
      'category': r['category'],
      'product': r['product'],
      'confidence': r['confidence'],
      'factory': None,
    }
    if r['factory_id'] is not None:
      supplier_entry['factory'] = {
        'id': r['factory_id'],
        'name_en': r['factory_name_en'],
        'city_en': r['factory_city_en'],
        'industry_en': r['factory_industry_en'],
      }
    suppliers.append(supplier_entry)

  return {
    'buyer': buyer_row['buyer_name'],
    'buyer_stock_id': buyer_row['buyer_stock_id'],
    'suppliers': suppliers,
    'match_rate': round(matched_count / total_count, 2) if total_count > 0 else 0.0,
  }


# ---------------------------------------------------------------------------
# GET /api/factory/{factory_id}/supply-chain-tags — 工廠的供應商標籤（供前端用）
# ---------------------------------------------------------------------------

@app.get('/api/factory/{factory_id}/supply-chain-tags')
def get_factory_supply_chain_tags(
  factory_id: int,
  _auth: dict = Depends(verify_api_key),
):
  try:
    with get_db() as conn:
      cur = conn.cursor()
      cur.execute(
        """
        SELECT buyer_name, buyer_stock_id, category, product, confidence
        FROM supply_chain
        WHERE supplier_factory_id = ?
        """,
        (factory_id,),
      )
      rows = cur.fetchall()
  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'factory_id': factory_id,
    'tags': [
      {
        'buyer_name': row['buyer_name'],
        'buyer_stock_id': row['buyer_stock_id'],
        'category': row['category'],
        'product': row['product'],
        'confidence': row['confidence'],
      }
      for row in rows
    ],
  }


# ---------------------------------------------------------------------------
# GET /api/hidden-champions
# ---------------------------------------------------------------------------

import json as _json


@app.get(
  '/api/hidden-champions',
  summary='List hidden champion companies',
  description='回傳隱形冠軍工廠清單，按 hidden_champion_score 降序，支援最低分數門檻及產業篩選。',
)
def get_hidden_champions(
  min_score: int = Query(default=1, ge=0, le=100, description='最低分數門檻（預設 1）'),
  industry: Optional[str] = Query(default=None, description='產業類別英文（精確比對）'),
  page: int = Query(default=1, ge=1, description='頁碼（從 1 開始）'),
  page_size: int = Query(default=20, ge=1, le=100, description='每頁數量（最大 100）'),
  _auth: dict = Depends(verify_api_key),
):
  """
  回傳隱形冠軍工廠清單，按 hidden_champion_score 降序。

  - min_score: 最低分數門檻（預設 1，即所有評過分的工廠）
  - industry: 產業類別篩選（精確比對 industry_en）
  - page / page_size: 分頁
  """
  offset = (page - 1) * page_size

  where_parts = ['hidden_champion_score >= ?']
  params: list = [min_score]

  if industry:
    where_parts.append('industry_en = ?')
    params.append(industry)

  where_clause = 'WHERE ' + ' AND '.join(where_parts)

  count_sql = f'SELECT COUNT(*) AS cnt FROM factories {where_clause}'
  data_sql = f"""
    SELECT id, tax_id, name_en, name_zh,
           industry_en, industry_zh,
           city_en, district_en,
           capital_amount,
           hidden_champion_score,
           hidden_champion_reasons,
           hidden_champion_updated_at
    FROM factories
    {where_clause}
    ORDER BY hidden_champion_score DESC, id ASC
    LIMIT ? OFFSET ?
  """

  try:
    with get_db() as conn:
      cur = conn.cursor()

      cur.execute(count_sql, params)
      total = cur.fetchone()['cnt']

      cur.execute(data_sql, params + [page_size, offset])
      rows = cur.fetchall()
  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  results = []
  for row in rows:
    reasons = None
    if row['hidden_champion_reasons']:
      try:
        reasons = _json.loads(row['hidden_champion_reasons'])
      except _json.JSONDecodeError:
        reasons = None

    results.append({
      'id': row['id'],
      'tax_id': row['tax_id'],
      'name_en': row['name_en'],
      'name_zh': row['name_zh'],
      'industry_en': row['industry_en'],
      'industry_zh': row['industry_zh'],
      'city_en': row['city_en'],
      'district_en': row['district_en'],
      'capital_amount': row['capital_amount'],
      'hidden_champion_score': row['hidden_champion_score'] or 0,
      'hidden_champion_reasons': reasons,
      'hidden_champion_updated_at': row['hidden_champion_updated_at'],
    })

  return {
    'total': total,
    'page': page,
    'page_size': page_size,
    'results': results,
  }


# ---------------------------------------------------------------------------
# GET /api/supply-chain/graph  — 供應鏈關係圖（圖表專用）
# ---------------------------------------------------------------------------

@app.get(
  '/api/supply-chain/graph',
  summary='Supply chain graph for a company',
  description='回傳以特定公司為中心的供應鏈關係圖，供前端圖表渲染使用。支援 depth=1~3 的多層關係展開。',
)
def get_supply_chain_graph(
  company: str = Query(..., description='公司名稱或統一編號'),
  depth: int = Query(default=1, ge=1, le=3, description='展開深度（1=直接上下游，2=二度關係，3=三度關係）'),
  _auth: dict = Depends(verify_api_key),
):
  """
  以指定公司為中心，回傳 depth 層供應鏈關係圖。

  Node 結構：
    id      — tax_id 優先，否則用 name_<名稱>
    name    — 英文名稱（若能從 factories 表查到）
    name_zh — 中文名稱
    type    — 'center' | 'buyer' | 'supplier'
    tax_id  — 統一編號（可能為 null）

  Edge 結構：
    source  — 供應商 node id
    target  — 買方 node id
    type    — 'supplier'
    year    — 年份
    ratio   — 採購佔比（%）
  """
  company = company.strip()
  if not company:
    raise HTTPException(
      status_code=400,
      detail={'error': {'code': 'MISSING_PARAM', 'message': 'company 參數不能為空'}},
    )

  def _node_id(tax_id: Optional[str], name: str) -> str:
    return tax_id if tax_id else f'name_{name}'

  def _fetch_factory_info(cur: sqlite3.Cursor, tax_ids: list[str]) -> dict[str, dict]:
    """從 factories 取每個 tax_id 的代表性名稱（優先取 official_name_en，其次最短 name_en）。"""
    if not tax_ids:
      return {}
    placeholders = ','.join('?' * len(tax_ids))
    # 取每個 tax_id 的所有記錄，後面再挑最佳的
    cur.execute(
      f'SELECT tax_id, name_en, name_zh, official_name_en FROM factories WHERE tax_id IN ({placeholders})',
      tax_ids,
    )
    result: dict[str, dict] = {}
    for row in cur.fetchall():
      tid = row['tax_id']
      if tid not in result:
        result[tid] = {
          'name_en': row['official_name_en'] or row['name_en'],
          'name_zh': row['name_zh'],
        }
      else:
        # 已有記錄：若 official_name_en 有值則優先，否則比較名稱長度取短的（較可能是總公司）
        existing = result[tid]
        if row['official_name_en'] and not existing['name_en']:
          result[tid]['name_en'] = row['official_name_en']
        elif (
          not existing['name_en'] or
          (row['name_en'] and len(row['name_en']) < len(existing['name_en'] or ''))
        ):
          if not (row['official_name_en'] or existing.get('name_en', '').startswith(row['name_en'] or '')):
            result[tid]['name_en'] = row['name_en']
        if row['name_zh'] and (not existing['name_zh'] or len(row['name_zh']) < len(existing['name_zh'])):
          result[tid]['name_zh'] = row['name_zh']
    return result

  try:
    with get_db() as conn:
      cur = conn.cursor()

      # 解析起始公司（先精確 tax_id，再名稱模糊）
      cur.execute(
        """
        SELECT buyer_name AS name, buyer_tax_id AS tax_id
        FROM supply_chain_links
        WHERE buyer_tax_id = ?
        UNION
        SELECT supplier_name AS name, supplier_tax_id AS tax_id
        FROM supply_chain_links
        WHERE supplier_tax_id = ?
        LIMIT 1
        """,
        (company, company),
      )
      center_row = cur.fetchone()

      if not center_row:
        cur.execute(
          """
          SELECT buyer_name AS name, buyer_tax_id AS tax_id
          FROM supply_chain_links
          WHERE buyer_name LIKE ?
          LIMIT 1
          """,
          (f'%{company}%',),
        )
        center_row = cur.fetchone()

      if not center_row:
        cur.execute(
          """
          SELECT supplier_name AS name, supplier_tax_id AS tax_id
          FROM supply_chain_links
          WHERE supplier_name LIKE ?
          LIMIT 1
          """,
          (f'%{company}%',),
        )
        center_row = cur.fetchone()

      if not center_row:
        raise HTTPException(
          status_code=404,
          detail={'error': {'code': 'NOT_FOUND', 'message': f'找不到公司：{company}'}},
        )

      center_name = center_row['name']
      center_tax_id = center_row['tax_id']
      center_id = _node_id(center_tax_id, center_name)

      nodes: dict[str, dict] = {
        center_id: {
          'id': center_id,
          'name': center_name,
          'name_zh': center_name,
          'type': 'center',
          'is_center': True,
          'tax_id': center_tax_id,
        }
      }
      edges_raw: list[dict] = []

      # BFS 展開 depth 層（同時向上、向下）
      frontier: list[tuple[str, Optional[str]]] = [(center_name, center_tax_id)]

      for _level in range(depth):
        next_frontier: list[tuple[str, Optional[str]]] = []

        for node_name, node_tax_id in frontier:
          node_id = _node_id(node_tax_id, node_name)

          # 向下：該節點為 buyer，找 supplier
          if node_tax_id:
            cond = '(buyer_tax_id = ? OR buyer_name = ?)'
            p: list = [node_tax_id, node_name]
          else:
            cond = 'buyer_name = ?'
            p = [node_name]

          cur.execute(
            f"""
            SELECT supplier_tax_id, supplier_name,
                   source_year, purchase_amount, purchase_ratio
            FROM supply_chain_links
            WHERE {cond}
            ORDER BY source_year DESC, purchase_ratio DESC
            """,
            p,
          )
          for row in cur.fetchall():
            sup_tax_id = row['supplier_tax_id']
            sup_name = row['supplier_name']
            sup_id = _node_id(sup_tax_id, sup_name)

            if sup_id not in nodes:
              nodes[sup_id] = {
                'id': sup_id,
                'name': sup_name,
                'name_zh': sup_name,
                'type': 'supplier',
                'is_center': False,
                'tax_id': sup_tax_id,
              }
              next_frontier.append((sup_name, sup_tax_id))

            edges_raw.append({
              'source': sup_id,
              'target': node_id,
              'type': 'supplier',
              'year': row['source_year'],
              'ratio': row['purchase_ratio'],
              'amount': row['purchase_amount'],
            })

          # 向上：該節點為 supplier，找 buyer
          if node_tax_id:
            cond_up = '(supplier_tax_id = ? OR supplier_name = ?)'
            p_up: list = [node_tax_id, node_name]
          else:
            cond_up = 'supplier_name = ?'
            p_up = [node_name]

          cur.execute(
            f"""
            SELECT buyer_tax_id, buyer_name,
                   source_year, purchase_amount, purchase_ratio
            FROM supply_chain_links
            WHERE {cond_up}
            ORDER BY source_year DESC, purchase_ratio DESC
            """,
            p_up,
          )
          for row in cur.fetchall():
            buyer_tax_id_val = row['buyer_tax_id']
            buyer_name_val = row['buyer_name']
            buyer_id = _node_id(buyer_tax_id_val, buyer_name_val)

            if buyer_id not in nodes:
              nodes[buyer_id] = {
                'id': buyer_id,
                'name': buyer_name_val,
                'name_zh': buyer_name_val,
                'type': 'buyer',
                'is_center': False,
                'tax_id': buyer_tax_id_val,
              }
              next_frontier.append((buyer_name_val, buyer_tax_id_val))

            edges_raw.append({
              'source': node_id,
              'target': buyer_id,
              'type': 'supplier',
              'year': row['source_year'],
              'ratio': row['purchase_ratio'],
              'amount': row['purchase_amount'],
            })

        frontier = next_frontier

      # 從 factories 補充英文名（只覆蓋 name，保留 name_zh 中文不動）
      all_tax_ids = [n['tax_id'] for n in nodes.values() if n['tax_id']]
      factory_info = _fetch_factory_info(cur, all_tax_ids)
      for node in nodes.values():
        tid = node['tax_id']
        if tid and tid in factory_info:
          fi = factory_info[tid]
          # 只在有 official_name_en 或原本 name 就是中文時才覆蓋英文名
          en_name = fi.get('name_en') or ''
          if en_name and any('\u4e00' <= c <= '\u9fff' for c in (node['name'] or '')):
            # 原本 name 是中文，嘗試補充英文名
            node['name'] = en_name
          # name_zh 固定使用 supply_chain_links 中的名稱（比廠區名更精準）

  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  # 去重 edges（同 source/target/year 保留 ratio 最大者）
  edge_map: dict[str, dict] = {}
  for edge in edges_raw:
    key = f'{edge["source"]}->{edge["target"]}@{edge["year"]}'
    if key not in edge_map or (edge['ratio'] or 0) > (edge_map[key]['ratio'] or 0):
      edge_map[key] = edge

  return {
    'center': {
      'name': center_name,
      'name_zh': center_name,
      'tax_id': center_tax_id,
      'type': 'center',
    },
    'nodes': list(nodes.values()),
    'edges': list(edge_map.values()),
  }


# ---------------------------------------------------------------------------
# GET /api/supply-chain/buyers  — 所有有供應鏈資料的買方（supply_chain_links）
# ---------------------------------------------------------------------------

@app.get(
  '/api/supply-chain/buyers',
  summary='List all buyers with supply chain data',
  description='回傳所有在 supply_chain_links 表中有供應鏈資料的買方列表（去重）。',
)
def get_supply_chain_buyers(
  _auth: dict = Depends(verify_api_key),
):
  """回傳所有有供應鏈資料的買方，含供應商數量。"""
  try:
    with get_db() as conn:
      cur = conn.cursor()
      cur.execute(
        """
        SELECT
          buyer_name,
          buyer_tax_id,
          COUNT(DISTINCT supplier_name) AS supplier_count,
          MAX(source_year) AS latest_year
        FROM supply_chain_links
        WHERE buyer_name IS NOT NULL AND buyer_name != ''
        GROUP BY buyer_name, buyer_tax_id
        ORDER BY supplier_count DESC
        """
      )
      rows = cur.fetchall()
  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  return {
    'buyers': [
      {
        'name': row['buyer_name'],
        'tax_id': row['buyer_tax_id'],
        'supplier_count': row['supplier_count'],
        'latest_year': row['latest_year'],
      }
      for row in rows
    ]
  }


# ---------------------------------------------------------------------------
# 搜尋建議（Autocomplete）
# ---------------------------------------------------------------------------

@app.get(
  '/api/suggest',
  summary='Search autocomplete suggestions',
  description='回傳搜尋框自動完成建議，來源包含產業類別、縣市、公司名稱及產品關鍵字。',
)
def get_suggestions(
  q: str = Query(default='', description='搜尋關鍵詞（至少 2 個字元）'),
  limit: int = Query(default=8, ge=1, le=20, description='最多回傳筆數'),
  _auth: dict = Depends(verify_api_key),
):
  """
  回傳搜尋建議，來源：industry_en、products_en、city_en、公司名稱。
  至少需輸入 2 個字元才會回傳結果。
  """
  if not q or len(q.strip()) < 2:
    return {'suggestions': []}

  q = q.strip()
  suggestions: list[dict] = []
  seen: set[str] = set()

  # 每類各取上限，總數不超過 limit
  per_type_limit = max(2, limit // 3)

  try:
    with get_db() as conn:
      cur = conn.cursor()

      # 1. 產業建議
      cur.execute(
        '''
        SELECT DISTINCT industry_en AS text, COUNT(*) AS cnt
        FROM factories
        WHERE industry_en IS NOT NULL AND industry_en LIKE ? || '%'
        GROUP BY industry_en
        ORDER BY cnt DESC
        LIMIT ?
        ''',
        (q, per_type_limit),
      )
      for row in cur.fetchall():
        key = row['text'].lower()
        if key not in seen:
          seen.add(key)
          suggestions.append({'text': row['text'], 'type': 'industry', 'count': row['cnt']})

      # 2. 城市建議
      cur.execute(
        '''
        SELECT DISTINCT city_en AS text, COUNT(*) AS cnt
        FROM factories
        WHERE city_en IS NOT NULL AND city_en LIKE ? || '%'
        GROUP BY city_en
        ORDER BY cnt DESC
        LIMIT 2
        ''',
        (q,),
      )
      for row in cur.fetchall():
        key = row['text'].lower()
        if key not in seen:
          seen.add(key)
          suggestions.append({'text': row['text'], 'type': 'city', 'count': row['cnt']})

      # 3. 公司名稱建議（official_name_en 優先，否則 name_en）
      cur.execute(
        '''
        SELECT COALESCE(official_name_en, name_en) AS text, COUNT(*) AS cnt
        FROM factories
        WHERE (
          (name_en IS NOT NULL AND name_en LIKE ? || '%')
          OR (official_name_en IS NOT NULL AND official_name_en LIKE ? || '%')
        )
        GROUP BY text
        ORDER BY cnt DESC
        LIMIT ?
        ''',
        (q, q, per_type_limit),
      )
      for row in cur.fetchall():
        if not row['text']:
          continue
        key = row['text'].lower()
        if key not in seen:
          seen.add(key)
          suggestions.append({'text': row['text'], 'type': 'company', 'count': row['cnt']})

      # 4. 產品建議（products_en 欄位，部分比對）
      cur.execute(
        '''
        SELECT DISTINCT products_en AS text, COUNT(*) AS cnt
        FROM factories
        WHERE products_en IS NOT NULL AND products_en LIKE '%' || ? || '%'
        GROUP BY products_en
        ORDER BY cnt DESC
        LIMIT ?
        ''',
        (q, per_type_limit),
      )
      for row in cur.fetchall():
        if not row['text']:
          continue
        key = row['text'].lower()
        if key not in seen:
          seen.add(key)
          suggestions.append({'text': row['text'], 'type': 'product', 'count': row['cnt']})

  except HTTPException:
    raise
  except sqlite3.OperationalError as e:
    raise HTTPException(status_code=400, detail=f'Query error: {e}')
  except Exception as e:
    raise HTTPException(status_code=500, detail=f'Database error: {e}')

  # 依 count 降序，截取 limit 筆
  suggestions.sort(key=lambda x: x['count'], reverse=True)
  return {'suggestions': suggestions[:limit]}


# ---------------------------------------------------------------------------
# Admin Endpoints（需要 TMDB_ADMIN_KEY 環境變數認證）
# ---------------------------------------------------------------------------

@app.post('/api/admin/keys', summary='Create API key')
def admin_create_key(
  name: str = Query(..., description='客戶名稱'),
  email: Optional[str] = Query(default=None, description='聯絡 email'),
  tier: str = Query(default='free', description='free / pro / enterprise'),
  rate_limit_per_minute: int = Query(default=30, ge=1, description='每分鐘請求上限'),
  rate_limit_per_day: int = Query(default=1000, ge=1, description='每天請求上限'),
  expires_at: Optional[str] = Query(default=None, description='過期時間（ISO 8601，NULL = 永不過期）'),
  _admin: None = Depends(verify_admin_key),
):
  new_key = f'tmdb-{secrets.token_urlsafe(16)}'
  with get_db() as conn:
    conn.execute(
      """
      INSERT INTO api_keys (key, name, email, tier, rate_limit_per_minute, rate_limit_per_day, expires_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """,
      (new_key, name, email, tier, rate_limit_per_minute, rate_limit_per_day, expires_at),
    )
    conn.commit()
    cur = conn.cursor()
    cur.execute('SELECT * FROM api_keys WHERE key = ?', (new_key,))
    row = cur.fetchone()
  return {
    'id': row['id'],
    'key': row['key'],
    'name': row['name'],
    'email': row['email'],
    'tier': row['tier'],
    'rate_limit_per_minute': row['rate_limit_per_minute'],
    'rate_limit_per_day': row['rate_limit_per_day'],
    'is_active': row['is_active'],
    'created_at': row['created_at'],
    'expires_at': row['expires_at'],
  }


@app.get('/api/admin/keys', summary='List all API keys')
def admin_list_keys(
  _admin: None = Depends(verify_admin_key),
):
  with get_db() as conn:
    cur = conn.cursor()
    cur.execute(
      'SELECT id, key, name, email, tier, rate_limit_per_minute, rate_limit_per_day, is_active, created_at, expires_at FROM api_keys ORDER BY created_at DESC'
    )
    rows = cur.fetchall()
  return {
    'keys': [
      {
        'id': row['id'],
        'key': row['key'],
        'name': row['name'],
        'email': row['email'],
        'tier': row['tier'],
        'rate_limit_per_minute': row['rate_limit_per_minute'],
        'rate_limit_per_day': row['rate_limit_per_day'],
        'is_active': row['is_active'],
        'created_at': row['created_at'],
        'expires_at': row['expires_at'],
      }
      for row in rows
    ]
  }


@app.get('/api/admin/usage', summary='API usage statistics')
def admin_get_usage(
  key_id: Optional[int] = Query(default=None, description='指定 API key ID（不填則查全部）'),
  limit: int = Query(default=100, ge=1, le=1000, description='回傳筆數上限'),
  _admin: None = Depends(verify_admin_key),
):
  with get_db() as conn:
    cur = conn.cursor()
    if key_id:
      cur.execute(
        """
        SELECT u.id, u.api_key_id, k.name AS key_name, u.endpoint, u.method,
               u.status_code, u.response_time_ms, u.ip_address, u.created_at
        FROM api_usage u
        JOIN api_keys k ON k.id = u.api_key_id
        WHERE u.api_key_id = ?
        ORDER BY u.created_at DESC
        LIMIT ?
        """,
        (key_id, limit),
      )
    else:
      cur.execute(
        """
        SELECT u.id, u.api_key_id, k.name AS key_name, u.endpoint, u.method,
               u.status_code, u.response_time_ms, u.ip_address, u.created_at
        FROM api_usage u
        JOIN api_keys k ON k.id = u.api_key_id
        ORDER BY u.created_at DESC
        LIMIT ?
        """,
        (limit,),
      )
    rows = cur.fetchall()

    # 統計摘要（各 key 的總請求數）
    cur.execute(
      """
      SELECT k.id, k.name, COUNT(u.id) AS total_requests,
             SUM(CASE WHEN u.status_code = 200 THEN 1 ELSE 0 END) AS success_count,
             AVG(u.response_time_ms) AS avg_response_ms
      FROM api_keys k
      LEFT JOIN api_usage u ON u.api_key_id = k.id
      GROUP BY k.id, k.name
      ORDER BY total_requests DESC
      """
    )
    summary_rows = cur.fetchall()

  return {
    'summary': [
      {
        'key_id': row['id'],
        'key_name': row['name'],
        'total_requests': row['total_requests'],
        'success_count': row['success_count'],
        'avg_response_ms': round(row['avg_response_ms'] or 0, 1),
      }
      for row in summary_rows
    ],
    'recent': [
      {
        'id': row['id'],
        'api_key_id': row['api_key_id'],
        'key_name': row['key_name'],
        'endpoint': row['endpoint'],
        'method': row['method'],
        'status_code': row['status_code'],
        'response_time_ms': row['response_time_ms'],
        'ip_address': row['ip_address'],
        'created_at': row['created_at'],
      }
      for row in rows
    ],
  }


# ---------------------------------------------------------------------------
# 靜態檔案 & 首頁
# ---------------------------------------------------------------------------

STATIC_DIR = APP_DIR / 'static'

@app.get('/')
def serve_index():
  return FileResponse(
    STATIC_DIR / 'index.html',
    headers={'Cache-Control': 'no-cache, no-store, must-revalidate', 'Pragma': 'no-cache'},
  )


app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')
