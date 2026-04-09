"""
build_database.py — 建立 SQLite 資料庫並建立 FTS5 全文搜尋索引

讀取 src/data/factories_translated.json，建立：
  - factories 主表
  - factories_fts FTS5 虛擬表（僅索引英文欄位）

輸出：src/data/tmdb.db
"""

import json
import sqlite3
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'

INPUT_PATH = DATA_DIR / 'factories_translated.json'
DB_PATH = DATA_DIR / 'tmdb.db'

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_FACTORIES = """
CREATE TABLE IF NOT EXISTS factories (
    id                INTEGER PRIMARY KEY,
    tax_id            TEXT,
    name_zh           TEXT NOT NULL,
    name_en           TEXT NOT NULL,
    industry_zh       TEXT,
    industry_en       TEXT,
    address_zh        TEXT,
    city_en           TEXT,
    district_en       TEXT,
    registration_date TEXT,
    status            TEXT,
    needs_llm_translation INTEGER DEFAULT 0
);
"""

DDL_FTS5 = """
CREATE VIRTUAL TABLE IF NOT EXISTS factories_fts USING fts5(
    name_en,
    industry_en,
    city_en,
    district_en,
    search_tags,
    content='factories',
    content_rowid='id'
);
"""

# FTS5 content table triggers（保持 FTS 索引與主表同步）
DDL_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS factories_ai AFTER INSERT ON factories BEGIN
    INSERT INTO factories_fts(rowid, name_en, industry_en, city_en, district_en, search_tags)
    VALUES (new.id, new.name_en, new.industry_en, new.city_en, new.district_en, new.search_tags);
END;

CREATE TRIGGER IF NOT EXISTS factories_ad AFTER DELETE ON factories BEGIN
    INSERT INTO factories_fts(factories_fts, rowid, name_en, industry_en, city_en, district_en, search_tags)
    VALUES ('delete', old.id, old.name_en, old.industry_en, old.city_en, old.district_en, old.search_tags);
END;

CREATE TRIGGER IF NOT EXISTS factories_au AFTER UPDATE ON factories BEGIN
    INSERT INTO factories_fts(factories_fts, rowid, name_en, industry_en, city_en, district_en, search_tags)
    VALUES ('delete', old.id, old.name_en, old.industry_en, old.city_en, old.district_en, old.search_tags);
    INSERT INTO factories_fts(rowid, name_en, industry_en, city_en, district_en, search_tags)
    VALUES (new.id, new.name_en, new.industry_en, new.city_en, new.district_en, new.search_tags);
END;
"""

# 一般欄位索引（加速 WHERE 過濾）
DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_factories_tax_id   ON factories(tax_id);
CREATE INDEX IF NOT EXISTS idx_factories_city_en  ON factories(city_en);
CREATE INDEX IF NOT EXISTS idx_factories_industry ON factories(industry_en);
CREATE INDEX IF NOT EXISTS idx_factories_status   ON factories(status);
"""


# ---------------------------------------------------------------------------
# 資料庫操作
# ---------------------------------------------------------------------------

def create_schema(conn: sqlite3.Connection) -> None:
    """建立所有表格、FTS 虛擬表、觸發器與索引。"""
    cur = conn.cursor()
    cur.executescript(DDL_FACTORIES)
    cur.executescript(DDL_FTS5)
    cur.executescript(DDL_TRIGGERS)
    cur.executescript(DDL_INDEXES)
    conn.commit()
    print('Schema created.')


BATCH_SIZE = 5000  # 每批插入筆數，平衡記憶體與效能


def insert_factories(conn: sqlite3.Connection, factories: list[dict[str, Any]]) -> int:
    """
    批次插入工廠資料（每 BATCH_SIZE 筆一批），並顯示進度。

    觸發器在此函數呼叫前已關閉，FTS5 索引由 rebuild_fts_index 統一處理。

    Args:
        conn: SQLite 連線
        factories: 翻譯後的工廠 dict list

    Returns:
        實際插入總筆數
    """
    INSERT_SQL = """
    INSERT OR REPLACE INTO factories
        (id, tax_id, name_zh, name_en, industry_zh, industry_en,
         address_zh, city_en, district_en, registration_date, status,
         needs_llm_translation)
    VALUES
        (:id, :tax_id, :name_zh, :name_en, :industry_zh, :industry_en,
         :address_zh, :city_en, :district_en, :registration_date, :status,
         :needs_llm_translation)
    """

    total_inserted = 0
    cur = conn.cursor()

    for batch_start in range(0, len(factories), BATCH_SIZE):
        batch = factories[batch_start:batch_start + BATCH_SIZE]
        rows = [
            {
                'id': f.get('id'),
                'tax_id': f.get('tax_id', ''),
                'name_zh': f.get('name_zh', ''),
                'name_en': f.get('name_en', ''),
                'industry_zh': f.get('industry_zh', ''),
                'industry_en': f.get('industry_en', ''),
                'address_zh': f.get('address_zh', ''),
                'city_en': f.get('city_en', ''),
                'district_en': f.get('district_en', ''),
                'registration_date': f.get('registration_date', ''),
                'status': f.get('status', ''),
                'needs_llm_translation': 1 if f.get('needs_llm_translation') else 0,
            }
            for f in batch
        ]
        cur.executemany(INSERT_SQL, rows)
        conn.commit()
        total_inserted += len(batch)
        print(f'  Inserted {total_inserted:,} / {len(factories):,} records...')

    return total_inserted


def rebuild_fts_index(conn: sqlite3.Connection) -> None:
    """
    重建 FTS5 索引（rebuild 命令）。
    適用於批次插入後確保索引完整性。
    """
    conn.execute("INSERT INTO factories_fts(factories_fts) VALUES ('rebuild')")
    conn.commit()
    print('FTS5 index rebuilt.')


def verify_fts(conn: sqlite3.Connection) -> None:
    """驗證 FTS5 搜尋功能正常。"""
    cur = conn.cursor()

    # 取任意一筆資料的 city_en 進行搜尋驗證
    cur.execute('SELECT city_en FROM factories LIMIT 1')
    row = cur.fetchone()
    if not row:
        print('No data to verify FTS.')
        return

    city_en = row[0].split()[0] if row[0] else 'Hsinchu'

    cur.execute(
        'SELECT COUNT(*) FROM factories_fts WHERE factories_fts MATCH ?',
        (city_en,)
    )
    count = cur.fetchone()[0]
    print(f'FTS verification: search "{city_en}" → {count} result(s). OK')


def print_stats(conn: sqlite3.Connection) -> None:
    """印出資料庫統計資訊。"""
    cur = conn.cursor()

    cur.execute('SELECT COUNT(*) FROM factories')
    total = cur.fetchone()[0]
    print(f'\nDatabase stats:')
    print(f'  Total factories : {total}')

    cur.execute('SELECT COUNT(DISTINCT city_en) FROM factories')
    cities = cur.fetchone()[0]
    print(f'  Unique cities   : {cities}')

    cur.execute('SELECT COUNT(DISTINCT industry_en) FROM factories')
    industries = cur.fetchone()[0]
    print(f'  Unique industries: {industries}')

    cur.execute('SELECT COUNT(*) FROM factories WHERE needs_llm_translation = 1')
    needs_llm = cur.fetchone()[0]
    print(f'  Needs LLM transl: {needs_llm}')

    print('\nTop 5 cities:')
    cur.execute(
        'SELECT city_en, COUNT(*) as cnt FROM factories GROUP BY city_en ORDER BY cnt DESC LIMIT 5'
    )
    for city, cnt in cur.fetchall():
        print(f'  {city}: {cnt}')


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    if not INPUT_PATH.exists():
        print(f'Input file not found: {INPUT_PATH}')
        print('Please run translate_factories.py first.')
        return

    print(f'Loading {INPUT_PATH}...')
    with INPUT_PATH.open(encoding='utf-8') as f:
        factories: list[dict[str, Any]] = json.load(f)
    print(f'Loaded {len(factories)} translated records.')

    # 若 DB 已存在，先刪除重建（確保 schema 乾淨）
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f'Removed existing database: {DB_PATH}')

    print(f'Creating database: {DB_PATH}')
    conn = sqlite3.connect(str(DB_PATH))

    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA foreign_keys=ON')

        create_schema(conn)

        print(f'Inserting {len(factories):,} records in batches of {BATCH_SIZE:,}...')
        # 插入時不使用觸發器（批次效率低），先關閉再手動 rebuild
        conn.execute('DROP TRIGGER IF EXISTS factories_ai')
        conn.execute('DROP TRIGGER IF EXISTS factories_ad')
        conn.execute('DROP TRIGGER IF EXISTS factories_au')

        inserted = insert_factories(conn, factories)
        print(f'Insert complete: {inserted:,} records.')

        # 重建 FTS 索引
        rebuild_fts_index(conn)

        # 重建觸發器
        conn.executescript(DDL_TRIGGERS)
        conn.commit()
        print('Triggers re-created.')

        verify_fts(conn)
        print_stats(conn)

    finally:
        conn.close()

    print(f'\nDatabase ready: {DB_PATH}')


if __name__ == '__main__':
    main()
