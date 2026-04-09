"""
fix_industry_translation.py

Translates Chinese industry_en values back to English by:
1. Querying all unique industry_zh from DB
2. Mapping via a complete ZH→EN lookup table
3. Handling composite formats like '金屬製品製造業、29機械設備製造業'
4. Batch-updating only rows where industry_en still contains Chinese
5. Rebuilding the FTS5 index
6. Printing before/after statistics
"""

import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / 'data' / 'tmdb.db'

# Complete MOEA manufacturing industry ZH→EN map
INDUSTRY_ZH_TO_EN = {
    '食品製造業': 'Food Manufacturing',
    '飲料製造業': 'Beverage Manufacturing',
    '菸草製造業': 'Tobacco Manufacturing',
    '紡織業': 'Textile Manufacturing',
    '成衣及服飾品製造業': 'Apparel & Clothing Manufacturing',
    '皮革、毛皮及其製品製造業': 'Leather & Fur Products Manufacturing',
    '木竹製品製造業': 'Wood & Bamboo Products Manufacturing',
    '紙漿、紙及紙製品製造業': 'Pulp, Paper & Paper Products Manufacturing',
    '印刷及資料儲存媒體複製業': 'Printing & Reproduction of Recorded Media',
    '石油及煤製品製造業': 'Petroleum & Coal Products Manufacturing',
    '化學材料及肥料製造業': 'Chemical Materials & Fertilizers Manufacturing',
    '其他化學製品製造業': 'Other Chemical Products Manufacturing',
    '藥品及醫用化學製品製造業': 'Pharmaceuticals Manufacturing',
    '橡膠製品製造業': 'Rubber Products Manufacturing',
    '塑膠製品製造業': 'Plastics Products Manufacturing',
    '非金屬礦物製品製造業': 'Non-Metallic Mineral Products Manufacturing',
    '基本金屬製造業': 'Basic Metal Manufacturing',
    '金屬製品製造業': 'Metal Products Manufacturing',
    '電子零組件製造業': 'Electronic Components Manufacturing',
    '電腦、電子產品及光學製品製造業': 'Computer, Electronic & Optical Products Manufacturing',
    '電力設備及配備製造業': 'Electrical Equipment Manufacturing',
    '機械設備製造業': 'Machinery & Equipment Manufacturing',
    '汽車及其零件製造業': 'Motor Vehicles & Parts Manufacturing',
    '其他運輸工具及其零件製造業': 'Other Transport Equipment Manufacturing',
    '家具製造業': 'Furniture Manufacturing',
    '其他製造業': 'Other Manufacturing',
    '產業用機械設備維修及安裝業': 'Industrial Machinery Repair & Installation',
}

# Regex to detect Chinese characters
CHINESE_RE = re.compile(r'[一-龥]')

# Regex to split composite industry strings: split on '、' optionally followed by digits
SPLIT_RE = re.compile(r'、\d*')


def translate_industry(industry_zh: str) -> str | None:
    """
    Returns the English translation for an industry_zh value,
    or None if it cannot be translated (should not happen with full map).
    """
    if not industry_zh:
        return None

    # Direct lookup
    if industry_zh in INDUSTRY_ZH_TO_EN:
        return INDUSTRY_ZH_TO_EN[industry_zh]

    # Composite format: split on '、' (with optional leading digits), take first segment
    parts = SPLIT_RE.split(industry_zh)
    first = parts[0].strip()
    if first in INDUSTRY_ZH_TO_EN:
        return INDUSTRY_ZH_TO_EN[first]

    # Partial / substring match (fallback)
    for zh, en in INDUSTRY_ZH_TO_EN.items():
        if zh in industry_zh:
            return en

    # No match found
    return None


def main():
    if not DB_PATH.exists():
        print(f'ERROR: DB not found at {DB_PATH}', file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # ── Step 1: Count baseline ───────────────────────────────────────────────
    total = conn.execute('SELECT COUNT(*) FROM factories').fetchone()[0]
    before_chinese = conn.execute(
        "SELECT COUNT(*) FROM factories WHERE industry_en GLOB '*[一-龥]*'"
    ).fetchone()[0]
    print(f'Total factories  : {total:,}')
    print(f'Chinese industry_en (before): {before_chinese:,}')

    # ── Step 2: Fetch all rows that still have Chinese in industry_en ─────────
    rows = conn.execute(
        "SELECT id, industry_zh, industry_en FROM factories WHERE industry_en GLOB '*[一-龥]*'"
    ).fetchall()

    # ── Step 3: Build update list ─────────────────────────────────────────────
    updates = []          # (new_en, id)
    skipped = []          # industry_zh values we couldn't map
    skipped_counts: dict[str, int] = {}

    for row in rows:
        new_en = translate_industry(row['industry_zh'] or '')
        if new_en:
            updates.append((new_en, row['id']))
        else:
            key = row['industry_zh'] or '(null)'
            skipped_counts[key] = skipped_counts.get(key, 0) + 1

    print(f'\nMappable rows    : {len(updates):,}')
    print(f'Unmappable rows  : {len(skipped_counts):,} distinct values')

    if skipped_counts:
        print('\nUnmapped industry_zh values (distinct):')
        for zh, cnt in sorted(skipped_counts.items(), key=lambda x: -x[1]):
            print(f'  [{cnt:>5}]  {zh}')

    # ── Step 4: Batch update ──────────────────────────────────────────────────
    print(f'\nUpdating {len(updates):,} rows...')
    conn.executemany(
        'UPDATE factories SET industry_en = ? WHERE id = ?',
        updates
    )
    conn.commit()
    print('DB update committed.')

    # ── Step 5: Rebuild FTS5 index ────────────────────────────────────────────
    print('Rebuilding FTS5 index...')
    try:
        conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
        conn.commit()
        print('FTS5 index rebuilt.')
    except sqlite3.OperationalError as e:
        print(f'FTS5 rebuild skipped (non-fatal): {e}')

    # ── Step 6: Verify ────────────────────────────────────────────────────────
    after_chinese = conn.execute(
        "SELECT COUNT(*) FROM factories WHERE industry_en GLOB '*[一-龥]*'"
    ).fetchone()[0]
    print(f'\n── Verification ──────────────────────────────')
    print(f'Chinese industry_en (after) : {after_chinese:,}')
    print(f'Rows translated             : {before_chinese - after_chinese:,}')

    if after_chinese == 0:
        print('SUCCESS: No Chinese characters remain in industry_en.')
    else:
        print(f'WARNING: {after_chinese} rows still have Chinese in industry_en.')
        remaining = conn.execute(
            "SELECT DISTINCT industry_zh, industry_en, COUNT(*) as cnt "
            "FROM factories WHERE industry_en GLOB '*[一-龥]*' "
            "GROUP BY industry_zh ORDER BY cnt DESC LIMIT 20"
        ).fetchall()
        print('\nRemaining problematic rows (top 20):')
        for r in remaining:
            print(f'  [{r[2]:>5}]  industry_zh={r[0]}  ->  industry_en={r[1]}')

    conn.close()


if __name__ == '__main__':
    main()
