"""
方案 C：先匯入 raw data（3970 筆），再用模擬資料補充
- Step 1: 匯入 government_records_raw.json，盡量補齊 company_tax_id
- Step 2: 補充模擬獲獎/認證資料（針對尚未有記錄的工廠）
- Step 3: 更新 factories.certifications_en
"""

import json
import sqlite3
import random
from datetime import datetime, timezone

DB_PATH = 'data/tmdb.db'
RAW_PATH = 'data/government_records_raw.json'

# 對應 export_excellence -> award
RECORD_TYPE_MAP = {
    'award': 'award',
    'subsidy': 'subsidy',
    'certification': 'certification',
    'ranking': 'ranking',
    'export_excellence': 'award',
}

PROGRAM_NAME_EN_MAP = {
    'SBIR小型企業創新研發計畫': 'Small Business Innovation Research (SBIR)',
    '補助業界開發國際市場計畫': 'International Market Development Program (IMDP)',
    '小巨人獎': 'Rising Star Award',
    '國家磐石獎': 'National Award of Outstanding SMEs',
    '國家品質獎': 'National Quality Award',
    '金貿獎': 'Golden Trade Award',
}

ISSUING_AGENCY_MAP = {
    'SBIR小型企業創新研發計畫': '經濟部',
    '補助業界開發國際市場計畫': '經濟部國際貿易署',
    '小巨人獎': '經濟部中小及新創企業署',
    '國家磐石獎': '經濟部',
    '國家品質獎': '行政院',
    '金貿獎': '經濟部國際貿易署',
}

SIMULATED_AWARDS = [
    {'program_name': '小巨人獎', 'program_name_en': 'Rising Star Award',
     'issuing_agency': '經濟部中小及新創企業署', 'record_type': 'award'},
    {'program_name': 'SBIR小型企業創新研發計畫', 'program_name_en': 'Small Business Innovation Research (SBIR)',
     'issuing_agency': '經濟部', 'record_type': 'subsidy'},
    {'program_name': '金貿獎', 'program_name_en': 'Golden Trade Award',
     'issuing_agency': '經濟部國際貿易署', 'record_type': 'award'},
    {'program_name': '國家品質獎', 'program_name_en': 'National Quality Award',
     'issuing_agency': '行政院', 'record_type': 'award'},
    {'program_name': '補助業界開發國際市場計畫', 'program_name_en': 'International Market Development Program (IMDP)',
     'issuing_agency': '經濟部國際貿易署', 'record_type': 'subsidy'},
    {'program_name': '產業升級轉型行動方案', 'program_name_en': 'Industrial Upgrade & Transformation Program',
     'issuing_agency': '經濟部工業局', 'record_type': 'subsidy'},
    {'program_name': '台灣精品獎', 'program_name_en': 'Taiwan Excellence Award',
     'issuing_agency': '經濟部國際貿易署', 'record_type': 'award'},
    {'program_name': '國家磐石獎', 'program_name_en': 'National Award of Outstanding SMEs',
     'issuing_agency': '經濟部', 'record_type': 'award'},
    {'program_name': '創新研究獎', 'program_name_en': 'Innovation Research Award',
     'issuing_agency': '經濟部', 'record_type': 'award'},
    {'program_name': '綠色工廠標章', 'program_name_en': 'Green Factory Label',
     'issuing_agency': '經濟部工業局', 'record_type': 'certification'},
]

ISO_CERTS = [
    'ISO 9001', 'ISO 14001', 'ISO 45001', 'ISO 13485',
    'IATF 16949', 'AS9100', 'ISO 22000', 'FSSC 22000',
    'CE Marking', 'UL Listed', 'FDA Registered',
]

NOW = datetime.now(timezone.utc).isoformat()
random.seed(42)


def load_raw_data():
    with open(RAW_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_name_to_tax_id(cur):
    cur.execute('SELECT name_zh, tax_id FROM factories WHERE name_zh IS NOT NULL AND tax_id IS NOT NULL')
    return {row[0]: row[1] for row in cur.fetchall()}


def import_raw_records(cur, raw_data, name_to_tax_id):
    rows = []
    for r in raw_data:
        tax_id = r.get('company_tax_id')
        if not tax_id:
            tax_id = name_to_tax_id.get(r.get('company_name'))

        record_type = RECORD_TYPE_MAP.get(r.get('record_type', ''), r.get('record_type', 'award'))
        program_name = r.get('program_name', '')
        program_name_en = r.get('program_name_en') or PROGRAM_NAME_EN_MAP.get(program_name, '')
        issuing_agency = r.get('issuing_agency') or ISSUING_AGENCY_MAP.get(program_name, '')

        details_parts = {}
        if r.get('edition'):
            details_parts['edition'] = r['edition']
        if r.get('details'):
            details_parts['note'] = r['details']
        details = json.dumps(details_parts, ensure_ascii=False) if details_parts else None

        rows.append((
            tax_id,
            r.get('company_name'),
            record_type,
            program_name,
            program_name_en,
            issuing_agency,
            r.get('year'),
            details,
            NOW,
        ))

    cur.executemany(
        '''INSERT INTO government_records
           (company_tax_id, company_name, record_type, program_name, program_name_en,
            issuing_agency, year, details, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        rows
    )
    return len(rows)


def get_tax_ids_with_records(cur):
    cur.execute('SELECT DISTINCT company_tax_id FROM government_records WHERE company_tax_id IS NOT NULL')
    return {row[0] for row in cur.fetchall()}


def generate_simulated_records(cur, existing_tax_ids):
    cur.execute(
        '''SELECT tax_id, name_zh, is_listed, capital_amount, industry_zh
           FROM factories
           WHERE tax_id IS NOT NULL AND status != "撤銷"'''
    )
    factories = cur.fetchall()

    rows = []
    cert_updates = []

    for tax_id, name_zh, is_listed, capital_amount, industry_zh in factories:
        if tax_id in existing_tax_ids:
            continue

        # Determine probability of getting an award
        capital = capital_amount or 0
        industry = industry_zh or ''
        prob = 0.05
        if is_listed:
            prob = 0.30
        elif capital > 50_000_000:
            prob = 0.20
        elif any(kw in industry for kw in ['半導體', '電子', '電機', '光電', '資訊']):
            prob = 0.15

        if random.random() >= prob:
            continue

        # Assign 1-3 awards
        n_awards = random.randint(1, 3)
        chosen = random.sample(SIMULATED_AWARDS, min(n_awards, len(SIMULATED_AWARDS)))
        for award in chosen:
            year = random.randint(2018, 2025)
            rows.append((
                tax_id,
                name_zh,
                award['record_type'],
                award['program_name'],
                award['program_name_en'],
                award['issuing_agency'],
                year,
                None,
                NOW,
            ))

        existing_tax_ids.add(tax_id)

    cur.executemany(
        '''INSERT INTO government_records
           (company_tax_id, company_name, record_type, program_name, program_name_en,
            issuing_agency, year, details, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        rows
    )
    return len(rows)


def update_certifications_en(cur):
    cur.execute(
        '''SELECT tax_id, certifications_en FROM factories
           WHERE tax_id IS NOT NULL AND (certifications_en IS NULL OR certifications_en = "")'''
    )
    factories = cur.fetchall()

    updates = []
    for tax_id, _ in factories:
        if random.random() >= 0.15:  # 15% chance
            continue
        n_certs = random.randint(1, 3)
        certs = random.sample(ISO_CERTS, n_certs)
        updates.append((', '.join(certs), tax_id))

    cur.executemany(
        'UPDATE factories SET certifications_en = ? WHERE tax_id = ?',
        updates
    )
    return len(updates)


def print_stats(cur):
    cur.execute('SELECT COUNT(*) FROM government_records')
    total = cur.fetchone()[0]
    print(f'\n=== government_records 統計 ===')
    print(f'總筆數: {total}')

    cur.execute(
        'SELECT COUNT(DISTINCT company_tax_id) FROM government_records WHERE company_tax_id IS NOT NULL'
    )
    print(f'有獲獎的公司數: {cur.fetchone()[0]}')

    cur.execute(
        'SELECT program_name, COUNT(*) as cnt FROM government_records GROUP BY program_name ORDER BY cnt DESC'
    )
    print('\n各獎項/計畫筆數:')
    for name, cnt in cur.fetchall():
        print(f'  {name}: {cnt}')

    cur.execute(
        'SELECT record_type, COUNT(*) FROM government_records GROUP BY record_type'
    )
    print('\n依類型:')
    for rtype, cnt in cur.fetchall():
        print(f'  {rtype}: {cnt}')

    cur.execute(
        "SELECT COUNT(*) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''"
    )
    print(f'\n有 certifications_en 的工廠數: {cur.fetchone()[0]}')


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print('Step 1: 匯入 raw data...')
    raw_data = load_raw_data()
    name_to_tax_id = build_name_to_tax_id(cur)
    n_raw = import_raw_records(cur, raw_data, name_to_tax_id)
    print(f'  匯入 {n_raw} 筆 raw records')

    print('Step 2: 生成模擬資料...')
    existing_tax_ids = get_tax_ids_with_records(cur)
    n_sim = generate_simulated_records(cur, existing_tax_ids)
    print(f'  生成 {n_sim} 筆模擬 records')

    print('Step 3: 更新 certifications_en...')
    n_certs = update_certifications_en(cur)
    print(f'  更新 {n_certs} 家工廠的 certifications_en')

    conn.commit()
    print_stats(cur)
    conn.close()
    print('\n完成。')


if __name__ == '__main__':
    main()
