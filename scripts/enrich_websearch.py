#!/usr/bin/env python3
"""
enrich_websearch.py — 用 alltwcompany.com 批次抓取公司營業項目

alltwcompany.com 不需要 JS 渲染，可以用 urllib 直接抓。
包含：營業項目、資本額、地址、負責人、成立日期。

用法:
  python3 -m scripts.enrich_websearch --limit 1000
  python3 -m scripts.enrich_websearch --status
"""

import argparse
import json
import re
import sqlite3
import ssl
import time
import urllib.error
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
PROGRESS_FILE = BASE_DIR / 'data' / 'websearch_progress.json'

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'


# Business type translation map (Chinese → English)
BIZ_TYPE_EN = {
    '製造業': 'Manufacturing',
    '批發業': 'Wholesale',
    '零售業': 'Retail',
    '國際貿易業': 'International Trade',
    '進出口貿易業': 'Import/Export Trade',
    '電子資訊供應服務業': 'IT Services',
    '資訊軟體服務業': 'Software Services',
    '管理顧問業': 'Management Consulting',
    '機械設備': 'Machinery & Equipment',
    '電子零組件': 'Electronic Components',
    '半導體': 'Semiconductor',
    '金屬': 'Metal Products',
    '塑膠': 'Plastic Products',
    '食品': 'Food Products',
    '化學': 'Chemical Products',
    '紡織': 'Textile Products',
    '印刷': 'Printing',
    '電機': 'Electrical',
    '光電': 'Optoelectronics',
    '精密': 'Precision',
    '自動化': 'Automation',
    '模具': 'Molds & Tooling',
    '包裝': 'Packaging',
    '環保': 'Environmental',
    '能源': 'Energy',
    '生技': 'Biotechnology',
    '醫療': 'Medical',
    '建材': 'Construction Materials',
    '汽車': 'Automotive',
    '航太': 'Aerospace',
    '通訊': 'Telecommunications',
    '安全': 'Security',
    '儀器': 'Instruments',
    '運輸': 'Transport',
    '倉儲': 'Warehousing',
    '農產品': 'Agricultural Products',
    '水產品': 'Aquatic Products',
    '畜產品': 'Livestock Products',
    '飲料': 'Beverages',
    '烘焙': 'Bakery',
    '調味品': 'Seasonings',
    '冷凍': 'Frozen Food',
    '罐頭': 'Canned Food',
    '乳品': 'Dairy Products',
}


class CompanyPageParser(HTMLParser):
    """Parse alltwcompany.com company page to extract info."""

    def __init__(self):
        super().__init__()
        self.data = {}
        self._current_label = None
        self._in_td = False
        self._td_text = ''
        self._biz_items = []
        self._in_biz_section = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self._in_td = True
            self._td_text = ''

    def handle_endtag(self, tag):
        if tag == 'td' and self._in_td:
            self._in_td = False
            text = self._td_text.strip()
            if text:
                self._process_td(text)

    def handle_data(self, data):
        if self._in_td:
            self._td_text += data

    def _process_td(self, text):
        # Label detection
        labels = {
            '統一編號': 'tax_id',
            '公司名稱': 'name',
            '代表人': 'representative',
            '資本總額': 'capital',
            '公司所在地': 'address',
            '核准設立日期': 'setup_date',
            '變更日期': 'change_date',
            '公司狀態': 'status',
        }
        for zh, key in labels.items():
            if zh in text:
                self._current_label = key
                # If value is in same cell after colon
                if '：' in text:
                    val = text.split('：', 1)[1].strip()
                    if val:
                        self.data[key] = val
                        self._current_label = None
                return

        if '所營事業資料' in text or '營業項目' in text:
            self._in_biz_section = True
            return

        if self._current_label and text:
            self.data[self._current_label] = text
            self._current_label = None
        elif self._in_biz_section and text and len(text) > 2:
            self._biz_items.append(text)


def fetch_company_page(tax_id: str) -> dict | None:
    """Fetch company info from alltwcompany.com."""
    url = f'https://alltwcompany.com/nd-C-{tax_id}.html'
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=10, context=SSL_CTX) as resp:
            html = resp.read().decode('utf-8', errors='ignore')
    except Exception:
        return None

    parser = CompanyPageParser()
    try:
        parser.feed(html)
    except Exception:
        pass

    result = parser.data
    if parser._biz_items:
        result['business_items'] = parser._biz_items
    return result if result else None


def translate_biz_items(items: list) -> str:
    """Translate Chinese business items to English summary."""
    en_items = set()
    for item in items:
        for zh, en in BIZ_TYPE_EN.items():
            if zh in item:
                en_items.add(en)
    if en_items:
        return ', '.join(sorted(en_items))
    return ''


def get_pending(limit: int) -> list:
    """Get companies needing enrichment, ordered by importance."""
    done = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done = set(json.load(f).get('done', []))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT tax_id, name_zh
        FROM factories
        WHERE tax_id IS NOT NULL AND tax_id != ''
          AND (products_en IS NULL OR products_en = '')
        ORDER BY capital_amount DESC NULLS LAST, is_listed DESC, tax_id
    """)
    companies = []
    for row in cur.fetchall():
        if row[0] not in done:
            companies.append({'tax_id': row[0], 'name_zh': row[1]})
            if limit and len(companies) >= limit:
                break
    conn.close()
    return companies


def update_db(tax_id: str, products_en: str, capital: int | None):
    """Update factory records."""
    conn = sqlite3.connect(str(DB_PATH))
    if products_en:
        conn.execute(
            'UPDATE factories SET products_en = ? WHERE tax_id = ? AND (products_en IS NULL OR products_en = "")',
            (products_en, tax_id)
        )
    if capital:
        conn.execute(
            'UPDATE factories SET capital_amount = ? WHERE tax_id = ? AND (capital_amount IS NULL OR capital_amount = 0)',
            (capital, tax_id)
        )
    conn.commit()
    conn.close()


def parse_capital(text: str) -> int | None:
    """Parse capital string like '3,000,000元' to integer."""
    if not text:
        return None
    nums = re.sub(r'[^\d]', '', text)
    return int(nums) if nums else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--delay', type=float, default=0.5)
    args = parser.parse_args()

    if args.status:
        conn = sqlite3.connect(str(DB_PATH))
        total = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories').fetchone()[0]
        has_products = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE products_en IS NOT NULL AND products_en != ""').fetchone()[0]
        has_phone = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE phone IS NOT NULL AND phone != ""').fetchone()[0]
        conn.close()
        done = 0
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE) as f:
                done = len(json.load(f).get('done', []))
        print(f'Total: {total:,} | Products: {has_products:,} | Phone: {has_phone:,} | Searched: {done:,}')
        return

    companies = get_pending(args.limit)
    print(f'=== Enrichment Agent: {len(companies)} companies to process ===')

    done = []
    stats = {'found_products': 0, 'found_capital': 0, 'no_data': 0}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
            done = p.get('done', [])
            stats = p.get('stats', stats)

    for i, co in enumerate(companies):
        tax_id = co['tax_id']
        name = co['name_zh'][:20]
        print(f'[{i+1}/{len(companies)}] {tax_id} {name}', end=' ', flush=True)

        data = fetch_company_page(tax_id)
        if not data:
            stats['no_data'] += 1
            print('— page not found')
            done.append(tax_id)
            time.sleep(args.delay)
            continue

        # Extract products
        products_en = ''
        if 'business_items' in data:
            products_en = translate_biz_items(data['business_items'])

        # Extract capital
        capital = parse_capital(data.get('capital', ''))

        if products_en or capital:
            update_db(tax_id, products_en, capital)
            parts = []
            if products_en:
                parts.append(f'products={products_en[:40]}')
                stats['found_products'] += 1
            if capital:
                parts.append(f'cap={capital:,}')
                stats['found_capital'] += 1
            print(f'✅ {" | ".join(parts)}')
        else:
            stats['no_data'] += 1
            print('— no useful data')

        done.append(tax_id)

        if (i + 1) % 50 == 0:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)
            print(f'  [saved: {stats["found_products"]} products, {stats["found_capital"]} capitals]')

        time.sleep(args.delay)

    # Final save
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)

    print(f'\n=== Done ===')
    print(f'Products found: {stats["found_products"]}')
    print(f'Capital found: {stats["found_capital"]}')
    print(f'No data: {stats["no_data"]}')


if __name__ == '__main__':
    main()
