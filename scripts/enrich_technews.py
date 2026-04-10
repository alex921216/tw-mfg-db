#!/usr/bin/env python3
"""
enrich_technews.py — 從 info.technews.tw 批次抓取公司詳細資訊

此網站包含：電話、網站、英文地址、產品描述、英文公司名。
支援斷點續傳。

用法:
  python3 -m scripts.enrich_technews --limit 1000
  python3 -m scripts.enrich_technews --status
"""

import argparse
import json
import re
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
PROGRESS_FILE = BASE_DIR / 'data' / 'technews_progress.json'

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

# Phone regex for Taiwan landlines only (must start with area code pattern)
# 02-XXXX-XXXX, 03-XXX-XXXX, 04-XXXX-XXXX, etc.
TW_PHONE = re.compile(r'(?<!\d)0[2-9][\-\s]?\d{3,4}[\-\s]?\d{3,4}(?!\d)')


class TechNewsParser(HTMLParser):
    """Parse info.technews.tw company page."""

    def __init__(self):
        super().__init__()
        self.data = {}
        self._in_td = False
        self._td_text = ''
        self._current_key = None
        self._all_text = []

    def handle_starttag(self, tag, attrs):
        if tag in ('td', 'th', 'span', 'div', 'p', 'li', 'dd', 'dt', 'a'):
            self._in_td = True
            self._td_text = ''
            # Check for links
            if tag == 'a':
                for k, v in attrs:
                    if k == 'href' and v and ('http' in v or 'www' in v):
                        if 'technews' not in v and 'google' not in v:
                            self.data.setdefault('_links', []).append(v)

    def handle_endtag(self, tag):
        if tag in ('td', 'th', 'span', 'div', 'p', 'li', 'dd', 'dt', 'a'):
            self._in_td = False
            text = self._td_text.strip()
            if text:
                self._all_text.append(text)

    def handle_data(self, data):
        if self._in_td:
            self._td_text += data

    def extract_info(self) -> dict:
        """Extract structured info from collected text."""
        full_text = '\n'.join(self._all_text)
        result = {}

        # Phone — must contain a dash or space (to distinguish from tax IDs)
        phones = TW_PHONE.findall(full_text)
        for phone in phones:
            clean = re.sub(r'\s', '', phone)
            # Must have dash separator to be a real phone number
            if '-' in phone and 9 <= len(clean) <= 13:
                result['phone'] = clean
                break

        # Website - look for company domains, skip non-company sites
        skip_domains = ['technews', 'google', 'judicial', 'judgment', 'facebook',
                       'youtube', 'wikipedia', 'linkedin', 'twitter', 'instagram',
                       'pchome', '104.com', '1111.com', 'shopee', 'momo.com',
                       'gov.tw', 'twse.com', 'tpex.org', 'mops.twse']
        for link in self.data.get('_links', []):
            if not any(skip in link.lower() for skip in skip_domains):
                result['website'] = link
                break

        # Look for specific patterns in text
        for text in self._all_text:
            # Capital
            if '資本' in text and ('元' in text or 'TWD' in text):
                nums = re.sub(r'[^\d]', '', text.split('元')[0].split(':')[-1].split('：')[-1])
                if nums and len(nums) > 3:
                    result['capital'] = int(nums)

            # English name
            if re.match(r'^[A-Z][a-zA-Z\s,\.&]+(?:Co\.|Ltd\.|Corp\.|Inc\.)', text):
                result['name_en'] = text.strip()

            # Business description (Chinese)
            if '製造' in text and '銷售' in text and len(text) > 10:
                result['biz_zh'] = text

            # Products
            if any(kw in text for kw in ['產品', '主要', '項目']) and len(text) > 5:
                result.setdefault('products_zh', []).append(text)

        return result


def fetch_company(tax_id: str, name_zh: str) -> dict | None:
    """Fetch company page from info.technews.tw."""
    # URL format: /company/{tax_id}-{name}
    encoded_name = urllib.parse.quote(name_zh)
    url = f'https://info.technews.tw/company/{tax_id}-{encoded_name}'

    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=15, context=SSL_CTX) as resp:
            html = resp.read().decode('utf-8', errors='ignore')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Try without name
            url2 = f'https://info.technews.tw/company/{tax_id}'
            req2 = urllib.request.Request(url2, headers={'User-Agent': USER_AGENT})
            try:
                with urllib.request.urlopen(req2, timeout=15, context=SSL_CTX) as resp:
                    html = resp.read().decode('utf-8', errors='ignore')
            except Exception:
                return None
        else:
            return None
    except Exception:
        return None

    parser = TechNewsParser()
    try:
        parser.feed(html)
    except Exception:
        pass

    return parser.extract_info()


def translate_biz_description(biz_zh: str) -> str:
    """Simple rule-based translation of business description."""
    translations = {
        '製造': 'manufacturing', '銷售': 'sales', '設計': 'design',
        '研發': 'R&D', '加工': 'processing', '組裝': 'assembly',
        '進出口': 'import/export', '批發': 'wholesale', '零售': 'retail',
        '技術服務': 'technical services', '維修': 'maintenance',
        '安裝': 'installation', '測試': 'testing', '封裝': 'packaging',
        '積體電路': 'integrated circuits', '半導體': 'semiconductors',
        '電子': 'electronics', '光電': 'optoelectronics',
        '機械': 'machinery', '金屬': 'metal', '塑膠': 'plastic',
        '化學': 'chemical', '食品': 'food', '紡織': 'textile',
    }
    result = biz_zh
    for zh, en in translations.items():
        result = result.replace(zh, en)
    return result


def get_pending(limit: int) -> list:
    """Get companies needing enrichment."""
    done = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done = set(json.load(f).get('done', []))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    # Prioritize: companies without phone or website
    cur.execute("""
        SELECT DISTINCT tax_id, name_zh
        FROM factories
        WHERE tax_id IS NOT NULL AND tax_id != ''
          AND (phone IS NULL OR phone = '' OR website IS NULL OR website = '')
        ORDER BY
            capital_amount DESC NULLS LAST,
            is_listed DESC,
            tax_id
    """)
    companies = []
    for row in cur.fetchall():
        if row[0] not in done:
            companies.append({'tax_id': row[0], 'name_zh': row[1]})
            if limit and len(companies) >= limit:
                break
    conn.close()
    return companies


def update_db(tax_id: str, info: dict):
    """Update factory records with found info."""
    conn = sqlite3.connect(str(DB_PATH))
    updates = []
    params = []

    if info.get('phone'):
        updates.append('phone = COALESCE(phone, ?)')
        params.append(info['phone'])
    if info.get('website'):
        updates.append('website = COALESCE(website, ?)')
        params.append(info['website'])
    if info.get('name_en'):
        updates.append('official_name_en = COALESCE(official_name_en, ?)')
        params.append(info['name_en'])
    if info.get('capital'):
        updates.append('capital_amount = COALESCE(capital_amount, ?)')
        params.append(info['capital'])

    if updates:
        params.append(tax_id)
        sql = f"UPDATE factories SET {', '.join(updates)} WHERE tax_id = ?"
        conn.execute(sql, params)
        conn.commit()

    conn.close()


def show_status():
    conn = sqlite3.connect(str(DB_PATH))
    total = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories').fetchone()[0]
    has_phone = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE phone IS NOT NULL AND phone != ""').fetchone()[0]
    has_website = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE website IS NOT NULL AND website != ""').fetchone()[0]
    has_capital = conn.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE capital_amount IS NOT NULL AND capital_amount > 0').fetchone()[0]
    conn.close()

    done = 0
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
            done = len(p.get('done', []))
            stats = p.get('stats', {})
            print(f"Last: {p.get('last', '?')} | Phones: {stats.get('phone', 0)} | Websites: {stats.get('website', 0)}")

    print(f'Total: {total:,} | Phone: {has_phone:,} ({has_phone/total*100:.1f}%) | Website: {has_website:,} ({has_website/total*100:.1f}%) | Capital: {has_capital:,} | Searched: {done:,}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--delay', type=float, default=1.0)
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    companies = get_pending(args.limit)
    print(f'=== TechNews Enrichment: {len(companies)} companies ===\n')

    done = []
    stats = {'phone': 0, 'website': 0, 'name_en': 0, 'capital': 0, 'no_data': 0}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
            done = p.get('done', [])
            stats = p.get('stats', stats)

    for i, co in enumerate(companies):
        tax_id = co['tax_id']
        name = co['name_zh'] or ''
        # Clean name: remove factory suffix for search
        clean_name = re.sub(r'(第[一二三四五六七八九十\d]+廠|[一二三四五六七八九十\d]+廠|.{2,3}廠|工廠|總廠|分廠|分公司.*)$', '', name).strip()

        print(f'[{i+1}/{len(companies)}] {tax_id} {clean_name[:25]}', end=' ', flush=True)

        info = fetch_company(tax_id, clean_name)
        if info and any(info.get(k) for k in ('phone', 'website', 'name_en', 'capital')):
            update_db(tax_id, info)
            parts = []
            if info.get('phone'):
                parts.append(f'📞{info["phone"]}')
                stats['phone'] += 1
            if info.get('website'):
                parts.append(f'🌐{info["website"][:25]}')
                stats['website'] += 1
            if info.get('name_en'):
                parts.append(f'🏢{info["name_en"][:25]}')
                stats['name_en'] += 1
            if info.get('capital'):
                stats['capital'] += 1
            print(f'✅ {" ".join(parts)}')
        else:
            stats['no_data'] += 1
            print('—')

        done.append(tax_id)

        if (i + 1) % 50 == 0:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)
            total_found = stats['phone'] + stats['website']
            print(f'  💾 saved | 📞{stats["phone"]} 🌐{stats["website"]} found so far')

        time.sleep(args.delay)

    with open(PROGRESS_FILE, 'w') as f:
        json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)

    print(f'\n=== Results ===')
    print(f'📞 Phones: {stats["phone"]}')
    print(f'🌐 Websites: {stats["website"]}')
    print(f'🏢 English names: {stats["name_en"]}')
    print(f'💰 Capitals: {stats["capital"]}')
    print(f'❌ No data: {stats["no_data"]}')


if __name__ == '__main__':
    main()
