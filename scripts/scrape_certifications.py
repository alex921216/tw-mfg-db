#!/usr/bin/env python3
"""
scrape_certifications.py — 用 Scrapling 爬每家公司網站找 ISO 認證

爬取策略：
1. 取有 website 的公司
2. 爬首頁 + /about 相關頁面
3. 用 regex 找 ISO/認證關鍵字
4. 寫入 DB

用法:
  python3 -m scripts.scrape_certifications --limit 100
  python3 -m scripts.scrape_certifications --status
"""

import argparse
import json
import re
import sqlite3
import time
from pathlib import Path

try:
    from patchright.sync_api import sync_playwright
    HAS_BROWSER = True
except ImportError:
    HAS_BROWSER = False

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
PROGRESS_FILE = BASE_DIR / 'data' / 'cert_scrape_progress.json'

# Certification patterns to search for
CERT_PATTERNS = [
    # ISO standards
    (r'ISO\s*9001', 'ISO 9001'),
    (r'ISO\s*14001', 'ISO 14001'),
    (r'ISO\s*45001', 'ISO 45001'),
    (r'ISO\s*13485', 'ISO 13485'),
    (r'ISO\s*22000', 'ISO 22000'),
    (r'ISO\s*27001', 'ISO 27001'),
    (r'ISO\s*17025', 'ISO 17025'),
    (r'ISO\s*50001', 'ISO 50001'),
    (r'ISO\s*22716', 'ISO 22716'),
    # Automotive
    (r'IATF\s*16949', 'IATF 16949'),
    (r'TS\s*16949', 'IATF 16949'),
    # Aerospace
    (r'AS\s*9100', 'AS 9100'),
    (r'NADCAP', 'NADCAP'),
    # Safety / Product
    (r'CE\s*[Mm]ark', 'CE Marking'),
    (r'UL\s*[Ll]ist', 'UL Listed'),
    (r'UL\s*[Rr]ecog', 'UL Recognized'),
    (r'RoHS\s*[Cc]ompl', 'RoHS'),  # require "compliant" to avoid false positive
    # REACH removed — too many false positives
    # Food
    (r'HACCP\s*[Cc]ertif', 'HACCP'),  # require "certified" context
    (r'FSSC\s*22000', 'FSSC 22000'),
    # GMP removed — too many false positives
    (r'FDA\s*[Rr]egist', 'FDA Registered'),
    # Textile
    (r'OEKO[\-\s]*TEX', 'OEKO-TEX'),
    (r'bluesign', 'bluesign'),
    # WRAP removed - too many false positives (common English word)
    # Electronics
    (r'QC\s*080000', 'QC 080000'),
    (r'Sony\s*G[Pp]', 'Sony GP'),
    (r'IECQ\s*QC', 'IECQ'),  # require QC context
    # General
    # TUV removed — too many false positives
    # SGS removed — too many false positives
]

# Pages to check beyond homepage
CERT_PATHS = [
    '/about', '/about-us', '/about/certification', '/about/certifications',
    '/certification', '/certifications', '/certificate',
    '/quality', '/quality-assurance', '/quality-policy',
    '/en/about', '/en/about-us', '/en/certification',
    '/tw/about', '/tw/tech', '/tw/about/certificate',
    '/company', '/company/about',
]


def normalize_url(website: str) -> str:
    """Ensure URL has scheme."""
    if not website:
        return ''
    w = website.strip()
    if not w.startswith('http'):
        w = 'https://' + w
    return w.rstrip('/')


def scrape_site_certs(page, website: str) -> list[str]:
    """Scrape a company website for certification mentions using browser."""
    base = normalize_url(website)
    if not base:
        return []

    # Skip IP addresses
    import urllib.parse
    host = urllib.parse.urlparse(base).hostname or ''
    if re.match(r'^\d+\.\d+\.\d+\.\d+$', host):
        return []

    found_certs = set()
    pages_to_check = [base]

    # Add cert-related subpages
    for path in CERT_PATHS:
        pages_to_check.append(base + path)

    checked = 0
    for url in pages_to_check:
        if checked >= 4:  # max 4 pages per company
            break
        try:
            resp = page.goto(url, timeout=10000, wait_until='domcontentloaded')
            if not resp or resp.status != 200:
                continue
            page.wait_for_timeout(1500)
            text = page.content()
            if len(text) < 200:
                continue
            checked += 1

            for pattern, cert_name in CERT_PATTERNS:
                if re.search(pattern, text, re.IGNORECASE):
                    found_certs.add(cert_name)

            if found_certs:
                break

        except Exception:
            continue

    return sorted(found_certs)


def get_pending(limit: int) -> list:
    """Get companies with websites that need cert scraping."""
    done = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done = set(json.load(f).get('done', []))

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute("""
        SELECT tax_id, MAX(website) as website, MIN(name_zh) as name_zh
        FROM factories
        WHERE website IS NOT NULL AND website != ''
          AND (certifications_en IS NULL OR certifications_en = '')
        GROUP BY tax_id
        ORDER BY MAX(capital_amount) DESC NULLS LAST, MAX(is_listed) DESC
    """)
    companies = []
    for row in cur.fetchall():
        if row[0] not in done:
            companies.append({
                'tax_id': row[0],
                'website': row[1],
                'name_zh': row[2],
            })
            if limit and len(companies) >= limit:
                break
    conn.close()
    return companies


def update_db(tax_id: str, certs: list[str]):
    """Update certifications_en for a company."""
    if not certs:
        return
    cert_str = ', '.join(certs)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(
        'UPDATE factories SET certifications_en = ? WHERE tax_id = ?',
        (cert_str, tax_id)
    )
    conn.commit()
    conn.close()


def show_status():
    conn = sqlite3.connect(str(DB_PATH))
    total = conn.execute("SELECT COUNT(DISTINCT tax_id) FROM factories WHERE website IS NOT NULL AND website != ''").fetchone()[0]
    has_cert = conn.execute("SELECT COUNT(DISTINCT tax_id) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''").fetchone()[0]
    conn.close()
    done = 0
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
            done = len(p.get('done', []))
            print(f"Last: {p.get('last', '?')} | Found certs: {p.get('stats', {}).get('found', 0)}")
    print(f'With website: {total} | Has certs: {has_cert} | Scraped: {done}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--delay', type=float, default=1.0)
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if not HAS_BROWSER:
        print('ERROR: patchright not installed. Run: pip install patchright')
        return

    companies = get_pending(args.limit)
    print(f'=== Cert Scraper: {len(companies)} companies to check ===\n')

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()

    done = []
    stats = {'found': 0, 'empty': 0, 'error': 0}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
            done = p.get('done', [])
            stats = p.get('stats', stats)

    for i, co in enumerate(companies):
        tax_id = co['tax_id']
        website = co['website']
        name = (co['name_zh'] or '')[:20]

        print(f'[{i+1}/{len(companies)}] {name} ({website[:30]})', end=' ', flush=True)

        try:
            certs = scrape_site_certs(page, website)
            if certs:
                update_db(tax_id, certs)
                stats['found'] += 1
                print(f'-> {", ".join(certs)}')
            else:
                stats['empty'] += 1
                print('-> no certs found')
        except Exception as e:
            stats['error'] += 1
            print(f'-> ERROR: {e}')

        done.append(tax_id)

        if (i + 1) % 20 == 0:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)

        time.sleep(args.delay)

    with open(PROGRESS_FILE, 'w') as f:
        json.dump({'done': done, 'stats': stats, 'last': time.strftime('%Y-%m-%d %H:%M')}, f)

    browser.close()
    pw.stop()

    print(f'\n=== Results ===')
    print(f'Found certs: {stats["found"]}')
    print(f'No certs: {stats["empty"]}')
    print(f'Errors: {stats["error"]}')


if __name__ == '__main__':
    main()
