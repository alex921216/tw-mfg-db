#!/usr/bin/env python3
"""
enrich_agent.py — 自動爬蟲 Agent，用 Google 搜尋補齊公司電話和網站

持續運行，每處理一家公司就更新 DB。支援斷點續傳。

用法:
  python3 -m scripts.enrich_agent                # 持續運行
  python3 -m scripts.enrich_agent --limit 100    # 只跑 100 家
  python3 -m scripts.enrich_agent --status       # 看進度
"""

import argparse
import json
import re
import sqlite3
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
BROWSE = Path.home() / '.claude' / 'skills' / 'gstack' / 'browse' / 'dist' / 'browse'
PROGRESS_FILE = BASE_DIR / 'data' / 'enrich_progress.json'

# Phone pattern for Taiwan numbers
TW_PHONE_RE = re.compile(
    r'(?:(?:\+886|886)[\s\-]?|0)'           # country code or leading 0
    r'(?:'
    r'[2-9]\d?[\s\-]?\d{3,4}[\s\-]?\d{3,4}'  # landline
    r'|9\d{2}[\s\-]?\d{3}[\s\-]?\d{3}'        # mobile
    r')'
)

# Website pattern
WEBSITE_RE = re.compile(
    r'(?:https?://)?(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*(?:\.[a-zA-Z0-9][-a-zA-Z0-9]*)+)'
    r'(?:/[^\s<>"\']*)?',
    re.IGNORECASE
)

# Exclude common non-company websites
WEBSITE_EXCLUDE = {
    'google.com', 'facebook.com', 'youtube.com', 'wikipedia.org',
    'twitter.com', 'instagram.com', 'linkedin.com', 'yahoo.com',
    'gov.tw', 'pchome.com.tw', 'ruten.com.tw', 'shopee.tw',
    'momo.com', 'books.com.tw', '104.com.tw', '1111.com.tw',
    'pixnet.net', 'xuite.net', 'blogspot.com', 'medium.com',
    'github.com', 'stackoverflow.com', 'apple.com', 'microsoft.com',
    'twse.com.tw', 'tpex.org.tw', 'mops.twse.com.tw',
    'data.gov.tw', 'findbiz.nat.gov.tw', 'gcis.nat.gov.tw',
    'zeabur.app', 'vercel.app',
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
EMAIL_EXCLUDE = {'example.com', 'test.com', 'email.com'}


def browse_cmd(cmd: str, timeout: int = 15) -> str:
    """Run a browse daemon command and return output."""
    try:
        result = subprocess.run(
            [str(BROWSE)] + cmd.split(' ', 1)[0:1] + (cmd.split(' ', 1)[1:] if ' ' in cmd else []),
            capture_output=True, text=True, timeout=timeout
        )
        return result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return ''
    except Exception as e:
        return f'ERROR: {e}'


def search_company(name_zh: str, tax_id: str) -> dict:
    """Search Google for a company and extract phone, website, email."""
    result = {'phone': None, 'website': None, 'email': None}

    # Search Google
    query = f'{name_zh} 電話 網站'
    search_url = f'https://www.google.com/search?q={query}&hl=zh-TW'

    output = browse_cmd(f'goto {search_url}')
    if 'ERROR' in output or 'Navigated' not in output:
        time.sleep(2)
        return result

    time.sleep(1.5)  # Wait for page load

    # Get page text
    text_output = browse_cmd('text')
    if not text_output or 'UNTRUSTED' not in text_output:
        return result

    # Extract content between markers
    content = text_output

    # Extract phone
    phones = TW_PHONE_RE.findall(content)
    if phones:
        # Clean and take first valid one
        for phone in phones:
            clean = re.sub(r'[\s]', '', phone)
            if len(clean) >= 8:  # Minimum valid length
                # Format nicely
                result['phone'] = clean
                break

    # Extract website
    websites = WEBSITE_RE.findall(content)
    for site in websites:
        domain = site.lower().split('/')[0]
        # Skip excluded domains
        if any(excl in domain for excl in WEBSITE_EXCLUDE):
            continue
        # Prefer .com.tw domains (Taiwan companies)
        if '.com.tw' in domain or '.tw' in domain:
            result['website'] = f'https://www.{site}' if not site.startswith('http') else site
            break
    # If no .tw domain found, take first non-excluded
    if not result['website']:
        for site in websites:
            domain = site.lower().split('/')[0]
            if not any(excl in domain for excl in WEBSITE_EXCLUDE):
                result['website'] = f'https://{site}' if not site.startswith('http') else site
                break

    # Extract email
    emails = EMAIL_RE.findall(content)
    for email in emails:
        domain = email.split('@')[1].lower()
        if not any(excl in domain for excl in EMAIL_EXCLUDE):
            result['email'] = email
            break

    return result


def get_pending_companies(limit: int = 0) -> list:
    """Get companies that need enrichment."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load progress
    done_ids = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
            done_ids = set(progress.get('done', []))

    query = """
        SELECT DISTINCT tax_id, name_zh
        FROM factories
        WHERE (phone IS NULL OR phone = '')
          AND (website IS NULL OR website = '')
          AND tax_id IS NOT NULL AND tax_id != ''
        ORDER BY
            is_listed DESC,
            capital_amount DESC NULLS LAST,
            tax_id
    """
    if limit:
        query += f' LIMIT {limit + len(done_ids)}'

    cur = conn.cursor()
    cur.execute(query)
    companies = []
    for row in cur.fetchall():
        if row['tax_id'] not in done_ids:
            companies.append({
                'tax_id': row['tax_id'],
                'name_zh': row['name_zh'],
            })
    conn.close()

    if limit:
        companies = companies[:limit]
    return companies


def update_company(tax_id: str, data: dict) -> int:
    """Update company in DB. Returns number of rows affected."""
    if not any(data.values()):
        return 0

    conn = sqlite3.connect(str(DB_PATH))
    sets = []
    params = []
    for field in ('phone', 'website', 'email'):
        if data.get(field):
            sets.append(f'{field} = ?')
            params.append(data[field])

    if not sets:
        conn.close()
        return 0

    params.append(tax_id)
    sql = f"UPDATE factories SET {', '.join(sets)} WHERE tax_id = ?"
    cur = conn.cursor()
    cur.execute(sql, params)
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


def save_progress(done_ids: list, stats: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            'done': done_ids,
            'stats': stats,
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
        }, f)


def show_status():
    """Show enrichment progress."""
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    total = cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories').fetchone()[0]
    has_phone = cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE phone IS NOT NULL AND phone != ""').fetchone()[0]
    has_website = cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE website IS NOT NULL AND website != ""').fetchone()[0]
    has_email = cur.execute('SELECT COUNT(DISTINCT tax_id) FROM factories WHERE email IS NOT NULL AND email != ""').fetchone()[0]
    conn.close()

    done_count = 0
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
            done_count = len(progress.get('done', []))
            stats = progress.get('stats', {})
            print(f"Last run: {progress.get('last_updated', 'unknown')}")
            print(f"Found phone: {stats.get('found_phone', 0)}")
            print(f"Found website: {stats.get('found_website', 0)}")
            print(f"Found email: {stats.get('found_email', 0)}")

    print(f"\n=== DB Status ===")
    print(f"Total companies:  {total:,}")
    print(f"Has phone:        {has_phone:,} ({has_phone/total*100:.1f}%)")
    print(f"Has website:      {has_website:,} ({has_website/total*100:.1f}%)")
    print(f"Has email:        {has_email:,} ({has_email/total*100:.1f}%)")
    print(f"Searched so far:  {done_count:,}")
    print(f"Remaining:        {total - has_phone - done_count:,}")


def main():
    parser = argparse.ArgumentParser(description='Enrich company data via Google search')
    parser.add_argument('--limit', type=int, default=0, help='Max companies to process (0=unlimited)')
    parser.add_argument('--status', action='store_true', help='Show progress')
    parser.add_argument('--delay', type=float, default=3.0, help='Seconds between searches')
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    # Check browse daemon
    if not BROWSE.exists():
        print(f'ERROR: Browse daemon not found at {BROWSE}')
        return

    # Load previous progress
    done_ids = []
    stats = {'found_phone': 0, 'found_website': 0, 'found_email': 0, 'searched': 0, 'no_result': 0}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
            done_ids = progress.get('done', [])
            stats = progress.get('stats', stats)

    companies = get_pending_companies(args.limit)
    total = len(companies)
    print(f'=== Enrich Agent Started ===')
    print(f'Companies to search: {total:,}')
    print(f'Already searched: {len(done_ids):,}')
    print(f'Delay: {args.delay}s between searches')
    print()

    # Initialize browse daemon
    print('Starting browse daemon...')
    browse_cmd('status')
    time.sleep(2)

    for i, company in enumerate(companies):
        tax_id = company['tax_id']
        name_zh = company['name_zh']

        print(f'[{i+1}/{total}] {tax_id} {name_zh[:20]}...', end=' ', flush=True)

        try:
            data = search_company(name_zh, tax_id)

            if any(data.values()):
                rows = update_company(tax_id, data)
                found = []
                if data['phone']:
                    found.append(f'📞 {data["phone"]}')
                    stats['found_phone'] += 1
                if data['website']:
                    found.append(f'🌐 {data["website"][:30]}')
                    stats['found_website'] += 1
                if data['email']:
                    found.append(f'📧 {data["email"]}')
                    stats['found_email'] += 1
                print(f'✅ {" | ".join(found)} ({rows} rows)')
            else:
                stats['no_result'] += 1
                print('— no results')

        except Exception as e:
            print(f'❌ {e}')

        stats['searched'] += 1
        done_ids.append(tax_id)

        # Save progress every 10 companies
        if (i + 1) % 10 == 0:
            save_progress(done_ids, stats)
            print(f'  [Progress saved: {stats["found_phone"]} phones, {stats["found_website"]} websites found]')

        time.sleep(args.delay)

    # Final save
    save_progress(done_ids, stats)
    print(f'\n=== Done ===')
    print(f'Searched: {stats["searched"]}')
    print(f'Found phone: {stats["found_phone"]}')
    print(f'Found website: {stats["found_website"]}')
    print(f'Found email: {stats["found_email"]}')
    print(f'No result: {stats["no_result"]}')


if __name__ == '__main__':
    main()
