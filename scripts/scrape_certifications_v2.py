#!/usr/bin/env python3
"""
scrape_certifications_v2.py — 更積極地爬取認證、獎項、成立年份

改進點：
1. 嘗試多個 cert/about URL（最多 6 個）
2. 同時比對中文認證關鍵字
3. 掃描 img alt text
4. 頁面內容太短時用 StealthyFetcher 重試
5. 抓取獎項和成立年份

用法:
  python3 -m scripts.scrape_certifications_v2 --limit 1300
  python3 -m scripts.scrape_certifications_v2 --status
"""

import argparse
import json
import re
import sqlite3
import time
import urllib.parse
from pathlib import Path

try:
  from patchright.sync_api import sync_playwright
  HAS_BROWSER = True
except ImportError:
  HAS_BROWSER = False

# StealthyFetcher disabled — has known bugs and hangs; patchright is the primary fetcher
HAS_STEALTHY = False

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
PROGRESS_FILE = BASE_DIR / 'data' / 'cert_scrape_progress_v2.json'

# Strict certification patterns — require word boundary or digit pattern to avoid false positives
CERT_PATTERNS = [
  (r'\bISO\s*9001\b', 'ISO 9001'),
  (r'\bISO\s*14001\b', 'ISO 14001'),
  (r'\bISO\s*45001\b', 'ISO 45001'),
  (r'\bISO\s*13485\b', 'ISO 13485'),
  (r'\bISO\s*22000\b', 'ISO 22000'),
  (r'\bISO\s*27001\b', 'ISO 27001'),
  (r'\bISO\s*50001\b', 'ISO 50001'),
  (r'\bISO\s*22716\b', 'ISO 22716'),
  (r'\bISO\s*17025\b', 'ISO 17025'),
  (r'\bIATF\s*16949\b', 'IATF 16949'),
  (r'\bTS\s*16949\b', 'IATF 16949'),
  (r'\bAS\s*9100\b', 'AS 9100'),
  (r'\bAS9100\b', 'AS 9100'),
  (r'\bNADCAP\b', 'NADCAP'),
  (r'\bQC\s*080000\b', 'QC 080000'),
  (r'\bIECQ\s*QC\b', 'IECQ'),
  (r'\bFSSC\s*22000\b', 'FSSC 22000'),
  (r'\bOEKO[\s\-]*TEX\b', 'OEKO-TEX'),
  (r'\bbluesign[\s\-]*system', 'bluesign'),
  (r'\bCE\s*[Mm]ark', 'CE Marking'),
  (r'\bUL\s*[Ll]ist', 'UL Listed'),
  (r'\bUL\s*[Rr]ecog', 'UL Recognized'),
  (r'\bRoHS\s*[Cc]ompl', 'RoHS'),
  (r'\bHACCP\s*[Cc]ertif', 'HACCP'),
  (r'\bFDA\s*[Rr]egist', 'FDA Registered'),
  (r'\bSony\s*G[Pp]\b', 'Sony GP'),
]

# Chinese certification patterns
ZH_CERT_PATTERNS = [
  (r'品質管理.*9001|9001.*品質', 'ISO 9001'),
  (r'環境管理.*14001|14001.*環境', 'ISO 14001'),
  (r'職業安全.*45001|45001.*職業', 'ISO 45001'),
  (r'汽車.*16949|16949.*汽車|TS\s*16949', 'IATF 16949'),
  (r'醫療.*13485|13485.*醫療', 'ISO 13485'),
  (r'食品安全.*22000|22000.*食品', 'ISO 22000'),
  (r'資訊安全.*27001|27001.*資訊', 'ISO 27001'),
  (r'能源管理.*50001|50001.*能源', 'ISO 50001'),
  (r'航太.*9100|9100.*航太', 'AS 9100'),
  (r'有害物質.*080000|080000.*有害|危害物質.*QC', 'QC 080000'),
  (r'CE\s*認證|歐盟.*CE\b', 'CE Marking'),
  (r'UL\s*認證|UL\s*列名', 'UL Listed'),
  (r'HACCP.*認證|認證.*HACCP', 'HACCP'),
  (r'清真.*認證|HALAL', 'HALAL'),
  (r'GMP.*認證|認證.*GMP', 'GMP'),
  (r'FSC.*認證|認證.*FSC', 'FSC'),
  (r'ESG.*評等|永續.*評等', 'ESG Rated'),
]

# Award patterns
AWARD_PATTERNS = [
  (r'台灣精品獎|台灣精品', 'Taiwan Excellence Award'),
  (r'國家品質獎', 'National Quality Award'),
  (r'小巨人獎', 'Small Giant Award'),
  (r'創新研究獎', 'Innovation Research Award'),
  (r'磐石獎', 'Panshi Award'),
  (r'金貿獎', 'Golden Trade Award'),
  (r'Taiwan\s*Excellence', 'Taiwan Excellence Award'),
  (r'精品獎', 'Taiwan Excellence Award'),
]

# Founded year patterns
FOUNDED_PATTERNS = [
  r'[Ff]ounded\s+in\s+(\d{4})',
  r'[Ee]stablished\s+in\s+(\d{4})',
  r'[Ss]ince\s+(\d{4})',
  r'成立於\s*(\d{4})',
  r'創立於\s*(\d{4})',
  r'創業於\s*(\d{4})',
  r'founded\s*[:：]\s*(\d{4})',
  r'(\d{4})\s*年\s*成立',
  r'(\d{4})\s*年\s*創立',
  r'(\d{4})\s*年\s*創業',
]

# URL paths to try — ordered by likelihood
CERT_URLS = [
  '',           # homepage
  '/about',
  '/about-us',
  '/about/about',
  '/company',
  '/certification',
  '/certifications',
  '/certificate',
  '/certificates',
  '/quality',
  '/quality-policy',
  '/quality-system',
  '/quality-control',
  '/about/certification',
  '/about/certificate',
  '/about/quality',
  '/company/certification',
  '/company/quality',
  '/en/about',
  '/en/certification',
  '/en/quality',
  '/tw/about',
  '/tw/about/certificate',
  '/tw/tech/page',
  '/zh/about',
  '/zh/certification',
  '/sustainability',
  '/csr',
  '/esg',
  '/about/honor',
  '/honor',
  '/awards',
]

MAX_URLS_PER_COMPANY = 4
MIN_CONTENT_LEN = 1000


def normalize_url(website: str) -> str:
  if not website:
    return ''
  w = website.strip()
  if not w.startswith('http'):
    w = 'https://' + w
  return w.rstrip('/')


def is_ip_address(host: str) -> bool:
  return bool(re.match(r'^\d+\.\d+\.\d+\.\d+$', host))


def extract_from_text(text: str) -> tuple[set, set, int | None]:
  """Return (certs, awards, founded_year) extracted from page text."""
  found_certs = set()
  found_awards = set()
  found_year = None

  # English cert patterns
  for pattern, name in CERT_PATTERNS:
    if re.search(pattern, text, re.IGNORECASE):
      found_certs.add(name)

  # Chinese cert patterns
  for pattern, name in ZH_CERT_PATTERNS:
    if re.search(pattern, text, re.IGNORECASE):
      found_certs.add(name)

  # img alt text scan
  alts = re.findall(r'<img[^>]*\balt=["\']([^"\']*)["\']', text, re.IGNORECASE)
  for alt in alts:
    for pattern, name in CERT_PATTERNS:
      if re.search(pattern, alt, re.IGNORECASE):
        found_certs.add(name)
    for pattern, name in ZH_CERT_PATTERNS:
      if re.search(pattern, alt, re.IGNORECASE):
        found_certs.add(name)

  # Award patterns
  for pattern, name in AWARD_PATTERNS:
    if re.search(pattern, text, re.IGNORECASE):
      found_awards.add(name)

  # Founded year
  for pattern in FOUNDED_PATTERNS:
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
      year = int(m.group(1))
      if 1900 <= year <= 2024:
        found_year = year
        break

  return found_certs, found_awards, found_year


def fetch_with_stealthy(url: str) -> str:
  """Fallback fetch — disabled, returns empty string."""
  return ''


def scrape_site(page, website: str) -> tuple[list, list, int | None]:
  """
  Try multiple URL paths. Return (certs, awards, founded_year).
  Stops after finding certs or exhausting MAX_URLS_PER_COMPANY successful fetches.
  """
  base = normalize_url(website)
  if not base:
    return [], [], None

  host = urllib.parse.urlparse(base).hostname or ''
  if is_ip_address(host):
    return [], [], None

  all_certs: set = set()
  all_awards: set = set()
  found_year: int | None = None
  tried = 0

  for path in CERT_URLS:
    if tried >= MAX_URLS_PER_COMPANY:
      break

    url = base + path
    try:
      resp = page.goto(url, timeout=8000, wait_until='commit')
      if not resp or resp.status not in (200, 304):
        continue
      page.wait_for_timeout(100)
      html = page.content()
    except Exception:
      continue

    tried += 1

    # Fallback to StealthyFetcher if content too short
    if len(html) < MIN_CONTENT_LEN and HAS_STEALTHY:
      html = fetch_with_stealthy(url) or html

    if len(html) < 200:
      continue

    certs, awards, year = extract_from_text(html)
    all_certs |= certs
    all_awards |= awards
    if year and not found_year:
      found_year = year

    # Stop early if we already found certs
    if all_certs and tried >= 2:
      break

  return sorted(all_certs), sorted(all_awards), found_year


def get_pending(limit: int) -> list:
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


def update_db(tax_id: str, certs: list, awards: list, founded_year: int | None):
  conn = sqlite3.connect(str(DB_PATH))
  cert_str = ', '.join(certs) if certs else None
  awards_str = ', '.join(awards) if awards else None
  conn.execute(
    '''UPDATE factories
       SET certifications_en = ?,
           awards_text        = ?,
           founded_year       = ?
       WHERE tax_id = ?''',
    (cert_str, awards_str, founded_year, tax_id)
  )
  conn.commit()
  conn.close()


def ensure_columns():
  conn = sqlite3.connect(str(DB_PATH))
  for ddl in [
    'ALTER TABLE factories ADD COLUMN awards_text TEXT',
    'ALTER TABLE factories ADD COLUMN founded_year INTEGER',
  ]:
    try:
      conn.execute(ddl)
    except Exception:
      pass
  conn.commit()
  conn.close()


def show_status():
  conn = sqlite3.connect(str(DB_PATH))
  total = conn.execute(
    "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE website IS NOT NULL AND website != ''"
  ).fetchone()[0]
  has_cert = conn.execute(
    "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''"
  ).fetchone()[0]
  has_award = conn.execute(
    "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE awards_text IS NOT NULL AND awards_text != ''"
  ).fetchone()[0]
  has_year = conn.execute(
    'SELECT COUNT(DISTINCT tax_id) FROM factories WHERE founded_year IS NOT NULL'
  ).fetchone()[0]
  conn.close()

  done = 0
  if PROGRESS_FILE.exists():
    with open(PROGRESS_FILE) as f:
      p = json.load(f)
      done = len(p.get('done', []))
      print(f'Last run : {p.get("last", "?")}')
      s = p.get('stats', {})
      print(f'Found certs: {s.get("found_certs", 0)} | Found awards: {s.get("found_awards", 0)} | Found year: {s.get("found_year", 0)}')

  print(f'With website: {total} | Has certs: {has_cert} | Has awards: {has_award} | Has year: {has_year} | Scraped: {done}')


def main():
  parser = argparse.ArgumentParser(description='Aggressive certification + award scraper v2')
  parser.add_argument('--limit', type=int, default=100)
  parser.add_argument('--status', action='store_true')
  parser.add_argument('--delay', type=float, default=0.3)
  args = parser.parse_args()

  if args.status:
    show_status()
    return

  if not HAS_BROWSER:
    print('ERROR: patchright not installed')
    return

  ensure_columns()

  companies = get_pending(args.limit)
  print(f'=== Cert Scraper v2: {len(companies)} companies to process ===')
  print(f'StealthyFetcher fallback: {"enabled" if HAS_STEALTHY else "disabled"}\n')

  pw = sync_playwright().start()
  browser = pw.chromium.launch(headless=True)
  context = browser.new_context(
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    locale='zh-TW',
  )
  page = context.new_page()
  page.set_default_navigation_timeout(8000)
  page.set_default_timeout(5000)

  # Load existing progress
  done: list = []
  stats = {'found_certs': 0, 'found_awards': 0, 'found_year': 0, 'empty': 0, 'error': 0}
  if PROGRESS_FILE.exists():
    with open(PROGRESS_FILE) as f:
      p = json.load(f)
      done = p.get('done', [])
      stats = p.get('stats', stats)

  for i, co in enumerate(companies):
    tax_id = co['tax_id']
    website = co['website']
    name = (co['name_zh'] or '')[:20]

    print(f'[{i+1}/{len(companies)}] {name} ({website[:35]})', end=' ', flush=True)

    try:
      certs, awards, year = scrape_site(page, website)

      markers = []
      if certs:
        stats['found_certs'] += 1
        markers.append(f'CERTS: {", ".join(certs)}')
      if awards:
        stats['found_awards'] += 1
        markers.append(f'AWARDS: {", ".join(awards)}')
      if year:
        stats['found_year'] += 1
        markers.append(f'YEAR: {year}')

      if certs or awards or year:
        update_db(tax_id, certs, awards, year)
        print(f'-> {" | ".join(markers)}')
      else:
        stats['empty'] += 1
        print('-> nothing found')

    except Exception as e:
      stats['error'] += 1
      print(f'-> ERROR: {e}')

    done.append(tax_id)

    # Save progress every 20 companies
    if (i + 1) % 20 == 0:
      with open(PROGRESS_FILE, 'w') as f:
        json.dump({
          'done': done,
          'stats': stats,
          'last': time.strftime('%Y-%m-%d %H:%M'),
        }, f)

    time.sleep(args.delay)

  # Final progress save
  with open(PROGRESS_FILE, 'w') as f:
    json.dump({
      'done': done,
      'stats': stats,
      'last': time.strftime('%Y-%m-%d %H:%M'),
    }, f)

  browser.close()
  pw.stop()

  print(f'\n=== Results ===')
  print(f'Companies with certs  : {stats["found_certs"]}')
  print(f'Companies with awards : {stats["found_awards"]}')
  print(f'Companies with year   : {stats["found_year"]}')
  print(f'No data found         : {stats["empty"]}')
  print(f'Errors                : {stats["error"]}')

  # Final DB summary
  conn = sqlite3.connect(str(DB_PATH))
  total_certs = conn.execute(
    "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE certifications_en IS NOT NULL AND certifications_en != ''"
  ).fetchone()[0]
  total_awards = conn.execute(
    "SELECT COUNT(DISTINCT tax_id) FROM factories WHERE awards_text IS NOT NULL AND awards_text != ''"
  ).fetchone()[0]
  conn.close()
  print(f'\nDB totals — Has certs: {total_certs} | Has awards: {total_awards}')


if __name__ == '__main__':
  main()
