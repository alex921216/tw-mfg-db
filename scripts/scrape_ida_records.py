"""
scrape_ida_records.py — 爬取政府獎項及認證紀錄

資料來源：
  1. 小巨人獎（已驗證，387 筆）：經濟部中小及新創企業署 CSV
  2. SBIR 小型企業創新研發計畫：經濟部中小及新創企業署 CSV（含統一編號）
  3. 國家磐石獎：經濟部中小及新創企業署 CSV（352 筆）
  4. 國家品質獎：經濟部產業發展署 CSV（226 筆）
  5. 金貿獎：經濟部國際貿易署 CSV（94 筆）
  7. MEET TAIWAN IMDP 補助業界開發國際市場計畫：歷屆受補助廠商（105～114 年度）

輸出：src/data/government_records_raw.json
"""

import csv
import glob
import io
import json
import logging
import re
import ssl
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# 路徑設定
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'

# ---------------------------------------------------------------------------
# SSL 設定（bypass 政府網站常見的憑證問題）
# ---------------------------------------------------------------------------

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# ---------------------------------------------------------------------------
# 常數
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 30  # seconds

SMALL_GIANT_URL = (
    'https://www.sme.gov.tw/files/4537/C6012AF2-2BAC-4FC9-9C78-5558F65B46E5'
)

SBIR_URLS = [
    'https://www.sme.gov.tw/files/4527/43D5CB54-3601-48D5-9146-89480402E46C',
    'https://www.sme.gov.tw/files/4527/B1EEF760-EC4B-4A4D-AF16-A399A8CD4830',
    'https://www.sme.gov.tw/files/4527/DF85E1EB-58A6-4B90-A816-7C394E018705',
    'https://www.sme.gov.tw/files/4527/39DBD6FB-5CCD-4647-A7E5-15C7179BC7D8',
    'https://www.sme.gov.tw/files/4527/A3D740C8-0884-4754-B403-FE9DD69EF9D2',
    'https://www.sme.gov.tw/files/4527/12BEFFA3-08FC-4A6E-9B53-DC0EC7E53B88',
    'https://www.sme.gov.tw/files/4527/14E8D1B9-109C-44DE-94E0-7EA7BFEE8750',
    'https://www.sme.gov.tw/files/4527/16F1A429-A7C9-47CE-849D-1BDB7E53D72E',
    'https://www.sme.gov.tw/files/4527/E28EDC4A-7E7B-4432-8097-2A1ED5BF64B0',
    'https://www.sme.gov.tw/files/4527/983FEAA7-52ED-4ECC-AD77-D8CBFF20D781',
]

SBIR_TMP_DIR = Path('/tmp')

PANSHI_LOCAL = Path('/tmp/panshi.csv')
PANSHI_URL = 'https://www.sme.gov.tw/files/4537/976200EE-81C2-49FC-829E-0E1B2E54E65C'

QUALITY_LOCAL = Path('/tmp/quality.csv')
QUALITY_URL = 'https://www.ida.gov.tw/opendata/03/8288.csv'

GOLDEN_TRADE_LOCAL = Path('/tmp/golden_trade.csv')
GOLDEN_TRADE_URL = 'https://www.trade.gov.tw/OpenData/getOpenData.aspx?oid=445C207751A817E0'

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 小巨人獎
# ---------------------------------------------------------------------------

def fetch_small_giant() -> list[dict[str, Any]]:
    """
    下載小巨人獎 CSV 並解析為標準 government_records 格式。

    CSV 欄位：序號, 屆別, 公司名稱, 電話, 地址
    """
    log.info('Fetching 小巨人獎 from %s', SMALL_GIANT_URL)

    req = urllib.request.Request(
        SMALL_GIANT_URL,
        headers={'User-Agent': 'tw-mfg-db/1.0'},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
        raw = resp.read().decode('utf-8-sig')  # 處理 BOM

    reader = csv.DictReader(io.StringIO(raw))
    records: list[dict[str, Any]] = []

    for row in reader:
        edition_str = row.get('屆別', '').strip()
        edition = int(edition_str) if edition_str.isdigit() else None

        records.append({
            'company_name': row.get('公司名稱', '').strip(),
            'company_tax_id': None,
            'record_type': 'award',
            'program_name': '小巨人獎',
            'program_name_en': 'Small Giant Award',
            'issuing_agency': '經濟部中小及新創企業署',
            'year': None,
            'edition': edition,
            'details': f'第{edition_str}屆小巨人獎得主' if edition_str else '小巨人獎得主',
            'address': row.get('地址', '').strip(),
            'phone': row.get('電話', '').strip(),
        })

    log.info('Parsed %d records from 小巨人獎', len(records))
    return records


# ---------------------------------------------------------------------------
# SBIR 小型企業創新研發計畫
# ---------------------------------------------------------------------------

def _download_sbir_csvs() -> None:
    """從線上下載 SBIR CSV 檔案到 /tmp/sbir_1.csv ~ sbir_10.csv。失敗的檔案跳過。"""
    for idx, url in enumerate(SBIR_URLS, start=1):
        dest = SBIR_TMP_DIR / f'sbir_{idx}.csv'
        log.info('Downloading SBIR file %d/%d → %s', idx, len(SBIR_URLS), dest)
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'tw-mfg-db/1.0'})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
                data = resp.read()
            dest.write_bytes(data)
            log.info('  Downloaded %d bytes', len(data))
        except Exception as exc:
            log.warning('  Failed to download %s: %s — skipping', url, exc)


def _parse_sbir_csv(path: Path) -> list[dict[str, Any]]:
    """解析單一 SBIR CSV 檔案，回傳標準化紀錄列表。"""
    try:
        raw = path.read_bytes().decode('utf-8-sig')  # 處理 BOM
    except Exception as exc:
        log.warning('Cannot read %s: %s', path, exc)
        return []

    reader = csv.DictReader(io.StringIO(raw))
    records: list[dict[str, Any]] = []

    for row in reader:
        company_name = (row.get('獎補助對象') or '').strip()
        tax_id = (row.get('統一編號') or '').strip()
        details = (row.get('獎補助事項') or '').strip()
        approve_date = (row.get('核准日期') or '').strip()
        amount_raw = (row.get('補助金額(元)') or '').strip()

        if not company_name:
            continue

        # 年份：取核准日期前 4 位
        year: int | None = None
        if approve_date and len(approve_date) >= 4 and approve_date[:4].isdigit():
            year = int(approve_date[:4])

        # 補助金額
        try:
            subsidy_amount = int(amount_raw.replace(',', '')) if amount_raw and amount_raw != '-' else None
        except ValueError:
            subsidy_amount = None

        records.append({
            'company_name': company_name,
            'company_tax_id': tax_id if tax_id else None,
            'record_type': 'subsidy',
            'program_name': 'SBIR小型企業創新研發計畫',
            'program_name_en': 'Small Business Innovation Research (SBIR)',
            'issuing_agency': '經濟部中小及新創企業署',
            'year': year,
            'details': details,
            'subsidy_amount': subsidy_amount,
        })

    return records


def fetch_sbir_records() -> list[dict[str, Any]]:
    """
    下載並解析所有 SBIR CSV 檔案，合併去重後回傳標準化紀錄列表。

    去重邏輯：同一統一編號 + 同一計畫名稱（details）只保留一筆。
    """
    # 先嘗試重新下載
    _download_sbir_csvs()

    # 讀取所有 /tmp/sbir_*.csv
    csv_paths = sorted(SBIR_TMP_DIR.glob('sbir_*.csv'))
    if not csv_paths:
        log.warning('No SBIR CSV files found in %s', SBIR_TMP_DIR)
        return []

    log.info('Parsing %d SBIR CSV files...', len(csv_paths))
    all_records: list[dict[str, Any]] = []
    for path in csv_paths:
        recs = _parse_sbir_csv(path)
        log.info('  %s → %d records', path.name, len(recs))
        all_records.extend(recs)

    # 去重：以 (統一編號 + 計畫名稱) 為 key；無統一編號則以 (公司名 + 計畫名稱) 為 key
    seen: set[tuple] = set()
    deduped: list[dict[str, Any]] = []
    for rec in all_records:
        tax_id = rec.get('company_tax_id') or ''
        company = rec.get('company_name') or ''
        details = rec.get('details') or ''
        key = (tax_id, details) if tax_id else (company, details)
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    log.info('SBIR: %d raw → %d after dedup', len(all_records), len(deduped))
    return deduped


# ---------------------------------------------------------------------------
# 台灣精品獎（嘗試一次，失敗跳過）
# ---------------------------------------------------------------------------

def fetch_taiwan_excellence() -> list[dict[str, Any]]:
    """
    嘗試從台灣精品獎網站取得得獎廠商資料。
    僅嘗試一次，失敗則回傳空列表。
    """
    url = 'https://www.taiwanexcellence.org/en/award/winners'
    log.info('Trying 台灣精品獎 from %s', url)

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'tw-mfg-db/1.0'},
        )
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
            content = resp.read().decode('utf-8', errors='replace')

        # 若頁面無可解析的結構化資料，僅回傳空列表
        # 實際解析需視頁面結構而定，此處保守處理
        log.info('台灣精品獎 page fetched (%d bytes), no structured data parser implemented — skipping', len(content))
        return []

    except Exception as exc:
        log.warning('台灣精品獎 fetch failed: %s — skipping', exc)
        return []


# ---------------------------------------------------------------------------
# 國家磐石獎
# ---------------------------------------------------------------------------

def fetch_panshi_records() -> list[dict[str, Any]]:
    """
    讀取或下載國家磐石獎 CSV，解析為標準 government_records 格式。

    CSV 欄位：屆別, 公司名稱, 網址, 地址, 電話
    """
    if PANSHI_LOCAL.exists():
        log.info('讀取本地磐石獎 CSV：%s', PANSHI_LOCAL)
        raw = PANSHI_LOCAL.read_bytes().decode('utf-8-sig')
    else:
        log.info('下載磐石獎 CSV from %s', PANSHI_URL)
        req = urllib.request.Request(PANSHI_URL, headers={'User-Agent': 'tw-mfg-db/1.0'})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
            raw = resp.read().decode('utf-8-sig')

    reader = csv.DictReader(io.StringIO(raw))
    records: list[dict[str, Any]] = []

    for row in reader:
        edition_str = row.get('屆別', '').strip()
        edition = int(edition_str) if edition_str.isdigit() else None

        records.append({
            'company_name': row.get('公司名稱', '').strip(),
            'company_tax_id': None,
            'record_type': 'award',
            'program_name': '國家磐石獎',
            'program_name_en': 'National Cornerstone Award',
            'issuing_agency': '經濟部中小及新創企業署',
            'year': None,
            'edition': edition,
            'details': f'第{edition_str}屆國家磐石獎得主' if edition_str else '國家磐石獎得主',
            'address': row.get('地址', '').strip(),
            'phone': row.get('電話', '').strip(),
        })

    log.info('Parsed %d records from 國家磐石獎', len(records))
    return records


# ---------------------------------------------------------------------------
# 國家品質獎
# ---------------------------------------------------------------------------

def fetch_quality_records() -> list[dict[str, Any]]:
    """
    讀取或下載國家品質獎 CSV，解析為標準 government_records 格式。

    CSV 欄位：序號, 年度, 屆數, 名單類別, 公司名稱
    """
    if QUALITY_LOCAL.exists():
        log.info('讀取本地品質獎 CSV：%s', QUALITY_LOCAL)
        raw = QUALITY_LOCAL.read_bytes().decode('utf-8-sig')
    else:
        log.info('下載品質獎 CSV from %s', QUALITY_URL)
        req = urllib.request.Request(QUALITY_URL, headers={'User-Agent': 'tw-mfg-db/1.0'})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
            raw = resp.read().decode('utf-8-sig')

    reader = csv.DictReader(io.StringIO(raw))
    records: list[dict[str, Any]] = []

    for row in reader:
        year_str = row.get('年度', '').strip()
        # 年度欄位可能為民國年（如 79），轉為西元年
        year: int | None = None
        if year_str.isdigit():
            y = int(year_str)
            year = y + 1911 if y < 200 else y

        edition_str = row.get('屆數', '').strip()
        category = row.get('名單類別', '').strip()

        records.append({
            'company_name': row.get('公司名稱', '').strip(),
            'company_tax_id': None,
            'record_type': 'award',
            'program_name': '國家品質獎',
            'program_name_en': 'National Quality Award',
            'issuing_agency': '經濟部產業發展署',
            'year': year,
            'edition': edition_str,
            'details': f'{edition_str}國家品質獎{category}得主' if edition_str else '國家品質獎得主',
        })

    log.info('Parsed %d records from 國家品質獎', len(records))
    return records


# ---------------------------------------------------------------------------
# 金貿獎
# ---------------------------------------------------------------------------

def fetch_golden_trade_records() -> list[dict[str, Any]]:
    """
    讀取或下載金貿獎 CSV，解析為標準 government_records 格式。

    CSV 欄位：表揚年度, 得獎廠商公司名稱, 縣市, 縣市別, 得獎類別
    """
    if GOLDEN_TRADE_LOCAL.exists():
        log.info('讀取本地金貿獎 CSV：%s', GOLDEN_TRADE_LOCAL)
        raw = GOLDEN_TRADE_LOCAL.read_bytes().decode('utf-8-sig')
    else:
        log.info('下載金貿獎 CSV from %s', GOLDEN_TRADE_URL)
        req = urllib.request.Request(GOLDEN_TRADE_URL, headers={'User-Agent': 'tw-mfg-db/1.0'})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
            raw = resp.read().decode('utf-8-sig')

    reader = csv.DictReader(io.StringIO(raw))
    records: list[dict[str, Any]] = []

    for row in reader:
        year_str = row.get('表揚年度', '').strip()
        # 表揚年度為民國年（如 103），轉為西元年
        year: int | None = None
        if year_str.isdigit():
            y = int(year_str)
            year = y + 1911 if y < 200 else y

        award_category = row.get('得獎類別', '').strip()
        city = row.get('縣市', '').strip()

        records.append({
            'company_name': row.get('得獎廠商公司名稱', '').strip(),
            'company_tax_id': None,
            'record_type': 'export_excellence',
            'program_name': '金貿獎',
            'program_name_en': 'Golden Trade Award',
            'issuing_agency': '經濟部國際貿易署',
            'year': year,
            'edition': None,
            'details': f'{year_str}年金貿獎{award_category}得主（{city}）' if award_category else f'{year_str}年金貿獎得主',
        })

    log.info('Parsed %d records from 金貿獎', len(records))
    return records


# ---------------------------------------------------------------------------
# MEET TAIWAN IMDP 補助業界開發國際市場計畫
# ---------------------------------------------------------------------------

IMDP_BASE_URL = 'https://market.meettaiwan.com/imdp/?action=achievement&cid={cid}'
# cid=1 → 105年度（西元2016），cid=10 → 114年度（西元2025）
IMDP_CID_RANGE = range(1, 11)


class _IMDPParser(HTMLParser):
    """
    解析 IMDP achievement 頁面的 div-based 列表。

    頁面結構：
      <div class="downloadlist achlist">
        <div class="oneset thhead">...</div>
        <div class="tbody">
          <div class="oneset">
            <div class="box box1">序號</div>
            <div class="box box2">編號</div>
            <div class="box box3">廠商名稱</div>
            <div class="box box4">計畫名稱</div>
            <div class="box box5">核定補助金額</div>
          </div>
          ...
        </div>
      </div>

    備取名單也可能出現另一個 achlist 區塊，用 section_label 標記。
    """

    def __init__(self) -> None:
        super().__init__()
        # 解析狀態
        self._in_achlist: bool = False          # 在 downloadlist achlist 內
        self._in_tbody: bool = False            # 在 .tbody 內
        self._in_oneset: bool = False           # 在 .oneset（非 thhead）內
        self._current_box: int | None = None   # 目前在第幾個 box（1~5）
        self._depth_achlist: int = 0
        self._depth_tbody: int = 0
        self._depth_oneset: int = 0
        self._depth_box: int = 0
        self._global_depth: int = 0

        # 當前 row 暫存
        self._row: dict[str, str] = {}

        # 區塊標籤（正取 / 備取）
        self.current_section: str = '正取名單'

        # 收集結果
        self.rows: list[dict[str, str]] = []  # {'no', 'code', 'company', 'plan', 'amount', 'section'}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._global_depth += 1
        attr_dict = dict(attrs)
        classes = (attr_dict.get('class') or '').split()

        if tag == 'div':
            # 偵測 achlist 進入
            if 'downloadlist' in classes and 'achlist' in classes:
                self._in_achlist = True
                self._depth_achlist = self._global_depth
                return

            if self._in_achlist:
                # tbody
                if 'tbody' in classes:
                    self._in_tbody = True
                    self._depth_tbody = self._global_depth
                    return

                if self._in_tbody:
                    # oneset（排除 thhead）
                    if 'oneset' in classes and 'thhead' not in classes:
                        self._in_oneset = True
                        self._depth_oneset = self._global_depth
                        self._row = {}
                        return

                    if self._in_oneset:
                        # box1 ~ box5
                        for n in range(1, 6):
                            if f'box{n}' in classes:
                                self._current_box = n
                                self._depth_box = self._global_depth
                                break

    def handle_endtag(self, tag: str) -> None:
        if tag == 'div':
            if self._in_oneset and self._global_depth == self._depth_oneset:
                # 結束 oneset → 儲存 row
                self._in_oneset = False
                self._current_box = None
                row = dict(self._row)
                row['section'] = self.current_section
                self.rows.append(row)

            elif self._in_tbody and self._global_depth == self._depth_tbody:
                self._in_tbody = False

            elif self._in_achlist and self._global_depth == self._depth_achlist:
                self._in_achlist = False

            # 離開 box
            if self._current_box is not None and self._global_depth == self._depth_box:
                self._current_box = None

        self._global_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._in_oneset or self._current_box is None:
            return
        text = data.strip()
        if not text:
            return
        key_map = {1: 'no', 2: 'code', 3: 'company', 4: 'plan', 5: 'amount'}
        key = key_map.get(self._current_box)
        if key:
            # 累加（一個 box 裡可能有多段文字或 span）
            self._row[key] = self._row.get(key, '') + text


def _parse_imdp_amount(raw: str) -> int | None:
    """
    解析補助金額字串，例如 '$3,606,000' → 3606000。
    空白或無數字則回傳 None。
    """
    cleaned = re.sub(r'[^\d]', '', raw)
    if cleaned:
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


def _split_companies(company_field: str) -> list[str]:
    """
    B 類計畫的廠商欄位可能包含多家公司，以換行或全形分隔符分開。
    拆成個別公司名，過濾空值。
    """
    # 常見分隔符：換行、全形頓號、全形分號
    parts = re.split(r'[\n\r　、；;]+', company_field)
    return [p.strip() for p in parts if p.strip()]


def fetch_imdp_records() -> list[dict[str, Any]]:
    """
    爬取 MEET TAIWAN IMDP 歷屆受補助廠商（cid=1 到 cid=10，對應 105～114 年度）。
    回傳標準 government_records 格式的列表。
    """
    records: list[dict[str, Any]] = []

    for cid in IMDP_CID_RANGE:
        # cid=1 → 民國105年 → 西元2016
        roc_year = 104 + cid
        western_year = roc_year + 1911

        url = IMDP_BASE_URL.format(cid=cid)
        log.info('Fetching IMDP cid=%d (%d年度) from %s', cid, roc_year, url)

        try:
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'tw-mfg-db/1.0'},
            )
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
                content = resp.read().decode('utf-8', errors='replace')
        except Exception as exc:
            log.warning('IMDP cid=%d 下載失敗：%s — skipping', cid, exc)
            continue

        # 偵測備取名單區塊標題，在 parser 處理前先找到分界點
        # 頁面可能有兩個 achlist，第二個是備取。用簡易 regex 找到「備取」關鍵字位置
        standby_marker = re.search(r'備取', content)
        standby_pos = standby_marker.start() if standby_marker else len(content)

        # 切成「正取」和「備取」兩段分別解析
        sections = [
            ('正取名單', content[:standby_pos]),
            ('備取名單', content[standby_pos:]),
        ]

        page_count = 0
        for section_label, html_chunk in sections:
            if not html_chunk.strip():
                continue
            parser = _IMDPParser()
            parser.current_section = section_label
            parser.feed(html_chunk)

            for row in parser.rows:
                company_field = row.get('company', '').strip()
                code = row.get('code', '').strip()
                plan = row.get('plan', '').strip()
                amount_raw = row.get('amount', '').strip()
                section = row.get('section', section_label)

                if not company_field:
                    continue

                subsidy_amount = _parse_imdp_amount(amount_raw)

                # B 類計畫或多家廠商：拆分
                companies = _split_companies(company_field)
                if not companies:
                    companies = [company_field]

                for company in companies:
                    detail_parts = []
                    if code:
                        detail_parts.append(code)
                    if plan:
                        detail_parts.append(plan)
                    if section == '備取名單':
                        detail_parts.append('（備取）')
                    details = ': '.join(detail_parts) if detail_parts else f'{roc_year}年度IMDP補助'

                    records.append({
                        'company_name': company,
                        'company_tax_id': None,
                        'record_type': 'subsidy',
                        'program_name': '補助業界開發國際市場計畫',
                        'program_name_en': 'IMDP (Industry Market Development Plan)',
                        'issuing_agency': '經濟部國際貿易署',
                        'year': western_year,
                        'details': details,
                        'subsidy_amount': subsidy_amount,
                    })
                    page_count += 1

        log.info('  IMDP cid=%d → %d records', cid, page_count)

    log.info('IMDP total: %d records across %d years', len(records), len(IMDP_CID_RANGE))
    return records


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / 'government_records_raw.json'

    all_records: list[dict[str, Any]] = []

    # 1. 小巨人獎（主要資料源，已驗證）
    try:
        records = fetch_small_giant()
        all_records.extend(records)
    except Exception as exc:
        log.error('小巨人獎 failed: %s', exc)

    # 2. SBIR 小型企業創新研發計畫
    try:
        sbir_records = fetch_sbir_records()
        all_records.extend(sbir_records)
        log.info('SBIR: %d 筆已加入', len(sbir_records))
    except Exception as exc:
        log.error('SBIR failed: %s', exc)

    # 3. 台灣精品獎（嘗試一次）
    taiwan_excellence = fetch_taiwan_excellence()
    all_records.extend(taiwan_excellence)

    # 4. 國家磐石獎
    try:
        panshi_records = fetch_panshi_records()
        all_records.extend(panshi_records)
        log.info('國家磐石獎：%d 筆已加入', len(panshi_records))
    except Exception as exc:
        log.error('國家磐石獎 failed: %s', exc)

    # 5. 國家品質獎
    try:
        quality_records = fetch_quality_records()
        all_records.extend(quality_records)
        log.info('國家品質獎：%d 筆已加入', len(quality_records))
    except Exception as exc:
        log.error('國家品質獎 failed: %s', exc)

    # 6. 金貿獎
    try:
        golden_trade_records = fetch_golden_trade_records()
        all_records.extend(golden_trade_records)
        log.info('金貿獎：%d 筆已加入', len(golden_trade_records))
    except Exception as exc:
        log.error('金貿獎 failed: %s', exc)

    # 7. MEET TAIWAN IMDP 補助業界開發國際市場計畫
    try:
        imdp_records = fetch_imdp_records()
        all_records.extend(imdp_records)
        log.info('IMDP：%d 筆已加入', len(imdp_records))
    except Exception as exc:
        log.error('IMDP failed: %s', exc)

    # 輸出
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    log.info('Done. Written %d records to %s', len(all_records), output_path)


if __name__ == '__main__':
    main()
