"""
scrape_tipo_patents.py — 從 TIPO Open Data API 下載台灣專利資料

資料來源：TIPO（智慧財產局）Open Data API
  Base URL: https://cloud.tipo.gov.tw/S220/opdataapi/api/{DatasetName}
  更新頻率：每月 6 號、16 號、26 號

流程：
  1. 分頁從 PatentPub endpoint 批次下載專利公告資料
  2. 過濾台灣申請人（TW 國籍）
  3. 嘗試將申請人名稱與 factories.json 做初步比對
  4. 輸出 src/data/patents_raw.json

fallback：若 TIPO API 不可達，改用 data.gov.tw 鏡像資料集。

注意：API 不支援 server-side 過濾，需下載後本地過濾。
      資料量約 187 萬筆，預設只取近 3 年資料。
"""

import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'

# ---------------------------------------------------------------------------
# TIPO API 設定
# ---------------------------------------------------------------------------

TIPO_BASE_URL = 'https://cloud.tipo.gov.tw/S220/opdataapi/api'
TIPO_DEMO_TOKEN = '43b47d07-4795-45d9-819a-9c71c72e4105'
TIPO_ENDPOINT = 'PatentPub'

PAGE_SIZE = 1000          # TIPO API 每頁上限
REQUEST_TIMEOUT = 30      # seconds
RETRY_MAX = 3             # 每個請求最多重試次數
RETRY_DELAY = 2.0         # 重試間隔（秒）
REQUEST_INTERVAL = 0.5    # 分頁請求間隔（秒）

# 近 3 年資料（用來在本地過濾公開日期）
YEARS_TO_FETCH = 3
CUTOFF_DATE = (datetime.now() - timedelta(days=365 * YEARS_TO_FETCH)).strftime('%Y%m%d')

# ---------------------------------------------------------------------------
# data.gov.tw fallback（TIPO 鏡像）
# ---------------------------------------------------------------------------

# 若 TIPO 主站不可達，可嘗試 data.gov.tw 上的 TIPO 專利資料集
DATAGOV_FALLBACK_URLS: list[str] = [
    'https://data.gov.tw/api/3/action/datastore_search?resource_id=7afe47c5-0082-4a01-a413-4d7a89ec96f6&limit=1000',
]


# ---------------------------------------------------------------------------
# HTTP 工具
# ---------------------------------------------------------------------------

def _fetch_json(url: str, retries: int = RETRY_MAX) -> Any:
    """
    帶 retry 機制的 JSON GET 請求。

    Args:
        url: 完整 URL
        retries: 剩餘重試次數

    Returns:
        解析後的 JSON 物件

    Raises:
        Exception: 所有重試耗盡後仍失敗
    """
    req = urllib.request.Request(url, headers={
        'User-Agent': 'tw-mfg-db/1.0 (patent-scraper)',
        'Accept': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
            raw = resp.read()
            return json.loads(raw.decode('utf-8'))
    except Exception as e:
        if retries > 0:
            print(f'  [retry] {e} — 剩餘重試 {retries} 次，等待 {RETRY_DELAY}s...')
            time.sleep(RETRY_DELAY)
            return _fetch_json(url, retries - 1)
        raise


# ---------------------------------------------------------------------------
# TIPO API 分頁下載
# ---------------------------------------------------------------------------

def build_tipo_url(skip: int, top: int = PAGE_SIZE, token: str = TIPO_DEMO_TOKEN) -> str:
    """
    組裝 TIPO Open Data API URL。

    TIPO API 使用 OData 風格分頁：$top / $skip（或 top / skip 參數）。
    實際參數格式依官方文件：format=json&top=N&tk=TOKEN&skip=N
    """
    params = urllib.parse.urlencode({
        'format': 'json',
        'top': top,
        'tk': token,
        'skip': skip,
    })
    return f'{TIPO_BASE_URL}/{TIPO_ENDPOINT}?{params}'


def download_tipo_all(token: str = TIPO_DEMO_TOKEN) -> list[dict[str, Any]]:
    """
    分頁下載 TIPO PatentPub 全量資料（本地過濾近 3 年）。

    Returns:
        原始專利 record list（API 原始格式）
    """
    all_records: list[dict[str, Any]] = []
    # API 按 appl-no 升冪排序（舊→新），從 skip=900000 開始掃近年資料
    # 總量約 1,051,021 筆，近 3 年約在最後 15 萬筆
    skip = 900000
    page_num = 1
    stop_early = False

    print(f'開始從 TIPO API 下載專利資料（token: {token[:8]}...）')
    print(f'截止日期篩選：公開日期 >= {CUTOFF_DATE}')
    print(f'從 skip={skip} 開始（跳過舊資料）')

    while not stop_early:
        url = build_tipo_url(skip=skip, top=PAGE_SIZE, token=token)
        print(f'  [Page {page_num}] skip={skip}, top={PAGE_SIZE}...')

        try:
            data = _fetch_json(url)
        except Exception as e:
            print(f'  [ERROR] 第 {page_num} 頁下載失敗：{e}')
            break

        # TIPO API 回傳格式：可能為 list 或包含 value 欄位的 dict
        records = _extract_records(data)

        if not records:
            print(f'  [完成] 第 {page_num} 頁回傳 0 筆，停止分頁。')
            break

        # 本地日期過濾：公開日期早於截止日則停止（資料通常依日期降冪排序）
        filtered, stop_early = _filter_by_date(records)
        all_records.extend(filtered)

        total = len(all_records)
        if total % 1000 == 0 or len(records) < PAGE_SIZE:
            print(f'  累計 {total} 筆（近 {YEARS_TO_FETCH} 年）')

        if len(records) < PAGE_SIZE:
            print(f'  [完成] 最後一頁（{len(records)} 筆），停止分頁。')
            break

        skip += PAGE_SIZE
        page_num += 1
        time.sleep(REQUEST_INTERVAL)

    print(f'TIPO 下載完成，共取得 {len(all_records)} 筆近 {YEARS_TO_FETCH} 年資料。')
    return all_records


def _extract_records(data: Any) -> list[dict[str, Any]]:
    """
    從 TIPO API 回應中萃取 record list。

    TIPO PatentPub 實際回傳結構：
      {
        "status": "ok",
        "total-count": 1051021,
        "tw-patent-pub": {
          "-page-count": N,
          "patentcontent": [ {...}, ... ]
        }
      }

    也相容 list 直接回傳或其他常見格式。
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # TIPO 實際格式
        pub_block = data.get('tw-patent-pub', {})
        if isinstance(pub_block, dict):
            patentcontent = pub_block.get('patentcontent', [])
            if isinstance(patentcontent, list):
                return patentcontent

        # 通用 fallback
        for key in ('value', 'data', 'records', 'result'):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def _filter_by_date(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """
    依公開日期過濾，只保留近 3 年資料。

    Returns:
        (filtered_records, should_stop_early)
        should_stop_early: True 表示遇到超出範圍的舊資料，可提前終止分頁
    """
    filtered: list[dict[str, Any]] = []
    stop_early = False

    for rec in records:
        pub_date = _get_pub_date(rec)
        if pub_date and pub_date >= CUTOFF_DATE:
            filtered.append(rec)
        # API 依 appl-no 升冪排序（舊→新），不能提前停止

    return filtered, False


def _get_pub_date(rec: dict[str, Any]) -> str:
    """
    從 record 中萃取公開日期字串，正規化為 YYYYMMDD 以便字串比較。

    TIPO 實際結構：rec['publication-reference']['notice-date'] = '2024/03/15'
    也支援扁平格式（fallback 資料來源）。
    """
    # TIPO 巢狀格式
    pub_ref = rec.get('publication-reference', {})
    if isinstance(pub_ref, dict):
        val = pub_ref.get('notice-date', '')
        if val:
            return str(val).replace('-', '').replace('/', '')[:8]

    # 扁平格式 fallback
    for key in ('公開日期', 'pubDate', 'pub_date', 'PublicationDate', 'publication_date'):
        val = rec.get(key, '')
        if val:
            return str(val).replace('-', '').replace('/', '')[:8]
    return ''


# ---------------------------------------------------------------------------
# 台灣申請人過濾
# ---------------------------------------------------------------------------

def filter_tw_applicants(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    只保留至少有一位台灣（TW）申請人的專利。

    TIPO 實際結構：
      rec['parties']['applicants'][i]['english-country'] = 'TW'
      rec['parties']['applicants'][i]['chinese-country'] = '中華民國'

    也相容扁平格式（fallback 資料來源）。
    """
    TW_CODES = {'TW', 'TWN', '中華民國', 'TAIWAN', '台灣', 'R.O.C', 'ROC'}

    result: list[dict[str, Any]] = []
    for rec in records:
        # TIPO 巢狀格式：檢查 parties.applicants
        parties = rec.get('parties', {})
        applicants = parties.get('applicants', []) if isinstance(parties, dict) else []

        if applicants:
            has_tw = any(
                str(a.get('english-country', '')).upper() in TW_CODES or
                str(a.get('chinese-country', '')) in TW_CODES
                for a in applicants
                if isinstance(a, dict)
            )
            if has_tw:
                result.append(rec)
            # 若所有申請人都是外國籍，跳過
            continue

        # 扁平格式 fallback
        nationality = (
            rec.get('applicantNationality') or
            rec.get('申請人國籍') or
            rec.get('nationality') or
            ''
        )
        if not nationality or str(nationality).upper() in TW_CODES:
            result.append(rec)

    return result


# ---------------------------------------------------------------------------
# 欄位正規化
# ---------------------------------------------------------------------------

def normalize_patent(rec: dict[str, Any]) -> dict[str, Any]:
    """
    將 TIPO 原始 record 正規化為標準輸出格式。

    TIPO 實際巢狀結構：
      publication-reference.notice-no          → patent_number（公開號）
      application-reference.appl-no            → application_number
      application-reference.appl-date          → application_date
      publication-reference.notice-date        → publication_date
      patent-title.patent-name-chinese         → title_zh
      patent-title.patent-name-english         → title_en
      parties.applicants[0].chinese-name       → applicant_name
      classification-ipc[0].ipc-full           → tech_category

    也相容扁平格式（fallback 資料來源）。
    """
    def flat_get(d: dict, *keys: str, default: str = '') -> str:
        """從扁平 dict 嘗試多個 key。"""
        for k in keys:
            v = d.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return default

    # --- 公開號 ---
    pub_ref = rec.get('publication-reference', {}) or {}
    patent_number = str(pub_ref.get('notice-no', '')).strip()
    if not patent_number:
        patent_number = flat_get(rec, '公開號', 'pubNo', 'PatentNo', 'patent_number')

    # --- 申請號 ---
    appl_ref = rec.get('application-reference', {}) or {}
    application_number = str(appl_ref.get('appl-no', '')).strip()
    if not application_number:
        application_number = flat_get(rec, '申請號', 'applNo', 'ApplicationNo', 'application_number')

    # --- 日期 ---
    pub_date_raw = str(pub_ref.get('notice-date', '')).strip()
    if not pub_date_raw:
        pub_date_raw = flat_get(rec, '公開日期', 'pubDate', 'publication_date')

    app_date_raw = str(appl_ref.get('appl-date', '')).strip()
    if not app_date_raw:
        app_date_raw = flat_get(rec, '申請日期', 'applDate', 'application_date')

    # --- 標題 ---
    title_block = rec.get('patent-title', {}) or {}
    title_zh = str(title_block.get('patent-name-chinese', '') or '').strip()
    if not title_zh:
        title_zh = flat_get(rec, '專利名稱', '中文名稱', 'titleZh', 'title_zh')
    title_en = str(title_block.get('patent-name-english', '') or '').strip()
    if not title_en:
        title_en = flat_get(rec, '英文名稱', 'titleEn', 'title_en')

    # --- 申請人（取第一位台灣申請人）---
    applicant_name = ''
    parties = rec.get('parties', {}) or {}
    applicants = parties.get('applicants', []) or []
    if applicants and isinstance(applicants, list):
        # 優先取台灣申請人
        for a in applicants:
            if isinstance(a, dict) and str(a.get('english-country', '')).upper() == 'TW':
                applicant_name = str(a.get('chinese-name', '') or a.get('english-name', '') or '').strip()
                break
        if not applicant_name and isinstance(applicants[0], dict):
            applicant_name = str(applicants[0].get('chinese-name', '') or '').strip()
    if not applicant_name:
        applicant_name = flat_get(rec, '申請人名稱', 'applicantName', 'applicant_name')

    return {
        'patent_number': patent_number,
        'application_number': application_number,
        'title_zh': title_zh,
        'title_en': title_en,
        'applicant_name': applicant_name,
        'tech_category': _extract_ipc(rec),
        'abstract_zh': flat_get(rec, '摘要', '中文摘要', 'abstractZh', 'abstract_zh'),
        'publication_date': _format_date(pub_date_raw),
        'application_date': _format_date(app_date_raw),
    }


def _extract_ipc(rec: dict[str, Any]) -> str:
    """
    萃取主要 IPC 分類（取第一個）。

    TIPO 實際結構：
      rec['classification-ipc'] = [{"ipc-full": "H01L33/00", ...}, ...]

    也相容扁平格式。
    """
    # TIPO 巢狀格式
    ipc_list = rec.get('classification-ipc', [])
    if isinstance(ipc_list, list) and ipc_list:
        first = ipc_list[0]
        if isinstance(first, dict):
            ipc_full = str(first.get('ipc-full', '')).strip()
            if ipc_full:
                return ipc_full[:10]

    # 扁平格式 fallback
    for key in ('IPC分類', 'ipc', 'IPC', 'ipcCode', 'tech_category'):
        val = rec.get(key, '')
        if val:
            import re
            first = re.split(r'[\s;,]', str(val).strip())[0]
            return first[:10]
    return ''


def _format_date(raw: str) -> str:
    """
    將各種日期格式正規化為 YYYY-MM-DD。
    輸入可能為：20240315, 2024-03-15, 2024/03/15, 1130315（民國年）
    """
    raw = raw.strip().replace('/', '').replace('-', '')
    if len(raw) == 7 and raw[0].isdigit():
        # 民國年：1130315 → 2024-03-15
        try:
            roc_year = int(raw[:3])
            month = raw[3:5]
            day = raw[5:7]
            ad_year = roc_year + 1911
            return f'{ad_year}-{month}-{day}'
        except ValueError:
            pass
    if len(raw) == 8:
        return f'{raw[:4]}-{raw[4:6]}-{raw[6:8]}'
    return raw


# ---------------------------------------------------------------------------
# 工廠名稱比對
# ---------------------------------------------------------------------------

def load_factory_names(factories_path: Path) -> set[str]:
    """
    載入 factories.json 中的工廠名稱集合，用於申請人比對。

    Returns:
        工廠名稱 set（去除空白、去重）
    """
    if not factories_path.exists():
        print(f'  [警告] 找不到 {factories_path}，跳過工廠比對。')
        return set()

    with factories_path.open('r', encoding='utf-8') as f:
        factories = json.load(f)

    names: set[str] = set()
    for fac in factories:
        name = fac.get('name_zh', '').strip()
        if name:
            names.add(name)
    print(f'  載入 {len(names)} 個工廠名稱用於比對。')
    return names


def match_factory(applicant: str, factory_names: set[str]) -> bool:
    """
    判斷申請人名稱是否出現在工廠名冊中（精確比對）。

    簡單模式：精確字串比對。
    進階可擴充：模糊比對、關鍵字萃取、統編比對等。
    """
    return applicant in factory_names


# ---------------------------------------------------------------------------
# data.gov.tw Fallback
# ---------------------------------------------------------------------------

def download_datagov_fallback() -> list[dict[str, Any]]:
    """
    當 TIPO API 不可達時，嘗試 data.gov.tw 鏡像資料集。

    Returns:
        原始 record list（格式可能與 TIPO 不同，但走相同正規化流程）
    """
    all_records: list[dict[str, Any]] = []

    for base_url in DATAGOV_FALLBACK_URLS:
        print(f'  嘗試 data.gov.tw fallback: {base_url[:60]}...')
        offset = 0
        page_num = 1

        try:
            while True:
                url = f'{base_url}&offset={offset}'
                data = _fetch_json(url)

                if not isinstance(data, dict) or not data.get('success'):
                    break

                records: list[dict[str, Any]] = data.get('result', {}).get('records', [])
                all_records.extend(records)

                if not records or len(records) < PAGE_SIZE:
                    break

                if page_num % 10 == 0:
                    print(f'  [data.gov.tw Page {page_num}] 累計 {len(all_records)} 筆')

                offset += PAGE_SIZE
                page_num += 1
                time.sleep(REQUEST_INTERVAL)

            if all_records:
                print(f'  data.gov.tw 下載成功，共 {len(all_records)} 筆。')
                return all_records

        except Exception as e:
            print(f'  data.gov.tw fallback 失敗：{e}')

    return all_records


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / 'patents_raw.json'
    factories_path = DATA_DIR / 'factories.json'

    raw_records: list[dict[str, Any]] = []
    source = 'tipo'

    # Step 1: 嘗試 TIPO Open Data API
    print('=' * 60)
    print('TIPO 專利資料爬蟲')
    print('=' * 60)
    try:
        raw_records = download_tipo_all()
    except Exception as e:
        print(f'[ERROR] TIPO API 失敗：{e}')
        raw_records = []

    # Step 2: Fallback 至 data.gov.tw
    if not raw_records:
        print('\nTIPO API 無資料，切換至 data.gov.tw fallback...')
        source = 'datagov'
        try:
            raw_records = download_datagov_fallback()
        except Exception as e:
            print(f'[ERROR] data.gov.tw fallback 亦失敗：{e}')

    if not raw_records:
        print('\n[ABORT] 所有資料來源皆不可達，輸出空檔案。')
        with output_path.open('w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
        return

    # Step 3: 過濾台灣申請人
    print(f'\n過濾台灣申請人（原始 {len(raw_records)} 筆）...')
    tw_records = filter_tw_applicants(raw_records)
    print(f'台灣申請人專利：{len(tw_records)} 筆')

    # Step 4: 正規化
    print('\n正規化欄位...')
    normalized: list[dict[str, Any]] = [normalize_patent(r) for r in tw_records]

    # Step 5: 與工廠名冊比對（標記 matched_factory 欄位）
    print('\n載入工廠名冊進行初步比對...')
    factory_names = load_factory_names(factories_path)
    matched_count = 0
    for patent in normalized:
        applicant = patent.get('applicant_name', '')
        if factory_names and match_factory(applicant, factory_names):
            patent['matched_factory'] = True
            matched_count += 1
        else:
            patent['matched_factory'] = False

    print(f'工廠比對結果：{matched_count} / {len(normalized)} 筆命中工廠名冊')

    # Step 6: 加入 metadata
    output = {
        'metadata': {
            'source': source,
            'fetched_at': datetime.now().isoformat(),
            'cutoff_date': CUTOFF_DATE,
            'total_tw_patents': len(normalized),
            'factory_matched': matched_count,
        },
        'patents': normalized,
    }

    # Step 7: 寫出
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'\n完成。寫出 {len(normalized)} 筆至 {output_path}')
    if source != 'tipo':
        print('NOTE: 使用 fallback 資料來源，欄位格式可能與 TIPO 原始格式不同。')


if __name__ == '__main__':
    main()
