"""
download_factory_data.py — 從經濟部工廠登記開放資料下載工廠名冊

資料來源：data.gov.tw 資料集 9842（工廠登記資料）
若 API 不可達，自動 fallback 至 generate_sample_data.py 建立模擬資料集。

輸出：src/data/factories.json
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'

# data.gov.tw CKAN API endpoint
DATASET_ID = '9842'
CKAN_API_BASE = 'https://data.gov.tw/api/3/action'
RESOURCE_URL_TEMPLATE = 'https://data.gov.tw/dataset/{dataset_id}'

# 已知的直接 CSV 下載 URL（可能因資料集更新而變動）
KNOWN_RESOURCE_URLS: list[str] = [
    'https://data.gov.tw/api/3/action/datastore_search?resource_id=c1e0ef8c-80b6-409e-8b2e-f4e7c64a7ac8&limit=1000',
]

REQUEST_TIMEOUT = 30  # seconds

# 已下載的本地 CSV 路徑（優先使用）
LOCAL_CSV_PATH = DATA_DIR / 'moea_factories.csv'


# ---------------------------------------------------------------------------
# 欄位映射
# ---------------------------------------------------------------------------

# 工廠登記資料欄位名可能因版本不同而異，以下為常見欄位名稱對照
FIELD_MAPPING: dict[str, str] = {
    # 統一編號
    '統一編號': 'tax_id',
    'tax_id': 'tax_id',
    # 工廠名稱
    '工廠名稱': 'name_zh',
    '名稱': 'name_zh',
    # 產業類別
    '行業代碼': 'industry_code',
    '行業別': 'industry_zh',
    '產業類別': 'industry_zh',
    # 地址
    '工廠地址': 'address_zh',
    '地址': 'address_zh',
    # 縣市
    '縣市': 'city_zh',
    # 鄉鎮市區
    '鄉鎮市區': 'district_zh',
    # 登記日期
    '登記日期': 'registration_date',
    '設立日期': 'registration_date',
    # 營運狀態
    '狀態': 'status',
    '工廠狀態': 'status',
}


# ---------------------------------------------------------------------------
# 下載與解析
# ---------------------------------------------------------------------------

def fetch_dataset_resources(dataset_id: str) -> list[dict[str, Any]]:
    """
    透過 CKAN API 查詢資料集的資源列表。

    Args:
        dataset_id: data.gov.tw 資料集 ID

    Returns:
        資源列表（每個資源包含 url、format 等欄位）

    Raises:
        urllib.error.URLError: 網路不可達時
    """
    url = f'{CKAN_API_BASE}/package_show?id={dataset_id}'
    req = urllib.request.Request(url, headers={'User-Agent': 'tw-mfg-db/1.0'})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        data = json.loads(resp.read().decode('utf-8'))

    if not data.get('success'):
        raise ValueError(f'CKAN API returned success=false for dataset {dataset_id}')

    return data['result'].get('resources', [])


def download_csv_resource(url: str) -> list[dict[str, Any]]:
    """
    下載單個 CSV 資源並解析為 dict list。

    Args:
        url: CSV 直接下載 URL

    Returns:
        解析後的 row list
    """
    import csv
    import io

    req = urllib.request.Request(url, headers={'User-Agent': 'tw-mfg-db/1.0'})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        raw = resp.read().decode('utf-8-sig')  # 處理 BOM

    reader = csv.DictReader(io.StringIO(raw))
    return list(reader)


def normalize_row(row: dict[str, Any], index: int) -> dict[str, Any]:
    """
    將原始 CSV row 正規化為標準 factories.json 格式。

    Args:
        row: CSV 原始 row
        index: 1-based 序號

    Returns:
        標準化的工廠 dict
    """
    normalized: dict[str, Any] = {'id': index}

    for src_key, dst_key in FIELD_MAPPING.items():
        value = row.get(src_key, '').strip()
        if value and dst_key not in normalized:
            normalized[dst_key] = value

    # 預設值填充
    normalized.setdefault('tax_id', '')
    normalized.setdefault('name_zh', row.get('名稱', '').strip())
    normalized.setdefault('industry_code', '')
    normalized.setdefault('industry_zh', '')
    normalized.setdefault('address_zh', '')
    normalized.setdefault('city_zh', _extract_city(normalized.get('address_zh', '')))
    normalized.setdefault('district_zh', _extract_district(normalized.get('address_zh', '')))
    normalized.setdefault('registration_date', '')
    normalized.setdefault('status', '正常營業')

    return normalized


def _extract_city(address: str) -> str:
    """從地址字串萃取縣市（前 3 字若含 '市' 或 '縣'）。"""
    if len(address) >= 3:
        candidate = address[:3]
        if '市' in candidate or '縣' in candidate:
            return candidate
    return ''


def _extract_district(address: str) -> str:
    """從地址字串萃取鄉鎮市區。"""
    import re
    match = re.search(r'[市縣](.{2,4}[區鄉鎮市])', address)
    if match:
        return match.group(1)
    return ''


def download_with_pagination(resource_url: str, page_size: int = 1000) -> list[dict[str, Any]]:
    """
    使用分頁方式從 CKAN datastore_search API 下載完整資料集。

    注意：CKAN 預設 limit 上限可能低於實際總筆數，必須分頁讀取，
    避免靜默截斷（參考 Supabase PostgREST 教訓）。

    Args:
        resource_url: 含 resource_id 的 datastore_search URL
        page_size: 每頁筆數

    Returns:
        所有 records 的 list
    """
    all_records: list[dict[str, Any]] = []
    offset = 0

    while True:
        url = f'{resource_url}&offset={offset}&limit={page_size}'
        print(f'  Fetching offset={offset}...')
        req = urllib.request.Request(url, headers={'User-Agent': 'tw-mfg-db/1.0'})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode('utf-8'))

        if not data.get('success'):
            break

        records: list[dict[str, Any]] = data['result'].get('records', [])
        all_records.extend(records)

        if len(records) < page_size:
            break  # 最後一頁

        offset += page_size
        time.sleep(0.5)  # 避免過度頻繁請求

    return all_records


# ---------------------------------------------------------------------------
# Fallback：使用模擬資料
# ---------------------------------------------------------------------------

def _parse_roc_date(roc_date_str: str) -> str:
    """
    將民國年日期字串轉換為西元 ISO 格式（YYYY-MM-DD）。

    Args:
        roc_date_str: 民國年字串，格式如 '1131126'（7 碼）或 '980101'（6 碼）

    Returns:
        ISO 格式日期字串，如 '2024-11-26'；無法解析時回傳空字串
    """
    s = roc_date_str.strip()
    if not s:
        return ''
    try:
        if len(s) == 7:
            roc_year = int(s[:3])
            month = int(s[3:5])
            day = int(s[5:7])
        elif len(s) == 6:
            roc_year = int(s[:2])
            month = int(s[2:4])
            day = int(s[4:6])
        else:
            return ''
        ad_year = roc_year + 1911
        return f'{ad_year:04d}-{month:02d}-{day:02d}'
    except (ValueError, IndexError):
        return ''


def _parse_location_from_village(village_str: str) -> tuple[str, str]:
    """
    從「工廠市鎮鄉村里」欄位萃取縣市和區域。

    例：'桃園市龜山區嶺頂里' → ('桃園市', '龜山區')

    Args:
        village_str: 如 '桃園市龜山區嶺頂里'

    Returns:
        (city_zh, district_zh) 元組
    """
    import re
    city_match = re.match(r'^(.{2,4}[市縣])', village_str)
    city_zh = city_match.group(1) if city_match else ''

    dist_match = re.search(r'[市縣](.{2,4}[區鄉鎮市])', village_str)
    district_zh = dist_match.group(1) if dist_match else ''

    return city_zh, district_zh


def normalize_moea_csv_row(row: dict[str, Any], index: int) -> dict[str, Any]:
    """
    將 MOEA 工廠登記 CSV 的原始 row 正規化為標準 factories.json 格式。

    CSV 欄位：工廠名稱、工廠登記編號、工廠設立許可案號、工廠地址、
              工廠市鎮鄉村里、工廠負責人姓名、統一編號、工廠組織型態、
              工廠設立核准日期、工廠登記核准日期、工廠登記狀態、產業類別、主要產品

    Args:
        row: CSV 原始 row
        index: 1-based 序號

    Returns:
        標準化的工廠 dict
    """
    village = row.get('工廠市鎮鄉村里', '').strip()
    city_zh, district_zh = _parse_location_from_village(village)

    # 產業類別格式：'08食品製造業' → code='08', name='食品製造業'
    industry_raw = row.get('產業類別', '').strip()
    industry_code_match = __import__('re').match(r'^(\d+)', industry_raw)
    industry_code = industry_code_match.group(1) if industry_code_match else ''
    industry_zh = __import__('re').sub(r'^\d+', '', industry_raw).strip()

    # 日期：優先 工廠登記核准日期，次之 工廠設立核准日期
    reg_date_raw = row.get('工廠登記核准日期', '').strip() or row.get('工廠設立核准日期', '').strip()
    registration_date = _parse_roc_date(reg_date_raw)

    return {
        'id': index,
        'tax_id': row.get('統一編號', '').strip(),
        'registration_no': row.get('工廠登記編號', '').strip(),
        'name_zh': row.get('工廠名稱', '').strip(),
        'industry_code': industry_code,
        'industry_zh': industry_zh,
        'address_zh': row.get('工廠地址', '').strip(),
        'city_zh': city_zh,
        'district_zh': district_zh,
        'registration_date': registration_date,
        'status': row.get('工廠登記狀態', '').strip(),
        'org_type': row.get('工廠組織型態', '').strip(),
        'products_zh': row.get('主要產品', '').strip(),
    }


def load_local_csv() -> list[dict[str, Any]]:
    """
    讀取本地已下載的 MOEA 工廠登記 CSV，轉換為標準格式。

    Returns:
        工廠資料 list
    """
    import csv

    if not LOCAL_CSV_PATH.exists():
        raise FileNotFoundError(f'Local CSV not found: {LOCAL_CSV_PATH}')

    print(f'Loading local CSV: {LOCAL_CSV_PATH}')
    with LOCAL_CSV_PATH.open(encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    factories = [normalize_moea_csv_row(row, i + 1) for i, row in enumerate(rows)]
    print(f'Loaded {len(factories)} records from local CSV.')
    return factories


def fallback_to_sample_data() -> list[dict[str, Any]]:
    """
    當 API 不可達且無本地 CSV 時，呼叫 generate_sample_data.py 產生大量模擬資料。

    Returns:
        50,000 筆模擬工廠資料（確保後續管線有足夠資料量）
    """
    import sys
    sys.path.insert(0, str(SCRIPT_DIR))
    from generate_sample_data import generate_factories  # type: ignore[import]

    count = 50000
    print(f'Using sample data generator as fallback ({count:,} records)...')
    return generate_factories(count)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / 'factories.json'

    factories: list[dict[str, Any]] = []
    download_succeeded = False

    # Step 0: 優先使用本地已下載的 CSV（moea_factories.csv）
    if LOCAL_CSV_PATH.exists():
        try:
            factories = load_local_csv()
            if factories:
                download_succeeded = True
                print(f'Using local CSV data: {len(factories):,} records.')
        except Exception as e:
            print(f'Local CSV load failed: {e}')

    if download_succeeded:
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(factories, f, ensure_ascii=False, indent=2)
        print(f'\nDone. Written {len(factories):,} records to {output_path}')
        return

    # Step 1: 嘗試透過 CKAN API 取得資源列表
    print(f'Attempting to fetch dataset {DATASET_ID} from data.gov.tw...')
    try:
        resources = fetch_dataset_resources(DATASET_ID)
        csv_resources = [r for r in resources if r.get('format', '').upper() == 'CSV']
        print(f'Found {len(csv_resources)} CSV resource(s).')

        for resource in csv_resources[:1]:  # 只取第一個 CSV
            url: str = resource.get('url', '')
            if not url:
                continue
            print(f'Downloading resource: {url}')
            raw_rows = download_csv_resource(url)
            factories = [normalize_row(row, i + 1) for i, row in enumerate(raw_rows)]
            download_succeeded = True
            print(f'Downloaded {len(factories)} records from CSV.')
            break

    except Exception as e:
        print(f'CSV download failed: {e}')

    # Step 2: 嘗試已知的 datastore_search URL
    if not download_succeeded:
        for url in KNOWN_RESOURCE_URLS:
            try:
                print(f'Trying datastore_search: {url}')
                records = download_with_pagination(url)
                factories = [normalize_row(row, i + 1) for i, row in enumerate(records)]
                if factories:
                    download_succeeded = True
                    print(f'Downloaded {len(factories)} records via datastore_search.')
                    break
            except Exception as e:
                print(f'  Failed: {e}')

    # Step 3: Fallback
    if not download_succeeded or not factories:
        print('All download attempts failed. Falling back to sample data.')
        factories = fallback_to_sample_data()

    # 寫出
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(factories, f, ensure_ascii=False, indent=2)

    print(f'\nDone. Written {len(factories)} records to {output_path}')
    if not download_succeeded:
        print('NOTE: Data is simulated. Run again with network access for real data.')


if __name__ == '__main__':
    main()
