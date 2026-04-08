"""
process_moea_data.py — 處理 MOEA 真實工廠登記資料

讀取 src/data/moea_factories.csv（BOM UTF-8，99,913 筆），處理並輸出
src/data/factories_translated.json。

處理邏輯：
  1. 讀取 CSV（BOM UTF-8 編碼）
  2. 解析並清理各欄位
  3. 民國年日期轉換為西元年
  4. 從產業類別欄位提取代碼並翻譯
  5. 從地址欄位提取縣市、區域並翻譯公司名稱
  6. 排除負責人姓名（個人隱私）
  7. 輸出 factories_translated.json
"""

import csv
import json
import re
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DATA_DIR = SRC_DIR / 'data'
TRANSLATIONS_DIR = SRC_DIR / 'translations'

INPUT_PATH = DATA_DIR / 'moea_factories.csv'
OUTPUT_PATH = DATA_DIR / 'factories_translated.json'


# ---------------------------------------------------------------------------
# 載入翻譯對照表
# ---------------------------------------------------------------------------

def load_translations() -> tuple[dict[str, str], dict[str, Any], dict[str, Any]]:
    with (TRANSLATIONS_DIR / 'industry_codes.json').open(encoding='utf-8') as f:
        industry_codes: dict[str, str] = json.load(f)
    with (TRANSLATIONS_DIR / 'locations.json').open(encoding='utf-8') as f:
        locations: dict[str, Any] = json.load(f)
    with (TRANSLATIONS_DIR / 'company_name_rules.json').open(encoding='utf-8') as f:
        name_rules: dict[str, Any] = json.load(f)
    return industry_codes, locations, name_rules


# ---------------------------------------------------------------------------
# 日期轉換：民國年 → 西元年
# ---------------------------------------------------------------------------

def convert_roc_date(roc_date: str) -> str:
    """
    將民國年日期字串轉為西元年 YYYY-MM-DD 格式。

    支援格式：
      - 7 位數字 YYYMMDD（如 1131126 → 2024-11-26）
      - 6 位數字 YYMMDD（如 850601 → 1996-06-01）
      - 其他格式回傳空字串

    Args:
        roc_date: 民國年日期字串

    Returns:
        西元年日期字串（YYYY-MM-DD）或空字串
    """
    if not roc_date:
        return ''

    digits = re.sub(r'\D', '', roc_date)

    if len(digits) == 7:
        # YYYMMDD：前3位是民國年
        roc_year = int(digits[:3])
        month = digits[3:5]
        day = digits[5:7]
    elif len(digits) == 6:
        # YYMMDD：前2位是民國年
        roc_year = int(digits[:2])
        month = digits[2:4]
        day = digits[4:6]
    else:
        return ''

    ad_year = roc_year + 1911

    # 基本驗證
    try:
        m = int(month)
        d = int(day)
        if not (1 <= m <= 12 and 1 <= d <= 31):
            return ''
    except ValueError:
        return ''

    return f'{ad_year}-{month}-{day}'


# ---------------------------------------------------------------------------
# 產業類別處理
# ---------------------------------------------------------------------------

# MOEA 資料的 2 位數字代碼 → 英文翻譯
# 對應 industry_codes.json 的 C-prefix 代碼
MOEA_INDUSTRY_MAP: dict[str, str] = {
    '08': 'Food Manufacturing',
    '09': 'Beverage Manufacturing',
    '10': 'Tobacco Products Manufacturing',
    '11': 'Textile Manufacturing',
    '12': 'Apparel & Clothing Accessories Manufacturing',
    '13': 'Leather, Fur & Related Products Manufacturing',
    '14': 'Wood & Bamboo Products Manufacturing',
    '15': 'Paper & Paper Products Manufacturing',
    '16': 'Printing & Reproduction of Recorded Media',
    '17': 'Petroleum & Coal Products Manufacturing',
    '18': 'Chemical Materials & Fertilizers Manufacturing',
    '19': 'Chemical Products Manufacturing',
    '20': 'Pharmaceuticals & Medicinal Chemical Manufacturing',
    '21': 'Rubber Products Manufacturing',
    '22': 'Plastics Products Manufacturing',
    '23': 'Non-Metallic Mineral Products Manufacturing',
    '24': 'Basic Metal Manufacturing',
    '25': 'Metal Products Manufacturing',
    '26': 'Electronic Components Manufacturing',
    '27': 'Computer, Electronic & Optical Products Manufacturing',
    '28': 'Electrical Equipment Manufacturing',
    '29': 'Machinery & Equipment Manufacturing',
    '30': 'Motor Vehicles & Parts Manufacturing',
    '31': 'Other Transport Equipment Manufacturing',
    '32': 'Furniture Manufacturing',
    '33': 'Other Manufacturing',
    '34': 'Repair & Installation of Machinery & Equipment',
}


def parse_industry(raw: str) -> tuple[str, str, str]:
    """
    解析產業類別欄位，回傳第一個產業的中文名稱、代碼、英文翻譯。

    多個產業類別以頓號（、）分隔，且頓號後跟隨 2 位數字。
    例：'25金屬製品製造業、29機械設備製造業' → 取第一個

    Args:
        raw: 原始產業類別欄位值

    Returns:
        (industry_zh, industry_code, industry_en)
    """
    if not raw:
        return '', '', ''

    # 以 「、後跟2位數字」分割，確保不誤切產業名稱內部的頓號
    parts = re.split(r'、(?=\d\d)', raw.strip())
    first = parts[0].strip()

    if not first:
        return '', '', ''

    # 提取前 2 位數字作為代碼
    m = re.match(r'^(\d{2})', first)
    if m:
        code = m.group(1)
        name_zh = first[2:]  # 去除數字前綴
    else:
        code = ''
        name_zh = first

    industry_en = MOEA_INDUSTRY_MAP.get(code, name_zh)

    return name_zh, code, industry_en


# ---------------------------------------------------------------------------
# 地址解析
# ---------------------------------------------------------------------------

def parse_location(location_str: str) -> tuple[str, str]:
    """
    從「工廠市鎮鄉村里」欄位提取縣市和區（鎮/鄉/市）。

    格式範例：「桃園市龜山區嶺頂里」→ city='桃園市', district='龜山區'

    Args:
        location_str: 工廠市鎮鄉村里欄位值

    Returns:
        (city_zh, district_zh)
    """
    if not location_str:
        return '', ''

    # 縣市：遇到第一個「市」或「縣」截止
    city_zh = ''
    for i, c in enumerate(location_str):
        if c in '市縣':
            city_zh = location_str[:i + 1]
            break

    if not city_zh:
        return '', ''

    # 區/鎮/鄉/市：縣市之後，遇到「區」「鎮」「鄉」「市」截止
    remaining = location_str[len(city_zh):]
    district_zh = ''
    for i, c in enumerate(remaining):
        if c in '區鎮鄉市':
            district_zh = remaining[:i + 1]
            break

    return city_zh, district_zh


def translate_city(city_zh: str, locations: dict[str, Any]) -> str:
    cities = locations.get('cities', {})
    return cities.get(city_zh, city_zh)


def translate_district(district_zh: str, locations: dict[str, Any]) -> str:
    districts = locations.get('districts', {})
    return districts.get(district_zh, district_zh)


# ---------------------------------------------------------------------------
# 公司名稱翻譯（規則翻譯）
# ---------------------------------------------------------------------------

def translate_company_name(
    name_zh: str,
    name_rules: dict[str, Any],
) -> tuple[str, bool]:
    """
    使用規則翻譯公司名稱（後綴 + 關鍵字替換）。
    不翻譯固有名詞（留給 LLM 處理）。

    Returns:
        (name_en, needs_llm)
    """
    suffixes: dict[str, str] = name_rules.get('suffixes', {})
    industry_keywords: dict[str, str] = name_rules.get('industry_keywords', {})
    common_words: dict[str, str] = name_rules.get('common_words', {})

    working = name_zh

    # Step 1: 後綴替換（最長優先）
    suffix_en = ''
    for zh_suffix, en_suffix in sorted(suffixes.items(), key=lambda x: -len(x[0])):
        if working.endswith(zh_suffix):
            suffix_en = en_suffix
            working = working[: -len(zh_suffix)]
            break

    # Step 2: 產業關鍵字替換（最長優先）
    for zh_kw, en_kw in sorted(industry_keywords.items(), key=lambda x: -len(x[0])):
        working = working.replace(zh_kw, f' {en_kw} ')

    # Step 3: 常用詞替換（最長優先）
    for zh_word, en_word in sorted(common_words.items(), key=lambda x: -len(x[0])):
        working = working.replace(zh_word, f' {en_word} ')

    # Step 4: 剩餘中文字元 → 需要 LLM
    remaining_chinese = re.findall(r'[\u4e00-\u9fff]+', working)
    needs_llm = len(remaining_chinese) > 0

    name_parts = [p for p in working.split() if p]
    if suffix_en:
        name_parts.append(suffix_en)

    name_en = ' '.join(name_parts).strip()

    if not name_en or all(ord(c) > 127 for c in name_en.replace(' ', '')):
        name_en = name_zh
        needs_llm = True

    return name_en, needs_llm


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def process_row(
    row: dict[str, str],
    row_id: int,
    industry_codes: dict[str, str],
    locations: dict[str, Any],
    name_rules: dict[str, Any],
) -> dict[str, Any]:
    """
    處理單筆 CSV 資料列，轉換為標準輸出格式。

    注意：故意不讀取「工廠負責人姓名」欄位（個人隱私保護）。
    """
    name_zh = row.get('工廠名稱', '').strip()
    tax_id = row.get('統一編號', '').strip()
    registration_no = row.get('工廠登記編號', '').strip()
    address_zh = row.get('工廠地址', '').strip()
    location_str = row.get('工廠市鎮鄉村里', '').strip()
    org_type = row.get('工廠組織型態', '').strip()
    raw_industry = row.get('產業類別', '').strip()
    raw_date = row.get('工廠登記核准日期', '').strip()
    status = row.get('工廠登記狀態', '').strip()
    products_zh = row.get('主要產品', '').strip()

    # 產業解析
    industry_zh, industry_code, industry_en = parse_industry(raw_industry)

    # 地址解析
    city_zh, district_zh = parse_location(location_str)
    city_en = translate_city(city_zh, locations)
    district_en = translate_district(district_zh, locations)

    # 日期轉換
    registration_date = convert_roc_date(raw_date)

    # 公司名稱翻譯
    name_en, needs_llm = translate_company_name(name_zh, name_rules)

    return {
        'id': row_id,
        'tax_id': tax_id,
        'registration_no': registration_no,
        'name_zh': name_zh,
        'name_en': name_en,
        'needs_llm_translation': needs_llm,
        'industry_zh': industry_zh,
        'industry_code': industry_code,
        'industry_en': industry_en,
        'address_zh': address_zh,
        'city_zh': city_zh,
        'city_en': city_en,
        'district_zh': district_zh,
        'district_en': district_en,
        'registration_date': registration_date,
        'status': status,
        'org_type': org_type,
        'products_zh': products_zh,
    }


def main() -> None:
    if not INPUT_PATH.exists():
        print(f'Input file not found: {INPUT_PATH}')
        return

    print(f'Loading translation tables...')
    industry_codes, locations, name_rules = load_translations()

    print(f'Reading {INPUT_PATH}...')
    results: list[dict[str, Any]] = []

    with INPUT_PATH.open(encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row_id, row in enumerate(reader, start=1):
            record = process_row(row, row_id, industry_codes, locations, name_rules)
            results.append(record)

            if row_id % 10000 == 0:
                print(f'  Processed {row_id:,} rows...')

    print(f'Processed {len(results):,} total rows.')

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f'Writing to {OUTPUT_PATH}...')
    with OUTPUT_PATH.open('w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 統計摘要
    needs_llm_count = sum(1 for r in results if r.get('needs_llm_translation'))
    no_industry = sum(1 for r in results if not r.get('industry_en'))
    no_city = sum(1 for r in results if not r.get('city_en') or r.get('city_en') == r.get('city_zh'))
    no_date = sum(1 for r in results if not r.get('registration_date'))

    print(f'\nProcessing complete. Written to {OUTPUT_PATH}')
    print(f'  Total records      : {len(results):,}')
    print(f'  Needs LLM (name)   : {needs_llm_count:,} ({needs_llm_count / len(results) * 100:.1f}%)')
    print(f'  Missing industry   : {no_industry:,}')
    print(f'  City not translated: {no_city:,}')
    print(f'  Missing reg date   : {no_date:,}')


if __name__ == '__main__':
    main()
