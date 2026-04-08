"""
translate_factories.py — 將工廠資料從中文翻譯為英文

翻譯策略：
1. 產業類別 → industry_codes.json 查表
2. 縣市 / 區域 → locations.json 查表
3. 公司名稱 → company_name_rules.json 規則替換；無法翻譯的字元標記 needs_llm=True
"""

import json
import re
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DATA_DIR = SRC_DIR / 'data'
TRANSLATIONS_DIR = SRC_DIR / 'translations'


# ---------------------------------------------------------------------------
# 載入對照表
# ---------------------------------------------------------------------------

def load_translations() -> tuple[dict[str, str], dict[str, Any], dict[str, Any]]:
    """
    載入三個翻譯對照表。

    Returns:
        (industry_codes, locations, name_rules)
    """
    with (TRANSLATIONS_DIR / 'industry_codes.json').open(encoding='utf-8') as f:
        industry_codes: dict[str, str] = json.load(f)

    with (TRANSLATIONS_DIR / 'locations.json').open(encoding='utf-8') as f:
        locations: dict[str, Any] = json.load(f)

    with (TRANSLATIONS_DIR / 'company_name_rules.json').open(encoding='utf-8') as f:
        name_rules: dict[str, Any] = json.load(f)

    return industry_codes, locations, name_rules


# ---------------------------------------------------------------------------
# 翻譯函數
# ---------------------------------------------------------------------------

def translate_industry(code: str, industry_codes: dict[str, str]) -> str:
    """
    將產業類別代碼或中文名稱翻譯為英文。
    優先使用精確代碼查表，其次遍歷對照表值域比對中文名稱。

    Args:
        code: 產業類別代碼（如 'C271'）或中文名稱
        industry_codes: 代碼 → 英文對照表

    Returns:
        英文產業名稱，查無則回傳 'Unknown Industry'
    """
    if code in industry_codes:
        return industry_codes[code]

    # 嘗試以中文名稱在值域中搜尋（factories.json 可能只有中文）
    for key, value in industry_codes.items():
        # 如果 key 是中文名稱形式（不是英文縮寫代碼），跳過
        pass

    return 'Unknown Industry'


def translate_industry_by_name(name_zh: str, industry_codes: dict[str, str]) -> str:
    """
    以中文產業名稱查詢英文翻譯。
    先嘗試精確比對，再嘗試部分包含比對。

    Args:
        name_zh: 中文產業名稱
        industry_codes: 代碼 → 英文對照表（值域中也包含英文）

    Returns:
        英文產業名稱，查無則回傳 name_zh 本身（待 LLM 翻譯）
    """
    # 建立反向對照表（中文 → 英文），從翻譯對照表中自行維護的中文名稱對應
    # 由於 industry_codes.json key 是代碼，需要有對應中文名稱才能查反向
    # 此處使用 generate_sample_data.py 裡定義的 industry 資料建立的中文名稱
    ZH_TO_EN: dict[str, str] = {
        '半導體製造業': 'Semiconductor Manufacturing',
        '被動電子元件製造業': 'Passive Components Manufacturing',
        '印刷電路板製造業': 'Printed Circuit Boards Manufacturing',
        '顯示器製造業': 'Display Panels Manufacturing',
        '太陽電池及模組製造業': 'Solar Cells & Modules Manufacturing',
        '其他電子零組件製造業': 'Other Electronic Components Manufacturing',
        '電腦及其周邊設備製造業': 'Computers & Peripheral Equipment Manufacturing',
        '通訊傳播設備製造業': 'Communications Equipment Manufacturing',
        '量測、導航、控制設備及精密光學儀器製造業': 'Measuring & Testing Instruments Manufacturing',
        '電力機械器材製造業': 'Electric Motors, Generators & Transformers Manufacturing',
        '電池製造業': 'Batteries & Accumulators Manufacturing',
        '配線器材製造業': 'Wiring & Wiring Devices Manufacturing',
        '其他電力設備及配備製造業': 'Other Electrical Equipment Manufacturing',
        '一般機械設備製造業': 'General-Purpose Machinery Manufacturing',
        '特殊機械設備製造業': 'Special-Purpose Machinery Manufacturing',
        '金屬切削工具機製造業': 'Metalworking Machinery & Machine Tools Manufacturing',
        '半導體及顯示器生產用機械設備製造業': 'Semiconductor & Display Manufacturing Equipment',
        '金屬結構及建築組件製造業': 'Structural Metal Products Manufacturing',
        '手工具及鎖具製造業': 'Hand Tools & General Hardware Manufacturing',
        '螺絲、螺帽及其他扣件製造業': 'Fasteners & Springs Manufacturing',
        '其他金屬製品製造業': 'Other Metal Products Manufacturing',
        '航空器及其零件製造業': 'Aircraft & Spacecraft Manufacturing',
        '軍用車輛製造業': 'Military Vehicles Manufacturing',
        '其他運輸工具及其零件製造業': 'Other Transport Equipment Manufacturing',
        '安全及監控設備製造業': 'Security & Surveillance Equipment Manufacturing',
        '醫療器材及用品製造業': 'Medical & Dental Instruments Manufacturing',
        '基本化學材料製造業': 'Basic Chemical Manufacturing',
        '塑膠及合成橡膠原料製造業': 'Plastics & Synthetic Rubber in Primary Forms Manufacturing',
        '塑膠包裝材料製造業': 'Plastic Packaging Products Manufacturing',
        '其他塑膠製品製造業': 'Other Plastics Products Manufacturing',
    }

    if name_zh in ZH_TO_EN:
        return ZH_TO_EN[name_zh]

    # 部分比對 fallback
    for zh, en in ZH_TO_EN.items():
        if name_zh in zh or zh in name_zh:
            return en

    return name_zh  # 無法翻譯，原文回傳（後續 LLM 處理）


def translate_city(city_zh: str, locations: dict[str, Any]) -> str:
    """
    將中文縣市翻譯為英文。

    Args:
        city_zh: 中文縣市名稱
        locations: locations.json 內容

    Returns:
        英文縣市名稱，查無則回傳 city_zh
    """
    cities = locations.get('cities', {})
    return cities.get(city_zh, city_zh)


def translate_district(district_zh: str, locations: dict[str, Any]) -> str:
    """
    將中文區域翻譯為英文。

    Args:
        district_zh: 中文區域名稱（含 '區'/'市'/'鄉'/'鎮' 等）
        locations: locations.json 內容

    Returns:
        英文區域名稱，查無則回傳 district_zh
    """
    districts = locations.get('districts', {})
    return districts.get(district_zh, district_zh)


def translate_company_name(
    name_zh: str,
    name_rules: dict[str, Any],
) -> tuple[str, bool]:
    """
    使用規則翻譯公司名稱。

    翻譯策略（依序）：
    1. 後綴替換（股份有限公司 → Co., Ltd.）
    2. 產業關鍵字替換（精密 → Precision 等）
    3. 常用詞替換（台灣 → Taiwan 等）
    4. 剩餘中文字元：檢查是否需要 LLM

    Args:
        name_zh: 原始中文公司名稱
        name_rules: company_name_rules.json 內容

    Returns:
        (name_en, needs_llm): 英文名稱與是否需要 LLM 翻譯的旗標
    """
    suffixes: dict[str, str] = name_rules.get('suffixes', {})
    industry_keywords: dict[str, str] = name_rules.get('industry_keywords', {})
    common_words: dict[str, str] = name_rules.get('common_words', {})

    working = name_zh

    # Step 1: 後綴替換（從最長的開始，避免部分替換）
    suffix_en = ''
    for zh_suffix, en_suffix in sorted(suffixes.items(), key=lambda x: -len(x[0])):
        if working.endswith(zh_suffix):
            suffix_en = en_suffix
            working = working[: -len(zh_suffix)]
            break

    # Step 2: 產業關鍵字替換（較長關鍵字優先）
    for zh_kw, en_kw in sorted(industry_keywords.items(), key=lambda x: -len(x[0])):
        working = working.replace(zh_kw, f' {en_kw} ')

    # Step 3: 常用詞替換（較長關鍵字優先）
    for zh_word, en_word in sorted(common_words.items(), key=lambda x: -len(x[0])):
        working = working.replace(zh_word, f' {en_word} ')

    # Step 4: 檢查是否還有中文字元
    remaining_chinese = re.findall(r'[\u4e00-\u9fff]+', working)
    needs_llm = len(remaining_chinese) > 0

    # 清理多餘空白
    name_parts = [p for p in working.split() if p]
    if suffix_en:
        name_parts.append(suffix_en)

    name_en = ' '.join(name_parts).strip()

    # 若完全無法翻譯（只有中文），標記並原文填入
    if not name_en or all(ord(c) > 127 for c in name_en.replace(' ', '')):
        name_en = name_zh
        needs_llm = True

    return name_en, needs_llm


# ---------------------------------------------------------------------------
# 主要翻譯流程
# ---------------------------------------------------------------------------

def translate_factory(
    factory: dict[str, Any],
    industry_codes: dict[str, str],
    locations: dict[str, Any],
    name_rules: dict[str, Any],
) -> dict[str, Any]:
    """
    翻譯單筆工廠資料。

    Args:
        factory: 原始工廠 dict（來自 factories.json）
        industry_codes: 產業代碼對照表
        locations: 縣市 / 區域對照表
        name_rules: 公司名稱翻譯規則

    Returns:
        翻譯後的工廠 dict，格式符合 factories_translated.json spec
    """
    # 產業翻譯：先嘗試代碼，再嘗試中文名稱
    industry_zh: str = factory.get('industry_zh', '')
    industry_code: str = factory.get('industry_code', '')
    industry_en = translate_industry(industry_code, industry_codes)
    if industry_en == 'Unknown Industry' and industry_zh:
        industry_en = translate_industry_by_name(industry_zh, industry_codes)

    # 縣市翻譯
    city_zh: str = factory.get('city_zh', '')
    city_en = translate_city(city_zh, locations)

    # 區域翻譯
    district_zh: str = factory.get('district_zh', '')
    district_en = translate_district(district_zh, locations)

    # 公司名稱翻譯
    name_zh: str = factory.get('name_zh', '')
    name_en, needs_llm = translate_company_name(name_zh, name_rules)

    return {
        'id': factory.get('id'),
        'name_zh': name_zh,
        'name_en': name_en,
        'needs_llm_translation': needs_llm,
        'industry_zh': industry_zh,
        'industry_en': industry_en,
        'address_zh': factory.get('address_zh', ''),
        'city_en': city_en,
        'district_en': district_en,
        'registration_date': factory.get('registration_date', ''),
        'tax_id': factory.get('tax_id', ''),
        'status': factory.get('status', ''),
    }


def main() -> None:
    input_path = DATA_DIR / 'factories.json'
    output_path = DATA_DIR / 'factories_translated.json'

    if not input_path.exists():
        print(f'Input file not found: {input_path}')
        print('Please run generate_sample_data.py first.')
        return

    print(f'Loading {input_path}...')
    with input_path.open(encoding='utf-8') as f:
        factories: list[dict[str, Any]] = json.load(f)

    print('Loading translation tables...')
    industry_codes, locations, name_rules = load_translations()

    print(f'Translating {len(factories)} records...')
    translated = [
        translate_factory(factory, industry_codes, locations, name_rules)
        for factory in factories
    ]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8') as f:
        json.dump(translated, f, ensure_ascii=False, indent=2)

    # 統計
    needs_llm_count = sum(1 for r in translated if r.get('needs_llm_translation'))
    print(f'\nTranslation complete. Written to {output_path}')
    print(f'  Total records   : {len(translated)}')
    print(f'  Needs LLM       : {needs_llm_count} ({needs_llm_count / len(translated) * 100:.1f}%)')
    print(f'  Rule-translated : {len(translated) - needs_llm_count}')


if __name__ == '__main__':
    main()
