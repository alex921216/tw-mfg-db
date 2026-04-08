"""
generate_sample_data.py — 生成 500 筆模擬台灣工廠資料

產業涵蓋：半導體、AI（電子/資訊）、軍工、安控、通訊
地理重點：新竹、台中、台南、高雄等製造業重鎮
"""

import json
import random
import string
from datetime import date, timedelta
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'

# ---------------------------------------------------------------------------
# 名稱構成素材
# ---------------------------------------------------------------------------

PREFIXES: list[str] = [
    '台灣', '臺灣', '新竹', '台中', '台南', '高雄', '桃園', '彰化',
    '聯合', '統一', '全球', '東亞', '亞太', '太平洋', '鴻海', '富泰',
    '華碩', '廣達', '緯創', '英業達', '仁寶', '大立', '長億', '勝利',
    '冠德', '中鋼', '正崴', '金仁寶', '群光', '光寶', '上銀',
    '豐泰', '和碩', '建碁', '景碩', '欣興', '南亞', '台塑',
]

MIDDLES: list[str] = [
    '精密', '半導體', '科技', '電子', '機械', '光電', '通訊', '材料',
    '自動化', '感測', '光學', '雷射', '能源', '太陽能', '儲能',
    '航太', '國防', '安控', '監控', '無線', '網路', '系統', '設備',
    '儀器', '工具', '模組', '晶片', '封裝', '測試', '研發',
    '先進', '創新', '卓越', '鴻益', '盛達', '宏達', '崇德',
]

SUFFIXES: list[str] = [
    '股份有限公司', '股份有限公司', '股份有限公司',  # weighted higher
    '有限公司', '有限公司',
    '工業股份有限公司',
]

# ---------------------------------------------------------------------------
# 產業資料（依 industry_codes.json 的代碼格式）
# ---------------------------------------------------------------------------

INDUSTRIES: list[dict[str, str]] = [
    # 半導體 / 電子零組件
    {'code': 'C271', 'name_zh': '半導體製造業'},
    {'code': 'C272', 'name_zh': '被動電子元件製造業'},
    {'code': 'C273', 'name_zh': '印刷電路板製造業'},
    {'code': 'C274', 'name_zh': '顯示器製造業'},
    {'code': 'C275', 'name_zh': '太陽電池及模組製造業'},
    {'code': 'C279', 'name_zh': '其他電子零組件製造業'},
    # 電腦、電子產品（AI 相關）
    {'code': 'C281', 'name_zh': '電腦及其周邊設備製造業'},
    {'code': 'C282', 'name_zh': '通訊傳播設備製造業'},
    {'code': 'C284', 'name_zh': '量測、導航、控制設備及精密光學儀器製造業'},
    # 電力設備
    {'code': 'C291', 'name_zh': '電力機械器材製造業'},
    {'code': 'C292', 'name_zh': '電池製造業'},
    {'code': 'C293', 'name_zh': '配線器材製造業'},
    {'code': 'C299', 'name_zh': '其他電力設備及配備製造業'},
    # 機械設備
    {'code': 'C301', 'name_zh': '一般機械設備製造業'},
    {'code': 'C302', 'name_zh': '特殊機械設備製造業'},
    {'code': 'C304', 'name_zh': '金屬切削工具機製造業'},
    {'code': 'C305', 'name_zh': '半導體及顯示器生產用機械設備製造業'},
    # 金屬製品
    {'code': 'C261', 'name_zh': '金屬結構及建築組件製造業'},
    {'code': 'C264', 'name_zh': '手工具及鎖具製造業'},
    {'code': 'C265', 'name_zh': '螺絲、螺帽及其他扣件製造業'},
    {'code': 'C269', 'name_zh': '其他金屬製品製造業'},
    # 軍工 / 航太
    {'code': 'C323', 'name_zh': '航空器及其零件製造業'},
    {'code': 'C324', 'name_zh': '軍用車輛製造業'},
    {'code': 'C329', 'name_zh': '其他運輸工具及其零件製造業'},
    # 安控 / 監控
    {'code': 'C346', 'name_zh': '安全及監控設備製造業'},
    {'code': 'C345', 'name_zh': '醫療器材及用品製造業'},
    # 化學 / 材料
    {'code': 'C201', 'name_zh': '基本化學材料製造業'},
    {'code': 'C203', 'name_zh': '塑膠及合成橡膠原料製造業'},
    # 塑膠
    {'code': 'C221', 'name_zh': '塑膠包裝材料製造業'},
    {'code': 'C229', 'name_zh': '其他塑膠製品製造業'},
]

# 依指定五大產業給予不同權重
INDUSTRY_WEIGHTS: list[int] = [
    10, 6, 8, 5, 4, 4,   # 半導體 / 電子零組件（6 項）
    8, 9, 5,              # 電腦 / 通訊 / AI（3 項）
    5, 5, 4, 3,           # 電力設備（4 項）
    5, 4, 4, 3,           # 機械設備（4 項）
    3, 3, 3, 3,           # 金屬製品（4 項）
    6, 4, 3,              # 軍工 / 航太（3 項）
    8, 4,                 # 安控 / 監控（2 項）
    3, 3,                 # 化學 / 材料（2 項）
    2, 2,                 # 塑膠（2 項）
]

# ---------------------------------------------------------------------------
# 地理資料（製造業重鎮優先）
# ---------------------------------------------------------------------------

LOCATIONS: list[dict[str, Any]] = [
    # 新竹縣市 — 半導體重鎮
    {'city': '新竹市', 'districts': ['東區', '北區', '香山區'], 'weight': 15},
    {'city': '新竹縣', 'districts': ['竹北市', '竹東鎮', '新埔鎮', '關西鎮', '湖口鄉', '新豐鄉'], 'weight': 14},
    # 台中 — 工具機、精密機械
    {'city': '臺中市', 'districts': ['西屯區', '南屯區', '北屯區', '豐原區', '大里區', '太平區', '沙鹿區', '梧棲區', '大甲區'], 'weight': 14},
    # 台南 — 南科、石化
    {'city': '臺南市', 'districts': ['新營區', '永康區', '仁德區', '歸仁區', '善化區', '安平區', '安南區', '南區', '北區'], 'weight': 12},
    # 高雄 — 重工業
    {'city': '高雄市', 'districts': ['三民區', '楠梓區', '仁武區', '大社區', '岡山區', '路竹區', '橋頭區', '燕巢區'], 'weight': 12},
    # 桃園 — 航空城、工業區
    {'city': '桃園市', 'districts': ['桃園區', '中壢區', '大溪區', '楊梅區', '蘆竹區', '龜山區', '八德區', '平鎮區'], 'weight': 10},
    # 台北 / 新北
    {'city': '臺北市', 'districts': ['中正區', '內湖區', '南港區', '士林區', '信義區'], 'weight': 6},
    {'city': '新北市', 'districts': ['板橋區', '新莊區', '中和區', '土城區', '樹林區', '鶯歌區', '三重區', '蘆洲區', '五股區', '林口區'], 'weight': 8},
    # 彰化 — 螺絲王國
    {'city': '彰化縣', 'districts': ['彰化市', '員林市', '和美鎮', '鹿港鎮', '溪湖鎮'], 'weight': 7},
    # 苗栗
    {'city': '苗栗縣', 'districts': ['頭份市', '苗栗市', '竹南鎮'], 'weight': 2},
]

LOCATION_WEIGHTS: list[int] = [loc['weight'] for loc in LOCATIONS]

# ---------------------------------------------------------------------------
# 地址路名素材
# ---------------------------------------------------------------------------

ROAD_TYPES: list[str] = ['路', '街', '大道', '工業路', '科技路']
ROAD_NAMES: list[str] = [
    '中山', '中正', '民族', '民主', '民權', '光復', '博愛', '仁愛',
    '工業', '科技', '創業', '建業', '自強', '自由', '文化', '學府',
    '新興', '興業', '成功', '和平', '三民', '五福', '復興', '忠孝',
]

# ---------------------------------------------------------------------------
# 工具函數
# ---------------------------------------------------------------------------

def _random_date(start_year: int = 1980, end_year: int = 2024) -> str:
    """產生 start_year 到 end_year 之間的隨機日期，格式 YYYY-MM-DD。"""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def _random_tax_id() -> str:
    """產生 8 位模擬統一編號。"""
    return ''.join(random.choices(string.digits, k=8))


def _random_address(city: str, district: str) -> str:
    road = random.choice(ROAD_NAMES) + random.choice(ROAD_TYPES)
    section = random.choice(['', '一段', '二段', '三段'])
    number = random.randint(1, 500)
    floor_part = ''
    if random.random() < 0.3:
        floor_part = f'{random.randint(1, 15)}樓'
    return f'{city}{district}{road}{section}{number}號{floor_part}'


def _random_company_name() -> str:
    """組合前綴 + 中段 + 後綴，避免重複組合。"""
    prefix = random.choice(PREFIXES)
    middle = random.choice(MIDDLES)
    suffix = random.choice(SUFFIXES)
    return f'{prefix}{middle}{suffix}'


def _random_status() -> str:
    """90% 正常營業，5% 停業，5% 歇業。"""
    return random.choices(
        ['正常營業', '停業', '歇業'],
        weights=[90, 5, 5],
        k=1
    )[0]


# ---------------------------------------------------------------------------
# 主要生成邏輯
# ---------------------------------------------------------------------------

def generate_factories(count: int = 500) -> list[dict[str, Any]]:
    """
    生成 count 筆模擬台灣工廠資料。

    Returns:
        list of factory dicts with raw Chinese fields.
    """
    factories: list[dict[str, Any]] = []
    used_tax_ids: set[str] = set()

    for i in range(count):
        # 統一編號（唯一）
        tax_id = _random_tax_id()
        while tax_id in used_tax_ids:
            tax_id = _random_tax_id()
        used_tax_ids.add(tax_id)

        # 產業
        industry = random.choices(INDUSTRIES, weights=INDUSTRY_WEIGHTS, k=1)[0]

        # 地點
        location = random.choices(LOCATIONS, weights=LOCATION_WEIGHTS, k=1)[0]
        city: str = location['city']
        district: str = random.choice(location['districts'])

        factories.append({
            'id': i + 1,
            'tax_id': tax_id,
            'name_zh': _random_company_name(),
            'industry_code': industry['code'],
            'industry_zh': industry['name_zh'],
            'address_zh': _random_address(city, district),
            'city_zh': city,
            'district_zh': district,
            'registration_date': _random_date(),
            'status': _random_status(),
        })

    return factories


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / 'factories.json'

    print('Generating 500 sample factory records...')
    factories = generate_factories(500)

    with output_path.open('w', encoding='utf-8') as f:
        json.dump(factories, f, ensure_ascii=False, indent=2)

    print(f'Done. Written {len(factories)} records to {output_path}')

    # 簡單統計
    city_counts: dict[str, int] = {}
    for factory in factories:
        city = factory['city_zh']
        city_counts[city] = city_counts.get(city, 0) + 1
    print('\nCity distribution:')
    for city, count in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f'  {city}: {count}')


if __name__ == '__main__':
    main()
