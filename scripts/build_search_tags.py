"""
build_search_tags.py — 為每個 factory 生成 search_tags 欄位

標籤來源：
  A. 供應鏈關係（supply_chain_links 表）
  B. 產業鏈關鍵字對照表
  C. 台灣大廠供應鏈標籤
  D. 公司名稱關鍵字提取

執行前請確保 DB 已有 search_tags 欄位：
  ALTER TABLE factories ADD COLUMN search_tags TEXT DEFAULT '';
"""

import sqlite3
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR.parent / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 標籤來源 B：產業鏈關鍵字對照表
# ---------------------------------------------------------------------------

INDUSTRY_SUPPLY_CHAIN_TAGS: dict[str, list[str]] = {
    'Electronic Components Manufacturing': [
        'semiconductor supply chain', 'IC manufacturing', 'chip maker',
        'wafer fabrication', 'TSMC supply chain', 'MediaTek supply chain',
        'UMC supply chain', 'ASE supply chain', 'silicon',
    ],
    'Computer, Electronic & Optical Products Manufacturing': [
        'electronics supply chain', 'PCB', 'printed circuit board',
        'IC design', 'IC packaging', 'IC testing', 'OSAT',
        'Foxconn supply chain', 'Hon Hai supply chain',
        'Pegatron supply chain', 'Quanta supply chain',
    ],
    'Metal Products Manufacturing': [
        'metal parts', 'precision parts', 'CNC machining',
        'die casting', 'forging', 'stamping', 'metal forming',
        'fastener', 'screw', 'bolt', 'nut', 'spring',
        'tool and die', 'mold making',
    ],
    'Other Metal Products Manufacturing': [
        'metal parts', 'precision parts', 'CNC machining',
        'fastener', 'screw', 'bolt', 'nut', 'spring',
        'tool and die', 'mold making', 'sheet metal',
    ],
    'Machinery & Equipment Manufacturing': [
        'machine tool', 'CNC', 'lathe', 'milling machine',
        'automation', 'industrial robot', 'conveyor',
        'packaging machine', 'injection molding machine',
    ],
    'General-Purpose Machinery Manufacturing': [
        'pump', 'compressor', 'valve', 'bearing',
        'hydraulic', 'pneumatic', 'gear', 'motor',
        'general machinery',
    ],
    'Electrical Equipment Manufacturing': [
        'power supply', 'transformer', 'inverter', 'UPS',
        'cable', 'wire', 'connector', 'switch', 'relay',
        'electric vehicle parts', 'EV supply chain',
    ],
    'Other Electrical Equipment Manufacturing': [
        'power supply', 'transformer', 'inverter',
        'cable', 'wire', 'connector', 'switch', 'relay',
        'electrical components',
    ],
    'Plastics Products Manufacturing': [
        'plastic injection', 'blow molding', 'extrusion',
        'plastic parts', 'packaging', 'container',
    ],
    'Other Plastics Products Manufacturing': [
        'plastic injection', 'plastic parts', 'packaging', 'container',
        'plastic molding',
    ],
    'Rubber Products Manufacturing': [
        'rubber parts', 'seal', 'gasket', 'O-ring',
        'tire', 'rubber compound',
    ],
    'Chemical Materials & Fertilizers Manufacturing': [
        'chemical', 'petrochemical', 'resin', 'adhesive',
        'coating', 'paint', 'solvent',
    ],
    'Other Chemical Products Manufacturing': [
        'chemical', 'coating', 'adhesive', 'solvent',
        'specialty chemical', 'industrial chemical',
    ],
    'Textile Manufacturing': [
        'fabric', 'yarn', 'fiber', 'textile',
        'functional textile', 'sportswear material',
        'Nike supply chain', 'Adidas supply chain',
    ],
    'Apparel & Clothing Manufacturing': [
        'garment', 'clothing', 'OEM apparel',
        'sportswear', 'outdoor gear',
    ],
    'Food Manufacturing': [
        'food processing', 'beverage', 'snack',
        'frozen food', 'bakery', 'seasoning',
    ],
    'Beverage Manufacturing': [
        'beverage', 'drink', 'soft drink', 'bottling',
        'food processing',
    ],
    'Motor Vehicles & Parts Manufacturing': [
        'auto parts', 'automotive', 'car parts',
        'Tesla supply chain', 'Toyota supply chain',
        'brake', 'transmission', 'engine parts',
    ],
    'Basic Metal Manufacturing': [
        'steel', 'aluminum', 'copper', 'alloy',
        'stainless steel', 'casting', 'smelting',
    ],
    'Other Transport Equipment Manufacturing': [
        'bicycle', 'e-bike', 'motorcycle parts',
        'Giant supply chain', 'Merida supply chain',
        'yacht', 'boat', 'marine',
    ],
    'Pharmaceuticals Manufacturing': [
        'pharma', 'medical device', 'biotech',
        'drug manufacturing', 'API',
    ],
    'Other Electronic Components Manufacturing': [
        'LED', 'display', 'panel', 'touchscreen',
        'optical', 'lens', 'camera module',
        'sensor', 'MEMS', 'passive component',
        'capacitor', 'resistor', 'inductor',
    ],
    'Non-Metallic Mineral Products Manufacturing': [
        'cement', 'glass', 'ceramic', 'stone',
        'concrete', 'construction materials',
    ],
    'Petroleum & Coal Products Manufacturing': [
        'petroleum', 'petrochemical', 'refinery',
        'fuel', 'lubricant',
    ],
    'Printing & Reproduction of Recorded Media': [
        'printing', 'packaging printing', 'label',
        'publication',
    ],
    'Pulp, Paper & Paper Products Manufacturing': [
        'paper', 'pulp', 'paperboard', 'packaging',
        'corrugated', 'tissue',
    ],
    'Furniture Manufacturing': [
        'furniture', 'cabinet', 'wood furniture',
        'office furniture',
    ],
    'Wood & Bamboo Products Manufacturing': [
        'wood products', 'bamboo', 'timber',
        'flooring', 'wood panel',
    ],
    'Leather & Fur Products Manufacturing': [
        'leather', 'fur', 'shoe', 'bag',
        'leather goods',
    ],
}

# ---------------------------------------------------------------------------
# 標籤來源 C：台灣大廠英文別名對照表
# ---------------------------------------------------------------------------

MAJOR_BUYERS_TAGS: dict[str, list[str]] = {
    '台積電': ['TSMC', 'Taiwan Semiconductor'],
    '聯電': ['UMC', 'United Microelectronics'],
    '日月光': ['ASE', 'ASE Technology'],
    '聯發科': ['MediaTek'],
    '台達電': ['Delta Electronics'],
    '鴻海': ['Foxconn', 'Hon Hai'],
    '和碩': ['Pegatron'],
    '廣達': ['Quanta'],
    '仁寶': ['Compal'],
    '緯創': ['Wistron'],
    '英業達': ['Inventec'],
    '巨大': ['Giant'],
    '美利達': ['Merida'],
    '台塑': ['Formosa Plastics'],
    '中鋼': ['China Steel', 'CSC'],
    '統一': ['Uni-President'],
    '友達': ['AUO', 'AU Optronics'],
    '群創': ['Innolux'],
    '台灣水泥': ['TCC', 'Taiwan Cement'],
    '亞洲水泥': ['Asia Cement'],
    '嘉新水泥': ['Chia Hsin Cement'],
    '遠東': ['Far Eastern'],
    '台化': ['Formosa Chemicals'],
    '南亞': ['Nan Ya Plastics'],
    '中華電信': ['Chunghwa Telecom'],
}

# ---------------------------------------------------------------------------
# 標籤來源 D：公司名稱關鍵字提取對照表
# ---------------------------------------------------------------------------

NAME_KEYWORD_MAP: dict[str, str] = {
    '精密': 'precision',
    '光電': 'optoelectronics',
    '半導體': 'semiconductor',
    '自動化': 'automation',
    '模具': 'mold tooling',
    '螺絲': 'screw fastener',
    '彈簧': 'spring',
    '電池': 'battery',
    '面板': 'display panel',
    '觸控': 'touchscreen',
    '太陽能': 'solar',
    '風力': 'wind power',
    '生技': 'biotech',
    '醫療': 'medical',
    '航太': 'aerospace',
    '車用': 'automotive',
    '鋼鐵': 'steel',
    '鋁業': 'aluminum',
    '銅': 'copper',
    '塑膠': 'plastic',
    '橡膠': 'rubber',
    '化工': 'chemical',
    '紡織': 'textile',
    '食品': 'food',
    '包裝': 'packaging',
    '印刷': 'printing',
    '機械': 'machinery',
    '電機': 'electric motor',
    '電子': 'electronics',
    '通訊': 'telecom',
    '光學': 'optical',
    '雷射': 'laser',
    '感測': 'sensor',
    '儀器': 'instrument',
    '材料': 'materials',
    '封裝': 'packaging assembly',
    '測試': 'testing',
    '研磨': 'grinding polishing',
    '切割': 'cutting',
    '焊接': 'welding',
    '表面處理': 'surface treatment plating',
    '熱處理': 'heat treatment',
    '鍛造': 'forging',
    '鑄造': 'casting',
    '沖壓': 'stamping pressing',
    '射出': 'injection molding',
    '押出': 'extrusion',
    '吹塑': 'blow molding',
    'PCB': 'PCB printed circuit board',
    'LED': 'LED lighting',
    'IC': 'IC integrated circuit',
}


# ---------------------------------------------------------------------------
# 核心邏輯
# ---------------------------------------------------------------------------

def build_buyer_tag_lookup(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """
    從 supply_chain_links 建立 supplier_name → [buyer alias tags] 對照表。

    回傳：
      {
        '亞東預拌混凝土': ['Asia Cement supplier', '亞洲水泥 supplier'],
        ...
      }
    """
    lookup: dict[str, list[str]] = {}

    rows = conn.execute(
        'SELECT DISTINCT buyer_name, supplier_name FROM supply_chain_links '
        'WHERE relationship_type = ? AND supplier_name IS NOT NULL AND buyer_name IS NOT NULL',
        ('supplier',)
    ).fetchall()

    for buyer_name, supplier_name in rows:
        if not supplier_name or not supplier_name.strip():
            continue
        # 清理 supplier_name（有些有換行或編號前綴）
        supplier_name = supplier_name.strip()

        # 生成買方的 alias tags（包含英文別名）
        buyer_tags = [f'{buyer_name} supplier']
        for zh_key, en_aliases in MAJOR_BUYERS_TAGS.items():
            if zh_key in buyer_name:
                for alias in en_aliases:
                    buyer_tags.append(f'{alias} supplier')

        if supplier_name not in lookup:
            lookup[supplier_name] = []
        for tag in buyer_tags:
            if tag not in lookup[supplier_name]:
                lookup[supplier_name].append(tag)

    return lookup


def build_tags_for_factory(
    name_zh: str,
    name_en: str,
    industry_en: str,
    buyer_tag_lookup: dict[str, list[str]],
) -> str:
    """
    為單一工廠生成 search_tags 字串。
    """
    tags: list[str] = []

    # --- 標籤來源 A：供應鏈關係 ---
    # 用 name_zh 比對 buyer_tag_lookup（精確 + 部分匹配）
    for supplier_key, supplier_tags in buyer_tag_lookup.items():
        if name_zh and (name_zh in supplier_key or supplier_key in name_zh):
            tags.extend(supplier_tags)

    # --- 標籤來源 B：產業鏈關鍵字 ---
    if industry_en and industry_en in INDUSTRY_SUPPLY_CHAIN_TAGS:
        tags.extend(INDUSTRY_SUPPLY_CHAIN_TAGS[industry_en])

    # --- 標籤來源 C：大廠供應鏈（從 name_zh 比對）---
    # 如果公司本身就是大廠，加上自身的英文別名
    for zh_key, en_aliases in MAJOR_BUYERS_TAGS.items():
        if name_zh and zh_key in name_zh:
            tags.extend(en_aliases)

    # --- 標籤來源 D：公司名稱關鍵字提取 ---
    combined_name = f'{name_zh or ""}{name_en or ""}'
    for zh_kw, en_kw in NAME_KEYWORD_MAP.items():
        if zh_kw in combined_name:
            if en_kw not in tags:
                tags.append(en_kw)

    # 去重、過濾空值，用逗號空格連接
    seen: set[str] = set()
    unique_tags: list[str] = []
    for tag in tags:
        tag = tag.strip()
        if tag and tag not in seen:
            seen.add(tag)
            unique_tags.append(tag)

    return ', '.join(unique_tags)


def build_all_search_tags(conn: sqlite3.Connection) -> int:
    """
    為所有 factory 生成並寫入 search_tags。
    回傳更新筆數。
    """
    print('Building buyer tag lookup from supply_chain_links...')
    buyer_tag_lookup = build_buyer_tag_lookup(conn)
    print(f'  Found {len(buyer_tag_lookup)} unique supplier entries in supply_chain_links.')

    print('Fetching all factories...')
    rows = conn.execute(
        'SELECT id, name_zh, name_en, industry_en FROM factories'
    ).fetchall()
    print(f'  Total factories: {len(rows):,}')

    updates: list[tuple[str, int]] = []
    for factory_id, name_zh, name_en, industry_en in rows:
        tags = build_tags_for_factory(
            name_zh or '',
            name_en or '',
            industry_en or '',
            buyer_tag_lookup,
        )
        updates.append((tags, factory_id))

    print('Writing search_tags to database...')
    BATCH_SIZE = 2000
    updated = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        conn.executemany('UPDATE factories SET search_tags = ? WHERE id = ?', batch)
        conn.commit()
        updated += len(batch)
        print(f'  Updated {updated:,} / {len(updates):,}...')

    return updated


def rebuild_fts_with_search_tags(conn: sqlite3.Connection) -> None:
    """
    重建 factories_fts 以包含 search_tags 欄位。
    因 FTS5 不支援 ALTER，需 DROP 重建。
    """
    print('Rebuilding factories_fts with search_tags...')
    conn.execute('DROP TABLE IF EXISTS factories_fts')
    conn.execute("""
        CREATE VIRTUAL TABLE factories_fts USING fts5(
            name_en,
            industry_en,
            city_en,
            district_en,
            search_tags,
            content='factories',
            content_rowid='id'
        )
    """)
    conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
    conn.commit()
    print('FTS5 index rebuilt with search_tags.')


def verify_search_tags(conn: sqlite3.Connection) -> None:
    """驗證幾個關鍵搜尋詞是否能找到結果。"""
    test_queries = [
        ('TSMC supplier', 'TSMC supplier'),
        ('semiconductor supply chain', 'semiconductor supply chain'),
        ('precision', 'precision'),
        ('fastener screw', 'fastener screw'),
    ]
    print('\nVerification:')
    for label, query in test_queries:
        fts_terms = ' '.join(f'{t}*' for t in query.split())
        count = conn.execute(
            'SELECT COUNT(*) FROM factories_fts WHERE factories_fts MATCH ?',
            (fts_terms,)
        ).fetchone()[0]
        print(f'  "{label}" → {count} results')

    # 統計有 search_tags 的工廠比例
    total = conn.execute('SELECT COUNT(*) FROM factories').fetchone()[0]
    tagged = conn.execute(
        "SELECT COUNT(*) FROM factories WHERE search_tags IS NOT NULL AND search_tags != ''"
    ).fetchone()[0]
    print(f'\n  Factories with search_tags: {tagged:,} / {total:,} ({tagged*100//total}%)')


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    if not DB_PATH.exists():
        print(f'Database not found: {DB_PATH}')
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')

    try:
        # 確保 search_tags 欄位存在
        try:
            conn.execute("ALTER TABLE factories ADD COLUMN search_tags TEXT DEFAULT ''")
            conn.commit()
            print("Added search_tags column to factories.")
        except Exception as e:
            if 'duplicate column name' not in str(e).lower():
                raise
            print("search_tags column already exists, skipping ALTER.")

        updated = build_all_search_tags(conn)
        print(f'\nTotal updated: {updated:,} records.')

        rebuild_fts_with_search_tags(conn)
        verify_search_tags(conn)

    finally:
        conn.close()

    print(f'\nDone. Database: {DB_PATH}')


if __name__ == '__main__':
    main()
