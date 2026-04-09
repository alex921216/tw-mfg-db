"""
generate_company_profiles.py — 為每家工廠生成英文 Company Profile

執行方式（在 src/ 目錄下）：
  python scripts/generate_company_profiles.py

功能：
  1. ALTER TABLE factories ADD COLUMN company_profile_en TEXT（若尚未存在）
  2. 為每家工廠根據現有欄位生成 3-5 句英文簡介
  3. 批次更新至 DB（每 1000 筆 commit 一次）
  4. 印出統計報告
"""

import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 產品關鍵字對照（從中文公司名稱提取）
# ---------------------------------------------------------------------------

PRODUCT_KEYWORDS = {
  # 加工方式
  '精密': 'precision-engineered components',
  '模具': 'molds and tooling',
  '沖壓': 'metal stamping parts',
  '鑄造': 'cast metal products',
  '鍛造': 'forged metal products',
  '射出': 'injection-molded plastic products',
  '押出': 'extruded products',
  '研磨': 'precision grinding services',
  '切割': 'cutting and machining services',
  '焊接': 'welding and fabrication services',
  '表面處理': 'surface treatment and plating services',
  '熱處理': 'heat treatment services',
  '電鍍': 'electroplating services',
  '烤漆': 'painting and coating services',
  '加工': 'precision machining services',
  '製造': 'custom manufacturing services',
  '加工廠': 'processing and manufacturing services',
  # 產品類型
  '螺絲': 'screws, bolts, and fasteners',
  '螺帽': 'nuts and fasteners',
  '螺栓': 'bolts and fasteners',
  '彈簧': 'springs and elastic components',
  '軸承': 'bearings',
  '齒輪': 'gears and transmission parts',
  '閥': 'valves and fittings',
  '泵': 'pumps',
  '馬達': 'motors and drives',
  '電動機': 'electric motors',
  '發電機': 'generators',
  '變壓器': 'transformers',
  '電源': 'power supplies',
  '連接器': 'connectors',
  '線材': 'cables and wiring',
  '電線': 'electrical cables',
  '電纜': 'power cables',
  '電池': 'batteries',
  'PCB': 'printed circuit boards (PCB)',
  'LED': 'LED lighting products',
  'IC': 'integrated circuits',
  '半導體': 'semiconductor products',
  '晶片': 'semiconductor chips',
  '面板': 'display panels',
  '觸控': 'touch panels',
  '光電': 'optoelectronic products',
  '光學': 'optical components and lenses',
  '感測': 'sensors',
  '感應': 'sensors and detection systems',
  '太陽能': 'solar energy equipment',
  '風力': 'wind power equipment',
  # 材料
  '鋼鐵': 'steel products',
  '不鏽鋼': 'stainless steel products',
  '鋁': 'aluminum products',
  '銅': 'copper products',
  '塑膠': 'plastic products',
  '橡膠': 'rubber products',
  '玻璃': 'glass products',
  '陶瓷': 'ceramic products',
  '碳纖維': 'carbon fiber products',
  '複合材料': 'composite materials',
  # 行業特定
  '食品': 'processed food products',
  '飲料': 'beverages',
  '紡織': 'textiles and fabrics',
  '成衣': 'garments and apparel',
  '服飾': 'garments and apparel',
  '鞋': 'footwear',
  '家具': 'furniture',
  '包裝': 'packaging materials',
  '印刷': 'printed materials',
  '化工': 'chemical products',
  '化學': 'chemical products',
  '藥品': 'pharmaceutical products',
  '製藥': 'pharmaceutical products',
  '醫療': 'medical devices',
  '醫材': 'medical devices',
  '汽車': 'automotive parts',
  '自行車': 'bicycle components',
  '腳踏車': 'bicycle components',
  '機車': 'motorcycle parts',
  '航太': 'aerospace components',
  '航空': 'aerospace components',
  '船舶': 'marine equipment',
  '機械': 'machinery and equipment',
  '自動化': 'automation equipment',
  '機器人': 'robotics',
  '儀器': 'instruments and testing equipment',
  '電子': 'electronic components',
  '通訊': 'telecommunications equipment',
  '安控': 'security and surveillance equipment',
  '監控': 'monitoring systems',
  '水處理': 'water treatment equipment',
  '環保': 'environmental protection equipment',
  '農機': 'agricultural machinery',
  '五金': 'hardware components',
  '工具': 'tools and implements',
  '刀具': 'cutting tools',
  '模板': 'formwork and templates',
  '沙發': 'sofas and upholstered furniture',
  '木材': 'wood and timber products',
  '紙': 'paper products',
  '印染': 'dyeing and printing services',
}

# 較長關鍵字優先（避免短關鍵字遮蓋長的）
SORTED_PRODUCT_KEYWORDS = sorted(PRODUCT_KEYWORDS.keys(), key=len, reverse=True)

# ---------------------------------------------------------------------------
# 地理位置優勢對照
# ---------------------------------------------------------------------------

LOCATION_ADVANTAGES = {
  'Hsinchu City': "Taiwan's Silicon Valley and semiconductor hub",
  'Hsinchu County': 'home to Hsinchu Science Park, a global semiconductor center',
  'Taichung City': "Taiwan's precision machinery and machine tool capital",
  'Tainan City': 'home to Southern Taiwan Science Park (STSP) and semiconductor clusters',
  'Kaohsiung City': "Taiwan's heavy industry and petrochemical center",
  'Taoyuan City': "Taiwan's industrial corridor with excellent logistics access",
  'New Taipei City': "Taiwan's largest manufacturing base by number of factories",
  'Changhua County': "Taiwan's fastener kingdom, producing over 50% of global screw exports",
  'Miaoli County': 'known for glass, ceramics, and chemical manufacturing',
  'Taipei City': "Taiwan's business and R&D headquarters",
  'Yunlin County': 'agricultural processing and petrochemical manufacturing hub',
  'Pingtung County': 'food processing and agricultural manufacturing center',
  'Chiayi County': 'wood products and traditional manufacturing area',
  'Chiayi City': 'central Taiwan gateway with strong manufacturing tradition',
  'Nantou County': "central Taiwan's manufacturing and agri-processing base",
  'Yilan County': 'northeast Taiwan food processing and chemical center',
  'Keelung City': 'northern Taiwan port city with marine and logistics industry',
  'Taitung County': 'eastern Taiwan specialty manufacturing base',
  'Hualien County': 'eastern Taiwan marble and materials manufacturing',
  'Penghu County': 'island-based specialty manufacturing',
}

# ---------------------------------------------------------------------------
# 核心邏輯
# ---------------------------------------------------------------------------

def extract_products(name_zh: str) -> str | None:
  """從中文公司名稱提取最匹配的產品描述"""
  if not name_zh:
    return None
  for keyword in SORTED_PRODUCT_KEYWORDS:
    if keyword in name_zh:
      return PRODUCT_KEYWORDS[keyword]
  return None


def get_location_advantage(city_en: str) -> str | None:
  """取得城市的產業優勢描述"""
  if not city_en:
    return None
  return LOCATION_ADVANTAGES.get(city_en)


def parse_certifications(certs_en: str) -> list[str]:
  """解析認證字串為清單"""
  if not certs_en:
    return []
  # 可能是逗號分隔或換行分隔
  parts = [c.strip() for c in certs_en.replace('\n', ',').split(',') if c.strip()]
  # 去除太短或不像認證名稱的項目
  return [p for p in parts if len(p) >= 3][:5]


def generate_profile(
  factory: dict,
  gov_records: list[dict],
  factory_count: int,
  supply_chain_buyers: list[str],
) -> str:
  """為一家公司生成 3-5 句英文簡介"""
  parts = []

  industry = factory.get('industry_en') or ''
  name_zh = factory.get('name_zh') or ''
  city_en = factory.get('city_en') or ''

  # --- 第一句：公司做什麼 ---
  products = extract_products(name_zh)
  if products:
    if industry:
      parts.append(f'Manufacturer of {products}, operating in the {industry} sector.')
    else:
      parts.append(f'Taiwan-based manufacturer of {products}.')
  else:
    if industry:
      parts.append(f'Taiwan-based manufacturer in the {industry} sector.')
    else:
      parts.append('Taiwan-based manufacturing company.')

  # --- 第二句：地理位置優勢 ---
  location_advantage = get_location_advantage(city_en)
  if location_advantage and city_en:
    parts.append(f'Located in {city_en}, {location_advantage}.')
  elif city_en:
    parts.append(f'Based in {city_en}, Taiwan.')

  # --- 第三句：規模（資本額或多廠） ---
  capital = factory.get('capital_amount')
  if factory_count > 1:
    parts.append(f'Operates {factory_count} factory facilities across Taiwan.')
  elif capital and capital >= 100_000_000:
    usd_approx = int(capital / 30)
    parts.append(
      f'Registered capital of NT${capital:,.0f}'
      f' (approximately USD {usd_approx:,.0f}).'
    )

  # --- 第四句：認證 ---
  cert_list = parse_certifications(factory.get('certifications_en') or '')
  if cert_list:
    certs_str = ', '.join(cert_list)
    parts.append(f'Certified to international standards including {certs_str}.')

  # --- 第五句：供應鏈關係 ---
  if supply_chain_buyers:
    buyers = ', '.join(supply_chain_buyers[:3])
    parts.append(f'Recognized supplier to: {buyers}.')

  # --- 第六句：政府獎項 ---
  if gov_records:
    award_names = []
    seen = set()
    for g in gov_records:
      name = g.get('program_name_en') or ''
      if name and name not in seen:
        award_names.append(name)
        seen.add(name)
    if award_names:
      parts.append(f'Award recipient: {", ".join(award_names[:2])}.')

  # --- 備用：成立年份 ---
  if len(parts) < 3:
    reg_date = factory.get('registration_date') or ''
    if reg_date and len(reg_date) >= 4:
      year = reg_date[:4]
      parts.append(f'Factory established and registered since {year}.')

  return ' '.join(parts)


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

def main() -> None:
  if not DB_PATH.exists():
    print(f'[ERROR] 找不到資料庫：{DB_PATH}')
    sys.exit(1)

  print(f'[INFO] 連接資料庫：{DB_PATH}')
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')

  cur = conn.cursor()

  # 1. 新增欄位（若不存在）
  existing_cols = [row[1] for row in cur.execute('PRAGMA table_info(factories)').fetchall()]
  if 'company_profile_en' not in existing_cols:
    cur.execute('ALTER TABLE factories ADD COLUMN company_profile_en TEXT')
    conn.commit()
    print('[INFO] 新增欄位 company_profile_en')
  else:
    print('[INFO] 欄位 company_profile_en 已存在')

  # 2. 預載 government_records（by tax_id）
  print('[INFO] 載入政府紀錄...')
  gov_map: dict[str, list[dict]] = {}
  for row in cur.execute(
    'SELECT company_tax_id, program_name_en FROM government_records WHERE company_tax_id IS NOT NULL'
  ).fetchall():
    tax_id = row['company_tax_id']
    if tax_id not in gov_map:
      gov_map[tax_id] = []
    gov_map[tax_id].append({'program_name_en': row['program_name_en']})
  print(f'[INFO] 已載入 {len(gov_map)} 家公司的政府紀錄')

  # 3. 預載 supply_chain_links（by supplier_tax_id）
  print('[INFO] 載入供應鏈關係...')
  supply_map: dict[str, list[str]] = {}
  for row in cur.execute(
    'SELECT supplier_tax_id, buyer_name FROM supply_chain_links WHERE supplier_tax_id IS NOT NULL'
  ).fetchall():
    tax_id = row['supplier_tax_id']
    buyer = row['buyer_name']
    if buyer:
      if tax_id not in supply_map:
        supply_map[tax_id] = []
      supply_map[tax_id].append(buyer)
  print(f'[INFO] 已載入 {len(supply_map)} 家公司的供應鏈資料')

  # 4. 預算每個 tax_id 的工廠數量
  print('[INFO] 統計工廠數量...')
  factory_count_map: dict[str, int] = {}
  for row in cur.execute(
    "SELECT tax_id, COUNT(*) as cnt FROM factories WHERE tax_id IS NOT NULL AND tax_id != '' GROUP BY tax_id"
  ).fetchall():
    factory_count_map[row['tax_id']] = row['cnt']

  # 5. 讀取所有工廠並生成 profile
  print('[INFO] 讀取工廠資料...')
  factories = cur.execute(
    '''SELECT id, tax_id, name_zh, industry_en, city_en,
              registration_date, capital_amount, certifications_en
       FROM factories'''
  ).fetchall()
  total = len(factories)
  print(f'[INFO] 共 {total:,} 家工廠')

  updates = []
  stats = {
    'total': total,
    'with_products': 0,
    'with_location': 0,
    'with_certs': 0,
    'with_awards': 0,
    'with_supply_chain': 0,
    'avg_sentences': 0,
  }
  sentence_count_sum = 0

  for factory in factories:
    f = dict(factory)
    tax_id = f.get('tax_id') or ''

    gov_records = gov_map.get(tax_id, [])
    supply_buyers = supply_map.get(tax_id, [])
    fcount = factory_count_map.get(tax_id, 1)

    profile = generate_profile(f, gov_records, fcount, supply_buyers)
    updates.append((profile, f['id']))

    # 統計
    sentence_count_sum += profile.count('.')
    if extract_products(f.get('name_zh') or ''):
      stats['with_products'] += 1
    if get_location_advantage(f.get('city_en') or ''):
      stats['with_location'] += 1
    if f.get('certifications_en'):
      stats['with_certs'] += 1
    if gov_records:
      stats['with_awards'] += 1
    if supply_buyers:
      stats['with_supply_chain'] += 1

  # 6. 批次更新 DB
  print('[INFO] 寫入資料庫...')
  BATCH_SIZE = 1000
  for i in range(0, len(updates), BATCH_SIZE):
    batch = updates[i:i + BATCH_SIZE]
    cur.executemany('UPDATE factories SET company_profile_en = ? WHERE id = ?', batch)
    conn.commit()
    done = min(i + BATCH_SIZE, total)
    print(f'[INFO] 進度：{done:,} / {total:,} ({done * 100 // total}%)')

  stats['avg_sentences'] = round(sentence_count_sum / total, 2) if total else 0

  # 7. 驗證
  verify_count = cur.execute(
    "SELECT COUNT(*) FROM factories WHERE company_profile_en IS NOT NULL AND company_profile_en != ''"
  ).fetchone()[0]

  conn.close()

  # 8. 統計報告
  print()
  print('=' * 60)
  print('Company Profile 生成完成')
  print('=' * 60)
  print(f'  總工廠數：       {stats["total"]:>10,}')
  print(f'  已寫入 profile： {verify_count:>10,}')
  print(f'  有產品關鍵字：   {stats["with_products"]:>10,} ({stats["with_products"]*100//stats["total"]}%)')
  print(f'  有地理優勢：     {stats["with_location"]:>10,} ({stats["with_location"]*100//stats["total"]}%)')
  print(f'  有認證資訊：     {stats["with_certs"]:>10,} ({stats["with_certs"]*100//stats["total"]}%)')
  print(f'  有政府獎項：     {stats["with_awards"]:>10,} ({stats["with_awards"]*100//stats["total"]}%)')
  print(f'  有供應鏈關係：   {stats["with_supply_chain"]:>10,} ({stats["with_supply_chain"]*100//stats["total"]}%)')
  print(f'  平均句數：       {stats["avg_sentences"]:>10}')
  print('=' * 60)

  # 9. 範例輸出
  conn2 = sqlite3.connect(str(DB_PATH))
  conn2.row_factory = sqlite3.Row
  cur2 = conn2.cursor()
  print('\n--- 範例輸出（前 5 筆）---')
  samples = cur2.execute(
    "SELECT name_zh, city_en, industry_en, company_profile_en FROM factories "
    "WHERE company_profile_en IS NOT NULL LIMIT 5"
  ).fetchall()
  for s in samples:
    print(f'\n公司：{s["name_zh"]} | {s["city_en"]} | {s["industry_en"]}')
    print(f'Profile：{s["company_profile_en"]}')
  conn2.close()


if __name__ == '__main__':
  main()
