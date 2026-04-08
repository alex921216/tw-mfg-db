"""
match_suppliers.py — 台灣龍頭公司供應鏈批次比對

功能：
  1. 硬編碼多家買方公司的供應商名單（ALL_SUPPLY_CHAINS）
  2. 清空 supply_chain 表後全部重新比對
  3. 對每個供應商在 factories 表中做精確 / 模糊比對
  4. 將結果寫入 supply_chain 表
  5. 印出每家公司的比對報告與總覽

執行：
  cd src/  &&  python -m scripts.match_suppliers
  或
  cd src/  &&  python scripts/match_suppliers.py
"""

import gzip
import re
import shutil
import sqlite3
import unicodedata
from pathlib import Path

# ---------------------------------------------------------------------------
# 路徑
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'
DB_GZ_PATH = SRC_DIR / 'data' / 'tmdb.db.gz'

# ---------------------------------------------------------------------------
# 多公司供應鏈清單
# ---------------------------------------------------------------------------

SOURCE = 'MULTI_COMPANY_2024'

ALL_SUPPLY_CHAINS = {
  '台積電': {
    'stock_id': '2330',
    'suppliers': [
      # 先進封裝
      {'name': '辛耘企業', 'stock': '3583', 'category': '先進封裝', 'product': '濕製程設備'},

      # 建廠與廠務
      {'name': '漢唐集成', 'stock': '2404', 'category': '建廠工程', 'product': '無塵室工程'},
      {'name': '達欣工程', 'stock': '2535', 'category': '建廠工程', 'product': '土建營造'},

      # 材料
      {'name': '崇越科技', 'stock': '5434', 'category': '材料', 'product': '石英爐管'},
      {'name': '李長榮化工', 'stock': '', 'category': '綠色製造', 'product': '電子級IPA溶劑'},

      # 設備
      {'name': '家登精密', 'stock': '3680', 'category': '設備', 'product': 'EUV光罩盒'},
      {'name': '弘塑科技', 'stock': '3131', 'category': '設備', 'product': '濕製程設備'},
      {'name': '漢民科技', 'stock': '', 'category': '設備', 'product': '離子佈植'},
      {'name': '京鼎精密', 'stock': '3413', 'category': '設備', 'product': '真空腔體'},
      {'name': '翔名科技', 'stock': '8091', 'category': '設備零件', 'product': '半導體零組件'},

      # 廠務
      {'name': '帆宣系統', 'stock': '6196', 'category': '廠務', 'product': '無塵室機電'},
      {'name': '亞翔工程', 'stock': '6139', 'category': '廠務', 'product': '無塵室工程'},

      # 材料（補充）
      {'name': '中砂', 'stock': '1560', 'category': '材料', 'product': '研磨材料'},
      {'name': '長春石化', 'stock': '', 'category': '材料', 'product': '電子化學品'},
      {'name': '光洋科技', 'stock': '1785', 'category': '材料', 'product': '靶材'},
      {'name': '三福化工', 'stock': '4755', 'category': '材料', 'product': '電子級化學品'},
      {'name': '勝一化工', 'stock': '1773', 'category': '材料', 'product': '電子級溶劑'},
      {'name': '關東鑫林', 'stock': '', 'category': '材料', 'product': '電子級化學品'},
      {'name': '聯華氣體', 'stock': '6505', 'category': '材料', 'product': '特殊氣體'},
      {'name': '先豐通訊', 'stock': '5765', 'category': '材料', 'product': 'PCB基板'},

      # IT / 電力 / 通訊
      {'name': '中華電信', 'stock': '2412', 'category': 'IT服務', 'product': '通訊網路'},
      {'name': '台達電子', 'stock': '2308', 'category': '電力設備', 'product': 'UPS電源'},
      {'name': '研華科技', 'stock': '2395', 'category': 'IT設備', 'product': '工業電腦'},

      # IC / 晶圓
      {'name': '創意電子', 'stock': '3443', 'category': 'IC設計服務', 'product': 'IC設計'},
      {'name': '世界先進', 'stock': '5347', 'category': '晶圓代工', 'product': '特殊製程'},

      # 原物料供應商（從 vocus.cc 文章整理）
      {'name': '台塑勝高', 'stock': '3532', 'category': '矽晶圓', 'product': '矽晶圓'},
      {'name': '環球晶', 'stock': '6488', 'category': '矽晶圓', 'product': '矽晶圓'},
      {'name': '勝一化工', 'stock': '1773', 'category': '化學原料', 'product': '製程化學品'},
      {'name': '華立企業', 'stock': '3010', 'category': '化學原料', 'product': '化學原料代理'},
      {'name': '聯華林德', 'stock': '', 'category': '特殊氣體', 'product': '特殊氣體'},
      {'name': '茂泰利', 'stock': '', 'category': '特殊氣體', 'product': '特殊氣體'},
    ],
  },

  '鴻海': {
    'stock_id': '2317',
    'suppliers': [
      # 連接器
      {'name': '正崴精密', 'stock': '2392', 'category': '連接器', 'product': '連接器'},
      {'name': '宏致電子', 'stock': '3605', 'category': '連接器', 'product': '連接器'},
      {'name': '瀚荃', 'stock': '8103', 'category': '連接器', 'product': '連接器'},
      {'name': '嘉澤端子', 'stock': '3533', 'category': '連接器', 'product': '連接器'},
      {'name': '信邦電子', 'stock': '3023', 'category': '連接器', 'product': '連接線束'},
      {'name': '良維科技', 'stock': '6290', 'category': '連接器', 'product': '連接器'},
      # PCB
      {'name': '臻鼎科技', 'stock': '4958', 'category': 'PCB', 'product': '印刷電路板'},
      {'name': '欣興電子', 'stock': '3037', 'category': 'PCB', 'product': 'IC載板'},
      {'name': '華通電腦', 'stock': '2313', 'category': 'PCB', 'product': '印刷電路板'},
      {'name': '健鼎科技', 'stock': '3044', 'category': 'PCB', 'product': '印刷電路板'},
      # 機殼/機構件
      {'name': '可成科技', 'stock': '2474', 'category': '機殼', 'product': '金屬機殼'},
      {'name': '鑫禾科技', 'stock': '', 'category': '機殼', 'product': '金屬機殼'},
      {'name': '巨騰國際', 'stock': '9136', 'category': '機殼', 'product': '筆電機殼'},
      # 被動元件
      {'name': '國巨', 'stock': '2327', 'category': '被動元件', 'product': '電阻電容'},
      {'name': '華新科技', 'stock': '2492', 'category': '被動元件', 'product': '電阻電容'},
      # 散熱
      {'name': '超眾科技', 'stock': '6230', 'category': '散熱', 'product': '散熱模組'},
      {'name': '奇鋐科技', 'stock': '3017', 'category': '散熱', 'product': '散熱模組'},
      # 電源
      {'name': '光寶科技', 'stock': '2301', 'category': '電源', 'product': '電源供應器'},
      {'name': '群電', 'stock': '6412', 'category': '電源', 'product': '電源供應器'},
    ],
  },

  '漢翔': {
    'stock_id': '2634',
    'suppliers': [
      # 航太 A-Team 4.0 成員
      {'name': '千附精密', 'stock': '8383', 'category': '航太零件', 'product': '航太精密加工'},
      {'name': '晟田科技', 'stock': '4541', 'category': '航太零件', 'product': '航太結構件'},
      {'name': '拓凱實業', 'stock': '4536', 'category': '複合材料', 'product': '碳纖維複合材料'},
      {'name': '豐達科技', 'stock': '3004', 'category': '航太零件', 'product': '航太零組件'},
      {'name': '駐龍精密', 'stock': '4572', 'category': '航太零件', 'product': '航太引擎零件'},
      {'name': '全訊科技', 'stock': '5222', 'category': '航太電子', 'product': '軍用微波元件'},
      {'name': '寶一科技', 'stock': '8222', 'category': '航太零件', 'product': '航太引擎零件'},
      {'name': '長亨精密', 'stock': '4546', 'category': '航太零件', 'product': '航太精密鑄造'},
      {'name': '公準精密', 'stock': '3178', 'category': '航太零件', 'product': '航太精密加工'},
      {'name': '新復興銲接', 'stock': '', 'category': '航太加工', 'product': '航太銲接'},
    ],
  },

  '台達電': {
    'stock_id': '2308',
    'suppliers': [
      # 被動元件
      {'name': '國巨', 'stock': '2327', 'category': '被動元件', 'product': '電阻電容'},
      {'name': '華新科技', 'stock': '2492', 'category': '被動元件', 'product': '被動元件'},
      {'name': '奇力新', 'stock': '2456', 'category': '被動元件', 'product': '電感'},
      # PCB
      {'name': '欣興電子', 'stock': '3037', 'category': 'PCB', 'product': 'IC載板'},
      {'name': '敬鵬工業', 'stock': '2355', 'category': 'PCB', 'product': '印刷電路板'},
      # 散熱
      {'name': '奇鋐科技', 'stock': '3017', 'category': '散熱', 'product': '散熱模組'},
      {'name': '雙鴻科技', 'stock': '3324', 'category': '散熱', 'product': '散熱模組'},
      {'name': '富世達', 'stock': '', 'category': '散熱', 'product': '散熱風扇'},
      # 機殼/機構
      {'name': '勤誠興業', 'stock': '8210', 'category': '機殼', 'product': '伺服器機殼'},
      {'name': '營邦企業', 'stock': '3693', 'category': '機殼', 'product': '伺服器機殼'},
      # 電源零件
      {'name': '飛宏科技', 'stock': '2457', 'category': '電源', 'product': '電源供應器'},
      {'name': '全漢企業', 'stock': '3015', 'category': '電源', 'product': '電源供應器'},
    ],
  },

  '研華': {
    'stock_id': '2395',
    'suppliers': [
      {'name': '凌華科技', 'stock': '6166', 'category': '工業電腦', 'product': '嵌入式電腦'},
      {'name': '艾訊', 'stock': '3088', 'category': '工業電腦', 'product': '嵌入式主機板'},
      {'name': '友通資訊', 'stock': '2397', 'category': '工業電腦', 'product': '工業主機板'},
      {'name': '瑞傳科技', 'stock': '6105', 'category': '網通', 'product': '網路設備'},
      {'name': '威強電', 'stock': '3022', 'category': '工業電腦', 'product': '強固型電腦'},
    ],
  },

  '中華電信': {
    'stock_id': '2412',
    'suppliers': [
      {'name': '中磊電子', 'stock': '5388', 'category': '網通', 'product': '寬頻網路設備'},
      {'name': '智邦科技', 'stock': '2345', 'category': '網通', 'product': '網路交換器'},
      {'name': '合勤科技', 'stock': '2391', 'category': '網通', 'product': '網路設備'},
      {'name': '正文科技', 'stock': '4906', 'category': '網通', 'product': '無線網路設備'},
      {'name': '啟碁科技', 'stock': '6285', 'category': '網通', 'product': '衛星通訊設備'},
      {'name': '仲琦科技', 'stock': '2419', 'category': '網通', 'product': '網路設備'},
    ],
  },

  '聯電': {
    'stock_id': '2303',
    'suppliers': [
      {'name': '台塑勝高', 'stock': '3532', 'category': '矽晶圓', 'product': '矽晶圓'},
      {'name': '環球晶', 'stock': '6488', 'category': '矽晶圓', 'product': '矽晶圓'},
      {'name': '勝一化工', 'stock': '1773', 'category': '化學原料', 'product': '電子級溶劑'},
      {'name': '華立企業', 'stock': '3010', 'category': '化學原料', 'product': '化學原料代理'},
      {'name': '三福化工', 'stock': '4755', 'category': '化學原料', 'product': '電子級化學品'},
      {'name': '聯華林德', 'stock': '', 'category': '特殊氣體', 'product': '特殊氣體'},
      {'name': '家登精密', 'stock': '3680', 'category': '設備', 'product': '光罩盒'},
      {'name': '弘塑科技', 'stock': '3131', 'category': '設備', 'product': '濕製程設備'},
      {'name': '辛耘企業', 'stock': '3583', 'category': '設備', 'product': '濕製程設備'},
      {'name': '漢民科技', 'stock': '', 'category': '設備', 'product': '離子佈植'},
      {'name': '中砂', 'stock': '1560', 'category': '材料', 'product': '研磨材料'},
    ],
  },

  '日月光': {
    'stock_id': '3711',
    'suppliers': [
      # 低碳供應聯盟 14 家
      {'name': '一詮精密', 'stock': '2486', 'category': '封裝材料', 'product': '精密沖壓'},
      {'name': '志聖工業', 'stock': '2467', 'category': '設備', 'product': '烘烤設備'},
      {'name': '鈦昇科技', 'stock': '7480', 'category': '設備', 'product': '封裝設備'},
      # 封裝材料/基板
      {'name': '景碩科技', 'stock': '3228', 'category': '基板', 'product': 'IC封裝基板'},
      {'name': '南亞電路板', 'stock': '8046', 'category': '基板', 'product': 'IC封裝基板'},
      {'name': '欣興電子', 'stock': '3037', 'category': '基板', 'product': 'IC載板'},
      # 封裝測試同業/供應商
      {'name': '力成科技', 'stock': '6239', 'category': '封裝測試', 'product': '記憶體封裝'},
      {'name': '京元電子', 'stock': '2449', 'category': '測試', 'product': 'IC測試'},
      {'name': '矽格', 'stock': '6257', 'category': '封裝測試', 'product': '封裝測試'},
      # 材料
      {'name': '長春石化', 'stock': '', 'category': '材料', 'product': '銅箔基板材料'},
      {'name': '聯茂電子', 'stock': '6213', 'category': '材料', 'product': '銅箔基板'},
      {'name': '台虹科技', 'stock': '8039', 'category': '材料', 'product': '軟板材料'},
    ],
  },

  '和碩': {
    'stock_id': '4938',
    'suppliers': [
      # 子公司/關係企業
      {'name': '華擎科技', 'stock': '3515', 'category': '主機板', 'product': '主機板'},
      {'name': '景碩科技', 'stock': '3228', 'category': '基板', 'product': 'IC封裝基板'},
      {'name': '海華科技', 'stock': '3694', 'category': '通訊模組', 'product': '無線通訊模組'},
      # 零組件供應商
      {'name': '可成科技', 'stock': '2474', 'category': '機殼', 'product': '金屬機殼'},
      {'name': '國巨', 'stock': '2327', 'category': '被動元件', 'product': '電阻電容'},
      {'name': '華新科技', 'stock': '2492', 'category': '被動元件', 'product': '被動元件'},
      {'name': '奇鋐科技', 'stock': '3017', 'category': '散熱', 'product': '散熱模組'},
      {'name': '雙鴻科技', 'stock': '3324', 'category': '散熱', 'product': '散熱模組'},
      {'name': '光寶科技', 'stock': '2301', 'category': '電源', 'product': '電源供應器'},
      {'name': '正崴精密', 'stock': '2392', 'category': '連接器', 'product': '連接器'},
      {'name': '臻鼎科技', 'stock': '4958', 'category': 'PCB', 'product': '印刷電路板'},
    ],
  },

  '廣達': {
    'stock_id': '2382',
    'suppliers': [
      {'name': '奇鋐科技', 'stock': '3017', 'category': '散熱', 'product': '伺服器散熱'},
      {'name': '雙鴻科技', 'stock': '3324', 'category': '散熱', 'product': '液冷散熱'},
      {'name': '勤誠興業', 'stock': '8210', 'category': '機殼', 'product': '伺服器機殼'},
      {'name': '營邦企業', 'stock': '3693', 'category': '機殼', 'product': '伺服器機殼'},
      {'name': '光寶科技', 'stock': '2301', 'category': '電源', 'product': '伺服器電源'},
      {'name': '台達電子', 'stock': '2308', 'category': '電源', 'product': '伺服器電源'},
      {'name': '嘉澤端子', 'stock': '3533', 'category': '連接器', 'product': '高速連接器'},
      {'name': '信邦電子', 'stock': '3023', 'category': '連接線', 'product': '高速線纜'},
      {'name': '健鼎科技', 'stock': '3044', 'category': 'PCB', 'product': '高階PCB'},
      {'name': '欣興電子', 'stock': '3037', 'category': 'PCB', 'product': 'IC載板'},
    ],
  },

  '緯創': {
    'stock_id': '3231',
    'suppliers': [
      {'name': '奇鋐科技', 'stock': '3017', 'category': '散熱', 'product': '散熱模組'},
      {'name': '超眾科技', 'stock': '6230', 'category': '散熱', 'product': '散熱模組'},
      {'name': '光寶科技', 'stock': '2301', 'category': '電源', 'product': '電源供應器'},
      {'name': '國巨', 'stock': '2327', 'category': '被動元件', 'product': '電阻電容'},
      {'name': '臻鼎科技', 'stock': '4958', 'category': 'PCB', 'product': '印刷電路板'},
      {'name': '嘉澤端子', 'stock': '3533', 'category': '連接器', 'product': '高速連接器'},
      {'name': '勤誠興業', 'stock': '8210', 'category': '機殼', 'product': '伺服器機殼'},
    ],
  },
}

# ---------------------------------------------------------------------------
# 正規化工具（與 match_supply_chain.py 一致）
# ---------------------------------------------------------------------------

_SUFFIXES = sorted([
  '股份有限公司', '有限公司', '股份有限', '無限公司',
  '企業股份有限公司', '工業股份有限公司', '科技股份有限公司',
  '企業有限公司', '工業有限公司', '科技有限公司',
  '企業社', '企業', '實業', '工業', '科技', '國際',
  '(股)公司', '(有)公司', '(股)', '(有)',
  '公司',
], key=len, reverse=True)


def normalize(name: str) -> str:
  if not name:
    return ''
  name = unicodedata.normalize('NFKC', name)
  name = re.sub(r'[\s\u3000]+', '', name)
  return name.strip()


def strip_suffixes(name: str) -> str:
  for suffix in _SUFFIXES:
    if name.endswith(suffix):
      name = name[: -len(suffix)]
      break
  return name.strip()


# ---------------------------------------------------------------------------
# 載入工廠資料（分頁，避免截斷）
# ---------------------------------------------------------------------------

PAGE_SIZE = 5000


def load_factories(conn: sqlite3.Connection):
  """
  回傳兩個索引：
    normalized_index: { normalize(name_zh) -> (id, name_zh, city_en, industry_en, capital_amount) }
    keyword_index:    { strip_suffixes(normalize(name_zh)) -> same tuple }
  """
  normalized_index = {}
  keyword_index = {}

  offset = 0
  total_loaded = 0

  while True:
    cur = conn.cursor()
    cur.execute(
      'SELECT id, name_zh, city_en, industry_en, capital_amount '
      'FROM factories WHERE name_zh IS NOT NULL ORDER BY id LIMIT ? OFFSET ?',
      (PAGE_SIZE, offset),
    )
    rows = cur.fetchall()
    if not rows:
      break

    for row in rows:
      fid, name_zh, city_en, industry_en, capital_amount = row
      if not name_zh:
        continue
      norm = normalize(name_zh)
      kw = strip_suffixes(norm)
      entry = (fid, name_zh, city_en, industry_en, capital_amount)
      normalized_index[norm] = entry
      if kw and len(kw) >= 2:
        keyword_index[kw] = entry

    total_loaded += len(rows)
    offset += PAGE_SIZE

    if len(rows) < PAGE_SIZE:
      break

  print(f'載入工廠資料：{total_loaded} 筆，normalized_index={len(normalized_index)}，keyword_index={len(keyword_index)}')
  return normalized_index, keyword_index


# ---------------------------------------------------------------------------
# 比對策略
# ---------------------------------------------------------------------------

def match_supplier(supplier_name: str, normalized_index: dict, keyword_index: dict, conn: sqlite3.Connection):
  """
  四階段比對，回傳 (entry, confidence) 或 (None, None)。
  entry = (id, name_zh, city_en, industry_en, capital_amount)
  confidence = 'exact' | 'fuzzy'
  """
  norm = normalize(supplier_name)
  if not norm:
    return None, None

  # 1. 精確比對（normalize 後完全相同）
  entry = normalized_index.get(norm)
  if entry:
    return entry, 'exact'

  # 2. keyword 比對（去後綴後相同）
  kw = strip_suffixes(norm)
  if kw and len(kw) >= 2:
    entry = keyword_index.get(kw)
    if entry:
      return entry, 'fuzzy'

  # 3. contains 比對（name 包含在工廠名，或工廠名包含在 name 中）
  #    只取最短命中以減少誤配
  if len(norm) >= 3:
    best = None
    best_len = 9999
    for norm_factory, e in normalized_index.items():
      if norm in norm_factory or norm_factory in norm:
        if len(norm_factory) < best_len:
          best = e
          best_len = len(norm_factory)
    if best:
      return best, 'fuzzy'

  # 4. LIKE 搜尋（關鍵字 >= 3 字）
  search_kw = kw if (kw and len(kw) >= 3) else (norm if len(norm) >= 3 else None)
  if search_kw:
    cur = conn.cursor()
    cur.execute(
      'SELECT id, name_zh, city_en, industry_en, capital_amount '
      'FROM factories WHERE name_zh LIKE ? LIMIT 1',
      (f'%{search_kw}%',),
    )
    row = cur.fetchone()
    if row:
      return tuple(row), 'fuzzy'

  return None, None


# ---------------------------------------------------------------------------
# 寫入 supply_chain 表
# ---------------------------------------------------------------------------

def upsert_supply_chain(conn: sqlite3.Connection, buyer_name: str, buyer_stock_id: str,
                        supplier: dict, factory_id, confidence: str, source: str):
  """
  先刪除同 buyer + supplier_name_zh 的舊記錄，再插入新記錄（冪等）。
  """
  conn.execute(
    'DELETE FROM supply_chain WHERE buyer_name = ? AND supplier_name_zh = ?',
    (buyer_name, supplier['name']),
  )
  conn.execute(
    '''INSERT INTO supply_chain
       (buyer_name, buyer_stock_id, supplier_name_zh, supplier_stock_id,
        supplier_factory_id, category, product, source, confidence)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
    (
      buyer_name,
      buyer_stock_id,
      supplier['name'],
      supplier.get('stock') or None,
      factory_id,
      supplier.get('category'),
      supplier.get('product'),
      source,
      confidence,
    ),
  )


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

def main():
  # 自動解壓
  if not DB_PATH.exists():
    if DB_GZ_PATH.exists():
      print(f'解壓 {DB_GZ_PATH} ...')
      with gzip.open(str(DB_GZ_PATH), 'rb') as f_in, open(str(DB_PATH), 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    else:
      raise FileNotFoundError(f'找不到資料庫：{DB_PATH}')

  # 確保 supply_chain 表存在
  conn = sqlite3.connect(str(DB_PATH))
  conn.execute('PRAGMA journal_mode=WAL')
  conn.execute('''
    CREATE TABLE IF NOT EXISTS supply_chain (
      id                   INTEGER PRIMARY KEY AUTOINCREMENT,
      buyer_name           TEXT NOT NULL,
      buyer_stock_id       TEXT,
      supplier_name_zh     TEXT NOT NULL,
      supplier_stock_id    TEXT,
      supplier_factory_id  INTEGER,
      category             TEXT,
      product              TEXT,
      source               TEXT,
      confidence           TEXT,
      created_at           TEXT DEFAULT (datetime('now'))
    )
  ''')
  conn.execute('CREATE INDEX IF NOT EXISTS idx_supply_chain_buyer ON supply_chain (buyer_name)')
  conn.execute('CREATE INDEX IF NOT EXISTS idx_supply_chain_factory ON supply_chain (supplier_factory_id)')

  # 清空舊資料，全部重新比對
  conn.execute('DELETE FROM supply_chain')
  conn.commit()
  print('已清空 supply_chain 表，開始重新比對...')
  print()

  # 載入工廠索引（只需一次）
  normalized_index, keyword_index = load_factories(conn)
  print()

  # 累計統計
  grand_total = 0
  grand_matched = 0
  company_summaries = []

  for buyer_name, buyer_data in ALL_SUPPLY_CHAINS.items():
    buyer_stock_id = buyer_data['stock_id']
    suppliers = buyer_data['suppliers']

    matched = []
    not_found = []

    for supplier in suppliers:
      entry, confidence = match_supplier(
        supplier['name'], normalized_index, keyword_index, conn
      )

      if entry:
        factory_id, factory_name_zh, city_en, industry_en, capital_amount = entry
        upsert_supply_chain(conn, buyer_name, buyer_stock_id, supplier, factory_id, confidence, SOURCE)
        matched.append({
          'supplier': supplier,
          'factory_id': factory_id,
          'factory_name_zh': factory_name_zh,
          'city_en': city_en,
          'industry_en': industry_en,
          'capital_amount': capital_amount,
          'confidence': confidence,
        })
      else:
        upsert_supply_chain(conn, buyer_name, buyer_stock_id, supplier, None, 'unmatched', SOURCE)
        not_found.append(supplier)

    conn.commit()

    total = len(suppliers)
    matched_count = len(matched)
    rate = matched_count / total * 100 if total > 0 else 0.0
    grand_total += total
    grand_matched += matched_count

    company_summaries.append({
      'buyer_name': buyer_name,
      'total': total,
      'matched': matched_count,
      'rate': rate,
    })

    # ── 每家公司報告 ───────────────────────────────────────────────
    print(f'=== {buyer_name}（{buyer_stock_id}）供應鏈比對結果 ===')
    print()

    for m in matched:
      sup = m['supplier']
      cap_str = ''
      if m['capital_amount']:
        cap = m['capital_amount']
        if cap >= 1_000_000_000:
          cap_str = f'NT$ {cap / 1_000_000_000:.1f}B'
        elif cap >= 1_000_000:
          cap_str = f'NT$ {cap / 1_000_000:.0f}M'
        else:
          cap_str = f'NT$ {cap:,}'
      else:
        cap_str = 'N/A'

      confidence_label = 'EXACT' if m['confidence'] == 'exact' else 'FUZZY'
      print(f'  [{confidence_label}] {sup["name"]} → {m["factory_name_zh"]} (ID: {m["factory_id"]}, {m["city_en"] or "N/A"})')
      print(f'    工廠登記: {m["industry_en"] or "N/A"} | 資本額: {cap_str}')

    for sup in not_found:
      print(f'  [NOT FOUND] {sup["name"]}')

    exact_count = sum(1 for m in matched if m['confidence'] == 'exact')
    fuzzy_count = matched_count - exact_count
    print()
    print(f'  小計：{matched_count}/{total}（{rate:.0f}%）  精確:{exact_count}  模糊:{fuzzy_count}  未比對:{len(not_found)}')
    print()

  conn.close()

  # ── 總覽 ──────────────────────────────────────────────────────────
  grand_rate = grand_matched / grand_total * 100 if grand_total > 0 else 0.0
  print('=' * 60)
  print('=== 總覽 ===')
  print()
  for s in company_summaries:
    bar = '#' * int(s['rate'] / 5)
    print(f'  {s["buyer_name"]:6s}  {s["matched"]:3d}/{s["total"]:3d}  ({s["rate"]:5.1f}%)  {bar}')
  print()
  print(f'  總計：{grand_matched}/{grand_total}（{grand_rate:.0f}%）')
  print()
  print(f'資料已寫入 supply_chain 表（共 {grand_matched} 筆已比對 + {grand_total - grand_matched} 筆未比對）')


if __name__ == '__main__':
  main()
