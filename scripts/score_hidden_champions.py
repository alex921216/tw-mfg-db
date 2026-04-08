"""
score_hidden_champions.py — 隱形冠軍評分與標記系統

評分維度：
  A. 政府背書（最高 30 分）
  B. 技術能力（最高 30 分）
  C. 供應鏈嵌入度（最高 25 分）
  D. 產業特殊性（最高 15 分）

執行方式（在 src/ 目錄下）：
  python scripts/score_hidden_champions.py

功能：
  - 在 factories 表新增 hidden_champion_score、hidden_champion_reasons、
    hidden_champion_updated_at 欄位（IF NOT EXISTS）
  - 交叉比對 patents、government_records、supply_chain_links
  - 批次更新（每 500 筆 commit）
  - 輸出統計報表
"""

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parent
DB_PATH = SRC_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 五大信賴產業 IPC / 行業代碼前綴
# ---------------------------------------------------------------------------

STRATEGIC_INDUSTRY_CODES = {
  'C271': 'Semiconductor Manufacturing',
  'C270': 'Electronic Component Manufacturing',
  'C273': 'Communication Equipment Manufacturing',
  'C274': 'Optoelectronic Component Manufacturing',
  'C290': 'Precision Machinery Manufacturing',
}

# 後綴清單（用於正規化公司名稱）
_COMPANY_SUFFIXES = re.compile(
  r'(股份有限公司|有限公司|股份有限|有限|公司|企業社|工業社|工廠|廠)$'
)


def normalize_name(name: str) -> str:
  """去除常見公司後綴、空白，小寫化，以便模糊比對。"""
  if not name:
    return ''
  name = name.strip()
  name = _COMPANY_SUFFIXES.sub('', name)
  return name.strip()


# ---------------------------------------------------------------------------
# Schema 遷移：新增欄位
# ---------------------------------------------------------------------------

def migrate_schema(conn: sqlite3.Connection) -> None:
  """在 factories 表新增 hidden champion 相關欄位（已存在則靜默跳過）。"""
  alterations = [
    "ALTER TABLE factories ADD COLUMN hidden_champion_score INTEGER DEFAULT 0",
    "ALTER TABLE factories ADD COLUMN hidden_champion_reasons TEXT DEFAULT NULL",
    "ALTER TABLE factories ADD COLUMN hidden_champion_updated_at TEXT DEFAULT NULL",
  ]
  cur = conn.cursor()
  for stmt in alterations:
    try:
      cur.execute(stmt)
      print(f'  [migrate] 執行：{stmt}')
    except sqlite3.OperationalError as e:
      if 'duplicate column name' in str(e).lower():
        col = stmt.split('ADD COLUMN ')[1].split(' ')[0]
        print(f'  [migrate] 欄位 {col} 已存在，略過')
      else:
        raise
  conn.commit()
  print('[migrate] Schema 遷移完成\n')


# ---------------------------------------------------------------------------
# 預載入輔助資料
# ---------------------------------------------------------------------------

def load_patent_map(conn: sqlite3.Connection) -> dict[str, list[dict]]:
  """
  回傳 {normalized_applicant_name: [patent_record, ...]}

  patent_record keys: tech_category
  """
  cur = conn.cursor()
  cur.execute("""
    SELECT applicant_name, applicant_tax_id, tech_category
    FROM patents
    WHERE applicant_name IS NOT NULL AND applicant_name != ''
  """)
  result: dict[str, list[dict]] = {}
  for row in cur.fetchall():
    key = normalize_name(row['applicant_name'])
    if key:
      result.setdefault(key, []).append({
        'tech_category': row['tech_category'],
        'applicant_tax_id': row['applicant_tax_id'],
      })
  return result


def load_patent_map_by_tax_id(conn: sqlite3.Connection) -> dict[str, list[dict]]:
  """回傳 {tax_id: [patent_record, ...]}"""
  cur = conn.cursor()
  cur.execute("""
    SELECT applicant_tax_id, tech_category
    FROM patents
    WHERE applicant_tax_id IS NOT NULL AND applicant_tax_id != ''
  """)
  result: dict[str, list[dict]] = {}
  for row in cur.fetchall():
    result.setdefault(row['applicant_tax_id'], []).append({
      'tech_category': row['tech_category'],
    })
  return result


def load_gov_record_map(conn: sqlite3.Connection) -> dict[str, list[dict]]:
  """
  回傳 {normalized_company_name: [gov_record, ...]}

  gov_record keys: record_type, program_name, program_name_en, year, details, subsidy_amount
  """
  cur = conn.cursor()
  cur.execute("""
    SELECT company_name, company_tax_id, record_type,
           program_name, program_name_en, year, details, subsidy_amount
    FROM government_records
    WHERE company_name IS NOT NULL AND company_name != ''
  """)
  result: dict[str, list[dict]] = {}
  for row in cur.fetchall():
    key = normalize_name(row['company_name'])
    if key:
      result.setdefault(key, []).append({
        'record_type': row['record_type'],
        'program_name': row['program_name'],
        'program_name_en': row['program_name_en'],
        'year': row['year'],
        'details': row['details'],
        'company_tax_id': row['company_tax_id'],
        'subsidy_amount': row['subsidy_amount'],
      })
  return result


def load_gov_record_map_by_tax_id(conn: sqlite3.Connection) -> dict[str, list[dict]]:
  """回傳 {tax_id: [gov_record, ...]}"""
  cur = conn.cursor()
  cur.execute("""
    SELECT company_tax_id, record_type, program_name, program_name_en, year, details, subsidy_amount
    FROM government_records
    WHERE company_tax_id IS NOT NULL AND company_tax_id != ''
  """)
  result: dict[str, list[dict]] = {}
  for row in cur.fetchall():
    result.setdefault(row['company_tax_id'], []).append({
      'record_type': row['record_type'],
      'program_name': row['program_name'],
      'program_name_en': row['program_name_en'],
      'year': row['year'],
      'details': row['details'],
      'subsidy_amount': row['subsidy_amount'],
    })
  return result


def load_supply_chain_set(conn: sqlite3.Connection) -> tuple[set[str], set[str], dict[str, list[str]]]:
  """
  回傳：
    - supplier_tax_ids: 出現在 supply_chain_links.supplier_tax_id 的 tax_id 集合
    - supplier_norm_names: 出現在 supply_chain_links.supplier_name 的正規化名稱集合
    - buyer_map_by_tax_id: {supplier_tax_id: [buyer_name, ...]}
    - buyer_map_by_name: {normalized_supplier_name: [buyer_name, ...]}  (不在 tuple 回傳，另查)
  """
  cur = conn.cursor()
  cur.execute("""
    SELECT supplier_tax_id, supplier_name, buyer_name
    FROM supply_chain_links
    WHERE supplier_name IS NOT NULL OR supplier_tax_id IS NOT NULL
  """)
  rows = cur.fetchall()

  supplier_tax_ids: set[str] = set()
  supplier_norm_names: set[str] = set()
  buyer_map_by_tax_id: dict[str, list[str]] = {}
  buyer_map_by_name: dict[str, list[str]] = {}

  for row in rows:
    tax_id = row['supplier_tax_id']
    name = row['supplier_name']
    buyer = row['buyer_name'] or ''

    if tax_id:
      supplier_tax_ids.add(tax_id)
      buyer_map_by_tax_id.setdefault(tax_id, [])
      if buyer:
        buyer_map_by_tax_id[tax_id].append(buyer)

    if name:
      norm = normalize_name(name)
      if norm:
        supplier_norm_names.add(norm)
        buyer_map_by_name.setdefault(norm, [])
        if buyer:
          buyer_map_by_name[norm].append(buyer)

  return supplier_tax_ids, supplier_norm_names, buyer_map_by_tax_id, buyer_map_by_name


# ---------------------------------------------------------------------------
# 評分邏輯
# ---------------------------------------------------------------------------

def extract_ipc_top_categories(patents: list[dict], top_n: int = 3) -> list[str]:
  """從專利列表取出最常見的 tech_category 前 N 項。"""
  counts: dict[str, int] = {}
  for p in patents:
    cat = p.get('tech_category') or ''
    if cat:
      counts[cat] = counts.get(cat, 0) + 1
  return sorted(counts, key=lambda k: counts[k], reverse=True)[:top_n]


def score_government(
  tax_id: str,
  norm_name: str,
  gov_by_tax: dict[str, list[dict]],
  gov_by_name: dict[str, list[dict]],
) -> tuple[int, str | None]:
  """
  維度 A：政府背書（最高 30 分）。

  評分規則（各獎項個別計算，取最高分，不疊加，上限 30 分）：
    - 小巨人獎：30 分
    - 國家磐石獎：30 分（中小企業最高榮譽，與小巨人同等級）
    - 國家品質獎：25 分
    - 金貿獎：25 分
    - SBIR 補助：20 分
    - 其他政府獎項：15 分
  額外規則：若同時擁有 2 個以上不同獎項，+5（多重政府背書加分）

  Returns (score, reason_string)
  """
  records: list[dict] = []

  # 優先以 tax_id 精確比對
  if tax_id and tax_id in gov_by_tax:
    records = gov_by_tax[tax_id]
  elif norm_name and norm_name in gov_by_name:
    records = gov_by_name[norm_name]

  if not records:
    # 嘗試部分比對名稱
    for key, recs in gov_by_name.items():
      if norm_name and len(norm_name) >= 4 and (norm_name in key or key in norm_name):
        records = recs
        break

  if not records:
    return 0, None

  # 各獎項旗標與說明
  has_small_giant = False
  small_giant_reason: str | None = None

  has_panshi = False
  panshi_reason: str | None = None

  has_quality = False
  quality_reason: str | None = None

  has_golden_trade = False
  golden_trade_reason: str | None = None

  sbir_records: list[dict] = []

  for rec in records:
    pname = rec.get('program_name') or ''
    details_str = rec.get('details') or ''

    # 小巨人獎
    if '小巨人' in pname or '小巨人' in details_str:
      has_small_giant = True
      edition_match = re.search(r'第?(\d+)屆', details_str + pname)
      edition = edition_match.group(1) if edition_match else rec.get('year', '')
      small_giant_reason = (
        f"Won the {edition}th Small Giant Award (小巨人獎), a national-level recognition "
        f"for outstanding SMEs by Taiwan's Ministry of Economic Affairs"
      )

    # 國家磐石獎
    if '磐石' in pname or '磐石' in details_str or 'Cornerstone' in (rec.get('program_name_en') or ''):
      has_panshi = True
      edition_match = re.search(r'第?(\d+)屆', details_str + pname)
      edition = edition_match.group(1) if edition_match else rec.get('year', '')
      panshi_reason = (
        f"Won the {edition}th National Cornerstone Award (國家磐石獎), "
        f"the highest honor for Taiwan's outstanding SMEs"
      )

    # 國家品質獎
    if '品質獎' in pname or '品質獎' in details_str or 'Quality Award' in (rec.get('program_name_en') or ''):
      has_quality = True
      edition_str = rec.get('edition') or ''
      quality_reason = (
        f"Received the {edition_str} National Quality Award (國家品質獎), "
        f"the highest quality recognition awarded by Taiwan's Executive Yuan"
      )

    # 金貿獎
    if '金貿' in pname or '金貿' in details_str or 'Golden Trade' in (rec.get('program_name_en') or ''):
      has_golden_trade = True
      year = rec.get('year') or ''
      golden_trade_reason = (
        f"Won the Golden Trade Award (金貿獎) in {year}, "
        f"recognized for outstanding export performance by Taiwan's Ministry of Trade"
      )

    # SBIR
    if 'SBIR' in pname or 'SBIR' in (rec.get('program_name_en') or ''):
      sbir_records.append(rec)

  # 計算 SBIR 分數與說明
  sbir_score = 0
  sbir_reason: str | None = None
  if sbir_records:
    sbir_count = len(sbir_records)
    total_amount = sum(
      r.get('subsidy_amount') or 0
      for r in sbir_records
      if r.get('subsidy_amount') is not None
    )
    amount_str = f'NT${total_amount:,}' if total_amount > 0 else 'undisclosed amount'
    sbir_reason = (
      f"Received SBIR innovation research grant ({sbir_count} projects, total {amount_str}), "
      f"demonstrating government-recognized R&D capability"
    )
    sbir_score = 20

  # 建立候選獎項列表（score, reason, has_flag）
  candidates: list[tuple[int, str]] = []
  if has_small_giant and small_giant_reason:
    candidates.append((30, small_giant_reason))
  if has_panshi and panshi_reason:
    candidates.append((30, panshi_reason))
  if has_quality and quality_reason:
    candidates.append((25, quality_reason))
  if has_golden_trade and golden_trade_reason:
    candidates.append((25, golden_trade_reason))
  if sbir_score > 0 and sbir_reason:
    candidates.append((sbir_score, sbir_reason))

  if not candidates:
    # 其他政府獎項或補助
    best = records[0]
    pname_en = best.get('program_name_en') or best.get('program_name') or 'a government program'
    reason = f"Recognized under {pname_en} by a Taiwan government agency"
    return 15, reason

  # 取最高分，不疊加，上限 30
  candidates.sort(key=lambda x: x[0], reverse=True)
  best_score, best_reason = candidates[0]
  final_score = min(best_score, 30)

  # 多重政府背書加分：2 個以上不同獎項，+5
  if len(candidates) >= 2:
    final_score = min(final_score + 5, 30)
    award_names = ', '.join(r for _, r in candidates[:3])
    best_reason = f"{best_reason} [Multi-award bonus: recognized by multiple government programs]"

  return final_score, best_reason


def score_technology(
  tax_id: str,
  norm_name: str,
  patent_by_tax: dict[str, list[dict]],
  patent_by_name: dict[str, list[dict]],
) -> tuple[int, str | None]:
  """
  維度 B：技術能力（最高 30 分）。

  Returns (score, reason_string)
  """
  patents: list[dict] = []

  # 優先以 tax_id 精確比對
  if tax_id and tax_id in patent_by_tax:
    patents = patent_by_tax[tax_id]
  elif norm_name and norm_name in patent_by_name:
    patents = patent_by_name[norm_name]

  if not patents:
    # 嘗試部分比對名稱
    for key, recs in patent_by_name.items():
      if norm_name and len(norm_name) >= 4 and (norm_name in key or key in norm_name):
        patents = recs
        break

  count = len(patents)
  if count == 0:
    return 0, None

  top_cats = extract_ipc_top_categories(patents)
  cats_str = ', '.join(top_cats) if top_cats else 'various technology fields'

  if count >= 10:
    score = 30
  elif count >= 5:
    score = 20
  else:
    score = 10

  reason = (
    f"Holds {count} patents in the past 3 years, primarily in {cats_str}, "
    f"demonstrating strong R&D capability"
  )
  return score, reason


def score_supply_chain(
  tax_id: str,
  norm_name: str,
  supplier_tax_ids: set[str],
  supplier_norm_names: set[str],
  buyer_map_by_tax: dict[str, list[str]],
  buyer_map_by_name: dict[str, list[str]],
) -> tuple[int, str | None]:
  """
  維度 C：供應鏈嵌入度（最高 25 分）。

  Returns (score, reason_string)
  """
  buyers: list[str] = []

  if tax_id and tax_id in supplier_tax_ids:
    buyers = list(set(buyer_map_by_tax.get(tax_id, [])))
  elif norm_name and norm_name in supplier_norm_names:
    buyers = list(set(buyer_map_by_name.get(norm_name, [])))

  if not buyers:
    return 0, None

  unique_buyers = list(dict.fromkeys(b for b in buyers if b))[:5]  # 去重，最多列 5 家
  buyer_names_str = ', '.join(unique_buyers) if unique_buyers else 'major companies'
  reason = (
    f"Identified as a supplier to {buyer_names_str}, "
    f"indicating integration into major supply chains"
  )
  return 25, reason


def score_industry(
  industry_code: str,
  capital_amount,
) -> tuple[int, str | None]:
  """
  維度 D：產業特殊性（最高 15 分）。

  Returns (score, combined_reason_or_none)
  """
  score = 0
  reasons: list[str] = []

  # 五大信賴產業
  if industry_code:
    for code, industry_en in STRATEGIC_INDUSTRY_CODES.items():
      if industry_code.startswith(code):
        score += 10
        reasons.append(
          f"Operates in {industry_en}, a strategically important sector in Taiwan's industrial policy"
        )
        break

  # 資本額 5千萬 ~ 50億（新台幣）
  try:
    cap = int(capital_amount) if capital_amount is not None else 0
  except (ValueError, TypeError):
    cap = 0

  if 50_000_000 <= cap <= 5_000_000_000:
    score += 5
    cap_display = f'{cap:,}'
    reasons.append(
      f"Capital of NT${cap_display} positions it as a mid-sized specialist manufacturer"
    )

  if not reasons:
    return 0, None

  return score, '; '.join(reasons)


# ---------------------------------------------------------------------------
# 主評分流程
# ---------------------------------------------------------------------------

def run_scoring() -> None:
  if not DB_PATH.exists():
    print(f'[error] 資料庫不存在：{DB_PATH}')
    return

  print(f'[start] 連接資料庫：{DB_PATH}')
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  conn.execute('PRAGMA journal_mode=WAL')

  # 1. 執行 schema 遷移
  print('[step 1] 執行 schema 遷移...')
  migrate_schema(conn)

  # 2. 預載入輔助資料
  print('[step 2] 預載入輔助資料...')
  patent_by_name = load_patent_map(conn)
  patent_by_tax = load_patent_map_by_tax_id(conn)
  print(f'  專利申請人（名稱）：{len(patent_by_name)} 筆')
  print(f'  專利申請人（tax_id）：{len(patent_by_tax)} 筆')

  gov_by_name = load_gov_record_map(conn)
  gov_by_tax = load_gov_record_map_by_tax_id(conn)
  print(f'  政府紀錄（名稱）：{len(gov_by_name)} 筆')
  print(f'  政府紀錄（tax_id）：{len(gov_by_tax)} 筆')

  supplier_tax_ids, supplier_norm_names, buyer_map_by_tax, buyer_map_by_name = load_supply_chain_set(conn)
  print(f'  供應鏈供應商（tax_id）：{len(supplier_tax_ids)} 筆')
  print(f'  供應鏈供應商（名稱）：{len(supplier_norm_names)} 筆')

  # 3. 讀取所有工廠（分頁避免記憶體爆炸）
  print('\n[step 3] 開始評分...')
  cur = conn.cursor()

  # 先計算總數
  cur.execute('SELECT COUNT(*) AS cnt FROM factories')
  total_factories = cur.fetchone()['cnt']
  print(f'  工廠總數：{total_factories:,}')

  PAGE_SIZE = 1000
  COMMIT_BATCH = 500
  now_str = datetime.now(timezone.utc).isoformat()

  updates: list[tuple] = []  # (score, reasons_json, updated_at, factory_id)
  processed = 0
  offset = 0

  while True:
    cur.execute("""
      SELECT id, tax_id, name_zh, industry_en, capital_amount
      FROM factories
      LIMIT ? OFFSET ?
    """, (PAGE_SIZE, offset))
    rows = cur.fetchall()
    if not rows:
      break

    for row in rows:
      factory_id = row['id']
      tax_id = row['tax_id'] or ''
      name_zh = row['name_zh'] or ''
      industry_en = row['industry_en'] or ''
      capital_amount = row['capital_amount']
      norm_name = normalize_name(name_zh)

      # 維度評分
      score_gov, reason_gov = score_government(tax_id, norm_name, gov_by_tax, gov_by_name)
      score_tech, reason_tech = score_technology(tax_id, norm_name, patent_by_tax, patent_by_name)
      score_sc, reason_sc = score_supply_chain(
        tax_id, norm_name,
        supplier_tax_ids, supplier_norm_names,
        buyer_map_by_tax, buyer_map_by_name,
      )
      score_ind, reason_ind = score_industry(industry_en, capital_amount)

      total_score = score_gov + score_tech + score_sc + score_ind

      reasons = [
        {'dimension': 'government', 'score': score_gov, 'reason': reason_gov},
        {'dimension': 'technology', 'score': score_tech, 'reason': reason_tech},
        {'dimension': 'supply_chain', 'score': score_sc, 'reason': reason_sc},
        {'dimension': 'industry', 'score': score_ind, 'reason': reason_ind},
      ]

      reasons_json = json.dumps(reasons, ensure_ascii=False) if total_score > 0 else None

      updates.append((total_score, reasons_json, now_str, factory_id))
      processed += 1

      # 批次 commit
      if len(updates) >= COMMIT_BATCH:
        _flush_updates(conn, updates)
        updates = []
        print(f'  已處理 {processed:,} / {total_factories:,} 筆...', end='\r', flush=True)

    offset += PAGE_SIZE

  # 最後一批
  if updates:
    _flush_updates(conn, updates)

  print(f'\n  評分完成，共處理 {processed:,} 筆')

  # 4. 統計輸出
  print('\n[step 4] 統計結果...')
  _print_statistics(conn, processed)

  conn.close()
  print('\n[done] 完成')


def _flush_updates(conn: sqlite3.Connection, updates: list[tuple]) -> None:
  """批次更新 hidden champion 欄位。"""
  cur = conn.cursor()
  cur.executemany("""
    UPDATE factories
    SET hidden_champion_score = ?,
        hidden_champion_reasons = ?,
        hidden_champion_updated_at = ?
    WHERE id = ?
  """, updates)
  conn.commit()


def _print_statistics(conn: sqlite3.Connection, total_processed: int) -> None:
  cur = conn.cursor()

  cur.execute('SELECT COUNT(*) AS cnt FROM factories WHERE hidden_champion_score >= 50')
  high_potential = cur.fetchone()['cnt']

  cur.execute('SELECT COUNT(*) AS cnt FROM factories WHERE hidden_champion_score >= 30')
  potential = cur.fetchone()['cnt']

  print(f'  總共評分：{total_processed:,} 家')
  print(f'  score >= 50（高潛力隱形冠軍）：{high_potential:,} 家')
  print(f'  score >= 30（潛力隱形冠軍）：{potential:,} 家')

  print('\n  Top 20 隱形冠軍：')
  print(f'  {"排名":<4} {"名稱":<30} {"分數":<6} 主要理由')
  print('  ' + '-' * 90)

  cur.execute("""
    SELECT id, name_zh, name_en, hidden_champion_score, hidden_champion_reasons
    FROM factories
    WHERE hidden_champion_score > 0
    ORDER BY hidden_champion_score DESC
    LIMIT 20
  """)
  for rank, row in enumerate(cur.fetchall(), 1):
    score = row['hidden_champion_score']
    name = row['name_zh'] or row['name_en'] or f'(id={row["id"]})'
    reasons_str = ''
    if row['hidden_champion_reasons']:
      try:
        reasons_data = json.loads(row['hidden_champion_reasons'])
        top_reasons = [
          r['reason'] for r in reasons_data
          if r.get('score', 0) > 0 and r.get('reason')
        ]
        reasons_str = ' | '.join(top_reasons[:2])  # 最多顯示兩條
      except (json.JSONDecodeError, KeyError):
        reasons_str = row['hidden_champion_reasons'][:60]
    print(f'  {rank:<4} {name[:28]:<30} {score:<6} {reasons_str[:60]}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
  run_scoring()
