"""
scrape_mops_suppliers.py — 年報 PDF 供應商資料爬蟲

流程：
  1. 讀取 src/data/listed_companies.json 取得公司清單
  2. 對每家公司 GET doc.twse.com.tw 查詢年報檔名（F04 = 年報）
  3. POST 下載 PDF
  4. 用 pdfplumber 搜尋含「供應商」「供應狀況」「主要原料」的頁面
  5. 提取表格中的供應商名稱、金額、比例
  6. 輸出 src/data/supply_chain_raw.json

輸出格式：
  [{buyer_stock_code, buyer_name, supplier_name, supplier_tax_id,
    source, source_year, purchase_amount, purchase_ratio}]
"""

import io
import json
import logging
import re
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# SSL context（台灣政府網站憑證問題）
# ---------------------------------------------------------------------------

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# ---------------------------------------------------------------------------
# 設定常數
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / 'data'
OUTPUT_PATH = DATA_DIR / 'supply_chain_raw.json'
LISTED_COMPANIES_PATH = DATA_DIR / 'listed_companies.json'

MAX_COMPANIES = 1958      # 全量抓取所有上市櫃公司
DEFAULT_ROC_YEAR = 114    # 民國 114 年 = 2025，對應 2024 年報
REQUEST_TIMEOUT = 60
RETRY_MAX = 3
RETRY_DELAY = 5
REQUEST_DELAY = 3         # 每次請求間隔（秒）

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

TWSE_QUERY_URL = 'https://doc.twse.com.tw/server-java/t57sb01'

# 供應商相關關鍵字（頁面搜尋用）
SUPPLIER_PAGE_KEYWORDS = ['供應商', '供應狀況', '主要原料', '進貨', '主要供應']

# 供應商頁面必須同時具備的財務相關詞（AND 條件）
SUPPLIER_FINANCIAL_KEYWORDS = ['金額', '比例', '佔', '進貨', '採購', '%']

# 排除含這些關鍵字的頁面（公司治理等非供應商章節）
SUPPLIER_PAGE_EXCLUDE_KEYWORDS = ['公司治理', '股東常會', '董事會', '監察人', '委員會', '薪酬委員']

# 表格欄位關鍵字
COL_NAME_KEYWORDS = ['名稱', '廠商', '供應商']
COL_RATIO_KEYWORDS = ['比例', '佔', '%', '百分比']
COL_AMOUNT_KEYWORDS = ['金額', '採購', '進貨']

# 跳過的無意義供應商名稱
SKIP_SUPPLIER_NAMES = {'合計', '小計', '其他', '其它', '小  計', '合  計', '總計'}

# supplier_name 中若包含以下字串則視為噪音
SUPPLIER_NAME_NOISE_STRINGS = [
  '公司是否', '董事', '股東', '監察', '委員會', '薪酬', '審計',
  '公司治理', '內部控制', '章程', '議案', '決議', '報告事項',
  '討論事項', '選任', '解任', '通過', '同意',
]

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP 工具
# ---------------------------------------------------------------------------

def http_get(url: str, headers: Optional[dict] = None) -> bytes:
  """
  HTTP GET，含 retry。

  Args:
    url: 目標 URL
    headers: 額外請求標頭

  Returns:
    response bytes

  Raises:
    RuntimeError: 所有 retry 均失敗
  """
  default_headers = {'User-Agent': USER_AGENT}
  if headers:
    default_headers.update(headers)

  for attempt in range(1, RETRY_MAX + 1):
    try:
      req = urllib.request.Request(url, headers=default_headers)
      with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
        return resp.read()
    except urllib.error.HTTPError as e:
      log.warning(f'HTTP {e.code} on attempt {attempt}: {url}')
      if e.code == 404:
        raise RuntimeError(f'404 Not Found: {url}') from e
    except urllib.error.URLError as e:
      log.warning(f'URLError on attempt {attempt}: {e.reason}')
    except Exception as e:
      log.warning(f'Unexpected error on attempt {attempt}: {e}')

    if attempt < RETRY_MAX:
      log.info(f'Retrying in {RETRY_DELAY}s...')
      time.sleep(RETRY_DELAY)

  raise RuntimeError(f'All {RETRY_MAX} attempts failed for: {url}')


def http_post(url: str, data: dict, headers: Optional[dict] = None) -> bytes:
  """
  HTTP POST (application/x-www-form-urlencoded)，含 retry。

  Args:
    url: 目標 URL
    data: POST 表單欄位
    headers: 額外請求標頭

  Returns:
    response bytes

  Raises:
    RuntimeError: 所有 retry 均失敗
  """
  default_headers = {
    'User-Agent': USER_AGENT,
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': 'https://doc.twse.com.tw/',
  }
  if headers:
    default_headers.update(headers)

  encoded_data = urllib.parse.urlencode(data).encode('utf-8')

  for attempt in range(1, RETRY_MAX + 1):
    try:
      req = urllib.request.Request(
        url, data=encoded_data, headers=default_headers, method='POST'
      )
      with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=_SSL_CTX) as resp:
        return resp.read()
    except urllib.error.HTTPError as e:
      log.warning(f'HTTP {e.code} on attempt {attempt}: {url}')
    except urllib.error.URLError as e:
      log.warning(f'URLError on attempt {attempt}: {e.reason}')
    except Exception as e:
      log.warning(f'Unexpected error on attempt {attempt}: {e}')

    if attempt < RETRY_MAX:
      log.info(f'Retrying in {RETRY_DELAY}s...')
      time.sleep(RETRY_DELAY)

  raise RuntimeError(f'All {RETRY_MAX} attempts failed for: {url}')


# ---------------------------------------------------------------------------
# 年報 PDF 查詢與下載
# ---------------------------------------------------------------------------

def query_annual_report_filename(stock_code: str, roc_year: int) -> Optional[str]:
  """
  查詢指定公司指定年度的年報 PDF 檔名。

  doc.twse.com.tw 回傳 HTML，其中含有可下載的檔名清單。
  年報（mtype=F）中 filename 以 F04 結尾的為完整年報。

  Args:
    stock_code: 股票代號（如 '2330'）
    roc_year: 民國年（如 114）

  Returns:
    PDF 檔名字串，或 None（查無資料）
  """
  url = (
    f'{TWSE_QUERY_URL}?step=1&colorchg=1'
    f'&co_id={stock_code}&year={roc_year}&mtype=F&'
  )

  try:
    raw_bytes = http_get(url)
  except RuntimeError as e:
    log.warning(f'[{stock_code}] 查詢年報檔名失敗: {e}')
    return None

  # 解析 HTML 找 filename
  # 回傳格式通常是 <a href="...">XXXXXXXXf04.pdf</a> 或類似
  html = raw_bytes.decode('big5', errors='replace')

  # 從 HTML 中提取所有 PDF 檔名（引號包住的）
  all_pdfs = re.findall(r'"([^"]+\.pdf)"', html, re.IGNORECASE)
  if all_pdfs:
    # 優先選 F04（年報）
    for pdf in all_pdfs:
      if 'F04' in pdf or 'f04' in pdf:
        log.info(f'[{stock_code}] 找到年報檔名: {pdf}')
        return pdf
    log.info(f'[{stock_code}] 找到 PDF 但無 F04: {all_pdfs[:5]}')
    return None

  log.info(f'[{stock_code}] 未找到年報 PDF（roc_year={roc_year}）')
  return None


def download_annual_report_pdf(stock_code: str, filename: str) -> Optional[bytes]:
  """
  下載年報 PDF 內容。

  Args:
    stock_code: 股票代號
    filename: 由 query_annual_report_filename 取得的檔名

  Returns:
    PDF bytes，或 None（下載失敗）
  """
  post_data = {
    'step': '9',
    'kind': 'F',
    'co_id': stock_code,
    'filename': filename,
  }

  try:
    # Step 1: POST 取得含 PDF 連結的 HTML 頁面
    html_bytes = http_post(TWSE_QUERY_URL, post_data)
    html = html_bytes.decode('big5', errors='replace')

    # Step 2: 從 HTML 中提取實際 PDF URL
    match = re.search(r"href='(/pdf/[^']+\.pdf)'", html)
    if not match:
      match = re.search(r'href="(/pdf/[^"]+\.pdf)"', html)
    if not match:
      log.warning(f'[{stock_code}] POST 回應中找不到 PDF 連結')
      return None

    pdf_path = match.group(1)
    pdf_url = f'https://doc.twse.com.tw{pdf_path}'
    log.info(f'[{stock_code}] 取得 PDF URL: {pdf_url}')

    # Step 3: GET 下載實際 PDF
    time.sleep(1)
    pdf_bytes = http_get(pdf_url)
    if pdf_bytes[:4] != b'%PDF':
      log.warning(f'[{stock_code}] 下載內容非 PDF（前 4 bytes: {pdf_bytes[:4]}）')
      return None
    log.info(f'[{stock_code}] PDF 下載成功（{len(pdf_bytes):,} bytes）')
    return pdf_bytes
  except RuntimeError as e:
    log.warning(f'[{stock_code}] PDF 下載失敗: {e}')
    return None


# ---------------------------------------------------------------------------
# PDF 解析
# ---------------------------------------------------------------------------

def is_supplier_page(page_text: str) -> bool:
  """
  判斷頁面是否包含供應商相關內容。

  篩選邏輯（三個條件全部成立才回傳 True）：
    1. 頁面含供應商主題關鍵字（SUPPLIER_PAGE_KEYWORDS）
    2. 頁面含財務相關詞（SUPPLIER_FINANCIAL_KEYWORDS），確認是資料性頁面
    3. 頁面不含公司治理排除關鍵字（SUPPLIER_PAGE_EXCLUDE_KEYWORDS）

  Args:
    page_text: 頁面文字

  Returns:
    True 表示此頁可能有供應商資料
  """
  has_supplier_keyword = any(kw in page_text for kw in SUPPLIER_PAGE_KEYWORDS)
  if not has_supplier_keyword:
    return False

  has_financial_keyword = any(kw in page_text for kw in SUPPLIER_FINANCIAL_KEYWORDS)
  if not has_financial_keyword:
    return False

  has_exclude_keyword = any(kw in page_text for kw in SUPPLIER_PAGE_EXCLUDE_KEYWORDS)
  if has_exclude_keyword:
    return False

  return True


def is_valid_supplier_name(name: str) -> bool:
  """
  驗證供應商名稱是否合理。

  有效名稱必須：
    - 長度在 2-30 個字元之間
    - 不包含常見的非供應商噪音文字
    - 不是純數字或純標點
    - 不包含換行符（多行文字幾乎都是噪音）

  Args:
    name: 待驗證的供應商名稱

  Returns:
    True 表示名稱合理
  """
  if not name:
    return False

  stripped = name.strip()

  # 長度檢查
  if len(stripped) < 2 or len(stripped) > 30:
    return False

  # 含換行符視為多行噪音
  if '\n' in stripped:
    return False

  # 純數字或純標點
  if re.match(r'^[\d\s\W]+$', stripped):
    return False

  # 含噪音關鍵字
  if any(noise in stripped for noise in SUPPLIER_NAME_NOISE_STRINGS):
    return False

  return True


def is_valid_purchase_ratio(ratio: Optional[float]) -> bool:
  """
  驗證採購比例是否合理。

  有效比例必須在 0.1 到 100 之間，避免將頁碼或章節編號誤判為比例。

  Args:
    ratio: 待驗證的比例值（百分比）

  Returns:
    True 表示比例合理（None 視為未知，回傳 True 以保留記錄）
  """
  if ratio is None:
    return True
  return 0.1 <= ratio <= 100


def parse_float(text: str) -> Optional[float]:
  """
  嘗試將字串解析為浮點數，去除常見格式符號。

  Args:
    text: 原始字串（可能含逗號、空格、%）

  Returns:
    float 或 None
  """
  cleaned = text.strip().rstrip('%').replace(',', '').replace(' ', '')
  if not cleaned:
    return None
  try:
    return float(cleaned)
  except ValueError:
    return None


def find_col_index(headers: list[str], keywords: list[str]) -> Optional[int]:
  """
  在 header 列表中找包含任一關鍵字的欄位索引。

  Args:
    headers: 表頭欄位字串列表
    keywords: 目標關鍵字

  Returns:
    欄位 index 或 None
  """
  for i, h in enumerate(headers):
    if any(kw in h for kw in keywords):
      return i
  return None


def extract_suppliers_from_table(
  rows: list[list[str]],
  stock_code: str,
  company_name: str,
  source_year: int,
) -> list[dict[str, Any]]:
  """
  從 pdfplumber 解出的表格 rows 提取供應商記錄。

  嘗試自動識別表頭並映射欄位。若無法辨識表頭，嘗試啟發式解析。

  Args:
    rows: 表格 rows（list of list of str）
    stock_code: 買方股票代號
    company_name: 買方公司名稱
    source_year: 資料年度（西元年）

  Returns:
    供應商 dict list
  """
  if not rows or len(rows) < 2:
    return []

  records = []

  # 找表頭列（含名稱或供應商關鍵字的那列）
  header_row_idx = None
  for idx, row in enumerate(rows[:5]):  # 只看前 5 列
    row_text = ' '.join(str(c) for c in row if c)
    if any(kw in row_text for kw in COL_NAME_KEYWORDS):
      header_row_idx = idx
      break

  if header_row_idx is None:
    log.debug(f'[{stock_code}] 無法辨識表頭，嘗試啟發式解析')
    return _heuristic_extract(rows, stock_code, company_name, source_year)

  headers = [str(c) if c else '' for c in rows[header_row_idx]]
  name_col = find_col_index(headers, COL_NAME_KEYWORDS)
  ratio_col = find_col_index(headers, COL_RATIO_KEYWORDS)
  amount_col = find_col_index(headers, COL_AMOUNT_KEYWORDS)

  if name_col is None:
    return []

  for row in rows[header_row_idx + 1:]:
    if not row or name_col >= len(row):
      continue

    supplier_name = str(row[name_col]).strip() if row[name_col] else ''
    if not supplier_name or supplier_name in SKIP_SUPPLIER_NAMES:
      continue
    if not is_valid_supplier_name(supplier_name):
      log.debug(f'[{stock_code}] 跳過無效供應商名稱: {repr(supplier_name[:50])}')
      continue

    purchase_ratio: Optional[float] = None
    if ratio_col is not None and ratio_col < len(row) and row[ratio_col]:
      raw_ratio = parse_float(str(row[ratio_col]))
      if is_valid_purchase_ratio(raw_ratio):
        purchase_ratio = raw_ratio
      else:
        log.debug(f'[{stock_code}] 跳過無效比例值: {row[ratio_col]}')

    purchase_amount: Optional[float] = None
    if amount_col is not None and amount_col < len(row) and row[amount_col]:
      purchase_amount = parse_float(str(row[amount_col]))

    records.append({
      'buyer_stock_code': stock_code,
      'buyer_name': company_name,
      'supplier_name': supplier_name,
      'supplier_tax_id': None,
      'source': 'annual_report_pdf',
      'source_year': source_year,
      'purchase_amount': purchase_amount,
      'purchase_ratio': purchase_ratio,
    })

  return records


def _heuristic_extract(
  rows: list[list[str]],
  stock_code: str,
  company_name: str,
  source_year: int,
) -> list[dict[str, Any]]:
  """
  啟發式解析：當無法識別標準表頭時，嘗試從文字特徵抓供應商。

  策略：找含中文公司名稱特徵（有限公司、股份、公司）且後面接數字的列。

  Args:
    rows: 表格 rows
    stock_code: 買方股票代號
    company_name: 買方公司名稱
    source_year: 資料年度（西元年）

  Returns:
    供應商 dict list（可能為空）
  """
  records = []
  company_pattern = re.compile(r'([\w\s]+(?:有限公司|股份|公司|企業|工業|科技|電子|實業)[\w\s]*)')

  for row in rows:
    row_text = ' '.join(str(c) for c in row if c)
    match = company_pattern.search(row_text)
    if not match:
      continue

    supplier_name = match.group(1).strip()
    if supplier_name in SKIP_SUPPLIER_NAMES:
      continue
    if not is_valid_supplier_name(supplier_name):
      log.debug(f'[{stock_code}] 啟發式解析跳過無效名稱: {repr(supplier_name[:50])}')
      continue

    # 嘗試找同列的比例數字
    numbers = re.findall(r'\b(\d+(?:\.\d+)?)\s*%?', row_text)
    purchase_ratio: Optional[float] = None
    for num_str in numbers:
      val = parse_float(num_str)
      if val is not None and 0 < val <= 100:
        purchase_ratio = val
        break

    records.append({
      'buyer_stock_code': stock_code,
      'buyer_name': company_name,
      'supplier_name': supplier_name,
      'supplier_tax_id': None,
      'source': 'annual_report_pdf',
      'source_year': source_year,
      'purchase_amount': None,
      'purchase_ratio': purchase_ratio,
    })

  return records


def extract_suppliers_from_text(
  page_text: str,
  stock_code: str,
  company_name: str,
  source_year: int,
) -> list[dict[str, Any]]:
  """
  當頁面無表格時，嘗試從純文字提取供應商資料。

  搜尋類似「供應商名稱：XXX公司，佔比 15%」的文字模式。

  Args:
    page_text: 頁面純文字
    stock_code: 買方股票代號
    company_name: 買方公司名稱
    source_year: 資料年度（西元年）

  Returns:
    供應商 dict list（通常為空或少量）
  """
  records = []

  # 常見文字格式：「1. XXX公司 金額 XX,XXX 千元 佔XX%」
  pattern = re.compile(
    r'(\d+[.、]?\s*)'            # 序號（可選）
    r'([\w\s]{3,20}(?:公司|股份|企業|工業|科技|電子|實業)[\w\s]*?)'  # 供應商名稱
    r'.*?'
    r'(\d+(?:\.\d+)?)\s*%',     # 比例
    re.DOTALL
  )

  for match in pattern.finditer(page_text):
    supplier_name = match.group(2).strip()
    ratio_str = match.group(3)

    if not supplier_name or supplier_name in SKIP_SUPPLIER_NAMES:
      continue

    purchase_ratio = parse_float(ratio_str)
    if purchase_ratio is None or purchase_ratio <= 0 or purchase_ratio > 100:
      continue

    records.append({
      'buyer_stock_code': stock_code,
      'buyer_name': company_name,
      'supplier_name': supplier_name,
      'supplier_tax_id': None,
      'source': 'annual_report_pdf',
      'source_year': source_year,
      'purchase_amount': None,
      'purchase_ratio': purchase_ratio,
    })

  return records


def parse_pdf_for_suppliers(
  pdf_bytes: bytes,
  stock_code: str,
  company_name: str,
  roc_year: int,
) -> list[dict[str, Any]]:
  """
  用 pdfplumber 解析 PDF，提取供應商資料。

  流程：
    1. 搜尋含供應商關鍵字的頁面
    2. 對每個候選頁面嘗試提取表格
    3. 若無表格，嘗試文字解析
    4. 彙整去重後回傳

  Args:
    pdf_bytes: PDF 二進位內容
    stock_code: 股票代號
    company_name: 公司名稱
    roc_year: 民國年（用於推算西元年）

  Returns:
    供應商 dict list
  """
  try:
    import pdfplumber
  except ImportError:
    log.error('pdfplumber 未安裝，請執行: pip install pdfplumber')
    return []

  source_year = roc_year + 1911

  records: list[dict[str, Any]] = []

  try:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
      total_pages = len(pdf.pages)
      log.info(f'[{stock_code}] PDF 共 {total_pages} 頁')

      candidate_pages = []
      for i, page in enumerate(pdf.pages):
        try:
          page_text = page.extract_text() or ''
          if is_supplier_page(page_text):
            candidate_pages.append((i, page, page_text))
        except Exception as e:
          log.debug(f'[{stock_code}] 頁 {i+1} 文字提取失敗: {e}')
          continue

      log.info(f'[{stock_code}] 找到 {len(candidate_pages)} 個供應商候選頁面')

      for page_idx, page, page_text in candidate_pages:
        log.debug(f'[{stock_code}] 解析第 {page_idx+1} 頁...')

        # 嘗試提取表格
        page_records: list[dict[str, Any]] = []
        try:
          tables = page.extract_tables()
          if tables:
            for table in tables:
              # pdfplumber 的 table 是 list[list[str|None]]
              # 轉換為 list[list[str]]
              str_table = [
                [str(c) if c is not None else '' for c in row]
                for row in table
              ]
              table_records = extract_suppliers_from_table(
                str_table, stock_code, company_name, source_year
              )
              page_records.extend(table_records)
        except Exception as e:
          log.debug(f'[{stock_code}] 頁 {page_idx+1} 表格提取失敗: {e}')

        # 若表格沒找到，嘗試文字解析
        if not page_records and page_text:
          page_records = extract_suppliers_from_text(
            page_text, stock_code, company_name, source_year
          )

        if page_records:
          log.info(f'[{stock_code}] 第 {page_idx+1} 頁找到 {len(page_records)} 筆供應商')
          records.extend(page_records)

  except Exception as e:
    log.warning(f'[{stock_code}] PDF 解析失敗: {e}')
    return []

  # 去重（同一供應商名稱在同一公司只保留一筆）
  seen = set()
  deduped = []
  for r in records:
    key = (r['buyer_stock_code'], r['supplier_name'])
    if key not in seen:
      seen.add(key)
      deduped.append(r)

  # 後處理過濾：移除高機率噪音記錄
  cleaned = []
  noise_count = 0
  for r in deduped:
    name = r.get('supplier_name', '')
    ratio = r.get('purchase_ratio')
    amount = r.get('purchase_amount')

    # 移除 supplier_name 超過 20 個字的記錄
    if len(name) > 20:
      log.debug(f'[{stock_code}] 後處理移除長名稱: {repr(name[:50])}')
      noise_count += 1
      continue

    # 移除 supplier_name 含 3 個以上換行的記錄
    if name.count('\n') >= 3:
      log.debug(f'[{stock_code}] 後處理移除多行名稱: {repr(name[:50])}')
      noise_count += 1
      continue

    # 移除完全沒有財務數據的記錄（purchase_ratio 和 purchase_amount 均為 None）
    if ratio is None and amount is None:
      log.debug(f'[{stock_code}] 後處理移除無財務數據記錄: {name}')
      noise_count += 1
      continue

    cleaned.append(r)

  if noise_count > 0:
    log.info(f'[{stock_code}] 後處理過濾了 {noise_count} 筆噪音記錄')

  return cleaned


# ---------------------------------------------------------------------------
# 主要流程
# ---------------------------------------------------------------------------

def process_company(
  stock_code: str,
  company_name: str,
  roc_year: int,
) -> list[dict[str, Any]]:
  """
  對單一公司執行完整流程：查詢 → 下載 → 解析。

  Args:
    stock_code: 股票代號
    company_name: 公司名稱
    roc_year: 民國年

  Returns:
    供應商 dict list（空 list 表示無資料或失敗）
  """
  log.info(f'[{stock_code}] 開始處理: {company_name}')

  # Step 1: 查詢年報檔名
  time.sleep(REQUEST_DELAY)
  filename = query_annual_report_filename(stock_code, roc_year)
  if not filename:
    # 嘗試上一年
    log.info(f'[{stock_code}] 嘗試上一年 ({roc_year - 1})...')
    time.sleep(REQUEST_DELAY)
    filename = query_annual_report_filename(stock_code, roc_year - 1)
    if not filename:
      log.info(f'[{stock_code}] 無年報可下載，略過')
      return []

  # Step 2: 下載 PDF
  time.sleep(REQUEST_DELAY)
  pdf_bytes = download_annual_report_pdf(stock_code, filename)
  if not pdf_bytes:
    return []

  # Step 3: 解析 PDF
  records = parse_pdf_for_suppliers(pdf_bytes, stock_code, company_name, roc_year)
  log.info(f'[{stock_code}] 共提取 {len(records)} 筆供應商記錄')

  return records


def load_listed_companies() -> list[dict[str, Any]]:
  """
  讀取 listed_companies.json。

  Returns:
    公司 dict list；若檔案不存在回傳空 list 並記錄警告
  """
  if not LISTED_COMPANIES_PATH.exists():
    log.warning(f'listed_companies.json 不存在: {LISTED_COMPANIES_PATH}')
    log.warning('請先執行 fetch_listed_companies.py 產生公司清單')
    return []

  with LISTED_COMPANIES_PATH.open('r', encoding='utf-8') as f:
    return json.load(f)


def main() -> None:
  DATA_DIR.mkdir(parents=True, exist_ok=True)

  companies = load_listed_companies()
  if not companies:
    log.error('無公司清單，中止執行')
    return

  target_companies = companies[:MAX_COMPANIES]
  log.info(f'共 {len(companies)} 家公司，本次處理前 {len(target_companies)} 家（MAX_COMPANIES={MAX_COMPANIES}）')

  all_records: list[dict[str, Any]] = []
  success_count = 0
  fail_count = 0

  for i, company in enumerate(target_companies, 1):
    stock_code = company.get('stock_code', '')
    company_name = company.get('company_name', '')

    if not stock_code:
      log.warning(f'第 {i} 筆公司資料缺少 stock_code，略過')
      continue

    log.info(f'=== [{i}/{len(target_companies)}] {stock_code} {company_name} ===')

    try:
      records = process_company(stock_code, company_name, DEFAULT_ROC_YEAR)
      if records:
        all_records.extend(records)
        success_count += 1
      else:
        fail_count += 1
    except Exception as e:
      log.error(f'[{stock_code}] 處理時發生未預期錯誤: {e}')
      fail_count += 1

  # 全域去重
  seen_keys: set[tuple] = set()
  deduped_records: list[dict[str, Any]] = []
  for record in all_records:
    key = (
      record.get('buyer_stock_code', ''),
      record.get('supplier_name', ''),
      record.get('source_year', 0),
    )
    if key not in seen_keys:
      seen_keys.add(key)
      deduped_records.append(record)

  log.info(f'去重後: {len(deduped_records)} 筆（原始 {len(all_records)} 筆）')

  # 輸出
  with OUTPUT_PATH.open('w', encoding='utf-8') as f:
    json.dump(deduped_records, f, ensure_ascii=False, indent=2)

  log.info(f'已輸出 {len(deduped_records)} 筆至 {OUTPUT_PATH}')
  log.info(f'處理結果: 成功 {success_count} 家，失敗/無資料 {fail_count} 家')


if __name__ == '__main__':
  main()
