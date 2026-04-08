"""
LLM 批次翻譯公司名稱 (GPT-4o-mini)

從 SQLite 讀取 needs_llm_translation=1 的記錄，批次呼叫 GPT-4o-mini 翻譯，
結果寫回 DB 並重建 FTS5 索引。

用法:
  python3 -m scripts.llm_translate_names                # 完整翻譯
  python3 -m scripts.llm_translate_names --dry-run      # 只顯示筆數與預估成本
  python3 -m scripts.llm_translate_names --limit 100    # 只翻譯前 100 筆（測試用）
  python3 -m scripts.llm_translate_names --status       # 顯示目前進度
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 路徑常數
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
PROGRESS_PATH = BASE_DIR / 'data' / 'llm_progress.json'

# ---------------------------------------------------------------------------
# GPT-4o-mini 費率（每 1M tokens，美元）
# ---------------------------------------------------------------------------
PRICE_INPUT_PER_M = 0.150   # $0.150 / 1M tokens
PRICE_OUTPUT_PER_M = 0.600  # $0.600 / 1M tokens

# 每筆名稱的 token 估算（含 prompt overhead 分攤）
AVG_INPUT_TOKENS_PER_NAME = 20   # 名稱本身 ~15-17 + overhead 分攤
AVG_OUTPUT_TOKENS_PER_NAME = 13  # 英文翻譯 ~10-15

BATCH_SIZE = 50
MAX_RETRIES = 3
RATE_LIMIT_CALLS_PER_SEC = 5   # 每秒最多 5 次 API call
MIN_SLEEP_BETWEEN_BATCHES = 1.0 / RATE_LIMIT_CALLS_PER_SEC  # 0.2s

SYSTEM_PROMPT = """你是台灣公司名稱的中英翻譯專家。請將以下台灣公司/工廠名稱翻譯為英文。

規則：
1. 固有名詞（品牌名、人名）使用官方英文名或音譯
2. 常見後綴：股份有限公司 = Co., Ltd.、有限公司 = Ltd.、企業社 = Enterprise
3. 產業詞：精密 = Precision、科技 = Technology、工業 = Industrial、電子 = Electronics
4. 廠區名：第一廠 = Plant 1、桃園廠 = Taoyuan Plant
5. 保持簡潔，不加額外說明

每行輸入一個中文名稱，請每行輸出對應的英文翻譯，行數必須與輸入完全一致。"""


# ---------------------------------------------------------------------------
# 進度管理（斷點續傳）
# ---------------------------------------------------------------------------

def load_progress() -> set[int]:
  """載入已翻譯的 ID 集合。"""
  if PROGRESS_PATH.exists():
    try:
      data = json.loads(PROGRESS_PATH.read_text(encoding='utf-8'))
      return set(data.get('translated_ids', []))
    except (json.JSONDecodeError, KeyError):
      return set()
  return set()


def save_progress(translated_ids: set[int]) -> None:
  """儲存已翻譯的 ID 集合。"""
  PROGRESS_PATH.write_text(
    json.dumps(
      {'translated_ids': sorted(translated_ids)},
      ensure_ascii=False,
      indent=2,
    ),
    encoding='utf-8',
  )


# ---------------------------------------------------------------------------
# DB 工具
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
  """建立並回傳 SQLite 連線。"""
  conn = sqlite3.connect(str(DB_PATH))
  conn.row_factory = sqlite3.Row
  return conn


def fetch_pending(conn: sqlite3.Connection, limit: Optional[int] = None) -> list[sqlite3.Row]:
  """讀取 needs_llm_translation=1 的所有記錄。"""
  sql = 'SELECT id, name_zh, name_en FROM factories WHERE needs_llm_translation=1 ORDER BY id'
  if limit:
    sql += f' LIMIT {limit}'
  return conn.execute(sql).fetchall()


def update_batch(
  conn: sqlite3.Connection,
  updates: list[tuple[str, int]],
) -> None:
  """批次更新翻譯結果：(name_en, id)"""
  conn.executemany(
    'UPDATE factories SET name_en = ?, needs_llm_translation = 0 WHERE id = ?',
    updates,
  )
  conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
  """重建 FTS5 索引。"""
  print('\n重建 FTS5 索引...')
  conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
  conn.commit()
  print('FTS5 索引重建完成。')


# ---------------------------------------------------------------------------
# 成本估算
# ---------------------------------------------------------------------------

def estimate_cost(count: int) -> tuple[float, float, float]:
  """
  估算翻譯成本。

  Returns:
    (input_cost, output_cost, total_cost) 單位 USD
  """
  input_tokens = count * AVG_INPUT_TOKENS_PER_NAME
  output_tokens = count * AVG_OUTPUT_TOKENS_PER_NAME
  input_cost = input_tokens / 1_000_000 * PRICE_INPUT_PER_M
  output_cost = output_tokens / 1_000_000 * PRICE_OUTPUT_PER_M
  return input_cost, output_cost, input_cost + output_cost


def calc_actual_cost(prompt_tokens: int, completion_tokens: int) -> float:
  """計算實際 API 費用（美元）。"""
  return (
    prompt_tokens / 1_000_000 * PRICE_INPUT_PER_M
    + completion_tokens / 1_000_000 * PRICE_OUTPUT_PER_M
  )


# ---------------------------------------------------------------------------
# OpenAI 翻譯
# ---------------------------------------------------------------------------

def translate_batch(
  client,
  names_zh: list[str],
) -> tuple[list[str], int, int]:
  """
  呼叫 GPT-4o-mini 翻譯一批公司名稱，含 exponential backoff retry。

  Returns:
    (translations, prompt_tokens, completion_tokens)

  Raises:
    ValueError: 回傳行數與輸入不一致。
    Exception: 超過最大重試次數。
  """
  user_message = '\n'.join(names_zh)

  for attempt in range(MAX_RETRIES):
    try:
      response = client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
          {'role': 'system', 'content': SYSTEM_PROMPT},
          {'role': 'user', 'content': user_message},
        ],
        temperature=0.1,
      )
      break
    except Exception as e:
      err_str = str(e)
      is_rate_limit = '429' in err_str or 'rate_limit' in err_str.lower()
      if attempt < MAX_RETRIES - 1:
        wait = 2 ** attempt  # 1s → 2s → 4s
        reason = 'Rate limit' if is_rate_limit else f'錯誤（{type(e).__name__}）'
        print(f'\n  {reason}，等待 {wait}s 後重試（第 {attempt + 1}/{MAX_RETRIES} 次）...', end='', flush=True)
        time.sleep(wait)
      else:
        raise

  content = response.choices[0].message.content or ''
  translations = [line.strip() for line in content.strip().split('\n') if line.strip()]

  usage = response.usage
  prompt_tokens = usage.prompt_tokens if usage else 0
  completion_tokens = usage.completion_tokens if usage else 0

  if len(translations) != len(names_zh):
    raise ValueError(
      f'行數不一致：輸入 {len(names_zh)} 行，回傳 {len(translations)} 行\n'
      f'回傳內容：{content[:200]}'
    )

  return translations, prompt_tokens, completion_tokens


# ---------------------------------------------------------------------------
# 主要流程
# ---------------------------------------------------------------------------

def cmd_status(conn: sqlite3.Connection) -> None:
  """顯示翻譯進度統計。"""
  total = conn.execute('SELECT COUNT(*) FROM factories').fetchone()[0]
  needs = conn.execute('SELECT COUNT(*) FROM factories WHERE needs_llm_translation=1').fetchone()[0]
  done = total - needs

  translated_ids = load_progress()
  progress_count = len(translated_ids)

  _, _, estimated_cost = estimate_cost(needs)

  print('=== 翻譯進度 ===')
  print(f'總筆數:              {total:>10,}')
  print(f'已翻譯 (needs=0):    {done:>10,} ({done / total * 100:.1f}%)')
  print(f'待翻譯 (needs=1):    {needs:>10,} ({needs / total * 100:.1f}%)')
  print(f'進度檔已記錄 ID 數:  {progress_count:>10,}')
  print(f'剩餘預估費用:        ${estimated_cost:.4f} USD')


def cmd_dry_run(conn: sqlite3.Connection) -> None:
  """Dry-run：只顯示筆數和預估成本，不實際呼叫 API。"""
  translated_ids = load_progress()
  records = fetch_pending(conn)
  pending = [r for r in records if r['id'] not in translated_ids]
  count = len(pending)

  input_cost, output_cost, total_cost = estimate_cost(count)
  input_tokens = count * AVG_INPUT_TOKENS_PER_NAME
  output_tokens = count * AVG_OUTPUT_TOKENS_PER_NAME
  batches = (count + BATCH_SIZE - 1) // BATCH_SIZE

  print('=== Dry-Run 模式（不實際呼叫 API）===')
  print(f'待翻譯筆數:          {count:>10,}')
  print(f'預計批次數:          {batches:>10,} 批（每批 {BATCH_SIZE} 筆）')
  print(f'預估 input tokens:   {input_tokens:>10,}')
  print(f'預估 output tokens:  {output_tokens:>10,}')
  print(f'預估 input 費用:     ${input_cost:.4f} USD')
  print(f'預估 output 費用:    ${output_cost:.4f} USD')
  print(f'預估總費用:          ${total_cost:.4f} USD')
  print(f'預估執行時間:        {batches * MIN_SLEEP_BETWEEN_BATCHES / 60:.1f} 分鐘（最少）')
  print()
  print('執行翻譯請移除 --dry-run 參數，並確保設定 OPENAI_API_KEY 環境變數。')


def run_translation(
  conn: sqlite3.Connection,
  client,
  records: list[sqlite3.Row],
) -> None:
  """執行翻譯主迴圈。"""
  # 載入進度，跳過已翻譯
  translated_ids = load_progress()
  pending = [r for r in records if r['id'] not in translated_ids]
  total = len(pending)

  if not total:
    print('所有記錄均已翻譯完成（斷點續傳進度檔顯示無需重做）。')
    print('如需重新翻譯，請刪除 data/llm_progress.json 後再執行。')
    return

  skipped = len(records) - total
  if skipped:
    print(f'斷點續傳：跳過已翻譯 {skipped:,} 筆，剩餘 {total:,} 筆。')

  total_prompt_tokens = 0
  total_completion_tokens = 0
  total_done = 0
  total_errors = 0
  batch_count = 0
  start_time = time.time()

  print(f'開始翻譯，共 {total:,} 筆，批次大小 {BATCH_SIZE}...')

  for i in range(0, total, BATCH_SIZE):
    batch = pending[i:i + BATCH_SIZE]
    batch_ids = [r['id'] for r in batch]
    batch_zh = [r['name_zh'] for r in batch]
    batch_num = batch_count + 1
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    print(
      f'\r批次 {batch_num}/{total_batches}（筆 {i + 1}~{min(i + BATCH_SIZE, total)}/{total}）...',
      end='',
      flush=True,
    )

    try:
      translations, pt, ct = translate_batch(client, batch_zh)

      updates = [(t, row['id']) for row, t in zip(batch, translations)]
      update_batch(conn, updates)

      # 更新進度
      translated_ids.update(batch_ids)
      save_progress(translated_ids)

      total_prompt_tokens += pt
      total_completion_tokens += ct
      total_done += len(updates)
      batch_count += 1

      cost_so_far = calc_actual_cost(total_prompt_tokens, total_completion_tokens)
      print(f' 完成 {len(updates)} 筆 | 累計費用 ${cost_so_far:.4f}', end='', flush=True)

    except ValueError as ve:
      # 行數不一致：跳過此批，記錄 warning
      print(f'\n  警告（跳過此批）: {ve}')
      total_errors += len(batch)

    except Exception as e:
      # API 嚴重錯誤：儲存進度後中止
      print(f'\n  API 錯誤（中止）: {e}')
      save_progress(translated_ids)
      raise

    # Rate limiting
    time.sleep(MIN_SLEEP_BETWEEN_BATCHES)

  elapsed = time.time() - start_time
  final_cost = calc_actual_cost(total_prompt_tokens, total_completion_tokens)

  print(f'\n\n=== 翻譯完成 ===')
  print(f'成功翻譯:        {total_done:>10,} 筆')
  print(f'跳過（錯誤）:    {total_errors:>10,} 筆')
  print(f'執行批次:        {batch_count:>10,}')
  print(f'Prompt tokens:   {total_prompt_tokens:>10,}')
  print(f'Completion tokens:{total_completion_tokens:>9,}')
  print(f'實際費用:        ${final_cost:.4f} USD')
  print(f'執行時間:        {elapsed / 60:.1f} 分鐘')


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description='LLM 批次翻譯台灣工廠名稱（GPT-4o-mini）')
  parser.add_argument(
    '--dry-run',
    action='store_true',
    help='只顯示筆數與預估成本，不實際呼叫 API',
  )
  parser.add_argument(
    '--status',
    action='store_true',
    help='顯示目前翻譯進度後退出',
  )
  parser.add_argument(
    '--limit',
    type=int,
    default=None,
    metavar='N',
    help='只翻譯前 N 筆（測試用）',
  )
  return parser.parse_args()


def main() -> None:
  args = parse_args()

  # 載入 .env（若存在）
  env_file = BASE_DIR / '.env'
  if env_file.exists():
    for line in env_file.read_text(encoding='utf-8').splitlines():
      line = line.strip()
      if line and not line.startswith('#') and '=' in line:
        key, _, value = line.partition('=')
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)

  conn = get_connection()

  if args.status:
    cmd_status(conn)
    conn.close()
    return

  if args.dry_run:
    cmd_dry_run(conn)
    conn.close()
    return

  # 需要 API key 才能繼續
  openai_key = os.environ.get('OPENAI_API_KEY')
  if not openai_key:
    print('錯誤：OPENAI_API_KEY 未設定。')
    print('請先執行：export OPENAI_API_KEY=sk-...')
    print('或建立 .env 檔案寫入：OPENAI_API_KEY=sk-...')
    print()
    print('若只想查看預估成本，請加上 --dry-run 參數。')
    conn.close()
    return

  try:
    from openai import OpenAI
  except ImportError:
    print('錯誤：openai 套件未安裝。')
    print('請先執行：pip install openai')
    conn.close()
    return

  client = OpenAI(api_key=openai_key)
  records = fetch_pending(conn, limit=args.limit)

  if not records:
    print('沒有 needs_llm_translation=1 的記錄，無需翻譯。')
    conn.close()
    return

  run_translation(conn, client, records)

  # 翻譯完成後重建 FTS5 索引
  rebuild_fts(conn)

  conn.close()


if __name__ == '__main__':
  main()
