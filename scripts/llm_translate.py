"""
LLM 批次翻譯腳本

將資料庫中 name_en 仍含中文字元的工廠名稱，
透過 OpenAI GPT-4o-mini 批次翻譯為英文，並更新回資料庫。

用法:
  python3 -m scripts.llm_translate               # 完整翻譯
  python3 -m scripts.llm_translate --limit 100   # 只翻譯前 100 筆（測試用）
  python3 -m scripts.llm_translate --status       # 顯示翻譯進度
  python3 -m scripts.llm_translate --retry-errors # 重試錯誤的批次
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 路徑常數
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'
ERRORS_PATH = BASE_DIR / 'data' / 'translation_errors.json'

# ---------------------------------------------------------------------------
# 中文字元偵測
# ---------------------------------------------------------------------------
CJK_RE = re.compile(r'[\u4e00-\u9fff]')

# ---------------------------------------------------------------------------
# GPT-4o-mini 費率（每 1000 token，美元）
# ---------------------------------------------------------------------------
PRICE_INPUT_PER_1K = 0.000150   # $0.150 / 1M tokens
PRICE_OUTPUT_PER_1K = 0.000600  # $0.600 / 1M tokens

SYSTEM_PROMPT = """你是台灣公司名稱的中英翻譯專家。請將以下台灣公司/工廠名稱翻譯為英文。

規則：
1. 固有名詞（品牌名、人名）使用官方英文名或音譯
2. 常見後綴：股份有限公司 = Co., Ltd.、有限公司 = Ltd.、企業社 = Enterprise
3. 產業詞：精密 = Precision、科技 = Technology、工業 = Industrial、電子 = Electronics
4. 廠區名：第一廠 = Plant 1、桃園廠 = Taoyuan Plant
5. 保持簡潔，不加額外說明

每行輸入一個中文名稱，請每行輸出對應的英文翻譯。行數必須一致。"""

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 0.5
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# DB 工具
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """建立並回傳 SQLite 連線。"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_translation_status_column(conn: sqlite3.Connection) -> None:
    """若 translation_status 欄位不存在則新增之。"""
    cursor = conn.execute("PRAGMA table_info(factories)")
    columns = [row['name'] for row in cursor.fetchall()]
    if 'translation_status' not in columns:
        conn.execute(
            "ALTER TABLE factories ADD COLUMN translation_status TEXT DEFAULT 'pending'"
        )
        # 已是完整英文的記錄標記為 done
        conn.execute(
            """
            UPDATE factories
            SET translation_status = 'done'
            WHERE name_en NOT REGEXP_CONTAINS_CJK
            """
        )
        conn.commit()


def ensure_translation_status_column_v2(conn: sqlite3.Connection) -> None:
    """若 translation_status 欄位不存在則新增，並初始化狀態。"""
    cursor = conn.execute("PRAGMA table_info(factories)")
    columns = [row['name'] for row in cursor.fetchall()]
    if 'translation_status' not in columns:
        conn.execute(
            "ALTER TABLE factories ADD COLUMN translation_status TEXT DEFAULT 'pending'"
        )
        conn.commit()

    # 將已不含中文的 name_en 標記為 done
    rows = conn.execute("SELECT id, name_en FROM factories WHERE translation_status IS NULL OR translation_status = 'pending'").fetchall()
    done_ids = [row['id'] for row in rows if not CJK_RE.search(row['name_en'] or '')]
    if done_ids:
        conn.execute(
            f"UPDATE factories SET translation_status = 'done' WHERE id IN ({','.join('?' * len(done_ids))})",
            done_ids
        )
        conn.commit()


def fetch_pending(conn: sqlite3.Connection, limit: Optional[int] = None) -> list[sqlite3.Row]:
    """讀取需要翻譯的記錄（status = 'pending' 且 name_en 含中文）。"""
    sql = """
        SELECT id, name_zh, name_en
        FROM factories
        WHERE (translation_status = 'pending' OR translation_status IS NULL)
        ORDER BY id
    """
    if limit:
        sql += f" LIMIT {limit}"
    return conn.execute(sql).fetchall()


def fetch_error_records(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """讀取翻譯失敗的記錄。"""
    return conn.execute(
        "SELECT id, name_zh, name_en FROM factories WHERE translation_status = 'error' ORDER BY id"
    ).fetchall()


def update_translations(
    conn: sqlite3.Connection,
    updates: list[tuple[str, str, int]]
) -> None:
    """批次更新翻譯結果。updates = [(name_en, status, id), ...]"""
    conn.executemany(
        "UPDATE factories SET name_en = ?, translation_status = ? WHERE id = ?",
        updates
    )
    conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """重建 FTS5 索引。"""
    print('\n重建 FTS5 索引...')
    conn.execute("INSERT INTO factories_fts(factories_fts) VALUES('rebuild')")
    conn.commit()
    print('FTS5 索引重建完成。')


# ---------------------------------------------------------------------------
# 錯誤日誌
# ---------------------------------------------------------------------------

def load_error_log() -> list[dict]:
    """載入錯誤日誌。"""
    if ERRORS_PATH.exists():
        return json.loads(ERRORS_PATH.read_text(encoding='utf-8'))
    return []


def append_error_log(entries: list[dict]) -> None:
    """追加錯誤記錄到 JSON 日誌。"""
    log = load_error_log()
    log.extend(entries)
    ERRORS_PATH.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding='utf-8')


# ---------------------------------------------------------------------------
# OpenAI 翻譯
# ---------------------------------------------------------------------------

def detect_model(client) -> str:
    """根據 client 的 base_url 選擇模型。"""
    base_url = str(getattr(client, '_base_url', ''))
    if 'minimax' in base_url:
        return 'MiniMax-Text-01'
    return 'gpt-4o-mini'


def translate_batch(
    client,
    names_zh: list[str],
    model: Optional[str] = None,
) -> tuple[list[str], int, int]:
    """
    呼叫 LLM 翻譯一批公司名稱。支援 OpenAI 和 MiniMax。

    Returns:
        (translations, prompt_tokens, completion_tokens)

    Raises:
        ValueError: 若回傳行數與輸入不一致。
    """
    if model is None:
        model = detect_model(client)

    user_message = '\n'.join(names_zh)

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {'role': 'system', 'content': SYSTEM_PROMPT},
                    {'role': 'user', 'content': user_message},
                ],
                temperature=0.1,
            )
            break
        except Exception as e:
            err_str = str(e)
            if '429' in err_str and attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) * 2
                print(f'  Rate limit，等待 {wait}s 後重試（第 {attempt + 1} 次）...')
                time.sleep(wait)
            else:
                raise

    content = response.choices[0].message.content or ''
    translations = content.strip().split('\n')

    usage = response.usage
    prompt_tokens = usage.prompt_tokens if usage else 0
    completion_tokens = usage.completion_tokens if usage else 0

    if len(translations) != len(names_zh):
        raise ValueError(
            f'行數不一致：輸入 {len(names_zh)} 行，回傳 {len(translations)} 行'
        )

    return translations, prompt_tokens, completion_tokens


# ---------------------------------------------------------------------------
# 主要流程
# ---------------------------------------------------------------------------

def cmd_status(conn: sqlite3.Connection) -> None:
    """顯示翻譯進度統計。"""
    total = conn.execute('SELECT COUNT(*) FROM factories').fetchone()[0]

    # 各狀態計數
    rows = conn.execute(
        "SELECT translation_status, COUNT(*) as cnt FROM factories GROUP BY translation_status"
    ).fetchall()
    status_map = {r['translation_status']: r['cnt'] for r in rows}

    done = status_map.get('done', 0)
    error = status_map.get('error', 0)
    pending = status_map.get('pending', 0) + status_map.get(None, 0)

    # 確認還有多少 pending 實際含中文
    pending_with_cjk = conn.execute(
        """
        SELECT COUNT(*) FROM factories
        WHERE (translation_status = 'pending' OR translation_status IS NULL)
        """
    ).fetchone()[0]

    print('=== 翻譯進度 ===')
    print(f'總筆數:         {total:,}')
    print(f'已完成 (done):  {done:,} ({done / total * 100:.1f}%)')
    print(f'待翻譯 (pending): {pending_with_cjk:,} ({pending_with_cjk / total * 100:.1f}%)')
    print(f'錯誤 (error):   {error:,} ({error / total * 100:.1f}%)')


def run_translation(
    conn: sqlite3.Connection,
    client,
    records: list[sqlite3.Row],
    label: str = '翻譯'
) -> None:
    """執行翻譯主迴圈。"""
    total = len(records)
    total_prompt_tokens = 0
    total_completion_tokens = 0
    batch_count = 0

    print(f'共 {total:,} 筆需要{label}。批次大小: {BATCH_SIZE}')

    for i in range(0, total, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_ids = [r['id'] for r in batch]
        batch_zh = [r['name_zh'] for r in batch]

        print(f'\n批次 {batch_count + 1}（{i + 1}~{min(i + BATCH_SIZE, total)}/{total}）...', end=' ', flush=True)

        try:
            translations, pt, ct = translate_batch(client, batch_zh)

            updates: list[tuple[str, str, int]] = []
            error_entries: list[dict] = []

            for j, (row, translated) in enumerate(zip(batch, translations)):
                translated = translated.strip()
                if CJK_RE.search(translated):
                    # 翻譯結果仍含中文 → 標為 error
                    updates.append((row['name_en'], 'error', row['id']))
                    error_entries.append({
                        'id': row['id'],
                        'name_zh': row['name_zh'],
                        'name_en_original': row['name_en'],
                        'name_en_translated': translated,
                        'reason': 'translated result contains CJK',
                    })
                else:
                    updates.append((translated, 'done', row['id']))

            update_translations(conn, updates)

            if error_entries:
                append_error_log(error_entries)

            total_prompt_tokens += pt
            total_completion_tokens += ct
            batch_count += 1

            done_in_batch = sum(1 for u in updates if u[1] == 'done')
            err_in_batch = len(error_entries)
            print(f'完成 {done_in_batch} 筆，錯誤 {err_in_batch} 筆。', end='')

            # 每 10 批印出進度和累計成本
            if batch_count % 10 == 0:
                cost = _calc_cost(total_prompt_tokens, total_completion_tokens)
                print(
                    f'\n[進度] 已處理 {min(i + BATCH_SIZE, total):,}/{total:,} 筆'
                    f' | 累計費用: ${cost:.4f} USD'
                )

            time.sleep(SLEEP_BETWEEN_BATCHES)

        except ValueError as e:
            # 行數不一致 → 整批標為 error
            print(f'錯誤（行數不一致）: {e}')
            error_updates = [(r['name_en'], 'error', r['id']) for r in batch]
            update_translations(conn, error_updates)
            error_entries = [
                {
                    'id': r['id'],
                    'name_zh': r['name_zh'],
                    'name_en_original': r['name_en'],
                    'reason': str(e),
                }
                for r in batch
            ]
            append_error_log(error_entries)

        except Exception as e:
            print(f'API 錯誤: {e}')
            raise

    # 最終統計
    cost = _calc_cost(total_prompt_tokens, total_completion_tokens)
    print(f'\n\n=== 翻譯完成 ===')
    print(f'處理批次:       {batch_count}')
    print(f'Prompt tokens:  {total_prompt_tokens:,}')
    print(f'Completion tokens: {total_completion_tokens:,}')
    print(f'總費用:         ${cost:.4f} USD')


def _calc_cost(prompt_tokens: int, completion_tokens: int) -> float:
    """計算 OpenAI API 費用（美元）。"""
    return (
        prompt_tokens / 1000 * PRICE_INPUT_PER_1K
        + completion_tokens / 1000 * PRICE_OUTPUT_PER_1K
    )


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='LLM 批次翻譯台灣工廠名稱')
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        metavar='N',
        help='只翻譯前 N 筆（測試用）',
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='顯示翻譯進度後退出',
    )
    parser.add_argument(
        '--retry-errors',
        action='store_true',
        help='將 error 狀態重設為 pending 並重試',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # 確認 API Key（支援 .env 檔案）
    env_file = BASE_DIR / '.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key.strip(), value)
    # 優先使用 MiniMax，其次 OpenAI
    minimax_key = os.environ.get('MINIMAX_API_KEY')
    openai_key = os.environ.get('OPENAI_API_KEY')

    conn = get_connection()
    ensure_translation_status_column_v2(conn)

    if args.status:
        cmd_status(conn)
        conn.close()
        return

    try:
        from openai import OpenAI
    except ImportError:
        print('錯誤：openai 套件未安裝。')
        print('請先執行：pip install openai>=1.0.0')
        conn.close()
        return

    if minimax_key:
        client = OpenAI(
            api_key=minimax_key,
            base_url='https://api.minimaxi.chat/v1',
        )
        print('使用 MiniMax API（MiniMax-Text-01）')
    elif openai_key:
        client = OpenAI(api_key=openai_key)
        print('使用 OpenAI API（gpt-4o-mini）')
    else:
        print('錯誤：MINIMAX_API_KEY 或 OPENAI_API_KEY 皆未設定。')
        print('請在 .env 中設定其中一個。')
        conn.close()
        return

    if args.retry_errors:
        # 重設 error → pending
        conn.execute("UPDATE factories SET translation_status = 'pending' WHERE translation_status = 'error'")
        conn.commit()
        print('已將所有 error 記錄重設為 pending。')

    records = fetch_pending(conn, limit=args.limit)

    # 過濾出實際含中文的記錄
    records = [r for r in records if CJK_RE.search(r['name_en'] or '')]

    if not records:
        print('沒有需要翻譯的記錄。執行 --status 查看目前進度。')
        conn.close()
        return

    run_translation(conn, client, records)

    # 翻譯完成後重建 FTS5 索引
    rebuild_fts(conn)

    conn.close()


if __name__ == '__main__':
    main()
