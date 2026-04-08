"""
pinyin_translate.py — 用拼音翻譯剩餘中文公司名稱

對 official_name_en IS NULL 的工廠，將 name_en 中殘餘的中文字元
以 pypinyin 轉換為拼音（首字母大寫），並保留已翻譯的英文部分。

用法:
  python3 -m scripts.pinyin_translate
  python3 -m scripts.pinyin_translate --dry-run   # 只印結果，不寫入 DB
  python3 -m scripts.pinyin_translate --limit 100  # 只處理前 N 筆（測試）
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

from pypinyin import Style, pinyin

# ---------------------------------------------------------------------------
# 路徑常數
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / 'data' / 'tmdb.db'

# ---------------------------------------------------------------------------
# 中文字元偵測
# ---------------------------------------------------------------------------
CJK_RE = re.compile(r'[\u4e00-\u9fff]')

# ---------------------------------------------------------------------------
# 廠區後綴翻譯表（長字串優先）
# ---------------------------------------------------------------------------
PLANT_SUFFIXES: dict[str, str] = {
    '第一廠': 'Plant 1',
    '第二廠': 'Plant 2',
    '第三廠': 'Plant 3',
    '第四廠': 'Plant 4',
    '第五廠': 'Plant 5',
    '第六廠': 'Plant 6',
    '竹科廠': 'Hsinchu Science Park Plant',
    '南科廠': 'Southern Science Park Plant',
    '中科廠': 'Central Science Park Plant',
    '桃園廠': 'Taoyuan Plant',
    '台中廠': 'Taichung Plant',
    '臺中廠': 'Taichung Plant',
    '台南廠': 'Tainan Plant',
    '臺南廠': 'Tainan Plant',
    '高雄廠': 'Kaohsiung Plant',
    '新竹廠': 'Hsinchu Plant',
    '龜山廠': 'Guishan Plant',
    '觀音廠': 'Guanyin Plant',
    '楠梓廠': 'Nanzi Plant',
    '湖口廠': 'Hukou Plant',
    '彰化廠': 'Changhua Plant',
    '苗栗廠': 'Miaoli Plant',
    '屏東廠': 'Pingtung Plant',
    '宜蘭廠': 'Yilan Plant',
    '一廠': 'Plant 1',
    '二廠': 'Plant 2',
    '三廠': 'Plant 3',
    '四廠': 'Plant 4',
    '五廠': 'Plant 5',
    '工廠': 'Factory',
    '總廠': 'Main Plant',
    '分廠': 'Branch Plant',
}

# ---------------------------------------------------------------------------
# 常用中文詞翻譯表（在拼音轉換前先替換）
# ---------------------------------------------------------------------------
WORD_MAP: dict[str, str] = {
    '股份有限公司': 'Co., Ltd.',
    '有限公司': 'Ltd.',
    '企業社': 'Enterprise',
    '實業股份有限公司': 'Industrial Co., Ltd.',
    '科技股份有限公司': 'Technology Co., Ltd.',
    '工業股份有限公司': 'Industrial Co., Ltd.',
    '電子股份有限公司': 'Electronics Co., Ltd.',
    '精密股份有限公司': 'Precision Co., Ltd.',
    '機械股份有限公司': 'Machinery Co., Ltd.',
    '實業有限公司': 'Industrial Ltd.',
    '科技有限公司': 'Technology Ltd.',
    '工業有限公司': 'Industrial Ltd.',
    '電子有限公司': 'Electronics Ltd.',
    '精密有限公司': 'Precision Ltd.',
    '機械有限公司': 'Machinery Ltd.',
    '食品': 'Food',
    '電機': 'Electric Machinery',
    '電子': 'Electronics',
    '科技': 'Technology',
    '工業': 'Industrial',
    '精密': 'Precision',
    '機械': 'Machinery',
    '實業': 'Industrial',
    '國際': 'International',
    '化學': 'Chemical',
    '金屬': 'Metal',
    '光學': 'Optical',
    '自動化': 'Automation',
    '系統': 'Systems',
    '能源': 'Energy',
    '電力': 'Power',
    '汽車': 'Auto',
    '企業': 'Enterprise',
}

# 批次大小
BATCH_SIZE = 5000


# ---------------------------------------------------------------------------
# 核心轉換函數
# ---------------------------------------------------------------------------

def apply_plant_suffix(text: str) -> tuple[str, str]:
    """
    從字串尾端比對廠區後綴，找到則回傳 (去除後綴的本體, 英文後綴)。
    依長度降序掃描，確保長字串優先。
    """
    for zh_suffix, en_suffix in sorted(PLANT_SUFFIXES.items(), key=lambda x: -len(x[0])):
        if text.endswith(zh_suffix):
            return text[: -len(zh_suffix)], en_suffix
    return text, ''


def chinese_to_pinyin(text: str) -> str:
    """
    將純中文字串轉為拼音，每個字首字母大寫，以空格分隔。
    非中文字元直接保留。
    """
    result_chars: list[str] = []
    for char in text:
        if CJK_RE.match(char):
            py_list = pinyin(char, style=Style.NORMAL)
            if py_list and py_list[0]:
                result_chars.append(py_list[0][0].capitalize())
        else:
            result_chars.append(char)
    return ''.join(result_chars)


def translate_name(name_en: str) -> str:
    """
    將含中文的 name_en 轉為全英文。

    流程：
    1. 偵測廠區後綴並暫存
    2. 套用 WORD_MAP 替換常用詞
    3. 用 pypinyin 轉換剩餘中文字元
    4. 重組：本體 + 廠區後綴
    5. 清理多餘空白
    """
    if not CJK_RE.search(name_en):
        return name_en  # 已是純英文，跳過

    # Step 1: 廠區後綴
    body, plant_suffix = apply_plant_suffix(name_en)

    # Step 2: 常用詞替換（長字串優先）
    for zh_word, en_word in sorted(WORD_MAP.items(), key=lambda x: -len(x[0])):
        body = body.replace(zh_word, f' {en_word} ')

    # Step 3: 剩餘中文 → 拼音
    # 逐段處理：遇到連續中文字元轉拼音，非中文直接保留
    segments: list[str] = []
    current_chinese = ''
    current_other = ''

    for char in body:
        if CJK_RE.match(char):
            if current_other:
                segments.append(current_other)
                current_other = ''
            current_chinese += char
        else:
            if current_chinese:
                # 轉換中文段落為拼音
                py_words = pinyin(current_chinese, style=Style.NORMAL)
                segments.append(' '.join(p[0].capitalize() for p in py_words if p))
                current_chinese = ''
            current_other += char

    # 處理尾端剩餘
    if current_chinese:
        py_words = pinyin(current_chinese, style=Style.NORMAL)
        segments.append(' '.join(p[0].capitalize() for p in py_words if p))
    if current_other:
        segments.append(current_other)

    result = ''.join(segments)

    # Step 4: 加回廠區後綴
    if plant_suffix:
        result = result.rstrip() + ' ' + plant_suffix

    # Step 5: 清理多餘空白
    result = re.sub(r'\s+', ' ', result).strip()

    return result


# ---------------------------------------------------------------------------
# DB 操作
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_pending(conn: sqlite3.Connection, limit: int | None) -> list[sqlite3.Row]:
    """取得 official_name_en IS NULL 且 name_en 含中文的工廠。"""
    sql = '''
        SELECT id, name_en
        FROM factories
        WHERE official_name_en IS NULL
    '''
    if limit:
        sql += f' LIMIT {limit}'
    return conn.execute(sql).fetchall()


def batch_update(conn: sqlite3.Connection, updates: list[tuple[str, int]]) -> None:
    """批次更新 name_en。"""
    conn.executemany(
        'UPDATE factories SET name_en = ? WHERE id = ?',
        updates,
    )
    conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """重建 FTS5 索引（只操作虛擬表，排除內部 _data/_idx 等）。"""
    print('Rebuilding FTS5 index...')
    # 只取虛擬表（shadowtable 以 fts5 為 module）
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE '%_fts' AND name NOT LIKE '%_fts_%'"
        ).fetchall()
    ]
    if not tables:
        print('  No FTS virtual tables found, skipping.')
        return
    for table in tables:
        print(f'  Rebuilding {table}...')
        conn.execute(f"INSERT INTO {table}({table}) VALUES('rebuild')")
    conn.commit()
    print('  FTS rebuild complete.')


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description='用拼音翻譯剩餘中文公司名稱')
    parser.add_argument('--dry-run', action='store_true', help='只印結果，不寫入 DB')
    parser.add_argument('--limit', type=int, default=None, help='只處理前 N 筆（測試用）')
    args = parser.parse_args()

    conn = get_connection()

    print(f'Fetching records with official_name_en IS NULL...')
    rows = fetch_pending(conn, args.limit)
    print(f'  Found {len(rows):,} records to process.')

    updates: list[tuple[str, int]] = []
    translated_count = 0
    skipped_count = 0

    for i, row in enumerate(rows, 1):
        factory_id: int = row['id']
        name_en: str = row['name_en'] or ''

        if not CJK_RE.search(name_en):
            skipped_count += 1
            continue

        new_name = translate_name(name_en)
        updates.append((new_name, factory_id))
        translated_count += 1

        if args.dry_run and translated_count <= 20:
            print(f'  [{factory_id}] {name_en!r:50s} → {new_name!r}')

        # 每 BATCH_SIZE 筆 commit 一次
        if not args.dry_run and len(updates) >= BATCH_SIZE:
            batch_update(conn, updates)
            updates = []
            pct = i / len(rows) * 100
            print(f'  Progress: {i:,}/{len(rows):,} ({pct:.1f}%) — translated {translated_count:,}')

    # 寫入剩餘資料
    if not args.dry_run and updates:
        batch_update(conn, updates)
        print(f'  Final batch committed ({len(updates):,} records).')

    # 驗證：確認還有多少中文
    if not args.dry_run:
        rebuild_fts(conn)

        remaining_rows = conn.execute(
            'SELECT name_en FROM factories WHERE official_name_en IS NULL LIMIT 500'
        ).fetchall()
        still_chinese = sum(1 for r in remaining_rows if CJK_RE.search(r['name_en'] or ''))
        print(f'\nSpot-check (first 500 rows): {still_chinese} still contain Chinese characters.')

    conn.close()

    print('\n--- Summary ---')
    print(f'  Total fetched   : {len(rows):,}')
    print(f'  Translated      : {translated_count:,}')
    print(f'  Already English : {skipped_count:,}')
    if args.dry_run:
        print('  (dry-run mode, no DB changes made)')


if __name__ == '__main__':
    main()
