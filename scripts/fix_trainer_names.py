"""
race_log の調教師名プレフィックスを修正するスクリプト。

問題:
  1. JRA: trainer_name に '美浦 ' / '栗東 ' プレフィックスが付いている
     例: '美浦 上原佑' → '上原佑', '栗東 中内田充正' → '中内田充正'
  2. NAR: trainer_name が馬体重パターン (e.g. '480(+24)')になっている
     → ML データから正しい調教師名で上書き
  3. NAR: trainer_name に地方名プレフィックスが付いている
     例: '北海道村上正和' → '村上正和', '大井的場直之' → '的場直之'

Usage:
    python scripts/fix_trainer_names.py
    python scripts/fix_trainer_names.py --dry-run
"""
import sys, os, re, json, glob, sqlite3, argparse, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.stdout.reconfigure(encoding='utf-8')

from src.database import get_db, init_schema

ML_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'ml')

# NAR 地方名プレフィックス一覧
NAR_VENUE_PREFIXES = [
    '北海道', '青森', '岩手', '盛岡', '水沢', '宮城', '福島',
    '大井', '川崎', '浦和', '船橋',
    '金沢', '笠松', '名古屋', '園田', '姫路',
    '高知', '佐賀', '荒尾', '帯広', '岩見沢', 'ばんえい',
]
# 最長プレフィックスから試すため長さ降順でソート
NAR_VENUE_PREFIXES.sort(key=len, reverse=True)

_HW_RE = re.compile(r'^\d{3,4}\s*[\(（][+-]?\d+[\)）]$')


def strip_nar_prefix(name: str) -> str:
    """地方名プレフィックスを除去"""
    for pf in NAR_VENUE_PREFIXES:
        if name.startswith(pf) and len(name) > len(pf):
            return name[len(pf):]
    return name


def fix_jra_prefixes(conn: sqlite3.Connection, dry_run: bool):
    """JRA: '美浦 ' / '栗東 ' プレフィックスを除去"""
    print("\n--- JRA 美浦/栗東プレフィックス除去 ---")
    cnt = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE is_jra=1 AND (trainer_name LIKE '美浦 %' OR trainer_name LIKE '栗東 %')"
    ).fetchone()[0]
    print(f"対象: {cnt:,} 件")
    if cnt == 0:
        return

    if not dry_run:
        conn.execute("""
            UPDATE race_log
            SET trainer_name = TRIM(SUBSTR(trainer_name, INSTR(trainer_name, ' ') + 1))
            WHERE is_jra=1 AND (trainer_name LIKE '美浦 %' OR trainer_name LIKE '栗東 %')
        """)
        conn.commit()
        print(f"  → {cnt:,} 件更新完了")
    else:
        print(f"  → (dry-run) {cnt:,} 件 を更新予定")
        rows = conn.execute(
            "SELECT DISTINCT trainer_name FROM race_log WHERE is_jra=1 AND (trainer_name LIKE '美浦 %' OR trainer_name LIKE '栗東 %') LIMIT 5"
        ).fetchall()
        for r in rows:
            stripped = r[0].split(' ', 1)[-1].strip()
            print(f"    {r[0]!r} → {stripped!r}")


def build_ml_trainer_map() -> dict:
    """ML データから (race_id, horse_no) → trainer_name マッピングを構築"""
    print("\n--- ML データから調教師マップを構築中 ---")
    files = sorted(glob.glob(os.path.join(ML_DIR, '2*.json')))
    trainer_map = {}  # (race_id, horse_no) → trainer_name_stripped
    t0 = time.time()
    for i, fp in enumerate(files):
        try:
            with open(fp, encoding='utf-8') as f:
                d = json.load(f)
        except Exception:
            continue
        for race in d.get('races', []):
            if race.get('is_jra', True):
                continue
            race_id = race.get('race_id', '')
            if not race_id:
                continue
            for horse in race.get('horses', []):
                horse_no = horse.get('horse_no')
                raw_name = horse.get('trainer', '') or ''
                clean = strip_nar_prefix(raw_name)
                if clean:
                    trainer_map[(str(race_id), horse_no)] = clean
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(files)}] {len(trainer_map):,} エントリ  ({time.time()-t0:.0f}s)")
    print(f"  マップ構築完了: {len(trainer_map):,} エントリ ({time.time()-t0:.0f}s)")
    return trainer_map


def fix_nar_horse_weight(conn: sqlite3.Connection, trainer_map: dict, dry_run: bool):
    """NAR: 馬体重パターン trainer_name を ML データから正しい名前に更新"""
    print("\n--- NAR 馬体重 trainer_name 修正 ---")
    bad_rows = conn.execute("""
        SELECT race_id, horse_no, trainer_id, trainer_name FROM race_log
        WHERE is_jra=0 AND trainer_name GLOB '[0-9][0-9][0-9]*'
    """).fetchall()
    print(f"対象: {len(bad_rows):,} 件")

    updates = []
    no_match = 0
    for race_id, horse_no, trainer_id, _ in bad_rows:
        key = (str(race_id), horse_no)
        new_name = trainer_map.get(key)
        if new_name:
            updates.append((new_name, race_id, horse_no))
        else:
            no_match += 1

    print(f"  ML マップヒット: {len(updates):,} 件 / マッチなし: {no_match:,} 件")
    if updates and not dry_run:
        conn.executemany(
            "UPDATE race_log SET trainer_name=? WHERE race_id=? AND horse_no=? AND is_jra=0",
            updates
        )
        conn.commit()
        print(f"  → {len(updates):,} 件更新完了")
    elif dry_run:
        print(f"  → (dry-run) {len(updates):,} 件を更新予定")
        if updates:
            print(f"  サンプル: {updates[0]}")


def fix_nar_venue_prefix(conn: sqlite3.Connection, dry_run: bool):
    """NAR: 地方名プレフィックスを除去"""
    print("\n--- NAR 地方名プレフィックス除去 ---")
    # Prefixのパターンを OR で結合
    # SQLiteはREGEXPをサポートしないので、LIKE節を複数組み合わせる
    like_clauses = " OR ".join([f"trainer_name LIKE '{pf}%'" for pf in NAR_VENUE_PREFIXES])
    cnt = conn.execute(
        f"SELECT COUNT(*) FROM race_log WHERE is_jra=0 AND ({like_clauses})"
    ).fetchone()[0]
    print(f"対象: {cnt:,} 件")
    if cnt == 0:
        return

    # Pythonで処理（SQLiteのSUBSTR/INSTRでは複数プレフィックスが難しい）
    rows = conn.execute(
        f"SELECT race_id, horse_no, trainer_name FROM race_log WHERE is_jra=0 AND ({like_clauses})"
    ).fetchall()

    updates = []
    for race_id, horse_no, name in rows:
        stripped = strip_nar_prefix(name)
        if stripped != name:
            updates.append((stripped, race_id, horse_no))

    print(f"  プレフィックスあり: {len(updates):,} 件")
    if updates and not dry_run:
        conn.executemany(
            "UPDATE race_log SET trainer_name=? WHERE race_id=? AND horse_no=? AND is_jra=0",
            updates
        )
        conn.commit()
        print(f"  → {len(updates):,} 件更新完了")
    elif dry_run:
        print(f"  → (dry-run) {len(updates):,} 件を更新予定")
        for old, new in [(r[2], strip_nar_prefix(r[2])) for r in rows[:5]]:
            print(f"    {old!r} → {new!r}")


def main():
    parser = argparse.ArgumentParser(description='race_log 調教師名プレフィックス修正')
    parser.add_argument('--dry-run', action='store_true', help='DBには書き込まない')
    args = parser.parse_args()

    init_schema()
    conn = get_db()

    t0 = time.time()

    # 1. JRA 美浦/栗東プレフィックス除去
    fix_jra_prefixes(conn, args.dry_run)

    # 2. ML データ読み込み
    trainer_map = build_ml_trainer_map()

    # 3. NAR 馬体重trainer_name修正
    fix_nar_horse_weight(conn, trainer_map, args.dry_run)

    # 4. NAR 地方名プレフィックス除去
    fix_nar_venue_prefix(conn, args.dry_run)

    print(f"\n完了 ({time.time()-t0:.1f}秒)")
    if args.dry_run:
        print("（dry-run: DBは変更なし）")

    # 修正後の状況確認
    print("\n--- 修正後の確認 ---")
    jra_pref = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE is_jra=1 AND (trainer_name LIKE '美浦 %' OR trainer_name LIKE '栗東 %')"
    ).fetchone()[0]
    nar_bad = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE is_jra=0 AND trainer_name GLOB '[0-9][0-9][0-9]*'"
    ).fetchone()[0]
    print(f"JRA 美浦/栗東プレフィックス残り: {jra_pref:,}")
    print(f"NAR 馬体重trainer_name残り: {nar_bad:,}")


if __name__ == '__main__':
    main()
