"""
data/ml/*.json から course_db を再構築するスクリプト。
2024-01-01 ～ 2026-03-01 の全ML データを取り込む。

Usage:
    python scripts/rebuild_course_db_from_ml.py
    python scripts/rebuild_course_db_from_ml.py --dry-run
    python scripts/rebuild_course_db_from_ml.py --from 20250101
"""
import sys, os, json, glob, argparse, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.stdout.reconfigure(encoding='utf-8')

from collections import defaultdict
from src.database import set_course_db, get_course_db, init_schema

ML_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'ml')


def build_course_key(venue_code: str, surface: str, distance: int) -> str:
    return f"{venue_code}_{surface}_{distance}"


def process_ml_file(fpath: str) -> dict:
    """1つのMLファイルを読んでコースキー別レコードリストを返す"""
    with open(fpath, encoding='utf-8') as f:
        data = json.load(f)

    result = defaultdict(list)
    races = data.get('races', [])

    for race in races:
        venue_code = race.get('venue_code', '')
        surface    = race.get('surface', '')
        distance   = race.get('distance')
        if not venue_code or not surface or not distance:
            continue

        course_key = build_course_key(venue_code, surface, distance)
        race_date  = race.get('date', '')
        grade      = race.get('grade', '')
        condition  = race.get('condition', '')
        race_name  = race.get('race_name', '')
        field_count= race.get('field_count', 0)
        first_3f   = race.get('first_3f')
        pace       = race.get('pace', '')
        is_jra     = bool(race.get('is_jra', False))

        for horse in race.get('horses', []):
            finish_pos = horse.get('finish_pos')
            status     = horse.get('status', '')
            # 競走中止・除外などはスキップ
            if not isinstance(finish_pos, int) or finish_pos <= 0 or finish_pos > 99:
                continue

            positions_corners = horse.get('positions_corners') or []
            pos4c = None
            if isinstance(positions_corners, list) and len(positions_corners) >= 4:
                pos4c = positions_corners[3]
            elif isinstance(positions_corners, list) and positions_corners:
                pos4c = positions_corners[-1]

            record = {
                'race_date':         race_date,
                'venue':             venue_code,
                'course_id':         course_key,
                'distance':          distance,
                'surface':           surface,
                'condition':         condition,
                'class_name':        race_name,
                'grade':             grade,
                'field_count':       field_count,
                'gate_no':           horse.get('gate_no'),
                'horse_no':          horse.get('horse_no'),
                'jockey':            horse.get('jockey', ''),
                'jockey_id':         horse.get('jockey_id', ''),
                'trainer':           horse.get('trainer', ''),
                'trainer_id':        horse.get('trainer_id', ''),
                'weight_kg':         horse.get('weight_kg'),
                'position_4c':       pos4c,
                'positions_corners': positions_corners,
                'finish_pos':        finish_pos,
                'finish_time_sec':   horse.get('finish_time_sec'),
                'last_3f_sec':       horse.get('last_3f_sec'),
                'margin_behind':     horse.get('margin', None),
                'margin_ahead':      None,
                'first_3f_sec':      first_3f,
                'pace':              pace,
                'is_generation':     False,
                'win_odds':          horse.get('odds'),  # 単勝オッズ
                'is_jra':            is_jra,
            }
            result[course_key].append(record)

    return dict(result)


def main():
    parser = argparse.ArgumentParser(description='ML data から course_db を再構築')
    parser.add_argument('--dry-run', action='store_true', help='DBには書き込まない')
    parser.add_argument('--from',    dest='from_date', default='20240101', help='開始日 YYYYMMDD')
    parser.add_argument('--to',      dest='to_date',   default='20261231', help='終了日 YYYYMMDD')
    parser.add_argument('--batch',   type=int, default=30, help='一括書き込みバッチサイズ（日単位）')
    args = parser.parse_args()

    init_schema()

    pattern = os.path.join(ML_DIR, '2*.json')
    files = sorted(glob.glob(pattern))
    # date filter
    files = [
        f for f in files
        if args.from_date <= os.path.basename(f).replace('.json','') <= args.to_date
    ]
    print(f"対象ファイル: {len(files)} 件  ({args.from_date} ～ {args.to_date})")
    if not files:
        print("対象なし")
        return

    # 既存データを全ロード（増分マージ用）
    print("既存 course_db を読み込み中...", end='', flush=True)
    existing = get_course_db()  # {key: [records]}
    accumulated = defaultdict(list, {k: list(v) for k, v in existing.items()})
    print(f" {len(existing)} コースキー")

    # 既存レコードの race_date+horse_no+finish_pos の重複チェック用セット
    existing_keys: dict = defaultdict(set)
    for key, recs in accumulated.items():
        for r in recs:
            ek = (r.get('race_date',''), r.get('horse_no'), r.get('finish_pos'))
            existing_keys[key].add(ek)

    t0 = time.time()
    total_added = 0
    batch_acc = {}  # {key: [records]}
    batch_files = 0

    for i, fpath in enumerate(files):
        fname = os.path.basename(fpath)
        try:
            day_data = process_ml_file(fpath)
        except Exception as e:
            print(f"  SKIP {fname}: {e}")
            continue

        for key, new_recs in day_data.items():
            added = 0
            for r in new_recs:
                ek = (r.get('race_date',''), r.get('horse_no'), r.get('finish_pos'))
                if ek not in existing_keys[key]:
                    accumulated[key].append(r)
                    existing_keys[key].add(ek)
                    added += 1
                    if key not in batch_acc:
                        batch_acc[key] = accumulated[key]
                    else:
                        batch_acc[key] = accumulated[key]
            total_added += added

        batch_files += 1

        # バッチ書き込み
        if batch_files >= args.batch:
            if not args.dry_run and batch_acc:
                set_course_db(batch_acc)
                batch_acc.clear()
            batch_files = 0

        # 進捗表示
        if (i + 1) % 50 == 0 or i == len(files) - 1:
            elapsed = time.time() - t0
            pct = (i + 1) / len(files) * 100
            print(f"  [{i+1:4d}/{len(files)}] {pct:5.1f}%  追加: {total_added:,}件  経過: {elapsed:.0f}s")

    # 残りをフラッシュ
    if not args.dry_run and batch_acc:
        set_course_db(batch_acc)

    elapsed = time.time() - t0
    total_keys = len(accumulated)
    total_recs = sum(len(v) for v in accumulated.values())
    print(f"\n完了: コースキー {total_keys} 件 / レコード総数 {total_recs:,} 件")
    print(f"追加分: {total_added:,} 件  ({elapsed:.1f}秒)")
    if args.dry_run:
        print("（dry-run: DBは変更なし）")


if __name__ == '__main__':
    main()
