#!/usr/bin/env python3
"""
×印修正スクリプト（高速版）
既存pred.jsonの×印を新v2ロジックで再判定する。

スクレイピング不要・DB不要・モデルロード不要。
pred.jsonの既存データ（win_prob, composite, popularity, odds, tokusen_kiken_score）を使い、
新しい必須条件ゲートで再フィルタリングするだけ。

変更内容:
  旧: OR条件 + 1番人気含む → 複勝率33.2%
  新: win_prob絶対値ベース + AND条件 + 1番人気除外 → 複勝率19%前後
"""
import json, os, sys, glob, time, io, argparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

parser = argparse.ArgumentParser()
parser.add_argument('--start', default='20240101')
parser.add_argument('--end', default='20260413')
parser.add_argument('--dry-run', action='store_true', help='実際にファイルを書き換えない')
args = parser.parse_args()

PRED_DIR = "data/predictions"
START = args.start
END = args.end

# v2パラメータ（config/settings.pyと同一値）
POP_MIN_JRA = 2
POP_LIMIT = 3
ODDS_LIMIT = 15.0
WP_RATIO = 0.30
EXPECTED_WP = {1: 0.234, 2: 0.148, 3: 0.121}
COMP_PCT = 0.25
SCORE_THRESHOLD = 3.0
MAX_PER_RACE = 2


def should_be_kiken_v2(h, all_horses):
    """新v2ロジックで×候補かどうか判定"""
    pop = h.get("popularity")
    odds = h.get("odds")
    if pop is None or pop < POP_MIN_JRA or pop > POP_LIMIT:
        return False
    if odds is None or odds >= ODDS_LIMIT:
        return False

    # win_prob絶対値チェック
    wp = h.get("win_prob", 0) or 0
    expected = EXPECTED_WP.get(pop, 0.10)
    if wp >= expected * WP_RATIO:
        return False

    # composite下位チェック
    n = len(all_horses)
    comp_threshold = max(3, int(n * COMP_PCT))
    sorted_by_comp = sorted(all_horses, key=lambda x: x.get("composite", 0) or 0, reverse=True)
    rank_comp = next(
        (i + 1 for i, x in enumerate(sorted_by_comp) if x.get("horse_no") == h.get("horse_no")),
        n,
    )
    if rank_comp < comp_threshold:
        return False

    # 追加スコアチェック
    score = h.get("tokusen_kiken_score", 0) or 0
    if score < SCORE_THRESHOLD:
        return False

    return True


def process_file(filepath, dry_run=False):
    """1ファイルの×印を再判定"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return 0, 0, 0, 0, False

    races = data.get('races', [])
    if not races:
        return 0, 0, 0, 0, False

    old_x_count = 0
    new_x_count = 0
    total_races = len(races)
    changed = False

    for race in races:
        horses = race.get('horses', [])
        n = len(horses)
        if n < 4:
            continue

        # 旧×をカウント
        old_x = [h for h in horses if h.get("mark") == "\u00d7"]
        old_x_count += len(old_x)

        # 全馬の×印をリセット
        for h in horses:
            if h.get("mark") == "\u00d7":
                h["mark"] = "\uff0d"  # × → （無印）に戻す
                h["is_tokusen_kiken"] = False

        # 新v2ロジックで×候補を選出
        candidates = []
        for h in horses:
            if should_be_kiken_v2(h, horses):
                candidates.append(h)

        # スコア降順でMAX_PER_RACE頭
        candidates.sort(key=lambda x: x.get("tokusen_kiken_score", 0) or 0, reverse=True)
        for h in candidates[:MAX_PER_RACE]:
            # 既に他の印（◎○▲△★☆）が付いている馬には×を付けない
            current_mark = h.get("mark", "\uff0d")
            protected = {"\u25c9", "\u25ce", "\u25cb", "\u25b2", "\u25b3", "\u2605", "\u2606"}
            if current_mark in protected:
                continue
            h["mark"] = "\u00d7"
            h["is_tokusen_kiken"] = True
            new_x_count += 1

        # 変更があったかチェック
        new_x_set = {h.get("horse_no") for h in horses if h.get("mark") == "\u00d7"}
        old_x_set = {h.get("horse_no") for h in old_x}
        if new_x_set != old_x_set:
            changed = True

    if not dry_run and changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return total_races, old_x_count, new_x_count, len(races), changed


# === メイン処理 ===
print(f"{'='*70}")
print(f"  x印修正（v2ロジック適用）  {START} -> {END}")
print(f"  モード: {'DRY RUN' if args.dry_run else 'LIVE（ファイル書き換え）'}")
print(f"  条件: 2-3番人気, wp<期待値*{WP_RATIO}, comp下位{int(COMP_PCT*100)}%, スコア>={SCORE_THRESHOLD}")
print(f"{'='*70}")

t0 = time.time()

# 対象ファイル
files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
target_files = []
for f in files:
    bn = os.path.basename(f)
    dt = bn[:8]
    if START <= dt <= END and '_prev' not in bn and '_backup' not in bn:
        target_files.append((dt, f))

print(f"  対象ファイル: {len(target_files)}")

# 処理
total_old_x = 0
total_new_x = 0
total_races = 0
files_changed = 0
processed = 0

for i, (dt, fp) in enumerate(target_files):
    nr, old_x, new_x, races, changed = process_file(fp, dry_run=args.dry_run)
    total_races += races
    total_old_x += old_x
    total_new_x += new_x
    if changed:
        files_changed += 1
    processed += 1

    if (i + 1) % 50 == 0 or i == len(target_files) - 1:
        pct = (i + 1) / len(target_files) * 100
        elapsed = time.time() - t0
        if processed > 0:
            eta = elapsed / processed * (len(target_files) - processed)
            eta_str = f"ETA={eta:.0f}s"
        else:
            eta_str = "..."
        print(f"  [{pct:>5.1f}%] {i+1}/{len(target_files)} done  x: {total_old_x}->{total_new_x}  {elapsed:.0f}s {eta_str}", flush=True)

elapsed = time.time() - t0

print(f"\n{'='*70}")
print(f"  結果")
print(f"{'='*70}")
print(f"  ファイル数:     {processed}")
print(f"  レース数:       {total_races}")
print(f"  変更ファイル:   {files_changed}")
print(f"  x印数:")
print(f"    旧: {total_old_x}")
print(f"    新: {total_new_x}")
print(f"    削減: {total_old_x - total_new_x} ({(1 - total_new_x/max(1,total_old_x))*100:.1f}%減)")
print(f"  処理時間: {elapsed:.1f}s")
