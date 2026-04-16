#!/usr/bin/env python3
"""
確率値修正スクリプト（超高速版）
既存pred.jsonのwin_prob/place2_prob/place3_probに対して
新しい比率保持正規化ロジックを適用する。

スクレイピング不要・DB不要・モデルロード不要。
1日あたり数ミリ秒で完了。

修正内容:
  旧: 独立正規化 → 勝率≒連対率の矛盾が発生
  新: 比率保持正規化 → win < place2 < place3 を保証
"""
import json, os, sys, glob, shutil, time, io, argparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

parser = argparse.ArgumentParser()
parser.add_argument('--start', default='20240101')
parser.add_argument('--end', default='20240412')
parser.add_argument('--dry-run', action='store_true', help='実際にファイルを書き換えない')
args = parser.parse_args()

PRED_DIR = "data/predictions"
START = args.start
END = args.end


def normalize_probs(horses):
    """
    engine.pyの新正規化ロジックを適用（比率保持方式）
    horsesはdict listで、win_prob/place2_prob/place3_probを持つ
    """
    n = len(horses)
    if n == 0:
        return

    place2_target = min(n, 2) / n
    place3_target = min(n, 3) / n

    # Step 1: 比率保存
    ratios_p2 = []
    ratios_p3 = []
    for h in horses:
        wp = h.get('win_prob', 0) or 0
        p2 = h.get('place2_prob', 0) or 0
        p3 = h.get('place3_prob', 0) or 0
        if wp > 0.001:
            ratios_p2.append(p2 / wp)
            ratios_p3.append(p3 / wp)
        else:
            ratios_p2.append(2.0)
            ratios_p3.append(3.0)

    # Step 2: win正規化
    total_win = sum(h.get('win_prob', 0) or 0 for h in horses)
    if total_win > 0:
        for h in horses:
            h['win_prob'] = min(1.0, (h.get('win_prob', 0) or 0) / total_win)

    # Step 3: 比率ベースで算出
    for i, h in enumerate(horses):
        h['place2_prob'] = h['win_prob'] * ratios_p2[i]
        h['place3_prob'] = h['win_prob'] * ratios_p3[i]

    # Step 4: 合計調整
    p2_sum = sum(h['place2_prob'] for h in horses)
    p3_sum = sum(h['place3_prob'] for h in horses)
    p2_target = place2_target * n
    p3_target = place3_target * n
    if p2_sum > 0:
        adj2 = p2_target / p2_sum
        for h in horses:
            h['place2_prob'] = min(1.0, h['place2_prob'] * adj2)
    if p3_sum > 0:
        adj3 = p3_target / p3_sum
        for h in horses:
            h['place3_prob'] = min(1.0, h['place3_prob'] * adj3)

    # Step 5: 反復制約
    for _iter in range(5):
        needs = False
        for h in horses:
            min_p2 = h['win_prob'] * 1.3
            if h['place2_prob'] < min_p2:
                h['place2_prob'] = min_p2
                needs = True
            min_p3 = max(h['place2_prob'] * 1.1, h['win_prob'] * 1.5)
            if h['place3_prob'] < min_p3:
                h['place3_prob'] = min_p3
                needs = True
        if not needs:
            break
        if n >= 2:
            p2_sum2 = sum(h['place2_prob'] for h in horses)
            p3_sum2 = sum(h['place3_prob'] for h in horses)
            if p2_sum2 > p2_target and p2_sum2 > 0:
                adj2b = p2_target / p2_sum2
                for h in horses:
                    new_p2 = h['place2_prob'] * adj2b
                    h['place2_prob'] = max(new_p2, h['win_prob'] * 1.2)
            if p3_sum2 > p3_target and p3_sum2 > 0:
                adj3b = p3_target / p3_sum2
                for h in horses:
                    new_p3 = h['place3_prob'] * adj3b
                    h['place3_prob'] = max(new_p3, h['place2_prob'])


def check_violations(horses):
    """win >= place2 の矛盾件数"""
    count = 0
    for h in horses:
        wp = h.get('win_prob', 0) or 0
        p2 = h.get('place2_prob', 0) or 0
        if wp > 0.001 and p2 <= wp * 1.01:
            count += 1
    return count


def process_file(filepath, dry_run=False):
    """1ファイルを処理"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return 0, 0, 0, 0

    races = data.get('races', [])
    if not races:
        return 0, 0, 0, 0

    total_horses = 0
    violations_before = 0
    violations_after = 0

    for race in races:
        horses = race.get('horses', [])
        total_horses += len(horses)
        violations_before += check_violations(horses)
        normalize_probs(horses)
        violations_after += check_violations(horses)

    if not dry_run:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return len(races), total_horses, violations_before, violations_after


# === メイン処理 ===
print(f"{'='*70}")
print(f"  Probability Fix  {START} -> {END}")
print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE (writing files)'}")
print(f"{'='*70}")

t0 = time.time()

# 対象ファイル
files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
target_files = []
for f in files:
    dt = os.path.basename(f)[:8]
    if START <= dt <= END:
        target_files.append((dt, f))

print(f"  Target files: {len(target_files)}")

# バックアップ確認
backed_up = 0
for dt, fp in target_files:
    prev = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
    if not os.path.exists(prev):
        shutil.copy2(fp, prev)
        backed_up += 1
if backed_up > 0:
    print(f"  Backed up: {backed_up} files")

# 処理
total_races = 0
total_horses = 0
total_vb = 0  # violations before
total_va = 0  # violations after
processed = 0

for i, (dt, fp) in enumerate(target_files):
    nr, nh, vb, va = process_file(fp, dry_run=args.dry_run)
    total_races += nr
    total_horses += nh
    total_vb += vb
    total_va += va
    processed += 1

    # 10日ごとにプログレス表示
    if (i + 1) % 10 == 0 or i == len(target_files) - 1:
        pct = (i + 1) / len(target_files) * 100
        print(f"  [{pct:>5.1f}%] {i+1}/{len(target_files)} done  ({dt})", flush=True)

elapsed = time.time() - t0

print(f"\n{'='*70}")
print(f"  Result")
print(f"{'='*70}")
print(f"  Files:   {processed}")
print(f"  Races:   {total_races}")
print(f"  Horses:  {total_horses}")
print(f"  Violations (win>=place2):")
print(f"    Before: {total_vb}")
print(f"    After:  {total_va}")
print(f"    Fixed:  {total_vb - total_va}")
print(f"  Time:    {elapsed:.1f}s")
print(f"  Speed:   {processed/elapsed:.0f} files/sec" if elapsed > 0 else "")
