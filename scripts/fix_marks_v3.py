#!/usr/bin/env python3
"""
×印/☆印 v3修正スクリプト（全pred.json一括更新）

スクレイピング不要・DB不要・モデルロード不要。
pred.jsonの既存データ（win_prob, composite, popularity, odds, tokusen_score等）を使い、
v3条件で×と☆を再判定する。

変更内容:
  ×（NAR v3）: 断層5pt+下 + ML wp<3%(1-3人気)/wp<6%(4-6人気) + comp下位30% AND条件
  ×（JRA）: v2を維持（win_prob絶対値ベース + comp下位25%）
  ☆スコア: 断層直下+3pt / ML>>オッズ-3pt / ML≒オッズ+1pt のボーナス/ペナルティ

2026-04-13 v3
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

JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

# ========== ×パラメータ ==========
# JRA v2（変更なし）
JRA_POP_MIN = 2
JRA_POP_LIMIT = 3
JRA_ODDS_LIMIT = 15.0
JRA_WP_RATIO = 0.30
JRA_EXPECTED_WP = {1: 0.234, 2: 0.148, 3: 0.121}
JRA_COMP_PCT = 0.25

# NAR v3
NAR_POP_MIN = 1
NAR_POP_LIMIT = 6
NAR_ODDS_LIMIT = 30.0

SCORE_THRESHOLD = 3.0
MAX_KIKEN_PER_RACE = 2

# ========== ☆パラメータ ==========
TOKUSEN_ODDS_THRESHOLD = 6.0  # 穴馬の最低オッズ
TOKUSEN_MIN_POP = 5            # 5番人気以上は対象外
TOKUSEN_MIN_WP = 0.04          # 最低win_prob
TOKUSEN_SCORE_THRESHOLD = 5.5  # v3: 3.0→5.5に引き上げ（断層ボーナス追加に伴い質を維持）
TOKUSEN_MAX_PER_RACE = 2

PROTECTED_MARKS = {"\u25c9", "\u25ce", "\u25cb", "\u25b2", "\u25b3", "\u2605"}  # ◉◎○▲△★


# ========== 断層ユーティリティ ==========
def find_first_gap(sorted_horses, min_gap=2.5, max_pos=8):
    """composite降順ソート済み馬リストから最初の断層を返す"""
    for i in range(1, min(len(sorted_horses), max_pos)):
        g = (sorted_horses[i-1].get("composite", 0) or 0) - (sorted_horses[i].get("composite", 0) or 0)
        if g >= min_gap:
            return i, g
    return None, 0

def horse_comp_rank(h, sorted_horses):
    hno = h.get("horse_no")
    for i, s in enumerate(sorted_horses):
        if s.get("horse_no") == hno:
            return i + 1
    return 99


# ========== ×判定 ==========
def should_be_kiken_v3(h, all_horses, is_jra, sorted_comp, gap_pos_5):
    """v3ロジックで×候補かどうか判定"""
    pop = h.get("popularity")
    odds = h.get("odds")
    wp = h.get("win_prob", 0) or 0
    n = len(all_horses)

    if is_jra:
        # JRA v2（変更なし）
        if pop is None or pop < JRA_POP_MIN or pop > JRA_POP_LIMIT:
            return False
        if odds is None or odds >= JRA_ODDS_LIMIT:
            return False
        expected = JRA_EXPECTED_WP.get(pop, 0.10)
        if wp >= expected * JRA_WP_RATIO:
            return False
        # comp下位25% = rank >= n*0.75位（v2と同一ロジック）
        comp_threshold = max(3, int(n * JRA_COMP_PCT))
        rank = horse_comp_rank(h, sorted_comp)
        if rank < comp_threshold:
            return False
    else:
        # NAR v3
        if pop is None or pop < NAR_POP_MIN or pop > NAR_POP_LIMIT:
            return False
        if odds is None or odds >= NAR_ODDS_LIMIT:
            return False
        # 人気帯でwp閾値を分離
        if pop <= 3:
            if wp >= 0.03:
                return False
        else:
            if wp >= 0.06:
                return False
        # comp下位30%
        comp_threshold_nar = max(3, int(n * 0.70))
        rank = horse_comp_rank(h, sorted_comp)
        if rank < comp_threshold_nar:
            return False
        # 断層5pt+下チェック
        if gap_pos_5 is not None:
            if rank <= gap_pos_5:
                return False  # 断層上 → ×対象外

    return True


def calc_kiken_score_from_pred(h, all_horses):
    """pred.jsonのデータから追加スコアを再計算"""
    score = 0.0
    # tokusen_kiken_scoreがpred.jsonに保存されている場合はそれを使う
    # ただしv3では必須条件ゲートが変わっただけなので、追加スコアはそのまま使える
    saved = h.get("tokusen_kiken_score", 0) or 0
    if saved > 0:
        return saved
    # 保存されていない場合は最低スコアを返す（必須条件通過済みなので）
    return SCORE_THRESHOLD


# ========== ☆スコア再計算 ==========
def calc_tokusen_score_v3(h, all_horses, sorted_comp, gap_pos, gap_size):
    """v3ロジックで☆スコアを再計算（断層ボーナス/ML乖離ペナルティ追加）"""
    odds = h.get("odds", 0) or 0
    pop = h.get("popularity")
    wp = h.get("win_prob", 0) or 0

    if odds < TOKUSEN_ODDS_THRESHOLD:
        return 0.0
    if pop is not None and pop < TOKUSEN_MIN_POP:
        return 0.0
    if wp < TOKUSEN_MIN_WP:
        return 0.0

    # composite上位5頭は☆対象外
    n = len(all_horses)
    rank = horse_comp_rank(h, sorted_comp)
    if rank <= 5:
        return 0.0

    score = 0.0

    # 1. win_prob（主軸）
    if wp >= 0.08:    score += 3.5
    elif wp >= 0.06:  score += 2.5
    elif wp >= 0.04:  score += 1.5

    # 2. course系スコアはpred.jsonに直接ないのでスキップ
    #    （元のtokusen_scoreに含まれているため、ベーススコアとして使う）
    base_score = h.get("tokusen_score", 0) or 0
    # ベーススコアからwp分を差し引いて残りを加算
    if base_score > 0:
        wp_part = 3.5 if wp >= 0.08 else (2.5 if wp >= 0.06 else (1.5 if wp >= 0.04 else 0))
        extra = max(0, base_score - wp_part)
        score += extra

    # 6. 断層ボーナス/ペナルティ（v3新規）
    if gap_pos is not None:
        h_rank = horse_comp_rank(h, sorted_comp)
        is_above = h_rank <= gap_pos
        if not is_above:
            dist = h_rank - gap_pos - 1
            if dist <= 1:
                # 断層直下（+1～2位）→ ボーナス +3pt
                score += 3.0

    # ML vs オッズ乖離
    if odds > 0 and wp > 0:
        odds_wp = 1.0 / odds * 0.8
        if odds_wp > 0:
            ratio = wp / odds_wp
            if ratio >= 2.0:
                # ML>>オッズ: 市場に見放された馬 → ペナルティ -3pt
                score -= 3.0
            elif 1.0 <= ratio < 1.5:
                # ML≒オッズ: 市場と合致 → 小ボーナス +1pt
                score += 1.0

    return score


# ========== ファイル処理 ==========
def process_file(filepath, dry_run=False):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return {"races": 0, "old_x": 0, "new_x": 0, "old_ana": 0, "new_ana": 0, "changed": False}

    races = data.get('races', [])
    if not races:
        return {"races": 0, "old_x": 0, "new_x": 0, "old_ana": 0, "new_ana": 0, "changed": False}

    stats = {"races": len(races), "old_x": 0, "new_x": 0, "old_ana": 0, "new_ana": 0, "changed": False}

    for race in races:
        horses = race.get('horses', [])
        n = len(horses)
        if n < 4:
            continue

        venue = race.get("venue", "")
        is_jra = venue in JRA_VENUES

        # composite降順ソート
        sorted_comp = sorted(horses, key=lambda x: x.get("composite", 0) or 0, reverse=True)

        # 断層計算
        gap_pos, gap_size = find_first_gap(sorted_comp)
        gap_pos_5, gap_size_5 = find_first_gap(sorted_comp, min_gap=5.0)

        # --- 旧マークのカウントと保存 ---
        old_marks = {h.get("horse_no"): h.get("mark", "\uff0d") for h in horses}
        stats["old_x"] += sum(1 for m in old_marks.values() if m == "\u00d7")
        stats["old_ana"] += sum(1 for m in old_marks.values() if m == "\u2606")

        # --- ×印リセット ---
        for h in horses:
            if h.get("mark") == "\u00d7":
                h["mark"] = "\uff0d"
                h["is_tokusen_kiken"] = False

        # --- ☆印リセット ---
        for h in horses:
            if h.get("mark") == "\u2606":
                h["mark"] = "\uff0d"
                h["is_tokusen"] = False

        # --- ☆再計算（×より先に処理。☆は保護マークに含まれないため×で上書き可能） ---
        ana_candidates = []
        for h in horses:
            s = calc_tokusen_score_v3(h, horses, sorted_comp, gap_pos, gap_size)
            h["tokusen_score"] = s
            if s >= TOKUSEN_SCORE_THRESHOLD:
                current_mark = h.get("mark", "\uff0d")
                if current_mark not in PROTECTED_MARKS:
                    ana_candidates.append(h)

        ana_candidates.sort(key=lambda x: x.get("tokusen_score", 0), reverse=True)
        for h in ana_candidates[:TOKUSEN_MAX_PER_RACE]:
            h["mark"] = "\u2606"
            h["is_tokusen"] = True
            stats["new_ana"] += 1

        # --- ×再計算 ---
        kiken_candidates = []
        for h in horses:
            if should_be_kiken_v3(h, horses, is_jra, sorted_comp, gap_pos_5):
                score = calc_kiken_score_from_pred(h, horses)
                if score >= SCORE_THRESHOLD:
                    kiken_candidates.append((h, score))

        kiken_candidates.sort(key=lambda x: x[1], reverse=True)
        for h, score in kiken_candidates[:MAX_KIKEN_PER_RACE]:
            current_mark = h.get("mark", "\uff0d")
            if current_mark in PROTECTED_MARKS or current_mark == "\u2606":
                continue  # ◉◎○▲△★☆は保護
            h["mark"] = "\u00d7"
            h["is_tokusen_kiken"] = True
            stats["new_x"] += 1

        # 変更チェック
        new_marks = {h.get("horse_no"): h.get("mark", "\uff0d") for h in horses}
        if new_marks != old_marks:
            stats["changed"] = True

    if not dry_run and stats["changed"]:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return stats


# ========== メイン処理 ==========
t0 = time.time()
print(f"{'='*70}")
print(f"  ×/☆印 v3修正  {START} -> {END}")
if args.dry_run:
    print(f"  *** DRY RUN モード ***")
print(f"{'='*70}")

files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
target = [(fp, os.path.basename(fp)[:8]) for fp in files
          if START <= os.path.basename(fp)[:8] <= END
          and '_prev' not in os.path.basename(fp) and '_backup' not in os.path.basename(fp)]

total_stats = {"files": 0, "changed": 0, "old_x": 0, "new_x": 0, "old_ana": 0, "new_ana": 0}

for i, (fp, dt) in enumerate(target):
    st = process_file(fp, dry_run=args.dry_run)
    total_stats["files"] += 1
    if st["changed"]: total_stats["changed"] += 1
    total_stats["old_x"] += st["old_x"]
    total_stats["new_x"] += st["new_x"]
    total_stats["old_ana"] += st["old_ana"]
    total_stats["new_ana"] += st["new_ana"]

    if (i+1) % 100 == 0:
        elapsed = time.time() - t0
        pct = (i+1) / len(target) * 100
        eta = elapsed / (i+1) * (len(target) - i - 1)
        print(f"  [{pct:5.1f}%] {i+1}/{len(target)}  "
              f"×:{total_stats['new_x']}  ☆:{total_stats['new_ana']}  "
              f"変更:{total_stats['changed']}件  "
              f"経過{elapsed:.0f}s  残{eta:.0f}s", flush=True)

elapsed = time.time() - t0
print(f"\n{'='*70}")
print(f"  完了  処理時間: {elapsed:.1f}s")
print(f"{'='*70}")
print(f"  対象ファイル: {total_stats['files']}")
print(f"  変更ファイル: {total_stats['changed']}")
print(f"  × 旧: {total_stats['old_x']}頭 → 新: {total_stats['new_x']}頭")
print(f"  ☆ 旧: {total_stats['old_ana']}頭 → 新: {total_stats['new_ana']}頭")
print(f"{'='*70}")
