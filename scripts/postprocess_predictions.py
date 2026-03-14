"""
過去予想JSONの軽量ポストプロセス

既存 pred.json を読み込み、以下を反映して上書き保存:
  1. 印の再割り当て（composite 降順、☆/× 含む全再割り当て）
  2. tokusen_score 計算 & is_tokusen フラグ付与

ML推論は行わない。composite / course_record 等の値はそのまま。
推定処理時間: 15-20分（671ファイル）

Usage:
  python scripts/postprocess_predictions.py
  python scripts/postprocess_predictions.py --dry-run   # 変更せず統計のみ
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "predictions")

# ============================================================
# 印の再割り当て（composite順、☆/×含む）
# ============================================================

TEKIPAN_GAP = 3.0
MARK_SEQ = ["◎", "○", "▲", "△", "★"]


def reassign_all_marks(horses):
    """composite 順で印を全再割り当て（☆/×含む）

    formatter.py の assign_marks() と同一ロジックの dict 版。
    """
    if not horses:
        return

    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)

    # 全印クリア
    for h in horses:
        h["mark"] = "－"

    # ◉/◎: composite 1位
    c1 = sorted_h[0].get("composite", 0)
    c2 = sorted_h[1].get("composite", 0) if len(sorted_h) > 1 else 0
    sorted_h[0]["mark"] = "◉" if (c1 - c2) >= TEKIPAN_GAP else "◎"

    # ○▲△★: composite 2-5位
    mark_idx = 0
    for h in sorted_h[1:]:
        if mark_idx >= len(MARK_SEQ):
            break
        h["mark"] = MARK_SEQ[mark_idx]
        mark_idx += 1

    # ☆: ana_type != "none" の未印馬（最大2頭、ana_score降順）
    ana_candidates = [
        h for h in sorted_h
        if h["mark"] == "－" and h.get("ana_type", "none") != "none"
    ]
    ana_candidates.sort(key=lambda h: h.get("ana_score", 0), reverse=True)
    for h in ana_candidates[:2]:
        h["mark"] = "☆"

    # ×: kiken_type != "none" の未印馬（最大1頭、kiken_score降順）
    kiken_candidates = [
        h for h in sorted_h
        if h["mark"] == "－" and h.get("kiken_type", "none") != "none"
    ]
    kiken_candidates.sort(key=lambda h: h.get("kiken_score", 0), reverse=True)
    for h in kiken_candidates[:1]:
        h["mark"] = "×"


# ============================================================
# tokusen_score 計算（Cohen's d ベース重み付き）
# ============================================================

TOKUSEN_ODDS_THRESHOLD = 15.0
TOKUSEN_SCORE_THRESHOLD = 7.0
TOKUSEN_MAX_PER_RACE = 2


def calc_tokusen_scores(horses):
    """dict版 tokusen_score 計算

    jockey_trainer.py の calc_tokusen_score() と同一ロジック。
    """
    field_count = len(horses) or 1

    for h in horses:
        h["tokusen_score"] = 0.0
        h["is_tokusen"] = False

        odds = h.get("odds") or h.get("predicted_tansho_odds")
        if not odds or odds < TOKUSEN_ODDS_THRESHOLD:
            continue

        score = 0.0
        factor_hits = 0

        # 1. course_record (d=1.17) — コース実績偏差値
        cr = h.get("course_record", 0) or 0
        if cr >= 58:
            score += 4.0; factor_hits += 1
        elif cr >= 52:
            score += 2.5; factor_hits += 1
        elif cr >= 45:
            score += 1.5; factor_hits += 1

        # 2. course_total (d=0.85) — コース総合偏差値
        ct = h.get("course_total", 0) or 0
        if ct >= 57:
            score += 3.0; factor_hits += 1
        elif ct >= 52:
            score += 1.5; factor_hits += 1

        # 3. composite (d=0.68) — 総合指数
        comp = h.get("composite", 0) or 0
        if comp >= 55:
            score += 2.5; factor_hits += 1
        elif comp >= 50:
            score += 1.0; factor_hits += 1

        # 4. place3_prob (d=0.54) — 複勝率推定
        base_p3 = 3.0 / field_count
        p3 = h.get("place3_prob", 0) or 0
        if p3 >= base_p3 * 1.8:
            score += 2.0; factor_hits += 1
        elif p3 >= base_p3 * 1.3:
            score += 1.0; factor_hits += 1

        # 5. odds_consistency_adj (d=0.50) — オッズ整合性
        oc = h.get("odds_consistency_adj", 0) or 0
        if oc >= 2.0:
            score += 1.5
        elif oc >= 0.5:
            score += 0.5

        # 6. ability_trend (lift 3.5x) — 近走トレンド
        trend = h.get("ability_trend", "") or ""
        if trend == "急上昇":
            score += 2.0
        elif trend == "上昇":
            score += 1.5

        # 7. pace_last3f_eval (d=0.46) — 上がり3F評価
        last3f = h.get("pace_last3f_eval", 0) or 0
        if last3f >= 55:
            score += 1.0

        # 主要4因子のうち2つ以上必須
        if factor_hits < 2:
            continue

        h["tokusen_score"] = round(score, 2)

    # 上位N頭のみ is_tokusen=True
    candidates = sorted(
        [h for h in horses if h["tokusen_score"] >= TOKUSEN_SCORE_THRESHOLD],
        key=lambda h: h["tokusen_score"], reverse=True,
    )
    for h in candidates[:TOKUSEN_MAX_PER_RACE]:
        h["is_tokusen"] = True


# ============================================================
# メイン処理
# ============================================================

def process_file(filepath, dry_run=False):
    """1ファイルをポストプロセス。変更統計を返す。"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    races = data.get("races", [])
    if isinstance(races, dict):
        races = list(races.values())

    mark_changes = 0
    tokusen_total = 0
    total_horses = 0

    for race in races:
        horses = race.get("horses", [])
        if not horses:
            continue

        total_horses += len(horses)

        # 旧印を記録
        old_marks = {h.get("horse_no"): h.get("mark", "－") for h in horses}

        # 印再割り当て
        reassign_all_marks(horses)

        # tokusen_score 計算
        calc_tokusen_scores(horses)

        # 変更カウント
        for h in horses:
            hno = h.get("horse_no")
            if old_marks.get(hno) != h.get("mark"):
                mark_changes += 1
            if h.get("is_tokusen"):
                tokusen_total += 1

    if not dry_run:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return len(races), total_horses, mark_changes, tokusen_total


def main():
    parser = argparse.ArgumentParser(description="過去予想JSONの軽量ポストプロセス")
    parser.add_argument("--dry-run", action="store_true", help="変更せず統計のみ表示")
    args = parser.parse_args()

    # pred.json ファイル一覧
    files = sorted([
        f for f in os.listdir(PRED_DIR)
        if f.endswith("_pred.json")
    ])

    if not files:
        print("pred.json ファイルが見つかりません")
        return

    print(f"{'='*60}")
    print(f"  ポストプロセス{'（dry-run）' if args.dry_run else ''}")
    print(f"  対象: {len(files)} ファイル")
    print(f"  ディレクトリ: {PRED_DIR}")
    print(f"{'='*60}\n")

    t0 = time.time()
    total_files = 0
    total_races = 0
    total_horses = 0
    total_mark_changes = 0
    total_tokusen = 0

    for i, fname in enumerate(files, 1):
        filepath = os.path.join(PRED_DIR, fname)
        try:
            n_races, n_horses, n_mark_changes, n_tokusen = process_file(
                filepath, dry_run=args.dry_run
            )
            total_files += 1
            total_races += n_races
            total_horses += n_horses
            total_mark_changes += n_mark_changes
            total_tokusen += n_tokusen

            if i % 50 == 0 or i == len(files):
                elapsed = time.time() - t0
                eta = elapsed / i * (len(files) - i)
                print(
                    f"  [{i}/{len(files)}] {fname}  "
                    f"印変更={n_mark_changes}  特選={n_tokusen}  "
                    f"経過={elapsed:.0f}s  残={eta:.0f}s"
                )
        except Exception as e:
            print(f"  [ERROR] {fname}: {e}")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  完了{'（dry-run）' if args.dry_run else ''}")
    print(f"  ファイル: {total_files}")
    print(f"  レース:   {total_races:,}")
    print(f"  馬:       {total_horses:,}")
    print(f"  印変更:   {total_mark_changes:,}")
    print(f"  特選穴馬: {total_tokusen:,}")
    print(f"  処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
