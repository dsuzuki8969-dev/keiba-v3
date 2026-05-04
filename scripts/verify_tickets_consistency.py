"""verify_tickets_consistency.py — M' 戦略の本番 pred.json vs バックテスト再計算 整合性検証。

概要:
    5/4 pred.json の各レース tickets_by_mode.fixed (本番) と、
    regenerate_m_prime_tickets.py ロジック (バックテスト基準) で再計算した買い目を比較し、
    不一致レースを洗い出す。

出力:
    logs/verify_tickets_consistency_20260504.json
"""
from __future__ import annotations

import json
import sys
import io
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# パス設定
# ---------------------------------------------------------------------------
BASE_DIR  = Path(__file__).parent.parent
PRED_DIR  = BASE_DIR / "data" / "predictions"
LOG_DIR   = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

PRED_FILE = PRED_DIR / "20260504_pred.json"
OUT_FILE  = LOG_DIR / "verify_tickets_consistency_20260504.json"

# ---------------------------------------------------------------------------
# 印グループ定数 (regenerate_m_prime_tickets.py / backtest_5patterns.py と完全一致)
# ---------------------------------------------------------------------------
HONMEI_MARKS         = {"◉", "◎"}
HONMEI_TAIKOU_MARKS  = {"◉", "◎", "○", "〇"}
C_2ND                = {"○", "〇", "▲"}
D_2ND                = {"◉", "◎", "○", "〇", "▲"}
ABC_3RD              = {"○", "〇", "▲", "△", "★", "☆"}
E_2ND                = {"○", "〇"}
E_3RD                = {"▲", "△", "★", "☆"}

M_PRIME_STRATEGY: dict[str, str | None] = {
    "SS": "E",
    "S":  "C",
    "A":  "C",
    "B":  "D",
    "C":  "D",
    "D":  "D",
    "E":  None,
}

M_PRIME_PATTERN_MARKS: dict[str, tuple[set, set, set]] = {
    "E": (HONMEI_MARKS,        E_2ND, E_3RD),
    "C": (HONMEI_MARKS,        C_2ND, ABC_3RD),
    "D": (HONMEI_TAIKOU_MARKS, D_2ND, ABC_3RD),
}

MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
}


# ---------------------------------------------------------------------------
# バックテスト基準のチケット再計算 (regenerate_m_prime_tickets.py 流用)
# ---------------------------------------------------------------------------

def _filter_active(horses: list) -> list:
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _horse_mark(h: dict) -> str:
    return (h.get("mark") or "").strip()


def _horses_by_marks(horses: list, marks: set) -> list[dict]:
    cands = [h for h in horses if _horse_mark(h) in marks]
    cands.sort(key=lambda h: (MARK_PRIORITY.get(_horse_mark(h), 99), -(h.get("composite") or 0)))
    seen: set = set()
    out: list = []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(h)
    return out


def build_expected_tickets(horses: list, m1: set, m2: set, m3: set) -> list[tuple[int, int, int]]:
    """三連複フォーメーション買い目 (バックテスト基準)。"""
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen: set = set()
    tickets: list = []
    for ha in h1:
        a_no = ha.get("horse_no")
        for hb in h2:
            b_no = hb.get("horse_no")
            if b_no == a_no:
                continue
            for hc in h3:
                c_no = hc.get("horse_no")
                if c_no == a_no or c_no == b_no:
                    continue
                key = tuple(sorted([a_no, b_no, c_no]))
                if key in seen:
                    continue
                seen.add(key)
                tickets.append(key)
    return tickets


def calc_expected_for_race(race: dict) -> tuple[list[tuple], str | None, bool]:
    """
    レース dict からバックテスト基準の買い目タプルリストを返す。
    Returns: (expected_combos, sub_pattern, skipped)
    """
    confidence: str = (
        race.get("overall_confidence")
        or race.get("confidence")
        or ""
    ).strip()
    sub_pattern = M_PRIME_STRATEGY.get(confidence)

    if sub_pattern is None:
        return [], None, True

    all_horses = race.get("horses", [])
    active     = _filter_active(all_horses)
    m1, m2, m3 = M_PRIME_PATTERN_MARKS[sub_pattern]
    combos = build_expected_tickets(active, m1, m2, m3)
    return combos, sub_pattern, False


# ---------------------------------------------------------------------------
# 本番 pred.json から fixed チケット取得
# ---------------------------------------------------------------------------

def get_actual_tickets(race: dict) -> list[tuple]:
    """tickets_by_mode.fixed から combo を sorted tuple で返す。"""
    tbm   = race.get("tickets_by_mode")
    if not isinstance(tbm, dict):
        return []
    fixed = tbm.get("fixed") or []
    result = []
    for t in fixed:
        combo = t.get("combo")
        if isinstance(combo, list) and len(combo) == 3:
            result.append(tuple(sorted(combo)))
    return result


# ---------------------------------------------------------------------------
# メイン: 検証実行
# ---------------------------------------------------------------------------

def main() -> None:
    if not PRED_FILE.exists():
        print(f"ERROR: {PRED_FILE} が見つかりません。")
        sys.exit(1)

    print(f"[1/3] pred.json を読み込み中: {PRED_FILE}")
    with open(PRED_FILE, encoding="utf-8") as f:
        data = json.load(f)

    races = data.get("races", [])
    total = len(races)
    print(f"  → 総レース数: {total}")

    print("[2/3] 各レースで整合性チェック中...")

    ok_count       = 0
    skip_count     = 0
    mismatch_races = []

    for i, race in enumerate(races):
        race_id = race.get("race_id", f"idx{i}")
        conf    = (race.get("overall_confidence") or race.get("confidence") or "?").strip()

        # バックテスト基準で再計算
        expected_combos, sub_pattern, skipped = calc_expected_for_race(race)
        expected_set = set(expected_combos)

        # 本番 pred.json の固定買い目
        actual_combos = get_actual_tickets(race)
        actual_set    = set(actual_combos)

        # skip レースは一致チェック不要
        tbm   = race.get("tickets_by_mode") or {}
        meta  = tbm.get("_meta", {}) if isinstance(tbm, dict) else {}
        prod_skipped = meta.get("skipped", False)

        if skipped and prod_skipped:
            skip_count += 1
            continue

        # 不一致チェック
        only_expected = expected_set - actual_set   # バックテストにあるが本番にない
        only_actual   = actual_set - expected_set   # 本番にあるがバックテストにない

        # combo=[0,0,0] は不正値として検出
        invalid_actual = [c for c in actual_combos if 0 in c]

        if not only_expected and not only_actual and not invalid_actual:
            ok_count += 1
        else:
            # 不一致レース記録
            entry = {
                "race_id":          race_id,
                "confidence":       conf,
                "sub_pattern":      sub_pattern,
                "expected_count":   len(expected_combos),
                "actual_count":     len(actual_combos),
                "only_expected":    sorted(only_expected),
                "only_actual":      sorted(only_actual),
                "invalid_actual":   invalid_actual,
                "issue_type":       [],
            }
            if invalid_actual:
                entry["issue_type"].append("combo_zero")
            if only_expected:
                entry["issue_type"].append("missing_in_prod")
            if only_actual:
                entry["issue_type"].append("extra_in_prod")

            mismatch_races.append(entry)

    mismatch_count = len(mismatch_races)
    ok_total       = ok_count + skip_count
    print(f"  → 整合性 OK: {ok_count}, skip (両側): {skip_count}, 不一致: {mismatch_count}")

    # ---------------------------------------------------------------------------
    # 不一致サマリ
    # ---------------------------------------------------------------------------
    issue_type_counts: dict[str, int] = {}
    for r in mismatch_races:
        for it in r["issue_type"]:
            issue_type_counts[it] = issue_type_counts.get(it, 0) + 1

    # ---------------------------------------------------------------------------
    # 出力 JSON
    # ---------------------------------------------------------------------------
    result = {
        "generated_at":       datetime.now().isoformat(),
        "pred_file":          str(PRED_FILE),
        "total_races":        total,
        "ok_count":           ok_count,
        "skip_count":         skip_count,
        "mismatch_count":     mismatch_count,
        "issue_type_summary": issue_type_counts,
        "mismatch_races":     mismatch_races,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[3/3] 結果を保存: {OUT_FILE}")
    print()
    print("=" * 60)
    print(f"  検証結果サマリー")
    print("=" * 60)
    print(f"  総レース数       : {total}")
    print(f"  整合性 OK        : {ok_count}")
    print(f"  両側 skip        : {skip_count}")
    print(f"  不一致レース数   : {mismatch_count}")
    print(f"  不一致タイプ内訳 : {issue_type_counts}")
    if mismatch_races:
        print()
        print("  ── 不一致レース詳細 (最大10件) ──")
        for r in mismatch_races[:10]:
            print(
                f"    race_id={r['race_id']} conf={r['confidence']} pat={r['sub_pattern']}"
                f" 期待={r['expected_count']}点 実際={r['actual_count']}点"
                f" 問題={r['issue_type']}"
            )
    print("=" * 60)
    if mismatch_count == 0:
        print("  ✓ 整合性 OK — 本番と バックテストは一致しています。")
    else:
        print("  ✗ 不一致あり — 修正が必要です。")


if __name__ == "__main__":
    main()
