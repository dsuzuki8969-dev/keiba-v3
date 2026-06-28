"""
表示率較正プレビュー v2 (本番非改変・読み取り専用)

目的:
  「第三の道」(偏差値別実率アンカー + 実力印固定 + 当日オッズは乖離シグナル) の
  Before/After を当日 pred.json に対してプレビューする。

変更の方針:
  - 表示率(勝率/連対率/複勝率)を「composite 別実率」にアンカーし、
    オッズ変動ではなく実力で確定させる。
  - 印(◎○▲△★)も実力(composite)順で前日確定し、当日オッズで動かさない。
  - 当日オッズは「実力 vs 市場人気の乖離(妙味/危険)」シグナルとして別表示。

注意:
  - pred.json への書き戻しは一切しない（読み取り専用）
  - 本番ファイル(config/settings.py, src/ 配下)を変更しない
  - git add / git commit は絶対にしない
"""

import sys
import io

# Windows cp932 環境での日本語 print 即死を防ぐ (必須)
sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace'
)

import json
import math
from pathlib import Path

# ================== パス設定 ==================
PROJECT_ROOT = Path(__file__).parent.parent
PRED_FILE = PROJECT_ROOT / "data" / "predictions" / "20260628_pred.json"
CALIB_COMPOSITE_FILE = PROJECT_ROOT / "data" / "_diag" / "calibration_composite.json"
CALIB_RATES_FILE = PROJECT_ROOT / "data" / "_diag" / "calibration_rates.json"

# ================== composite bin 定義 ==================
BINS = [
    (0,   29.9, "<30"),
    (30,  34.9, "30-34"),
    (35,  39.9, "35-39"),
    (40,  44.9, "40-44"),
    (45,  49.9, "45-49"),
    (50,  54.9, "50-54"),
    (55,  59.9, "55-59"),
    (60,  64.9, "60-64"),
    (65,  69.9, "65-69"),
    (70,  74.9, "70-74"),
    (75,  999,  "75+"),
]


def composite_to_bin(c: float) -> str | None:
    """composite 値を bin ラベルに変換。"""
    if c is None:
        return None
    for lo, hi, label in BINS:
        if lo <= c <= hi:
            return label
    return None


# ================== calibration_rates.json のオッズ bin ==================

def parse_odds_bin_ranges(bin_keys: list[str]) -> list[tuple[float, float, str]]:
    """オッズ bin 名を (下限, 上限, ラベル) リストに変換。"""
    result = []
    for key in bin_keys:
        k = key.strip()
        if k.startswith('<'):
            upper = float(k[1:])
            result.append((0.0, upper, key))
        elif k.endswith('+'):
            lower = float(k[:-1])
            result.append((lower, math.inf, key))
        elif '-' in k:
            parts = k.split('-', 1)
            result.append((float(parts[0]), float(parts[1]), key))
        else:
            v = float(k)
            result.append((v, v, key))
    return sorted(result, key=lambda x: x[0])


def find_odds_bin(odds_val: float, bin_ranges: list[tuple]) -> str | None:
    """オッズ値に対応する bin 名を返す。"""
    for lo, hi, name in bin_ranges:
        if lo <= odds_val <= hi:
            return name
    # 全 bin を超えた場合は最大 bin（高オッズのはみ出し）
    if bin_ranges:
        return bin_ranges[-1][2]
    return None


# ================== 印マーク定義 (実力=composite 順) ==================
ABILITY_MARKS = ["◎", "○", "▲", "△", "★"]


# ================== 乖離シグナル ==================

def divergence_signal(ability_rank: int, popularity: int | None) -> str:
    """
    ability_rank: composite 降順の順位 (1 = 最高実力)
    popularity:   market 人気順位 (1 = 1番人気)
    """
    if popularity is None:
        return "-"
    # 順位は 1 が最上位 (数字が小さいほど上位)。
    diff = ability_rank - popularity
    if diff <= -3:
        return "妙味"  # 実力順位が人気順位より上位 = 実力高いのに人気薄
    if diff >= 3:
        return "危険"  # 人気順位が実力順位より上位 = 人気だが実力低い
    return "-"


# ================== After 計算 ==================

def _bin_center(bin_label: str) -> float | None:
    """bin 名 → 中心 composite 値 (線形補間用)"""
    if bin_label.startswith("<"):
        return float(bin_label[1:]) - 3.0   # "<30" → 27
    if bin_label.endswith("+"):
        return float(bin_label[:-1]) + 2.0  # "75+" → 77
    if "-" in bin_label:
        lo, hi = bin_label.split("-")
        return (float(lo) + float(hi)) / 2.0
    return None


def _interp_composite_rate(comp: float, org: str, calib_composite: dict):
    """composite を隣接 bin 中心間で線形補間し (win, p2, p3) を 0〜1 で返す。
    bin 代表値の階段でなく連続値にすることで同偏差値帯の同値を解消する。
    org → ALL フォールバック。該当なしは None。"""
    for try_org in (org, "ALL"):
        table = calib_composite.get(try_org, {})
        pts = []
        for bin_label, row in table.items():
            if not row or row.get("win") is None:
                continue
            c = _bin_center(bin_label)
            if c is None:
                continue
            pts.append((c, row["win"] / 100.0, row["place2"] / 100.0, row["place3"] / 100.0))
        if not pts:
            continue
        pts.sort(key=lambda x: x[0])
        if comp <= pts[0][0]:
            return pts[0][1], pts[0][2], pts[0][3]
        if comp >= pts[-1][0]:
            return pts[-1][1], pts[-1][2], pts[-1][3]
        for i in range(len(pts) - 1):
            c0, w0, p20, p30 = pts[i]
            c1, w1, p21, p31 = pts[i + 1]
            if c0 <= comp <= c1:
                t = (comp - c0) / (c1 - c0) if c1 > c0 else 0.0
                return (w0 + t * (w1 - w0), p20 + t * (p21 - p20), p30 + t * (p31 - p30))
    return None


def calc_after(
    race: dict,
    calib_composite: dict,
    calib_rates: dict,
    odds_bin_cache: dict,
    k_odds: float = 0.10,
    gamma: float = 1.0,
) -> list[dict]:
    """
    1 レースの非取消馬に対し After の win/place2/place3 と印を計算。

    calib_composite: {org: {bin: {win, place2, place3}}}   (% 単位)
    calib_rates:     {org: {odds: {bin: {win, place2, place3}}}} (% 単位)
    k_odds:          オッズ微調整の強度 (デフォルト 0.10)

    返り値: [{horse_no, mark_after, win_after, p2_after, p3_after}, ...]
      ※ horse_no 順でなく composite 降順で返す
    """
    org = "JRA" if race.get("is_jra", False) else "NAR"
    horses = [h for h in race.get("horses", []) if not h.get("is_scratched", False)]
    if not horses:
        return []

    # composite 降順ソート (印付与のため)
    horses_sorted = sorted(
        horses,
        key=lambda h: h.get("composite") or 0.0,
        reverse=True,
    )

    # Step1: composite 実値の線形補間で base 率を取得 (0〜1 の小数)
    #   bin 代表値の階段でなく連続値にすることで同偏差値帯の同値を解消。
    horse_calc = []
    for rank, h in enumerate(horses_sorted, start=1):
        comp = h.get("composite") or 0.0

        interp = _interp_composite_rate(comp, org, calib_composite)
        if interp is not None:
            base_win, base_p2, base_p3 = interp
        else:
            # フォールバック (ALL の中間 bin 50-54)
            all_avg = calib_composite.get("ALL", {})
            fallback = all_avg.get("50-54", {})
            base_win = (fallback.get("win") or 8.9) / 100.0
            base_p2  = (fallback.get("place2") or 19.9) / 100.0
            base_p3  = (fallback.get("place3") or 31.9) / 100.0

        # Step2: オッズによる微調整 (nudge)
        # ※ 運用では当日 6:00 の初回オッズを固定値として使う予定
        #    (当日変動を最小化し、実力ベースを維持するため k_odds を弱めに設定)
        odds_val = h.get("odds")
        market_win = market_p2 = market_p3 = None
        if odds_val and odds_val > 0:
            bin_ranges = odds_bin_cache.get(org) or odds_bin_cache.get("ALL")
            if bin_ranges:
                odds_bin_name = find_odds_bin(odds_val, bin_ranges)
                if odds_bin_name:
                    for try_org in (org, "ALL"):
                        rates_row = (
                            calib_rates.get(try_org, {})
                            .get("odds", {})
                            .get(odds_bin_name)
                        )
                        if rates_row:
                            market_win = rates_row["win"] / 100.0
                            market_p2  = rates_row["place2"] / 100.0
                            market_p3  = rates_row["place3"] / 100.0
                            break

        if market_win is not None:
            win_adj = base_win * (1.0 - k_odds) + market_win * k_odds
            p2_adj  = base_p2  * (1.0 - k_odds) + market_p2  * k_odds
            p3_adj  = base_p3  * (1.0 - k_odds) + market_p3  * k_odds
        else:
            win_adj, p2_adj, p3_adj = base_win, base_p2, base_p3

        horse_calc.append({
            "horse_no":    h.get("horse_no"),
            "ability_rank": rank,
            "raw_win":  max(0.0, win_adj),
            "raw_p2":   max(0.0, p2_adj),
            "raw_p3":   max(0.0, p3_adj),
        })

    # Step3-pre: gamma シャープ化 (メリハリ調整)
    # 較正で「水準」を実態に合わせた後、レース内の相対差を gamma 乗で広げる。
    # floor/cap は使わず直後の Σ 正規化で水準を戻すため、飽和(92%張り付き)は起きない。
    if gamma != 1.0:
        for c in horse_calc:
            c["raw_win"] = c["raw_win"] ** gamma
            c["raw_p2"]  = c["raw_p2"]  ** gamma
            c["raw_p3"]  = c["raw_p3"]  ** gamma

    # Step3: レース内正規化 (win Σ=1.0 / p2 Σ=2.0 / p3 Σ=3.0)
    sum_win = sum(c["raw_win"] for c in horse_calc)
    sum_p2  = sum(c["raw_p2"]  for c in horse_calc)
    sum_p3  = sum(c["raw_p3"]  for c in horse_calc)

    scale_win = 1.0 / sum_win if sum_win > 0 else 1.0
    scale_p2  = 2.0 / sum_p2  if sum_p2  > 0 else 1.0
    scale_p3  = 3.0 / sum_p3  if sum_p3  > 0 else 1.0

    results = []
    for i, c in enumerate(horse_calc):
        w  = min(1.0, max(0.0, c["raw_win"] * scale_win))
        p2 = min(1.0, max(0.0, c["raw_p2"]  * scale_p2))
        p3 = min(1.0, max(0.0, c["raw_p3"]  * scale_p3))

        # Step4: 個馬整合 win ≤ p2 ≤ p3
        w  = min(w, p2, p3)
        p2 = max(p2, w)
        p2 = min(p2, p3)

        # 印 (composite 降順で既にソート済)
        rank = c["ability_rank"]
        mark_after = ABILITY_MARKS[rank - 1] if rank <= len(ABILITY_MARKS) else "-"

        results.append({
            "horse_no":     c["horse_no"],
            "ability_rank": rank,
            "mark_after":   mark_after,
            "win_after":    w,
            "p2_after":     p2,
            "p3_after":     p3,
        })

    return results


# ================== メイン ==================

def main() -> None:
    print("=" * 80)
    print("表示率較正プレビュー v2 (本番非改変・読み取り専用)")
    print("方針: composite 別実率アンカー + 実力印固定 + 乖離シグナル")
    print("=" * 80)

    # ファイル読み込み
    with open(PRED_FILE, encoding="utf-8") as f:
        pred_data = json.load(f)
    with open(CALIB_COMPOSITE_FILE, encoding="utf-8") as f:
        calib_composite = json.load(f)
    with open(CALIB_RATES_FILE, encoding="utf-8") as f:
        calib_rates = json.load(f)

    # オッズ bin キャッシュ構築
    odds_bin_cache: dict[str, list] = {}
    for org_key, org_data in calib_rates.items():
        if org_key == "_venue_names" or not isinstance(org_data, dict):
            continue
        if "odds" not in org_data:
            continue
        odds_bin_cache[org_key] = parse_odds_bin_ranges(list(org_data["odds"].keys()))

    races = pred_data.get("races", [])
    print(f"総レース数: {len(races)}")

    # 各レースの「現状 win_prob 最大値」で上位3レースを選出
    race_max_wins = []
    for race in races:
        if race.get("is_banei"):
            continue
        active = [h for h in race.get("horses", []) if not h.get("is_scratched", False)]
        if not active:
            continue
        max_win = max((h.get("win_prob") or 0) for h in active)
        race_max_wins.append((max_win, race))

    race_max_wins.sort(key=lambda x: x[0], reverse=True)
    top3 = race_max_wins[:3]

    # -------------------- 凡例 --------------------
    print()
    print("【凡例】")
    print("  現状: pred.json に入っている win_prob/place3_prob および印 mark")
    print("  変更後: composite 別実率テーブルにアンカー後 + オッズ微調整 + 正規化")
    print("  乖離: 実力順位 vs 市場人気の差")
    print("    妙味: 実力順位 が 人気順位 より 3 以上 上位 (穴候補)")
    print("    危険: 人気順位 が 実力順位 より 3 以上 上位 (切り候補)")
    print("  馬番 / 馬名 / 偏差値 / 人気 / [現状]印・勝%・複% / [変更後]印・勝%・複% / 乖離")
    print()

    # -------------------- 各レース出力 --------------------
    for idx, (max_win_prob, race) in enumerate(top3, 1):
        venue      = race.get("venue", "?")
        race_no    = race.get("race_no", "?")
        org_str    = "JRA" if race.get("is_jra", False) else "NAR"
        field_count = race.get("field_count", "?")
        race_name  = race.get("race_name", "")

        print(f"{'─' * 80}")
        print(f"[{idx}] {venue} R{race_no}  {race_name}  ({org_str} / {field_count}頭)")
        print(f"      現状 max_win_prob = {max_win_prob*100:.1f}%")
        print(f"{'─' * 80}")

        # After 計算
        after_list = calc_after(race, calib_composite, calib_rates, odds_bin_cache, k_odds=0.0, gamma=2.0)
        after_by_no = {a["horse_no"]: a for a in after_list}

        # 非取消馬
        active = [h for h in race.get("horses", []) if not h.get("is_scratched", False)]

        # 現状 win_prob 降順ソート・上位8頭
        active_sorted = sorted(
            active,
            key=lambda h: h.get("win_prob") or 0,
            reverse=True,
        )
        display = active_sorted[:8]

        # composite 降順の ability_rank を割り当て
        active_by_composite = sorted(
            active,
            key=lambda h: h.get("composite") or 0.0,
            reverse=True,
        )
        ability_rank_map = {
            h.get("horse_no"): i + 1
            for i, h in enumerate(active_by_composite)
        }

        # ヘッダ
        print(
            f"{'馬番':>3} {'馬名':<11} "
            f"{'偏差値':>6} {'人気':>3} "
            f"| {'[現状]印':>5} {'現勝%':>6} {'現複%':>6} "
            f"| {'[変更]印':>5} {'後勝%':>6} {'後複%':>6} "
            f"| {'乖離':^4}"
        )
        print(
            f"{'─'*3} {'─'*11} "
            f"{'─'*6} {'─'*3} "
            f"┼ {'─'*5} {'─'*6} {'─'*6} "
            f"┼ {'─'*5} {'─'*6} {'─'*6} "
            f"┼ {'─'*4}"
        )

        sum_cur_win  = 0.0
        sum_aft_win  = 0.0

        for h in display:
            hno      = h.get("horse_no", "?")
            hname    = (h.get("horse_name") or "")[:10]
            comp     = h.get("composite") or 0.0
            pop      = h.get("popularity")
            cur_mark = h.get("mark") or "-"
            cur_win  = (h.get("win_prob") or 0) * 100
            cur_p3   = (h.get("place3_prob") or 0) * 100

            sum_cur_win += (h.get("win_prob") or 0)

            # After
            after = after_by_no.get(hno)
            if after:
                aft_mark = after["mark_after"]
                aft_win  = after["win_after"] * 100
                aft_p3   = after["p3_after"] * 100
                sum_aft_win += after["win_after"]
                ability_rank = after["ability_rank"]
            else:
                aft_mark, aft_win, aft_p3 = "-", 0.0, 0.0
                ability_rank = ability_rank_map.get(hno, 99)

            # 乖離シグナル
            sig = divergence_signal(ability_rank, pop)

            pop_s = str(pop) if pop is not None else "?"
            print(
                f"{str(hno):>3} {hname:<11} "
                f"{comp:>6.1f} {pop_s:>3} "
                f"| {cur_mark:>5} {cur_win:>5.1f}% {cur_p3:>5.1f}% "
                f"| {aft_mark:>5} {aft_win:>5.1f}% {aft_p3:>5.1f}% "
                f"| {sig:^4}"
            )

        print()
        print(
            f"  Σwin: 現状={sum_cur_win:.3f}  変更後={sum_aft_win:.3f}"
            f" (変更後は1.0のはず・上位8頭のみ表示のため<1.0になる場合あり)"
        )

    # -------------------- 全レース統計 --------------------
    print()
    print("=" * 80)
    print("全レース要約: win_prob 分布の変化")
    print("=" * 80)

    cur_wins_all  = []
    aft_wins_all  = []
    divergence_cnt = {"妙味": 0, "危険": 0, "-": 0}

    for race in races:
        if race.get("is_banei"):
            continue
        active = [h for h in race.get("horses", []) if not h.get("is_scratched", False)]
        if not active:
            continue

        for h in active:
            cur_wins_all.append((h.get("win_prob") or 0) * 100)

        after_list = calc_after(race, calib_composite, calib_rates, odds_bin_cache, k_odds=0.0, gamma=2.0)
        after_by_no = {a["horse_no"]: a for a in after_list}

        # ability_rank は after_list が composite 降順なので rank そのまま
        for a in after_list:
            aft_wins_all.append(a["win_after"] * 100)
            # 乖離カウント
            h_match = next(
                (h for h in active if h.get("horse_no") == a["horse_no"]), None
            )
            pop = h_match.get("popularity") if h_match else None
            sig = divergence_signal(a["ability_rank"], pop)
            divergence_cnt[sig] = divergence_cnt.get(sig, 0) + 1

    def stats_str(vals: list[float]) -> str:
        if not vals:
            return "データなし"
        n   = len(vals)
        avg = sum(vals) / n
        mx  = max(vals)
        s   = sorted(vals)
        med = s[n // 2]
        return f"n={n:,}  平均={avg:.2f}%  中央={med:.2f}%  最大={mx:.2f}%"

    print(f"現状 win_prob:   {stats_str(cur_wins_all)}")
    print(f"変更後 win_after: {stats_str(aft_wins_all)}")
    print()
    print("乖離シグナル集計 (全馬):")
    total_div = sum(divergence_cnt.values())
    for k, v in [("妙味", divergence_cnt.get("妙味", 0)),
                 ("危険", divergence_cnt.get("危険", 0)),
                 ("-",   divergence_cnt.get("-", 0))]:
        pct = v / total_div * 100 if total_div > 0 else 0
        print(f"  {k}: {v}頭  ({pct:.1f}%)")

    print()
    print("=" * 80)
    print("完了 — pred.json は一切変更していません")
    print("=" * 80)


if __name__ == "__main__":
    main()
