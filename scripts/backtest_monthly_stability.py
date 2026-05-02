"""9 パターンの三連複フォーメーションを月別・週別で集計し、
最大 DD と連敗月数を計算するバックテストスクリプト。

マスター指示 2026-05-02:
  - 月次・週次の安定性 (収支ばらつき・最大 DD) を出力
  - 全期間 ROI だけでは判断不可なので月次・週次分解が目的

集計階層:
  pattern × period_type × period_key × segment
    period_type : monthly (YYYY-MM) / weekly (ISO 年週 YYYY-Www)
    segment     : jra / nar / all

派生指標:
  的中率, ROI, 収支, 最大 DD (月次収支累積の最大下落幅),
  連敗期間 (月次収支マイナスの最長連続月数), 赤字月率, σ (月次収支標準偏差)
"""
from __future__ import annotations

import csv
import io
import json
import math
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# 出力を UTF-8 に固定 (Windows コンソール文字化け防止)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent  # keiba-v3/
PRED_DIR = BASE_DIR / "data" / "predictions"
RES_DIR  = BASE_DIR / "data" / "results"
LOG_DIR  = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

MONTHLY_CSV = LOG_DIR / "bt_monthly.csv"
WEEKLY_CSV  = LOG_DIR / "bt_weekly.csv"
STABILITY_LOG = LOG_DIR / "bt_monthly_stability.log"

STAKE = 100  # 1 点あたり賭け金 (円)

# ---------------------------------------------------------------------------
# 印グループ定義 (backtest_5patterns.py と完全一致)
# ---------------------------------------------------------------------------
HONMEI_MARKS       = {"◉", "◎"}
HONMEI_TAIKOU_MARKS = {"◉", "◎", "○", "〇"}
A_2ND   = {"○", "〇", "▲", "☆"}
B_2ND   = {"○", "〇", "▲", "△"}
C_2ND   = {"○", "〇", "▲"}
D_2ND   = {"◉", "◎", "○", "〇", "▲"}
E_2ND   = {"○", "〇"}
F_2ND   = {"◉", "◎", "○", "〇", "▲", "☆"}
G_2ND   = {"◉", "◎", "○", "〇", "▲", "△"}
H_2ND   = {"○", "〇", "▲", "△", "★", "☆"}
ABC_3RD = {"○", "〇", "▲", "△", "★", "☆"}
ALL_MARKS = {"◉", "◎", "○", "〇", "▲", "△", "★", "☆"}
E_3RD   = {"▲", "△", "★", "☆"}

# 印優先度 (上位ほど小さい)
MARK_PRIORITY = {"◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6}

PATTERNS: dict[str, tuple[set, set, set]] = {
    "A": (HONMEI_MARKS,        A_2ND,   ABC_3RD),
    "B": (HONMEI_MARKS,        B_2ND,   ABC_3RD),
    "C": (HONMEI_MARKS,        C_2ND,   ABC_3RD),
    "D": (HONMEI_TAIKOU_MARKS, D_2ND,   ABC_3RD),
    "E": (HONMEI_MARKS,        E_2ND,   E_3RD),
    "F": (HONMEI_TAIKOU_MARKS, F_2ND,   ABC_3RD),
    "G": (HONMEI_TAIKOU_MARKS, G_2ND,   ABC_3RD),
    "H": (HONMEI_MARKS,        H_2ND,   ABC_3RD),
    "I": (ALL_MARKS,           ALL_MARKS, ALL_MARKS),  # BOX
}

# ---------------------------------------------------------------------------
# ユーティリティ関数 (backtest_5patterns.py から引用・完全一致)
# ---------------------------------------------------------------------------

def _filter_active(horses: list) -> list:
    """取消・特殊競争除外の出走馬だけ返す。"""
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _horse_mark(h: dict) -> str:
    return (h.get("mark") or "").strip()


def _horses_by_marks(horses: list, marks: set) -> list[dict]:
    """指定印の出走馬リスト (重複除去・印優先度昇順 + composite 降順)。"""
    cands = [h for h in horses if _horse_mark(h) in marks]
    cands.sort(key=lambda h: (MARK_PRIORITY.get(_horse_mark(h), 99),
                              -(h.get("composite") or 0)))
    seen, out = set(), []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(h)
    return out


def build_tickets(horses: list, m1: set, m2: set, m3: set) -> list[tuple[int, int, int]]:
    """三連複フォーメーション買い目: unordered set (馬番昇順 tuple) を返す。

    - 三連複 (unordered) として計算
    - 1 軸 (m1) + 2 着候補 (m2) + 3 着候補 (m3) の組合せを全列挙
    - {a, b, c} を sorted tuple で重複除外
    """
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen, tickets = set(), []
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
                key = tuple(sorted([a_no, b_no, c_no]))  # unordered set
                if key in seen:
                    continue
                seen.add(key)
                tickets.append(key)
    return tickets


def lookup_sanrenpuku(payouts: dict, combo: tuple[int, int, int]) -> int:
    """三連複 payouts から指定 combo の払戻を取得 (combo は sorted tuple)。"""
    bucket = payouts.get("三連複") or payouts.get("sanrenpuku")
    if bucket is None:
        return 0
    target = "-".join(str(x) for x in combo)
    if isinstance(bucket, dict):
        if str(bucket.get("combo", "")) == target:
            return int(bucket.get("payout", 0) or 0)
    elif isinstance(bucket, list):
        for it in bucket:
            if isinstance(it, dict) and str(it.get("combo", "")) == target:
                return int(it.get("payout", 0) or 0)
    return 0


# ---------------------------------------------------------------------------
# JRA 会場コード判定
# ---------------------------------------------------------------------------
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def _seg_of(rid: str) -> str:
    """race_id から JRA / NAR を判定。5-6 桁目が "01"〜"10" なら JRA。"""
    if len(rid) >= 6 and rid[4:6] in JRA_VENUE_CODES:
        return "jra"
    return "nar"


# ---------------------------------------------------------------------------
# 期間キー変換
# ---------------------------------------------------------------------------

def _monthly_key(date_str: str) -> str:
    """YYYYMMDD → YYYY-MM"""
    return f"{date_str[:4]}-{date_str[4:6]}"


def _weekly_key(date_str: str) -> str:
    """YYYYMMDD → ISO 年週 (YYYY-Www)。
    Python の datetime.isocalendar() を利用。
    """
    import datetime
    d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso[0]}-W{iso[1]:02d}"


# ---------------------------------------------------------------------------
# 統計構造体
# ---------------------------------------------------------------------------

def _new_period_stat():
    """1 期間 × 1 セグメントの統計バケツ。"""
    return {
        "races_played": 0,
        "races_hit":    0,
        "tickets_total": 0,
        "tickets_hit":   0,
        "stake":   0,
        "payback": 0,
        "tickets_dist": Counter(),  # 点数 → 試合数
    }


# ---------------------------------------------------------------------------
# 派生指標計算
# ---------------------------------------------------------------------------

def _calc_derived(stat: dict) -> dict:
    """races_played / stake / payback から派生指標を計算して返す。"""
    rp = stat["races_played"]
    hit_rate = (stat["races_hit"] / rp * 100) if rp > 0 else 0.0
    roi      = (stat["payback"] / stat["stake"] * 100) if stat["stake"] > 0 else 0.0
    balance  = stat["payback"] - stat["stake"]
    dist     = stat["tickets_dist"]
    mode_pts = dist.most_common(1)[0][0] if dist else 0
    return {
        "races_played":  rp,
        "races_hit":     stat["races_hit"],
        "hit_rate":      hit_rate,
        "tickets_total": stat["tickets_total"],
        "stake":         stat["stake"],
        "payback":       stat["payback"],
        "balance":       balance,
        "roi":           roi,
        "mode_pts":      mode_pts,
    }


def _calc_stability(period_balances: list[tuple[str, int]]) -> dict:
    """月次または週次の収支リスト (period_key, balance) から安定性指標を計算。

    最大 DD の計算方法:
      1. 期間キーで昇順ソート
      2. 累積収支を計算
      3. running peak (累積最大値) からの下落幅を毎期間計算
      4. 最大下落幅 = 最大 DD

    連敗期間 (Max Losing Streak):
      収支がマイナスの最長連続期間数
    """
    if not period_balances:
        return {
            "max_dd": 0,
            "max_losing_streak": 0,
            "red_ratio": 0.0,
            "sigma": 0.0,
            "total_periods": 0,
            "red_periods": 0,
        }

    # 昇順ソート
    sorted_periods = sorted(period_balances, key=lambda x: x[0])
    balances = [b for _, b in sorted_periods]

    # 最大 DD 計算
    cumulative = 0
    peak = 0
    max_dd = 0
    for b in balances:
        cumulative += b
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative  # 下落幅 (正の値)
        if dd > max_dd:
            max_dd = dd

    # 連敗期間 (マイナス収支の最長連続)
    max_streak = 0
    cur_streak = 0
    for b in balances:
        if b < 0:
            cur_streak += 1
            if cur_streak > max_streak:
                max_streak = cur_streak
        else:
            cur_streak = 0

    # 赤字月率・標準偏差
    total = len(balances)
    red   = sum(1 for b in balances if b < 0)
    red_ratio = red / total * 100 if total > 0 else 0.0
    sigma = statistics.stdev(balances) if len(balances) >= 2 else 0.0

    return {
        "max_dd":             max_dd,
        "max_losing_streak":  max_streak,
        "red_ratio":          red_ratio,
        "sigma":              sigma,
        "total_periods":      total,
        "red_periods":        red,
    }


# ---------------------------------------------------------------------------
# メイン集計
# ---------------------------------------------------------------------------

def main():
    started = time.time()
    print("集計開始 (全期間)", flush=True)

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [f for f in pred_files if "_prev" not in f.name]
    n_total = len(pred_files)
    print(f"対象 pred ファイル数: {n_total}", flush=True)

    # 集計構造:
    #   monthly_stats[pattern][segment][year_month] = _new_period_stat()
    #   weekly_stats [pattern][segment][year_week]  = _new_period_stat()
    monthly_stats: dict[str, dict[str, dict[str, dict]]] = {
        pat: {seg: defaultdict(_new_period_stat) for seg in ["jra", "nar", "all"]}
        for pat in PATTERNS
    }
    weekly_stats: dict[str, dict[str, dict[str, dict]]] = {
        pat: {seg: defaultdict(_new_period_stat) for seg in ["jra", "nar", "all"]}
        for pat in PATTERNS
    }

    n_processed = 0

    for fi, fp in enumerate(pred_files):
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        res_fp = RES_DIR / f"{date_str}_results.json"
        if not res_fp.exists():
            continue
        try:
            pred    = json.loads(fp.read_text(encoding="utf-8"))
            results = json.loads(res_fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        n_processed += 1

        ym = _monthly_key(date_str)
        yw = _weekly_key(date_str)

        for r in pred.get("races", []):
            rid    = str(r.get("race_id", ""))
            horses = _filter_active(r.get("horses", []))
            if not horses:
                continue
            rdata = results.get(rid)
            if not rdata:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts.get("三連複") and not payouts.get("sanrenpuku"):
                continue

            seg = _seg_of(rid)  # "jra" or "nar"

            for pat_name, (m1, m2, m3) in PATTERNS.items():
                tickets = build_tickets(horses, m1, m2, m3)
                if not tickets:
                    continue

                stake   = len(tickets) * STAKE
                payback = 0
                hit_cnt = 0
                for combo in tickets:
                    pb = lookup_sanrenpuku(payouts, combo)
                    if pb > 0:
                        payback += pb
                        hit_cnt += 1
                race_hit = payback > 0

                # セグメント (jra/nar) + all に集計
                for sg in (seg, "all"):
                    # 月次
                    ms = monthly_stats[pat_name][sg][ym]
                    ms["races_played"]  += 1
                    ms["races_hit"]     += (1 if race_hit else 0)
                    ms["tickets_total"] += len(tickets)
                    ms["tickets_hit"]   += hit_cnt
                    ms["stake"]         += stake
                    ms["payback"]       += payback
                    ms["tickets_dist"][len(tickets)] += 1
                    # 週次
                    ws = weekly_stats[pat_name][sg][yw]
                    ws["races_played"]  += 1
                    ws["races_hit"]     += (1 if race_hit else 0)
                    ws["tickets_total"] += len(tickets)
                    ws["tickets_hit"]   += hit_cnt
                    ws["stake"]         += stake
                    ws["payback"]       += payback
                    ws["tickets_dist"][len(tickets)] += 1

        # 100 ファイルごとに進捗表示
        if (fi + 1) % 100 == 0 or (fi + 1) == n_total:
            el = time.time() - started
            print(f"  {fi+1}/{n_total} ({date_str}) elapsed={el:.1f}s", flush=True)

    print(f"\n集計対象 pred 日数: {n_processed}", flush=True)

    # -------------------------------------------------------------------------
    # CSV 出力
    # -------------------------------------------------------------------------
    _write_monthly_csv(monthly_stats)
    _write_weekly_csv(weekly_stats)

    # -------------------------------------------------------------------------
    # 安定性サマリ表示 (標準出力 + ログファイル)
    # -------------------------------------------------------------------------
    lines = _build_stability_summary(monthly_stats, weekly_stats)
    output_text = "\n".join(lines)

    print(output_text, flush=True)

    # ログファイルに保存 (BOM 付き UTF-8)
    with open(STABILITY_LOG, "w", encoding="utf-8-sig") as f:
        f.write(output_text + "\n")
    print(f"\nログ保存: {STABILITY_LOG}", flush=True)

    elapsed = time.time() - started
    print(f"Total elapsed: {elapsed:.1f}s", flush=True)


# ---------------------------------------------------------------------------
# CSV 書き出し
# ---------------------------------------------------------------------------

def _write_monthly_csv(monthly_stats: dict):
    """月次 CSV を BOM 付き UTF-8 で書き出す。"""
    fieldnames = [
        "pattern", "segment", "year_month",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    for pat in sorted(monthly_stats):
        for seg in ["jra", "nar", "all"]:
            for ym in sorted(monthly_stats[pat][seg]):
                d = _calc_derived(monthly_stats[pat][seg][ym])
                rows.append({
                    "pattern":      pat,
                    "segment":      seg,
                    "year_month":   ym,
                    "races_played": d["races_played"],
                    "races_hit":    d["races_hit"],
                    "hit_rate":     f"{d['hit_rate']:.1f}",
                    "tickets_total": d["tickets_total"],
                    "stake":        d["stake"],
                    "payback":      d["payback"],
                    "balance":      d["balance"],
                    "roi":          f"{d['roi']:.1f}",
                    "mode_pts":     d["mode_pts"],
                })
    with open(MONTHLY_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"月次 CSV 保存: {MONTHLY_CSV} ({len(rows)} 行)", flush=True)


def _write_weekly_csv(weekly_stats: dict):
    """週次 CSV を BOM 付き UTF-8 で書き出す。"""
    fieldnames = [
        "pattern", "segment", "year_week",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    for pat in sorted(weekly_stats):
        for seg in ["jra", "nar", "all"]:
            for yw in sorted(weekly_stats[pat][seg]):
                d = _calc_derived(weekly_stats[pat][seg][yw])
                rows.append({
                    "pattern":      pat,
                    "segment":      seg,
                    "year_week":    yw,
                    "races_played": d["races_played"],
                    "races_hit":    d["races_hit"],
                    "hit_rate":     f"{d['hit_rate']:.1f}",
                    "tickets_total": d["tickets_total"],
                    "stake":        d["stake"],
                    "payback":      d["payback"],
                    "balance":      d["balance"],
                    "roi":          f"{d['roi']:.1f}",
                    "mode_pts":     d["mode_pts"],
                })
    with open(WEEKLY_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"週次 CSV 保存: {WEEKLY_CSV} ({len(rows)} 行)", flush=True)


# ---------------------------------------------------------------------------
# 安定性サマリ表示
# ---------------------------------------------------------------------------

def _build_stability_summary(monthly_stats: dict, weekly_stats: dict) -> list[str]:
    """月次・週次安定性サマリ行リストを構築して返す。"""
    SEG_LABEL = {"jra": "JRA", "nar": "NAR", "all": "全体"}
    lines = []

    for seg in ["all", "jra", "nar"]:
        seg_label = SEG_LABEL[seg]

        # ---- 月次 ----
        lines.append("")
        lines.append(f"{'='*18} {seg_label} 月次安定性 {'='*18}")
        header = (
            f"{'P':<3}  {'全期間ROI':>10}  {'月数':>4}  {'赤字月率':>8}"
            f"  {'最大DD':>12}  {'連敗':>4}  {'σ':>10}  {'中央点':>6}"
        )
        lines.append(header)
        lines.append("─" * 80)

        for pat in sorted(PATTERNS):
            period_data = monthly_stats[pat][seg]
            if not period_data:
                continue

            # 全期間集計 (月次統計を合算)
            total_stake   = sum(s["stake"]   for s in period_data.values())
            total_payback = sum(s["payback"] for s in period_data.values())
            total_roi = (total_payback / total_stake * 100) if total_stake > 0 else 0.0

            # 月次収支リスト (月キー, 収支)
            period_balances = [
                (ym, s["payback"] - s["stake"])
                for ym, s in period_data.items()
            ]
            stab = _calc_stability(period_balances)

            # 代表点数 (全月の mode)
            combined_dist: Counter = Counter()
            for s in period_data.values():
                combined_dist.update(s["tickets_dist"])
            mode_pts = combined_dist.most_common(1)[0][0] if combined_dist else 0

            sigma_k = stab["sigma"] / 1000  # 千円単位
            line = (
                f"{pat:<3}  {total_roi:>9.1f}%  {stab['total_periods']:>4}  "
                f"{stab['red_ratio']:>7.1f}%  "
                f"{-stab['max_dd']:>+12,.0f}  "
                f"{stab['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{mode_pts:>4}点"
            )
            lines.append(line)

        lines.append("─" * 80)

        # ---- 週次 ----
        lines.append("")
        lines.append(f"{'='*18} {seg_label} 週次安定性 {'='*18}")
        header_w = (
            f"{'P':<3}  {'全期間ROI':>10}  {'週数':>4}  {'赤字週率':>8}"
            f"  {'最大DD':>12}  {'連敗':>4}  {'σ':>10}  {'中央点':>6}"
        )
        lines.append(header_w)
        lines.append("─" * 80)

        for pat in sorted(PATTERNS):
            period_data = weekly_stats[pat][seg]
            if not period_data:
                continue

            total_stake   = sum(s["stake"]   for s in period_data.values())
            total_payback = sum(s["payback"] for s in period_data.values())
            total_roi = (total_payback / total_stake * 100) if total_stake > 0 else 0.0

            period_balances = [
                (yw, s["payback"] - s["stake"])
                for yw, s in period_data.items()
            ]
            stab = _calc_stability(period_balances)

            combined_dist: Counter = Counter()
            for s in period_data.values():
                combined_dist.update(s["tickets_dist"])
            mode_pts = combined_dist.most_common(1)[0][0] if combined_dist else 0

            sigma_k = stab["sigma"] / 1000
            line = (
                f"{pat:<3}  {total_roi:>9.1f}%  {stab['total_periods']:>4}  "
                f"{stab['red_ratio']:>7.1f}%  "
                f"{-stab['max_dd']:>+12,.0f}  "
                f"{stab['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{mode_pts:>4}点"
            )
            lines.append(line)

        lines.append("─" * 80)

    return lines


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
