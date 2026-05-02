"""D パターン三連複 × 自信度別 × JRA/NAR/全体 × 月次/週次 バックテスト。

マスター指示 2026-05-02:
  D パターン (◉◎○ → ◉◎○▲ → ○▲△★☆ / 10点) を
  自信度 (SS/S/A/B/C/D/E) でフィルタすることで
  弱点 (NAR: ROI 157% / DD -532k 相当) を解消できるか検証する。

集計階層:
  D × confidence (SS/S/A/B/C/D/E/全体) × segment (jra/nar/all)
    × period (monthly / weekly)

出力:
  data/logs/bt_D_confidence_monthly.csv
  data/logs/bt_D_confidence_weekly.csv
  data/logs/bt_D_confidence_stability.log

引用元:
  - backtest_5patterns.py: 印グループ定義・MARK_PRIORITY・build_tickets・lookup_sanrenpuku
  - backtest_monthly_stability.py: _calc_stability / _seg_of / _monthly_key / _weekly_key
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
BASE_DIR  = Path(__file__).parent.parent  # keiba-v3/
PRED_DIR  = BASE_DIR / "data" / "predictions"
RES_DIR   = BASE_DIR / "data" / "results"
LOG_DIR   = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

MONTHLY_CSV    = LOG_DIR / "bt_D_confidence_monthly.csv"
WEEKLY_CSV     = LOG_DIR / "bt_D_confidence_weekly.csv"
STABILITY_LOG  = LOG_DIR / "bt_D_confidence_stability.log"

STAKE = 100  # 1 点あたり賭け金 (円)

# ---------------------------------------------------------------------------
# 印グループ定義 (backtest_5patterns.py と完全一致)
# ---------------------------------------------------------------------------
HONMEI_MARKS        = {"◉", "◎"}                        # 1着候補 (パターン A-C, E)
HONMEI_TAIKOU_MARKS = {"◉", "◎", "○", "〇"}             # 1着候補 (D)
A_2ND   = {"○", "〇", "▲", "☆"}                        # A の 2着
B_2ND   = {"○", "〇", "▲", "△"}                        # B の 2着
C_2ND   = {"○", "〇", "▲"}                              # C の 2着
D_2ND   = {"◉", "◎", "○", "〇", "▲"}                  # D の 2着
E_2ND   = {"○", "〇"}                                   # E の 2着
F_2ND   = {"◉", "◎", "○", "〇", "▲", "☆"}             # F の 2着
G_2ND   = {"◉", "◎", "○", "〇", "▲", "△"}             # G の 2着
H_2ND   = {"○", "〇", "▲", "△", "★", "☆"}             # H の 2着 (= ABC_3RD と同じ)
ABC_3RD = {"○", "〇", "▲", "△", "★", "☆"}             # A/B/C/D/F/G/H の 3着
ALL_MARKS = {"◉", "◎", "○", "〇", "▲", "△", "★", "☆"} # I (BOX) の全枠
E_3RD   = {"▲", "△", "★", "☆"}                        # E の 3着

# D パターン定義 (このスクリプトの対象)
D_M1 = HONMEI_TAIKOU_MARKS   # ◉◎○ (1着軸)
D_M2 = D_2ND                 # ◉◎○▲ (2着候補)
D_M3 = ABC_3RD               # ○▲△★☆ (3着候補)

# 印優先度 (上位ほど小さい)
MARK_PRIORITY = {"◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6}

# 自信度の定義 (E は backtest_5patterns.py L206 の "else → D" 扱いと異なり E として保持)
CONF_VALID = ("SS", "S", "A", "B", "C", "D", "E")
CONF_ALL_KEY = "全体"


# ---------------------------------------------------------------------------
# ユーティリティ (backtest_5patterns.py から完全引用)
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
    - 1 軸 (m1) + 2着候補 (m2) + 3着候補 (m3) の組合せを全列挙
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
    # 三連複 combo は通常昇順、入力 combo も sorted tuple
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
# JRA 会場コード判定 (backtest_monthly_stability.py から完全引用)
# ---------------------------------------------------------------------------
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def _seg_of(rid: str) -> str:
    """race_id から JRA / NAR を判定。5-6 桁目が "01"〜"10" なら JRA。"""
    if len(rid) >= 6 and rid[4:6] in JRA_VENUE_CODES:
        return "jra"
    return "nar"


# ---------------------------------------------------------------------------
# 期間キー変換 (backtest_monthly_stability.py から完全引用)
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

def _new_period_stat() -> dict:
    """1 期間 × 1 セグメント × 1 自信度バケツの統計。"""
    return {
        "races_played":  0,
        "races_hit":     0,
        "tickets_total": 0,
        "tickets_hit":   0,
        "stake":         0,
        "payback":       0,
        "tickets_dist":  Counter(),  # 点数 → レース数
    }


# ---------------------------------------------------------------------------
# 派生指標計算
# ---------------------------------------------------------------------------

def _calc_derived(stat: dict) -> dict:
    """races_played / stake / payback から派生指標を計算して返す。"""
    rp       = stat["races_played"]
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
    (backtest_monthly_stability.py から完全引用)

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
            "max_dd":             0,
            "max_losing_streak":  0,
            "red_ratio":          0.0,
            "sigma":              0.0,
            "total_periods":      0,
            "red_periods":        0,
        }

    # 昇順ソート
    sorted_periods = sorted(period_balances, key=lambda x: x[0])
    balances = [b for _, b in sorted_periods]

    # 最大 DD 計算
    cumulative = 0
    peak       = 0
    max_dd     = 0
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

    # 赤字率・標準偏差
    total     = len(balances)
    red       = sum(1 for b in balances if b < 0)
    red_ratio = red / total * 100 if total > 0 else 0.0
    sigma     = statistics.stdev(balances) if len(balances) >= 2 else 0.0

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
    print("D パターン × 自信度別バックテスト 開始", flush=True)

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [f for f in pred_files if "_prev" not in f.name]
    n_total    = len(pred_files)
    print(f"対象 pred ファイル数: {n_total}", flush=True)

    # -------------------------------------------------------------------------
    # 集計構造
    #   monthly_stats[confidence][segment][year_month] = _new_period_stat()
    #   weekly_stats [confidence][segment][year_week]  = _new_period_stat()
    #
    #   confidence: "SS" / "S" / "A" / "B" / "C" / "D" / "E" / "全体"
    #   segment   : "jra" / "nar" / "all"
    # -------------------------------------------------------------------------
    all_confs = list(CONF_VALID) + [CONF_ALL_KEY]

    monthly_stats: dict[str, dict[str, dict[str, dict]]] = {
        conf: {seg: defaultdict(_new_period_stat) for seg in ["jra", "nar", "all"]}
        for conf in all_confs
    }
    weekly_stats: dict[str, dict[str, dict[str, dict]]] = {
        conf: {seg: defaultdict(_new_period_stat) for seg in ["jra", "nar", "all"]}
        for conf in all_confs
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

            # D パターンの買い目を構築
            tickets = build_tickets(horses, D_M1, D_M2, D_M3)
            if not tickets:
                continue

            # 集計値を計算
            stake   = len(tickets) * STAKE
            payback = 0
            hit_cnt = 0
            for combo in tickets:
                pb = lookup_sanrenpuku(payouts, combo)
                if pb > 0:
                    payback += pb
                    hit_cnt += 1
            race_hit = payback > 0

            # 自信度取得 (backtest_5patterns.py L205-206 と同じロジック)
            raw_conf = (r.get("overall_confidence") or "").replace("⁺", "+").strip()
            if raw_conf not in CONF_VALID:
                raw_conf = "D"  # 不明・空は "D" 扱い

            seg = _seg_of(rid)  # "jra" or "nar"

            # 集計対象: (jra|nar/all) × (conf/全体) の 4 組合せ
            for sg in (seg, "all"):
                for cf in (raw_conf, CONF_ALL_KEY):
                    # 月次
                    ms = monthly_stats[cf][sg][ym]
                    ms["races_played"]  += 1
                    ms["races_hit"]     += (1 if race_hit else 0)
                    ms["tickets_total"] += len(tickets)
                    ms["tickets_hit"]   += hit_cnt
                    ms["stake"]         += stake
                    ms["payback"]       += payback
                    ms["tickets_dist"][len(tickets)] += 1
                    # 週次
                    ws = weekly_stats[cf][sg][yw]
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
    # 安定性サマリ出力 (標準出力 + ログファイル)
    # -------------------------------------------------------------------------
    lines = _build_stability_summary(monthly_stats, weekly_stats)
    output_text = "\n".join(lines)

    print(output_text, flush=True)

    # ログファイル (BOM 付き UTF-8)
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
        "confidence", "segment", "year_month",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    all_confs = list(CONF_VALID) + [CONF_ALL_KEY]
    for conf in all_confs:
        for seg in ["jra", "nar", "all"]:
            for ym in sorted(monthly_stats[conf][seg]):
                d = _calc_derived(monthly_stats[conf][seg][ym])
                rows.append({
                    "confidence":    conf,
                    "segment":       seg,
                    "year_month":    ym,
                    "races_played":  d["races_played"],
                    "races_hit":     d["races_hit"],
                    "hit_rate":      f"{d['hit_rate']:.1f}",
                    "tickets_total": d["tickets_total"],
                    "stake":         d["stake"],
                    "payback":       d["payback"],
                    "balance":       d["balance"],
                    "roi":           f"{d['roi']:.1f}",
                    "mode_pts":      d["mode_pts"],
                })
    with open(MONTHLY_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"月次 CSV 保存: {MONTHLY_CSV} ({len(rows)} 行)", flush=True)


def _write_weekly_csv(weekly_stats: dict):
    """週次 CSV を BOM 付き UTF-8 で書き出す。"""
    fieldnames = [
        "confidence", "segment", "year_week",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    all_confs = list(CONF_VALID) + [CONF_ALL_KEY]
    for conf in all_confs:
        for seg in ["jra", "nar", "all"]:
            for yw in sorted(weekly_stats[conf][seg]):
                d = _calc_derived(weekly_stats[conf][seg][yw])
                rows.append({
                    "confidence":    conf,
                    "segment":       seg,
                    "year_week":     yw,
                    "races_played":  d["races_played"],
                    "races_hit":     d["races_hit"],
                    "hit_rate":      f"{d['hit_rate']:.1f}",
                    "tickets_total": d["tickets_total"],
                    "stake":         d["stake"],
                    "payback":       d["payback"],
                    "balance":       d["balance"],
                    "roi":           f"{d['roi']:.1f}",
                    "mode_pts":      d["mode_pts"],
                })
    with open(WEEKLY_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"週次 CSV 保存: {WEEKLY_CSV} ({len(rows)} 行)", flush=True)


# ---------------------------------------------------------------------------
# 安定性サマリ構築
# ---------------------------------------------------------------------------

def _build_conf_summary_rows(monthly_stats: dict, seg: str) -> list[tuple[str, dict, dict]]:
    """自信度ごとの全期間 ROI + 月次安定性を計算してリストで返す。

    Returns: [(conf_label, derived_all, stability), ...]
    """
    all_confs = list(CONF_VALID) + [CONF_ALL_KEY]
    result = []
    for conf in all_confs:
        period_data = monthly_stats[conf][seg]
        if not period_data:
            continue
        # 全期間の合計 (月次統計を合算)
        total_stake   = sum(s["stake"]   for s in period_data.values())
        total_payback = sum(s["payback"] for s in period_data.values())
        total_races   = sum(s["races_played"] for s in period_data.values())
        total_hit     = sum(s["races_hit"]    for s in period_data.values())
        combined_dist: Counter = Counter()
        for s in period_data.values():
            combined_dist.update(s["tickets_dist"])

        roi      = (total_payback / total_stake * 100) if total_stake > 0 else 0.0
        hit_rate = (total_hit / total_races * 100) if total_races > 0 else 0.0
        mode_pts = combined_dist.most_common(1)[0][0] if combined_dist else 0

        # 月次安定性
        period_balances = [
            (ym, s["payback"] - s["stake"])
            for ym, s in period_data.items()
        ]
        stab = _calc_stability(period_balances)

        derived = {
            "races_played": total_races,
            "roi":          roi,
            "hit_rate":     hit_rate,
            "mode_pts":     mode_pts,
        }
        result.append((conf, derived, stab))
    return result


def _build_skip_simulation(monthly_stats: dict, seg: str) -> list[tuple[str, dict, dict]]:
    """skip 案 5 通りのシミュレーション結果リストを返す。

    skip 案:
      1. なし (全自信度運用)
      2. SS skip
      3. SS + E skip
      4. SS + D + E skip
      5. SS + C + D + E skip (S/A/B のみ運用)

    各案でレース単位の収支を再計算し ROI / 月次安定性を計算。
    """
    SKIP_PLANS: list[tuple[str, set[str]]] = [
        ("全自信度通し (skip なし)",                set()),
        ("SS skip",                                 {"SS"}),
        ("SS+E skip",                               {"SS", "E"}),
        ("SS+D+E skip",                             {"SS", "D", "E"}),
        ("SS+C+D+E skip (S/A/B のみ運用)",          {"SS", "C", "D", "E"}),
    ]

    # confidence → {year_month → [(stake, payback), ...]} の形でデータを保持
    # まず各自信度×月次の (stake, payback) ペアを再構成する
    # monthly_stats[conf][seg][ym] = stat dict を利用
    all_confs = list(CONF_VALID)  # 全体キーは除く

    # {conf: {ym: (stake, payback)}}
    conf_monthly: dict[str, dict[str, tuple[int, int]]] = {}
    for conf in all_confs:
        conf_monthly[conf] = {}
        for ym, stat in monthly_stats[conf][seg].items():
            conf_monthly[conf][ym] = (stat["stake"], stat["payback"])

    results = []
    for plan_label, skip_set in SKIP_PLANS:
        # skip_set に含まれない自信度のデータのみ合算
        active_confs = [c for c in all_confs if c not in skip_set]

        # 月次合算
        ym_totals: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
        total_stake   = 0
        total_payback = 0
        for conf in active_confs:
            for ym, (s, p) in conf_monthly[conf].items():
                prev = ym_totals[ym]
                ym_totals[ym] = (prev[0] + s, prev[1] + p)
                total_stake   += s
                total_payback += p

        roi = (total_payback / total_stake * 100) if total_stake > 0 else 0.0

        # 月次安定性
        period_balances = [(ym, p - s) for ym, (s, p) in ym_totals.items()]
        stab = _calc_stability(period_balances)

        # 総レース数 (各自信度の races_played を合算)
        total_races = sum(
            sum(stat["races_played"] for stat in monthly_stats[conf][seg].values())
            for conf in active_confs
        )

        derived = {
            "races_played": total_races,
            "roi":          roi,
        }
        results.append((plan_label, derived, stab))

    return results


def _build_stability_summary(monthly_stats: dict, weekly_stats: dict) -> list[str]:
    """月次・週次の自信度別安定性サマリを構築して返す。"""
    SEG_LABEL = {"jra": "JRA", "nar": "NAR", "all": "全体"}
    lines = []

    for seg in ["all", "jra", "nar"]:
        seg_label = SEG_LABEL[seg]

        # ================================================================
        # 自信度別 月次安定性テーブル
        # ================================================================
        lines.append("")
        lines.append(f"{'='*18} {seg_label} D × 自信度別 月次安定性 {'='*18}")
        header = (
            f"{'信頼度':<6}  {'R数':>6}  {'ROI':>9}  {'的中率':>7}  "
            f"{'赤字月':>7}  {'最大DD':>12}  {'連敗':>4}  {'σ':>10}  {'中央点':>6}"
        )
        lines.append(header)
        lines.append("─" * 90)

        rows = _build_conf_summary_rows(monthly_stats, seg)
        for conf, derived, stab in rows:
            sigma_k = stab["sigma"] / 1000
            line = (
                f"{conf:<6}  "
                f"{derived['races_played']:>6,}  "
                f"{derived['roi']:>8.1f}%  "
                f"{derived['hit_rate']:>6.1f}%  "
                f"{stab['red_ratio']:>6.1f}%  "
                f"{-stab['max_dd']:>+12,.0f}  "
                f"{stab['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{derived['mode_pts']:>4}点"
            )
            lines.append(line)
        lines.append("─" * 90)

        # ================================================================
        # 自信度別 週次安定性テーブル
        # ================================================================
        lines.append("")
        lines.append(f"{'='*18} {seg_label} D × 自信度別 週次安定性 {'='*18}")
        header_w = (
            f"{'信頼度':<6}  {'R数':>6}  {'ROI':>9}  {'的中率':>7}  "
            f"{'赤字週':>7}  {'最大DD':>12}  {'連敗':>4}  {'σ':>10}  {'中央点':>6}"
        )
        lines.append(header_w)
        lines.append("─" * 90)

        all_confs_keys = list(CONF_VALID) + [CONF_ALL_KEY]
        for conf in all_confs_keys:
            period_data = weekly_stats[conf][seg]
            if not period_data:
                continue
            total_stake   = sum(s["stake"]   for s in period_data.values())
            total_payback = sum(s["payback"] for s in period_data.values())
            total_races   = sum(s["races_played"] for s in period_data.values())
            total_hit     = sum(s["races_hit"]    for s in period_data.values())
            combined_dist: Counter = Counter()
            for s in period_data.values():
                combined_dist.update(s["tickets_dist"])

            roi      = (total_payback / total_stake * 100) if total_stake > 0 else 0.0
            hit_rate = (total_hit / total_races * 100) if total_races > 0 else 0.0
            mode_pts = combined_dist.most_common(1)[0][0] if combined_dist else 0

            period_balances_w = [
                (yw, s["payback"] - s["stake"])
                for yw, s in period_data.items()
            ]
            stab_w = _calc_stability(period_balances_w)
            sigma_k = stab_w["sigma"] / 1000
            line = (
                f"{conf:<6}  "
                f"{total_races:>6,}  "
                f"{roi:>8.1f}%  "
                f"{hit_rate:>6.1f}%  "
                f"{stab_w['red_ratio']:>6.1f}%  "
                f"{-stab_w['max_dd']:>+12,.0f}  "
                f"{stab_w['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{mode_pts:>4}点"
            )
            lines.append(line)
        lines.append("─" * 90)

        # ================================================================
        # skip シミュレーション (月次ベース) --- ★ 実装行: この関数の呼び出し
        # ================================================================
        lines.append("")
        lines.append(f"{'='*18} {seg_label} D × 自信度フィルタ skip シミュレーション {'='*18}")
        header_skip = (
            f"{'skip 案':<40}  {'R数':>6}  {'ROI':>9}  "
            f"{'赤字月':>7}  {'最大DD':>12}"
        )
        lines.append(header_skip)
        lines.append("─" * 90)

        skip_rows = _build_skip_simulation(monthly_stats, seg)  # ★ skip シミュレーション実装
        for plan_label, derived, stab in skip_rows:
            line = (
                f"{plan_label:<40}  "
                f"{derived['races_played']:>6,}  "
                f"{derived['roi']:>8.1f}%  "
                f"{stab['red_ratio']:>6.1f}%  "
                f"{-stab['max_dd']:>+12,.0f}"
            )
            lines.append(line)
        lines.append("─" * 90)

    return lines


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
