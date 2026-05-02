"""自信度別パターン振分戦略 (M / M') のバックテストスクリプト。

マスター提案 2026-05-02:
  自信度に応じて以下のパターンを選択し、一律戦略と比較する。

  戦略 M (マスター案):
    SS → E (◉◎ → ○ → ▲△★☆ / 4点)
    S  → C (◉◎ → ○▲ → ○▲△★☆ / 7点)
    A  → C (7点)
    B  → D (◉◎○ → ◉◎○▲ → ○▲△★☆ / 10点)
    C  → D (10点)
    D  → skip (買わない)
    E  → skip (買わない)

  戦略 M' (M との差分: D 自信度を skip → D パターンに変更):
    SS → E
    S  → C
    A  → C
    B  → D
    C  → D
    D  → D   ★ M との差分
    E  → skip

比較戦略:
  1. M       : マスター案 (上記マッピング)
  2. M'      : D 自信度も D パターンで買う
  3. D一律    : 全レース D パターン (D/E 自信度も買う)
  4. D+skip  : D パターンで D/E 自信度のみ skip
  5. C一律    : 全レース C パターン (D/E 自信度も買う)
  6. E一律    : 全レース E パターン (D/E 自信度も買う)

出力:
  data/logs/bt_master_strategy_monthly.csv  (BOM付きUTF-8)
  data/logs/bt_master_strategy_weekly.csv   (BOM付きUTF-8)
  data/logs/bt_master_strategy.log          (BOM付きUTF-8)
"""
from __future__ import annotations

import csv
import datetime
import io
import json
import math
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# Windows コンソール文字化け防止
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent  # keiba-v3/
PRED_DIR = BASE_DIR / "data" / "predictions"
RES_DIR  = BASE_DIR / "data" / "results"
LOG_DIR  = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

MONTHLY_CSV   = LOG_DIR / "bt_master_strategy_monthly.csv"
WEEKLY_CSV    = LOG_DIR / "bt_master_strategy_weekly.csv"
SUMMARY_LOG   = LOG_DIR / "bt_master_strategy.log"

STAKE = 100  # 1点あたり賭け金 (円)

# ---------------------------------------------------------------------------
# 印グループ定義 (backtest_5patterns.py / backtest_monthly_stability.py と完全一致)
# ---------------------------------------------------------------------------
HONMEI_MARKS        = {"◉", "◎"}
HONMEI_TAIKOU_MARKS = {"◉", "◎", "○", "〇"}
C_2ND   = {"○", "〇", "▲"}
D_2ND   = {"◉", "◎", "○", "〇", "▲"}
E_2ND   = {"○", "〇"}
ABC_3RD = {"○", "〇", "▲", "△", "★", "☆"}
E_3RD   = {"▲", "△", "★", "☆"}

# 印優先度 (上位ほど小さい)
MARK_PRIORITY = {"◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6}

# パターン定義 (m1=1着軸, m2=2着候補, m3=3着候補)
PATTERNS: dict[str, tuple[set, set, set]] = {
    "C": (HONMEI_MARKS,        C_2ND, ABC_3RD),   # 7点
    "D": (HONMEI_TAIKOU_MARKS, D_2ND, ABC_3RD),   # 10点
    "E": (HONMEI_MARKS,        E_2ND, E_3RD),     # 4点
}

# ---------------------------------------------------------------------------
# 戦略 M のマッピング定義 (実装行: 77行目付近)
# ---------------------------------------------------------------------------
STRATEGY_M: dict[str, str | None] = {
    "SS": "E",    # E パターン (4点)
    "S":  "C",    # C パターン (7点)
    "A":  "C",    # C パターン (7点)
    "B":  "D",    # D パターン (10点)
    "C":  "D",    # D パターン (10点)
    "D":  None,   # skip (買わない)
    "E":  None,   # skip (買わない)
}

# ---------------------------------------------------------------------------
# 戦略 M' のマッピング定義 (M との差分: D 自信度を skip → D パターンに変更)
# ---------------------------------------------------------------------------
STRATEGY_M_PRIME: dict[str, str | None] = {
    "SS": "E",    # E パターン (4点)
    "S":  "C",    # C パターン (7点)
    "A":  "C",    # C パターン (7点)
    "B":  "D",    # D パターン (10点)
    "C":  "D",    # D パターン (10点)
    "D":  "D",    # D パターン (10点) ★ M との差分: skip → D
    "E":  None,   # skip (買わない)
}

# 比較戦略定義
# strategy_name → (pattern_name or None, skip_de: bool)
#   pattern_name: 固定パターン名 (一律戦略)
#   None: 戦略 M または M' (マッピング使用)
#   skip_de: True の場合 D/E 自信度の全レースを skip
STRATEGIES: list[tuple[str, str | None, bool]] = [
    ("M",        None, False),  # マスター案 (STRATEGY_M マッピング)
    ("M'",       None, False),  # D 自信度も D パターン (STRATEGY_M_PRIME マッピング)
    ("D一律",     "D",  False),  # 全レース D パターン
    ("D+skip",   "D",  True),   # D パターン + D/E 自信度 skip
    ("C一律",     "C",  False),  # 全レース C パターン
    ("E一律",     "E",  False),  # 全レース E パターン
]

# ---------------------------------------------------------------------------
# JRA 会場コード判定
# ---------------------------------------------------------------------------
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def _seg_of(rid: str) -> str:
    """race_id から JRA / NAR を判定。5-6桁目が "01"〜"10" なら JRA。"""
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
    """YYYYMMDD → ISO 年週 (YYYY-Www)。"""
    d = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso[0]}-W{iso[1]:02d}"


# ---------------------------------------------------------------------------
# ユーティリティ関数 (backtest_5patterns.py と完全一致)
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
    - 1軸 (m1) + 2着候補 (m2) + 3着候補 (m3) の組合せを全列挙
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
                key = tuple(sorted([a_no, b_no, c_no]))  # unordered set (三連複)
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
# 統計構造体
# ---------------------------------------------------------------------------

def _new_stat() -> dict:
    """1期間 × 1セグメントの統計バケツ。"""
    return {
        "races_played":  0,
        "races_hit":     0,
        "tickets_total": 0,
        "tickets_hit":   0,
        "stake":         0,
        "payback":       0,
        "tickets_dist":  Counter(),  # 点数 → R数
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

    最大 DD:
      1. 期間キーで昇順ソート
      2. 累積収支を計算
      3. running peak (累積最大値) からの下落幅を毎期間計算
      4. 最大下落幅 = 最大 DD

    連敗期間 (Max Losing Streak):
      収支がマイナスの最長連続期間数
    """
    if not period_balances:
        return {
            "max_dd":            0,
            "max_losing_streak": 0,
            "red_ratio":         0.0,
            "sigma":             0.0,
            "total_periods":     0,
            "red_periods":       0,
        }

    # 昇順ソート (期間キー文字列でソートすると時系列順になる)
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
    total     = len(balances)
    red       = sum(1 for b in balances if b < 0)
    red_ratio = red / total * 100 if total > 0 else 0.0
    sigma     = statistics.stdev(balances) if len(balances) >= 2 else 0.0

    return {
        "max_dd":            max_dd,
        "max_losing_streak": max_streak,
        "red_ratio":         red_ratio,
        "sigma":             sigma,
        "total_periods":     total,
        "red_periods":       red,
    }


# ---------------------------------------------------------------------------
# メイン集計
# ---------------------------------------------------------------------------

def main():
    started = time.time()
    print("集計開始 (全期間・マスター戦略 M vs 比較戦略)", flush=True)

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [f for f in pred_files if "_prev" not in f.name]
    n_total = len(pred_files)
    print(f"対象 pred ファイル数: {n_total}", flush=True)

    # -----------------------------------------------------------------
    # 集計構造:
    #   monthly_stats[strategy_name][segment][year_month] = _new_stat()
    #   weekly_stats [strategy_name][segment][year_week]  = _new_stat()
    # -----------------------------------------------------------------
    strategy_names = [s[0] for s in STRATEGIES]

    monthly_stats: dict[str, dict[str, dict[str, dict]]] = {
        sname: {seg: defaultdict(_new_stat) for seg in ["jra", "nar", "all"]}
        for sname in strategy_names
    }
    weekly_stats: dict[str, dict[str, dict[str, dict]]] = {
        sname: {seg: defaultdict(_new_stat) for seg in ["jra", "nar", "all"]}
        for sname in strategy_names
    }

    # M 戦略の自信度別振分内訳集計:
    #   breakdown_stats[conf_key][segment] = _new_stat()
    CONF_KEYS = ["SS", "S", "A", "B", "C", "D", "E"]
    breakdown_stats: dict[str, dict[str, dict]] = {
        conf: {seg: _new_stat() for seg in ["jra", "nar", "all"]}
        for conf in CONF_KEYS
    }

    # M' 戦略の自信度別振分内訳集計 (M と同構造)
    breakdown_stats_mprime: dict[str, dict[str, dict]] = {
        conf: {seg: _new_stat() for seg in ["jra", "nar", "all"]}
        for conf in CONF_KEYS
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

            # 自信度の取得 (仕様: SS/S/A/B/C/D/E 以外は "D" 扱い)
            raw_conf = (r.get("overall_confidence") or "").replace("⁺", "+").strip()
            if raw_conf not in ("SS", "S", "A", "B", "C", "D", "E"):
                raw_conf = "D"

            # 各戦略ごとに買い目を生成して集計
            for sname, fixed_pat, skip_de in STRATEGIES:
                # skip_de=True かつ自信度 D/E なら skip
                if skip_de and raw_conf in ("D", "E"):
                    continue

                # パターン決定
                if fixed_pat is not None:
                    # 一律戦略: 固定パターン
                    pat_name = fixed_pat
                elif sname == "M'":
                    # 戦略 M': STRATEGY_M_PRIME マッピング参照
                    pat_name = STRATEGY_M_PRIME.get(raw_conf)
                    if pat_name is None:
                        # None = skip
                        continue
                else:
                    # 戦略 M: STRATEGY_M マッピング参照
                    pat_name = STRATEGY_M.get(raw_conf)
                    if pat_name is None:
                        # None = skip
                        continue

                m1, m2, m3 = PATTERNS[pat_name]
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
                    ms = monthly_stats[sname][sg][ym]
                    ms["races_played"]  += 1
                    ms["races_hit"]     += (1 if race_hit else 0)
                    ms["tickets_total"] += len(tickets)
                    ms["tickets_hit"]   += hit_cnt
                    ms["stake"]         += stake
                    ms["payback"]       += payback
                    ms["tickets_dist"][len(tickets)] += 1
                    # 週次
                    ws = weekly_stats[sname][sg][yw]
                    ws["races_played"]  += 1
                    ws["races_hit"]     += (1 if race_hit else 0)
                    ws["tickets_total"] += len(tickets)
                    ws["tickets_hit"]   += hit_cnt
                    ws["stake"]         += stake
                    ws["payback"]       += payback
                    ws["tickets_dist"][len(tickets)] += 1

            # -----------------------------------------------------------------
            # M 戦略 振分内訳集計 (自信度ごとの詳細)
            # -----------------------------------------------------------------
            m_pat = STRATEGY_M.get(raw_conf)  # None=skip
            if m_pat is not None:
                m1, m2, m3 = PATTERNS[m_pat]
                tickets = build_tickets(horses, m1, m2, m3)
                if tickets:
                    stake   = len(tickets) * STAKE
                    payback = 0
                    hit_cnt = 0
                    for combo in tickets:
                        pb = lookup_sanrenpuku(payouts, combo)
                        if pb > 0:
                            payback += pb
                            hit_cnt += 1
                    race_hit = payback > 0
                    for sg in (seg, "all"):
                        bd = breakdown_stats[raw_conf][sg]
                        bd["races_played"]  += 1
                        bd["races_hit"]     += (1 if race_hit else 0)
                        bd["tickets_total"] += len(tickets)
                        bd["tickets_hit"]   += hit_cnt
                        bd["stake"]         += stake
                        bd["payback"]       += payback
                        bd["tickets_dist"][len(tickets)] += 1
            else:
                # skip の場合も R数をカウント (払戻 0 として記録)
                for sg in (seg, "all"):
                    bd = breakdown_stats[raw_conf][sg]
                    bd["races_played"] += 1

            # -----------------------------------------------------------------
            # M' 戦略 振分内訳集計 (自信度ごとの詳細)
            # -----------------------------------------------------------------
            mp_pat = STRATEGY_M_PRIME.get(raw_conf)  # None=skip
            if mp_pat is not None:
                m1, m2, m3 = PATTERNS[mp_pat]
                tickets = build_tickets(horses, m1, m2, m3)
                if tickets:
                    stake   = len(tickets) * STAKE
                    payback = 0
                    hit_cnt = 0
                    for combo in tickets:
                        pb = lookup_sanrenpuku(payouts, combo)
                        if pb > 0:
                            payback += pb
                            hit_cnt += 1
                    race_hit = payback > 0
                    for sg in (seg, "all"):
                        bd = breakdown_stats_mprime[raw_conf][sg]
                        bd["races_played"]  += 1
                        bd["races_hit"]     += (1 if race_hit else 0)
                        bd["tickets_total"] += len(tickets)
                        bd["tickets_hit"]   += hit_cnt
                        bd["stake"]         += stake
                        bd["payback"]       += payback
                        bd["tickets_dist"][len(tickets)] += 1
            else:
                # skip の場合も R数をカウント (払戻 0 として記録)
                for sg in (seg, "all"):
                    bd = breakdown_stats_mprime[raw_conf][sg]
                    bd["races_played"] += 1

        # 100ファイルごとに進捗表示
        if (fi + 1) % 100 == 0 or (fi + 1) == n_total:
            el = time.time() - started
            print(f"  {fi+1}/{n_total} ({date_str}) elapsed={el:.1f}s", flush=True)

    print(f"\n集計対象 pred 日数: {n_processed}", flush=True)

    # -----------------------------------------------------------------
    # CSV 出力
    # -----------------------------------------------------------------
    _write_monthly_csv(monthly_stats, strategy_names)
    _write_weekly_csv(weekly_stats, strategy_names)

    # -----------------------------------------------------------------
    # サマリログ出力
    # -----------------------------------------------------------------
    lines = _build_summary(monthly_stats, weekly_stats, breakdown_stats, breakdown_stats_mprime, strategy_names)
    output_text = "\n".join(lines)

    print(output_text, flush=True)

    # ログファイルに保存 (BOM付きUTF-8)
    with open(SUMMARY_LOG, "w", encoding="utf-8-sig") as f:
        f.write(output_text + "\n")
    print(f"\nログ保存: {SUMMARY_LOG}", flush=True)

    elapsed = time.time() - started
    print(f"Total elapsed: {elapsed:.1f}s", flush=True)


# ---------------------------------------------------------------------------
# CSV 書き出し
# ---------------------------------------------------------------------------

def _write_monthly_csv(monthly_stats: dict, strategy_names: list[str]):
    """月次 CSV を BOM付きUTF-8 で書き出す。"""
    fieldnames = [
        "strategy", "segment", "year_month",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    for sname in strategy_names:
        for seg in ["jra", "nar", "all"]:
            for ym in sorted(monthly_stats[sname][seg]):
                d = _calc_derived(monthly_stats[sname][seg][ym])
                rows.append({
                    "strategy":      sname,
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


def _write_weekly_csv(weekly_stats: dict, strategy_names: list[str]):
    """週次 CSV を BOM付きUTF-8 で書き出す。"""
    fieldnames = [
        "strategy", "segment", "year_week",
        "races_played", "races_hit", "hit_rate",
        "tickets_total", "stake", "payback", "balance", "roi", "mode_pts",
    ]
    rows = []
    for sname in strategy_names:
        for seg in ["jra", "nar", "all"]:
            for yw in sorted(weekly_stats[sname][seg]):
                d = _calc_derived(weekly_stats[sname][seg][yw])
                rows.append({
                    "strategy":      sname,
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
# サマリ表構築
# ---------------------------------------------------------------------------

def _build_summary(
    monthly_stats: dict,
    weekly_stats:  dict,
    breakdown_stats: dict,
    breakdown_stats_mprime: dict,
    strategy_names: list[str],
) -> list[str]:
    """サマリログ行リストを構築して返す。"""
    SEG_LABEL = {"all": "全体", "jra": "JRA", "nar": "NAR"}
    STRATEGY_LABEL = {
        "M":       "M (マスター案)",
        "M'":      "M' (D自信度→D)",
        "D一律":    "D 一律",
        "D+skip":  "D + SS+D+E skip",
        "C一律":    "C 一律",
        "E一律":    "E 一律",
    }
    lines = []

    for seg in ["all", "jra", "nar"]:
        seg_label = SEG_LABEL[seg]

        # ---- 月次安定性表 ----
        lines.append("")
        lines.append(f"{'='*18} {seg_label} 月次安定性 {'='*18}")
        header = (
            f"{'戦略':<18}  {'R数':>10}  {'ROI':>8}  {'的中率':>7}  {'赤字月':>6}"
            f"  {'最大DD':>14}  {'連敗':>4}  {'σ':>10}  {'中央点':>5}"
        )
        lines.append(header)
        lines.append("─" * 100)

        for sname in strategy_names:
            period_data = monthly_stats[sname][seg]
            if not period_data:
                lines.append(f"{STRATEGY_LABEL.get(sname, sname):<18}  (データなし)")
                continue

            # 全期間集計
            total_races   = sum(s["races_played"] for s in period_data.values())
            total_stake   = sum(s["stake"]   for s in period_data.values())
            total_payback = sum(s["payback"] for s in period_data.values())
            total_hit     = sum(s["races_hit"] for s in period_data.values())
            total_roi     = (total_payback / total_stake * 100) if total_stake > 0 else 0.0
            total_hitrate = (total_hit / total_races * 100) if total_races > 0 else 0.0

            # 月次安定性
            period_balances = [
                (ym, s["payback"] - s["stake"])
                for ym, s in period_data.items()
            ]
            stab = _calc_stability(period_balances)

            # 代表点数
            combined_dist: Counter = Counter()
            for s in period_data.values():
                combined_dist.update(s["tickets_dist"])
            mode_pts = combined_dist.most_common(1)[0][0] if combined_dist else 0

            sigma_k = stab["sigma"] / 1000
            label = STRATEGY_LABEL.get(sname, sname)
            line = (
                f"{label:<18}  {total_races:>10,}  {total_roi:>7.1f}%  "
                f"{total_hitrate:>6.1f}%  {stab['red_ratio']:>5.1f}%  "
                f"{-stab['max_dd']:>+13,}  "
                f"{stab['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{mode_pts:>3}点"
            )
            lines.append(line)

        lines.append("─" * 100)

        # ---- 週次安定性表 ----
        lines.append("")
        lines.append(f"{'='*18} {seg_label} 週次安定性 {'='*18}")
        header_w = (
            f"{'戦略':<18}  {'R数':>10}  {'ROI':>8}  {'的中率':>7}  {'赤字週':>6}"
            f"  {'最大DD':>14}  {'連敗':>4}  {'σ':>10}  {'中央点':>5}"
        )
        lines.append(header_w)
        lines.append("─" * 100)

        for sname in strategy_names:
            period_data = weekly_stats[sname][seg]
            if not period_data:
                lines.append(f"{STRATEGY_LABEL.get(sname, sname):<18}  (データなし)")
                continue

            total_races   = sum(s["races_played"] for s in period_data.values())
            total_stake   = sum(s["stake"]   for s in period_data.values())
            total_payback = sum(s["payback"] for s in period_data.values())
            total_hit     = sum(s["races_hit"] for s in period_data.values())
            total_roi     = (total_payback / total_stake * 100) if total_stake > 0 else 0.0
            total_hitrate = (total_hit / total_races * 100) if total_races > 0 else 0.0

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
            label = STRATEGY_LABEL.get(sname, sname)
            line = (
                f"{label:<18}  {total_races:>10,}  {total_roi:>7.1f}%  "
                f"{total_hitrate:>6.1f}%  {stab['red_ratio']:>5.1f}%  "
                f"{-stab['max_dd']:>+13,}  "
                f"{stab['max_losing_streak']:>4}  "
                f"¥{sigma_k:>8.0f}k  "
                f"{mode_pts:>3}点"
            )
            lines.append(line)

        lines.append("─" * 100)

    # ---- M 戦略 振分内訳 ----
    for seg in ["all", "jra", "nar"]:
        seg_label = SEG_LABEL[seg]
        lines.append("")
        lines.append(f"{'='*18} M 戦略 振分内訳 ({seg_label}) {'='*18}")
        # ヘッダー
        header_bd = (
            f"{'信頼度':<6}  {'パターン':<8}  {'R数':>8}  "
            f"{'購入':>12}  {'払戻':>12}  {'収支':>13}  {'ROI':>8}"
        )
        lines.append(header_bd)
        lines.append("─" * 90)

        total_races   = 0
        total_stake   = 0
        total_payback = 0

        for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
            m_pat = STRATEGY_M.get(conf)
            pat_label = m_pat if m_pat is not None else "skip"
            bd = breakdown_stats[conf][seg]
            rp      = bd["races_played"]
            stake   = bd["stake"]
            payback = bd["payback"]
            balance = payback - stake
            roi     = (payback / stake * 100) if stake > 0 else None

            if roi is not None:
                roi_str = f"{roi:.1f}%"
            else:
                roi_str = "-"

            total_races   += rp
            total_stake   += stake
            total_payback += payback

            line = (
                f"{conf:<6}  {pat_label:<8}  {rp:>8,}  "
                f"{stake:>11,}  {payback:>11,}  {balance:>+12,}  {roi_str:>8}"
            )
            lines.append(line)

        lines.append("─" * 90)
        # 全体合計行
        total_balance = total_payback - total_stake
        total_roi_val = (total_payback / total_stake * 100) if total_stake > 0 else None
        total_roi_str = f"{total_roi_val:.1f}%" if total_roi_val is not None else "-"
        lines.append(
            f"{'全体合計':<6}  {'-':<8}  {total_races:>8,}  "
            f"{total_stake:>11,}  {total_payback:>11,}  {total_balance:>+12,}  {total_roi_str:>8}"
        )
        lines.append("─" * 90)

    # ---- M' 戦略 振分内訳 ----
    for seg in ["all", "jra", "nar"]:
        seg_label = SEG_LABEL[seg]
        lines.append("")
        lines.append(f"{'='*18} M' 戦略 振分内訳 ({seg_label}) {'='*18}")
        # ヘッダー
        header_bd = (
            f"{'信頼度':<6}  {'パターン':<8}  {'R数':>8}  "
            f"{'購入':>12}  {'払戻':>12}  {'収支':>13}  {'ROI':>8}"
        )
        lines.append(header_bd)
        lines.append("─" * 90)

        total_races   = 0
        total_stake   = 0
        total_payback = 0

        for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
            mp_pat = STRATEGY_M_PRIME.get(conf)
            pat_label = mp_pat if mp_pat is not None else "skip"
            bd = breakdown_stats_mprime[conf][seg]
            rp      = bd["races_played"]
            stake   = bd["stake"]
            payback = bd["payback"]
            balance = payback - stake
            roi     = (payback / stake * 100) if stake > 0 else None

            if roi is not None:
                roi_str = f"{roi:.1f}%"
            else:
                roi_str = "-"

            total_races   += rp
            total_stake   += stake
            total_payback += payback

            line = (
                f"{conf:<6}  {pat_label:<8}  {rp:>8,}  "
                f"{stake:>11,}  {payback:>11,}  {balance:>+12,}  {roi_str:>8}"
            )
            lines.append(line)

        lines.append("─" * 90)
        # 全体合計行
        total_balance = total_payback - total_stake
        total_roi_val = (total_payback / total_stake * 100) if total_stake > 0 else None
        total_roi_str = f"{total_roi_val:.1f}%" if total_roi_val is not None else "-"
        lines.append(
            f"{'全体合計':<6}  {'-':<8}  {total_races:>8,}  "
            f"{total_stake:>11,}  {total_payback:>11,}  {total_balance:>+12,}  {total_roi_str:>8}"
        )
        lines.append("─" * 90)

    return lines


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
