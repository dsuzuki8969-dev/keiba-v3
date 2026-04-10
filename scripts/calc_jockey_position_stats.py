"""
全JRA・NAR騎手の位置取り傾向を集計
- 逃げ率（1角1位率）
- 先行率（1角2-3位率）
- 前行き率（1角3位以内率）
- 中団率（1角で相対位置30-60%）
- 後方率（1角で相対位置60%超）
- 差し追込馬での逃げ率・先行率・前行き率
- 平均1角相対位置
出力: data/jockey_position_stats.csv + SQLite jockey_position_stats テーブル
"""
import sys, os, json, csv, sqlite3, statistics
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "keiba.db")
OUT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "jockey_position_stats.csv")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("全騎手の位置取り傾向を集計中...")
    t0 = datetime.now()

    # 全レコード取得（通過順有効 + 5頭以上）
    rows = conn.execute("""
        SELECT jockey_name, jockey_id, positions_corners, field_count,
               running_style, is_jra, finish_pos, race_date
        FROM race_log
        WHERE jockey_name IS NOT NULL AND jockey_name != ''
          AND positions_corners IS NOT NULL AND positions_corners != '' AND positions_corners != '[]'
          AND field_count >= 5
        ORDER BY jockey_name, race_date
    """).fetchall()

    print(f"  対象レコード数: {len(rows):,}")

    # 騎手ごとに集計
    stats = defaultdict(lambda: {
        "jockey_id": "",
        "rides_total": 0,        # 総騎乗数
        "rides_jra": 0,          # JRA騎乗数
        "rides_nar": 0,          # NAR騎乗数
        # 全体
        "nige": 0,               # 逃げ（1角1位）
        "senko": 0,              # 先行（1角2-3位）
        "mae_iki": 0,            # 前行き（1角3位以内）
        "chudan": 0,             # 中団（相対位置30-60%）
        "koho": 0,               # 後方（相対位置60%超）
        "pos1c_sum": 0.0,        # 1角相対位置の合計
        # 差し追込馬限定
        "ds_rides": 0,           # 差し追込馬の騎乗数
        "ds_nige": 0,            # 差し追込馬で逃げ
        "ds_senko": 0,           # 差し追込馬で先行
        "ds_mae_iki": 0,         # 差し追込馬で前行き
        # 逃げ先行馬限定
        "ns_rides": 0,           # 逃げ先行馬の騎乗数
        "ns_koho": 0,            # 逃げ先行馬で後方
        # 勝利数（参考）
        "wins": 0,
        # 直近1年
        "recent_rides": 0,
        "recent_nige": 0,
        "recent_senko": 0,
        "recent_mae_iki": 0,
    })

    # 直近1年の基準日
    recent_cutoff = "2025-04-08"  # 1年前

    for r in rows:
        jname = r["jockey_name"]
        jid = r["jockey_id"] or ""
        fc = r["field_count"]
        rs = r["running_style"] or ""
        is_jra = r["is_jra"]
        fp = r["finish_pos"] or 99
        rdate = r["race_date"] or ""

        # 通過順パース
        try:
            corners = json.loads(r["positions_corners"])
        except (json.JSONDecodeError, TypeError):
            continue

        if not corners:
            continue

        # 1角位置を取得（0は欠損、有効値を前方から探す）
        pos1c = None
        for p in corners:
            if isinstance(p, (int, float)) and p > 0:
                pos1c = p
                break
        if pos1c is None:
            continue

        rel_pos = pos1c / fc if fc > 0 else 0.5

        s = stats[jname]
        if not s["jockey_id"]:
            s["jockey_id"] = jid

        s["rides_total"] += 1
        if is_jra:
            s["rides_jra"] += 1
        else:
            s["rides_nar"] += 1

        # 位置分類
        if pos1c == 1:
            s["nige"] += 1
            s["mae_iki"] += 1
        elif pos1c <= 3:
            s["senko"] += 1
            s["mae_iki"] += 1

        if 0.3 < rel_pos <= 0.6:
            s["chudan"] += 1
        elif rel_pos > 0.6:
            s["koho"] += 1

        s["pos1c_sum"] += rel_pos

        # 差し追込馬限定
        if "差" in rs or "追" in rs:
            s["ds_rides"] += 1
            if pos1c == 1:
                s["ds_nige"] += 1
                s["ds_mae_iki"] += 1
            elif pos1c <= 3:
                s["ds_senko"] += 1
                s["ds_mae_iki"] += 1

        # 逃げ先行馬限定
        if "逃" in rs or "先" in rs:
            s["ns_rides"] += 1
            if rel_pos > 0.6:
                s["ns_koho"] += 1

        # 勝利
        if fp == 1:
            s["wins"] += 1

        # 直近1年
        if rdate >= recent_cutoff:
            s["recent_rides"] += 1
            if pos1c == 1:
                s["recent_nige"] += 1
                s["recent_mae_iki"] += 1
            elif pos1c <= 3:
                s["recent_senko"] += 1
                s["recent_mae_iki"] += 1

    print(f"  集計騎手数: {len(stats):,}")

    # CSV出力
    header = [
        "jockey_name", "jockey_id", "category",
        "rides_total", "rides_jra", "rides_nar",
        "nige_rate", "senko_rate", "mae_iki_rate", "chudan_rate", "koho_rate",
        "avg_pos1c_rel",
        "ds_rides", "ds_nige_rate", "ds_senko_rate", "ds_mae_iki_rate",
        "ns_rides", "ns_koho_rate",
        "wins", "win_rate",
        "recent_rides", "recent_nige_rate", "recent_mae_iki_rate",
    ]

    rows_out = []
    for jname, s in stats.items():
        t = s["rides_total"]
        if t < 1:
            continue

        # JRA/NAR/両方 カテゴリ
        if s["rides_jra"] > 0 and s["rides_nar"] > 0:
            cat = "JRA+NAR"
        elif s["rides_jra"] > 0:
            cat = "JRA"
        else:
            cat = "NAR"

        def pct(n, d):
            return round(n / d * 100, 2) if d > 0 else 0.0

        row = {
            "jockey_name": jname,
            "jockey_id": s["jockey_id"],
            "category": cat,
            "rides_total": t,
            "rides_jra": s["rides_jra"],
            "rides_nar": s["rides_nar"],
            "nige_rate": pct(s["nige"], t),
            "senko_rate": pct(s["senko"], t),
            "mae_iki_rate": pct(s["mae_iki"], t),
            "chudan_rate": pct(s["chudan"], t),
            "koho_rate": pct(s["koho"], t),
            "avg_pos1c_rel": round(s["pos1c_sum"] / t, 4) if t > 0 else 0.5,
            "ds_rides": s["ds_rides"],
            "ds_nige_rate": pct(s["ds_nige"], s["ds_rides"]),
            "ds_senko_rate": pct(s["ds_senko"], s["ds_rides"]),
            "ds_mae_iki_rate": pct(s["ds_mae_iki"], s["ds_rides"]),
            "ns_rides": s["ns_rides"],
            "ns_koho_rate": pct(s["ns_koho"], s["ns_rides"]),
            "wins": s["wins"],
            "win_rate": pct(s["wins"], t),
            "recent_rides": s["recent_rides"],
            "recent_nige_rate": pct(s["recent_nige"], s["recent_rides"]),
            "recent_mae_iki_rate": pct(s["recent_mae_iki"], s["recent_rides"]),
        }
        rows_out.append(row)

    # 騎乗数降順でソート
    rows_out.sort(key=lambda r: r["rides_total"], reverse=True)

    # CSV書き出し
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"  CSV出力: {OUT_CSV} ({len(rows_out)}騎手)")

    # SQLiteテーブルにも保存
    conn.execute("DROP TABLE IF EXISTS jockey_position_stats")
    conn.execute("""
        CREATE TABLE jockey_position_stats (
            jockey_name TEXT NOT NULL,
            jockey_id TEXT,
            category TEXT,
            rides_total INTEGER,
            rides_jra INTEGER,
            rides_nar INTEGER,
            nige_rate REAL,
            senko_rate REAL,
            mae_iki_rate REAL,
            chudan_rate REAL,
            koho_rate REAL,
            avg_pos1c_rel REAL,
            ds_rides INTEGER,
            ds_nige_rate REAL,
            ds_senko_rate REAL,
            ds_mae_iki_rate REAL,
            ns_rides INTEGER,
            ns_koho_rate REAL,
            wins INTEGER,
            win_rate REAL,
            recent_rides INTEGER,
            recent_nige_rate REAL,
            recent_mae_iki_rate REAL,
            PRIMARY KEY (jockey_name)
        )
    """)

    for r in rows_out:
        conn.execute("""
            INSERT INTO jockey_position_stats VALUES (
                :jockey_name, :jockey_id, :category,
                :rides_total, :rides_jra, :rides_nar,
                :nige_rate, :senko_rate, :mae_iki_rate, :chudan_rate, :koho_rate,
                :avg_pos1c_rel,
                :ds_rides, :ds_nige_rate, :ds_senko_rate, :ds_mae_iki_rate,
                :ns_rides, :ns_koho_rate,
                :wins, :win_rate,
                :recent_rides, :recent_nige_rate, :recent_mae_iki_rate
            )
        """, r)

    conn.commit()
    print(f"  SQLiteテーブル: jockey_position_stats ({len(rows_out)}行)")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"  完了: {elapsed:.1f}秒")

    # サマリ表示: 騎乗数30以上の騎手トップ30（前行き率順）
    print()
    print("=" * 120)
    print("【前行き率 TOP30（騎乗数30以上）】")
    print(f"{'騎手':<14} {'区分':<8} {'騎乗':>5} {'逃げ率':>7} {'先行率':>7} {'前行率':>7} {'中団率':>7} {'後方率':>7} {'平均位置':>8} | {'差追騎乗':>6} {'差追逃':>6} {'差追前行':>7} | {'直近1年':>6} {'直近前行':>7}")
    print("-" * 120)
    top30 = sorted(
        [r for r in rows_out if r["rides_total"] >= 30],
        key=lambda r: r["mae_iki_rate"], reverse=True
    )[:30]
    for r in top30:
        print(f"{r['jockey_name']:<14} {r['category']:<8} {r['rides_total']:>5} "
              f"{r['nige_rate']:>6.1f}% {r['senko_rate']:>6.1f}% {r['mae_iki_rate']:>6.1f}% "
              f"{r['chudan_rate']:>6.1f}% {r['koho_rate']:>6.1f}% {r['avg_pos1c_rel']:>7.3f} | "
              f"{r['ds_rides']:>6} {r['ds_nige_rate']:>5.1f}% {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['recent_rides']:>6} {r['recent_mae_iki_rate']:>6.1f}%")

    print()
    print("=" * 120)
    print("【差し追込馬の前行き率 TOP30（差追騎乗20以上）】")
    print(f"{'騎手':<14} {'区分':<8} {'差追騎乗':>6} {'差追逃':>6} {'差追先行':>7} {'差追前行':>7} | {'全体騎乗':>6} {'全体前行':>7} {'全体逃げ':>7}")
    print("-" * 120)
    ds_top30 = sorted(
        [r for r in rows_out if r["ds_rides"] >= 20],
        key=lambda r: r["ds_mae_iki_rate"], reverse=True
    )[:30]
    for r in ds_top30:
        print(f"{r['jockey_name']:<14} {r['category']:<8} {r['ds_rides']:>6} "
              f"{r['ds_nige_rate']:>5.1f}% {r['ds_senko_rate']:>6.1f}% {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['rides_total']:>6} {r['mae_iki_rate']:>6.1f}% {r['nige_rate']:>6.1f}%")

    # 主要JRA騎手一覧
    print()
    print("=" * 120)
    print("【JRA主要騎手（直近1年30騎乗以上、前行き率順）】")
    print(f"{'騎手':<14} {'騎乗':>5} {'逃げ率':>7} {'先行率':>7} {'前行率':>7} {'中団率':>7} {'後方率':>7} | {'差追騎乗':>6} {'差追前行':>7} | {'直近前行':>7} {'勝率':>6}")
    print("-" * 120)
    jra_main = sorted(
        [r for r in rows_out if r["category"] in ("JRA", "JRA+NAR") and r["recent_rides"] >= 30],
        key=lambda r: r["mae_iki_rate"], reverse=True
    )
    for r in jra_main:
        print(f"{r['jockey_name']:<14} {r['rides_total']:>5} "
              f"{r['nige_rate']:>6.1f}% {r['senko_rate']:>6.1f}% {r['mae_iki_rate']:>6.1f}% "
              f"{r['chudan_rate']:>6.1f}% {r['koho_rate']:>6.1f}% | "
              f"{r['ds_rides']:>6} {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['recent_mae_iki_rate']:>6.1f}% {r['win_rate']:>5.1f}%")

    # 主要NAR騎手一覧
    print()
    print("=" * 120)
    print("【NAR主要騎手（直近1年30騎乗以上、前行き率順）】")
    print(f"{'騎手':<14} {'騎乗':>5} {'逃げ率':>7} {'先行率':>7} {'前行率':>7} {'中団率':>7} {'後方率':>7} | {'差追騎乗':>6} {'差追前行':>7} | {'直近前行':>7} {'勝率':>6}")
    print("-" * 120)
    nar_main = sorted(
        [r for r in rows_out if r["category"] in ("NAR", "JRA+NAR") and r["recent_rides"] >= 30
         and r["rides_nar"] > r["rides_jra"]],
        key=lambda r: r["mae_iki_rate"], reverse=True
    )
    for r in nar_main:
        print(f"{r['jockey_name']:<14} {r['rides_total']:>5} "
              f"{r['nige_rate']:>6.1f}% {r['senko_rate']:>6.1f}% {r['mae_iki_rate']:>6.1f}% "
              f"{r['chudan_rate']:>6.1f}% {r['koho_rate']:>6.1f}% | "
              f"{r['ds_rides']:>6} {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['recent_mae_iki_rate']:>6.1f}% {r['win_rate']:>5.1f}%")

    conn.close()
    print()
    print(f"完了。CSV: {OUT_CSV}")
    print(f"SQLite: data/keiba.db → jockey_position_stats テーブル")


def calc_trainer_stats():
    """全調教師の位置取り傾向を集計（騎手と同じ項目）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    OUT_TRAINER_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "trainer_position_stats.csv")

    print()
    print("=" * 80)
    print("全調教師の位置取り傾向を集計中...")
    t0 = datetime.now()

    rows = conn.execute("""
        SELECT trainer_name, trainer_id, positions_corners, field_count,
               running_style, is_jra, finish_pos, race_date
        FROM race_log
        WHERE trainer_name IS NOT NULL AND trainer_name != ''
          AND positions_corners IS NOT NULL AND positions_corners != '' AND positions_corners != '[]'
          AND field_count >= 5
        ORDER BY trainer_name, race_date
    """).fetchall()

    print(f"  対象レコード数: {len(rows):,}")

    stats = defaultdict(lambda: {
        "trainer_id": "",
        "rides_total": 0,
        "rides_jra": 0,
        "rides_nar": 0,
        "nige": 0,
        "senko": 0,
        "mae_iki": 0,
        "chudan": 0,
        "koho": 0,
        "pos1c_sum": 0.0,
        "ds_rides": 0,
        "ds_nige": 0,
        "ds_senko": 0,
        "ds_mae_iki": 0,
        "ns_rides": 0,
        "ns_koho": 0,
        "wins": 0,
        "recent_rides": 0,
        "recent_nige": 0,
        "recent_senko": 0,
        "recent_mae_iki": 0,
    })

    recent_cutoff = "2025-04-08"

    for r in rows:
        tname = r["trainer_name"]
        tid = r["trainer_id"] or ""
        fc = r["field_count"]
        rs = r["running_style"] or ""
        is_jra = r["is_jra"]
        fp = r["finish_pos"] or 99
        rdate = r["race_date"] or ""

        try:
            corners = json.loads(r["positions_corners"])
        except (json.JSONDecodeError, TypeError):
            continue

        if not corners:
            continue

        pos1c = None
        for p in corners:
            if isinstance(p, (int, float)) and p > 0:
                pos1c = p
                break
        if pos1c is None:
            continue

        rel_pos = pos1c / fc if fc > 0 else 0.5

        s = stats[tname]
        if not s["trainer_id"]:
            s["trainer_id"] = tid

        s["rides_total"] += 1
        if is_jra:
            s["rides_jra"] += 1
        else:
            s["rides_nar"] += 1

        if pos1c == 1:
            s["nige"] += 1
            s["mae_iki"] += 1
        elif pos1c <= 3:
            s["senko"] += 1
            s["mae_iki"] += 1

        if 0.3 < rel_pos <= 0.6:
            s["chudan"] += 1
        elif rel_pos > 0.6:
            s["koho"] += 1

        s["pos1c_sum"] += rel_pos

        if "差" in rs or "追" in rs:
            s["ds_rides"] += 1
            if pos1c == 1:
                s["ds_nige"] += 1
                s["ds_mae_iki"] += 1
            elif pos1c <= 3:
                s["ds_senko"] += 1
                s["ds_mae_iki"] += 1

        if "逃" in rs or "先" in rs:
            s["ns_rides"] += 1
            if rel_pos > 0.6:
                s["ns_koho"] += 1

        if fp == 1:
            s["wins"] += 1

        if rdate >= recent_cutoff:
            s["recent_rides"] += 1
            if pos1c == 1:
                s["recent_nige"] += 1
                s["recent_mae_iki"] += 1
            elif pos1c <= 3:
                s["recent_senko"] += 1
                s["recent_mae_iki"] += 1

    print(f"  集計調教師数: {len(stats):,}")

    header = [
        "trainer_name", "trainer_id", "category",
        "rides_total", "rides_jra", "rides_nar",
        "nige_rate", "senko_rate", "mae_iki_rate", "chudan_rate", "koho_rate",
        "avg_pos1c_rel",
        "ds_rides", "ds_nige_rate", "ds_senko_rate", "ds_mae_iki_rate",
        "ns_rides", "ns_koho_rate",
        "wins", "win_rate",
        "recent_rides", "recent_nige_rate", "recent_mae_iki_rate",
    ]

    rows_out = []
    for tname, s in stats.items():
        t = s["rides_total"]
        if t < 1:
            continue

        if s["rides_jra"] > 0 and s["rides_nar"] > 0:
            cat = "JRA+NAR"
        elif s["rides_jra"] > 0:
            cat = "JRA"
        else:
            cat = "NAR"

        def pct(n, d):
            return round(n / d * 100, 2) if d > 0 else 0.0

        row = {
            "trainer_name": tname,
            "trainer_id": s["trainer_id"],
            "category": cat,
            "rides_total": t,
            "rides_jra": s["rides_jra"],
            "rides_nar": s["rides_nar"],
            "nige_rate": pct(s["nige"], t),
            "senko_rate": pct(s["senko"], t),
            "mae_iki_rate": pct(s["mae_iki"], t),
            "chudan_rate": pct(s["chudan"], t),
            "koho_rate": pct(s["koho"], t),
            "avg_pos1c_rel": round(s["pos1c_sum"] / t, 4) if t > 0 else 0.5,
            "ds_rides": s["ds_rides"],
            "ds_nige_rate": pct(s["ds_nige"], s["ds_rides"]),
            "ds_senko_rate": pct(s["ds_senko"], s["ds_rides"]),
            "ds_mae_iki_rate": pct(s["ds_mae_iki"], s["ds_rides"]),
            "ns_rides": s["ns_rides"],
            "ns_koho_rate": pct(s["ns_koho"], s["ns_rides"]),
            "wins": s["wins"],
            "win_rate": pct(s["wins"], t),
            "recent_rides": s["recent_rides"],
            "recent_nige_rate": pct(s["recent_nige"], s["recent_rides"]),
            "recent_mae_iki_rate": pct(s["recent_mae_iki"], s["recent_rides"]),
        }
        rows_out.append(row)

    rows_out.sort(key=lambda r: r["rides_total"], reverse=True)

    with open(OUT_TRAINER_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"  CSV出力: {OUT_TRAINER_CSV} ({len(rows_out)}調教師)")

    # SQLiteテーブル
    conn.execute("DROP TABLE IF EXISTS trainer_position_stats")
    conn.execute("""
        CREATE TABLE trainer_position_stats (
            trainer_name TEXT NOT NULL,
            trainer_id TEXT,
            category TEXT,
            rides_total INTEGER,
            rides_jra INTEGER,
            rides_nar INTEGER,
            nige_rate REAL,
            senko_rate REAL,
            mae_iki_rate REAL,
            chudan_rate REAL,
            koho_rate REAL,
            avg_pos1c_rel REAL,
            ds_rides INTEGER,
            ds_nige_rate REAL,
            ds_senko_rate REAL,
            ds_mae_iki_rate REAL,
            ns_rides INTEGER,
            ns_koho_rate REAL,
            wins INTEGER,
            win_rate REAL,
            recent_rides INTEGER,
            recent_nige_rate REAL,
            recent_mae_iki_rate REAL,
            PRIMARY KEY (trainer_name)
        )
    """)

    for r in rows_out:
        conn.execute("""
            INSERT INTO trainer_position_stats VALUES (
                :trainer_name, :trainer_id, :category,
                :rides_total, :rides_jra, :rides_nar,
                :nige_rate, :senko_rate, :mae_iki_rate, :chudan_rate, :koho_rate,
                :avg_pos1c_rel,
                :ds_rides, :ds_nige_rate, :ds_senko_rate, :ds_mae_iki_rate,
                :ns_rides, :ns_koho_rate,
                :wins, :win_rate,
                :recent_rides, :recent_nige_rate, :recent_mae_iki_rate
            )
        """, r)

    conn.commit()
    print(f"  SQLiteテーブル: trainer_position_stats ({len(rows_out)}行)")

    # サマリ: 前行き率TOP30
    print()
    print("=" * 120)
    print("【調教師 前行き率 TOP30（管理馬50頭以上出走）】")
    print(f"{'調教師':<14} {'区分':<8} {'出走':>5} {'逃げ率':>7} {'先行率':>7} {'前行率':>7} {'中団率':>7} {'後方率':>7} | {'差追出走':>6} {'差追前行':>7} | {'直近前行':>7} {'勝率':>6}")
    print("-" * 120)
    top30 = sorted(
        [r for r in rows_out if r["rides_total"] >= 50],
        key=lambda r: r["mae_iki_rate"], reverse=True
    )[:30]
    for r in top30:
        print(f"{r['trainer_name']:<14} {r['category']:<8} {r['rides_total']:>5} "
              f"{r['nige_rate']:>6.1f}% {r['senko_rate']:>6.1f}% {r['mae_iki_rate']:>6.1f}% "
              f"{r['chudan_rate']:>6.1f}% {r['koho_rate']:>6.1f}% | "
              f"{r['ds_rides']:>6} {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['recent_mae_iki_rate']:>6.1f}% {r['win_rate']:>5.1f}%")

    # JRA主要調教師
    print()
    print("=" * 120)
    print("【JRA主要調教師（直近1年30頭以上出走、前行き率順）】")
    print(f"{'調教師':<14} {'出走':>5} {'逃げ率':>7} {'先行率':>7} {'前行率':>7} {'中団率':>7} {'後方率':>7} | {'差追出走':>6} {'差追前行':>7} | {'直近前行':>7} {'勝率':>6}")
    print("-" * 120)
    jra_t = sorted(
        [r for r in rows_out if r["category"] in ("JRA", "JRA+NAR") and r["recent_rides"] >= 30],
        key=lambda r: r["mae_iki_rate"], reverse=True
    )
    for r in jra_t:
        print(f"{r['trainer_name']:<14} {r['rides_total']:>5} "
              f"{r['nige_rate']:>6.1f}% {r['senko_rate']:>6.1f}% {r['mae_iki_rate']:>6.1f}% "
              f"{r['chudan_rate']:>6.1f}% {r['koho_rate']:>6.1f}% | "
              f"{r['ds_rides']:>6} {r['ds_mae_iki_rate']:>6.1f}% | "
              f"{r['recent_mae_iki_rate']:>6.1f}% {r['win_rate']:>5.1f}%")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n  調教師集計完了: {elapsed:.1f}秒")
    print(f"  CSV: {OUT_TRAINER_CSV}")
    print(f"  SQLite: data/keiba.db → trainer_position_stats テーブル")

    conn.close()


if __name__ == "__main__":
    main()
    calc_trainer_stats()
