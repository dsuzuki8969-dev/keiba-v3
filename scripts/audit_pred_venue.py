#!/usr/bin/env python3
"""
audit_pred_venue.py — pred.json 全期間 venue 異常 全件洗い出し
T-033 マスター指摘「過去の開催レース venue がおかしい」対応

調査内容:
  A: 開催不可 venue 混入（元旦 JRA は中山+京都のみ等）
  B: race_id 形式不正（桁数不正・非数字等）
  C: 重複 race_id（同 race_id が複数日の pred.json に存在）
  D: race_id venue_code と pred.json venue 名の不整合

実行:
  python scripts/audit_pred_venue.py
"""

import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"

# venue_master から venue コード→名前マッピングを取得
VENUE_NAME_TO_CODE = {
    "札幌": "01", "函館": "02", "福島": "03", "新潟": "04",
    "東京": "05", "中山": "06", "中京": "07", "京都": "08",
    "阪神": "09", "小倉": "10",
    "帯広": "65", "門別": "30", "盛岡": "35", "水沢": "36",
    "浦和": "42", "船橋": "43", "大井": "44", "川崎": "45",
    "金沢": "46", "笠松": "47", "名古屋": "48", "園田": "50",  # 2026-04-28: 50 に統一
    "姫路": "51", "高知": "54", "佐賀": "55",
}
VENUE_CODE_TO_NAME = {v: k for k, v in VENUE_NAME_TO_CODE.items()}
VENUE_CODE_TO_NAME["49"] = "園田"  # 旧コード互換（SPAT4 ベース）
VENUE_CODE_TO_NAME["52"] = "帯広"

JRA_CODES = frozenset(["01","02","03","04","05","06","07","08","09","10"])

# JRA 元旦・特殊日開催判定（慣例）
# 元旦: 通常 中山金杯(06) + 京都金杯(08) のみ
NEWYEAR_JRA_VENUES = {"06", "08"}  # 中山・京都

def parse_nar_race_id(race_id: str):
    """
    NAR race_id (12桁): YYYY[venue_code]MMDDРР
    例: 202645010112 → year=2026, venue=45(川崎), month=01, day=01, race=12
    """
    if len(race_id) != 12 or not race_id.isdigit():
        return None
    year = race_id[0:4]
    venue = race_id[4:6]
    month = race_id[6:8]
    day = race_id[8:10]
    race_no = race_id[10:12]
    return {"year": year, "venue": venue, "month": month, "day": day, "race_no": race_no}

def parse_jra_race_id(race_id: str):
    """
    JRA race_id (12桁): YYYY[venue_code:2][kai:2][nichi:2][R:2]

    T-033 で確定した正しい構造:
      [0:4]  = 年YYYY
      [4:6]  = venue_code (例: 05=東京, 06=中山)
      [6:8]  = 開催回次 (kai)
      [8:10] = 開催日次 (nichi)
      [10:12]= R番号 (race_no)

    例: 202605010101 → year=2026, venue=05(東京), kai=01, nichi=01, race_no=01
    T-037 修正: [8:10] → [4:6] (旧コードは venue_code 位置を誤読していた)
    """
    if len(race_id) != 12 or not race_id.isdigit():
        return None
    year = race_id[0:4]
    venue = race_id[4:6]  # T-037修正: [8:10] → [4:6] が正しい venue_code 位置
    race_no = race_id[10:12]
    return {"year": year, "venue": venue, "race_no": race_no}

def get_weekday(date_str: str) -> str:
    """YYYY-MM-DD → 曜日（日本語）"""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        weekdays = ["月","火","水","木","金","土","日"]
        return weekdays[d.weekday()]
    except Exception:
        return "?"

def is_new_year(date_str: str) -> bool:
    return date_str.endswith("-01-01")

def expected_jra_venues_newyear() -> set:
    return {"06", "08"}  # 中山+京都

# ============================================================
# メイン調査
# ============================================================

def audit_all():
    pred_files = sorted(PREDICTIONS_DIR.glob("*_pred.json"))
    # バックアップ・prev を除外
    pred_files = [f for f in pred_files if "bak" not in f.name and "backup" not in f.name and "prev" not in f.name]

    print(f"対象 pred.json ファイル数: {len(pred_files)}")

    # パターン A: 開催不可venue
    anomaly_a = []
    # パターン B: race_id 形式不正
    anomaly_b = []
    # パターン C: 重複 race_id
    all_race_ids = defaultdict(list)  # race_id -> [(date, venue)]
    # パターン D: race_id venue_code と pred venue 名不整合
    anomaly_d = []

    # 全レース venue 集計
    date_venue_count = defaultdict(lambda: defaultdict(int))  # date -> venue -> count

    total_races = 0
    error_files = []

    for pred_file in pred_files:
        date_str = pred_file.name[:8]
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        weekday = get_weekday(date_formatted)

        try:
            with open(pred_file, "rb") as f:
                raw = f.read()
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            error_files.append((pred_file.name, str(e)))
            continue

        races = data.get("races", [])
        if not races:
            continue

        total_races += len(races)

        for race in races:
            race_id = str(race.get("race_id", ""))
            venue = race.get("venue", "")
            is_jra_flag = race.get("is_jra", False)

            # venue 集計
            date_venue_count[date_formatted][venue] += 1

            # ----- パターン B: race_id 形式チェック -----
            if not race_id or not race_id.isdigit() or len(race_id) != 12:
                anomaly_b.append({
                    "date": date_formatted,
                    "weekday": weekday,
                    "race_id": race_id,
                    "venue": venue,
                    "is_jra": is_jra_flag,
                    "issue": f"race_id 形式不正: '{race_id}' (len={len(race_id)})",
                })
                continue

            # ----- パターン C: 重複 race_id -----
            all_race_ids[race_id].append((date_formatted, venue))

            # ----- パターン D: race_id venue_code vs pred venue 不整合 -----
            # T-037修正: JRA も NAR も venue_code は [4:6] が正しい位置
            # (旧コードは JRA を [8:10] と誤読し 6,971 件の偽陽性を生んでいた)
            if is_jra_flag:
                # JRA: [4:6] が venue_code (T-033 確定構造)
                id_venue_code = race_id[4:6]
            else:
                # NAR: [4:6] が venue_code
                id_venue_code = race_id[4:6]

            expected_venue_name = VENUE_CODE_TO_NAME.get(id_venue_code)
            if expected_venue_name and expected_venue_name != venue:
                anomaly_d.append({
                    "date": date_formatted,
                    "weekday": weekday,
                    "race_id": race_id,
                    "pred_venue": venue,
                    "id_venue_code": id_venue_code,
                    "id_venue_name": expected_venue_name,
                    "is_jra": is_jra_flag,
                })

            # ----- パターン A: 開催不可 venue 混入 -----
            if is_jra_flag and is_new_year(date_formatted):
                if id_venue_code not in expected_jra_venues_newyear():
                    anomaly_a.append({
                        "date": date_formatted,
                        "weekday": weekday,
                        "race_id": race_id,
                        "venue": venue,
                        "id_venue_code": id_venue_code,
                        "issue": f"元旦JRA開催不可会場: {venue}(code={id_venue_code}). 元旦は中山+京都のみ",
                        "category": "A-元旦JRA会場誤り",
                    })

    # ----- パターン C: 重複 race_id 集計 -----
    anomaly_c = []
    for rid, occurrences in all_race_ids.items():
        if len(occurrences) > 1:
            dates = ", ".join(f"{d}({v})" for d, v in occurrences)
            anomaly_c.append({
                "race_id": rid,
                "occurrences": len(occurrences),
                "dates": dates,
                "venues": list(set(v for _, v in occurrences)),
            })
    anomaly_c.sort(key=lambda x: x["race_id"])

    # ============================================================
    # race_id 構造の深堀り: JRA race_id の年 vs ファイル日付
    # ============================================================
    anomaly_year_mismatch = []
    for pred_file in pred_files:
        date_str = pred_file.name[:8]
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        file_year = date_str[:4]
        try:
            with open(pred_file, "rb") as f:
                raw = f.read()
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            continue
        for race in data.get("races", []):
            race_id = str(race.get("race_id", ""))
            if len(race_id) == 12 and race_id.isdigit():
                id_year = race_id[:4]
                if id_year != file_year:
                    anomaly_year_mismatch.append({
                        "date": date_formatted,
                        "race_id": race_id,
                        "file_year": file_year,
                        "id_year": id_year,
                        "venue": race.get("venue", ""),
                        "is_jra": race.get("is_jra", False),
                    })

    # ============================================================
    # JRA 開催日別 venue 集計（異常なJRA会場を全日程で探す）
    # ============================================================
    # 2026-01-01: 東京・阪神・小倉が混入している問題を確認
    # 全日付で「is_jra=True かつ その日は通常開催されない会場」を洗い出す
    # → JRA の開催は土日が基本 (一部月曜). 平日 is_jra=True は異常の可能性大

    jra_weekday_anomaly = []
    for pred_file in pred_files:
        date_str = pred_file.name[:8]
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        weekday = get_weekday(date_formatted)
        # 土曜=土, 日曜=日
        if weekday in ("土", "日"):
            continue  # 土日は正常の可能性

        try:
            with open(pred_file, "rb") as f:
                raw = f.read()
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            continue

        for race in data.get("races", []):
            if race.get("is_jra", False):
                race_id = str(race.get("race_id", ""))
                venue = race.get("venue", "")
                # 月曜の JRA: 振替開催（稀にあり）
                # 祝日は一部開催あり
                # ただし火水木金は通常なし
                if weekday in ("火", "水", "木", "金"):
                    jra_weekday_anomaly.append({
                        "date": date_formatted,
                        "weekday": weekday,
                        "race_id": race_id,
                        "venue": venue,
                    })

    # ============================================================
    # 出力
    # ============================================================
    output_lines = []
    output_lines.append("=" * 70)
    output_lines.append("T-033 pred.json venue 異常 調査レポート")
    output_lines.append("=" * 70)
    output_lines.append(f"対象 pred.json ファイル数: {len(pred_files)}")
    output_lines.append(f"総レース数: {total_races}")
    output_lines.append(f"読み込みエラー: {len(error_files)} ファイル")
    output_lines.append("")

    # サマリ
    output_lines.append("【サマリ】")
    output_lines.append(f"  パターンA (元旦JRA会場誤り):          {len(anomaly_a)} 件")
    output_lines.append(f"  パターンB (race_id形式不正):           {len(anomaly_b)} 件")
    output_lines.append(f"  パターンC (重複race_id):               {len(anomaly_c)} 件")
    output_lines.append(f"  パターンD (race_id venue vs pred venue): {len(anomaly_d)} 件")
    output_lines.append(f"  JRA race_id 年 vs ファイル日付不整合:  {len(anomaly_year_mismatch)} 件")
    output_lines.append(f"  JRA 平日(火-金)開催:                   {len(jra_weekday_anomaly)} 件")
    output_lines.append("")

    # --- パターン A ---
    output_lines.append("=" * 70)
    output_lines.append("【パターン A: 元旦 JRA 開催不可会場】")
    output_lines.append(f"  件数: {len(anomaly_a)}")
    output_lines.append("  元旦の JRA 開催は慣例的に 中山(06) + 京都(08) のみ")
    output_lines.append("-" * 50)
    if anomaly_a:
        for item in anomaly_a[:50]:
            output_lines.append(f"  {item['date']}({item['weekday']}) | {item['venue']}(code={item['id_venue_code']}) | {item['race_id']} | {item['issue']}")
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- パターン B ---
    output_lines.append("=" * 70)
    output_lines.append("【パターン B: race_id 形式不正】")
    output_lines.append(f"  件数: {len(anomaly_b)}")
    output_lines.append("-" * 50)
    if anomaly_b:
        for item in anomaly_b[:30]:
            output_lines.append(f"  {item['date']}({item['weekday']}) | {item['venue']} | {item['race_id']} | {item['issue']}")
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- パターン C ---
    output_lines.append("=" * 70)
    output_lines.append("【パターン C: 重複 race_id（複数日に同一 race_id）】")
    output_lines.append(f"  件数: {len(anomaly_c)} race_id")
    output_lines.append("-" * 50)
    if anomaly_c:
        for item in anomaly_c[:30]:
            output_lines.append(f"  race_id={item['race_id']} | {item['occurrences']}回出現 | {item['dates']}")
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- パターン D ---
    output_lines.append("=" * 70)
    output_lines.append("【パターン D: race_id venue_code vs pred.json venue 名 不整合】")
    output_lines.append(f"  件数: {len(anomaly_d)}")
    output_lines.append("-" * 50)
    if anomaly_d:
        for item in anomaly_d[:50]:
            output_lines.append(
                f"  {item['date']}({item['weekday']}) | pred_venue={item['pred_venue']} | "
                f"race_id[4:6 or 8:10]={item['id_venue_code']}→{item['id_venue_name']} | "
                f"race_id={item['race_id']} | is_jra={item['is_jra']}"
            )
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- JRA 年不整合 ---
    output_lines.append("=" * 70)
    output_lines.append("【JRA race_id 先頭年 vs ファイル日付年 不整合】")
    output_lines.append(f"  件数: {len(anomaly_year_mismatch)}")
    output_lines.append("-" * 50)
    if anomaly_year_mismatch:
        shown = anomaly_year_mismatch[:20]
        for item in shown:
            output_lines.append(
                f"  {item['date']} | race_id={item['race_id']} | "
                f"file_year={item['file_year']} id_year={item['id_year']} | "
                f"venue={item['venue']} is_jra={item['is_jra']}"
            )
        if len(anomaly_year_mismatch) > 20:
            output_lines.append(f"  ... 以降 {len(anomaly_year_mismatch)-20} 件省略")
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- JRA 平日開催 ---
    output_lines.append("=" * 70)
    output_lines.append("【JRA 平日(火〜金)開催レース】")
    output_lines.append(f"  件数: {len(jra_weekday_anomaly)}")
    output_lines.append("  注: 振替開催・臨時開催は例外あり。全件が異常とは限らない")
    output_lines.append("-" * 50)
    if jra_weekday_anomaly:
        # 日付ごとにグループ集計
        by_date = defaultdict(lambda: defaultdict(int))
        for item in jra_weekday_anomaly:
            by_date[item["date"]][item["venue"]] += 1
        for d in sorted(by_date.keys()):
            weekday = get_weekday(d)
            venues_str = ", ".join(f"{v}:{cnt}R" for v, cnt in sorted(by_date[d].items()))
            output_lines.append(f"  {d}({weekday}) | {venues_str}")
    else:
        output_lines.append("  異常なし")
    output_lines.append("")

    # --- 日別 venue 集計（全期間上位異常日） ---
    output_lines.append("=" * 70)
    output_lines.append("【日別 venue 集計（参考：会場数が多い日 TOP 30）】")
    output_lines.append("-" * 50)
    date_venue_list = []
    for d, venues in date_venue_count.items():
        venue_names = sorted(venues.keys())
        jra_venues = [v for v in venue_names if VENUE_NAME_TO_CODE.get(v, "99") in JRA_CODES]
        nar_venues = [v for v in venue_names if VENUE_NAME_TO_CODE.get(v, "99") not in JRA_CODES]
        weekday = get_weekday(d)
        date_venue_list.append((d, weekday, jra_venues, nar_venues, sum(venues.values())))

    date_venue_list.sort(key=lambda x: -x[4])  # 総レース数降順
    for d, wday, jra_vs, nar_vs, total in date_venue_list[:30]:
        jra_str = "+".join(jra_vs) if jra_vs else "-"
        nar_str = "+".join(nar_vs) if nar_vs else "-"
        output_lines.append(f"  {d}({wday}) 計{total:3d}R | JRA: {jra_str} | NAR: {nar_str}")

    output_lines.append("")

    # --- エラーファイル ---
    if error_files:
        output_lines.append("=" * 70)
        output_lines.append("【読み込みエラーファイル】")
        for fname, err in error_files:
            output_lines.append(f"  {fname}: {err}")
        output_lines.append("")

    # --- 元旦 JRA 開催一覧（全年） ---
    output_lines.append("=" * 70)
    output_lines.append("【元旦(1/1) 全年 開催状況】")
    output_lines.append("-" * 50)
    newyear_dates = [(d, wday, jvs, nvs, tot) for d, wday, jvs, nvs, tot in date_venue_list
                     if d.endswith("-01-01")]
    newyear_dates.sort(key=lambda x: x[0])
    for d, wday, jra_vs, nar_vs, total in newyear_dates:
        jra_str = "+".join(jra_vs) if jra_vs else "JRA開催なし"
        nar_str = "+".join(nar_vs) if nar_vs else "NAR開催なし"
        output_lines.append(f"  {d}({wday}) | JRA: {jra_str} | NAR: {nar_str} | 計{total}R")
    output_lines.append("")

    report = "\n".join(output_lines)
    out_path = PROJECT_ROOT / "tmp_audit_pred_venue_report.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(report)
    print(f"\nレポート保存先: {out_path}")


if __name__ == "__main__":
    audit_all()
