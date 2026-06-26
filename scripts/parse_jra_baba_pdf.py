#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JRA archive 馬場PDF パーサ (P3 パイロット・プロトタイプ)

JRA公式 archive (jra.go.jp/keiba/baba/archive/{year}pdf/{racecourse}{kai}.pdf) の
クッション値・含水率PDFをパースして構造化する。

データ構造 (1行 = 1測定日):
  測定月日 / 芝使用コース / 芝クッション値 / 芝含水率(ゴール前,4角) / ダート含水率(ゴール前,4角)
  ※ クッション値は芝のみ (JRA仕様・ダートは含水率のみ)
  ※ 金曜(開催前日)の測定値もあり = 予測時点(前日)でリーク無

依存: pdfplumber (導入済み)。本番非改変・プロトタイプ。
実行: python scripts/parse_jra_baba_pdf.py <pdf_path>
"""
import sys
import re
import json
from typing import List, Dict

# 行パターン: 月 日 曜日 コース [芝時刻 クッション値] [含水率時刻 芝G 芝4 ダG ダ4]
_ROW = re.compile(
    r"(\d+)月\s*(\d+)日\s+(\S曜日)\s+([A-Z])\s+"
    r"(\d+:\d+)\s+([\d.]+)\s+"                                  # 芝測定時刻, 芝クッション値
    r"(\d+:\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"    # 含水率時刻, 芝G, 芝4, ダG, ダ4
)

# ヘッダ: 年・開催回・競馬場
_HEADER = re.compile(r"(\d{4})年\s*第?\s*(\d+)\s*回\s*(\S+?)競馬")


def parse_baba_pdf(pdf_path: str) -> Dict:
    """馬場PDF をパースして {meta, rows} を返す。"""
    import pdfplumber

    meta = {"year": None, "kai": None, "racecourse": None}
    rows: List[Dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for line in txt.split("\n"):
                if meta["year"] is None:
                    hm = _HEADER.search(line.replace("　", " "))
                    if hm:
                        meta["year"] = int(hm.group(1))
                        meta["kai"] = int(hm.group(2))
                        meta["racecourse"] = hm.group(3)
                m = _ROW.search(line)
                if m:
                    (month, day, dow, course, _t1, cushion,
                     _t2, tg, tc, dg, dc) = m.groups()
                    rows.append({
                        "month": int(month),
                        "day": int(day),
                        "dow": dow,
                        "turf_course": course,              # 芝使用コース(A/B/C..)
                        "cushion_value": float(cushion),    # 芝クッション値
                        "moist_turf_goal": float(tg),       # 芝含水率 ゴール前%
                        "moist_turf_corner": float(tc),     # 芝含水率 4コーナー%
                        "moist_dirt_goal": float(dg),       # ダ含水率 ゴール前%
                        "moist_dirt_corner": float(dc),     # ダ含水率 4コーナー%
                    })
    return {"meta": meta, "rows": rows}


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python scripts/parse_jra_baba_pdf.py <pdf_path> [--json]")
        sys.exit(1)
    result = parse_baba_pdf(sys.argv[1])
    if "--json" in sys.argv:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(f"meta: {result['meta']}")
    print(f"パース行数: {len(result['rows'])}")
    print(f"  {'月/日':<7}{'曜日':<6}{'芝C':<4}{'クッション':>8}{'芝含水G/4':>14}{'ダ含水G/4':>14}")
    for r in result["rows"]:
        tw = f"{r['moist_turf_goal']}/{r['moist_turf_corner']}"
        dw = f"{r['moist_dirt_goal']}/{r['moist_dirt_corner']}"
        print(f"  {r['month']:>2}/{r['day']:<4}{r['dow']:<6}{r['turf_course']:<4}"
              f"{r['cushion_value']:>8}{tw:>14}{dw:>14}")


if __name__ == "__main__":
    main()
