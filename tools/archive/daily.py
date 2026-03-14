"""
競馬解析マスターシステム v3.0 - 日付別全レース分析（メイン入口）

使い方:
  python daily.py

日付を入力するだけで、その日の中央競馬・地方競馬の全レースを分析し、
1つのHTML（競馬場タブ + 1R～12Rタブ）で全て見られます。
"""

import sys
import os
import re
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import run_date_analysis


def parse_date(s: str) -> Optional[str]:
    """ YYYY-MM-DD 形式に正規化。None=無効 """
    s = s.strip().replace("/", "-").replace(".", "-")
    # YYYYMMDD → YYYY-MM-DD
    if re.match(r"^\d{8}$", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", s):
        parts = s.split("-")
        return f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    return None


def main():
    print("\n[競馬解析マスターシステム v3.0] 日付別全レース分析")
    print("   日付を入力すると、その日の全レースを分析して1つのHTMLにまとめます")
    print("   例: 2025-03-15 または 20250315\n")

    today = datetime.now().strftime("%Y-%m-%d")

    while True:
        try:
            s = input(f"日付を入力してください (Enter={today}): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n終了します。")
            sys.exit(0)

        if not s:
            s = today

        date = parse_date(s)
        if not date:
            print("   日付は YYYY-MM-DD または YYYYMMDD で入力してください。")
            continue

        # 妥当な日付か確認
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            print(f"   無効な日付です: {date}")
            continue

        break

    run_date_analysis(date, output_dir="output")


if __name__ == "__main__":
    main()
