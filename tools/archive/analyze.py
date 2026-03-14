"""
競馬解析マスターシステム v3.0 - 対話式レース分析

使い方:
  python analyze.py

対話でレースIDを入力すると、main.run_analysis を呼び出して
HTML形式の予想を出力します。
README / QUICKSTART で案内している「レース分析」の入口です。
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import run_analysis


def main():
    print("\n[競馬解析マスターシステム v3.0] レース分析")
    print("   netkeiba のレースURLから race_id（12桁）をコピーしてください")
    print("   例: .../race/result.html?race_id=202506021011 → 202506021011\n")

    while True:
        try:
            race_id = input("レースIDを入力してください: ").strip().replace("-", "").replace(" ", "")
        except (EOFError, KeyboardInterrupt):
            print("\n終了します。")
            sys.exit(0)

        if not race_id:
            print("   race_id を入力してください。")
            continue

        if len(race_id) != 12 or not race_id.isdigit():
            print("   race_id は12桁の数字です。")
            continue

        break

    run_analysis(race_id, output_dir="output", open_browser=False)


if __name__ == "__main__":
    main()
