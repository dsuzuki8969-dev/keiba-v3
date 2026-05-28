"""2025年 results.json ワイド払戻バグ修正スクリプト

バグ内容:
  2025年の results.json (source=race_results_db) では
  ワイドの3通りの払戻が全部「最初の払戻」に複製されている。

修正方法:
  キャッシュ HTML (lz4 圧縮) からワイドの正しい払戻を再パース。
  修正版を data/results_fixed/ に保存 (元ファイルは変更しない)。

使用方法:
  python scripts/fix_results_wide_2025.py
  python scripts/fix_results_wide_2025.py --month 2025-06  # 特定月のみ
  python scripts/fix_results_wide_2025.py --dry-run         # テスト実行
"""
import argparse
import glob
import json
import lz4.frame
import os
import re
import sys
import time
from collections import defaultdict

from bs4 import BeautifulSoup

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
FIXED_DIR = os.path.join(PROJECT_ROOT, "data", "results_fixed")
CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "cache")
DIAG_DIR = os.path.join(PROJECT_ROOT, "data", "_diag")


def load_lz4_html(path: str) -> str | None:
    """lz4 圧縮 HTML を読み込んで文字列で返す"""
    try:
        with open(path, "rb") as f:
            raw = lz4.frame.decompress(f.read())
        return raw.decode("utf-8", errors="ignore")
    except Exception as e:
        return None


def parse_wide_from_html(html: str) -> list[dict] | None:
    """HTML からワイド払戻 [{combo, payout}, ...] を解析して返す
    見つからない場合は None を返す"""
    soup = BeautifulSoup(html, "html.parser")

    for tr in soup.find_all("tr"):
        th = tr.find("th")
        if th and "ワイド" in th.get_text(strip=True):
            td_result = tr.find("td", class_="Result")
            td_payout = tr.find("td", class_="Payout")

            if not td_result or not td_payout:
                return None

            # コンボ抽出 (各 <ul> が 1 通り)
            combos = []
            for ul in td_result.find_all("ul"):
                spans = ul.find_all("span")
                nums = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
                if nums:
                    combos.append("-".join(nums))

            # 払戻抽出
            text = td_payout.get_text(separator="|")
            payouts = [
                int(n.replace(",", ""))
                for n in re.findall(r"[\d,]+", text)
                if n.replace(",", "").isdigit() and int(n.replace(",", "")) >= 100
            ]

            if not combos or not payouts:
                return None

            # コンボと払戻を対応付け (数が一致しない場合はスキップ)
            if len(combos) != len(payouts):
                # 払戻が多い場合 (人気数等が混入) はコンボ数に切り詰め
                payouts = payouts[:len(combos)]
            if len(combos) != len(payouts):
                return None

            return [{"combo": c, "payout": p} for c, p in zip(combos, payouts)]

    return None


def find_cache_html(race_id: str) -> str | None:
    """race_id に対応するキャッシュ HTML パスを返す (JRA / NAR 両対応)"""
    # JRA: race.netkeiba.com
    jra_path = os.path.join(
        CACHE_DIR,
        f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
    )
    if os.path.exists(jra_path):
        return jra_path

    # NAR: nar.netkeiba.com
    nar_path = os.path.join(
        CACHE_DIR,
        f"nar.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
    )
    if os.path.exists(nar_path):
        return nar_path

    return None


def is_wide_bug(wide_list: list) -> bool:
    """ワイドが 3 通りで全部同額 → バグと判定"""
    if not isinstance(wide_list, list) or len(wide_list) < 3:
        return False
    payouts = [w.get("payout") for w in wide_list]
    return payouts[0] == payouts[1] == payouts[2]


def fix_results_file(
    src_path: str,
    dst_path: str,
    dry_run: bool = False,
) -> dict:
    """1 ファイルを処理して修正版を書き出す。統計 dict を返す"""
    stats = {
        "total_races": 0,
        "bug_detected": 0,
        "fixed_ok": 0,
        "fixed_fail_no_cache": 0,
        "fixed_fail_parse_error": 0,
        "skip_no_wide": 0,
        "skip_normal": 0,
    }

    with open(src_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fixed_data = {}
    changed = False

    for race_id, race in data.items():
        stats["total_races"] += 1
        payouts = race.get("payouts", {})
        wide = payouts.get("ワイド")

        if not wide:
            # ワイドなし (3頭立て等)
            fixed_data[race_id] = race
            stats["skip_no_wide"] += 1
            continue

        if not is_wide_bug(wide):
            # 正常
            fixed_data[race_id] = race
            stats["skip_normal"] += 1
            continue

        # バグ確定 → キャッシュから修正
        stats["bug_detected"] += 1
        cache_path = find_cache_html(race_id)

        if not cache_path:
            fixed_data[race_id] = race
            stats["fixed_fail_no_cache"] += 1
            continue

        html = load_lz4_html(cache_path)
        if not html:
            fixed_data[race_id] = race
            stats["fixed_fail_parse_error"] += 1
            continue

        new_wide = parse_wide_from_html(html)
        if not new_wide:
            fixed_data[race_id] = race
            stats["fixed_fail_parse_error"] += 1
            continue

        # 修正適用
        import copy
        fixed_race = copy.deepcopy(race)
        fixed_race["payouts"]["ワイド"] = new_wide
        fixed_race["wide_fix_applied"] = True
        fixed_data[race_id] = fixed_race
        stats["fixed_ok"] += 1
        changed = True

    if not dry_run and changed:
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        with open(dst_path, "w", encoding="utf-8") as f:
            json.dump(fixed_data, f, ensure_ascii=False, separators=(",", ":"))

    return stats


def main():
    parser = argparse.ArgumentParser(description="2025年 results.json ワイド払戻バグ修正")
    parser.add_argument("--month", help="特定月のみ処理 (例: 2025-06)")
    parser.add_argument("--dry-run", action="store_true", help="修正ファイル書き出しを行わない")
    args = parser.parse_args()

    # 対象ファイル収集
    if args.month:
        month_str = args.month.replace("-", "")
        pattern = os.path.join(RESULTS_DIR, f"{month_str}*_results.json")
    else:
        pattern = os.path.join(RESULTS_DIR, "2025*_results.json")

    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[エラー] 対象ファイルなし: {pattern}")
        sys.exit(1)

    print(f"[開始] 対象ファイル数: {len(files)} (dry_run={args.dry_run})")
    print(f"  入力: {RESULTS_DIR}")
    print(f"  出力: {FIXED_DIR}")

    # 全体統計
    total = defaultdict(int)
    sample_checks = []  # サンプル検証用
    start_time = time.time()

    for i, src_path in enumerate(files):
        fname = os.path.basename(src_path)
        dst_path = os.path.join(FIXED_DIR, fname)

        stats = fix_results_file(src_path, dst_path, dry_run=args.dry_run)

        for k, v in stats.items():
            total[k] += v

        # 進捗バー
        elapsed = time.time() - start_time
        pct = (i + 1) / len(files) * 100
        bar_filled = int(pct / 5)
        bar = "#" * bar_filled + "." * (20 - bar_filled)
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(files) - i - 1) / rate if rate > 0 else 0
        print(
            f"\r[{bar}] {pct:5.1f}% ({i+1}/{len(files)}) "
            f"修正OK={total['fixed_ok']} バグ={total['bug_detected']} "
            f"ETA={eta:.0f}s",
            end="",
            flush=True,
        )

        # サンプル検証: 202506020101 があれば記録
        if "20250602" in fname:
            src_path_check = src_path
            dst_path_check = dst_path
            sample_checks.append((src_path_check, dst_path_check, fname))

    print()  # 改行

    elapsed = time.time() - start_time
    print(f"\n[完了] 所要時間: {elapsed:.1f}s")

    # 結果サマリ
    print("\n===== 修正サマリ =====")
    print(f"  処理ファイル数   : {len(files)}")
    print(f"  処理 race 数     : {total['total_races']}")
    print(f"  ワイドなし       : {total['skip_no_wide']}")
    print(f"  正常 (バグなし)  : {total['skip_normal']}")
    print(f"  バグ検出数       : {total['bug_detected']}")
    print(f"  修正成功         : {total['fixed_ok']}")
    print(f"  修正失敗(キャッシュなし): {total['fixed_fail_no_cache']}")
    print(f"  修正失敗(パースエラー) : {total['fixed_fail_parse_error']}")
    bug_rate = total['bug_detected'] / total['total_races'] * 100 if total['total_races'] > 0 else 0
    fix_rate = total['fixed_ok'] / total['bug_detected'] * 100 if total['bug_detected'] > 0 else 0
    print(f"  バグ率           : {bug_rate:.1f}%")
    print(f"  修正成功率       : {fix_rate:.1f}%")

    # サンプル検証 (202506020101)
    if not args.dry_run:
        print("\n===== サンプル検証 (202506020101) =====")
        sample_file = os.path.join(FIXED_DIR, "20250602_results.json")
        if os.path.exists(sample_file):
            with open(sample_file, "r", encoding="utf-8") as f:
                check_data = json.load(f)
            # JRAのrace_idは特殊: 202506020101 は JRA 中山1R
            # しかし results.json のキーは "202506020101" ではなく
            # venue_code 付きの形式の可能性を確認
            found = False
            for race_id, race in check_data.items():
                if race_id.endswith("020101") and "2025" in race_id[:4]:
                    wide = race.get("payouts", {}).get("ワイド", [])
                    print(f"  race_id: {race_id}")
                    for w in wide:
                        print(f"    {w['combo']} = {w['payout']}円")
                    ok = len(wide) == 3 and wide[0]["payout"] != wide[1]["payout"]
                    print(f"  -> {'OK (3通り別々)' if ok else 'NG (まだバグの可能性)'}")
                    found = True
                    break
            if not found:
                print("  202506020101 に相当する race_id が見つからなかった")
                # 先頭3件を表示
                for race_id, race in list(check_data.items())[:3]:
                    wide = race.get("payouts", {}).get("ワイド", [])
                    print(f"  race_id={race_id}, ワイド: {wide}")

    # レポート出力
    report_lines = [
        "# 2025年 results.json ワイド払戻 修正レポート\n",
        f"実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n",
        "## サマリ\n",
        f"- 処理ファイル数: {len(files)}\n",
        f"- 処理 race 数: {total['total_races']}\n",
        f"- バグ検出数: {total['bug_detected']} ({bug_rate:.1f}%)\n",
        f"- 修正成功数: {total['fixed_ok']}\n",
        f"- 修正失敗(キャッシュなし): {total['fixed_fail_no_cache']}\n",
        f"- 修正失敗(パースエラー): {total['fixed_fail_parse_error']}\n",
        f"- 正常 (バグなし): {total['skip_normal']}\n",
        f"- ワイドなし: {total['skip_no_wide']}\n",
        f"- 修正成功率: {fix_rate:.1f}%\n\n",
        "## 出力先\n",
        f"- `data/results_fixed/2025*.json` ({len(files)} ファイル)\n",
    ]

    os.makedirs(DIAG_DIR, exist_ok=True)
    report_path = os.path.join(DIAG_DIR, "wide_fix_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.writelines(report_lines)
    print(f"\nレポート出力: {report_path}")


if __name__ == "__main__":
    main()
