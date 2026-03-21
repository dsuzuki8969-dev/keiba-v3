"""
バッチ再分析完了後の自動検証・ダッシュボード再起動スクリプト

使い方:
  python scripts/post_reanalysis.py

動作:
  1. batch_reanalyze.py の完了を待機（プロセス監視）
  2. 全pred.jsonの脚質分布を検証（逃げ0頭・追込過半のレースがないか）
  3. daily cacheをクリア（新pred.jsonで再照合させる）
  4. evaluate_kpi.py で全期間＋年別KPIを出力
  5. ダッシュボードを再起動
"""
import datetime
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from config.settings import PREDICTIONS_DIR


def _is_batch_running():
    """batch_reanalyze.py が稼働中かどうかをWMICで確認"""
    try:
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "CommandLine"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=30,
        )
        return "batch_reanalyze" in result.stdout
    except Exception:
        return True  # 確認失敗時は稼働中とみなす（安全側）


def wait_for_reanalysis():
    """batch_reanalyze.py プロセスの完了を待機"""
    print(f"[{_now()}] batch_reanalyze.py の完了を待機中...", flush=True)
    # 初回チェック: そもそも走っていなければ完了済み or 未起動
    if not _is_batch_running():
        # pred.jsonの更新状況で判断（90%以上更新済みなら完了とみなす）
        total, updated = _count_progress()
        if total > 0 and updated / total >= 0.90:
            print(f"[{_now()}] batch_reanalyze.py は既に完了済み ({updated}/{total})", flush=True)
            return True
        else:
            print(f"[{_now()}] batch_reanalyze.py が未稼働 ({updated}/{total}更新済み) — 完了を待機", flush=True)
    while True:
        if not _is_batch_running():
            total, updated = _count_progress()
            print(f"\n[{_now()}] batch_reanalyze.py 完了を検出 ({updated}/{total})", flush=True)
            return True
        # 進捗表示
        total, updated = _count_progress()
        pct = updated / total * 100 if total > 0 else 0
        print(f"[{_now()}] 進捗: {updated}/{total} ({pct:.1f}%)", flush=True)
        time.sleep(120)  # 2分間隔


def _count_progress():
    """更新済みpred.jsonの数をカウント"""
    cutoff = datetime.datetime(2026, 3, 18, 21, 0)
    total = 0
    updated = 0
    for f in os.listdir(PREDICTIONS_DIR):
        if f.endswith("_pred.json") and "backup" not in f:
            total += 1
            mtime = datetime.datetime.fromtimestamp(
                os.path.getmtime(os.path.join(PREDICTIONS_DIR, f))
            )
            if mtime >= cutoff:
                updated += 1
    return total, updated


def validate_predictions():
    """全pred.jsonの脚質分布を検証"""
    print(f"\n[{_now()}] === 脚質分布検証 ===")
    total_files = 0
    total_races = 0
    ng_count = 0
    style_counter = Counter()

    for fname in sorted(os.listdir(PREDICTIONS_DIR)):
        if not fname.endswith("_pred.json") or "backup" in fname:
            continue
        total_files += 1
        fpath = os.path.join(PREDICTIONS_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                pred = json.load(f)
        except Exception:
            print(f"  WARN: {fname} の読み込み失敗")
            continue

        for race in pred.get("races", []):
            total_races += 1
            horses = race.get("horses", [])
            styles = [h.get("running_style", "") for h in horses]
            for s in styles:
                if s:
                    style_counter[s] += 1
            n_nige = styles.count("逃げ")
            n_oikomi = styles.count("追込")
            if n_nige == 0 and len(horses) >= 4:
                ng_count += 1
            if len(horses) > 0 and n_oikomi / len(horses) > 0.5:
                ng_count += 1

    print(f"  ファイル数: {total_files}")
    print(f"  総レース数: {total_races}")
    print(f"  脚質分布:")
    for s, c in style_counter.most_common():
        print(f"    {s}: {c} ({c/sum(style_counter.values())*100:.1f}%)")
    print(f"  問題レース: {ng_count}件")

    if ng_count > total_races * 0.01:  # 1%以上は異常
        print(f"  NG: 問題レース率が高すぎます ({ng_count}/{total_races})")
        return False
    print(f"  OK: 検証パス")
    return True


def clear_daily_cache():
    """daily cacheを全削除（新pred.jsonで再照合させる）"""
    cache_dir = os.path.join(os.path.dirname(PREDICTIONS_DIR), "cache", "daily_agg")
    if not os.path.isdir(cache_dir):
        # 別の場所を探す
        from src.results_tracker import _DAILY_CACHE_DIR
        cache_dir = _DAILY_CACHE_DIR

    if os.path.isdir(cache_dir):
        count = len([f for f in os.listdir(cache_dir) if f.endswith(".json")])
        shutil.rmtree(cache_dir)
        os.makedirs(cache_dir, exist_ok=True)
        print(f"[{_now()}] daily cache クリア: {count}件削除")
    else:
        print(f"[{_now()}] daily cache ディレクトリ不在（スキップ）")


def run_kpi_evaluation():
    """evaluate_kpi.py を実行してKPIを出力"""
    print(f"\n[{_now()}] === KPI評価 ===")
    kpi_script = os.path.join(os.path.dirname(__file__), "evaluate_kpi.py")
    if not os.path.exists(kpi_script):
        print(f"  WARN: {kpi_script} が見つかりません（スキップ）")
        return
    result = subprocess.run(
        [sys.executable, kpi_script],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=600,
    )
    print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    if result.returncode != 0:
        print(f"  WARN: KPI評価がエラー終了 (code={result.returncode})")
        if result.stderr:
            print(result.stderr[-500:])


def restart_dashboard():
    """ダッシュボードプロセスを再起動（Flask直接起動）"""
    print(f"\n[{_now()}] === ダッシュボード再起動 ===", flush=True)
    # 既存のdashboard.pyプロセスを停止
    try:
        result = subprocess.run(
            ["wmic", "process", "where",
             "name='python.exe' and CommandLine like '%dashboard.py%'",
             "get", "ProcessId"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=15,
        )
        for line in result.stdout.strip().splitlines():
            pid = line.strip()
            if pid.isdigit():
                subprocess.run(["taskkill", "/PID", pid, "/F"],
                               capture_output=True, timeout=10)
    except Exception:
        pass
    # Flask直接起動
    dashboard_py = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "src", "dashboard.py"
    )
    subprocess.Popen(
        [sys.executable, "-u", dashboard_py],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )
    print(f"[{_now()}] ダッシュボード起動完了 (port 5051)", flush=True)


def _now():
    return datetime.datetime.now().strftime("%H:%M:%S")


def main():
    print(f"[{_now()}] === post_reanalysis.py 開始 ===")

    # 1. 再分析完了待ち
    wait_for_reanalysis()

    # 2. 検証
    ok = validate_predictions()

    # 3. daily cacheクリア
    clear_daily_cache()

    # 4. KPI評価
    run_kpi_evaluation()

    # 5. ダッシュボード再起動
    if ok:
        restart_dashboard()
        print(f"\n[{_now()}] === 全工程完了 ===")
    else:
        print(f"\n[{_now()}] === 検証失敗: ダッシュボード再起動をスキップ ===")
        print("手動で確認してください")


if __name__ == "__main__":
    main()
