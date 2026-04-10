"""
既存の予測JSONにtraining_devをバックフィルするスクリプト。

run_analysis_date.py を再実行せずに、TrainingFeatureExtractorを使って
training_devを算出し、予測JSONに追記する。
"""
import json
import os
import sys
import glob
from pathlib import Path

# プロジェクトルート
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.log import get_logger

logger = get_logger(__name__)


def patch_pred_files(target_dates=None):
    """
    予測JSONにtraining_devをバックフィルする。

    Args:
        target_dates: 対象日付リスト (例: ["20260402"]). Noneなら全ファイル。
    """
    from src.ml.training_features import TrainingFeatureExtractor, calc_training_dev

    # TrainingFeatureExtractorロード
    print("TrainingFeatureExtractorをロード中...")
    ext = TrainingFeatureExtractor()
    ext.load_all()
    print("  完了")

    pred_dir = os.path.join(PROJECT_ROOT, "data", "predictions")
    if target_dates:
        files = [os.path.join(pred_dir, f"{d}_pred.json") for d in target_dates]
        files = [f for f in files if os.path.exists(f)]
    else:
        files = sorted(glob.glob(os.path.join(pred_dir, "*_pred.json")))

    if not files:
        print("対象ファイルなし")
        return

    patched_count = 0
    skipped_count = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  {fname}: JSON解析エラー → スキップ ({e})")
            skipped_count += 1
            continue

        races = data.get("races", [])
        if not races:
            print(f"  {fname}: レースなし → スキップ")
            skipped_count += 1
            continue

        file_modified = False
        for race in races:
            horses = race.get("horses", [])
            if not horses:
                continue

            race_id = race.get("race_id", "")
            race_date = data.get("date", "")

            # 既にtraining_devが全馬にあるならスキップ
            has_any = any(h.get("training_dev") is not None for h in horses)
            if has_any:
                continue

            # 馬名リスト（キーは horse_name）
            horse_names = [h.get("horse_name", "") for h in horses if h.get("horse_name")]
            if len(horse_names) < 3:
                continue

            # 調教特徴量取得
            try:
                feat_map = ext.get_race_training_features(race_id, horse_names, race_date)
                if not feat_map:
                    continue

                dev_map = calc_training_dev(feat_map)

                # JSONに書き込み
                for h in horses:
                    hname = h.get("horse_name", "")
                    if hname in dev_map and dev_map[hname] is not None:
                        h["training_dev"] = round(max(20.0, min(100.0, dev_map[hname])), 1)
                        file_modified = True
                    else:
                        if "training_dev" not in h:
                            h["training_dev"] = None

            except Exception as e:
                logger.debug("レース %s の調教偏差値算出エラー: %s", race_id, e)
                continue

        if file_modified:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            patched_count += 1
            # パッチ後のtraining_dev分布を表示
            all_devs = []
            for race in races:
                for h in race.get("horses", []):
                    td = h.get("training_dev")
                    if td is not None:
                        all_devs.append(td)
            if all_devs:
                print(f"  {fname}: パッチ完了 - training_dev {len(all_devs)}頭, "
                      f"範囲 {min(all_devs):.1f}-{max(all_devs):.1f}, "
                      f"平均 {sum(all_devs)/len(all_devs):.1f}")
            else:
                print(f"  {fname}: パッチ完了（training_dev算出なし）")
        else:
            skipped_count += 1

    print(f"\n完了: パッチ {patched_count}件, スキップ {skipped_count}件")


if __name__ == "__main__":
    # コマンドライン引数があれば対象日付として使う
    if len(sys.argv) > 1:
        dates = sys.argv[1:]
    else:
        # デフォルト: 直近7日分
        from datetime import datetime, timedelta
        today = datetime.now()
        dates = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(7)]

    print(f"対象日付: {dates}")
    patch_pred_files(dates)
