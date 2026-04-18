"""調教データを data/training_ml/YYYYMMDD.json から pred.json に直接注入

経緯:
  DAI_Keiba_Predict (06:00) が 4/13 以降未実行で、4/17夜の公式モード予想には調教データが乗らなかった。
  事後に `python main.py --collect_training` で KB からスクレイピングして
  data/training_ml/YYYYMMDD.json は作成済み。これを pred.json に直接マージする。

処理:
  1. 該当日の training_ml/YYYYMMDD.json を読み込み
  2. data/predictions/YYYYMMDD_pred.json の各レース・各馬を race_id + horse_name で突合
  3. training_records フィールドに全件 dict を注入
  4. training_intensity も先頭レコードから生成（course / intensity / sigma）

使い方:
  python scripts/inject_training_into_pred.py 2026-04-18
  python scripts/inject_training_into_pred.py 2026-04-18 --dry-run
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = ROOT / "data" / "predictions"
ML_DIR = ROOT / "data" / "training_ml"


def _round2(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None


def _extract_intensity(records: list) -> dict | None:
    """training_records の先頭から training_intensity オブジェクトを生成"""
    if not records:
        return None
    best = records[0]
    course = best.get("course", "") or ""
    intensity = best.get("intensity_label", "") or ""
    sigma = _round2(best.get("sigma_from_mean"))
    # どれか一つでも入っていれば返す
    if not course and not intensity and sigma is None:
        return None
    return {
        "course": course,
        "intensity": intensity,
        "sigma": sigma,
    }


def main():
    parser = argparse.ArgumentParser(description="調教データを pred.json に注入")
    parser.add_argument("date", help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--dry-run", action="store_true", help="書き込みせず統計のみ")
    args = parser.parse_args()

    dkey = args.date.replace("-", "")
    ml_path = ML_DIR / f"{dkey}.json"
    pred_path = PRED_DIR / f"{dkey}_pred.json"

    if not ml_path.exists():
        print(f"[ERROR] {ml_path} が見つかりません")
        sys.exit(1)
    if not pred_path.exists():
        print(f"[ERROR] {pred_path} が見つかりません")
        sys.exit(1)

    with open(ml_path, "r", encoding="utf-8") as f:
        mldata = json.load(f)
    with open(pred_path, "r", encoding="utf-8") as f:
        pdata = json.load(f)

    # race_id → { horse_name: [records] } の辞書を構築
    ml_index: dict[str, dict[str, list]] = {}
    for r in mldata.get("races", []):
        rid = str(r.get("race_id", ""))
        if not rid:
            continue
        ml_index[rid] = r.get("training", {}) or {}

    print(f"=== 調教データ注入 {args.date} ===")
    print(f"  調教JSONレース数: {len(ml_index)}")
    print(f"  pred.jsonレース数: {len(pdata.get('races', []))}")

    matched_races = 0
    injected_horses = 0
    missing_races = []
    missing_horses = 0

    for race in pdata.get("races", []):
        rid = str(race.get("race_id", ""))
        training_map = ml_index.get(rid)
        if training_map is None:
            # JRA 以外（NAR）は training_ml に無いので無視
            venue = race.get("venue", "")
            if venue in ("中山", "阪神", "福島", "東京", "京都", "中京", "新潟", "小倉", "札幌", "函館"):
                missing_races.append(f"{venue}{race.get('race_no')}R ({rid})")
            continue
        matched_races += 1

        horses = race.get("horses", [])
        race_injected = 0
        race_missing = 0
        for h in horses:
            hname = h.get("horse_name", "") or ""
            if not hname:
                continue
            recs = training_map.get(hname)
            if not recs:
                race_missing += 1
                continue
            # records をそのまま書き込み（keys 完全一致）
            h["training_records"] = recs
            # training_intensity が未設定なら先頭レコードから生成
            if not h.get("training_intensity"):
                ti = _extract_intensity(recs)
                if ti:
                    h["training_intensity"] = ti
            race_injected += 1

        injected_horses += race_injected
        missing_horses += race_missing
        print(f"  {race.get('venue')}{race.get('race_no'):>2}R ({rid}): "
              f"注入={race_injected}/{len(horses)} 未取得={race_missing}")

    print()
    print(f"  マッチしたレース : {matched_races}/{len(ml_index)}")
    print(f"  注入した馬数合計 : {injected_horses}")
    print(f"  未取得馬数合計   : {missing_horses}")
    if missing_races:
        print(f"  JRA pred 側のみのレース: {len(missing_races)}件")
        for m in missing_races[:5]:
            print(f"    - {m}")

    if args.dry_run:
        print("[DRY-RUN モード: 書き込みなし]")
        return

    # バックアップ
    bak_path = pred_path.with_suffix(".json.bak_before_training")
    if not bak_path.exists():
        bak_path.write_bytes(pred_path.read_bytes())
        print(f"  バックアップ: {bak_path.name}")

    with open(pred_path, "w", encoding="utf-8") as f:
        json.dump(pdata, f, ensure_ascii=False, indent=2)
    print(f"  保存: {pred_path.name}")
    print(f"=== 完了 ===")


if __name__ == "__main__":
    main()
