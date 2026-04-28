#!/usr/bin/env python3
"""
fix_pred_json_venue_names.py
============================
pred.json 内の race[].venue フィールドを race_id の真値 venue_code で修正する。

T-033 D-2: D-1e (relocate_pred_json_by_master.py) で race 配置は修正済みだが、
race["venue"] 名フィールドが旧ファイル由来の誤った会場名のままになっている。

例: 20240406_pred.json 内 race_id=202403010101 (福島 venue_code=03) が
    race["venue"]="札幌" と誤記 → "福島" に修正する (1,691件相当)

使用方法:
  python scripts/fix_pred_json_venue_names.py --dry-run   # 影響確認のみ（変更なし）
  python scripts/fix_pred_json_venue_names.py --apply     # 本実行
"""

import argparse
import json
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# パス定義
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = BASE_DIR / "data" / "predictions"
BAK_LABEL = "t033_d2_20260428"

# ─────────────────────────────────────────────
# venue_master から venue_code → venue_name マッピング構築
# audit_pred_venue.py の VENUE_CODE_TO_NAME 定義を参考
# ─────────────────────────────────────────────
# 逆引き: venue_code (str) → venue_name (str)
VENUE_CODE_TO_NAME: dict[str, str] = {
    # JRA 10場
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
    # NAR 14場
    "30": "門別",
    "35": "盛岡",
    "36": "水沢",
    "42": "浦和",
    "43": "船橋",
    "44": "大井",
    "45": "川崎",
    "46": "金沢",
    "47": "笠松",
    "48": "名古屋",
    "49": "園田",
    "50": "園田",   # race_id 上の別コード (SPAT4: 49 だが netkeiba race_id では 50)
    "51": "姫路",
    "52": "帯広",   # SPAT4互換 (netkeiba race_id では 65)
    "54": "高知",
    "55": "佐賀",
    "65": "帯広",   # netkeiba race_id 上の帯広コード
}

# JRA 場コード集合
JRA_VENUE_CODES: frozenset[str] = frozenset(
    ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
)


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパースする"""
    parser = argparse.ArgumentParser(
        description="pred.json の race[].venue フィールドを race_id の真値で修正する"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="影響確認のみ。ファイルは変更しない",
    )
    group.add_argument(
        "--apply",
        action="store_true",
        help="本実行。race[].venue を修正し上書き保存する",
    )
    return parser.parse_args()


def create_backup(predictions_dir: Path) -> Path:
    """
    data/predictions/ を data/predictions.bak_{BAK_LABEL}/ にコピーする。
    既存バックアップがある場合は数値サフィックスを付与して保護する。
    バックアップ失敗時は例外を送出（assert で abort させる）。
    """
    bak_base = predictions_dir.parent / f"predictions.bak_{BAK_LABEL}"
    bak_dir = bak_base
    i = 1
    while bak_dir.exists():
        bak_dir = Path(f"{bak_base}_{i}")
        i += 1

    print(f"[BACKUP] バックアップ作成中: {bak_dir} ...")
    shutil.copytree(str(predictions_dir), str(bak_dir))

    bak_files = list(bak_dir.glob("**/*"))
    bak_size_mb = sum(f.stat().st_size for f in bak_files if f.is_file()) / 1024 / 1024
    print(
        f"[BACKUP] 完了: {len(bak_files)} ファイル, {bak_size_mb:.1f} MB → {bak_dir.name}"
    )
    assert bak_dir.exists(), "バックアップディレクトリが存在しません。処理を中断します。"
    return bak_dir


def get_venue_code_from_race_id(race_id: str) -> str | None:
    """
    race_id (12桁) の [4:6] から venue_code を取得する。
    JRA/NAR 共通で race_id[4:6] が venue_code (T-037 修正確定構造)。
    """
    if not race_id or len(race_id) != 12 or not race_id.isdigit():
        return None
    return race_id[4:6]


def compute_is_jra(venue_code: str) -> bool:
    """venue_code が JRA 会場か判定する"""
    return venue_code in JRA_VENUE_CODES


def load_pred_files(predictions_dir: Path) -> list[Path]:
    """
    *_pred.json を全件取得する。
    _prev.json / _backup.json / .bak / bak ディレクトリ配下は除外する。
    """
    files = sorted(predictions_dir.glob("*_pred.json"))
    return [
        f for f in files
        if "bak" not in f.name
        and "backup" not in f.name
        and "_prev" not in f.name
    ]


def atomic_write_json(path: Path, data: dict) -> None:
    """アトミック書き込み: .tmp ファイルに書いてからリネーム"""
    tmp_path = path.with_suffix(".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        # Windows では rename 前に dst が存在すると失敗するので replace を使用
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def scan_mismatches(
    pred_files: list[Path],
    verbose: bool = False,
) -> list[dict]:
    """
    全 pred.json を走査し、race["venue"] と race_id 由来の venue_name が
    不一致な race を全件収集して返す。

    戻り値: [
        {
            "pred_file":   Path,
            "race_id":     str,
            "old_venue":   str,  # pred.json の現在の venue
            "new_venue":   str,  # race_id から算出した正しい venue
            "venue_code":  str,
        },
        ...
    ]
    """
    mismatches: list[dict] = []
    unknown_codes: list[dict] = []  # venue_master に未収載の venue_code

    for pred_path in pred_files:
        try:
            with open(pred_path, "rb") as f:
                raw = f.read()
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            print(f"[WARN] {pred_path.name} 読み込み失敗 ({e}) — スキップ", file=sys.stderr)
            continue

        for race in data.get("races", []):
            race_id = str(race.get("race_id", ""))
            current_venue = race.get("venue", "")

            venue_code = get_venue_code_from_race_id(race_id)
            if venue_code is None:
                # race_id 形式不正はスキップ（パターン B: 別スクリプトの管轄）
                continue

            correct_venue = VENUE_CODE_TO_NAME.get(venue_code)
            if correct_venue is None:
                # venue_master 未収載 → 推定で埋めない (feedback_no_easy_escape)
                unknown_codes.append({
                    "pred_file": pred_path,
                    "race_id": race_id,
                    "venue_code": venue_code,
                    "current_venue": current_venue,
                })
                if verbose:
                    print(
                        f"[SKIP] venue_code={venue_code} は venue_master 未収載。"
                        f" race_id={race_id} ({pred_path.name}) をスキップ"
                    )
                continue

            if current_venue != correct_venue:
                mismatches.append({
                    "pred_file": pred_path,
                    "race_id": race_id,
                    "old_venue": current_venue,
                    "new_venue": correct_venue,
                    "venue_code": venue_code,
                })

    return mismatches, unknown_codes


def apply_fixes(
    mismatches: list[dict],
) -> dict:
    """
    mismatches リストに基づいて pred.json を修正する。

    処理手順:
      1. pred_file ごとにグループ化
      2. JSON を読み込み
      3. 対象 race の "venue" と "is_jra" を修正
      4. アトミック書き込みで上書き

    戻り値: {
        "fixed_races": int,  # 修正した race 数
        "fixed_files": int,  # 修正したファイル数
        "errors": list[str],
    }
    """
    # pred_file ごとに修正対象 race_id をグループ化
    fixes_by_file: dict[Path, dict[str, dict]] = defaultdict(dict)
    for m in mismatches:
        fixes_by_file[m["pred_file"]][m["race_id"]] = {
            "new_venue": m["new_venue"],
            "venue_code": m["venue_code"],
        }

    fixed_races_total = 0
    fixed_files = 0
    errors: list[str] = []

    for pred_path, race_fixes in sorted(fixes_by_file.items()):
        try:
            with open(pred_path, "rb") as f:
                raw = f.read()
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            msg = f"{pred_path.name} 読み込み失敗: {e}"
            errors.append(msg)
            print(f"[ERROR] {msg}", file=sys.stderr)
            continue

        fixed_in_file = 0
        for race in data.get("races", []):
            race_id = str(race.get("race_id", ""))
            if race_id in race_fixes:
                fix = race_fixes[race_id]
                # race["venue"] のみ修正。race_id / horses[] 等は絶対変更しない
                race["venue"] = fix["new_venue"]
                # is_jra フラグも venue_code から再計算して整合させる
                race["is_jra"] = compute_is_jra(fix["venue_code"])
                fixed_in_file += 1

        if fixed_in_file == 0:
            # 既に修正済み (dry-run→apply 間に外部が修正した等) はスキップ
            continue

        try:
            atomic_write_json(pred_path, data)
            fixed_races_total += fixed_in_file
            fixed_files += 1
            print(f"[FIXED] {pred_path.name}: {fixed_in_file} races 修正")
        except Exception as e:
            msg = f"{pred_path.name} 書き込み失敗: {e}"
            errors.append(msg)
            print(f"[ERROR] {msg}", file=sys.stderr)

    return {
        "fixed_races": fixed_races_total,
        "fixed_files": fixed_files,
        "errors": errors,
    }


def main() -> None:
    args = parse_args()
    t_start = time.time()

    print("=" * 65)
    print("fix_pred_json_venue_names.py")
    print(f"モード: {'DRY-RUN' if args.dry_run else '本実行 (--apply)'}")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ─────────────────────────────────
    # Step 1: バックアップ取得
    # ─────────────────────────────────
    print("\n[Step 1/4] バックアップ取得")
    if args.apply:
        # 本実行のみバックアップを作成（dry-run では変更がないため不要）
        bak_dir = create_backup(PREDICTIONS_DIR)
    else:
        print("  DRY-RUN モードのためバックアップをスキップ")
        bak_dir = None

    # ─────────────────────────────────
    # Step 2: pred.json 一覧取得
    # ─────────────────────────────────
    print("\n[Step 2/4] pred.json ファイル一覧取得")
    pred_files = load_pred_files(PREDICTIONS_DIR)
    print(f"  対象ファイル数: {len(pred_files)}")

    # ─────────────────────────────────
    # Step 3: 不整合スキャン
    # ─────────────────────────────────
    print("\n[Step 3/4] race[].venue 不整合スキャン中 ...")
    mismatches, unknown_codes = scan_mismatches(pred_files, verbose=False)

    # 影響ファイル数を算出
    affected_files = len(set(m["pred_file"] for m in mismatches))

    print(f"\n  ─ スキャン結果 ─")
    print(f"  不整合 race 数     : {len(mismatches):,} 件")
    print(f"  影響 pred.json 数  : {affected_files:,} ファイル")
    print(f"  venue_master 未収載: {len(unknown_codes):,} 件 (スキップ)")

    # 修正サンプル最大 20 件表示
    print(f"\n  ─ 修正サンプル (最大 20 件) ─")
    sample_count = min(20, len(mismatches))
    for i, m in enumerate(mismatches[:sample_count]):
        print(
            f"  [{i+1:02d}] {m['pred_file'].name} | race_id={m['race_id']} "
            f"| {m['old_venue']} → {m['new_venue']} (code={m['venue_code']})"
        )
    if len(mismatches) > 20:
        print(f"  ... 他 {len(mismatches) - 20:,} 件省略")

    # ─────────────────────────────────
    # DRY-RUN はここで停止
    # ─────────────────────────────────
    if args.dry_run:
        print("\n" + "=" * 65)
        print("DRY-RUN 完了。影響件数のみ確認しました。変更は行っていません。")
        print("本実行するには --apply を使用してください。")
        print("=" * 65)
        return

    # ─────────────────────────────────
    # Step 4: 本実行
    # ─────────────────────────────────
    print(f"\n[Step 4/4] race[].venue 修正実行 ({len(mismatches):,} 件) ...")
    result = apply_fixes(mismatches)

    elapsed = time.time() - t_start

    print("\n" + "=" * 65)
    print("最終サマリ")
    print("=" * 65)
    print(f"  バックアップ       : {bak_dir.name if bak_dir else '—'}")
    print(f"  修正 race 数       : {result['fixed_races']:,} 件")
    print(f"  修正ファイル数     : {result['fixed_files']:,} ファイル")
    print(f"  venue_master 未収載: {len(unknown_codes):,} 件 (スキップ)")
    print(f"  エラー件数         : {len(result['errors'])} 件")
    print(f"  経過時間           : {elapsed:.1f} 秒")

    if result["errors"]:
        print("\n[ERROR] エラー詳細:")
        for e in result["errors"]:
            print(f"  {e}")

    ok = len(result["errors"]) == 0
    print(f"\n  全体ステータス: {'[OK] 正常完了' if ok else '[NG] 要確認'}")
    print("=" * 65)
    print("\n次のコマンドで audit を再実行して パターン D = 0 件 を確認してください:")
    print(
        "  PYTHONIOENCODING=utf-8 python scripts/audit_pred_venue.py 2>&1 "
        '| grep -E "パターン[A-D]"'
    )

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
