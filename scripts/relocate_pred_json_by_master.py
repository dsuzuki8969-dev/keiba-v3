"""
relocate_pred_json_by_master.py
================================
pred.json 内の各 race を真値マスタ (race_id_date_master.json) で照合し、
汚染日付の pred.json から race を取り出して正しい日付の pred.json に再配置する。

T-033 Phase 2 (D-1e) — bulk_backfill_predictions.py の数時間タスクを回避する軽量修正。

使用方法:
  python scripts/relocate_pred_json_by_master.py --dry-run   # 影響予測のみ
  python scripts/relocate_pred_json_by_master.py --apply     # 本実行
  python scripts/relocate_pred_json_by_master.py --apply --verbose  # 詳細ログ付き
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────
# パス定義
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = BASE_DIR / "data" / "predictions"
MASTER_PATH = BASE_DIR / "data" / "masters" / "race_id_date_master.json"


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパースする"""
    parser = argparse.ArgumentParser(
        description="pred.json を真値マスタで正しい日付に再配置する"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run", action="store_true", help="バックアップ取得のみ。影響予測を表示して終了"
    )
    group.add_argument(
        "--apply", action="store_true", help="本実行。実際に pred.json を再配置する"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="詳細ログを出力する"
    )
    return parser.parse_args()


def load_master(path: Path) -> dict[str, str]:
    """
    race_id_date_master.json を読み込み {race_id: date_str} の辞書を返す。
    date_str は 'YYYY-MM-DD' 形式。
    """
    if not path.exists():
        print(f"[ERROR] マスタファイルが見つかりません: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # 構造: {"version": ..., "mapping": {"race_id": "YYYY-MM-DD", ...}}
    if "mapping" in raw:
        mapping = raw["mapping"]
    else:
        # フォールバック: トップレベルが直接 {race_id: date} 辞書の場合
        mapping = {k: v for k, v in raw.items() if not k.startswith("_") and isinstance(v, str)}
    print(f"[INFO] 真値マスタ読み込み完了: {len(mapping):,} エントリ")
    return mapping


def date_str_to_filename(date_str: str) -> str:
    """'YYYY-MM-DD' → 'YYYYMMDD_pred.json'"""
    return date_str.replace("-", "") + "_pred.json"


def filename_to_date_str(filename: str) -> str | None:
    """'YYYYMMDD_pred.json' → 'YYYY-MM-DD'。失敗時は None"""
    stem = Path(filename).stem  # 'YYYYMMDD_pred'
    parts = stem.split("_")
    if parts and len(parts[0]) == 8 and parts[0].isdigit():
        d = parts[0]
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return None


def load_pred_files(predictions_dir: Path) -> list[dict]:
    """
    *_pred.json を全件読み込み、以下のリストを返す。
    _prev.json / _backup.json / .bak 等は除外する。

    戻り値: [
        {
            "path": Path,
            "original_date": "YYYY-MM-DD",
            "data": {date, version, races, ...},
        },
        ...
    ]
    """
    results = []
    for p in sorted(predictions_dir.glob("*_pred.json")):
        # バックアップ系を除外
        name = p.name
        if any(x in name for x in ("_prev", "_backup", ".bak")):
            continue
        orig_date = filename_to_date_str(name)
        if orig_date is None:
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[WARN] {p.name} 読み込み失敗 ({e}) — スキップ", file=sys.stderr)
            continue
        if "races" not in data:
            continue
        results.append({"path": p, "original_date": orig_date, "data": data})
    return results


def create_backup(predictions_dir: Path, dry_run: bool) -> Path | None:
    """
    data/predictions/ を data/predictions.bak_t033_YYYYMMDD/ にコピーする。
    既存バックアップがある場合は数値サフィックスを付与して保護する。
    dry_run=True でもバックアップは作成する（影響範囲確認前に安全を確保）。
    """
    today = datetime.now().strftime("%Y%m%d")
    bak_base = predictions_dir.parent / f"predictions.bak_t033_{today}"
    bak_dir = bak_base
    i = 1
    while bak_dir.exists():
        bak_dir = Path(f"{bak_base}_{i}")
        i += 1

    print(f"[BACKUP] バックアップ作成中: {bak_dir} ...")
    shutil.copytree(str(predictions_dir), str(bak_dir))

    # ファイル数とサイズを確認
    bak_files = list(bak_dir.glob("**/*"))
    bak_size_mb = sum(f.stat().st_size for f in bak_files if f.is_file()) / 1024 / 1024
    print(f"[BACKUP] 完了: {len(bak_files)} ファイル, {bak_size_mb:.1f} MB → {bak_dir.name}")
    return bak_dir


def classify_races(
    pred_files: list[dict],
    master: dict[str, str],
    verbose: bool = False,
) -> tuple[dict, dict, int]:
    """
    全 pred.json の race を真値マスタで分類する。

    戻り値:
        race_by_true_date: {true_date_str: [race_dict, ...]} — 正しい date 別 race リスト
        meta_by_date: {date_str: {keys: values}} — メタフィールド（初出ファイルから）
        dup_count: 重複 race_id の件数
    """
    # {race_id: (true_date, race_dict, original_date)} の辞書（重複検出用）
    seen_races: dict[str, tuple[str, dict, str]] = {}
    dup_count = 0

    # メタフィールド: 各 original_date のファイルからメタを引き継ぐ
    meta_by_date: dict[str, dict] = {}

    for file_info in pred_files:
        orig_date = file_info["original_date"]
        data = file_info["data"]

        # メタフィールド保存（races 以外のキー）
        if orig_date not in meta_by_date:
            meta_by_date[orig_date] = {
                k: v for k, v in data.items() if k != "races"
            }

        for race in data.get("races", []):
            race_id = race.get("race_id")
            if not race_id:
                continue

            # 真値マスタで date を解決
            if race_id in master:
                true_date = master[race_id]
            else:
                # マスタにない（NAR 等）→ original_date を真値とみなす
                true_date = orig_date
                if verbose:
                    print(f"[FALLBACK] race_id={race_id} マスタ未登録 → original_date={orig_date} を採用")

            # 重複 race_id 検出
            if race_id in seen_races:
                prev_true_date, prev_race, prev_orig = seen_races[race_id]
                dup_count += 1
                # 真値マスタで解決済み date を優先。それが同じなら original_date が合っている方を優先
                if true_date == prev_true_date:
                    # 同一解決 date → original_date が真値と一致する方を採用
                    if orig_date == true_date:
                        seen_races[race_id] = (true_date, race, orig_date)
                    # else: 先着の prev_orig を維持
                else:
                    # 真値 date が異なる → 後勝ちで上書き（マスタ優先の原則）
                    if verbose:
                        print(
                            f"[DUP] race_id={race_id}: prev_date={prev_true_date}({prev_orig})"
                            f" vs new_date={true_date}({orig_date}) → new を採用"
                        )
                    seen_races[race_id] = (true_date, race, orig_date)
            else:
                seen_races[race_id] = (true_date, race, orig_date)

    # true_date 別に集約
    race_by_true_date: dict[str, list[dict]] = defaultdict(list)
    for race_id, (true_date, race_dict, _orig) in seen_races.items():
        race_by_true_date[true_date].append(race_dict)

    # race_id 順でソート
    for date_str in race_by_true_date:
        race_by_true_date[date_str].sort(key=lambda r: r.get("race_id", ""))

    return dict(race_by_true_date), meta_by_date, dup_count


def compute_diff(
    pred_files: list[dict],
    race_by_true_date: dict[str, list[dict]],
) -> dict:
    """
    現状の pred.json と再配置後の差分を計算する。

    戻り値: {
        "moved_races": int,         # 移動が必要な race 数
        "affected_src_dates": set,  # 移動元となる date
        "affected_dst_dates": set,  # 移動先となる date
        "delete_dates": set,        # 再配置後に空になる date（ファイル削除対象）
        "new_dates": set,           # 新規作成が必要な date
    }
    """
    # 現状: {date: set(race_id)}
    current: dict[str, set] = {}
    for fi in pred_files:
        orig_date = fi["original_date"]
        current[orig_date] = {r.get("race_id") for r in fi["data"].get("races", []) if r.get("race_id")}

    # 再配置後: {date: set(race_id)}
    after: dict[str, set] = {d: {r.get("race_id") for r in races} for d, races in race_by_true_date.items()}

    moved_races = 0
    affected_src = set()
    affected_dst = set()

    for date_str, true_races in after.items():
        if date_str in current:
            orig_races = current[date_str]
            added = true_races - orig_races
            removed = orig_races - true_races
            if added or removed:
                moved_races += len(added)
                if added:
                    affected_dst.add(date_str)
                if removed:
                    affected_src.add(date_str)
        else:
            # 新規 date
            moved_races += len(true_races)
            affected_dst.add(date_str)

    # 削除対象: 現状ファイルがある date で再配置後に race が 0 件
    delete_dates = set(current.keys()) - set(after.keys())
    new_dates = set(after.keys()) - set(current.keys())

    return {
        "moved_races": moved_races,
        "affected_src_dates": affected_src,
        "affected_dst_dates": affected_dst,
        "delete_dates": delete_dates,
        "new_dates": new_dates,
    }


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


def apply_relocation(
    predictions_dir: Path,
    pred_files: list[dict],
    race_by_true_date: dict[str, list[dict]],
    meta_by_date: dict[str, dict],
    diff: dict,
    verbose: bool = False,
) -> dict:
    """
    pred.json を再配置する本処理。

    戻り値: {
        "written": int,
        "deleted": int,
        "created": int,
        "errors": list[str],
    }
    """
    written = 0
    deleted = 0
    created = 0
    errors: list[str] = []

    # 書き込み対象 date の予想ファイルを構築
    for date_str, races in sorted(race_by_true_date.items()):
        pred_filename = date_str_to_filename(date_str)
        pred_path = predictions_dir / pred_filename

        # メタフィールドを取得（同 date が既存なら既存の meta、なければ最も近い date から借用）
        if date_str in meta_by_date:
            meta = dict(meta_by_date[date_str])
        else:
            # 新規 date: meta なし → 最低限の構造
            meta = {"version": 2}

        # date は true_date に更新
        meta["date"] = date_str
        meta["races"] = races

        # キー順: date, version, races, その他
        ordered = {}
        for key in ("date", "version", "races"):
            if key in meta:
                ordered[key] = meta[key]
        for key, val in meta.items():
            if key not in ordered:
                ordered[key] = val

        is_new = not pred_path.exists()
        try:
            atomic_write_json(pred_path, ordered)
            if is_new:
                created += 1
                if verbose:
                    print(f"[CREATE] {pred_filename} ({len(races)} races)")
            else:
                written += 1
                if verbose:
                    print(f"[WRITE]  {pred_filename} ({len(races)} races)")
        except Exception as e:
            msg = f"書き込み失敗: {pred_filename} — {e}"
            errors.append(msg)
            print(f"[ERROR] {msg}", file=sys.stderr)

    # 削除対象: race_by_true_date に含まれない元ファイル
    for date_str in diff["delete_dates"]:
        pred_filename = date_str_to_filename(date_str)
        pred_path = predictions_dir / pred_filename
        if pred_path.exists():
            try:
                pred_path.unlink()
                deleted += 1
                if verbose:
                    print(f"[DELETE] {pred_filename} (race 0件のため削除)")
            except Exception as e:
                msg = f"削除失敗: {pred_filename} — {e}"
                errors.append(msg)
                print(f"[ERROR] {msg}", file=sys.stderr)

    return {"written": written, "deleted": deleted, "created": created, "errors": errors}


def verify_results(predictions_dir: Path, master: dict[str, str], verbose: bool = False) -> dict:
    """
    再配置後の検証を実施する。

    1. 20260101_pred.json の venue 一覧確認 (期待: NAR のみ)
    2. 20260131_pred.json の venue 一覧確認 (期待: JRA 含む)
    3. 全 pred.json で race_id ↔ file date の整合性チェック
    """
    print("\n" + "=" * 60)
    print("検証フェーズ")
    print("=" * 60)

    # ──────────────────
    # 1. 20260101 確認
    # ──────────────────
    result_0101: dict = {}
    p0101 = predictions_dir / "20260101_pred.json"
    if p0101.exists():
        with open(p0101, "r", encoding="utf-8") as f:
            d0101 = json.load(f)
        venues_0101 = sorted(set(r.get("venue", "?") for r in d0101.get("races", [])))
        is_jra_list = [r.get("is_jra", False) for r in d0101.get("races", [])]
        has_jra = any(is_jra_list)
        result_0101 = {
            "race_count": len(d0101.get("races", [])),
            "venues": venues_0101,
            "has_jra": has_jra,
            "ok": not has_jra,
        }
        status = "OK" if not has_jra else "NG"
        print(f"\n[VERIFY] 20260101_pred.json: {len(d0101.get('races',[]))} races")
        print(f"  venue 一覧: {venues_0101}")
        print(f"  JRA 含む: {has_jra} → [{status}] (期待: JRA なし = NAR のみ)")
    else:
        print(f"[VERIFY] 20260101_pred.json: ファイルが存在しない (元旦 race 0件で削除済み)")
        result_0101 = {"race_count": 0, "venues": [], "has_jra": False, "ok": True}

    # ──────────────────
    # 2. 20260131 確認
    # ──────────────────
    result_0131: dict = {}
    p0131 = predictions_dir / "20260131_pred.json"
    if p0131.exists():
        with open(p0131, "r", encoding="utf-8") as f:
            d0131 = json.load(f)
        venues_0131 = sorted(set(r.get("venue", "?") for r in d0131.get("races", [])))
        is_jra_list = [r.get("is_jra", False) for r in d0131.get("races", [])]
        has_jra = any(is_jra_list)
        result_0131 = {
            "race_count": len(d0131.get("races", [])),
            "venues": venues_0131,
            "has_jra": has_jra,
            "ok": has_jra,
        }
        status = "OK" if has_jra else "NG"
        print(f"\n[VERIFY] 20260131_pred.json: {len(d0131.get('races',[]))} races")
        print(f"  venue 一覧: {venues_0131}")
        print(f"  JRA 含む: {has_jra} → [{status}] (期待: JRA あり)")
    else:
        print(f"[VERIFY] 20260131_pred.json: ファイルが存在しない")
        result_0131 = {"race_count": 0, "venues": [], "has_jra": False, "ok": False}

    # ──────────────────
    # 3. 整合性 100% チェック
    # ──────────────────
    total_races = 0
    mismatch_count = 0
    mismatch_details: list[str] = []

    for p in sorted(predictions_dir.glob("*_pred.json")):
        if any(x in p.name for x in ("_prev", "_backup", ".bak")):
            continue
        file_date = filename_to_date_str(p.name)
        if file_date is None:
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        for race in data.get("races", []):
            race_id = race.get("race_id")
            if not race_id:
                continue
            total_races += 1
            if race_id in master:
                true_date = master[race_id]
                if true_date != file_date:
                    mismatch_count += 1
                    detail = f"{p.name}: race_id={race_id} 真値={true_date} ファイル={file_date}"
                    mismatch_details.append(detail)
                    if verbose:
                        print(f"[MISMATCH] {detail}")

    consistency_pct = ((total_races - mismatch_count) / total_races * 100) if total_races > 0 else 100.0
    status = "OK" if mismatch_count == 0 else "NG"
    print(f"\n[VERIFY] 整合性チェック: 全 {total_races:,} races, 不一致 {mismatch_count} 件")
    print(f"  整合率: {consistency_pct:.2f}% → [{status}]")

    if mismatch_count > 0 and not verbose:
        print(f"  ※ --verbose で不一致詳細を表示")
    elif mismatch_count > 0:
        for d in mismatch_details[:20]:
            print(f"    {d}")
        if len(mismatch_details) > 20:
            print(f"    ... 他 {len(mismatch_details) - 20} 件")

    return {
        "result_0101": result_0101,
        "result_0131": result_0131,
        "total_races": total_races,
        "mismatch_count": mismatch_count,
        "consistency_pct": consistency_pct,
        "ok": mismatch_count == 0,
    }


def main() -> None:
    args = parse_args()
    verbose = args.verbose
    t_start = time.time()

    print("=" * 60)
    print("relocate_pred_json_by_master.py")
    print(f"モード: {'DRY-RUN' if args.dry_run else '本実行 (--apply)'}")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ──────────────────────────────────────────────
    # Step 1: マスタ読み込み
    # ──────────────────────────────────────────────
    print("\n[Step 1/6] 真値マスタ読み込み")
    master = load_master(MASTER_PATH)

    # ──────────────────────────────────────────────
    # Step 2: pred.json 全件読み込み
    # ──────────────────────────────────────────────
    print("\n[Step 2/6] pred.json 全件読み込み")
    pred_files = load_pred_files(PREDICTIONS_DIR)
    total_input_races = sum(len(fi["data"].get("races", [])) for fi in pred_files)
    print(f"  読み込み: {len(pred_files)} ファイル, {total_input_races:,} races")

    # ──────────────────────────────────────────────
    # Step 3: バックアップ取得（dry-run でも必須）
    # ──────────────────────────────────────────────
    print("\n[Step 3/6] バックアップ取得")
    bak_dir = create_backup(PREDICTIONS_DIR, dry_run=args.dry_run)
    assert bak_dir is not None and bak_dir.exists(), "バックアップ取得失敗。本実行を中断します。"

    # ──────────────────────────────────────────────
    # Step 4: race_id → true_date 分類
    # ──────────────────────────────────────────────
    print("\n[Step 4/6] race_id を真値マスタで分類")
    race_by_true_date, meta_by_date, dup_count = classify_races(pred_files, master, verbose)
    total_output_races = sum(len(r) for r in race_by_true_date.values())
    print(f"  分類後: {len(race_by_true_date)} dates, {total_output_races:,} races (重複除去: {dup_count} 件)")

    # ──────────────────────────────────────────────
    # Step 5: 差分計算
    # ──────────────────────────────────────────────
    print("\n[Step 5/6] 差分計算")
    diff = compute_diff(pred_files, race_by_true_date)

    affected_total = len(diff["affected_src_dates"] | diff["affected_dst_dates"])
    print(f"  移動対象 race 数   : {diff['moved_races']:,}")
    print(f"  影響ファイル数     : {affected_total} ({len(diff['affected_src_dates'])} src + {len(diff['affected_dst_dates'])} dst)")
    print(f"  削除予定ファイル数 : {len(diff['delete_dates'])}")
    print(f"  新規作成ファイル数 : {len(diff['new_dates'])}")
    if diff["delete_dates"]:
        print(f"  削除予定: {sorted(diff['delete_dates'])[:10]}")
    if diff["new_dates"]:
        print(f"  新規作成: {sorted(diff['new_dates'])[:10]}")

    # dry-run はここで終了
    if args.dry_run:
        print("\n" + "=" * 60)
        print("DRY-RUN 完了。影響予測のみ表示しました。")
        print("本実行するには --apply オプションを使用してください。")
        print("=" * 60)
        return

    # ──────────────────────────────────────────────
    # Step 6: 本実行
    # ──────────────────────────────────────────────
    print("\n[Step 6/6] pred.json 再配置実行")
    apply_result = apply_relocation(
        PREDICTIONS_DIR, pred_files, race_by_true_date, meta_by_date, diff, verbose
    )

    elapsed = time.time() - t_start
    print(f"\n  書き込み: {apply_result['written']} ファイル")
    print(f"  新規作成: {apply_result['created']} ファイル")
    print(f"  削除    : {apply_result['deleted']} ファイル")
    print(f"  エラー  : {len(apply_result['errors'])} 件")
    print(f"  経過時間: {elapsed:.1f} 秒")

    if apply_result["errors"]:
        print("[ERROR] エラー詳細:")
        for e in apply_result["errors"]:
            print(f"  {e}")

    # ──────────────────────────────────────────────
    # 検証フェーズ
    # ──────────────────────────────────────────────
    verify_result = verify_results(PREDICTIONS_DIR, master, verbose)

    # 最終サマリ
    print("\n" + "=" * 60)
    print("最終サマリ")
    print("=" * 60)
    print(f"  バックアップ   : {bak_dir.name}")
    print(f"  移動 race 数   : {diff['moved_races']:,}")
    print(f"  削除ファイル   : {apply_result['deleted']}")
    print(f"  新規ファイル   : {apply_result['created']}")
    print(f"  整合率         : {verify_result['consistency_pct']:.2f}%")
    print(f"  エラー件数     : {len(apply_result['errors'])}")
    ok_all = len(apply_result["errors"]) == 0 and verify_result["ok"]
    print(f"\n  全体ステータス : {'[OK] 正常完了' if ok_all else '[NG] 要確認'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
