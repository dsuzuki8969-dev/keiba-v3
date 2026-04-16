#!/usr/bin/env python3
"""バッチ予想再生成スクリプト
指定期間の予想を新バージョンのエンジンで再生成する。
既存pred.jsonは _prev.json にバックアップ。
完了後に改善レポートを自動生成。
"""
import json, os, sys, glob, time, shutil, subprocess, datetime, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 設定
START_DATE = "20240101"
END_DATE = "20240412"
PRED_DIR = "data/predictions"
LOG_FILE = "scripts/batch_regenerate.log"
WORKERS = 3

def get_target_dates():
    """再生成対象の日付リストを取得（_prev.jsonから会場情報を読む）"""
    targets = []
    for f in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json"))):
        bn = os.path.basename(f)
        dt = bn[:8]
        if START_DATE <= dt <= END_DATE:
            # _prev.jsonがあればそちらから読む（バックアップ済み＝元データ）
            prev = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
            src = prev if os.path.exists(prev) else f
            try:
                d = json.load(open(src, 'r', encoding='utf-8'))
                races = d.get('races', [])
                if len(races) > 0:
                    venues = list(set(r.get('venue', '') for r in races))
                    targets.append((dt, venues, len(races)))
            except Exception:
                pass
    return targets

def backup_pred(dt):
    """既存pred.jsonを_prev.jsonにバックアップ（既存ならスキップ）"""
    src = os.path.join(PRED_DIR, f"{dt}_pred.json")
    dst = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
    if not os.path.exists(dst) and os.path.exists(src):
        shutil.copy2(src, dst)

def run_date(dt, venues):
    """1日分の予想を再生成"""
    date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    venues_str = ",".join(venues)
    cmd = [
        sys.executable, "run_analysis_date.py", date_str,
        "--venues", venues_str,
        "--force", "--no-html",
        "--no-purge",
        "--race-ids-from-pred",
        "--workers", str(WORKERS),
    ]
    start = time.time()
    result = subprocess.run(
        cmd, capture_output=True, text=True, encoding='utf-8', errors='replace',
        timeout=1800,
    )
    elapsed = time.time() - start
    success = result.returncode == 0
    err_msg = result.stderr[-300:] if not success else ""
    return success, elapsed, err_msg

def generate_comparison_report():
    """新旧pred.jsonを比較してレポート生成"""
    print("\n" + "=" * 70)
    print("  改善レポート生成中...")
    print("=" * 70)

    years = {}  # year -> {old: {...}, new: {...}}
    overall_old = {'win':0,'p2':0,'p3':0,'total':0,'wide':0,'trio':0}
    overall_new = {'win':0,'p2':0,'p3':0,'total':0,'wide':0,'trio':0}

    for f in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred_prev.json"))):
        bn = os.path.basename(f)
        dt = bn[:8]
        if not (START_DATE <= dt <= END_DATE):
            continue
        new_f = os.path.join(PRED_DIR, f"{dt}_pred.json")
        if not os.path.exists(new_f):
            continue

        try:
            old_d = json.load(open(f, 'r', encoding='utf-8'))
            new_d = json.load(open(new_f, 'r', encoding='utf-8'))
        except Exception:
            continue

        year = dt[:4]
        if year not in years:
            years[year] = {
                'old': {'win':0,'p2':0,'p3':0,'total':0,'honmei_win':0,'honmei_total':0,
                         'taikou_p2':0,'taikou_total':0,'wide':0,'trio':0,'races':0},
                'new': {'win':0,'p2':0,'p3':0,'total':0,'honmei_win':0,'honmei_total':0,
                         'taikou_p2':0,'taikou_total':0,'wide':0,'trio':0,'races':0}
            }

        for label, data in [('old', old_d), ('new', new_d)]:
            stats = years[year][label]
            for race in data.get('races', []):
                horses = race.get('horses', [])
                stats['races'] += 1
                # 結果データがあるかチェック
                has_result = any(h.get('finish_pos') or h.get('result') for h in horses)
                if not has_result:
                    continue

                honmei = None
                taikou = None
                ana = None

                for h in horses:
                    fp = h.get('finish_pos') or h.get('result')
                    if fp is None:
                        continue
                    try:
                        fp = int(fp)
                    except (ValueError, TypeError):
                        continue
                    if fp <= 0:
                        continue

                    mk = h.get('mark', '')
                    if mk in ['◎', '◉']:
                        honmei = h
                        stats['honmei_total'] += 1
                        if fp == 1: stats['honmei_win'] += 1
                    elif mk == '○':
                        taikou = h
                        stats['taikou_total'] += 1
                        if fp <= 2: stats['taikou_p2'] += 1
                    elif mk == '▲':
                        ana = h

                    stats['total'] += 1
                    if fp == 1: stats['win'] += 1
                    if fp <= 2: stats['p2'] += 1
                    if fp <= 3: stats['p3'] += 1

                # ワイド (◎○両方2着以内)
                if honmei and taikou:
                    hfp = int(honmei.get('finish_pos') or honmei.get('result') or 99)
                    tfp = int(taikou.get('finish_pos') or taikou.get('result') or 99)
                    if hfp <= 2 and tfp <= 2:
                        stats['wide'] += 1

                # 三連 (◎○▲全て3着以内)
                if honmei and taikou and ana:
                    hfp = int(honmei.get('finish_pos') or honmei.get('result') or 99)
                    tfp = int(taikou.get('finish_pos') or taikou.get('result') or 99)
                    afp = int(ana.get('finish_pos') or ana.get('result') or 99)
                    if hfp <= 3 and tfp <= 3 and afp <= 3:
                        stats['trio'] += 1

    # レポート出力
    report_lines = []
    def p(s):
        print(s)
        report_lines.append(s)

    p("\n" + "=" * 70)
    p("  新旧バージョン比較レポート")
    p(f"  期間: {START_DATE[:4]}/{START_DATE[4:6]}/{START_DATE[6:8]} - {END_DATE[:4]}/{END_DATE[4:6]}/{END_DATE[6:8]}")
    p("=" * 70)

    for year in sorted(years.keys()):
        old = years[year]['old']
        new = years[year]['new']
        p(f"\n--- {year}年 ---")
        p(f"  レース数: old={old['races']}R  new={new['races']}R")

        if old['honmei_total'] > 0 and new['honmei_total'] > 0:
            old_hw = old['honmei_win'] / old['honmei_total'] * 100
            new_hw = new['honmei_win'] / new['honmei_total'] * 100
            diff = new_hw - old_hw
            sign = "+" if diff >= 0 else ""
            p(f"  ◎勝率:  old={old_hw:.1f}%  new={new_hw:.1f}%  ({sign}{diff:.1f}pt)")

        if old['taikou_total'] > 0 and new['taikou_total'] > 0:
            old_tp = old['taikou_p2'] / old['taikou_total'] * 100
            new_tp = new['taikou_p2'] / new['taikou_total'] * 100
            diff = new_tp - old_tp
            sign = "+" if diff >= 0 else ""
            p(f"  ○連対率: old={old_tp:.1f}%  new={new_tp:.1f}%  ({sign}{diff:.1f}pt)")

        if old['races'] > 0 and new['races'] > 0:
            old_wr = old['wide'] / old['races'] * 100
            new_wr = new['wide'] / new['races'] * 100
            diff = new_wr - old_wr
            sign = "+" if diff >= 0 else ""
            p(f"  ◎○W:   old={old_wr:.1f}%  new={new_wr:.1f}%  ({sign}{diff:.1f}pt)")

            old_tr = old['trio'] / old['races'] * 100
            new_tr = new['trio'] / new['races'] * 100
            diff = new_tr - old_tr
            sign = "+" if diff >= 0 else ""
            p(f"  三連率:  old={old_tr:.1f}%  new={new_tr:.1f}%  ({sign}{diff:.1f}pt)")

    # 全体
    p(f"\n--- 全体 ({START_DATE}-{END_DATE}) ---")
    all_old = {'honmei_win':0,'honmei_total':0,'taikou_p2':0,'taikou_total':0,'wide':0,'trio':0,'races':0}
    all_new = {'honmei_win':0,'honmei_total':0,'taikou_p2':0,'taikou_total':0,'wide':0,'trio':0,'races':0}
    for year in years:
        for k in all_old:
            all_old[k] += years[year]['old'][k]
            all_new[k] += years[year]['new'][k]

    if all_old['honmei_total'] > 0 and all_new['honmei_total'] > 0:
        old_hw = all_old['honmei_win'] / all_old['honmei_total'] * 100
        new_hw = all_new['honmei_win'] / all_new['honmei_total'] * 100
        diff = new_hw - old_hw
        sign = "+" if diff >= 0 else ""
        p(f"  ◎勝率:  old={old_hw:.1f}%  new={new_hw:.1f}%  ({sign}{diff:.1f}pt)")

    if all_old['taikou_total'] > 0 and all_new['taikou_total'] > 0:
        old_tp = all_old['taikou_p2'] / all_old['taikou_total'] * 100
        new_tp = all_new['taikou_p2'] / all_new['taikou_total'] * 100
        diff = new_tp - old_tp
        sign = "+" if diff >= 0 else ""
        p(f"  ○連対率: old={old_tp:.1f}%  new={new_tp:.1f}%  ({sign}{diff:.1f}pt)")

    if all_old['races'] > 0 and all_new['races'] > 0:
        old_wr = all_old['wide'] / all_old['races'] * 100
        new_wr = all_new['wide'] / all_new['races'] * 100
        diff = new_wr - old_wr
        sign = "+" if diff >= 0 else ""
        p(f"  ◎○W:   old={old_wr:.1f}%  new={new_wr:.1f}%  ({sign}{diff:.1f}pt)")

        old_tr = all_old['trio'] / all_old['races'] * 100
        new_tr = all_new['trio'] / all_new['races'] * 100
        diff = new_tr - old_tr
        sign = "+" if diff >= 0 else ""
        p(f"  三連率:  old={old_tr:.1f}%  new={new_tr:.1f}%  ({sign}{diff:.1f}pt)")

    p(f"\n  レース数: old={all_old['races']}R  new={all_new['races']}R")

    # ファイル保存
    report_path = "scripts/improvement_report.txt"
    with open(report_path, 'w', encoding='utf-8') as rf:
        rf.write('\n'.join(report_lines))
    p(f"\n  レポート保存: {report_path}")

def main():
    targets = get_target_dates()
    total_days = len(targets)
    total_races = sum(t[2] for t in targets)

    print(f"=" * 70)
    print(f"  batch regenerate  {START_DATE} -> {END_DATE}")
    print(f"  target: {total_days} days  {total_races} races")
    print(f"=" * 70)

    log_f = open(LOG_FILE, 'a', encoding='utf-8')
    log_f.write(f"\nbatch_regenerate restart: {datetime.datetime.now()}\n")
    log_f.write(f"target: {total_days} days, {total_races} races\n\n")

    completed = 0
    failed = 0
    skipped = 0
    total_elapsed = 0
    race_done = 0

    for i, (dt, venues, nr) in enumerate(targets):
        pct = (i / total_days) * 100
        elapsed_str = f"{total_elapsed/60:.0f}min"
        if completed > 0:
            avg_per_day = total_elapsed / completed
            remaining = avg_per_day * (total_days - i)
            eta_str = f"ETA={remaining/60:.0f}min"
        else:
            eta_str = "..."

        bar_len = 30
        filled = int(bar_len * i / total_days)
        bar = "#" * filled + "." * (bar_len - filled)
        print(f"  [{bar}] {pct:>5.1f}% ({i}/{total_days}) {dt} {nr}R  {elapsed_str} {eta_str}", flush=True)

        # バックアップ（既存prevがなければ作成）
        backup_pred(dt)

        # 既に再生成済みかチェック（_prev.jsonとpred.jsonのサイズが異なれば再生成済み）
        prev_f = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
        cur_f = os.path.join(PRED_DIR, f"{dt}_pred.json")
        if os.path.exists(prev_f) and os.path.exists(cur_f):
            prev_size = os.path.getsize(prev_f)
            cur_size = os.path.getsize(cur_f)
            cur_mtime = os.path.getmtime(cur_f)
            prev_mtime = os.path.getmtime(prev_f)
            if cur_mtime > prev_mtime and abs(cur_size - prev_size) > 100:
                skipped += 1
                race_done += nr
                log_f.write(f"SKIP {dt} {nr}R (already regenerated)\n")
                log_f.flush()
                continue

        # 再生成
        try:
            success, elapsed, err = run_date(dt, venues)
            total_elapsed += elapsed
            race_done += nr

            if success:
                completed += 1
                log_f.write(f"OK  {dt} {nr}R {elapsed:.0f}s\n")
            else:
                failed += 1
                log_f.write(f"ERR {dt} {nr}R {elapsed:.0f}s {err[:200]}\n")
                print(f"  !! {dt} FAILED ({elapsed:.0f}s)")
        except subprocess.TimeoutExpired:
            failed += 1
            log_f.write(f"TIMEOUT {dt} {nr}R\n")
            print(f"  !! {dt} TIMEOUT")
        except Exception as e:
            failed += 1
            log_f.write(f"EXC {dt} {str(e)[:200]}\n")

        log_f.flush()

    bar = "#" * bar_len
    print(f"  [{bar}] 100.0% ({total_days}/{total_days}) DONE! {total_elapsed/60:.0f}min")
    print(f"\n  OK: {completed}  FAIL: {failed}  SKIP: {skipped}  races: {race_done}")
    print(f"  time: {total_elapsed/3600:.1f}h ({total_elapsed/60:.0f}min)")
    if completed > 0:
        print(f"  avg: {total_elapsed/completed:.1f}s/day")

    log_f.write(f"\ncompleted: {completed}, failed: {failed}\n")
    log_f.write(f"total_time: {total_elapsed:.0f}s\n")
    log_f.write(f"finish: {datetime.datetime.now()}\n")
    log_f.close()

    # 比較レポート自動生成
    generate_comparison_report()

if __name__ == '__main__':
    main()
