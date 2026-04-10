"""
全MLモデル一括再学習スクリプト
  python retrain_all.py             # LightGBM全モデル + position + last3f + prob
  python retrain_all.py --lgbm      # LightGBMのみ
  python retrain_all.py --pos       # 位置取りモデルのみ
  python retrain_all.py --l3f       # 上がり3Fモデルのみ
  python retrain_all.py --prob      # 三連率モデルのみ
"""
import sys, io, os, argparse, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime

t_total = time.time()

parser = argparse.ArgumentParser()
parser.add_argument("--lgbm",  action="store_true", help="LightGBM全モデルのみ")
parser.add_argument("--pos",   action="store_true", help="位置取りモデルのみ")
parser.add_argument("--l3f",   action="store_true", help="上がり3Fモデルのみ")
parser.add_argument("--prob",  action="store_true", help="三連率モデルのみ")
args = parser.parse_args()

do_all  = not (args.lgbm or args.pos or args.l3f or args.prob)
do_lgbm = do_all or args.lgbm
do_pos  = do_all or args.pos
do_l3f  = do_all or args.l3f
do_prob = do_all or args.prob

print(f"\n{'='*60}")
print(f"  D-AI 全モデル再学習  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}")

# ─── 1. LightGBM 全分割モデル ─────────────────────────────────
if do_lgbm:
    print("\n[1/3] LightGBM 全分割モデル学習 (47タスク)...")
    t0 = time.time()
    from src.ml.lgbm_model import train_split_models
    results = train_split_models(valid_days=30)
    ok  = sum(1 for m in results.values() if not m.get("skipped"))
    skp = sum(1 for m in results.values() if m.get("skipped"))
    print(f"  完了: {ok}モデル学習 / {skp}モデルスキップ  ({time.time()-t0:.0f}秒)")
    if "global" in results and not results["global"].get("skipped"):
        g = results["global"]
        print(f"  global: AUC={g['auc']:.4f}  Top1={g['top1_hit_rate']*100:.1f}%  "
              f"iter={g['best_iteration']}")

# ─── 2. 位置取り予測モデル ────────────────────────────────────
if do_pos:
    print("\n[2/3] 位置取り予測モデル (position_model) 学習...")
    t0 = time.time()
    from src.ml.position_model import run_training_pipeline as pos_train
    pos_metrics = pos_train()
    print(f"  MAE={pos_metrics['mae_model']:.4f}  "
          f"±0.2={pos_metrics['within_0.2']:.1f}%  "
          f"iter={pos_metrics['best_iteration']}  ({time.time()-t0:.0f}秒)")

# ─── 3. 上がり3F予測モデル ────────────────────────────────────
if do_l3f:
    print("\n[3/3] 上がり3F予測モデル (last3f_model) 学習...")
    t0 = time.time()
    from src.ml.last3f_model import run_training_pipeline as l3f_train
    l3f_metrics = l3f_train()
    print(f"  MAE={l3f_metrics['mae_model']:.4f}  "
          f"±1s={l3f_metrics['within_1s_model']:.1f}%  "
          f"iter={l3f_metrics['best_iteration']}  ({time.time()-t0:.0f}秒)")

# ─── 4. 三連率予測モデル ────────────────────────────────────
if do_prob:
    print("\n[4/4] 三連率予測モデル (prob_win/top2/top3) 学習...")
    t0 = time.time()
    from src.ml.probability_model import train_probability_models
    prob_result = train_probability_models(valid_days=30)
    print(f"  完了: {len(prob_result.get('metrics', {}))}モデル  ({time.time()-t0:.0f}秒)")
    for tgt, m in prob_result.get("metrics", {}).items():
        print(f"  {tgt}: AUC={m.get('auc_calibrated', 0):.4f}  "
              f"Brier={m.get('brier_calibrated', 0):.4f}")

print(f"\n{'='*60}")
print(f"  全学習完了  合計時間: {(time.time()-t_total)/60:.1f}分")
print(f"{'='*60}\n")
