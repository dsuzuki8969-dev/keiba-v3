# A-3 Step 2-4 実行計画 (WF 学習完了後)

## 前提

A-3 Step 1 (WF 学習) 完了確認:
```bash
ls data/models/wf_2024/ data/models/wf_2025/ data/models/wf_2026/
```
3 ディレクトリに pkl ファイルが存在すれば Step 1 完了。

## Step 2: WF モデル ベース予想生成 (walk_forward_backtest.py)

### コマンド
```bash
python scripts/walk_forward_backtest.py --start 2024-01 --end 2026-05 > log/wf_backtest_20260525.log 2>&1
```

### 引数
- `--start 2024-01`: 開始年月
- `--end 2026-05`: 終了年月
- `--train-months 12`: 学習ウィンドウ (default)
- `--force`: 既存予想データも上書き

### 工数予測
月単位の予想生成 → 約 1 時間〜数時間。

### 影響
- pred.json の予想 (composite/win_prob 等) が WF モデル ベースに上書きされる
- 既存 pred.json は backup 推奨 (data/predictions_backup_pre_wf/)

### バックアップコマンド
```bash
cp -r data/predictions data/predictions_backup_pre_wf
```

## Step 3: A-1 + A-2 で新 ROI 算出

WF 適用後の pred.json で印・買い目再生成 + ROI 算定:

```bash
python scripts/regen_strategy.py --all > log/regen_after_wf.log 2>&1
python scripts/verify_all_tickets.py > log/verify_after_wf.log 2>&1
python scripts/analyze_r1_ticket_roi.py > log/r1_after_wf.log 2>&1
```

### 期待値
- 2024-2025 ROI 改善 (現状 ~87-88% → 95-100% 期待)
- 2026 ROI 維持 or 改善 (現状 166% → 170%+ 期待)
- 月次 calibrator 効果で安定化

## Step 4: 結果分析 + コミット

### 比較表
| 年 | WF 適用前 ROI | WF 適用後 ROI | 差 |
|---|---:|---:|---:|
| 2024 | 87.1% | TBD | TBD |
| 2025 | 88.3% | TBD | TBD |
| 2026 | 166.4% | TBD | TBD |
| ALL | 129.0% | TBD | TBD |

### コミット例
```
feat: A-3 Step 2-4 WF 適用 + 全期間 ROI 再算定

- WF モデル ベース予想生成 (2024-2026)
- regen + verify + R-1 で新 ROI 算出
- 2024-2025: +X.Xpt 改善
- 2026: +X.Xpt 改善
- 全期間 ALL: +X.Xpt 改善
```

## 注意事項

1. **データ整合性**: WF 適用後の pred.json は 旧モデル時代の予想と混在しないよう backup を取る
2. **batch_wf_fast.py 経由 vs walk_forward_backtest.py 経路**: 前者は 既存実装、後者は本格 WF 経路。Step 2 では後者を推奨
3. **Phase 1-3 連携**: WF 適用後も Phase 1 (NAR SMILE 4 モデル) + Phase 2-A (Platt 較正) + Phase 3 (Isotonic 較正) は維持される
4. **ML 合議削除 (B-1) + 戦略B (D-1) + skip 強化 (D-7/D-8) + B-2 (△★☆ 除外) + D-3 (A→D 昇格) + G-2 (金沢 skip) は WF 適用後も有効** (regen_strategy.py の変更で反映済)

## 推定総工数

Step 2-4 合計: 3-6 時間 (Step 2 が 1-数時間 + Step 3 が 数分 + Step 4 が 30 分)

## トリガー (このセッション or 次セッションで実行)

A-3 Step 1 完了 (data/models/wf_*/ 生成確認) → 即時 Step 2 起動。
