# B-3 補足: 派 5b (二重一致 ∩ ◎ のみ) 統合の実装計画

> 2026-05-26 v3 セッション作成 / B-3 既存設計 `docs/future_changes_post_a3.md` の派生
> 前提: A-3e Lv3 完了 → D-1c/D-1d Lv3 再検証 → 派 5b 採用判断

## 派 5b の運用仕様 (Lv1 値 - Lv3 再検証で更新予定)

| 項目 | 仕様 |
|---|---|
| 売買形式 | 単勝 1 点 |
| 対象馬 | ◎ かつ shobu_score TOP2 ∩ composite TOP2 (二重一致) |
| 対象 race | 上記条件を満たす ◎ がある race のみ (機会 36%, ◎ race の 64% を見送り) |
| 不採用時 | 何も買わない (◎ 単勝も買わない) |
| 適用範囲 | JRA 全場 (NAR は別判定 ROI 123.6%) |

## 派 5b 実装ステップ

### Step 1: フィルタ条件の engine 統合

`src/engine.py` (or 戦略判定箇所) に派 5b 判定ロジックを追加:

```python
def is_strategy_5b_match(race: dict) -> Optional[dict]:
    """派 5b: ◎ かつ shobu TOP2 ∩ composite TOP2 の馬を返す。該当なしなら None。"""
    horses = [h for h in race["horses"]
              if not h.get("is_scratched") and (h.get("shobu_score") or 0) > 0]
    if len(horses) < 2:
        return None

    by_shobu = sorted(horses, key=lambda h: h.get("shobu_score", 0), reverse=True)
    by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)

    shobu_top2_ids = {h["horse_no"] for h in by_shobu[:2]}
    comp_top2_ids = {h["horse_no"] for h in by_comp[:2]}
    overlap_ids = shobu_top2_ids & comp_top2_ids

    honmei = next((h for h in horses
                   if h.get("mark") in ("◉", "◎")
                   and h["horse_no"] in overlap_ids), None)
    return honmei
```

### Step 2: betting.py での買い目生成

`src/betting.py` に派 5b 専用関数:
```python
def build_tansho_strategy_5b(race: dict, stake: int = 100) -> list[dict]:
    """派 5b ◎単勝 1 点 (該当なしならスキップ)"""
    honmei = is_strategy_5b_match(race)
    if not honmei:
        return []
    return [{
        "ticket_type": "tansho",
        "horse_no": honmei["horse_no"],
        "stake": stake,
        "strategy": "5b_double_overlap_honmei",
    }]
```

### Step 3: 既存運用との切り替え

`config/settings.py` にフラグ追加:
```python
# 派 5b 採用フラグ (A-3e Lv3 検証完了後に True 化検討)
STRATEGY_5B_ENABLED = False  # 初期は OFF
```

`engine.py` で:
```python
if settings.STRATEGY_5B_ENABLED:
    tickets = build_tansho_strategy_5b(race)
else:
    tickets = build_tansho_honmei(race)  # 既存 ◎単勝 1 点
```

### Step 4: ダッシュボード表示

- フロントエンドで「派 5b 採用 race」と「見送り race」を区別表示
- 見送り race には「派 5b 条件不一致のため購入対象外」バッジ
- 既存 ◎ 馬は表示するが「購入対象外」マーカー

### Step 5: 実運用前検証

1. A-3e Lv3 で全期間 WF backtest を再実行 (本セッション中)
2. `scripts/diag_d1d_strategy_a_overlap_jra.py` を再実行して Lv3 数値を取得
3. **派 5b ROI が Lv3 でも 200% 超を維持しているか確認**
4. Lv3 vs Lv1 で派 5b の メンバー (overlap 馬) が大きく変わっていないか
5. 月別 ROI 分散 / 損失月数を再評価 (D-1d 当時 9/95)

## 採用判定基準 (Lv3 で再評価)

| 指標 | Lv1 値 (D-1d) | 採用基準 |
|---|---:|---|
| 派 5b JRA ROI | 207.3% | **>= 195% (案 4 +12pt 維持)** |
| CI 95% 下限 | 184.9% | >= 案 4 中央値 183.5% |
| 月別 < 100% 月数 | 9/95 | <= 15/95 (案 4 と同等以下) |
| 機会 (races) | 2,708 / JRA 全 7,509 = 36% | >= 30% (極端に少なくない) |

これら全てクリアなら派 5b 採用可。1 項目でも未達なら見送り or 修正検討。

## リスク・トレードオフ

| 項目 | リスク | 対策 |
|---|---|---|
| 機会損失 64% | ◎ がある race の 64% を見送る = 投資機会激減 | 案 4 と並列運用 (派 5b 適用 race は派 5b, 非適用 race は案 4) も検討 |
| Lv3 で派 5b メンバー変動 | shobu_score 計算ロジック変更で overlap 馬が変わる | Lv1/Lv3 で overlap 馬の一致率を確認 (80% 以上が望ましい) |
| 短期分散 | 小サンプル race での ROI 端値ブレ | 月次モニタリング + 6 ヶ月以上の本番運用で評価 |

## 関連
- 派生元: `docs/future_changes_post_a3.md` B-3 設計
- Lv1 検証: `scripts/diag_d1d_strategy_a_overlap_jra.py`
- 派 5b 数値: `memory/handoff_2026-05-26_v2.md` D-1d セクション
- Lv3 実装: `scripts/walk_forward_backtest.py:_calc_shobu_score_wf_lv3`
