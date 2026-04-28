# D-Aikeiba Design System v6.1

D-Aikeiba 競馬予想ダッシュボードのビジュアル言語定義。
Figma / ClaudeDesign MCP と React + Tailwind v4 + shadcn/ui 実装を橋渡しする"憲法"。

---

## 1. ブランド哲学

**D-AI keiba** は「文字と数字の羅列しかない競馬情報を、全頭見える化するシステム」。
デザインは次の 3 原則に従う:

1. **高級感（Premium）** — ターフクラブ・ウィナーズサークルを思わせる、重厚なネイビーと金箔
2. **読みやすさ（Readable）** — 18頭×10カラムの情報密度でも視線が迷子にならない
3. **信頼感（Trusted）** — 偏差値・確率・印が "当たっているように見える" 視覚ヒエラルキー

競馬ブックや netkeiba の「情報量至上主義」とは一線を画し、**SS/S/A などのグレード判定を
美術品のように際立たせる** ことで、マスター（ユーザー）の決断を後押しする。

---

## 2. カラーパレット

### 2.1 ブランドカラー

| トークン | ライト | ダーク | 用途 |
|---|---|---|---|
| `--brand-gold` | `#d4a853` | `#d4a853` | アクセント・◉鉄板・SS/G1・CTA |
| `--brand-gold-light` | `#f0d78c` | `#f0d78c` | グラデ明色 |
| `--brand-gold-dark` | `#a97f2b` | `#a97f2b` | グラデ暗色 |
| `--brand-navy` | `#0b1a3a` | `#0b1a3a` | ヘッダー・primary（ライト時） |
| `--brand-navy-light` | `#1e2b52` | `#1e2b52` | 副層 |
| `--brand-navy-dark` | `#050c1e` | `#050c1e` | 最深層 |

### 2.2 テーマ配色（`tokens.css` 参照）

**ライト**: `#f5f6fa` 背景 / `#ffffff` カード / `#0b1326` 文字
**ダーク（プレミアム基準）**: `#0a0e1a` 背景 / `#121a2e` カード / `#e4e9f5` 文字 / `#27324d` 境界

> ダークを第一級市民に昇格。`#0a0e1a` の深藍は金箔の映えを最大化する。

### 2.3 意味色

| 用途 | トークン | 値 |
|---|---|---|
| ポジティブ | `--positive` | `#10b981` |
| ネガティブ | `--negative` | `#dc2626` |
| 警告 | `--warning` | `#f59e0b` |
| 情報 | `--info` | `#2563eb` |

### 2.4 競馬固有色

**印**: ◉/◎=`--mark-tekipan`/`--mark-honmei`（緑）、○=taikou（青）、▲=tannuke（赤）、△=rendashi（紫）、★=rendashi2（墨）、☆=oana（青）、×=kiken（赤）

**自信度**: SS=金箔グラデ、S=`#2563eb`、A=`#dc2626`、B=`#1f2937`、C/D=`#9ca3af`

**馬場**: 芝=`#16a34a`、ダ=`#b45309`、障=`#7c3aed`

### 2.5 グレード背景（偏差値帯）

SS/S は金箔グラデ、A=青、B=墨、C=灰、D=砂。ダーク時は発光寄りに。
→ `.grade-cell` クラスで `--grade-bg`/`--grade-fg` から自動適用。

---

## 3. タイポグラフィ

### 3.1 フォントファミリー

- **Sans（本文）**: `Noto Sans JP` + system-ui
- **Heading**: `Geist Variable` + Noto Sans JP
- **数字（埋め込み適用）**: `tabular-nums` + `lnum` で桁揃え・ライニング数字

### 3.2 スケール

| トークン | サイズ | 用途 |
|---|---|---|
| `--fs-2xs` | 11px | バッジ・注記 |
| `--fs-xs`  | 12px | セル値・補助情報 |
| `--fs-sm`  | 13px | モバイル本文 |
| `--fs-md`  | 14px | **デフォルト本文** |
| `--fs-base`| 16px | PC 本文 |
| `--fs-lg`  | 18px | サブ見出し |
| `--fs-xl`  | 20px | 見出し |
| `--fs-2xl` | 24px | カード見出し |
| `--fs-3xl` | 30px | ページタイトル |
| `--fs-4xl` | 36px | セクション強調 |
| `--fs-5xl` | 48px | ヒーロー |

### 3.3 ウェイト

regular=400, medium=500, semibold=600, bold=700, **extra=800**（強調用）。

### 3.4 ライン高

tight=1.15（ヒーロー）, snug=1.35（見出し）, normal=1.55（本文）, relaxed=1.75（読み物）。

---

## 4. スペーシング（4px ベース）

`--space-0 … --space-20`。すべて 4px の倍数で統一。Tailwind の `p-*` / `gap-*` と自然に
整合する。特に値の揃った表組みでは `gap: var(--space-2)` を優先。

---

## 5. エレベーション（影）

| トークン | 用途 |
|---|---|
| `--shadow-xs` | 微細な浮き（バッジ） |
| `--shadow-sm` | カード既定 |
| `--shadow-md` | カードホバー |
| `--shadow-lg` | ダイアログ・オーバーレイ |
| `--shadow-xl` | モーダル最前面 |
| `--shadow-gold-glow` | 金箔リング＋発光（◉・SS・G1） |
| `--shadow-navy-glow` | ネイビー寄り発光（ダーク時の主 CTA） |

---

## 6. モーション

`--dur-fast` 120ms（バッジホバー） / `--dur-base` 180ms（カード） / `--dur-slow` 320ms（ページ遷移）。

イージング:
- `--ease-out` — 既定（出現・収束）
- `--ease-in-out` — 双方向（タブ切替）
- `--ease-spring` — 小さな跳ね（◉ ホバー）

**モーション量は控えめに**。18 頭の一覧が一斉に跳ねたら読めなくなる。

---

## 7. コンポーネントレイヤー

### 7.1 UI プリミティブ（shadcn/ui ベース）

- `Button` / `Badge` / `Card` / `Dialog` / `Input` / `Select` / `Separator` / `Table` / `Tabs`
- 破壊的変更せず、**プレミアムバリアントは `ui/premium/` 配下に追加**

### 7.2 プレミアムバリアント（`ui/premium/`）

- `PremiumCard` — CVA で `variant: default | gold | navy-glow | flat`
- `PremiumSection` — 大見出し + 区切り

### 7.3 ドメインコンポーネント（`keiba/`）

- `MarkBadge` — ◉ は金箔グラデ
- `ConfidenceBadge` — SS は金箔、他はグラデ単色
- `GradeBadge` — G1 は金箔、他はグラデ単色
- `SurfaceBadge` / `RaceCard` / `BreakdownTable` / `VenueTabs` / `ProgressTracker`

### 7.4 チャート（`charts/`、v6.1 新設）

- `AbilityHeatmap` — 6 軸偏差値ヒートマップ（能力/展開/適性/騎手/調教/血統）。isBanei=true 時は「展開」列をスキップ
- `WinProbBar` — 勝率/連対率/複勝率の 3 層スタック
- `PaceFlowChart` — 逃/先/差/追の頭数推移

---

## 8. レイアウト原則

### 8.1 グリッド

PC: `max-w-7xl`（1280px）中央寄せ、16px パディング。
モバイル: `max-w-[430px]`、12px パディング。
`useViewMode` フックで PC / mobile / auto を手動切替可。

### 8.2 スティッキーヘッダー

`--header-h` を `AppShell` が `ResizeObserver` で同期。子コンポーネントの
sticky オフセット計算に必ずこの変数を使う（ハードコード禁止）。

### 8.3 情報階層（レースカード例）

```
[Primary]   レース名（18px / extra-bold）
[Secondary] 距離・馬場・頭数（13px / medium / muted）
[Tertiary]  発走時刻（12px / regular / muted）
[Accent]    本命印＋馬名＋勝率（下段に border-top）
```

---

## 9. アクセシビリティ

- コントラスト比 **AA (4.5:1)** 必須。金箔バッジは地色と必ずコントラスト検証。
- インタラクティブ要素は `.focus-ring-gold` ユーティリティで統一。
- 印マーク等の絵文字/記号は `aria-label` に日本語ラベル（`MARKS[key].label`）を載せる。
- タップ領域は最低 **44×44px**（モバイル時）。

---

## 10. 命名規則

- CSS 変数: `--category-purpose`（例: `--brand-gold`, `--mark-honmei`）
- Tailwind クラス: トークン経由 `bg-mark-tekipan` / `text-conf-ss`
- コンポーネント: PascalCase（`MarkBadge`）
- ユーティリティクラス: kebab-case（`gold-gradient`, `micro-bar`）

---

## 11. 変更フロー

1. トークン追加/変更 → `tokens.css` を更新
2. TS からも参照する値 → `tokens.ts` にも同期
3. Tailwind から直接参照する必要あり → `index.css` の `@theme inline` にマッピング追加
4. ブランドレベルの変更 → この `design-system.md` を更新
5. コミット前に `npm run build` を通す

---

## 12. Figma / ClaudeDesign 連携

Figma ノードから `get_design_context` で React + Tailwind コードを取得する際、
返されるコードは以下の原則で適応:

- **CSS 変数名は既存の D-Aikeiba トークンに合わせる**（勝手に新規色を作らない）
- **shadcn/ui プリミティブがある場合はそれを使う**（import `@/components/ui/*`）
- **型ヒントを省略しない**（既存コードベースは厳格な TS 運用）
- **日本語コメント**は D-Aikeiba 規約（`CLAUDE.md` 参照）

---

## 付録 A: クイックリファレンス

```tsx
// 金箔テキスト
<span className="gold-gradient">◉</span>

// プレミアムカード
<PremiumCard variant="gold">...</PremiumCard>

// グレードセル（バックグラウンドで相対値）
<td className="grade-cell" style={{ "--grade-bg": "var(--grade-ss-bg)", "--grade-fg": "var(--grade-ss-fg)" }}>
  68
</td>

// micro-bar（偏差値セル内に相対バー）
<td className="micro-bar" style={{ "--pct": "72%" }}>68</td>

// 金アンダーライン（アクティブタブ）
<button className="gold-underline">出馬表</button>
```

---

## 狭幅レイアウト鉄則（2026-04-27 確立）

PC 表示モード強制（`viewMode=desktop`）かつ window<1024px、またはモバイル<768px で頻発するレイアウト崩壊を防ぐための必須ルール。

### 必須パターン

1. **横並び ≥ 5 セルは grid-cols-N で折返し化**
   - ❌ NG: `flex gap-1 justify-between` で 5+ セル → flex-1 でも数値が縮まず重なる
   - ✅ OK: `grid grid-cols-4 xl:grid-cols-8 gap-1` で xl 以上で全列、それ以下は折返し
   - **既存共通コンポーネント**: `frontend/src/components/keiba/ResponsiveAxes.tsx`（6/7/8 軸対応）

2. **長い数値（4 文字以上）は whitespace-nowrap + 親 min-w-0**
   - 例: `+27,952,160円` `+2,018,280円` `150.9%` 等
   - セル: `whitespace-nowrap` で折り返し禁止、親に `min-w-0` で flex 子の overflow を防ぐ

3. **テーブルは overflow-x-auto ラッパ + min-w 必須**
   - パターン: `<div className="overflow-x-auto"><table className="min-w-[600px] w-full">`
   - これで内容圧縮を防ぎ、横スクロールに任せる

4. **grid-cols-N (N≥4) は段階的縮退**
   - ❌ NG: `grid-cols-2 sm:grid-cols-4 md:grid-cols-7`（768-1024px で 4 列のまま）
   - ✅ OK: `grid-cols-2 sm:grid-cols-3 md:grid-cols-4 xl:grid-cols-7`（段階的に増やす）

### PC narrow mode 要件（viewMode=desktop かつ window<1024px）

- マスターは **デスクトップ表示を強制したまま狭幅で閲覧する**ケースがある（半幅ブラウザ / スマホ横向き）
- この状態で横方向にあふれると視覚的に「悲惨」になる
- 全 PC 専用コンポーネント（HorseCardPC.tsx 等）は **xl: 未満で必ず折返し or 縮退する**こと
- viewMode=auto かつ window<768px の場合は HorseCardMobile に切替わるため考慮不要

### Breakpoint 定数

`frontend/src/lib/breakpoints.ts`
```typescript
export const BREAKPOINTS = { SM: 640, MD: 768, LG: 1024, XL: 1280, XXL: 1536 } as const;
```

Tailwind v4 標準と整合。`useViewMode.tsx` で利用。マジックナンバー禁止。

### 検証フロー（新規 PR / 改修時）

主要ページを **6 幅** で目視確認（`frontend/e2e/responsive-check.spec.ts` 参照）：
- 320 / 375 / 520 / 768 / 1024 / 1440 px

各幅で以下を確認：
1. 横スクロール発生箇所（意図的な overflow-x-auto を除く）
2. テキスト切れ・重なり
3. ボタン・タップ領域 < 44×44px
4. 画像・チャートの極端な縮小
