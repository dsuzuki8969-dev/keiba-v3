import { useState, useMemo } from "react";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useFeatureImportance } from "@/api/hooks";
import type { FeatureImportanceItem } from "@/api/client";
import {
  Brain, Target, TrendingUp, Trophy, Users, Dna, Timer,
  Zap, Cpu, BarChart3, Shield, Layers, GitMerge,
  ChevronRight, Sparkles, Activity,
} from "lucide-react";

// ================================================================
// カテゴリスタイル定義（7因子: 能力/展開/適性/騎手/調教師/血統/調教）
// ================================================================
const CAT_STYLES: Record<string, { bg: string; text: string; border: string }> = {
  能力:   { bg: "bg-emerald-50", text: "text-emerald-700", border: "border-emerald-300" },
  展開:   { bg: "bg-blue-50",    text: "text-blue-700",    border: "border-blue-300" },
  適性:   { bg: "bg-teal-50",    text: "text-teal-700",    border: "border-teal-300" },
  騎手:   { bg: "bg-orange-50",  text: "text-orange-700",  border: "border-orange-300" },
  調教師: { bg: "bg-purple-50",  text: "text-purple-700",  border: "border-purple-300" },
  血統:   { bg: "bg-yellow-50",  text: "text-yellow-700",  border: "border-yellow-300" },
  調教:   { bg: "bg-rose-50",    text: "text-rose-700",    border: "border-rose-300" },
};

const BAR_COLORS: Record<string, string> = {
  能力: "bg-emerald-500",
  展開: "bg-blue-500",
  適性: "bg-teal-500",
  騎手: "bg-orange-500",
  調教師: "bg-purple-500",
  血統: "bg-yellow-500",
  調教: "bg-rose-500",
};

// ================================================================
// 7因子定義
// ================================================================
const SEVEN_FACTORS = [
  { key: "ability",  label: "能力",   icon: Zap,        color: "text-emerald-600", bg: "bg-emerald-50 dark:bg-emerald-950/30", border: "border-emerald-200 dark:border-emerald-800", desc: "走破タイム・着差・調子偏差値を統合した純粋なパフォーマンス指標" },
  { key: "pace",     label: "展開",   icon: Activity,   color: "text-blue-600",    bg: "bg-blue-50 dark:bg-blue-950/30",    border: "border-blue-200 dark:border-blue-800",    desc: "ペース予測・脚質分類・位置取りシミュレーションによる展開適性" },
  { key: "course",   label: "適性",   icon: Target,     color: "text-teal-600",    bg: "bg-teal-50 dark:bg-teal-950/30",    border: "border-teal-200 dark:border-teal-800",    desc: "コース実績・コース形状・騎手コース相性の多角的適性評価" },
  { key: "jockey",   label: "騎手",   icon: Users,      color: "text-orange-600",  bg: "bg-orange-50 dark:bg-orange-950/30", border: "border-orange-200 dark:border-orange-800", desc: "騎手の勝率・回収率・コース別成績・馬との相性を数値化" },
  { key: "trainer",  label: "調教師", icon: Trophy,     color: "text-purple-600",  bg: "bg-purple-50 dark:bg-purple-950/30", border: "border-purple-200 dark:border-purple-800", desc: "厩舎の出走パターン・仕上げ傾向・距離適性データを分析" },
  { key: "blood",    label: "血統",   icon: Dna,        color: "text-yellow-600",  bg: "bg-yellow-50 dark:bg-yellow-950/30", border: "border-yellow-200 dark:border-yellow-800", desc: "父系・母父系の距離/馬場適性・産駒傾向を統計的に評価" },
  { key: "training", label: "調教",   icon: Timer,      color: "text-rose-600",    bg: "bg-rose-50 dark:bg-rose-950/30",    border: "border-rose-200 dark:border-rose-800",    desc: "追切タイム・仕上がり度・厩舎評価を加味した直前コンディション" },
] as const;

// ================================================================
// 印体系 v5
// ================================================================
const MARKS_V5 = [
  { symbol: "◉", label: "鉄板", color: "text-emerald-600", bg: "bg-emerald-50 dark:bg-emerald-950/30", border: "border-emerald-300 dark:border-emerald-700", desc: "最有力。composite + win_prob 両方で圧倒的優位" },
  { symbol: "◎", label: "本命", color: "text-emerald-600", bg: "bg-emerald-50 dark:bg-emerald-950/30", border: "border-emerald-300 dark:border-emerald-700", desc: "強い勝ち候補。ML合議で高評価" },
  { symbol: "○", label: "対抗", color: "text-blue-600",    bg: "bg-blue-50 dark:bg-blue-950/30",    border: "border-blue-300 dark:border-blue-700",    desc: "本命に次ぐ2番手。逆転の可能性あり" },
  { symbol: "▲", label: "単穴", color: "text-red-600",     bg: "bg-red-50 dark:bg-red-950/30",      border: "border-red-300 dark:border-red-700",      desc: "一発の魅力あり。条件次第で上位食い込み" },
  { symbol: "△", label: "連下",  color: "text-purple-600",  bg: "bg-purple-50 dark:bg-purple-950/30", border: "border-purple-300 dark:border-purple-700", desc: "連対圏に絡む可能性。紐として有力" },
  { symbol: "★", label: "連下2", color: "text-foreground",  bg: "bg-muted/50",                        border: "border-border",                           desc: "連対圏にぎりぎり。展開次第で浮上" },
  { symbol: "☆", label: "連下3", color: "text-blue-600",    bg: "bg-blue-50 dark:bg-blue-950/30",    border: "border-blue-300 dark:border-blue-700",    desc: "大穴候補。高配当を狙う時の選択肢" },
] as const;

// ================================================================
// 自信度ランク
// ================================================================
const CONFIDENCE_RANKS = [
  { rank: "SS", threshold: "65以上", color: "text-emerald-600", bg: "bg-emerald-50 dark:bg-emerald-950/40", border: "border-emerald-400 dark:border-emerald-700", desc: "鉄板級。win_prob + gap ゲート通過" },
  { rank: "S",  threshold: "60以上", color: "text-blue-600",    bg: "bg-blue-50 dark:bg-blue-950/40",    border: "border-blue-400 dark:border-blue-700",    desc: "高信頼。ML全指標で上位安定" },
  { rank: "A",  threshold: "55以上", color: "text-red-600",     bg: "bg-red-50 dark:bg-red-950/40",      border: "border-red-400 dark:border-red-700",      desc: "注目。好走条件が揃う" },
  { rank: "B",  threshold: "50以上", color: "text-foreground",  bg: "bg-muted/40",                        border: "border-border",                           desc: "標準的。データ上は平均圏" },
  { rank: "C",  threshold: "45以上", color: "text-muted-foreground", bg: "bg-muted/20",                   border: "border-border/60",                        desc: "やや低調。積極推奨は難しい" },
  { rank: "D",  threshold: "45未満", color: "text-muted-foreground/70", bg: "bg-muted/10",                border: "border-border/40",                        desc: "低信頼。見送り推奨" },
] as const;


export default function AboutPage() {
  const { data, isLoading, error } = useFeatureImportance();
  const [filterCat, setFilterCat] = useState("all");
  const [search, setSearch] = useState("");

  // カテゴリ別カウント
  const catCounts = useMemo(() => {
    if (!data) return {};
    const counts: Record<string, number> = {};
    for (const d of data) {
      counts[d.cat] = (counts[d.cat] || 0) + 1;
    }
    return counts;
  }, [data]);

  // フィルタ適用
  const filtered = useMemo(() => {
    if (!data) return [];
    const q = search.toLowerCase();
    return data.filter(
      (d: FeatureImportanceItem) =>
        (filterCat === "all" || d.cat === filterCat) &&
        (!q ||
          (d.label || d.name).toLowerCase().includes(q) ||
          (d.desc || "").toLowerCase().includes(q))
    );
  }, [data, filterCat, search]);

  const maxPct = data?.[0]?.pct ?? 1;

  return (
    <div className="space-y-8 max-w-5xl mx-auto">

      {/* ================================================================ */}
      {/* セクション1: ヒーロー / キャッチ */}
      {/* ================================================================ */}
      <PremiumCard variant="gold" padding="lg">
        <div className="relative overflow-hidden">
          {/* 背景装飾 */}
          <div className="absolute -top-20 -right-20 w-60 h-60 rounded-full bg-gradient-to-br from-brand-gold/10 to-transparent blur-3xl pointer-events-none" />
          <div className="absolute -bottom-16 -left-16 w-48 h-48 rounded-full bg-gradient-to-tr from-blue-500/5 to-transparent blur-2xl pointer-events-none" />

          <div className="relative space-y-4">
            <div className="flex items-center gap-2">
              <Sparkles size={16} className="text-brand-gold" />
              <span className="gold-gradient font-extrabold tracking-wider uppercase text-xs">
                D-AI Keiba Prediction System
              </span>
            </div>

            <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight text-foreground leading-tight">
              文字や数字の羅列の競馬情報を、
              <br className="hidden sm:block" />
              <span className="gold-gradient">全頭見える化。</span>
            </h1>

            <p className="text-base text-muted-foreground leading-relaxed max-w-2xl">
              市場に騙されない "本当の力" をはかる。人気・オッズを一切排除した
              純粋データ分析で、JRA中央競馬・NAR地方競馬の全レースを予想します。
            </p>

            {/* キーメトリクス */}
            <div className="flex flex-wrap gap-3 pt-2">
              <Badge className="bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300 border border-emerald-300 dark:border-emerald-700 text-xs px-3 py-1">
                <Shield size={12} className="mr-1" />
                人気・オッズ不使用
              </Badge>
              <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300 border border-blue-300 dark:border-blue-700 text-xs px-3 py-1">
                <Layers size={12} className="mr-1" />
                42分割MLモデル
              </Badge>
              <Badge className="bg-purple-100 text-purple-800 dark:bg-purple-900/40 dark:text-purple-300 border border-purple-300 dark:border-purple-700 text-xs px-3 py-1">
                <Brain size={12} className="mr-1" />
                7因子コンポジット
              </Badge>
            </div>
          </div>
        </div>
      </PremiumCard>

      {/* ================================================================ */}
      {/* セクション2: システムアーキテクチャ概要 */}
      {/* ================================================================ */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <Cpu size={18} className="text-blue-600" />
          <h2 className="text-lg font-bold text-foreground">システムアーキテクチャ</h2>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          {/* 42分割LightGBM */}
          <PremiumCard variant="default" padding="md" className="relative">
            <div className="absolute top-0 left-0 right-0 h-1 rounded-t-xl bg-gradient-to-r from-emerald-500 to-teal-500" />
            <div className="space-y-3 pt-1">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 rounded-lg bg-emerald-50 dark:bg-emerald-950/40 flex items-center justify-center">
                  <Layers size={20} className="text-emerald-600" />
                </div>
                <div>
                  <h3 className="text-sm font-bold text-foreground">42分割LightGBM</h3>
                  <p className="text-[10px] text-muted-foreground">勾配ブースティング</p>
                </div>
              </div>
              <div className="space-y-1 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-emerald-500 shrink-0" />
                  <span>会場別 25モデル</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-emerald-500 shrink-0" />
                  <span>面距離別 8モデル</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-emerald-500 shrink-0" />
                  <span>汎用4 + 確率3 + ランカー1 + ばんえい1</span>
                </div>
              </div>
            </div>
          </PremiumCard>

          {/* Neural Ranker */}
          <PremiumCard variant="default" padding="md" className="relative">
            <div className="absolute top-0 left-0 right-0 h-1 rounded-t-xl bg-gradient-to-r from-blue-500 to-indigo-500" />
            <div className="space-y-3 pt-1">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 rounded-lg bg-blue-50 dark:bg-blue-950/40 flex items-center justify-center">
                  <Brain size={20} className="text-blue-600" />
                </div>
                <div>
                  <h3 className="text-sm font-bold text-foreground">Neural Ranker</h3>
                  <p className="text-[10px] text-muted-foreground">PyTorch 順位学習</p>
                </div>
              </div>
              <div className="space-y-1 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-blue-500 shrink-0" />
                  <span>ListMLE損失関数による順位学習</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-blue-500 shrink-0" />
                  <span>レース全体の相対評価</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-blue-500 shrink-0" />
                  <span>LightGBMと独立した視点で補完</span>
                </div>
              </div>
            </div>
          </PremiumCard>

          {/* ML合議制 */}
          <PremiumCard variant="default" padding="md" className="relative">
            <div className="absolute top-0 left-0 right-0 h-1 rounded-t-xl bg-gradient-to-r from-purple-500 to-pink-500" />
            <div className="space-y-3 pt-1">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 rounded-lg bg-purple-50 dark:bg-purple-950/40 flex items-center justify-center">
                  <GitMerge size={20} className="text-purple-600" />
                </div>
                <div>
                  <h3 className="text-sm font-bold text-foreground">ML合議制</h3>
                  <p className="text-[10px] text-muted-foreground">統合判定エンジン</p>
                </div>
              </div>
              <div className="space-y-1 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-purple-500 shrink-0" />
                  <span>win_prob x composite 統合スコア</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-purple-500 shrink-0" />
                  <span>6信号スコアによる自信度判定</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <ChevronRight size={10} className="text-purple-500 shrink-0" />
                  <span>印・買い目を自動生成</span>
                </div>
              </div>
            </div>
          </PremiumCard>
        </div>

        {/* オッズ不使用バッジ（目立つ配置） */}
        <div className="mt-4 flex justify-center">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full border-2 border-dashed border-emerald-400 dark:border-emerald-600 bg-emerald-50/50 dark:bg-emerald-950/20">
            <Shield size={16} className="text-emerald-600" />
            <span className="text-sm font-bold text-emerald-700 dark:text-emerald-400">
              人気・オッズは一切不使用 — 純粋なデータ分析のみ
            </span>
          </div>
        </div>
      </div>

      {/* ================================================================ */}
      {/* セクション3: 7因子コンポジット */}
      {/* ================================================================ */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <TrendingUp size={18} className="text-teal-600" />
          <h2 className="text-lg font-bold text-foreground">7因子コンポジット</h2>
        </div>
        <p className="text-xs text-muted-foreground mb-4">
          全評価を偏差値 20〜100 スケール（中央 50）で統一。7因子の加重平均で総合力を算出。
        </p>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {SEVEN_FACTORS.map((f) => {
            const Icon = f.icon;
            return (
              <PremiumCard key={f.key} variant="soft" padding="sm" className={`${f.border} border`}>
                <div className="flex items-start gap-3">
                  <div className={`w-9 h-9 rounded-lg ${f.bg} flex items-center justify-center shrink-0`}>
                    <Icon size={18} className={f.color} />
                  </div>
                  <div className="min-w-0">
                    <h4 className={`text-sm font-bold ${f.color}`}>{f.label}</h4>
                    <p className="text-[11px] text-muted-foreground leading-snug mt-0.5">{f.desc}</p>
                  </div>
                </div>
              </PremiumCard>
            );
          })}

          {/* 偏差値スケール説明カード */}
          <PremiumCard variant="soft" padding="sm" className="border border-border sm:col-span-2 lg:col-span-1">
            <div className="flex items-start gap-3">
              <div className="w-9 h-9 rounded-lg bg-muted flex items-center justify-center shrink-0">
                <BarChart3 size={18} className="text-foreground" />
              </div>
              <div className="min-w-0">
                <h4 className="text-sm font-bold text-foreground">偏差値スケール</h4>
                <p className="text-[11px] text-muted-foreground leading-snug mt-0.5">
                  全項目 20〜100 で統一。50 が平均、65以上が SS ランク。
                  人気順位やオッズによる補正は一切なし。
                </p>
              </div>
            </div>
          </PremiumCard>
        </div>
      </div>

      {/* ================================================================ */}
      {/* セクション4: 印体系 v5 */}
      {/* ================================================================ */}
      <PremiumCard variant="default" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Target size={10} className="inline mr-1" />
              <span className="section-eyebrow">Mark System v5</span>
            </PremiumCardAccent>
            <PremiumCardTitle>印体系</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div className="space-y-4">
          {/* 印カード横並び */}
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2">
            {MARKS_V5.map((m) => (
              <div
                key={m.symbol}
                className={`flex flex-col items-center gap-1.5 p-3 rounded-lg border ${m.border} ${m.bg} transition-all hover:scale-[1.02]`}
              >
                <span className={`text-2xl font-extrabold ${m.color}`}>
                  {m.symbol}
                </span>
                <span className={`text-xs font-bold ${m.color}`}>{m.label}</span>
                <span className="text-[10px] text-muted-foreground text-center leading-tight">
                  {m.desc}
                </span>
              </div>
            ))}
          </div>
          {/* 注記 */}
          <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-muted/50 border border-border/60">
            <span className="text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">v5 変更点:</span>{" "}
              ×（危険）印は廃止。全馬に前向きな評価軸のみで判定します。
            </span>
          </div>
        </div>
      </PremiumCard>

      {/* ================================================================ */}
      {/* セクション5: 自信度ランク */}
      {/* ================================================================ */}
      <PremiumCard variant="default" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Shield size={10} className="inline mr-1" />
              <span className="section-eyebrow">Confidence Rank</span>
            </PremiumCardAccent>
            <PremiumCardTitle>自信度ランク</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div className="space-y-4">
          {/* ランクバッジ横並び */}
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-2">
            {CONFIDENCE_RANKS.map((r) => (
              <div
                key={r.rank}
                className={`flex flex-col items-center gap-1 p-3 rounded-lg border ${r.border} ${r.bg} transition-all`}
              >
                <span className={`text-xl font-extrabold ${r.color}`}>
                  {r.rank}
                </span>
                <span className="text-[10px] font-semibold text-muted-foreground">
                  {r.threshold}
                </span>
                <span className="text-[10px] text-muted-foreground text-center leading-tight mt-0.5">
                  {r.desc}
                </span>
              </div>
            ))}
          </div>

          {/* 閾値テーブル（コンパクト版） */}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left px-3 py-1.5 text-muted-foreground font-medium">ランク</th>
                  <th className="text-center px-3 py-1.5 text-muted-foreground font-medium">閾値</th>
                  <th className="text-left px-3 py-1.5 text-muted-foreground font-medium">判定基準</th>
                </tr>
              </thead>
              <tbody>
                {CONFIDENCE_RANKS.map((r) => (
                  <tr key={r.rank} className="border-b border-border/40">
                    <td className="px-3 py-1.5">
                      <span className={`font-extrabold ${r.color}`}>{r.rank}</span>
                    </td>
                    <td className="text-center px-3 py-1.5 font-mono text-muted-foreground">{r.threshold}</td>
                    <td className="px-3 py-1.5 text-muted-foreground">{r.desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-muted/50 border border-border/60">
            <span className="text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">市場フリー:</span>{" "}
              6信号スコア（composite gap・ML一致・多因子一致等）で算出。JRA/NAR別パーセンタイル閾値で判定。
              SS/S は win_prob・gap ゲートで厳格フィルタ。
            </span>
          </div>
        </div>
      </PremiumCard>

      {/* ================================================================ */}
      {/* セクション6: M'戦略（買い目） */}
      {/* ================================================================ */}
      <PremiumCard variant="navy-glow" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Sparkles size={10} className="inline mr-1" />
              <span className="section-eyebrow">Betting Strategy</span>
            </PremiumCardAccent>
            <PremiumCardTitle>M' 戦略 — 三連複動的フォーメーション</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div className="space-y-4">
          {/* フォーメーション3列 */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {/* 1列目 */}
            <div className="rounded-lg border-2 border-emerald-400 dark:border-emerald-600 bg-emerald-50/50 dark:bg-emerald-950/20 p-4 text-center space-y-2">
              <div className="text-xs font-semibold text-emerald-700 dark:text-emerald-400 uppercase tracking-wider">1列目（軸）</div>
              <div className="flex justify-center gap-2">
                <span className="text-2xl font-extrabold text-emerald-600">◉</span>
                <span className="text-2xl font-extrabold text-emerald-600">◎</span>
              </div>
              <p className="text-[11px] text-muted-foreground">本命 — 最も信頼できる2頭</p>
            </div>

            {/* 2列目 */}
            <div className="rounded-lg border-2 border-blue-400 dark:border-blue-600 bg-blue-50/50 dark:bg-blue-950/20 p-4 text-center space-y-2">
              <div className="text-xs font-semibold text-blue-700 dark:text-blue-400 uppercase tracking-wider">2列目（相手）</div>
              <div className="flex justify-center gap-2">
                <span className="text-xl font-extrabold text-emerald-600">◉</span>
                <span className="text-xl font-extrabold text-emerald-600">◎</span>
                <span className="text-xl font-extrabold text-blue-600">○</span>
                <span className="text-xl font-extrabold text-red-600">▲</span>
              </div>
              <p className="text-[11px] text-muted-foreground">対抗まで — 連対圏候補</p>
            </div>

            {/* 3列目 */}
            <div className="rounded-lg border-2 border-purple-400 dark:border-purple-600 bg-purple-50/50 dark:bg-purple-950/20 p-4 text-center space-y-2">
              <div className="text-xs font-semibold text-purple-700 dark:text-purple-400 uppercase tracking-wider">3列目（広げ）</div>
              <div className="flex justify-center gap-1.5">
                <span className="text-lg font-extrabold text-emerald-600">◉</span>
                <span className="text-lg font-extrabold text-emerald-600">◎</span>
                <span className="text-lg font-extrabold text-blue-600">○</span>
                <span className="text-lg font-extrabold text-red-600">▲</span>
                <span className="text-lg font-extrabold text-purple-600">△</span>
                <span className="text-lg font-extrabold text-foreground">★</span>
                <span className="text-lg font-extrabold text-blue-600">☆</span>
              </div>
              <p className="text-[11px] text-muted-foreground">全印 + 穴候補で網羅</p>
            </div>
          </div>

          {/* 自動スキップ注記 */}
          <div className="flex items-start gap-2 px-3 py-2 rounded-md bg-muted/50 border border-border/60">
            <Shield size={14} className="text-muted-foreground shrink-0 mt-0.5" />
            <span className="text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">ROI最適化:</span>{" "}
              NAR C/D ランクは自動スキップ。低信頼レースを除外することで長期回収率を改善。
              買い点数は自信度に応じて動的に調整されます。
            </span>
          </div>
        </div>
      </PremiumCard>

      {/* ================================================================ */}
      {/* セクション7: 特徴量重要度（既存テーブル維持） */}
      {/* ================================================================ */}
      <PremiumCard variant="default" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <BarChart3 size={10} className="inline mr-1" />
              <span className="section-eyebrow">Feature Importance</span>
            </PremiumCardAccent>
            <PremiumCardTitle>特徴量重要度</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div>
          {isLoading && (
            <p className="text-sm text-muted-foreground">読み込み中...</p>
          )}
          {error && (
            <p className="text-sm text-destructive">
              読み込み失敗: {(error as Error).message}
            </p>
          )}
          {data && (
            <div className="space-y-3">
              {/* カテゴリフィルター */}
              <div className="flex gap-1.5 flex-wrap items-center">
                <span className="text-xs text-muted-foreground mr-1">カテゴリ:</span>
                <button
                  onClick={() => setFilterCat("all")}
                  className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                    filterCat === "all"
                      ? "bg-primary text-primary-foreground border-primary"
                      : "bg-secondary text-secondary-foreground border-border hover:bg-muted"
                  }`}
                >
                  全て({data.length})
                </button>
                {Object.keys(CAT_STYLES).map((cat) =>
                  catCounts[cat] ? (
                    <button
                      key={cat}
                      onClick={() => setFilterCat(cat)}
                      className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                        filterCat === cat
                          ? `${CAT_STYLES[cat].bg} ${CAT_STYLES[cat].text} ${CAT_STYLES[cat].border}`
                          : `bg-secondary text-secondary-foreground ${CAT_STYLES[cat].border} hover:${CAT_STYLES[cat].bg}`
                      }`}
                    >
                      {cat}({catCounts[cat]})
                    </button>
                  ) : null
                )}
              </div>

              {/* 検索 */}
              <Input
                type="text"
                placeholder="特徴量名・説明で検索..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full"
              />

              {/* テーブル */}
              <div className="max-h-[540px] overflow-y-auto border rounded-md">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-muted">
                    <tr>
                      <th className="text-right px-2 py-1.5 section-eyebrow w-10">
                        #
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow">
                        特徴量名
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow hidden sm:table-cell">
                        説明
                      </th>
                      <th className="text-center px-2 py-1.5 section-eyebrow w-20">
                        カテゴリ
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow w-40">
                        寄与度
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((d: FeatureImportanceItem) => {
                      const barW = Math.max(1, Math.round((d.pct / maxPct) * 100));
                      const pctStr = d.pct >= 0.01 ? `${d.pct.toFixed(2)}%` : "<0.01%";
                      const cs = CAT_STYLES[d.cat] ?? CAT_STYLES.コース;
                      const barColor = BAR_COLORS[d.cat] ?? "bg-gray-400";
                      return (
                        <tr key={d.rank} className="border-t hover:bg-brand-gold/5 transition-colors">
                          <td className="text-right px-2 py-1 stat-mono text-xs text-muted-foreground">
                            {d.rank}
                          </td>
                          <td className="px-2 py-1 text-xs font-semibold whitespace-nowrap">
                            {d.label || d.name}
                          </td>
                          <td className="px-2 py-1 text-xs text-muted-foreground leading-snug hidden sm:table-cell">
                            {d.desc || ""}
                          </td>
                          <td className="px-2 py-1 text-center">
                            <Badge
                              variant="outline"
                              className={`text-[10px] ${cs.bg} ${cs.text} ${cs.border}`}
                            >
                              {d.cat}
                            </Badge>
                          </td>
                          <td className="px-2 py-1">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
                                <div
                                  className={`h-full ${barColor} rounded-full`}
                                  style={{ width: `${barW}%` }}
                                />
                              </div>
                              <span className="stat-mono text-[10px] text-muted-foreground w-12 text-right">
                                {pctStr}
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </PremiumCard>
    </div>
  );
}
