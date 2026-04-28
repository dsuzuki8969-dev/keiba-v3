/**
 * 競馬固有の定数
 */

// 会場コード → 会場名マッピング
export const VENUE_MAP: Record<string, string> = {
  "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
  "05": "東京", "06": "中山", "07": "中京", "08": "京都",
  "09": "阪神", "10": "小倉",
  // NAR
  "30": "門別", "35": "盛岡", "36": "水沢", "42": "浦和",
  "43": "船橋", "44": "大井", "45": "川崎", "46": "金沢",
  "47": "笠松", "48": "名古屋", "50": "園田", "51": "姫路",
  "54": "高知", "55": "佐賀", "65": "帯広",
};

export const JRA_VENUE_CODES = ["01","02","03","04","05","06","07","08","09","10"];
export const NAR_VENUE_CODES = ["30","35","36","42","43","44","45","46","47","48","50","51","54","55","65"];

// 会場名 → netkeibaコード（コース画像ファイル名用）
export const VENUE_NAME_TO_CODE: Record<string, string> = Object.fromEntries(
  Object.entries(VENUE_MAP).map(([k, v]) => [v, k])
);

// 信頼度ランク
export const CONFIDENCE_RANKS = ["SS", "S", "A", "B", "C", "D", "F"] as const;
export type ConfidenceRank = typeof CONFIDENCE_RANKS[number];

// 印の定義
export const MARKS = {
  tekipan:  { symbol: "◉", label: "鉄板" },
  honmei:   { symbol: "◎", label: "本命" },
  taikou:   { symbol: "○", label: "対抗" },
  tannuke:  { symbol: "▲", label: "単穴" },
  rendashi: { symbol: "△", label: "連下" },
  rendashi2:{ symbol: "★", label: "連下2" },
  oana:     { symbol: "☆", label: "穴馬" },
  kiken:    { symbol: "×", label: "危険" },
} as const;

export type MarkType = keyof typeof MARKS;

// 馬場種別
export const SURFACE_LABELS: Record<string, string> = {
  turf: "芝",
  dirt: "ダート",
  obstacle: "障害",
};

// タブ定義
export const TABS = [
  { key: "home", label: "ホーム", shortLabel: "ホーム", path: "/home" },
  { key: "today", label: "本日予想", shortLabel: "予想", path: "/today" },
  { key: "results", label: "過去成績", shortLabel: "成績", path: "/results" },
  // T-038: 開催カレンダーページ（過去成績と競馬場研究の間）
  { key: "calendar", label: "開催カレンダー", shortLabel: "カレンダー", path: "/calendar" },
  { key: "venue", label: "競馬場研究", shortLabel: "競馬場", path: "/venue" },
  { key: "db", label: "データベース", shortLabel: "データ", path: "/db" },
  { key: "about", label: "About", shortLabel: "About", path: "/about" },
] as const;

// JRA会場コードセット
export const JRA_CODES = new Set(JRA_VENUE_CODES);

// 信頼度レベル（ソート用）
export function confLevel(c: string): number {
  const map: Record<string, number> = {
    SS: 6, "S+": 5, S: 4, "A+": 3, A: 2, "B+": 1, B: 0, C: 0,
  };
  return map[c.replace(/\u207a/g, "+")] ?? 0;
}

// 信頼度に対応するTailwindクラス（5色体系: SS=緑, S=青, A=赤, B=黒, C/D=灰）
export function confColorClass(c: string): string {
  const clean = (c || "").replace(/\u207a/g, "+");
  if (clean === "SS") return "text-emerald-600 font-bold";
  if (clean.startsWith("S")) return "text-blue-600 font-bold";
  if (clean.startsWith("A")) return "text-red-600 font-bold";
  if (clean.startsWith("B")) return "text-foreground font-bold";
  return "text-muted-foreground";
}

// 馬場短縮ラベル
export function surfShort(surf: string): string {
  if (surf === "ダート") return "ダ";
  return surf || "";
}

// ローカル日付 (YYYY-MM-DD)
export function localDate(d?: Date): string {
  const dt = d || new Date();
  return (
    dt.getFullYear() +
    "-" +
    String(dt.getMonth() + 1).padStart(2, "0") +
    "-" +
    String(dt.getDate()).padStart(2, "0")
  );
}

// 経過秒 → "X分Y秒"
export function fmtTime(s: number): string {
  return s < 60 ? s + "秒" : Math.floor(s / 60) + "分" + (s % 60) + "秒";
}

// 丸囲み数字
export function circledNum(n: number): string {
  const chars = "①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱";
  return n >= 1 && n <= 18 ? chars[n - 1] : String(n);
}

// 枠番背景色（JRA/NAR 共通8枠）
export const WAKU_BG: Record<number, string> = {
  1: "bg-white text-black border border-gray-300",
  2: "bg-black text-white",
  3: "bg-red-600 text-white",
  4: "bg-blue-600 text-white",
  5: "bg-yellow-400 text-black",
  6: "bg-green-600 text-white",
  7: "bg-orange-500 text-white",
  8: "bg-pink-400 text-white",
};

// 走破タイムフォーマット M:SS.S
export function fmtRaceTime(sec: number | null | undefined): string {
  if (!sec) return "—";
  const m = Math.floor(sec / 60);
  const s = (sec % 60).toFixed(1);
  return `${m}:${s.padStart(4, "0")}`;
}

// 偏差値グレード（7段階）— バックエンド dev_to_grade() と閾値統一（20-100スケール、B=50中央）
export function devGrade(v: number): string {
  if (v >= 65) return "SS";
  if (v >= 60) return "S";
  if (v >= 55) return "A";
  if (v >= 50) return "B";
  if (v >= 45) return "C";
  if (v >= 35) return "D";
  return "E";
}

/** @deprecated devGrade に統一 */
export const indexGrade = devGrade;

// ============================================================
// 5色体系: 緑=最良, 青=良好, 赤=注意, 黒=普通, 灰=低調
// ============================================================

/** グレード → Tailwindクラス（SS=緑, S=青, A=赤, B/C=黒, D/E=灰） */
export function gradeCls(g: string): string {
  switch (g) {
    case "SS": return "text-emerald-600 font-bold";
    case "S":  return "text-blue-600 font-bold";
    case "A":  return "text-red-600 font-bold";
    case "B":  return "text-foreground font-bold";
    case "C":  return "text-foreground";
    case "D":  return "text-muted-foreground";
    case "E":  return "text-muted-foreground/60";
    default:   return "text-foreground";
  }
}

/** 順位(1-based) → Tailwindクラス（1位=緑, 2位=青, 3位=赤, 4位=紫, 5+黒） */
export function rankCls(rank: number): string {
  if (rank === 1) return "text-emerald-600 font-bold";
  if (rank === 2) return "text-blue-600 font-bold";
  if (rank === 3) return "text-red-600 font-bold";
  if (rank === 4) return "text-purple-600 font-bold";
  return "";
}

/** 着順 → Tailwindクラス（1着=緑, 2着=青, 3着=赤, 4着=紫, 5+黒） */
export function posCls(pos: number | null | undefined): string {
  if (!pos) return "";
  if (pos === 1) return "text-emerald-600 font-bold";
  if (pos === 2) return "text-blue-600 font-bold";
  if (pos === 3) return "text-red-600 font-bold";
  if (pos === 4) return "text-purple-600 font-bold";
  return "";
}

/** 印 → Tailwindクラス（◉/◎=緑, ○/☆=青, ▲/×=赤, △=紫, ★=黒） */
export function markCls(mark: string): string {
  switch (mark) {
    case "◉": return "text-emerald-600 font-extrabold";
    case "◎": return "text-emerald-600 font-extrabold";
    case "○": return "text-blue-600 font-bold";
    case "▲": return "text-red-600 font-bold";
    case "△": return "text-purple-600 font-bold";
    case "★": return "text-foreground font-bold";
    case "☆": return "text-blue-600 font-bold";
    case "×": return "text-red-600 font-bold";
    default:  return "text-muted-foreground";
  }
}

/** EV(期待値) → Tailwindクラス（120%+=緑, 110-119%=青, 100-109%=赤） */
export function evCls(ev: number): string {
  if (ev >= 1.20) return "text-emerald-600 font-bold";
  if (ev >= 1.10) return "text-blue-600 font-bold";
  if (ev >= 1.00) return "text-red-600";
  return "text-muted-foreground";
}


/** トレンド → Tailwindクラス（急上昇/上昇=緑, 横ばい/安定=黒, 下降/急下降=灰） */
export function trendCls(trend: string | undefined): string {
  if (!trend) return "";
  if (trend === "急上昇" || trend === "上昇") return "text-emerald-600 font-bold";
  if (trend === "安定" || trend === "横ばい") return "";
  return "text-muted-foreground";
}

/** 馬体重変化 → Tailwindクラス（±0-4=緑, ±5-9=青, ±10+=赤） */
export function weightChgCls(chg: number | null | undefined): string {
  if (chg == null) return "";
  const abs = Math.abs(chg);
  if (abs <= 4) return "text-emerald-600";
  if (abs <= 9) return "text-blue-600";
  return "text-red-600 font-bold";
}

// NAR馬場コード（keiba.go.jp用）
const NAR_BABA_CODE: Record<string, string> = {
  "帯広": "3", "門別": "36", "盛岡": "10", "水沢": "11",
  "浦和": "18", "船橋": "19", "大井": "20", "川崎": "21",
  "金沢": "22", "笠松": "23", "名古屋": "24", "園田": "27",
  "姫路": "28", "高知": "31", "佐賀": "32",
};

/**
 * 過去走のレース結果URL生成
 * JRA(CNAME有) → JRA公式 (jra.go.jp/JRADB)
 * NAR(race_id有) → NAR公式 個別レース (keiba.go.jp/RaceMarkTable)
 * NAR(race_id無) → NAR公式 個別レース (keiba.go.jp/RaceMarkTable, race_no推定)
 * JRA(CNAME無,race_id有) → netkeiba DB (db.netkeiba.com/race/)
 * fallback → NAR開催一覧 or JRA成績一覧
 */
export function pastRunResultUrl(
  raceId: string | undefined,
  date: string | undefined,
  venue: string | undefined,
  resultCname?: string,
  raceNo?: number,
): string {
  // JRA公式（result_cnameがある場合）
  if (resultCname) {
    return `https://www.jra.go.jp/JRADB/accessS.html?CNAME=${resultCname}`;
  }

  // race_idが12桁以上ある場合
  if (raceId && raceId.length >= 12) {
    const vc = raceId.slice(4, 6);
    const isJra = parseInt(vc) <= 10;

    if (!isJra && date && venue) {
      // NAR: race_idからレース番号を抽出して公式個別レースへ
      const baba = NAR_BABA_CODE[venue];
      if (baba) {
        const ds = date.replace(/-/g, "");
        const d = `${ds.slice(0, 4)}/${ds.slice(4, 6)}/${ds.slice(6, 8)}`;
        const rno = parseInt(raceId.slice(10, 12));
        return `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=${encodeURIComponent(d)}&k_raceNo=${rno}&k_babaCode=${baba}`;
      }
    }

    // JRA(CNAME無) → netkeiba DBで個別レース結果を表示
    // JRA公式にはCNAMEなしでアクセスできるURLがないため
    return `https://db.netkeiba.com/race/${raceId}/`;
  }

  // race_id無し: NAR会場ならraceNoパラメータで個別レースリンク試行
  if (date && venue) {
    const baba = NAR_BABA_CODE[venue];
    if (baba) {
      const ds = date.replace(/-/g, "");
      const d = `${ds.slice(0, 4)}/${ds.slice(4, 6)}/${ds.slice(6, 8)}`;
      if (raceNo && raceNo > 0) {
        // レース番号指定あり → 個別レースへ
        return `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=${encodeURIComponent(d)}&k_raceNo=${raceNo}&k_babaCode=${baba}`;
      }
      // レース番号不明 → 開催一覧
      return `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceList?k_raceDate=${encodeURIComponent(d)}&k_babaCode=${baba}`;
    }
  }

  return "";
}
