/**
 * SVGコース図コンポーネント — Phase 2
 * JRA各場のコース形状をnetkeiba図に準拠して忠実再現
 * トラック幅表現、グリーン内馬場、コーナー番号、距離別スタート位置、
 * ラスト3F赤ハイライト、起伏プロファイル
 *
 * Phase 1 修正:
 *   A. 坂計算バグ修正: hillStart / dist → hillStart / track.perimeter
 *   B. ダート専用 perimeter 追加（10場）
 *   C. 主要ポケット発走15コースに枝SVG＋手動スタート座標
 *   D. スタート位置ロジック: ポケット優先 → 周長逆算 fallback
 */

// --- 型定義 ---
interface PocketEntry {
  /** ポケット枝SVGパス（楕円本体から外側に伸びる短い線） */
  branchPath: string;
  /** Sマーカー座標（枝の先端）[x, y] */
  startPos: [number, number];
  /** どのコーナーから出るか（表示用ラベル）"2C" "1C" "4C" 等 */
  cornerLabel: string;
}

/**
 * Phase 2-A: コース別オーバーライド用の路面・内外回り別パス定義
 * 芝内回り / ダート専用 / 千直など通常トップレベル path と異なる場合に使用
 */
interface CoursePath {
  /** 1周距離(m) */
  perimeter: number;
  /** ゴール前直線(m) */
  straight: number;
  /** SVGパス（viewBox 0 0 100 70内） */
  path: string;
  /** ゴール座標 [x, y] */
  goalPos: [number, number];
  /** コーナー割合 [4C, 3C, 2C, 1C] — 直線コースは空配列 */
  corners: [number, number, number, number] | [];
  /** 起伏プロファイル */
  elevation?: { pos: number; h: number }[];
  /** ポケット発走定義（芝内回りで使う場合など） */
  pockets?: Record<string, PocketEntry>;
  /** 坂情報 */
  hillStart?: number;
  hillEnd?: number;
  hillHeight?: number;
  /** 直線専用コースフラグ（新潟千直・帯広等） */
  isStraight?: boolean;
  /** 回り方向オーバーライド（大井左回り等、親トラックと異なる場合） */
  direction?: "右" | "左";
}

interface VenueTrack {
  direction: "右" | "左";
  // SVGパス（viewBox 0 0 100 70内、CW方向で描画）
  path: string;
  goalPos: [number, number];
  // コーナー割合（orderedPoints上の位置 0=ゴール付近、1=スタート付近）
  // [4C, 3C, 2C, 1C] の順（ゴールから逆走方向で遭遇する順）
  corners: [number, number, number, number];
  // 起伏プロファイル: pos=0がゴール、pos=1が1周
  elevation: { pos: number; h: number }[];
  // 坂情報（直線部分）
  hillStart?: number;
  hillEnd?: number;
  hillHeight?: number;
  // 1周距離(m) — 芝外回り基準
  perimeter: number;
  // ダート専用周長(m) — Phase 1B で追加
  dirtPerimeter?: number;
  // ゴール前直線(m)
  straight: number;
  // ポケット発走コース定義 — Phase 1C で追加
  // キー: "surfaceCode_distance" 例: "芝外1600", "ダ1400"
  pockets?: Record<string, PocketEntry>;
  // Phase 2-A: コース別オーバーライド
  // キー: "芝内" | "芝外" | "ダ" | "芝1000直" 等
  variants?: Record<string, CoursePath>;
}

// --- JRA 10場のトラックデータ ---
// パスはCW方向（screen座標）で描画。ゴールは底辺上。
// 左回り: ゴールから前方(CW)=逆走方向。右回り: ゴールから後方(CCW)=逆走方向。
//
// ポケット枝SVGの座標設計:
//   viewBox: 100×80（SVGは100×92だが楕円が70内に収まる）
//   各コーナーのトラック座標から center(50,33) と逆方向に 8〜10 単位伸ばした先が S 位置
//   branchPath: "M トラック上 L 枝先端" の短いSVGパス
const TRACKS: Record<string, VenueTrack> = {
  // === 札幌 === ほぼ正円形、平坦、右回り
  "札幌": {
    direction: "右", perimeter: 1641, dirtPerimeter: 1487, straight: 266,
    path: "M 56,57 L 44,57 C 20,57 6,48 6,33 C 6,18 20,9 44,9 L 56,9 C 80,9 94,18 94,33 C 94,48 80,57 56,57 Z",
    goalPos: [50, 57],
    corners: [0.14, 0.36, 0.64, 0.86],
    elevation: [
      { pos: 0, h: 0.5 }, { pos: 0.25, h: 0.6 }, { pos: 0.5, h: 0.5 },
      { pos: 0.75, h: 0.4 }, { pos: 1, h: 0.5 },
    ],
  },
  // === 函館 === やや楕円、起伏あり、右回り
  "函館": {
    direction: "右", perimeter: 1626, dirtPerimeter: 1476, straight: 262,
    path: "M 58,57 L 42,57 C 19,57 5,48 5,33 C 5,18 19,9 42,9 L 58,9 C 81,9 95,18 95,33 C 95,48 81,57 58,57 Z",
    goalPos: [50, 57],
    corners: [0.14, 0.36, 0.64, 0.86],
    elevation: [
      { pos: 0, h: 0.3 }, { pos: 0.15, h: 0.4 }, { pos: 0.3, h: 0.7 },
      { pos: 0.5, h: 1.0 }, { pos: 0.7, h: 0.7 }, { pos: 0.85, h: 0.3 },
      { pos: 1, h: 0.3 },
    ],
  },
  // === 福島 === 最小JRA、タイトコーナー、右回り
  "福島": {
    direction: "右", perimeter: 1600, dirtPerimeter: 1444, straight: 292,
    path: "M 62,57 L 38,57 C 18,57 6,48 6,33 C 6,18 18,9 38,9 L 62,9 C 82,9 94,18 94,33 C 94,48 82,57 62,57 Z",
    goalPos: [50, 57],
    corners: [0.15, 0.36, 0.64, 0.85],
    elevation: [
      { pos: 0, h: 0.5 }, { pos: 0.1, h: 0.7 }, { pos: 0.15, h: 0.7 },
      { pos: 0.3, h: 0.2 }, { pos: 0.5, h: 0.0 }, { pos: 0.7, h: 0.5 },
      { pos: 0.85, h: 0.3 }, { pos: 1, h: 0.5 },
    ],
    hillStart: 170, hillEnd: 50, hillHeight: 1.2,
  },
  // === 新潟 === 超横長（外回り）、平坦、左回り
  "新潟": {
    direction: "左", perimeter: 2223, dirtPerimeter: 1472, straight: 659,
    path: "M 87,53 L 13,53 C 4,53 2,45 2,35 C 2,25 4,17 13,17 L 87,17 C 96,17 98,25 98,35 C 98,45 96,53 87,53 Z",
    goalPos: [50, 53],
    corners: [0.24, 0.34, 0.66, 0.76],
    elevation: [
      { pos: 0, h: 0.3 }, { pos: 0.3, h: 0.3 }, { pos: 0.5, h: 0.3 },
      { pos: 0.65, h: 1.0 }, { pos: 0.75, h: 0.5 }, { pos: 0.85, h: 0.3 },
      { pos: 1, h: 0.3 },
    ],
    // Phase 2-A: 新潟バリアント
    variants: {
      // 芝内回り（1623m）— 外回りより縦方向をコンパクトにした横長楕円
      "芝内": {
        perimeter: 1623,
        straight: 359,
        path: "M 82,51 L 18,51 C 8,51 5,44 5,35 C 5,26 8,19 18,19 L 82,19 C 92,19 95,26 95,35 C 95,44 92,51 82,51 Z",
        goalPos: [50, 51],
        corners: [0.24, 0.34, 0.66, 0.76],
        elevation: [
          { pos: 0, h: 0.3 }, { pos: 0.3, h: 0.3 }, { pos: 0.5, h: 0.3 },
          { pos: 0.65, h: 1.0 }, { pos: 0.75, h: 0.5 }, { pos: 0.85, h: 0.3 },
          { pos: 1, h: 0.3 },
        ],
      },
      // 芝1000m 千直: 完全直線コース（新潟独自）
      // ゴールは右端、スタートは左端（左→右方向）
      "芝1000直": {
        perimeter: 1000,
        straight: 1000,
        path: "M 5,35 L 95,35",
        goalPos: [95, 35],
        corners: [],
        elevation: [],
        isStraight: true,
      },
    },
  },
  // === 東京 === 大型横長、長い直線、府中の坂、左回り
  // ポケット発走コース:
  //   芝1800: バックストレート内側(1-2C間) → 左回りでバック右側
  //   芝2000: 1C外側（スタンド前）
  //   芝2500: 4C奥（2C方向から外側）
  //
  // 東京楕円 viewBox 100×70 での各コーナー位置推定:
  //   corners: [0.20, 0.34, 0.66, 0.80]
  //   4C(0.20): バック右 ≈ (8, 33)  → 外向き（左方向）: sx ≈ 0, sy=33
  //   3C(0.34): バック上 ≈ (23, 9)  → 外向き（上方向）: sx=23, sy=0
  //   2C(0.66): バック左 ≈ (77, 9)  → 外向き（上方向）: sx=77, sy=0
  //   1C(0.80): スタンド右 ≈ (92,33) → 外向き（右方向）: sx=100, sy=33
  "東京": {
    direction: "左", perimeter: 2083, dirtPerimeter: 1899, straight: 526,
    path: "M 77,57 L 23,57 C 8,57 3,47 3,33 C 3,19 8,9 23,9 L 77,9 C 92,9 97,19 97,33 C 97,47 92,57 77,57 Z",
    goalPos: [50, 57],
    corners: [0.20, 0.34, 0.66, 0.80],
    elevation: [
      { pos: 0, h: 0.35 }, { pos: 0.05, h: 0.35 }, { pos: 0.10, h: 0.70 },
      { pos: 0.15, h: 0.35 }, { pos: 0.30, h: 0.10 }, { pos: 0.50, h: 0.10 },
      { pos: 0.65, h: 0.50 }, { pos: 0.75, h: 0.30 }, { pos: 0.85, h: 0.30 },
      { pos: 1, h: 0.35 },
    ],
    hillStart: 460, hillEnd: 300, hillHeight: 2.1,
    pockets: {
      // 東京芝1800: バックストレート内側（1-2C間付近）から発走
      // トラック上の 2C ≈ (77, 9) 付近のバック直線中央から外向き上に伸ばす
      "芝1800": {
        cornerLabel: "1-2C間",
        startPos: [77, 1],
        branchPath: "M 77,9 L 77,1",
      },
      // 東京芝2000: 1C外側（スタンド前ポケット）
      // 1C ≈ (92, 33) 付近から右外向き
      "芝2000": {
        cornerLabel: "1C外側",
        startPos: [100, 28],
        branchPath: "M 92,33 L 100,28",
      },
      // 東京芝2500: 4C奥（2C外側）
      // 2C ≈ (77, 9) 付近から上外向き（少し左）
      "芝2500": {
        cornerLabel: "4C奥",
        startPos: [60, 1],
        branchPath: "M 68, 9 L 60,1",
      },
    },
  },
  // === 中山 === 独特のおにぎり型、急坂、右回り
  // ポケット発走コース:
  //   芝外1600: 1C外側（バック右=スタンド前右）
  //   芝外2200: 4C外側（バック左奥）
  //
  // 中山 corners: [0.13, 0.35, 0.65, 0.87]
  //   4C(0.13): スタンド右 ≈ (82, 38)
  //   3C(0.35): バック上右 ≈ (68, 7)
  //   2C(0.65): バック上左 ≈ (30, 7)
  //   1C(0.87): スタンド左 ≈ (18, 38)
  "中山": {
    direction: "右", perimeter: 1840, dirtPerimeter: 1493, straight: 310,
    path: [
      "M 57,58 L 43,58",
      "C 26,58 14,52 11,42",
      "C 8,30 14,16 30,9",
      "L 50,4",
      "L 70,9",
      "C 86,16 92,30 89,42",
      "C 86,52 74,58 57,58 Z",
    ].join(" "),
    goalPos: [50, 58],
    corners: [0.13, 0.35, 0.65, 0.87],
    elevation: [
      { pos: 0, h: 0.30 }, { pos: 0.05, h: 0.70 }, { pos: 0.10, h: 0.30 },
      { pos: 0.20, h: 0.50 }, { pos: 0.35, h: 0.90 }, { pos: 0.50, h: 1.0 },
      { pos: 0.65, h: 0.70 }, { pos: 0.80, h: 0.40 }, { pos: 0.90, h: 0.20 },
      { pos: 1, h: 0.30 },
    ],
    hillStart: 180, hillEnd: 70, hillHeight: 2.2,
    pockets: {
      // 中山芝外1600: 1C外側（バック右側から右向きポケット）
      // 1C ≈ (18, 38) 付近から左外向き
      "芝外1600": {
        cornerLabel: "1C外側",
        startPos: [3, 43],
        branchPath: "M 11,42 L 3,43",
      },
      // 中山芝外2200: 4C外側（スタンド右）
      // 4C ≈ (82, 38) 付近から右外向き
      "芝外2200": {
        cornerLabel: "4C外側",
        startPos: [99, 43],
        branchPath: "M 89,42 L 99,43",
      },
    },
    // Phase 2-A: 中山 芝内回り（1667m）
    // 外回り(1840m)より小さい「おにぎり型」コンパクト版。急坂は同じ
    variants: {
      "芝内": {
        perimeter: 1667,
        straight: 310,
        path: [
          "M 55,56 L 45,56",
          "C 29,56 18,50 15,41",
          "C 12,30 18,17 33,11",
          "L 50,6",
          "L 67,11",
          "C 82,17 88,30 85,41",
          "C 82,50 71,56 55,56 Z",
        ].join(" "),
        goalPos: [50, 56],
        corners: [0.13, 0.35, 0.65, 0.87],
        elevation: [
          { pos: 0, h: 0.30 }, { pos: 0.05, h: 0.70 }, { pos: 0.10, h: 0.30 },
          { pos: 0.20, h: 0.50 }, { pos: 0.35, h: 0.90 }, { pos: 0.50, h: 1.0 },
          { pos: 0.65, h: 0.70 }, { pos: 0.80, h: 0.40 }, { pos: 0.90, h: 0.20 },
          { pos: 1, h: 0.30 },
        ],
        hillStart: 180, hillEnd: 70, hillHeight: 2.2,
      },
    },
  },
  // === 中京 === 東京縮小型楕円、坂あり、左回り
  // ポケット発走コース:
  //   芝1400: 2C外側（バック左）
  //   芝1600: 1-2C間（バック直線上）
  //
  // 中京 corners: [0.19, 0.35, 0.65, 0.81]
  //   4C(0.19): バック右 ≈ (10, 33)
  //   3C(0.35): バック上左 ≈ (30, 9)
  //   2C(0.65): バック上右 ≈ (70, 9)
  //   1C(0.81): スタンド右 ≈ (90, 33)
  "中京": {
    direction: "左", perimeter: 1706, dirtPerimeter: 1530, straight: 413,
    path: "M 70,57 L 30,57 C 12,57 5,47 5,33 C 5,19 12,9 30,9 L 70,9 C 88,9 95,19 95,33 C 95,47 88,57 70,57 Z",
    goalPos: [50, 57],
    corners: [0.19, 0.35, 0.65, 0.81],
    elevation: [
      { pos: 0, h: 0.35 }, { pos: 0.05, h: 0.35 }, { pos: 0.12, h: 0.70 },
      { pos: 0.18, h: 0.35 }, { pos: 0.40, h: 0.20 }, { pos: 0.60, h: 0.30 },
      { pos: 0.80, h: 0.20 }, { pos: 1, h: 0.35 },
    ],
    hillStart: 400, hillEnd: 200, hillHeight: 2.0,
    pockets: {
      // 中京芝1400: 2C外側（バック左上）
      // 2C ≈ (70, 9) 付近から上外向き
      "芝1400": {
        cornerLabel: "2C外側",
        startPos: [78, 1],
        branchPath: "M 70,9 L 78,1",
      },
      // 中京芝1600: 1-2C間（バック直線上中央付近）
      // バック直線中央 ≈ (80, 9) 付近から上外向き
      "芝1600": {
        cornerLabel: "1-2C間",
        startPos: [88, 1],
        branchPath: "M 82,9 L 88,1",
      },
    },
  },
  // === 京都 === 横長楕円、3C付近に淀の丘、右回り
  // ポケット発走コース:
  //   芝外1400: 2C外側
  //   芝外1600: 2C外側（マスター指摘）
  //   芝外2200: 4C外側
  //   芝外2400: 4C外側
  //
  // 京都 corners: [0.17, 0.35, 0.65, 0.83]
  // 右回りなので orderedPoints は pathPoints をゴールから逆方向（CCW）に並べる
  //   4C(0.17): ゴール右（スタンド側右）≈ (80, 48)
  //   3C(0.35): バック右 ≈ (90, 20)
  //   2C(0.65): バック左 ≈ (10, 20)
  //   1C(0.83): ゴール左（スタンド側左）≈ (20, 48)
  "京都": {
    direction: "右", perimeter: 1894, dirtPerimeter: 1607, straight: 404,
    path: "M 72,57 L 28,57 C 10,57 3,47 3,33 C 3,19 10,9 28,9 L 72,9 C 90,9 97,19 97,33 C 97,47 90,57 72,57 Z",
    goalPos: [50, 57],
    corners: [0.17, 0.35, 0.65, 0.83],
    elevation: [
      { pos: 0, h: 0.20 }, { pos: 0.15, h: 0.20 }, { pos: 0.30, h: 0.20 },
      { pos: 0.50, h: 0.30 }, { pos: 0.60, h: 0.70 }, { pos: 0.70, h: 1.0 },
      { pos: 0.78, h: 0.50 }, { pos: 0.85, h: 0.20 }, { pos: 1, h: 0.20 },
    ],
    pockets: {
      // 京都芝外1400: 2C外側（バック左上）
      // 2C ≈ バック左上端 (10, 20) 付近から外向き（左上）
      "芝外1400": {
        cornerLabel: "2C外側",
        startPos: [1, 12],
        branchPath: "M 7,18 L 1,12",
      },
      // 京都芝外1600: 2C外側（マスター指摘の主要コース）
      // 2C ≈ (8, 15) 付近から左外向き
      "芝外1600": {
        cornerLabel: "2C外側",
        startPos: [1, 7],
        branchPath: "M 5,14 L 1,7",
      },
      // 京都芝外2200: 4C外側（スタンド右）
      // 4C ≈ ゴール右スタンド側 (88, 48) 付近から右外向き
      "芝外2200": {
        cornerLabel: "4C外側",
        startPos: [99, 52],
        branchPath: "M 90,48 L 99,52",
      },
      // 京都芝外2400: 4C外側（スタンド右・少し前寄り）
      "芝外2400": {
        cornerLabel: "4C外側",
        startPos: [99, 44],
        branchPath: "M 90,44 L 99,44",
      },
    },
    // Phase 2-A: 京都 芝内回り（1783m）
    // 外回り(1894m)よりやや縦にコンパクトな楕円。上辺・下辺を内側に絞る
    variants: {
      "芝内": {
        perimeter: 1783,
        straight: 328,
        path: "M 68,54 L 32,54 C 14,54 7,46 7,35 C 7,24 14,14 32,14 L 68,14 C 86,14 93,24 93,35 C 93,46 86,54 68,54 Z",
        goalPos: [50, 54],
        corners: [0.17, 0.35, 0.65, 0.83],
        elevation: [
          { pos: 0, h: 0.20 }, { pos: 0.15, h: 0.20 }, { pos: 0.30, h: 0.20 },
          { pos: 0.50, h: 0.30 }, { pos: 0.60, h: 0.70 }, { pos: 0.70, h: 1.0 },
          { pos: 0.78, h: 0.50 }, { pos: 0.85, h: 0.20 }, { pos: 1, h: 0.20 },
        ],
        // 芝内回りポケット発走: 1400/1600 は 3C外側
        pockets: {
          "芝内1400": {
            cornerLabel: "3C外側",
            startPos: [97, 14],
            branchPath: "M 90,14 L 97,14",
          },
          "芝内1600": {
            cornerLabel: "3C外側",
            startPos: [97, 7],
            branchPath: "M 91,13 L 97,7",
          },
        },
      },
    },
  },
  // === 阪神 === 大型楕円、直線坂、右回り
  // ポケット発走コース:
  //   芝外1600: 2C外側
  //   芝外1800: 2C外側
  //   芝外2200: 4C外側
  //   芝外2600: 4C外側
  //
  // 阪神 corners: [0.18, 0.35, 0.65, 0.82]
  // 右回りなので:
  //   4C(0.18): ゴール右（スタンド側右）≈ (88, 46)
  //   3C(0.35): バック右上 ≈ (92, 20)
  //   2C(0.65): バック左上 ≈ (8, 20)
  //   1C(0.82): ゴール左（スタンド側左）≈ (12, 46)
  "阪神": {
    direction: "右", perimeter: 2089, dirtPerimeter: 1517, straight: 474,
    path: "M 76,57 L 24,57 C 8,57 2,47 2,33 C 2,19 8,9 24,9 L 76,9 C 92,9 98,19 98,33 C 98,47 92,57 76,57 Z",
    goalPos: [50, 57],
    corners: [0.18, 0.35, 0.65, 0.82],
    elevation: [
      { pos: 0, h: 0.35 }, { pos: 0.05, h: 0.60 }, { pos: 0.10, h: 0.35 },
      { pos: 0.30, h: 0.20 }, { pos: 0.50, h: 0.20 }, { pos: 0.70, h: 0.30 },
      { pos: 0.85, h: 0.20 }, { pos: 1, h: 0.35 },
    ],
    hillStart: 200, hillEnd: 90, hillHeight: 1.8,
    pockets: {
      // 阪神芝外1600: 2C外側（バック左上）
      // 2C ≈ バック左端 (6, 16) 付近から左外向き
      "芝外1600": {
        cornerLabel: "2C外側",
        startPos: [1, 8],
        branchPath: "M 5,15 L 1,8",
      },
      // 阪神芝外1800: 2C外側（やや前寄り）
      "芝外1800": {
        cornerLabel: "2C外側",
        startPos: [1, 3],
        branchPath: "M 5,10 L 1,3",
      },
      // 阪神芝外2200: 4C外側（スタンド右）
      // 4C ≈ (90, 46) 付近から右外向き
      "芝外2200": {
        cornerLabel: "4C外側",
        startPos: [99, 50],
        branchPath: "M 91,47 L 99,50",
      },
      // 阪神芝外2600: 4C外側（さらに前寄り）
      "芝外2600": {
        cornerLabel: "4C外側",
        startPos: [99, 44],
        branchPath: "M 91,42 L 99,44",
      },
    },
    // Phase 2-A: 阪神 芝内回り（1689m）
    // 外回り(2089m)より大幅に小さい楕円。直線も短い(360m)
    variants: {
      "芝内": {
        perimeter: 1689,
        straight: 360,
        path: "M 68,54 L 32,54 C 14,54 7,46 7,35 C 7,24 14,14 32,14 L 68,14 C 86,14 93,24 93,35 C 93,46 86,54 68,54 Z",
        goalPos: [50, 54],
        corners: [0.18, 0.35, 0.65, 0.82],
        elevation: [
          { pos: 0, h: 0.35 }, { pos: 0.05, h: 0.60 }, { pos: 0.10, h: 0.35 },
          { pos: 0.30, h: 0.20 }, { pos: 0.50, h: 0.20 }, { pos: 0.70, h: 0.30 },
          { pos: 0.85, h: 0.20 }, { pos: 1, h: 0.35 },
        ],
        hillStart: 200, hillEnd: 90, hillHeight: 1.8,
        pockets: {
          // 阪神芝内1400: 3C外側（バック右上）
          "芝内1400": {
            cornerLabel: "3C外側",
            startPos: [97, 14],
            branchPath: "M 90,14 L 97,14",
          },
        },
      },
    },
  },
  // === 小倉 === コンパクト楕円、平坦直線、右回り
  "小倉": {
    direction: "右", perimeter: 1615, dirtPerimeter: 1445, straight: 293,
    path: "M 64,57 L 36,57 C 16,57 5,48 5,33 C 5,18 16,9 36,9 L 64,9 C 84,9 95,18 95,33 C 95,48 84,57 64,57 Z",
    goalPos: [50, 57],
    corners: [0.15, 0.36, 0.64, 0.85],
    elevation: [
      { pos: 0, h: 0.30 }, { pos: 0.20, h: 0.50 }, { pos: 0.40, h: 0.80 },
      { pos: 0.50, h: 1.0 }, { pos: 0.70, h: 0.50 }, { pos: 0.85, h: 0.30 },
      { pos: 1, h: 0.30 },
    ],
  },
};

// --- NAR汎用スタジアム型 ---
function makeNarTrack(direction: "右" | "左"): VenueTrack {
  return {
    direction,
    perimeter: 1400,
    straight: 250,
    path: "M 65,55 L 35,55 C 15,55 5,46 5,35 C 5,24 15,15 35,15 L 65,15 C 85,15 95,24 95,35 C 95,46 85,55 65,55 Z",
    goalPos: [50, 55],
    corners: [0.16, 0.36, 0.64, 0.84],
    elevation: [],
  };
}

// --- Phase 2-B: NAR 5場個別トラックテーブル ---
// 帯広（ばんえい・直走路200m）/ 大井（外/内/左）/ 門別（外/内）/ 盛岡（ダ+芝）/ 船橋（スパイラル外/内）
const NAR_TRACKS: Record<string, VenueTrack> = {
  // === 帯広 === ばんえい競馬・完全直走路 200m ===
  // 左端 = S、右端 = G。コーナーなし。isStraight フラグで千直と同じ直線レンダリングを再利用。
  "帯広": {
    direction: "右",
    perimeter: 200,
    straight: 200,
    // 直走路: path はダミー楕円（variant["直"] の isStraight フラグで上書きされる）
    path: "M 5,35 L 95,35",
    goalPos: [95, 35],
    corners: [0.25, 0.5, 0.5, 0.75],
    elevation: [],
    variants: {
      // 直線コースとして isStraight フラグを持つ CoursePath
      "直": {
        perimeter: 200,
        straight: 200,
        path: "M 5,35 L 95,35",
        goalPos: [95, 35],
        corners: [],
        elevation: [],
        isStraight: true,
      },
    },
  },

  // === 大井 === 右回り。外回り1600m / 内回り1400m / 左回り1650m の3コース ===
  // 距離別 variant 選択ロジックは CourseMapSVG 内の variantKey 計算で実施
  "大井": {
    direction: "右",
    perimeter: 1600,
    straight: 386,
    // 外回り: やや横長の大型楕円
    path: "M 74,57 L 26,57 C 8,57 2,46 2,33 C 2,20 8,9 26,9 L 74,9 C 92,9 98,20 98,33 C 98,46 92,57 74,57 Z",
    goalPos: [50, 57],
    corners: [0.18, 0.35, 0.65, 0.82],
    elevation: [],
    variants: {
      // 内回り 1400m: コンパクトな楕円
      "ダ内": {
        perimeter: 1400,
        straight: 286,
        path: "M 68,54 L 32,54 C 14,54 7,45 7,35 C 7,25 14,15 32,15 L 68,15 C 86,15 93,25 93,35 C 93,45 86,54 68,54 Z",
        goalPos: [50, 54],
        corners: [0.18, 0.35, 0.65, 0.82],
        elevation: [],
      },
      // 左回り 1650m: 外回り楕円と同サイズだが方向が左回り
      "ダ左": {
        perimeter: 1650,
        straight: 386,
        direction: "左",
        path: "M 74,57 L 26,57 C 8,57 2,46 2,33 C 2,20 8,9 26,9 L 74,9 C 92,9 98,20 98,33 C 98,46 92,57 74,57 Z",
        goalPos: [50, 57],
        corners: [0.18, 0.35, 0.65, 0.82],
        elevation: [],
      },
    },
  },

  // === 門別 === 右回り。外1600m / 内1376m ===
  "門別": {
    direction: "右",
    perimeter: 1600,
    straight: 330,
    // 外回り: やや横長楕円
    path: "M 72,57 L 28,57 C 10,57 3,47 3,33 C 3,19 10,9 28,9 L 72,9 C 90,9 97,19 97,33 C 97,47 90,57 72,57 Z",
    goalPos: [50, 57],
    corners: [0.17, 0.35, 0.65, 0.83],
    elevation: [],
    variants: {
      // 内回り 1376m: 縦長気味のコンパクト楕円
      "ダ内": {
        perimeter: 1376,
        straight: 218,
        path: "M 64,55 L 36,55 C 18,55 8,47 8,35 C 8,23 18,14 36,14 L 64,14 C 82,14 92,23 92,35 C 92,47 82,55 64,55 Z",
        goalPos: [50, 55],
        corners: [0.16, 0.36, 0.64, 0.84],
        elevation: [],
      },
    },
  },

  // === 盛岡 === 左回り。ダート1600m + 芝1400m の異なるトラックが共存 ===
  "盛岡": {
    direction: "左",
    perimeter: 1600,
    straight: 400,
    // ダート: 標準楕円
    path: "M 72,57 L 28,57 C 10,57 3,47 3,33 C 3,19 10,9 28,9 L 72,9 C 90,9 97,19 97,33 C 97,47 90,57 72,57 Z",
    goalPos: [50, 57],
    corners: [0.19, 0.35, 0.65, 0.81],
    elevation: [],
    variants: {
      // 芝 1400m: ダートより小さい内側の楕円
      "芝": {
        perimeter: 1400,
        straight: 400,
        path: "M 66,54 L 34,54 C 16,54 8,46 8,35 C 8,24 16,15 34,15 L 66,15 C 84,15 92,24 92,35 C 92,46 84,54 66,54 Z",
        goalPos: [50, 54],
        corners: [0.19, 0.35, 0.65, 0.81],
        elevation: [],
      },
    },
  },

  // === 船橋 === 左回り。スパイラルカーブ（緩やかなコーナー）外1400m / 内1250m ===
  // スパイラルカーブ: ベジエ制御点で楕円より角を丸めた（コーナー曲率を緩めに）
  "船橋": {
    direction: "左",
    perimeter: 1400,
    straight: 308,
    // 外回り: コーナー部分をスパイラル風に制御点をやや外側に置いた path
    path: [
      "M 70,57 L 30,57",
      "C 12,57 3,50 3,40",
      "C 3,26 12,9 30,9",
      "L 70,9",
      "C 88,9 97,26 97,40",
      "C 97,50 88,57 70,57 Z",
    ].join(" "),
    goalPos: [50, 57],
    corners: [0.16, 0.37, 0.63, 0.84],
    elevation: [],
    variants: {
      // 内回り 1250m: さらに制御点を緩やかにしたコンパクト版
      "ダ内": {
        perimeter: 1250,
        straight: 308,
        path: [
          "M 65,54 L 35,54",
          "C 18,54 9,47 9,38",
          "C 9,25 18,13 35,13",
          "L 65,13",
          "C 82,13 91,25 91,38",
          "C 91,47 82,54 65,54 Z",
        ].join(" "),
        goalPos: [50, 54],
        corners: [0.16, 0.37, 0.63, 0.84],
        elevation: [],
      },
    },
  },
};

// NAR会場のデフォルト方向
const NAR_DIRECTION: Record<string, "右" | "左"> = {
  "帯広": "右", "門別": "右", "盛岡": "左", "水沢": "右",
  "浦和": "左", "船橋": "左", "大井": "右", "川崎": "左",
  "金沢": "右", "笠松": "左", "名古屋": "左", "園田": "右",
  "姫路": "右", "高知": "右", "佐賀": "右",
};

// --- SVGパス上のサンプリング ---
function samplePathPoints(pathD: string, steps: number = 360): [number, number][] {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", pathD);
  svg.appendChild(path);
  document.body.appendChild(svg);

  const totalLen = path.getTotalLength();
  const points: [number, number][] = [];
  for (let i = 0; i <= steps; i++) {
    const pt = path.getPointAtLength((i / steps) * totalLen);
    points.push([pt.x, pt.y]);
  }
  document.body.removeChild(svg);
  return points;
}

// フォールバック用楕円サンプリング
function fallbackEllipse(): [number, number][] {
  const pts: [number, number][] = [];
  for (let i = 0; i <= 240; i++) {
    const a = (i / 240) * Math.PI * 2;
    pts.push([50 + 42 * Math.cos(a), 33 + 22 * Math.sin(a)]);
  }
  return pts;
}

// --- ポリライン文字列化 ---
function toPolyline(pts: [number, number][]): string {
  return pts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
}

// --- メインコンポーネント ---
interface CourseMapSVGProps {
  race: {
    surface?: string;
    distance?: number;
    direction?: string;
    inside_outside?: string;   // Phase 1B/C: 内外回り判定 "外" | "内" | ""
    straight_m?: number;
    first_corner_m?: number;
    l3f_corners?: number;
    l3f_elevation?: number;
    l3f_straight_pct?: number;
    l3f_hill_start?: number;
  };
  venue: string;
}

export function CourseMapSVG({ race, venue }: CourseMapSVGProps) {
  const dist = race.distance || 1600;
  const direction = race.direction === "左" ? "左" : race.direction === "右" ? "右" : undefined;

  // トラック取得（JRA → NAR個別 → NAR汎用）— Phase 2-B: NAR_TRACKS 追加
  const track: VenueTrack = TRACKS[venue]
    ?? NAR_TRACKS[venue]
    ?? makeNarTrack(direction || NAR_DIRECTION[venue] || "右");
  // dir: race.direction > variant.direction（大井左回り等）> track.direction の優先順で決定
  // ※ variant は variantKey 計算後に確定するため、ここでは仮決定し後で上書き可

  // --- Phase 1B: ダート専用周長を使用 ---
  const isDirt = race.surface === "ダート" || race.surface === "ダ";

  // --- Phase 2-A: バリアント（内外回り別 path）選択 ---
  // race.inside_outside と surface から使用する CoursePath を決定する
  // 1. 新潟千直（芝1000m直線）を最優先で判定
  // 2. 芝内回り / 芝外回り / ダート の順で variant を探す
  // 3. なければトップレベル（Phase 1 互換）にフォールバック
  const variantKey = (() => {
    // --- Phase 2-B: NAR 5場の特殊 variant 選択 ---

    // 帯広（ばんえい）: 常に直線コース
    if (venue === "帯広") return "直";

    // 大井: 距離によって外/内/左を切替
    if (venue === "大井") {
      const oiLeftDistances = [1650];
      const oiInnerDistances = [1000, 1400, 1500, 1600, 1700];
      if (oiLeftDistances.includes(dist)) return "ダ左";
      if (oiInnerDistances.includes(dist)) return "ダ内";
      return null; // 外回り（デフォルト）
    }

    // 門別: 距離によって外/内を切替
    if (venue === "門別") {
      const monbetsuInnerDistances = [1500, 1600];
      if (monbetsuInnerDistances.includes(dist)) return "ダ内";
      return null; // 外回り（デフォルト）
    }

    // 船橋: 距離によって外/内を切替
    if (venue === "船橋") {
      const funabashiInnerDistances = [1000, 1200, 1500, 1700, 2000];
      if (funabashiInnerDistances.includes(dist)) return "ダ内";
      return null; // 外回り（デフォルト）
    }

    // 盛岡: surface（芝/ダ）で切替
    if (venue === "盛岡") {
      if (!isDirt) return "芝";
      return null; // ダート（デフォルト）
    }

    // --- 既存: JRA コース ---
    if (!isDirt) {
      // 新潟千直: 芝 1000m かつ新潟の場合
      if (venue === "新潟" && dist === 1000) return "芝1000直";
      if (race.inside_outside === "内") return "芝内";
      if (race.inside_outside === "外") return "芝外";
    }
    return null;
  })();

  // バリアントが存在すれば CoursePath を使う
  const variant: CoursePath | null = variantKey
    ? (track.variants?.[variantKey] ?? null)
    : null;

  // dir: race.direction > variant.direction（大井左回り等）> track.direction の優先順
  const dir = direction ?? variant?.direction ?? track.direction;

  // 千直フラグ
  const isStraightCourse = variant?.isStraight === true;

  // 有効パス・周長・goalPos を決定（バリアント優先）
  const effectivePath = variant ? variant.path : track.path;
  const effectiveGoalPos = variant ? variant.goalPos : track.goalPos;
  const effectiveCorners = variant ? variant.corners : track.corners;
  const effectiveElevation = variant?.elevation ?? track.elevation;
  const effectivePockets = variant?.pockets ?? track.pockets;

  const effectivePerimeter = (() => {
    if (variant) return variant.perimeter;
    if (isDirt && track.dirtPerimeter) return track.dirtPerimeter;
    return track.perimeter;
  })();

  // パス上のサンプル点を取得（バリアントがあればそちらのパスを使用）
  let pathPoints: [number, number][];
  if (isStraightCourse) {
    // 千直: 直線をサンプリング（左→右 の水平線）
    pathPoints = [];
    for (let i = 0; i <= 360; i++) {
      const x = 5 + (90 * i) / 360;
      pathPoints.push([x, 35]);
    }
  } else {
    try {
      pathPoints = samplePathPoints(effectivePath);
    } catch {
      pathPoints = fallbackEllipse();
    }
  }

  // ゴール最近接点
  let goalIdx = 0;
  let minD = Infinity;
  pathPoints.forEach(([px, py], i) => {
    const d = Math.hypot(px - effectiveGoalPos[0], py - effectiveGoalPos[1]);
    if (d < minD) { minD = d; goalIdx = i; }
  });

  // ゴールから逆走方向に並べ替え
  // パスはCW（screen座標）で描画。
  // 左回り: CW forward = 逆走方向（ゴールからの逆走=レース逆方向）
  // 右回り: CW backward = 逆走方向
  const isLeft = dir === "左";
  const orderedPoints: [number, number][] = [];
  const N = pathPoints.length - 1;
  for (let i = 0; i < N; i++) {
    const idx = isLeft
      ? (goalIdx + i) % N
      : (goalIdx - i + N) % N;
    orderedPoints.push(pathPoints[idx]);
  }

  // ラスト3F(600m) — トラック上の物理距離で計算（芝外回り基準周長を使用）
  const l3fTrackFrac = Math.min(1, 600 / track.perimeter);
  const l3fEndIdx = Math.round(l3fTrackFrac * (orderedPoints.length - 1));
  const l3fPoints = orderedPoints.slice(0, l3fEndIdx + 1);

  // --- Phase 1B/D: スタート位置計算（ダートは dirtPerimeter を使用）---
  const laps = dist / effectivePerimeter;
  const startRatio = laps >= 1
    ? ((dist % effectivePerimeter) / effectivePerimeter) || 0.95  // 整数周の場合はゴール手前
    : Math.min(0.98, laps);

  // --- Phase 1C/D: surfaceCode キーを生成してポケット定義を検索 ---
  const surfaceCode = (() => {
    if (isDirt) return "ダ";
    if (race.surface === "芝" || race.surface === "Turf") {
      if (race.inside_outside === "外") return "芝外";
      if (race.inside_outside === "内") return "芝内";
      return "芝";
    }
    // surface が未設定の場合は芝として扱う
    return race.surface || "芝";
  })();
  const pocketKey = `${surfaceCode}${dist}`;
  // Phase 2-A: バリアント内の pockets を優先して検索（芝内回りポケット等）
  const pocket = effectivePockets?.[pocketKey] ?? null;

  // スタート位置確定
  let sx: number, sy: number;
  if (pocket) {
    // ポケット発走: 手動スタート座標を使用
    [sx, sy] = pocket.startPos;
  } else {
    // 通常発走: 周長逆算 fallback
    const startIdx = Math.round(startRatio * (orderedPoints.length - 1));
    [sx, sy] = orderedPoints[startIdx] || track.goalPos;
  }

  // ゴール
  const [gx, gy] = orderedPoints[0] || track.goalPos;

  // ラスト3F開始地点
  const [l3sx, l3sy] = orderedPoints[l3fEndIdx] || [50, 33];

  // 前半3F（スタートから600m）— トラック上の区間をハイライト
  const f3fTrackFrac = Math.min(0.95, 600 / effectivePerimeter);
  const f3fSteps = Math.round(f3fTrackFrac * (orderedPoints.length - 1));
  const f3fPoints: [number, number][] = [];
  const NN = orderedPoints.length - 1;

  // ポケット発走の場合: cornerLabel から楕円本体の合流点インデックスを求める
  // cornerLabels と track.corners は [4C, 3C, 2C, 1C] の順
  const pocketCornerLabelOrder = ["4C", "3C", "2C", "1C"];
  const pocketJoinIdx = (() => {
    if (!pocket) return null;
    // cornerLabel が "Xc外側" "Xc間" 等を含む場合は最も近いコーナーを推定
    const label = pocket.cornerLabel;
    // まず完全一致で探す
    let ci = pocketCornerLabelOrder.indexOf(label);
    if (ci < 0) {
      // "2C外側" "1-2C間" などから数字を抽出して最近傍コーナーを選ぶ
      const m = label.match(/(\d)C/);
      if (m) {
        const cNum = parseInt(m[1], 10); // 1〜4
        // cornerLabels は ["4C","3C","2C","1C"] → index は 4-cNum
        ci = 4 - cNum;
      }
    }
    if (ci < 0 || ci >= effectiveCorners.length) return null;
    return Math.round(effectiveCorners[ci] * (orderedPoints.length - 1));
  })();

  {
    // 前半3F の起点: ポケットなら合流点、通常発走ならスタート点
    const startIdx = Math.round(startRatio * (orderedPoints.length - 1));
    const f3fOriginIdx = (pocket && pocketJoinIdx != null) ? pocketJoinIdx : startIdx;
    for (let i = 0; i <= f3fSteps; i++) {
      let idx = f3fOriginIdx - i;
      if (idx < 0) idx += NN;
      f3fPoints.push(orderedPoints[idx]);
    }
  }

  const [f3ex, f3ey] = f3fPoints.length > 0
    ? f3fPoints[f3fPoints.length - 1]
    : [50, 33];

  // コーナー位置の座標を計算（バリアントのコーナー定義を優先）
  const cornerLabels = ["4C", "3C", "2C", "1C"];
  const cornerPoints = effectiveCorners.map((frac, i) => {
    const idx = Math.round(frac * (orderedPoints.length - 1));
    const [cx, cy] = orderedPoints[idx] || [50, 33];
    // ラベルを内馬場側（中心方向）にオフセット
    const centerX = 50, centerY = 33;
    const dx = centerX - cx, dy = centerY - cy;
    const len = Math.hypot(dx, dy) || 1;
    const offsetDist = 6;
    return {
      label: cornerLabels[i],
      x: cx + (dx / len) * offsetDist,
      y: cy + (dy / len) * offsetDist,
      trackX: cx,
      trackY: cy,
    };
  });

  // 走行方向矢印（トラック約40%地点、レース進行方向=orderedPointsの逆方向）
  const arrowIdx = Math.round(orderedPoints.length * 0.42);
  const [ax, ay] = orderedPoints[arrowIdx] || [50, 33];
  const prevIdx = Math.max(arrowIdx - 4, 0);
  const [ax2, ay2] = orderedPoints[prevIdx] || [49, 33];
  const arrowAngle = Math.atan2(ay2 - ay, ax2 - ax) * (180 / Math.PI);

  // --- Phase 1A: 坂計算バグ修正 ---
  // 修正前: hillStart / dist（距離依存で坂位置が変わる設計バグ）
  // 修正後: hillStart / effectivePerimeter（バリアント対応版）
  const shape = TRACKS[venue];
  // バリアントに坂情報があればそちらを優先
  const hillHeight = variant?.hillHeight ?? shape?.hillHeight;
  const hillStartM = variant?.hillStart ?? shape?.hillStart;
  const hillEndM = variant?.hillEnd ?? shape?.hillEnd;
  const hasHill = hillHeight != null && hillHeight > 0;
  const hillStartRatio = hillStartM ? hillStartM / effectivePerimeter : 0;
  const hillEndRatio = hillEndM ? hillEndM / effectivePerimeter : 0;

  // 起伏プロファイル（バリアントの elevation を優先）
  const elevData = effectiveElevation;
  const elevH = 16;
  const elevY0 = 72;

  // コース名（帯広・千直・通常コースで専用ラベル）
  const surfLabel = (() => {
    if (venue === "帯広") return "ばんえい";
    if (isStraightCourse) return "芝(千直)";
    if (isDirt) return "ダ";
    return "芝";
  })();

  // コース概要文を自動生成
  const courseDesc = (() => {
    const straightM = race.straight_m || 0;
    const straightM_ = race.straight_m || 0;
    const firstCornerM = race.first_corner_m || (straightM_ > 0 ? Math.round(straightM_ * 0.8) : 0);
    const l3fCorners = race.l3f_corners || 0;
    const elevation = race.l3f_elevation || 0;
    const parts: string[] = [];

    // 基本情報
    parts.push(`${dir === "右" ? "右" : "左"}回り${dist}m。`);

    // 直線特性
    if (straightM > 0) {
      if (straightM >= 500) parts.push(`直線${straightM}mと長く、末脚勝負になりやすい。`);
      else if (straightM >= 350) parts.push(`直線${straightM}mは標準的。`);
      else parts.push(`直線${straightM}mと短く、先行力が問われる。`);
    }

    // 1角まで
    if (firstCornerM > 0) {
      if (firstCornerM < 250) parts.push(`1角まで${firstCornerM}mと短いため序盤から先行争いが激しくなる。`);
      else if (firstCornerM >= 400) parts.push(`1角まで${firstCornerM}mあり隊列が落ち着きやすい。`);
    }

    // ラスト3F区間
    if (l3fCorners >= 2) parts.push("ラスト3Fはコーナー途中から始まり、器用さと立ち回りが重要。");
    else if (l3fCorners === 1) parts.push("ラスト3Fはコーナー1つを含み、直線で末脚を発揮できる。");
    else parts.push("ラスト3Fはほぼ直線で、純粋なスピード勝負。");

    // 坂
    if (elevation >= 2.0) parts.push(`ゴール前に高低差${elevation.toFixed(1)}mの急坂がありスタミナと底力が必要。`);
    else if (elevation >= 0.5) parts.push(`軽い坂（${elevation.toFixed(1)}m）がありパワーも求められる。`);
    else parts.push("平坦コースで純粋なスピードが活きる。");

    return parts.join("");
  })();

  return (
    <div className="w-full">
    <svg viewBox="0 0 100 92" className="w-full" style={{ maxHeight: "320px" }}>
      {/* 背景 */}
      <rect x="0" y="0" width="100" height="92" rx="4" fill="currentColor" className="text-muted/5" />

      {/* === トラック描画 === */}

      {/* ① 内馬場フィル（グリーン）— 千直は非表示 */}
      {!isStraightCourse && (
        <path d={effectivePath} fill="#16a34a" opacity="0.07" stroke="none" />
      )}

      {/* ② トラック路面（太ストローク） */}
      <path
        d={effectivePath}
        fill="none"
        stroke="currentColor"
        strokeWidth="3.5"
        strokeLinejoin="round"
        className="text-muted-foreground/20"
      />

      {/* ③ 坂区間（オレンジ斜線）*/}
      {hasHill && (() => {
        const hsi = Math.round(hillStartRatio * (orderedPoints.length - 1));
        const hei = Math.round(hillEndRatio * (orderedPoints.length - 1));
        const hillPts = orderedPoints.slice(Math.min(hei, hsi), Math.max(hei, hsi) + 1);
        return hillPts.length > 1 ? (
          <polyline
            points={toPolyline(hillPts)}
            fill="none"
            stroke="#f59e0b"
            strokeWidth="5"
            strokeLinejoin="round"
            opacity="0.35"
            strokeDasharray="2,2"
          />
        ) : null;
      })()}

      {/* ③b 前半3F区間（青）— 通常発走: スタート点から600m / ポケット: 楕円合流点から600m */}
      {f3fPoints.length > 1 && (
        <polyline
          points={toPolyline(f3fPoints)}
          fill="none"
          stroke="#3b82f6"
          strokeWidth="4"
          strokeLinejoin="round"
          opacity="0.6"
        />
      )}

      {/* ④ ラスト3F区間（赤） */}
      <polyline
        points={toPolyline(l3fPoints)}
        fill="none"
        stroke="#ef4444"
        strokeWidth="4"
        strokeLinejoin="round"
        opacity="0.7"
      />

      {/* ⑤ トラック中心線（細）— 千直は省略 */}
      {!isStraightCourse && (
        <path
          d={effectivePath}
          fill="none"
          stroke="currentColor"
          strokeWidth="0.4"
          className="text-muted-foreground/30"
        />
      )}

      {/* ⑥ フィニッシュライン（白い横線） */}
      <line
        x1={gx} y1={gy - 3.5}
        x2={gx} y2={gy + 3.5}
        stroke="currentColor"
        strokeWidth="0.8"
        className="text-foreground/60"
      />

      {/* ⑦ コーナー番号ラベル（内馬場側） */}
      {cornerPoints.map((c) => (
        <g key={c.label}>
          <circle cx={c.x} cy={c.y} r="3" fill="currentColor" className="text-background" opacity="0.7" />
          <circle cx={c.x} cy={c.y} r="3" fill="none" stroke="currentColor" strokeWidth="0.4" className="text-muted-foreground/50" />
          <text
            x={c.x}
            y={c.y + 1.2}
            textAnchor="middle"
            fontSize="3.2"
            fill="currentColor"
            className="text-muted-foreground"
            fontWeight="600"
          >
            {c.label.replace("C", "")}
          </text>
        </g>
      ))}

      {/* ⑧ 走行方向矢印（大きめ三角＋ラベル） */}
      <g transform={`translate(${ax},${ay}) rotate(${arrowAngle})`}>
        <polygon points="-2,-3 5,0 -2,3" fill="currentColor" className="text-muted-foreground/50" />
      </g>

      {/* ⑨a ラスト3F開始マーク */}
      <circle cx={l3sx} cy={l3sy} r="1.5" fill="#ef4444" opacity="0.8" />
      <text
        x={l3sx}
        y={l3sy - 3.5}
        textAnchor="middle"
        fontSize="3"
        fill="#ef4444"
        fontWeight="bold"
      >
        3F
      </text>

      {/* ⑨b 前半3F終了マーク — 通常発走コースのみ */}
      {f3fPoints.length > 1 && (
        <>
          <circle cx={f3ex} cy={f3ey} r="1.5" fill="#3b82f6" opacity="0.8" />
          <text
            x={f3ex}
            y={f3ey - 3.5}
            textAnchor="middle"
            fontSize="3"
            fill="#3b82f6"
            fontWeight="bold"
          >
            3F
          </text>
        </>
      )}

      {/* ⑩a ポケット枝SVG（ポケット発走コースのみ） — Phase 1C
           前半3Fハイライトの一部として青で描画（ポケット枝も馬が走る600mの一部） */}
      {pocket && (
        <path
          d={pocket.branchPath}
          fill="none"
          stroke="#3b82f6"
          strokeWidth="4"
          strokeLinecap="round"
          opacity="0.6"
        />
      )}

      {/* ⑩ スタート(S) */}
      <circle cx={sx} cy={sy} r="3.2" fill="#0891b2" />
      <text
        x={sx} y={sy + 1.2}
        textAnchor="middle"
        fontSize="3.2"
        fill="white"
        fontWeight="bold"
      >
        S
      </text>
      {/* 周回レースの場合、周回数を表示 */}
      {laps >= 1.5 && (
        <text
          x={sx} y={sy - 4.5}
          textAnchor="middle"
          fontSize="2.6"
          fill="#0891b2"
          fontWeight="bold"
        >
          {Math.round(laps)}周
        </text>
      )}
      {/* ポケット発走の場合コーナーラベルを表示 */}
      {pocket && (
        <text
          x={sx} y={sy - 4.5}
          textAnchor="middle"
          fontSize="2.4"
          fill="#0891b2"
          fontWeight="bold"
        >
          {pocket.cornerLabel}
        </text>
      )}

      {/* ⑪ ゴール(G) */}
      <circle cx={gx} cy={gy} r="3.2" fill="#e11d48" />
      <text
        x={gx} y={gy + 1.2}
        textAnchor="middle"
        fontSize="3.2"
        fill="white"
        fontWeight="bold"
      >
        G
      </text>

      {/* ⑫ コース名ラベル */}
      <text
        x="50" y="5"
        textAnchor="middle"
        fontSize="4"
        fill="currentColor"
        className="text-foreground"
        fontWeight="bold"
      >
        {venue} {surfLabel}{dist}m ({dir})
      </text>

      {/* === 起伏プロファイル（下部） === */}
      {elevData.length > 0 && (
        <g>
          {/* ベースライン */}
          <line
            x1="8" y1={elevY0 + elevH}
            x2="92" y2={elevY0 + elevH}
            stroke="currentColor" strokeWidth="0.3"
            className="text-muted-foreground/30"
          />
          {/* 起伏エリア塗りつぶし */}
          <polygon
            points={[
              `8,${elevY0 + elevH}`,
              ...elevData.map(e =>
                `${(8 + e.pos * 84).toFixed(1)},${(elevY0 + elevH - e.h * elevH).toFixed(1)}`
              ),
              `92,${elevY0 + elevH}`,
            ].join(" ")}
            fill="currentColor"
            className="text-muted-foreground/8"
          />
          {/* 起伏ライン */}
          <polyline
            points={elevData.map(e => {
              const x = 8 + e.pos * 84;
              const y = elevY0 + elevH - e.h * elevH;
              return `${x.toFixed(1)},${y.toFixed(1)}`;
            }).join(" ")}
            fill="none"
            stroke="currentColor"
            strokeWidth="0.8"
            className="text-foreground/60"
          />
          {/* ラスト3Fエリアハイライト */}
          {(() => {
            const l3fElevPts = elevData.filter(e => e.pos <= l3fTrackFrac);
            if (l3fElevPts.length < 2) return null;
            return (
              <polygon
                points={[
                  `8,${elevY0 + elevH}`,
                  ...l3fElevPts.map(e =>
                    `${(8 + e.pos * 84).toFixed(1)},${(elevY0 + elevH - e.h * elevH).toFixed(1)}`
                  ),
                  `${(8 + l3fTrackFrac * 84).toFixed(1)},${elevY0 + elevH}`,
                ].join(" ")}
                fill="#ef4444"
                opacity="0.12"
              />
            );
          })()}
          {/* 坂マーク */}
          {hasHill && hillHeight && (
            <text
              x={8 + ((hillStartRatio + hillEndRatio) / 2) * 84}
              y={elevY0 + 3}
              textAnchor="middle"
              fontSize="2.6"
              fill="#f59e0b"
              fontWeight="bold"
            >
              坂{hillHeight.toFixed(1)}m
            </text>
          )}
          {/* ラベル */}
          <text x="8" y={elevY0 + elevH + 4} fontSize="2.3" fill="currentColor" className="text-muted-foreground">G</text>
          <text x="92" y={elevY0 + elevH + 4} fontSize="2.3" fill="currentColor" className="text-muted-foreground" textAnchor="end">S</text>
        </g>
      )}

      {/* === 凡例 === */}
      <g transform={`translate(3, ${elevData.length > 0 ? 68 : 65})`}>
        <line x1="0" y1="0" x2="4" y2="0" stroke="#3b82f6" strokeWidth="2" opacity="0.6" />
        <text x="6" y="1" fontSize="2.2" fill="#3b82f6">前半3F</text>
        <line x1="22" y1="0" x2="26" y2="0" stroke="currentColor" strokeWidth="2" className="text-muted-foreground/20" />
        <text x="28" y="1" fontSize="2.2" fill="currentColor" className="text-muted-foreground">コース</text>
        <line x1="43" y1="0" x2="47" y2="0" stroke="#ef4444" strokeWidth="2" opacity="0.7" />
        <text x="49" y="1" fontSize="2.2" fill="#ef4444">ラスト3F</text>
        {hasHill && (
          <>
            <line x1="67" y1="0" x2="71" y2="0" stroke="#f59e0b" strokeWidth="2" strokeDasharray="1,1" opacity="0.5" />
            <text x="73" y="1" fontSize="2.2" fill="#f59e0b">坂</text>
          </>
        )}
      </g>
    </svg>
    <p className="text-xs text-muted-foreground leading-relaxed mt-1 px-1">
      {courseDesc}
    </p>
    </div>
  );
}
