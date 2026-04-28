/**
 * 厩舎コメント（競馬ブック原文）を D-AI 独自に解析して箇条書きに分解する。
 * バックエンドで stable_comment_bullets が生成されていない場合の fallback。
 * 著作権リスク低減のため:
 *   - 冒頭の「○馬名（短評）助手 — 」フォーマットを除去
 *   - 句点で分割し、各文を独立した bullet に
 *   - キーワード辞書で簡易カテゴリタグを付与
 */

export interface StableBullet {
  text: string;
  category?: "馬体" | "調教" | "展開" | "メンタル" | "ローテ" | "適性" | "課題" | "期待";
}

/** カテゴリ判定キーワード辞書（先に合致したものを採用） */
const CATEGORY_KEYWORDS: Array<{ pattern: RegExp; category: StableBullet["category"] }> = [
  { pattern: /馬体|体重|デキ|気配|張り|肌|フックラ|腹周り|モリモリ|プラス|マイナス|仕上|体型/, category: "馬体" },
  { pattern: /坂路|追切|追い切り|時計|併せ|栗東|美浦|攻め|稽古|調教|乗込|乗り込|一杯/, category: "調教" },
  { pattern: /先行|逃げ|差し|追込|脚質|ペース|位置取り|スタート|ダッシュ|前残り|番手/, category: "展開" },
  { pattern: /気合|気持ち|集中|落ち着|テン|入れ込|気性|メンタル|煩い|うるさ|おとなし/, category: "メンタル" },
  { pattern: /休み明け|前走|間隔|連闘|放牧|ぶっつけ|久々|連戦|叩き|一戦|帰厩|初戦/, category: "ローテ" },
  { pattern: /コース|距離|芝|ダート|右回り|左回り|得意|苦手|相性|向く|小回り|大箱|重馬場/, category: "適性" },
  { pattern: /課題|不安|疑問|微妙|心配|心許な|今ひとつ|物足り|足りない|怪我|外傷/, category: "課題" },
  { pattern: /楽しみ|期待|チャンス|有力|勝負|狙え|勝てる|頭まで|連|複|好走|自信/, category: "期待" },
];

/**
 * 厩舎コメント原文を解析して箇条書き配列に変換する。
 * stable_comment_bullets がない場合の fallback として使用。
 */
export function parseStableComment(raw: string): StableBullet[] {
  if (!raw || raw.trim().length === 0) return [];

  // 1. 冒頭フォーマット除去:
  //    「○ギマール（ロスなく）大林助手 — 」「○馬名（XXX）YYY師 — 」等を削除
  //    T-022 (2026-04-28): 半角括弧 `(XXX)` 対応 + 「人名師——」単体 prefix + 「○馬名(短評)」のみの行除去
  //    T-022 リトライ (2026-04-28): HORIZONTAL BAR (U+2015) 「―」追加 + 全角スペース U+3000 許容
  //
  //    実データのダッシュバリエーション:
  //      U+2014 — (EM DASH)
  //      U+2015 ― (HORIZONTAL BAR) ← マスター環境でよく使われる
  //      U+FF0D － (FULLWIDTH HYPHEN-MINUS)
  //      U+002D - (ASCII)
  //    全部対応するため `[—\-－―]` (4 文字) を使用
  let cleaned = raw.trim();
  // パターン1: 「○[馬名]（...）[人名] — 」形式 (全角・半角括弧両対応 + 4 種ダッシュ対応)
  cleaned = cleaned.replace(/^[○●◯◎▲△★☆×]\S+[（(][^）)]*[）)][^\s—\-－―]*\s*[—\-－―]+\s*/, "");
  // パターン3: 「○[馬名]【[人名]師】」形式 (新パターン・2026-04-27)
  cleaned = cleaned.replace(/^[○●◯◎▲△★☆×]?[぀-ゟ゠-ヿ一-鿿A-Za-z]+【[^】]+】/, "");
  // パターン4 (T-022 改修 / T-025 拡張): 「[人名]師——」「担当厩務員——」「○○助手——」プレフィックス削除
  // 例: 「真島師――」「平松師――」「担当厩務員――」「大林助手――」
  // T-025: 「師」だけでなく「厩務員」「助手」「マネジャー」も対応
  cleaned = cleaned.replace(/(^|\n)[\s　]*[^\s　\n。．]*?(師|厩務員|助手|マネジャー)[—\-－―]+\s*/g, "$1");
  // パターン2: フォールバック — ダッシュ記号より前を全部削る（先頭のみ）
  if (cleaned === raw.trim()) {
    cleaned = cleaned.replace(/^[^。．\n]*[—\-－―]+\s*/, "");
  }

  // 2. 句点・改行で分割
  let sentences = cleaned
    .split(/[。．\n]/)
    .map((s) => s.trim())
    // 半角・全角スペース、冒頭の「。」「．」「　」を除去
    .map((s) => s.replace(/^[。．\s　]+/, "").trim())
    .filter((s) => s.length >= 5);

  // パターン5 (T-022 改修): 「○馬名(短評)」のみの文を箇条書き対象から除外
  // 例: 「○ソルベット(流れひとつ)」「○エースアビリティ(展開次第)」「○チャンチャン（慣れが必要）」
  // 全角括弧・半角括弧両対応
  sentences = sentences.filter(
    (s) => !/^[○●◯◎▲△★☆×]\S+[（(][^）)]+[）)]$/.test(s)
  );

  // パターン5b (T-028 / 2026-04-28): sentence 冒頭の口語接続詞「あとは、」「あと、」等を削除
  // 句点分割後の各 sentence 単位で適用
  sentences = sentences.map((s) =>
    s.replace(/^(あとは|あと|それと|で|また)[、,]\s*/, "").trim()
  ).filter((s) => s.length >= 5);

  // パターン6 (T-025 拡張): 「[人名]師——」「担当厩務員——」「○○助手——」最終削除
  sentences = sentences.map((s) =>
    s.replace(/^[\s　]*[^\s　\n。．]*?(師|厩務員|助手|マネジャー)[—\-－―]+\s*/, "").trim()
  ).filter((s) => s.length >= 5);

  // パターン7 (T-022 最終): 「○チャンチャン」のように prefix が散らばって残る馬名行を念入りに除外
  sentences = sentences.filter(
    (s) => !/^[○●◯◎▲△★☆×][^\s（(]+$/.test(s)
  );

  if (sentences.length === 0) return [];

  // T-025 (2026-04-28): 言い切り口調変換
  // マスター指示「「だよ」「と思う」じゃねーんだわ。言い切れ」
  // 文末の曖昧表現を断定形 or 削除に変換
  const ASSERTIVE_RULES: Array<{ from: RegExp; to: string }> = [
    // T-027 表層 (2026-04-28): 長母音付き感嘆表現も削除（「かな」より前に置いて確実にマッチ）
    { from: /(かなあ|かなー|だなあ|だなー|よなあ|だよなあ)$/, to: "" },
    // 削除系（文末で意味希薄）
    { from: /(と思う|と思います|と感じる|と感じます|気がする|気もする|印象だ|印象です|ように見える|ように映る|ようだ|みたいだ|だろうか|でしょうか)$/, to: "" },
    { from: /(かな|かも|かもしれない|かもしれません|かも知れない)$/, to: "" },
    // 断定化（口語→書き言葉）
    { from: /(だよ|だね|だな|だよね)$/, to: "だ" },
    { from: /(でしょう|だろう)$/, to: "" },
    { from: /(ですね|ですよ|だぜ|なのよ)$/, to: "" },
    { from: /(かもね|かもよ)$/, to: "" },
    // T-027 表層 (2026-04-28): 感嘆符末尾を除去
    { from: /[!！]+$/, to: "" },
    // T-028 (2026-04-28): 口語残存表現 4 種追加 (マスター指摘 1-9 番)
    // 口語疑問形「んじゃない」「じゃない」末尾削除
    { from: /(んじゃない(か|の)?|じゃない(か|の)?)$/, to: "" },
    // 口語接続「けど/けれど/けれども」末尾削除
    { from: /(けど|けれど|けれども)$/, to: "" },
    // 婉曲「したいところ/したいもの/したい感じ」末尾削除
    // 「期待したい」などの前向き表現は意図的に残す (安全側)
    { from: /(したいところ|したいもの|したい感じ)$/, to: "" },
  ];

  const toAssertive = (s: string): string => {
    let t = s.trim();
    // 末尾の句点・記号除去
    t = t.replace(/[。．、,]+$/, "").trim();
    // 複数回適用 (例: 「と思うかもしれない」→「と思う」→「」)
    for (let pass = 0; pass < 3; pass++) {
      const before = t;
      for (const { from, to } of ASSERTIVE_RULES) {
        t = t.replace(from, to).trim();
      }
      if (t === before) break;
    }
    return t;
  };

  sentences = sentences.map(toAssertive).filter((s) => s.length >= 5);

  // 3. 各文にカテゴリを付与
  const bullets: StableBullet[] = sentences.map((text) => {
    let category: StableBullet["category"] = undefined;
    for (const { pattern, category: cat } of CATEGORY_KEYWORDS) {
      if (pattern.test(text)) {
        category = cat;
        break;
      }
    }
    return { text, category };
  });

  // 4. 重複除去・短すぎる bullet を除外
  return bullets.filter((b, i, arr) => {
    if (b.text.length < 5) return false;
    if (arr.findIndex((x) => x.text === b.text) !== i) return false;
    return true;
  });
}
