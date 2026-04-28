import { useMemo } from "react";
import { VENUE_NAME_TO_CODE, WAKU_BG } from "@/lib/constants";
import { usePersonnelAggCourse, useCourseStats } from "@/api/hooks";
import { useViewMode } from "@/hooks/useViewMode";
import type { HorseData, RaceDetail } from "./RaceDetailView";

// ============================================================
// コースデータ + 出走馬の騎手/調教師/種牡馬/母父 DB資料表示
// ============================================================

interface Props {
  horses: HorseData[];
  race: RaceDetail;
}

// パーセント色
function pctCls(v: number, thresholds: [number, number] = [20, 35]): string {
  if (v >= thresholds[1]) return "text-emerald-600 font-bold";
  if (v >= thresholds[0]) return "text-blue-600";
  return "";
}

// 回収率色
function roiCls(v: number): string {
  if (v >= 100) return "text-red-600 font-bold";
  if (v >= 80) return "text-red-600";
  return "";
}

// 成績文字列
function recordStr(win: number, p2: number, p3: number, total: number): string {
  const out = total - p3;
  return `${win}-${p2 - win}-${p3 - p2}-${out}`;
}

export function DataAnalysisPanel({ horses, race }: Props) {
  // モバイルプレビュー検知（PC ブラウザの「モバイル」ビューモード時に sm: メディアクエリが効かない問題対応）
  const { isMobile } = useViewMode();
  const venueCode = VENUE_NAME_TO_CODE[race.venue || ""] || "";
  const surface = race.surface || "";
  const distance = race.distance || 0;
  const courseKey = venueCode && surface && distance ? `${venueCode}_${surface}_${distance}` : "";

  // コースデータ取得
  const { data: courseData, isLoading: courseLoading } = useCourseStats(courseKey);

  // 漢字異体字マップ（旧字体→新字体）
  const KANJI_VARIANTS: Record<string, string> = {
    "齊": "斉", "齋": "斉", "髙": "高", "﨑": "崎", "濵": "浜", "濱": "浜",
    "邊": "辺", "邉": "辺", "廣": "広", "眞": "真", "國": "国", "島": "島",
    "櫻": "桜", "澤": "沢", "關": "関", "鷗": "鴎", "戶": "戸", "條": "条",
    "藤": "藤", "瀨": "瀬", "龍": "竜", "萬": "万", "實": "実", "祐": "祐",
  };

  // 名前の正規化: 所属除去 + スペース除去 + 異体字統一
  const normName = (name: string): string => {
    let s = name.replace(/（.*）$/, "").replace(/\(.*\)$/, "").replace(/\s/g, "").trim();
    for (const [old, nw] of Object.entries(KANJI_VARIANTS)) {
      s = s.replaceAll(old, nw);
    }
    return s;
  };

  // 省略名マッチング（安全網）:
  // race_log は personnel_db フルネームに正規化済みだが、念のため:
  //  (1) 前方一致  (2) 末尾一致（DB 側「栗東野中」等の地域接頭辞対応）
  //  (3) 先頭2文字一致＋省略名ルール
  const nameMatches = (predName: string, dbName: string): boolean => {
    if (predName.length < 2 || dbName.length < 2) return false;
    // (1) 完全前方一致
    if (dbName.startsWith(predName) || predName.startsWith(dbName)) return true;
    // (2) 末尾一致（地域接頭辞ケース: DB「栗東野中」 vs pred「野中賢二」→ dbName が predName[0:2]「野中」で終わる）
    if (dbName.endsWith(predName) || predName.endsWith(dbName)) return true;
    if (predName.length >= 2 && dbName.endsWith(predName.slice(0, 2))) return true;
    // (3) 先頭2文字一致 ＆ 3文字目以降がDB名の残りに含まれる
    if (predName.slice(0, 2) === dbName.slice(0, 2)) {
      const predRest = predName.slice(2);
      const dbRest = dbName.slice(2);
      if (predRest.length > 0 && dbRest.includes(predRest[0])) return true;
    }
    return false;
  };

  // 出走馬からユニークな騎手/調教師/種牡馬/母父を抽出（ID も合わせて抽出）
  const raceNames = useMemo(() => {
    const jockeys = new Set<string>();
    const trainers = new Set<string>();
    const sires = new Set<string>();
    const bms = new Set<string>();
    const jockeyIds = new Set<string>();
    const trainerIds = new Set<string>();
    for (const h of horses) {
      if (h.jockey) jockeys.add(normName(h.jockey));
      if (h.trainer) trainers.add(normName(h.trainer));
      const sire = (h as Record<string, unknown>).sire as string;
      const mgs = (h as Record<string, unknown>).maternal_grandsire as string;
      if (sire) sires.add(sire);
      if (mgs) bms.add(mgs);
      const jid = (h as Record<string, unknown>).jockey_id as string | undefined;
      const tid = (h as Record<string, unknown>).trainer_id as string | undefined;
      if (jid) jockeyIds.add(jid);
      if (tid) trainerIds.add(tid);
    }
    return { jockeys, trainers, sires, bms, jockeyIds, trainerIds };
  }, [horses]);

  // 当該コース条件（会場×馬場×距離±200m）で race_log 直接集計
  const courseQsBase = venueCode && surface && distance
    ? `venue=${venueCode}&surface=${encodeURIComponent(surface)}&distance=${distance}`
    : "";
  const jockeyQs  = courseQsBase ? `type=jockey&${courseQsBase}` : "";
  const trainerQs = courseQsBase ? `type=trainer&${courseQsBase}` : "";
  const sireQs    = courseQsBase ? `type=sire&${courseQsBase}` : "";
  const bmsQs     = courseQsBase ? `type=bms&${courseQsBase}` : "";

  const { data: jockeyData,  isLoading: jockeyLoading  } = usePersonnelAggCourse(jockeyQs);
  const { data: trainerData, isLoading: trainerLoading } = usePersonnelAggCourse(trainerQs);
  const { data: sireData,    isLoading: sireLoading    } = usePersonnelAggCourse(sireQs);
  const { data: bmsData,     isLoading: bmsLoading     } = usePersonnelAggCourse(bmsQs);

  // レース出走者のみフィルタ
  //  1) ID 一致（最優先、確実）
  //  2) 名前一致（ID でヒットしなかった残り予想名を対象、異体字＋省略名対応）
  const filterPersons = (
    data: typeof jockeyData,
    names: Set<string>,
    ids?: Set<string>,
  ) => {
    if (!data) return [];
    const persons = data.persons || data.data || [];
    const matched = new Map<string, Record<string, unknown>>();

    // (1) ID 優先マッチ
    if (ids && ids.size > 0) {
      for (const p of persons) {
        const pid = (p.id as string | undefined) || "";
        if (pid && ids.has(pid)) {
          matched.set(pid, p);
        }
      }
    }

    // (2) 名前マッチ（ID で既に拾ってない人のみ対象）
    const nameArr = Array.from(names);
    for (const predName of nameArr) {
      let bestMatch: Record<string, unknown> | null = null;
      let bestTotal = -1;
      for (const p of persons) {
        const pid = (p.id as string | undefined) || "";
        if (pid && matched.has(pid)) continue; // 既にIDで拾われた人はスキップ
        const dbNorm = normName(p.name);
        if (nameMatches(predName, dbNorm)) {
          const total = (p.total as number) || 0;
          if (total > bestTotal) {
            bestMatch = p;
            bestTotal = total;
          }
        }
      }
      if (bestMatch) {
        const key = (bestMatch.id as string) || (bestMatch.name as string);
        if (!matched.has(key)) {
          matched.set(key, bestMatch);
        }
      }
    }
    return Array.from(matched.values());
  };

  const jockeys = filterPersons(jockeyData, raceNames.jockeys, raceNames.jockeyIds);
  const trainers = filterPersons(trainerData, raceNames.trainers, raceNames.trainerIds);
  const sires = filterPersons(sireData, raceNames.sires);
  const bmsList = filterPersons(bmsData, raceNames.bms);

  // コースデータの各セクション
  const runStyle = (courseData as Record<string, unknown>)?.running_style as Record<string, Record<string, number>> | undefined;
  const gateBias = (courseData as Record<string, unknown>)?.gate_bias as Record<string, Record<string, number>> | undefined;
  const condDiff = (courseData as Record<string, unknown>)?.condition_diff as Record<string, Record<string, unknown>> | undefined;
  const courseDesc = (courseData as Record<string, unknown>)?.course_description as string | undefined;
  const record = (courseData as Record<string, unknown>)?.record as Record<string, unknown> | undefined;

  return (
    <div className="space-y-4">
      {/* ── コースデータ ── */}
      {courseKey && (
        <div>
          <h4 className="text-xs font-bold text-muted-foreground mb-2">
            コースデータ — {race.venue} {surface} {distance}m
            {record && (
              <span className="font-normal ml-2">
                レコード {record.time_str as string}（{record.date as string} {record.class_name as string}）
              </span>
            )}
          </h4>
          {courseDesc && (
            <p className="text-xs text-muted-foreground mb-2">{courseDesc}</p>
          )}

          {courseLoading ? (
            <div className="text-xs text-muted-foreground py-2">読み込み中...</div>
          ) : (
            // モバイル時は強制 1 列（sm: メディアクエリは PC 上のモバイルプレビューでは反応しないため）
            <div className={isMobile ? "grid grid-cols-1 gap-3" : "grid grid-cols-1 sm:grid-cols-3 gap-3"}>
              {/* 馬場状態別タイム */}
              {condDiff && (
                <div>
                  <h5 className="text-[10px] font-bold text-muted-foreground mb-1">馬場状態別タイム</h5>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-[10px] text-muted-foreground">
                        <th className="py-1 px-1 text-left">馬場</th>
                        <th className="py-1 px-1 text-right">件数</th>
                        <th className="py-1 px-1 text-right">平均</th>
                        <th className="py-1 px-1 text-right">差</th>
                      </tr>
                    </thead>
                    <tbody>
                      {["良", "稍重", "重", "不良"].map((cond) => {
                        const d = condDiff[cond];
                        if (!d) return null;
                        const diff = d.diff as number | null;
                        return (
                          <tr key={cond} className="border-b border-border/30">
                            <td className="py-0.5 px-1 font-bold">{cond}</td>
                            <td className="py-0.5 px-1 text-right tabular-nums">{d.n as number}</td>
                            <td className="py-0.5 px-1 text-right tabular-nums">{d.avg_str as string}</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${diff != null && diff > 0 ? "text-red-600" : diff != null && diff < 0 ? "text-emerald-600" : ""}`}>
                              {diff == null ? "基準" : `${diff > 0 ? "+" : ""}${diff.toFixed(1)}秒`}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}

              {/* 脚質別成績 */}
              {runStyle && (
                <div>
                  <h5 className="text-[10px] font-bold text-muted-foreground mb-1">脚質別成績</h5>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-[10px] text-muted-foreground">
                        <th className="py-1 px-1 text-left">脚質</th>
                        <th className="py-1 px-1 text-right">出走</th>
                        <th className="py-1 px-1 text-right">勝率</th>
                        <th className="py-1 px-1 text-right">連対率</th>
                        <th className="py-1 px-1 text-right">複勝率</th>
                        <th className="py-1 px-1 text-right">単回</th>
                      </tr>
                    </thead>
                    <tbody>
                      {["逃げ", "先行", "差し", "追込"].map((style) => {
                        const s = runStyle[style];
                        if (!s) return null;
                        return (
                          <tr key={style} className="border-b border-border/30">
                            <td className="py-0.5 px-1 font-bold">{style}</td>
                            <td className="py-0.5 px-1 text-right tabular-nums">{s.total}</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(s.win_rate, [15, 25])}`}>{s.win_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(s.place2_rate, [25, 40])}`}>{s.place2_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(s.place3_rate, [30, 50])}`}>{s.place3_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${roiCls(s.roi)}`}>{s.roi.toFixed(0)}%</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}

              {/* 枠番別成績 */}
              {gateBias && (
                <div>
                  <h5 className="text-[10px] font-bold text-muted-foreground mb-1">枠番別成績</h5>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-[10px] text-muted-foreground">
                        <th className="py-1 px-1 text-center">枠</th>
                        <th className="py-1 px-1 text-right">出走</th>
                        <th className="py-1 px-1 text-right">勝率</th>
                        <th className="py-1 px-1 text-right">連対率</th>
                        <th className="py-1 px-1 text-right">複勝率</th>
                        <th className="py-1 px-1 text-right">単回</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.keys(gateBias).sort((a, b) => Number(a) - Number(b)).map((gate) => {
                        const g = gateBias[gate];
                        if (!g) return null;
                        return (
                          <tr key={gate} className="border-b border-border/30">
                            <td className="py-0.5 px-1 text-center">
                              <span className={`inline-flex w-4 h-4 items-center justify-center rounded-sm text-[9px] font-bold ${WAKU_BG[Number(gate)] || "bg-gray-200"}`}>
                                {gate}
                              </span>
                            </td>
                            <td className="py-0.5 px-1 text-right tabular-nums">{g.runs}</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(g.win_rate, [10, 18])}`}>{g.win_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(g.place2_rate, [20, 30])}`}>{g.place2_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${pctCls(g.place3_rate, [25, 40])}`}>{g.place3_rate.toFixed(1)}%</td>
                            <td className={`py-0.5 px-1 text-right tabular-nums ${roiCls(g.roi)}`}>{g.roi.toFixed(0)}%</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ── 出走騎手 ── */}
      <PersonnelSection title="出走騎手" venue={race.venue || ""} surface={surface} distance={distance} persons={jockeys} showLocation={false} isLoading={jockeyLoading} />

      {/* ── 出走調教師 ── */}
      <PersonnelSection title="出走調教師" venue={race.venue || ""} surface={surface} distance={distance} persons={trainers} showLocation={true} isLoading={trainerLoading} />

      {/* ── 出走種牡馬 ── */}
      <PersonnelSection title="出走種牡馬" venue={race.venue || ""} surface={surface} distance={distance} persons={sires} showLocation={false} isLoading={sireLoading} />

      {/* ── 出走母父 ── */}
      <PersonnelSection title="出走母父（BMS）" venue={race.venue || ""} surface={surface} distance={distance} persons={bmsList} showLocation={false} isLoading={bmsLoading} />
    </div>
  );
}

// ============================================================
// 人物/血統テーブル
// ============================================================

interface PersonnelSectionProps {
  title: string;
  venue: string;
  surface: string;
  distance: number;
  persons: Record<string, unknown>[];
  showLocation: boolean;
  isLoading?: boolean;
}

function PersonnelSection({ title, venue, surface, distance, persons, showLocation, isLoading }: PersonnelSectionProps) {
  // 勝率降順ソート
  const sorted = [...persons].sort((a, b) => ((b.win_rate as number) || 0) - ((a.win_rate as number) || 0));
  const dMin = Math.max(200, distance - 200);
  const dMax = distance + 200;
  const scopeLabel = venue && surface && distance
    ? `${venue} ${surface} ${dMin}〜${dMax}m`
    : venue;

  return (
    <div>
      <h4 className="text-xs font-bold text-muted-foreground mb-1">
        {title}
        <span className="font-normal ml-1">（{scopeLabel}）</span>
        <span className="font-normal text-[10px] ml-1">{sorted.length}件</span>
      </h4>
      {isLoading ? (
        <div className="text-xs text-muted-foreground py-1">読み込み中...</div>
      ) : sorted.length === 0 ? (
        <div className="text-xs text-muted-foreground py-1">データなし</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-[10px] text-muted-foreground">
                <th className="py-1 px-1 text-left">名前</th>
                {showLocation && <th className="py-1 px-1 text-left">所属</th>}
                <th className="py-1 px-1 text-right">出走</th>
                <th className="py-1 px-1 text-left">成績</th>
                <th className="py-1 px-1 text-right">勝率</th>
                <th className="py-1 px-1 text-right">連対率</th>
                <th className="py-1 px-1 text-right">複勝率</th>
                <th className="py-1 px-1 text-right">単回収</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((p) => {
                const name = p.name as string;
                const total = (p.total as number) || 0;
                const win = (p.win as number) || 0;
                const p2 = (p.place2 as number) || 0;
                const p3 = (p.place3 as number) || 0;
                const winRate = (p.win_rate as number) || 0;
                const rentaiRate = (p.place2_rate as number) || 0;
                const fukushoRate = (p.place3_rate as number) || 0;
                const roi = (p.roi as number) || 0;
                const location = (p.location as string) || "";

                return (
                  <tr key={name} className="border-b border-border/30 hover:bg-muted/30">
                    <td className="py-1 px-1 font-bold whitespace-nowrap">{name}</td>
                    {showLocation && <td className="py-1 px-1 text-muted-foreground whitespace-nowrap">{location}</td>}
                    <td className="py-1 px-1 text-right tabular-nums">{total}</td>
                    <td className="py-1 px-1 tabular-nums whitespace-nowrap">{recordStr(win, p2, p3, total)}</td>
                    <td className={`py-1 px-1 text-right tabular-nums ${pctCls(winRate)}`}>{winRate.toFixed(1)}%</td>
                    <td className={`py-1 px-1 text-right tabular-nums ${pctCls(rentaiRate, [30, 45])}`}>{rentaiRate.toFixed(1)}%</td>
                    <td className={`py-1 px-1 text-right tabular-nums ${pctCls(fukushoRate, [35, 50])}`}>{fukushoRate.toFixed(1)}%</td>
                    <td className={`py-1 px-1 text-right tabular-nums ${roiCls(roi)}`}>{roi.toFixed(0)}%</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
