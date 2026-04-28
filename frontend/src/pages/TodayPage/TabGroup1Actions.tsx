import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { MovieEmbed } from "./MovieEmbed";
import type { RaceDetail } from "./RaceDetailView";

// NAR映像トラックマップ
const NAR_TRACK_MAP: Record<string, string> = {
  "帯広": "obihiro", "門別": "monbetsu", "盛岡": "morioka", "水沢": "mizusawa",
  "浦和": "urawa", "船橋": "funabashi", "大井": "ooi", "川崎": "kawasaki",
  "金沢": "kanazawa", "笠松": "kasamatsu", "名古屋": "nagoya", "園田": "sonoda",
  "姫路": "himeji", "高知": "kouchi", "佐賀": "saga",
};
// NAR結果用babaCode
const NAR_BABA_CODE: Record<string, string> = {
  "帯広": "3", "門別": "36", "盛岡": "10", "水沢": "11",
  "浦和": "18", "船橋": "19", "大井": "20", "川崎": "21",
  "金沢": "22", "笠松": "23", "名古屋": "24", "園田": "27",
  "姫路": "28", "高知": "31", "佐賀": "32",
};

// JRA映像ターゲット: race_id (YYYY JJ KK NN RR) → (YYYY KK JJ NN RR)
function jraVideoTarget(raceId: string): string {
  if (!raceId || raceId.length < 12) return "";
  return raceId.slice(0, 4) + raceId.slice(6, 8) + raceId.slice(4, 6) + raceId.slice(8, 12);
}

interface Props {
  race: RaceDetail;
  date: string;
  raceNo: number;
  oddsFetching: boolean;
  oddsMsg: string;
  onFetchOdds: () => void;
}

export function TabGroup1Actions({ race, date, raceNo, oddsFetching, oddsMsg, onFetchOdds }: Props) {
  const dateStr = date.replace(/-/g, "");

  // レース結果URL
  const resultUrl = (() => {
    if (race.is_jra === false) {
      const baba = NAR_BABA_CODE[race.venue || ""];
      if (!baba) return "";
      const d = dateStr.slice(0, 4) + "/" + dateStr.slice(4, 6) + "/" + dateStr.slice(6, 8);
      return `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/RaceMarkTable?k_raceDate=${encodeURIComponent(d)}&k_raceNo=${raceNo}&k_babaCode=${baba}`;
    } else {
      if (race.result_cname) {
        return `https://www.jra.go.jp/JRADB/accessS.html?CNAME=${race.result_cname}`;
      }
      return "https://www.jra.go.jp/keiba/thisweek/seiseki/";
    }
  })();

  // レース映像URL
  const movieUrl = (() => {
    if (race.is_jra === false) {
      const track = NAR_TRACK_MAP[race.venue || ""];
      if (!track) return "";
      return `http://keiba-lv-st.jp/movie/player?date=${dateStr}&race=${raceNo}&track=${track}`;
    } else {
      const target = jraVideoTarget(race.race_id || "");
      if (!target) return "";
      return `/static/video_jra.html?target=${target}`;
    }
  })();

  // 出馬表URL
  const shutubaUrl = (() => {
    if (race.is_jra === false) {
      const baba = NAR_BABA_CODE[race.venue || ""];
      if (!baba) return "";
      const d = dateStr.slice(0, 4) + "/" + dateStr.slice(4, 6) + "/" + dateStr.slice(6, 8);
      return `https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable?k_raceDate=${encodeURIComponent(d)}&k_raceNo=${raceNo}&k_babaCode=${baba}`;
    } else {
      if (race.shutuba_cname) {
        return `https://www.jra.go.jp/JRADB/accessD.html?CNAME=${race.shutuba_cname}`;
      }
      return "https://www.jra.go.jp/keiba/thisweek/syutsuba/";
    }
  })();

  // レースライブURL
  const liveUrl = (() => {
    if (race.is_jra === false) {
      const track = NAR_TRACK_MAP[race.venue || ""];
      if (!track) return "";
      return `https://simple.keiba-lv-st.jp/?track=${track}`;
    } else {
      return "https://www.jra.go.jp/keiba/";
    }
  })();

  const linkBtn = "inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md transition-colors";

  return (
    <Tabs defaultValue="odds">
      <TabsList variant="line" className="w-full overflow-x-auto">
        <TabsTrigger value="odds">オッズ取得</TabsTrigger>
        <TabsTrigger value="shutsuba">出馬表</TabsTrigger>
        <TabsTrigger value="result">レース結果</TabsTrigger>
        <TabsTrigger value="movie">レース映像</TabsTrigger>
        <TabsTrigger value="live">レースライブ</TabsTrigger>
      </TabsList>

      <TabsContent value="odds" className="pt-2">
        <div className="flex items-center gap-3">
          <button
            onClick={onFetchOdds}
            disabled={oddsFetching || !race.race_id}
            className="px-4 py-2 text-sm font-medium rounded-md bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 transition-colors"
          >
            {oddsFetching ? "取得中…" : "オッズを取得する"}
          </button>
          {oddsMsg && (
            <span className="text-sm text-muted-foreground">{oddsMsg}</span>
          )}
        </div>
      </TabsContent>

      <TabsContent value="shutsuba" className="pt-2">
        {shutubaUrl ? (
          <a href={shutubaUrl} target="_blank" rel="noopener noreferrer"
            className={`${linkBtn} bg-yellow-500 text-black hover:opacity-90`}>
            公式出馬表を開く ↗
          </a>
        ) : (
          <span className="text-sm text-muted-foreground">URLなし</span>
        )}
      </TabsContent>

      <TabsContent value="result" className="pt-2">
        {resultUrl ? (
          <a href={resultUrl} target="_blank" rel="noopener noreferrer"
            className={`${linkBtn} bg-blue-600 text-white hover:opacity-90`}>
            レース結果を開く ↗
          </a>
        ) : (
          <span className="text-sm text-muted-foreground">URLなし</span>
        )}
      </TabsContent>

      <TabsContent value="movie" className="pt-2">
        {movieUrl ? (
          <MovieEmbed
            url={movieUrl}
            externalUrl={movieUrl}
            label={race.is_jra === false ? "NAR レース映像" : "JRA レース映像"}
            external={race.is_jra === false}
          />
        ) : (
          <span className="text-sm text-muted-foreground">映像URLなし</span>
        )}
      </TabsContent>

      <TabsContent value="live" className="pt-2">
        {liveUrl ? (
          <a href={liveUrl} target="_blank" rel="noopener noreferrer"
            className={`${linkBtn} bg-purple-600 text-white hover:opacity-90`}>
            レースライブを開く ↗
          </a>
        ) : (
          <span className="text-sm text-muted-foreground">ライブURLなし</span>
        )}
      </TabsContent>
    </Tabs>
  );
}
