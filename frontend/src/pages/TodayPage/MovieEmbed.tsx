import { useEffect, useRef, useState } from "react";

interface Props {
  /** iframe に表示するURL（同一オリジン or 外部） */
  url: string;
  /** iframe が拒否された/失敗した時に別タブで開くフォールバック用URL（通常 url と同じ） */
  externalUrl?: string;
  /** 埋め込み枠の上部に表示するラベル（例: "JRA映像" / "NAR映像"） */
  label?: string;
  /** iframe の src オリジンが外部ドメインなら true（フォールバック検知の閾値を調整） */
  external?: boolean;
}

/**
 * レース映像をページ内に iframe で埋め込むコンポーネント。
 *
 * 動作:
 *   1. iframe の src に url を読み込む
 *   2. onLoad が来れば「表示成功」とみなしスピナーを消す
 *   3. 外部オリジンで X-Frame-Options / CSP により表示できなかった場合、
 *      ブラウザは onLoad を発火しつつ中身を空にすることがあり確実な検知は不可能。
 *      そのため、外部オリジンは onLoad 発火後にもフォールバックリンクを常時併置する。
 *   4. 一定時間(=タイムアウト)ロードが来なければ「失敗」表示 + フォールバックリンク
 *
 * 設計メモ:
 *   同一オリジン(JRA video_jra.html)は確実に動作する想定。
 *   外部(NARの keiba-lv-st)は X-Frame-Options 次第。ダメならユーザーは
 *   フォールバックボタンから別タブで開ける。
 */
export function MovieEmbed({ url, externalUrl, label, external }: Props) {
  const [loaded, setLoaded] = useState(false);
  const [timedOut, setTimedOut] = useState(false);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  useEffect(() => {
    // URL が変わったらロード状態をリセット
    setLoaded(false);
    setTimedOut(false);
    // 6秒以内に onLoad が来なかったら timedOut にする
    const timer = window.setTimeout(() => setTimedOut(true), 6000);
    return () => window.clearTimeout(timer);
  }, [url]);

  const handleLoad = () => {
    setLoaded(true);
  };

  const fallbackHref = externalUrl || url;
  const showLoading = !loaded && !timedOut;
  const showTimeout = !loaded && timedOut;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-sm font-bold text-muted-foreground">
          {label || "レース映像"}
        </div>
        <a
          href={fallbackHref}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-blue-600 hover:underline whitespace-nowrap"
        >
          別タブで開く ↗
        </a>
      </div>

      <div className="relative w-full aspect-video bg-black rounded-md overflow-hidden border border-border">
        <iframe
          ref={iframeRef}
          src={url}
          onLoad={handleLoad}
          className="absolute inset-0 w-full h-full"
          allow="fullscreen; autoplay"
          referrerPolicy="no-referrer-when-downgrade"
          title={label || "レース映像"}
        />

        {showLoading && (
          <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground pointer-events-none">
            映像読込中…
          </div>
        )}

        {showTimeout && (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 bg-black/80 text-center px-4">
            <div className="text-sm text-white">
              映像を埋め込み表示できませんでした
            </div>
            <div className="text-xs text-white/70">
              サイト側で埋め込みが許可されていない可能性があります
            </div>
            <a
              href={fallbackHref}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center justify-center px-3 py-1.5 text-xs font-medium rounded-md bg-red-600 text-white hover:opacity-90"
            >
              別タブで開く ↗
            </a>
          </div>
        )}
      </div>

      {external && (
        <div className="text-[11px] text-muted-foreground">
          ※ 外部サイトの映像プレイヤーを埋め込んでいます。表示されない場合は「別タブで開く」からご覧ください。
        </div>
      )}
    </div>
  );
}
