import { Component, type ReactNode, type ErrorInfo } from "react";

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  /** chunk-load 失敗を検知して自動 reload を一度だけ試行するためのフラグ */
  reloading: boolean;
}

/**
 * dynamic import (lazy()) の chunk load 失敗を判定するユーティリティ。
 * vite build を繰り返すと古いブラウザ manifest が削除済 chunk を読みに行き
 * 404 → "Failed to fetch dynamically imported module" が発生する。
 * これを検知したら sessionStorage で 1 回だけ自動 hard reload して新 entry を取得する。
 */
function isChunkLoadError(err: Error | null): boolean {
  if (!err) return false;
  const msg = (err.message || "").toLowerCase();
  return (
    msg.includes("failed to fetch dynamically imported module") ||
    msg.includes("loading chunk") ||
    msg.includes("loading css chunk") ||
    msg.includes("importing a module script failed")
  );
}

const RELOAD_FLAG_KEY = "ErrorBoundary.chunkReloadAttempted";

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null, reloading: false };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, reloading: false };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("ErrorBoundary caught:", error, info.componentStack);

    // chunk-load 失敗の場合は 1 回だけ自動で hard reload を試みる
    // (sessionStorage で無限ループを防止)
    if (isChunkLoadError(error)) {
      try {
        const attempted = sessionStorage.getItem(RELOAD_FLAG_KEY);
        if (attempted !== "1") {
          sessionStorage.setItem(RELOAD_FLAG_KEY, "1");
          this.setState({ reloading: true });
          // 少し待ってからリロード (連打防止)
          setTimeout(() => {
            window.location.reload();
          }, 500);
          return;
        }
      } catch {
        /* sessionStorage 未対応環境では何もしない */
      }
    }
  }

  handleManualReload = () => {
    try {
      sessionStorage.removeItem(RELOAD_FLAG_KEY);
    } catch {
      /* ignore */
    }
    window.location.reload();
  };

  render() {
    if (this.state.hasError) {
      // chunk-load エラーで自動 reload 中: 何も表示せず白画面で reload を待つ (一瞬)
      if (this.state.reloading) {
        return (
          <div className="flex items-center justify-center min-h-[200px] text-sm text-muted-foreground">
            アプリを更新しています…
          </div>
        );
      }

      if (this.props.fallback) return this.props.fallback;

      const isChunk = isChunkLoadError(this.state.error);

      return (
        <div className="flex flex-col items-center justify-center min-h-[200px] p-6 text-center">
          <p className="text-lg font-semibold text-destructive mb-2">
            {isChunk ? "アプリの更新が必要です" : "表示エラーが発生しました"}
          </p>
          <p className="text-sm text-muted-foreground mb-4">
            {isChunk
              ? "新しいバージョンがリリースされました。再読み込みで最新版に切り替わります。"
              : this.state.error?.message || "不明なエラー"}
          </p>
          <button
            onClick={
              isChunk
                ? this.handleManualReload
                : () => this.setState({ hasError: false, error: null })
            }
            className="px-4 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
          >
            {isChunk ? "最新版に更新" : "再試行"}
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
