/**
 * 能力指数表示モード（絶対指数 / 相対指数）をグローバルで管理するフック
 *
 * - "absolute" : 通常の ability_total 偏差値（デフォルト）
 * - "relative" : 同レース内 z-score 正規化の race_relative_dev（20〜80, 50中心）
 *
 * LocalStorage キー: "dai-keiba/ability-display-mode"
 * グローバル設定（馬カード単位ではなく全馬で統一切替）
 */
import { useState, useCallback, useEffect } from "react";

export type AbilityDisplayMode = "absolute" | "relative";

const STORAGE_KEY = "dai-keiba/ability-display-mode";

/** LocalStorage から初期値を読み込む（不正値は "absolute" にフォールバック） */
function loadMode(): AbilityDisplayMode {
  if (typeof window === "undefined" || typeof localStorage === "undefined") {
    return "absolute";
  }
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    if (v === "absolute" || v === "relative") return v;
  } catch {
    /* 無視 */
  }
  return "absolute";
}

/** LocalStorage に保存する */
function saveMode(mode: AbilityDisplayMode): void {
  if (typeof window === "undefined" || typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, mode);
  } catch {
    /* 無視 */
  }
}

/**
 * 能力指数表示モードと切替関数を返す。
 * ページ全体で共有するグローバルステートの代わりに storage イベントで同期する。
 */
export function useAbilityDisplayMode(): {
  mode: AbilityDisplayMode;
  toggle: () => void;
  setMode: (m: AbilityDisplayMode) => void;
} {
  const [mode, setModeState] = useState<AbilityDisplayMode>(loadMode);

  // 別タブ/別コンポーネントからの LocalStorage 変更を受信して同期
  useEffect(() => {
    const handler = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY && (e.newValue === "absolute" || e.newValue === "relative")) {
        setModeState(e.newValue);
      }
    };
    window.addEventListener("storage", handler);
    return () => window.removeEventListener("storage", handler);
  }, []);

  const setMode = useCallback((m: AbilityDisplayMode) => {
    setModeState(m);
    saveMode(m);
  }, []);

  const toggle = useCallback(() => {
    setModeState((prev) => {
      const next: AbilityDisplayMode = prev === "absolute" ? "relative" : "absolute";
      saveMode(next);
      return next;
    });
  }, []);

  return { mode, toggle, setMode };
}
