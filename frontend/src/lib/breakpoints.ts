/**
 * Tailwind v4 標準 breakpoints と整合したピクセル定数。
 * コンポーネント内でのマジックナンバーを排除し、一元管理する。
 */
export const BREAKPOINTS = {
  SM: 640,
  MD: 768,
  LG: 1024,
  XL: 1280,
  XXL: 1536,
} as const;

export type BreakpointKey = keyof typeof BREAKPOINTS;
