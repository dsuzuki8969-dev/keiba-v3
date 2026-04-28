import { useState, useEffect } from "react";
import { localDate } from "@/lib/constants";

/**
 * 現在のローカル日付を返すフック。
 * 日付が変わった瞬間（0:00）に自動更新される。
 */
export function useLocalDate(): string {
  const [date, setDate] = useState(() => localDate());

  useEffect(() => {
    // 次の0:00までのミリ秒を計算
    const msUntilMidnight = () => {
      const now = new Date();
      const midnight = new Date(now);
      midnight.setHours(24, 0, 0, 0);
      return midnight.getTime() - now.getTime();
    };

    let timerId: ReturnType<typeof setTimeout>;

    const scheduleUpdate = () => {
      timerId = setTimeout(() => {
        setDate(localDate());
        // 次の0:00にも発火するよう再スケジュール
        scheduleUpdate();
      }, msUntilMidnight() + 500); // 500ms余裕を持たせる
    };

    scheduleUpdate();
    return () => clearTimeout(timerId);
  }, []);

  return date;
}
