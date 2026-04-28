import { useClock } from "@/hooks/useClock";

export function Clock() {
  const { formatted } = useClock();
  return (
    <span className="text-xs text-header-text/80 font-mono whitespace-nowrap">
      {formatted}
    </span>
  );
}
