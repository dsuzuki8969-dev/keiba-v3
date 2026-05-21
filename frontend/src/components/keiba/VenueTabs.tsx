import { memo } from "react";

interface Props {
  venues: string[];
  activeIndex: number;
  onChange: (index: number) => void;
}

export const VenueTabs = memo(function VenueTabs({ venues, activeIndex, onChange }: Props) {
  if (!venues.length) return null;
  return (
    <div className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg overflow-x-auto">
      {venues.map((v, i) => (
        <button
          key={v}
          onClick={() => onChange(i)}
          className={`px-3 py-1 text-xs font-semibold rounded-md whitespace-nowrap transition-all ${
            i === activeIndex
              ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
              : "text-muted-foreground hover:text-foreground hover:bg-background/60"
          }`}
        >
          {v}
        </button>
      ))}
    </div>
  );
});
VenueTabs.displayName = "VenueTabs";
