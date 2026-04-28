interface Props {
  venues: string[];
  activeIndex: number;
  onChange: (index: number) => void;
}

export function VenueTabs({ venues, activeIndex, onChange }: Props) {
  if (!venues.length) return null;
  return (
    <div className="flex gap-1 overflow-x-auto pb-1">
      {venues.map((v, i) => (
        <button
          key={v}
          onClick={() => onChange(i)}
          className={`px-4 py-2 text-sm font-medium rounded-md whitespace-nowrap transition-colors ${
            i === activeIndex
              ? "bg-primary text-primary-foreground"
              : "bg-secondary text-secondary-foreground hover:bg-muted"
          }`}
        >
          {v}
        </button>
      ))}
    </div>
  );
}
