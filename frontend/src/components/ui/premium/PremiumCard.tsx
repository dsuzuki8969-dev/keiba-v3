import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

/**
 * PremiumCard — D-Aikeiba v6.1
 * --------------------------------------------------------------
 * shadcn/ui の Card を CVA で拡張した高級感プリミティブ。
 *  - default  : 標準カード（微細シャドウ）
 *  - gold     : 金箔リング＋ゴールドグロー（◉鉄板・SS用）
 *  - navy-glow: ネイビー発光（ダークモード CTA 用）
 *  - flat     : 影なし・下線のみ（密度優先のテーブル行詳細など）
 *  - soft     : 柔らかい塗り（読み物）
 *
 * タグは `as="div" | "button" | ...` で切替可能。button タグ時には
 * type="button" を自動注入してフォーム内誤submitを防ぐ。
 */

const cardVariants = cva(
  [
    "rounded-xl bg-card text-card-foreground",
    "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
    "will-change-[transform,box-shadow]",
  ].join(" "),
  {
    variants: {
      variant: {
        default: "border border-border shadow-[var(--shadow-sm)] hover:shadow-[var(--shadow-md)]",
        gold:
          "border border-brand-gold/50 shadow-[var(--shadow-gold-glow)] " +
          "hover:shadow-[0_0_0_1px_rgba(212,168,83,0.55),0_14px_32px_-6px_rgba(212,168,83,0.45)]",
        "navy-glow":
          "border border-brand-navy-light/70 shadow-[var(--shadow-navy-glow)] " +
          "hover:shadow-[0_0_0_1px_rgba(96,165,250,0.40),0_14px_30px_-6px_rgba(30,64,175,0.50)]",
        flat: "border-b border-border bg-transparent shadow-none rounded-none",
        soft: "border border-border/60 bg-secondary/60 shadow-[var(--shadow-xs)]",
      },
      padding: {
        none: "p-0",
        sm: "p-3",
        md: "p-4",
        lg: "p-6",
      },
      interactive: {
        true: "cursor-pointer hover:-translate-y-[1px] active:translate-y-0",
        false: "",
      },
    },
    defaultVariants: {
      variant: "default",
      padding: "md",
      interactive: false,
    },
  },
);

type CardVariantProps = VariantProps<typeof cardVariants>;

// タグごとに型を分岐させることで `as never` を不要にする
type BaseProps = CardVariantProps & { className?: string };

type PremiumCardDivProps = BaseProps & { as?: "div" | "article" | "section" } & React.HTMLAttributes<HTMLDivElement>;
type PremiumCardButtonProps = BaseProps & { as: "button" } & React.ButtonHTMLAttributes<HTMLButtonElement>;

export type PremiumCardProps = PremiumCardDivProps | PremiumCardButtonProps;

export const PremiumCard = React.forwardRef<HTMLElement, PremiumCardProps>(
  function PremiumCard(props, ref) {
    const { className, variant, padding, interactive, ...rest } = props;
    const asTag = (props as { as?: string }).as ?? "div";
    const cls = cn(cardVariants({ variant, padding, interactive }), className);

    if (asTag === "button") {
      const buttonProps = rest as React.ButtonHTMLAttributes<HTMLButtonElement>;
      // as prop は DOM に流さない
      delete (buttonProps as Record<string, unknown>).as;
      return (
        <button
          ref={ref as React.Ref<HTMLButtonElement>}
          type={buttonProps.type ?? "button"}
          className={cls}
          {...buttonProps}
        />
      );
    }
    if (asTag === "article") {
      const divProps = rest as React.HTMLAttributes<HTMLElement>;
      delete (divProps as Record<string, unknown>).as;
      return <article ref={ref as React.Ref<HTMLElement>} className={cls} {...divProps} />;
    }
    if (asTag === "section") {
      const divProps = rest as React.HTMLAttributes<HTMLElement>;
      delete (divProps as Record<string, unknown>).as;
      return <section ref={ref as React.Ref<HTMLElement>} className={cls} {...divProps} />;
    }

    // default: div
    const divProps = rest as React.HTMLAttributes<HTMLDivElement>;
    delete (divProps as Record<string, unknown>).as;
    return <div ref={ref as React.Ref<HTMLDivElement>} className={cls} {...divProps} />;
  },
);

/** カードヘッダー（タイトル + 補助） */
export function PremiumCardHeader({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "flex items-start justify-between gap-3 border-b border-border/60 pb-2 mb-3",
        className,
      )}
      {...props}
    />
  );
}

/** カードタイトル（セクションレベル） */
export function PremiumCardTitle({
  className,
  ...props
}: React.HTMLAttributes<HTMLHeadingElement>) {
  return (
    <h3
      className={cn(
        "heading-section text-lg text-foreground",
        className,
      )}
      {...props}
    />
  );
}

/** カードアクセント（金箔グラデ見出し） */
export function PremiumCardAccent({
  className,
  ...props
}: React.HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn(
        "gold-gradient font-extrabold tracking-wide uppercase text-xs",
        className,
      )}
      {...props}
    />
  );
}
