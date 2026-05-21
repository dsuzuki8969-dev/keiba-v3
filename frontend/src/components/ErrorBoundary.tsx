import { Component, type ReactNode, type ErrorInfo } from "react";

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("ErrorBoundary caught:", error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) return this.props.fallback;
      return (
        <div className="flex flex-col items-center justify-center min-h-[200px] p-6 text-center">
          <p className="text-lg font-semibold text-destructive mb-2">表示エラーが発生しました</p>
          <p className="text-sm text-muted-foreground mb-4">
            {this.state.error?.message || "不明なエラー"}
          </p>
          <button
            onClick={() => this.setState({ hasError: false, error: null })}
            className="px-4 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
          >
            再試行
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
