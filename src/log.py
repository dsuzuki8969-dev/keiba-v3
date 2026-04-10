"""プロジェクト共通ログ設定。

使い方:
    from src.log import get_logger
    logger = get_logger(__name__)
    logger.info("メッセージ")
"""
import logging
import os
import sys

_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log")
_CONFIGURED = False

try:
    from rich.console import Console
    from rich.logging import RichHandler

    # nohup/バッチ実行時はRichを無効化（stdout肥大化防止）
    _HAS_RICH = sys.stdout is not None and sys.stdout.isatty()
    console = Console() if _HAS_RICH else None
except ImportError:
    _HAS_RICH = False
    console = None


def setup(level: int = logging.INFO, log_file: str = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    os.makedirs(_LOG_DIR, exist_ok=True)

    root = logging.getLogger("keiba")
    root.setLevel(level)

    if _HAS_RICH:
        rh = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
        )
        rh.setLevel(level)
        root.addHandler(rh)
    else:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)

    if log_file:
        file_fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        fh = logging.FileHandler(
            os.path.join(_LOG_DIR, log_file), encoding="utf-8"
        )
        fh.setFormatter(file_fmt)
        root.addHandler(fh)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    if not _CONFIGURED:
        setup()
    if name.startswith("src."):
        name = name[4:]
    return logging.getLogger(f"keiba.{name}")


# ============================================================
# 共通プログレスバー
# ============================================================

def progress_bar(iterable=None, total=None, description="処理中"):
    """全長時間処理で使える共通プログレスバー。

    経過時間・XX.X%・残り時間を表示する。

    使い方:
        for item in progress_bar(items, description="分析中"):
            process(item)

        # totalを明示的に指定
        for item in progress_bar(items, total=100, description="学習中"):
            process(item)
    """
    if _HAS_RICH:
        from rich.progress import (
            Progress, BarColumn, TextColumn, TimeElapsedColumn,
            TimeRemainingColumn, MofNCompleteColumn, SpinnerColumn,
        )
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("[progress.percentage]{task.percentage:>5.1f}%"),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TextColumn("残り"),
            TimeRemainingColumn(),
            console=console,
        )
        if iterable is not None:
            _total = total if total is not None else (len(iterable) if hasattr(iterable, "__len__") else None)
            with progress:
                task = progress.add_task(description, total=_total)
                for item in iterable:
                    yield item
                    progress.advance(task)
        else:
            yield progress
    else:
        # Rich非対応環境: シンプルなテキスト進捗
        import time as _time
        _total = total if total is not None else (len(iterable) if iterable and hasattr(iterable, "__len__") else None)
        t0 = _time.time()
        if iterable is not None:
            for i, item in enumerate(iterable):
                yield item
                if _total and (i + 1) % max(1, _total // 20) == 0:
                    elapsed = _time.time() - t0
                    pct = (i + 1) / _total * 100
                    remaining = elapsed / (i + 1) * (_total - i - 1) if i > 0 else 0
                    print(f"  {description}: {pct:.1f}% ({i+1}/{_total}) "
                          f"経過{elapsed:.0f}秒 残り{remaining:.0f}秒", flush=True)
        else:
            yield None
