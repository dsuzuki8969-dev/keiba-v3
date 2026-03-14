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
    _HAS_RICH = sys.stdout.isatty()
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
