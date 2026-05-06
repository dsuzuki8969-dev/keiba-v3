"""
netkeiba アクセス ブローカー - プロセス間排他ロック (フェーズ C)

目的:
  複数プロセスが同時に netkeiba へアクセスするのを物理的に防ぐ。
  過去事故 (2026-04-28: 並列アクセス → 403 × 10,398 件 + 3h 遅延) の再発防止。

依存:
  portalocker >= 2.0.0  (Windows + Unix 両対応 file lock)
  インポート失敗時は警告ログのみ出して機能を無効化する (起動阻害禁止)。

使い方:
  from src.scraper.netkeiba_access_broker import NetkeibaAccessBroker, get_default_broker

  # context manager (推奨)
  with NetkeibaAccessBroker() as broker:
      # この block 内だけ lock を保持
      client.get(url)

  # 明示的な acquire/release
  broker = get_default_broker()
  if broker.acquire(timeout=5.0):
      try:
          client.get(url)
      finally:
          broker.release()
"""

import os
import threading
from typing import Optional

from src.log import get_logger

logger = get_logger(__name__)

# portalocker を optional import - 取り込み失敗でも起動を妨げない
try:
    import portalocker

    _HAS_PORTALOCKER = True
except ImportError:
    portalocker = None  # type: ignore[assignment]
    _HAS_PORTALOCKER = False
    logger.warning(
        "portalocker が見つかりません。netkeiba_access_broker は無効化されます。"
        " (pip install portalocker>=2.0.0 で有効化できます)"
    )

# ============================================================
# 定数
# ============================================================

# プロジェクトルート/tmp/ 配下に lock ファイルを置く
_BROKER_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_BROKER_MODULE_DIR))
LOCK_FILE_DEFAULT = os.path.join(_PROJECT_ROOT, "tmp", "netkeiba_broker.lock")


# ============================================================
# NetkeibaAccessBroker クラス
# ============================================================


class NetkeibaAccessBroker:
    """netkeiba へのアクセス権をプロセス間で排他制御する file lock ブローカー。

    スレッドセーフ: インスタンスごとに threading.Lock を持ち、
    同一プロセス内の複数スレッドからの同時呼び出しも安全に処理する。

    portalocker 未インストール時は acquire() が即 True を返す (ノーオプ模倣)。
    これにより既存コードへの影響ゼロを保証する。
    """

    def __init__(
        self,
        lock_file: Optional[str] = None,
        timeout: float = 5.0,
    ) -> None:
        """
        Args:
            lock_file: ロックファイルパス。None の場合は LOCK_FILE_DEFAULT を使用。
            timeout: acquire() のデフォルトタイムアウト秒数。
        """
        self._lock_file = lock_file if lock_file is not None else LOCK_FILE_DEFAULT
        self._default_timeout = timeout
        self._file_handle = None  # portalocker が管理するファイルオブジェクト
        self._thread_lock = threading.Lock()  # スレッドレベルの排他 (同一プロセス保護)
        self._acquired = False  # 現在 lock を保持しているか

        # lock ファイルが置かれる tmp/ ディレクトリを事前作成
        lock_dir = os.path.dirname(self._lock_file)
        if lock_dir:
            try:
                os.makedirs(lock_dir, exist_ok=True)
            except Exception as e:
                logger.warning("lock ファイルディレクトリの作成に失敗 (続行): %s", e)

    # ----------------------------------------------------------
    # 公開 API
    # ----------------------------------------------------------

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """netkeiba アクセス権を取得する。

        Args:
            timeout: タイムアウト秒数。None の場合はインスタンスのデフォルト値を使用。

        Returns:
            True: ロック取得成功
            False: タイムアウトまたは portalocker 未インストール時 (ノーオプ) で True
        """
        if not _HAS_PORTALOCKER:
            # portalocker 未インストール: ノーオプ (警告は import 時に出し済み)
            with self._thread_lock:
                self._acquired = True
            return True

        actual_timeout = timeout if timeout is not None else self._default_timeout

        with self._thread_lock:
            if self._acquired:
                # 同一スレッドが二重に acquire → 既に lock 保持中なので True を返す
                logger.debug("broker: 既にロック取得済み (二重 acquire): %s", self._lock_file)
                return True

            try:
                # lock ファイルを open してから portalocker でロック
                self._file_handle = open(self._lock_file, "w", encoding="utf-8")
                portalocker.lock(
                    self._file_handle,
                    portalocker.LOCK_EX | portalocker.LOCK_NB,
                )
                self._acquired = True
                logger.debug("broker: ロック取得成功: %s", self._lock_file)
                return True

            except portalocker.LockException:
                # 別プロセスが lock 中 → timeout まで待機してリトライ
                logger.info(
                    "broker: 別プロセスが lock 中 - %.1f 秒待機します: %s",
                    actual_timeout,
                    self._lock_file,
                )
                # ファイルハンドルをいったん閉じてから待機ループへ
                self._close_file_handle()

                import time
                deadline = time.time() + actual_timeout
                while time.time() < deadline:
                    time.sleep(0.1)
                    try:
                        self._file_handle = open(self._lock_file, "w", encoding="utf-8")
                        portalocker.lock(
                            self._file_handle,
                            portalocker.LOCK_EX | portalocker.LOCK_NB,
                        )
                        self._acquired = True
                        logger.debug("broker: ロック取得成功 (待機後): %s", self._lock_file)
                        return True
                    except portalocker.LockException:
                        self._close_file_handle()

                # タイムアウト
                logger.warning(
                    "broker: ロック取得タイムアウト (%.1f 秒): %s - アクセスを続行します",
                    actual_timeout,
                    self._lock_file,
                )
                return False

            except Exception as e:
                # 予期しないエラー → warning + 続行 (起動阻害禁止)
                logger.warning(
                    "broker: acquire 中に予期しないエラー (続行): %s", e, exc_info=True
                )
                self._close_file_handle()
                return False

    def release(self) -> None:
        """netkeiba アクセス権を解放する。

        acquire() していない状態で呼び出しても安全 (ノーオプ)。
        """
        with self._thread_lock:
            if not self._acquired:
                return
            try:
                if _HAS_PORTALOCKER and self._file_handle is not None:
                    portalocker.unlock(self._file_handle)
            except Exception as e:
                logger.warning("broker: unlock 中にエラー (続行): %s", e, exc_info=True)
            finally:
                self._close_file_handle()
                self._acquired = False
                logger.debug("broker: ロック解放: %s", self._lock_file)

    def is_locked(self) -> bool:
        """現在このブローカーインスタンスが lock を保持しているか返す (診断用)。"""
        with self._thread_lock:
            return self._acquired

    # ----------------------------------------------------------
    # context manager
    # ----------------------------------------------------------

    def __enter__(self) -> "NetkeibaAccessBroker":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    # ----------------------------------------------------------
    # 内部ヘルパー
    # ----------------------------------------------------------

    def _close_file_handle(self) -> None:
        """ファイルハンドルをクローズする (エラーは無視)。"""
        if self._file_handle is not None:
            try:
                self._file_handle.close()
            except Exception:
                pass
            self._file_handle = None


# ============================================================
# モジュールレベル singleton 風ファクトリ
# ============================================================

_DEFAULT_BROKER: Optional[NetkeibaAccessBroker] = None
_SINGLETON_LOCK = threading.Lock()


def get_default_broker() -> NetkeibaAccessBroker:
    """デフォルト設定の NetkeibaAccessBroker を返す (singleton 風)。

    同一プロセス内で複数回呼び出しても同じインスタンスを返す。
    lock ファイルは LOCK_FILE_DEFAULT (tmp/netkeiba_broker.lock)。
    """
    global _DEFAULT_BROKER
    if _DEFAULT_BROKER is None:
        with _SINGLETON_LOCK:
            if _DEFAULT_BROKER is None:
                _DEFAULT_BROKER = NetkeibaAccessBroker(
                    lock_file=LOCK_FILE_DEFAULT,
                    timeout=5.0,
                )
                logger.debug(
                    "broker: default broker を初期化: %s", LOCK_FILE_DEFAULT
                )
    return _DEFAULT_BROKER
