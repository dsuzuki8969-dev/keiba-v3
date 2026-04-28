"""
JSON の原子書き込み + プロセス間排他ロック ユーティリティ。

なぜ必要か:
- pred.json は dashboard(Flask) と scheduler(APScheduler) の複数プロセスから書き込まれる
- Flask は threaded=True でマルチリクエスト並行
- ダッシュボードが 2 プロセス LISTENING していた場合、片方の書き込み中に他方が書き込み始めると
  `""key":`, `,,`, `key:: value` のような JSON 構文違反が混入する
- 2026-04-19 05:10 の pred.json 破損はこのパターン（L224996 以降 236行破損）

設計:
- 書き込み: tmpfile (同じディレクトリ) → json.dump → flush+fsync → os.replace
- os.replace は Windows/POSIX 両方で原子的（破損中のファイルがディスクに残らない）
- filelock は OS レベルのアドバイザリロック（別プロセスも待つ）
- .lock ファイルは対象ファイルと同じディレクトリに作成

使い方:
    from src.utils.atomic_json import atomic_write_json
    atomic_write_json("data/predictions/20260419_pred.json", pred_dict)
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

try:
    from filelock import FileLock, Timeout
    _HAS_FILELOCK = True
except ImportError:
    _HAS_FILELOCK = False


class AtomicJsonError(RuntimeError):
    """原子書き込み失敗時の例外"""


def _lock_path_for(path: str) -> str:
    """対象ファイルに対応するロックファイルパスを返す"""
    p = Path(path)
    return str(p.parent / f".{p.name}.lock")


def atomic_write_json(
    path: str | Path,
    data: Any,
    *,
    indent: Optional[int] = 2,
    ensure_ascii: bool = False,
    lock_timeout: float = 30.0,
    separators: Optional[tuple] = None,
) -> None:
    """JSON を原子的に書き込む（プロセス間ロック付き）。

    Parameters
    ----------
    path : str | Path
        書き込み先ファイルパス
    data : Any
        json.dump 可能なオブジェクト
    indent : int | None
        JSON インデント（None で minify）
    ensure_ascii : bool
        ASCII エスケープするか（False で UTF-8 そのまま）
    lock_timeout : float
        ロック取得タイムアウト秒
    separators : tuple | None
        json.dump の separators 引数（ファイルサイズ削減用）

    Raises
    ------
    AtomicJsonError
        ロック取得失敗や書き込み失敗時

    動作:
    1. filelock で対象ファイルの排他ロックを取る（他プロセス/スレッドが待つ）
    2. 同じディレクトリに tmpfile を作る（os.replace は同一デバイスが必要）
    3. json.dump → flush → fsync → close
    4. os.replace(tmp, target) で原子差し替え
    5. ロック解放
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _lock_path_for(str(target))

    def _do_write() -> None:
        # 同一ディレクトリ内 tmpfile（os.replace のデバイス同一性要件を満たす）
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{target.name}.tmp.",
            dir=str(target.parent),
            suffix=".json",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as wf:
                if separators is not None:
                    json.dump(data, wf, ensure_ascii=ensure_ascii, separators=separators)
                else:
                    json.dump(data, wf, ensure_ascii=ensure_ascii, indent=indent)
                wf.flush()
                os.fsync(wf.fileno())
            # ここまで成功したら原子的差し替え
            os.replace(tmp_path, str(target))
        except Exception as e:
            # 失敗時は tmp を片付ける
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise AtomicJsonError(f"atomic write failed: {target}: {e}") from e

    if _HAS_FILELOCK:
        try:
            with FileLock(lock_path, timeout=lock_timeout):
                _do_write()
        except Timeout as e:
            raise AtomicJsonError(
                f"lock timeout ({lock_timeout}s): {lock_path}"
            ) from e
    else:
        # filelock 無し: 原子書き込みのみ（並行はベストエフォート）
        _do_write()


def atomic_read_modify_write_json(
    path: str | Path,
    modifier,
    *,
    indent: Optional[int] = 2,
    ensure_ascii: bool = False,
    lock_timeout: float = 30.0,
) -> Any:
    """ロック取得 → 読み込み → modifier(data) → 書き戻し を一貫して行う。

    並行 read-modify-write の完全なシリアライズが必要な場合に使う。
    modifier は data dict を受け取って修正する（in-place でも戻り値でも可）。

    戻り値: 書き込み後の data。
    """
    target = Path(path)
    lock_path = _lock_path_for(str(target))

    def _do_rmw() -> Any:
        if target.exists():
            with open(target, "r", encoding="utf-8") as rf:
                data = json.load(rf)
        else:
            data = None
        result = modifier(data)
        data = result if result is not None else data

        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{target.name}.tmp.",
            dir=str(target.parent),
            suffix=".json",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as wf:
                json.dump(data, wf, ensure_ascii=ensure_ascii, indent=indent)
                wf.flush()
                os.fsync(wf.fileno())
            os.replace(tmp_path, str(target))
        except Exception as e:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise AtomicJsonError(f"rmw failed: {target}: {e}") from e
        return data

    if _HAS_FILELOCK:
        try:
            with FileLock(lock_path, timeout=lock_timeout):
                return _do_rmw()
        except Timeout as e:
            raise AtomicJsonError(
                f"lock timeout ({lock_timeout}s): {lock_path}"
            ) from e
    else:
        return _do_rmw()


__all__ = ["atomic_write_json", "atomic_read_modify_write_json", "AtomicJsonError"]
