"""
競馬解析マスターシステム v3.0 - 認証情報セットアップ

README / QUICKSTART で案内している以下のコマンドで実行します:
  python -m src.setup_credentials

netkeiba と 競馬ブックスマート のID・パスワードを対話式で入力し、
~/.keiba_credentials.json に保存します。
"""

import os
import sys

# プロジェクトルートをパスに追加
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)


if __name__ == "__main__":
    # auth モジュールの --setup を実行（netkeiba + 競馬ブックの両方設定）
    import subprocess

    code = subprocess.run(
        [sys.executable, "-m", "src.scraper.auth", "--setup"],
        cwd=_root,
    ).returncode
    sys.exit(code)
