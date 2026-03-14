"""
競馬ブック「能力表HTML」をログイン済みセッションで1件取得し、HTMLを保存する検証スクリプト。

使い方:
  python scripts/fetch_keibabook_nouryoku.py

認証: KEIBABOOK_ID / KEIBABOOK_PASS または ~/.keiba_credentials.json の keibabook_id / keibabook_pass
保存先: data/cache/keibabook/nouryoku_verification_*.html
"""
import os
import sys

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 地方・中央のサンプルURL（検証用）
KB_NOURYOKU_CHIHOU = "https://s.keibabook.co.jp/chihou/nouryoku_html/2026021302010225"
KB_NOURYOKU_CYUOU = "https://s.keibabook.co.jp/cyuou/nouryoku_html/202601040811"


def main():
    try:
        from src.scraper.keibabook_training import KeibabookClient, KeibabookCredentials, KB_BASE
    except ImportError as e:
        print("ImportError:", e)
        print("プロジェクトルートで実行してください: python scripts/fetch_keibabook_nouryoku.py")
        return 1

    try:
        from config.settings import KEIBABOOK_CACHE_DIR
        cache_dir = KEIBABOOK_CACHE_DIR
    except Exception:
        cache_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "cache", "keibabook"
        )
    os.makedirs(cache_dir, exist_ok=True)

    kid, pwd = KeibabookCredentials.load()
    if not kid or not pwd:
        print("認証情報がありません。KEIBABOOK_ID / KEIBABOOK_PASS を設定するか、")
        print("  python -m src.scraper.keibabook_training --setup")
        print("で ~/.keiba_credentials.json に保存してください。")
        return 1

    client = KeibabookClient(keibabook_id=kid, password=pwd)
    if not client.ensure_login():
        print("ログインに失敗しました。ID/パスワードを確認してください。")
        return 1
    print("ログイン成功")

    def fetch_and_save(label: str, url: str) -> None:
        path = os.path.join(
            cache_dir,
            f"nouryoku_verification_{label}.html"
        )
        try:
            resp = client.session.get(url, timeout=15)
            resp.encoding = "utf-8"
            with open(path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            size = len(resp.text)
            if "ログインして下さい" in resp.text or "ログイン" in resp.text[:2000]:
                print(f"  [{label}] 保存: {path} ({size} bytes) ※「ログイン」の文言あり＝要確認")
            else:
                print(f"  [{label}] 保存: {path} ({size} bytes)")
        except Exception as e:
            print(f"  [{label}] 取得失敗: {e}")

    print("能力表HTMLを取得中...")
    fetch_and_save("chihou", KB_NOURYOKU_CHIHOU)
    fetch_and_save("cyuou", KB_NOURYOKU_CYUOU)
    print("完了。保存したHTMLを開き、テーブル構造・馬名・枠順・騎手等が含まれるか確認してください。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
