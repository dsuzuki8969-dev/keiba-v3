#!/usr/bin/env bash
set -e
echo "================================================"
echo "競馬解析マスターシステム v3.0 - セットアップ"
echo "================================================"
echo ""

if ! command -v python3 &> /dev/null; then
    echo "[エラー] Python3 がインストールされていません。"
    exit 1
fi

echo "依存パッケージをインストールしています..."
python3 -m pip install -r requirements.txt

echo ""
echo "================================================"
echo "セットアップ完了！"
echo "================================================"
echo ""
echo "次のステップ:"
echo "  1. 認証設定: python3 -m src.setup_credentials"
echo "  2. デモ実行:  python3 demo.py"
echo "  3. レース分析: python3 analyze.py または python3 main.py --race_id レースID"
echo ""
