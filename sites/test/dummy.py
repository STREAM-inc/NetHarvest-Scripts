# scripts/sites/test/dummy.py
"""
インフラ稼働テスト用ダミークローラー

通信を一切行わず、ダミーデータを yield するだけのクローラー。
フレームワーク全体の動作確認に使用する。

テストカバレッジ:
    ✅ BaseCrawler の Template Method フロー (_setup → prepare → parse → finalize → teardown)
    ✅ self.logger によるクラス名付きログ出力
    ✅ Pipeline の検証 (Schema カラム + EXTRA_COLUMNS)
    ✅ Pipeline の正規化 (TEL 全角→半角)
    ✅ Pipeline の CSV ヘッダー事前決定 (Schema 定義順 + EXTRA)
    ✅ 複数件 yield (件数カウント item_count)
    ✅ observed_columns の自動収集 (データカタログ用)
    ✅ EXTRA_COLUMNS によるカスタムカラム追加
    ✅ None 値の空文字変換 (normalizer)
    ✅ DELAY による待機
    ✅ prepare() / finalize() のフック動作
    ✅ カラム数がアイテムごとに違っても正常動作

実行方法:
    # ローカル単体テスト（Docker 不要）
    cd /path/to/NetHarvest
    python scripts/sites/dummy.py

    # Prefect Flow 経由
    python bin/run_flow.py --site dummy --url https://example.com
"""

import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル実行用）
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.base import BaseCrawler
from src.const.schema import Schema


class DummyScraper(BaseCrawler):
    """
    インフラ稼働テスト用のダミークローラー。
    通信を行わず、即座にダミーデータを複数件 yield する。
    """

    # --- クラス変数（テスト対象） ---
    DELAY = 0.1              # テスト用に短い待機時間
    EXTRA_COLUMNS = ["備考","空欄"]  # カスタムカラムのテスト

    # =========================================================================
    # 内部リソース管理（BaseCrawler 抽象メソッドの実装）
    # =========================================================================

    def _setup(self):
        """ブラウザ/セッションの起動（ダミーなので何もしない）"""
        self.logger.debug("🔧 セットアップ完了")

    def _teardown_resources(self):
        """リソース解放（ダミーなので何もしない）"""
        self.logger.debug("🧹 リソース解放完了")

    # =========================================================================
    # フックメソッド（任意オーバーライド）
    # =========================================================================

    def prepare(self):
        """前処理のテスト"""
        self.logger.debug("📋 prepare() フック実行")

    def finalize(self):
        """後処理のテスト"""
        self.logger.debug("📋 finalize() フック実行")

    # =========================================================================
    # メインロジック（必須実装）
    # =========================================================================

    def parse(self, url: str):
        """
        ダミーデータを yield する。

        テストケース:
            1件目: 基本カラム（NAME, ADDR, TEL, URL）— 正常データ
            2件目: 全角 TEL — 正規化テスト
            3件目: EXTRA_COLUMNS — カスタムカラムテスト
            4件目: None 値 — 空文字変換テスト
            5件目: 多数のカラム — 幅広い observed_columns テスト
        """
        self.logger.debug("🚀 parse() 開始 (url=%s)", url)

        # --- 1件目: 基本的な正常データ ---
        yield {
            Schema.NAME: "株式会社テスト１号",
            Schema.ADDR: "東京都千代田区丸の内1-1-1",
            Schema.TEL:  "03-1234-5678",
            Schema.URL:  url,
        }

        # --- 2件目: TEL 全角→半角の正規化テスト ---
        yield {
            Schema.NAME: "株式会社テスト２号",
            Schema.ADDR: "大阪市北区梅田2-2-2",
            Schema.TEL:  "０６−９８７６−５４３２",  # ← 全角（normalizer で半角に変換される）
            Schema.URL:  url,
        }

        # --- 3件目: EXTRA_COLUMNS（カスタムカラム）テスト ---
        yield {
            Schema.NAME: "株式会社テスト３号",
            Schema.ADDR: "福岡市博多区博多駅前3-3-3",
            Schema.TEL:  "092-111-2222",
            Schema.URL:  url,
            "備考":       "カスタムカラム確認用",
        }

        # --- 4件目: None 値の空文字変換テスト ---
        yield {
            Schema.NAME: "株式会社テスト４号",
            Schema.ADDR: None,   # ← normalizer が "" に変換
            Schema.TEL:  None,   # ← normalizer が "" に変換
            Schema.URL:  url,
        }

        # --- 5件目: 多数の Schema カラムを使用 ---
        yield {
            Schema.NAME:      "株式会社テスト５号",
            Schema.NAME_KANA: "カブシキガイシャテストゴゴウ",
            Schema.PREF:      "北海道",
            Schema.ADDR:      "札幌市中央区北1条西5-5-5",
            Schema.TEL:       "011-333-4444",
            Schema.CO_NUM:    "1234567890123",
            Schema.REP_NM:    "代表 太郎",
            Schema.EMP_NUM:   "100",
            Schema.LOB:       "ソフトウェア開発",
            Schema.CAP:       "1000万円",
            Schema.HP:        "https://example.com",
            Schema.URL:       url,
            "備考":            "全カラム網羅テスト",
        }

        self.logger.info("✅ parse() 完了 (5件 yield)")


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DummyScraper()
    scraper.execute("https://example.com/dummy")

    # --- 実行結果の確認 ---
    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print(f"  EXTRA カラム:     {scraper.extra_columns}")
    print("=" * 60)

    # CSV の中身を表示
    if scraper.output_filepath:
        print("\n📄 CSV 内容:")
        print("-" * 60)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            print(f.read())
