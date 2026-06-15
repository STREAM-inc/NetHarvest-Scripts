"""
JADMA 正会員一覧 — 日本通信販売協会 正会員企業一覧

取得対象:
    - 正会員企業の基本情報（社名・フリガナ・代表者・役職・郵便番号・都道府県・住所・TEL・HP・業種カテゴリ・入会日）
    - 合計375社（2026-06時点）

取得フロー:
    一覧ページに対してNext.js Server Action（POST）を呼び出し、50件×8ページをページングして全件取得。
    詳細ページは存在せず、全フィールドがAPIレスポンスに含まれる。

    Server Action ID: f0233e1a5d9788b5676723e17027c39bd5a6373e
    POST ボディ: [{"pageParam": N, "keyword": "", "memberType": "regular", "abcOrder": "", "category": ""}]
    レスポンス形式: RSC テキスト (行頭 "1:" の行が JSON データ本体)

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jadma.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jadma
"""

import json
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_ACTION_ID = "f0233e1a5d9788b5676723e17027c39bd5a6373e"
_PAGE_SIZE = 50

# 正会員向け商品カテゴリ ID → ラベル（サイト JS の bG 定数から抽出）
_CATEGORY_MAP = {
    "1": "紳士衣料品",
    "2": "婦人衣料品",
    "3": "下着",
    "4": "子供・ベビー衣料用品",
    "5": "その他の衣料品",
    "6": "服飾雑貨",
    "7": "靴・鞄",
    "8": "アクセサリー、宝石、貴金属、時計",
    "9": "その他の服飾雑貨・貴金属",
    "10": "家具・収納用品",
    "11": "インテリア、敷物、寝具",
    "12": "ＴＶ・ビデオ・ＤＶＤ・ブルーレイ機器、カメラ、オーディオ類",
    "13": "パソコン（周辺機器を含む）",
    "14": "家庭電気製品",
    "15": "食器・台所家庭用品・トイレタリー",
    "16": "その他の家具・家電家庭用品",
    "17": "日曜大工、花・ガーデニング、ペット関連",
    "18": "スポーツ・レジャー用品、乗り物関連",
    "19": "手芸、工作用品",
    "20": "ゲーム機、おもちゃ、パソコン/ゲームソフト（ダウンロードサービスを除く）",
    "21": "本・雑誌・コミック（ダウンロードサービスを除く）",
    "22": "ＣＤ及びＤＶＤソフト（ダウンロードサービスを除く）",
    "23": "ダウンロードサービス（電子書籍・音楽・パソコン・ゲーム・映像など）",
    "24": "美術工芸品、骨董品",
    "25": "文房具、事務用品",
    "26": "その他の趣味、娯楽品",
    "27": "美容、健康・医療器具",
    "28": "化粧品",
    "29": "医薬品・介護用品等その他",
    "30": "食料品（健康食品、地方特産品・産直品・飲料を除く）",
    "31": "地方特産品・産直品",
    "32": "健康食品",
    "33": "飲料（水・酒類、ソフトドリンク等）",
    "34": "その他の食料品",
    "35": "通信教育講座",
    "36": "旅行",
    "37": "保険・金融",
    "38": "コンサート、演劇のチケット",
    "39": "交通機関等のチケット",
    "40": "その他のサービス",
    "41": "その他",
    "42": "ギフト・お取り寄せ",
    "43": "防災用品",
}


class JadmaCrawler(StaticCrawler):
    """JADMA 正会員一覧 スクレイパー"""

    DELAY = 0  # API レスポンスが50件単位のため per-item delay は不要。ページ間は parse() 内で制御。
    EXTRA_COLUMNS = ["入会年月日", "お客様相談室TEL"]
    _PAGE_DELAY = 1.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        page_param = 0
        total_count = None

        while True:
            try:
                resp = self.session.post(
                    url,
                    json=[{
                        "pageParam": page_param,
                        "keyword": "",
                        "memberType": "regular",
                        "abcOrder": "",
                        "category": "",
                    }],
                    headers={"Next-Action": _ACTION_ID},
                    timeout=self.TIMEOUT,
                )
                resp.raise_for_status()
                resp.encoding = "utf-8"  # Content-Type: text/x-component は charset 未指定のため強制指定
            except Exception as e:
                self.logger.error("Page %d fetch error: %s", page_param, e)
                break

            data = self._parse_rsc(resp.text)
            if data is None:
                self.logger.warning("RSC parse failed on page %d", page_param)
                break

            companies = data.get("companies", [])
            if not companies:
                break

            if total_count is None:
                total_count = data.get("totalCount", 0)
                self.total_items = total_count

            for c in companies:
                try:
                    yield self._map_company(c, url)
                except Exception as e:
                    self.logger.error("Map error for %s: %s", c.get("companyName"), e)
                    continue

            page_param += 1
            if total_count and page_param * _PAGE_SIZE >= total_count:
                break
            time.sleep(self._PAGE_DELAY)

    def _parse_rsc(self, text: str) -> dict | None:
        """RSC レスポンスから "1:" で始まる行を JSON としてパースする。"""
        for line in text.strip().split("\n"):
            if line.startswith("1:"):
                try:
                    return json.loads(line[2:])
                except json.JSONDecodeError:
                    return None
        return None

    def _map_company(self, c: dict, url: str) -> dict:
        addr_parts = [c.get("companyCity", ""), c.get("companyAddress", "")]
        addr = " ".join(p for p in addr_parts if p)

        join_date = c.get("joinDate", "")
        if join_date.startswith("$D"):
            join_date = join_date[2:].split("T")[0]

        return {
            Schema.URL: url,
            Schema.NAME: c.get("companyName", ""),
            Schema.NAME_KANA: c.get("companyNameFurigana", ""),
            Schema.TEL: c.get("companyPhone", ""),
            Schema.POS_NM: c.get("companyRepPosition", ""),
            Schema.REP_NM: c.get("companyRepName", ""),
            Schema.POST_CODE: c.get("companyPostCode", ""),
            Schema.PREF: c.get("companyPrefecture", ""),
            Schema.ADDR: addr,
            Schema.HP: c.get("companySiteUrl", ""),
            Schema.CAT_SITE: self._category_labels(c.get("categories", "")),
            "入会年月日": join_date,
            "お客様相談室TEL": c.get("companyContact", ""),
        }

    def _category_labels(self, categories_str: str) -> str:
        if not categories_str:
            return ""
        labels = [_CATEGORY_MAP.get(cid.strip(), "") for cid in categories_str.split(",")]
        return "、".join(lb for lb in labels if lb)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JadmaCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://jadma.or.jp/membercompany/fmember")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
